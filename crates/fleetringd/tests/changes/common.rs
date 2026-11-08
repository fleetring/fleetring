//! Data nodes built by hand, so a test can give them any sink: a control daemon, and registries
//! with replication and change streams around them. [`Harness`] runs one node whose databases
//! have one replica, so the primary alone is the write quorum and a write is acknowledged as soon
//! as it commits; [`Fleet`] runs three that replicate over real TCP, for failovers.

use std::collections::BTreeMap;
use std::error::Error;
use std::future::Future;
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use fleetring::{NodeId, ReplicationFactor};
use fleetring_control::{DatabaseName, LeaseEpoch, Member};
use fleetringd::changes::{
    CaptureRules, ChangeParts, ChangeSettings, ChangeStreams, ConfiguredSink, DatabaseMatchers,
};
use fleetringd::clock::SystemClock;
use fleetringd::engine::{
    CaptureMode, Database, DatabaseCapability, DatabaseEngine, DatabaseSnapshot, EngineCapability,
    EngineConfig, EngineError, EngineKind, Executed, Execution, Interrupt, LogicalChanges,
    LogicalSupport, OpenMode, Rows, SqliteEngine, Statement, Transaction,
};
use fleetringd::replication::{
    EpochHolders, PrimaryRole, Replication, ReplicationParts, ReplicationSettings,
};
use fleetringd::transport::{Peers, replication_server};
use fleetringd::{
    Config, Daemon, Membership, MembershipSettings, MembershipTasks, Network, Registry,
};
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tonic::transport::Server;
use tonic::transport::server::TcpIncoming;

use crate::support::Tls;

pub(crate) type TestResult = Result<(), Box<dyn Error>>;

/// Guards against a hang only; nothing here asserts how long anything takes.
pub(crate) const HANG_GUARD: Duration = Duration::from_secs(60);

/// The page size the tests give SQLite databases, small so that WAL files stay small.
const PAGE_SIZE: u32 = 512;

pub(crate) fn name(text: &str) -> DatabaseName {
    text.parse().expect("valid database name")
}

/// Every database the tests stream.
pub(crate) fn every_database() -> DatabaseMatchers {
    DatabaseMatchers::new(["*".parse().expect("a star")])
}

/// Waits for `future`, failing the test instead of hanging.
pub(crate) async fn guarded<T>(
    what: &str,
    future: impl Future<Output = T>,
) -> Result<T, Box<dyn Error>> {
    tokio::time::timeout(HANG_GUARD, future)
        .await
        .map_err(|_| format!("{what} didn't happen").into())
}

/// Polls `condition` until it holds.
pub(crate) async fn eventually(what: &str, mut condition: impl FnMut() -> bool) -> TestResult {
    guarded(what, async {
        while !condition() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
}

/// A control daemon and the directory the node keeps its files in.
pub(crate) struct Harness {
    control: Daemon,
    dir: TempDir,
    tls: Tls,
}

impl Harness {
    pub(crate) async fn start() -> Result<Self, Box<dyn Error>> {
        let _ = tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .with_test_writer()
            .try_init();
        let dir = tempfile::tempdir()?;
        let tls = Tls::create(dir.path())?;
        let control = Daemon::start(Config::from_toml(&format!(
            "data_dir = {:?}\nlisten = \"127.0.0.1:0\"\nrole = \"control\"\n\n[node]\nid = \"control\"\n{}",
            dir.path().join("control"),
            tls.node_table("control")?
        ))?)
        .await?;
        Ok(Self { control, dir, tls })
    }

    /// The node's root directory: its databases, replication state, and change positions.
    pub(crate) fn root(&self) -> PathBuf {
        self.dir.path().join("node")
    }

    /// Starts the one data node with `sinks`, over the root directory, which it reuses if a
    /// node ran there before. Every database records row changes in the default mode.
    pub(crate) async fn node(&self, sinks: Vec<ConfiguredSink>) -> Result<Node, Box<dyn Error>> {
        self.node_capturing(sinks, CaptureMode::default()).await
    }

    /// As [`node`](Self::node), with every database recording row changes in `mode`.
    pub(crate) async fn node_capturing(
        &self,
        sinks: Vec<ConfiguredSink>,
        mode: CaptureMode,
    ) -> Result<Node, Box<dyn Error>> {
        self.node_on(sqlite(), sinks, mode).await
    }

    /// As [`node_capturing`](Self::node_capturing), on `engine`.
    pub(crate) async fn node_on(
        &self,
        engine: impl DatabaseEngine + Clone,
        sinks: Vec<ConfiguredSink>,
        mode: CaptureMode,
    ) -> Result<Node, Box<dyn Error>> {
        let control_address = self.control.control_address().ok_or("no control address")?;
        let id: NodeId = "node".parse()?;
        let network = self.tls.node_network("node")?;
        let (membership, tasks) = Membership::start(
            Member::new(id.clone()),
            network.control_client(control_address.to_string()),
            &MembershipSettings {
                heartbeat_interval_ms: std::num::NonZeroU64::new(100).ok_or("nonzero")?,
                ..MembershipSettings::default()
            },
        );
        let root = self.root();
        let replication = start_replication(id, &root, engine, membership.clone(), network, mode);
        let streams = ChangeStreams::start(ChangeParts {
            sinks,
            replication: replication.clone(),
            state_dir: root.join("changes"),
            settings: ChangeSettings {
                retry_initial: Duration::from_millis(10),
                retry_max: Duration::from_millis(100),
            },
            metrics: fleetringd::observe::ChangeMetrics::default(),
        });
        guarded(
            "membership converging",
            membership.wait_for(|view| view.members().len() == 1),
        )
        .await?
        .ok_or("membership stopped")?;
        Ok(Node {
            replication,
            streams,
            _membership: tasks,
        })
    }
}

/// The data node.
pub(crate) struct Node {
    pub(crate) replication: Replication,
    pub(crate) streams: ChangeStreams,
    _membership: MembershipTasks,
}

impl Node {
    pub(crate) fn registry(&self) -> &Registry {
        self.replication.registry()
    }

    /// Creates `database` with one replica, serves it as primary at epoch 1, and runs `schema`.
    pub(crate) async fn create(&self, database: &DatabaseName, schema: &str) -> TestResult {
        guarded("creating the database", self.registry().create(database)).await??;
        self.replication.serve_primary(
            database.clone(),
            PrimaryRole::new(ReplicationFactor::ONE, LeaseEpoch::new(1)),
        )?;
        self.execute(database, Transaction::from(Statement::new(schema)))
            .await?;
        Ok(())
    }

    /// Runs `transaction` and waits for its acknowledgement.
    pub(crate) async fn execute(
        &self,
        database: &DatabaseName,
        transaction: Transaction,
    ) -> TestResult {
        guarded("the write", self.registry().execute(database, transaction)).await??;
        Ok(())
    }

    /// Waits until every pump has delivered everything acknowledged.
    pub(crate) async fn await_caught_up(&self) -> TestResult {
        if guarded(
            "sinks catching up",
            self.streams.await_caught_up(HANG_GUARD),
        )
        .await?
        {
            Ok(())
        } else {
            Err("sinks didn't catch up".into())
        }
    }

    /// Stops the streams at once, without waiting for them to catch up, and the node with them.
    pub(crate) async fn kill(self) {
        self.streams.stop(Duration::ZERO).await;
        self.replication.stop();
        self.registry().close_all().await;
    }
}

fn start_replication(
    node: NodeId,
    root: &Path,
    engine: impl DatabaseEngine + Clone,
    membership: Membership,
    network: Network,
    mode: CaptureMode,
) -> Replication {
    let config = EngineConfig {
        execution: Execution::Threads,
        max_open: NonZeroUsize::new(64).expect("nonzero"),
        page_size: PAGE_SIZE,
        ..EngineConfig::default()
    };
    let databases = root.join("databases");
    let registry_engine = engine.clone();
    Replication::start(
        ReplicationParts {
            node,
            engine: Arc::new(engine),
            membership,
            peers: Arc::new(Peers::with_network(network)),
            addresses: Arc::new(BTreeMap::<NodeId, String>::new()),
            authority: Arc::new(BTreeMap::<DatabaseName, EpochHolders>::new()),
            clock: Arc::new(SystemClock),
            execution: Execution::Threads,
            state_dir: root.join("replication"),
            settings: ReplicationSettings {
                retry_interval: Duration::from_millis(50),
                ..ReplicationSettings::new()
            },
            metrics: fleetringd::observe::ReplicationMetrics::default(),
        },
        move |sink| {
            Registry::new(registry_engine, sink, databases, config)
                .with_recorded_changes(CaptureRules::every(every_database(), mode))
        },
    )
}

/// The SQLite engine the tests run.
pub(crate) fn sqlite() -> SqliteEngine {
    SqliteEngine::new(PAGE_SIZE).expect("valid page size")
}

/// SQLite with the sink marks taken away: every operation is SQLite's except `sink_marks`, which
/// is `None`, as for an engine that can't keep positions inside its database (ADR-0028).
#[derive(Debug, Clone)]
pub(crate) struct NoMarks(SqliteEngine);

impl NoMarks {
    pub(crate) fn new() -> Self {
        Self(sqlite())
    }
}

impl DatabaseEngine for NoMarks {
    fn kind(&self) -> EngineKind {
        self.0.kind()
    }

    fn capability(&self) -> EngineCapability<'_> {
        self.0.capability()
    }

    fn logical_support(&self) -> LogicalSupport {
        self.0.logical_support()
    }

    fn open(&self, location: &Path, mode: OpenMode) -> Result<Box<dyn Database>, EngineError> {
        Ok(Box::new(NoMarksDatabase(self.0.open(location, mode)?)))
    }
}

#[derive(Debug)]
struct NoMarksDatabase(Box<dyn Database>);

impl Database for NoMarksDatabase {
    fn execute(
        &mut self,
        transaction: &Transaction,
        interrupt: &Interrupt,
    ) -> Result<Executed, EngineError> {
        self.0.execute(transaction, interrupt)
    }

    fn query(&mut self, statement: &Statement, interrupt: &Interrupt) -> Result<Rows, EngineError> {
        self.0.query(statement, interrupt)
    }

    fn snapshot(&mut self, destination: &Path) -> Result<DatabaseSnapshot, EngineError> {
        self.0.snapshot(destination)
    }

    fn capability(&mut self) -> DatabaseCapability<'_> {
        self.0.capability()
    }

    fn logical_changes(&mut self) -> Option<&mut dyn LogicalChanges> {
        self.0.logical_changes()
    }

    fn close(self: Box<Self>) -> Result<(), EngineError> {
        self.0.close()
    }
}

/// Inserts `value` into `t`.
pub(crate) fn insert(value: i64) -> Transaction {
    Transaction::from(Statement::with_params(
        "INSERT INTO t (value) VALUES (?1)",
        [value.into()],
    ))
}

/// A control daemon and data nodes that replicate over real TCP, each running change streams: the
/// shape a failover needs. A test moves the primary by hand, killing a node and serving the
/// database on another at the next epoch.
pub(crate) struct Fleet {
    nodes: Vec<FleetNode>,
    dir: TempDir,
    _control: Daemon,
    _tls: Tls,
}

/// One node of a [`Fleet`].
pub(crate) struct FleetNode {
    pub(crate) id: NodeId,
    pub(crate) replication: Replication,
    pub(crate) streams: ChangeStreams,
    /// The node's server, its membership, and whether it is still running.
    _running: Option<Running>,
}

struct Running {
    _server: (oneshot::Sender<()>, JoinHandle<()>),
    _membership: MembershipTasks,
}

fn node_id(text: &str) -> NodeId {
    text.parse().expect("valid node id")
}

/// The replication factor of every database in a [`Fleet`]: with three replicas the write quorum
/// is two, so a promoted follower and the third node still acknowledge writes once the old
/// primary is gone.
pub(crate) const REPLICAS: u8 = 3;

/// Settings that keep a loaded runner from tripping stall detection, and that stop a lagging
/// change stream from holding checkpoints back at all, so that a test can force the log to
/// restart past a sink's mark.
fn fleet_settings() -> ReplicationSettings {
    ReplicationSettings {
        stall_timeout: HANG_GUARD,
        ack_timeout: HANG_GUARD,
        retry_interval: Duration::from_millis(50),
        lag_limit: Duration::ZERO,
        ..ReplicationSettings::new()
    }
}

impl Fleet {
    /// Starts one node per name in `ids`, each streaming to the sinks `sinks` builds for it.
    pub(crate) async fn start(
        ids: &[&str],
        sinks: impl Fn(&str) -> Vec<ConfiguredSink>,
    ) -> Result<Self, Box<dyn Error>> {
        let _ = tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .with_test_writer()
            .try_init();
        let dir = tempfile::tempdir()?;
        let tls = Tls::create(dir.path())?;
        let control = Daemon::start(Config::from_toml(&format!(
            "data_dir = {:?}\nlisten = \"127.0.0.1:0\"\nrole = \"control\"\n\n[node]\nid = \"control\"\n{}",
            dir.path().join("control"),
            tls.node_table("control")?
        ))?)
        .await?;
        let control_address = control.control_address().ok_or("no control address")?;

        let mut listeners = Vec::new();
        let mut addresses = BTreeMap::new();
        for name in ids {
            let listener = TcpListener::bind("127.0.0.1:0").await?;
            addresses.insert(node_id(name), listener.local_addr()?.to_string());
            listeners.push(listener);
        }
        let addresses = Arc::new(addresses);
        let mut nodes = Vec::new();
        let mut memberships = Vec::new();
        for (name, listener) in ids.iter().zip(listeners) {
            let root = dir.path().join(name);
            let (membership, tasks) = Membership::start(
                Member::new(node_id(name)),
                tls.node_network(name)?
                    .control_client(control_address.to_string()),
                &MembershipSettings {
                    heartbeat_interval_ms: std::num::NonZeroU64::new(100).ok_or("nonzero")?,
                    ..MembershipSettings::default()
                },
            );
            let replication = start_fleet_replication(
                node_id(name),
                &root,
                membership.clone(),
                Arc::clone(&addresses),
            );
            let streams = ChangeStreams::start(ChangeParts {
                sinks: sinks(name),
                replication: replication.clone(),
                state_dir: root.join("changes"),
                settings: ChangeSettings {
                    retry_initial: Duration::from_millis(10),
                    retry_max: Duration::from_millis(100),
                },
                metrics: fleetringd::observe::ChangeMetrics::default(),
            });
            let router = tonic::service::Routes::new(replication_server(replication.service()));
            let (stop, stopped) = oneshot::channel::<()>();
            let server = tokio::spawn(async move {
                let shutdown = async {
                    let _stopped = stopped.await;
                };
                let served = Server::builder()
                    .add_routes(router)
                    .serve_with_incoming_shutdown(TcpIncoming::from(listener), shutdown)
                    .await;
                if let Err(error) = served {
                    tracing::error!(%error, "test server failed");
                }
            });
            memberships.push(membership);
            nodes.push(FleetNode {
                id: node_id(name),
                replication,
                streams,
                _running: Some(Running {
                    _server: (stop, server),
                    _membership: tasks,
                }),
            });
        }
        for membership in memberships {
            guarded(
                "membership converging",
                membership.wait_for(|view| view.members().len() == ids.len()),
            )
            .await?
            .ok_or("membership stopped")?;
        }
        Ok(Self {
            nodes,
            dir,
            _control: control,
            _tls: tls,
        })
    }

    pub(crate) fn node(&self, name: &str) -> &FleetNode {
        self.nodes
            .iter()
            .find(|node| node.id.as_str() == name)
            .expect("a node with that name")
    }

    /// Where `name` keeps its files.
    pub(crate) fn root(&self, name: &str) -> PathBuf {
        self.dir.path().join(name)
    }

    /// Creates `database` on `primary`, serves it there at epoch 1, runs `schema`, and waits until
    /// both followers hold it.
    pub(crate) async fn create(
        &self,
        primary: &str,
        database: &DatabaseName,
        schema: &str,
    ) -> TestResult {
        let node = self.node(primary);
        guarded("creating the database", node.registry().create(database)).await??;
        node.replication.serve_primary(database.clone(), role(1))?;
        let schema = Transaction::from(Statement::new(schema));
        // The first write waits until the streams are open.
        guarded("the first write", async {
            loop {
                match node.registry().execute(database, schema.clone()).await {
                    Ok(_) => return,
                    Err(_) => tokio::time::sleep(Duration::from_millis(20)).await,
                }
            }
        })
        .await?;
        self.await_followers(primary, database).await
    }

    /// Runs `transaction` on `primary` and waits for its acknowledgement.
    pub(crate) async fn execute(
        &self,
        primary: &str,
        database: &DatabaseName,
        transaction: Transaction,
    ) -> TestResult {
        let registry = self.node(primary).registry();
        guarded("the write", registry.execute(database, transaction)).await??;
        Ok(())
    }

    /// Waits until every follower of `primary` has acknowledged everything it committed.
    pub(crate) async fn await_followers(
        &self,
        primary: &str,
        database: &DatabaseName,
    ) -> TestResult {
        let replication = &self.node(primary).replication;
        eventually("followers catching up", || {
            replication.primary_status(database).is_some_and(|status| {
                status.committed.is_some()
                    && status
                        .followers
                        .iter()
                        .all(|follower| follower.acknowledged == status.committed)
            })
        })
        .await
    }

    /// Stops `name` at once, as a crash does: its streams, replication, and server, without
    /// flushing anything.
    pub(crate) async fn kill(&mut self, name: &str) {
        let node = self
            .nodes
            .iter_mut()
            .find(|node| node.id.as_str() == name)
            .expect("a node with that name");
        node.streams.stop(Duration::ZERO).await;
        node.replication.stop();
        node.registry().close_all().await;
        node._running = None;
    }

    /// Serves `database` on `name` as primary at `epoch`, as a promotion does.
    pub(crate) fn promote(&self, name: &str, database: &DatabaseName, epoch: u64) -> TestResult {
        self.node(name).replication.serve_primary(
            database.clone(),
            PrimaryRole::new(
                ReplicationFactor::new(REPLICAS).ok_or("a replication factor")?,
                LeaseEpoch::new(epoch),
            ),
        )?;
        Ok(())
    }
}

impl FleetNode {
    pub(crate) fn registry(&self) -> &Registry {
        self.replication.registry()
    }

    /// Waits until every pump of this node has delivered everything acknowledged.
    pub(crate) async fn await_caught_up(&self) -> TestResult {
        if guarded(
            "sinks catching up",
            self.streams.await_caught_up(HANG_GUARD),
        )
        .await?
        {
            Ok(())
        } else {
            Err("sinks didn't catch up".into())
        }
    }
}

fn role(epoch: u64) -> PrimaryRole {
    PrimaryRole::new(
        ReplicationFactor::new(REPLICAS).expect("nonzero"),
        LeaseEpoch::new(epoch),
    )
}

fn start_fleet_replication(
    node: NodeId,
    root: &Path,
    membership: Membership,
    addresses: Arc<BTreeMap<NodeId, String>>,
) -> Replication {
    let config = EngineConfig {
        execution: Execution::Threads,
        max_open: NonZeroUsize::new(64).expect("nonzero"),
        page_size: PAGE_SIZE,
        ..EngineConfig::default()
    };
    let databases = root.join("databases");
    let engine = sqlite();
    let registry_engine = engine.clone();
    Replication::start(
        ReplicationParts {
            node,
            engine: Arc::new(engine),
            membership,
            // The nodes' own servers are plain tonic servers, so they dial each other in
            // plaintext.
            peers: Arc::new(Peers::with_network(Network::default())),
            addresses,
            authority: Arc::new(BTreeMap::<DatabaseName, EpochHolders>::new()),
            clock: Arc::new(SystemClock),
            execution: Execution::Threads,
            state_dir: root.join("replication"),
            settings: fleet_settings(),
            metrics: fleetringd::observe::ReplicationMetrics::default(),
        },
        move |sink| {
            Registry::new(registry_engine, sink, databases, config).with_recorded_changes(
                CaptureRules::every(every_database(), CaptureMode::default()),
            )
        },
    )
}
