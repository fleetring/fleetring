//! A small cluster over real TCP: a control daemon, and data nodes that each run a registry and
//! replication behind their own `Replication` server.
//!
//! The data daemon wires replication the same way (see `tests/database.rs` for writes through
//! it). These nodes build it by hand so that a test can wrap a node's `Replication` service, to
//! hold back or cut its acknowledgements, and drive the registry and streams directly.

use std::collections::BTreeMap;
use std::error::Error;
use std::future::Future;
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use fleetring::{NodeId, ReplicationFactor};
use fleetring_control::{DatabaseName, LeaseEpoch, Liveness, Member};
use fleetring_proto::v1::replication_server::Replication as ReplicationRpc;
use fleetringd::clock::SystemClock;
use fleetringd::engine::{
    DatabaseEngine, EngineConfig, Execution, SqliteEngine, Statement, Transaction, Value,
};
use fleetringd::replication::{PrimaryRole, Replication, ReplicationParts, ReplicationSettings};
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

pub(crate) type TestResult = Result<(), Box<dyn Error>>;

/// Guards against a hang only; nothing here asserts how long anything takes.
pub(crate) const HANG_GUARD: Duration = Duration::from_secs(60);

/// The page size the tests give SQLite databases, small so that WAL files stay small.
pub(crate) const PAGE_SIZE: u32 = 512;

pub(crate) fn name(text: &str) -> DatabaseName {
    text.parse().expect("valid database name")
}

pub(crate) fn id(text: &str) -> NodeId {
    text.parse().expect("valid node id")
}

pub(crate) fn role(replicas: u8) -> PrimaryRole {
    PrimaryRole {
        replication_factor: ReplicationFactor::new(replicas).expect("nonzero"),
        epoch: LeaseEpoch::new(1),
    }
}

/// Settings that fail fast, so tests that break links don't wait long.
pub(crate) fn settings() -> ReplicationSettings {
    ReplicationSettings {
        stall_timeout: Duration::from_millis(500),
        retry_interval: Duration::from_millis(50),
        ack_timeout: Duration::from_secs(10),
        ..ReplicationSettings::new()
    }
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

/// Wraps a node's `Replication` service before it is served, as a test sees fit.
pub(crate) type Wrap = Box<dyn FnOnce(Replication) -> Router + Send>;

/// The router a node serves.
pub(crate) type Router = tonic::service::Routes;

/// Serves `service` as the node's `Replication` service.
pub(crate) fn routes<S: ReplicationRpc>(service: S) -> Router {
    tonic::service::Routes::new(replication_server(service))
}

/// One data node.
pub(crate) struct Node {
    pub(crate) id: NodeId,
    pub(crate) replication: Replication,
    pub(crate) root: PathBuf,
    membership: Membership,
    _server: (oneshot::Sender<()>, JoinHandle<()>),
    _membership: MembershipTasks,
}

impl Node {
    pub(crate) fn registry(&self) -> &Registry {
        self.replication.registry()
    }

    /// Where the node keeps `database`.
    pub(crate) fn database_dir(&self, database: &str) -> PathBuf {
        self.root.join("databases").join(database)
    }
}

/// A control daemon and data nodes.
pub(crate) struct Cluster {
    pub(crate) nodes: Vec<Node>,
    pub(crate) addresses: Arc<BTreeMap<NodeId, String>>,
    _control: Daemon,
    _dir: TempDir,
}

impl Cluster {
    /// Starts `ids.len()` data nodes running `engine`, each wrapped by `wraps` if it names it.
    pub(crate) async fn start(
        ids: &[&str],
        engine: impl DatabaseEngine + Clone,
        settings: ReplicationSettings,
        mut wraps: BTreeMap<String, Wrap>,
    ) -> Result<Self, Box<dyn Error>> {
        let _ = tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .with_test_writer()
            .try_init();
        let dir = tempfile::tempdir()?;
        let control = Daemon::start(Config::from_toml(&format!(
            "data_dir = {:?}\nlisten = \"127.0.0.1:0\"\nrole = \"control\"\n\n[node]\nid = \"control\"\n",
            dir.path().join("control")
        ))?)
        .await?;
        let control_address = control.control_address().ok_or("no control address")?;

        let mut listeners = Vec::new();
        let mut addresses = BTreeMap::new();
        for name in ids {
            let listener = TcpListener::bind("127.0.0.1:0").await?;
            addresses.insert(id(name), listener.local_addr()?.to_string());
            listeners.push(listener);
        }
        let addresses = Arc::new(addresses);
        let membership_settings = MembershipSettings {
            heartbeat_interval_ms: std::num::NonZeroU64::new(100).ok_or("nonzero")?,
        };
        let network = Network::default();
        let mut nodes = Vec::new();
        for (name, listener) in ids.iter().zip(listeners) {
            let root = dir.path().join(name);
            let (membership, tasks) = Membership::start(
                Member::new(id(name)),
                network.control_client(control_address.to_string()),
                &membership_settings,
            );
            let replication = start_node(
                id(name),
                &root,
                engine.clone(),
                membership.clone(),
                Arc::clone(&addresses),
                settings.clone(),
            );
            let router = match wraps.remove(*name) {
                Some(wrap) => wrap(replication.clone()),
                None => routes(replication.service()),
            };
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
            nodes.push(Node {
                id: id(name),
                replication,
                root,
                membership,
                _server: (stop, server),
                _membership: tasks,
            });
        }
        let cluster = Self {
            nodes,
            addresses,
            _control: control,
            _dir: dir,
        };
        cluster.await_members(ids.len()).await?;
        Ok(cluster)
    }

    /// Waits until every node sees `count` live members.
    async fn await_members(&self, count: usize) -> TestResult {
        for node in &self.nodes {
            let membership = &node.membership;
            guarded(
                "membership converging",
                membership.wait_for(|view| {
                    view.members().len() == count
                        && view
                            .members()
                            .all(|(_, liveness)| liveness == Liveness::Live)
                }),
            )
            .await?
            .ok_or("membership stopped")?;
        }
        Ok(())
    }

    pub(crate) fn node(&self, name: &str) -> &Node {
        self.nodes
            .iter()
            .find(|node| node.id.as_str() == name)
            .expect("a node with that name")
    }

    pub(crate) fn address(&self, name: &str) -> String {
        self.addresses
            .get(&id(name))
            .cloned()
            .expect("a node with that name")
    }

    /// Creates `database` on `primary` with a table, serves it there with `replicas` replicas,
    /// and waits until every follower holds the table.
    pub(crate) async fn create(
        &self,
        primary: &str,
        database: &DatabaseName,
        replicas: u8,
    ) -> TestResult {
        let node = self.node(primary);
        node.registry().create(database).await?;
        node.replication
            .serve_primary(database.clone(), role(replicas))?;
        let created = Transaction::from(Statement::new(
            "CREATE TABLE kv (k INTEGER PRIMARY KEY, v BLOB)",
        ));
        // The first write waits until the streams are open.
        guarded("the first write", async {
            loop {
                match node.registry().execute(database, created.clone()).await {
                    Ok(_) => return Ok::<_, Box<dyn Error>>(()),
                    Err(error) if error.to_string().contains("table kv already exists") => {
                        return Ok(());
                    }
                    Err(_) => tokio::time::sleep(Duration::from_millis(20)).await,
                }
            }
        })
        .await??;
        self.await_caught_up(primary, database).await
    }

    /// Waits until every follower has acknowledged everything the primary committed.
    pub(crate) async fn await_caught_up(
        &self,
        primary: &str,
        database: &DatabaseName,
    ) -> TestResult {
        let replication = &self.node(primary).replication;
        let caught_up = eventually("followers catching up", || {
            replication.primary_status(database).is_some_and(|status| {
                status.committed.is_some()
                    && status
                        .followers
                        .iter()
                        .all(|follower| follower.acknowledged == status.committed)
            })
        })
        .await;
        caught_up
            .map_err(|error| format!("{error}: {:?}", replication.primary_status(database)).into())
    }
}

/// Builds one node's registry and replication.
pub(crate) fn start_node(
    node: NodeId,
    root: &Path,
    engine: impl DatabaseEngine + Clone,
    membership: Membership,
    addresses: Arc<BTreeMap<NodeId, String>>,
    settings: ReplicationSettings,
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
            peers: Arc::new(Peers::with_network(Network::default())),
            addresses,
            clock: Arc::new(SystemClock),
            execution: Execution::Threads,
            state_dir: root.join("replication"),
            settings,
        },
        move |sink| Registry::new(registry_engine, sink, databases, config),
    )
}

/// The SQLite engine the tests run.
pub(crate) fn sqlite() -> SqliteEngine {
    SqliteEngine::new(PAGE_SIZE).expect("valid page size")
}

/// Inserts or replaces row `k` with `len` bytes.
pub(crate) fn put(k: i64, len: usize) -> Transaction {
    let byte = u8::try_from(k.rem_euclid(251)).expect("small");
    Transaction::from(Statement::with_params(
        "INSERT OR REPLACE INTO kv (k, v) VALUES (?1, ?2)",
        [Value::Integer(k), Value::Blob(vec![byte; len])],
    ))
}
