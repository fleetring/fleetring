//! One data node built by hand, so a test can give it any sink: a control daemon, and a
//! registry with replication and change streams around it. Databases have one replica, so the
//! primary alone is the write quorum and a write is acknowledged as soon as it commits.

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
    CaptureMode, EngineConfig, Execution, SqliteEngine, Statement, Transaction,
};
use fleetringd::replication::{PrimaryRole, Replication, ReplicationParts, ReplicationSettings};
use fleetringd::transport::Peers;
use fleetringd::{
    Config, Daemon, Membership, MembershipSettings, MembershipTasks, Network, Registry,
};
use tempfile::TempDir;

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
}

impl Harness {
    pub(crate) async fn start() -> Result<Self, Box<dyn Error>> {
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
        Ok(Self { control, dir })
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
        let control_address = self.control.control_address().ok_or("no control address")?;
        let id: NodeId = "node".parse()?;
        let network = Network::default();
        let (membership, tasks) = Membership::start(
            Member::new(id.clone()),
            network.control_client(control_address.to_string()),
            &MembershipSettings {
                heartbeat_interval_ms: std::num::NonZeroU64::new(100).ok_or("nonzero")?,
                ..MembershipSettings::default()
            },
        );
        let root = self.root();
        let replication = start_replication(id, &root, membership.clone(), mode);
        let streams = ChangeStreams::start(ChangeParts {
            sinks,
            replication: replication.clone(),
            state_dir: root.join("changes"),
            settings: ChangeSettings {
                retry_initial: Duration::from_millis(10),
                retry_max: Duration::from_millis(100),
            },
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
    membership: Membership,
    mode: CaptureMode,
) -> Replication {
    let config = EngineConfig {
        execution: Execution::Threads,
        max_open: NonZeroUsize::new(64).expect("nonzero"),
        page_size: PAGE_SIZE,
        ..EngineConfig::default()
    };
    let databases = root.join("databases");
    let engine = SqliteEngine::new(PAGE_SIZE).expect("valid page size");
    let registry_engine = engine.clone();
    Replication::start(
        ReplicationParts {
            node,
            engine: Arc::new(engine),
            membership,
            peers: Arc::new(Peers::with_network(Network::default())),
            addresses: Arc::new(BTreeMap::<NodeId, String>::new()),
            clock: Arc::new(SystemClock),
            execution: Execution::Threads,
            state_dir: root.join("replication"),
            settings: ReplicationSettings {
                retry_interval: Duration::from_millis(50),
                ..ReplicationSettings::new()
            },
        },
        move |sink| {
            Registry::new(registry_engine, sink, databases, config)
                .with_recorded_changes(CaptureRules::every(every_database(), mode))
        },
    )
}

/// Inserts `value` into `t`.
pub(crate) fn insert(value: i64) -> Transaction {
    Transaction::from(Statement::with_params(
        "INSERT INTO t (value) VALUES (?1)",
        [value.into()],
    ))
}
