//! What the archive tests share: a data node built by hand, with replication, change streams,
//! and an archive, serving its databases with one replica each, so that the primary alone is
//! the write quorum.

use std::collections::BTreeMap;
use std::error::Error;
use std::future::Future;
use std::num::NonZeroUsize;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use fleetring::{NodeId, ReplicationFactor};
use fleetring_control::{DatabaseName, LeaseEpoch};
use fleetringd::changes::{ChangeParts, ChangeSettings, ChangeStreams, DatabaseMatchers};
use fleetringd::clock::SystemClock;
use fleetringd::durability::{Archive, ArchiveParts, ArchiveSettings, Listing, ObjectStore};
use fleetringd::engine::{
    DatabaseEngine, EngineConfig, Execution, SqliteEngine, Statement, Transaction,
};
use fleetringd::replication::{
    EpochHolders, PrimaryRole, Replication, ReplicationParts, ReplicationSettings,
};
use fleetringd::transport::Peers;
use fleetringd::{ManualMembership, Membership, Network, Registry};

pub(crate) type TestResult = Result<(), Box<dyn Error>>;

/// Guards against a hang only; nothing here asserts how long anything takes.
pub(crate) const HANG_GUARD: Duration = Duration::from_secs(60);

/// The page size the tests give SQLite databases, small so that WAL files stay small.
pub(crate) const PAGE_SIZE: u32 = 512;

/// The archive's name in every test.
pub(crate) const ARCHIVE: &str = "backups";

/// The archive's key prefix in every test.
pub(crate) const PREFIX: &str = "fleetring";

pub(crate) fn name(text: &str) -> DatabaseName {
    text.parse().expect("valid database name")
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

/// The SQLite engine the tests run.
pub(crate) fn sqlite() -> SqliteEngine {
    SqliteEngine::new(PAGE_SIZE).expect("valid page size")
}

/// Settings that segment every 20 ms and never snapshot on their own.
pub(crate) fn settings() -> ArchiveSettings {
    let mut settings = ArchiveSettings::new().with_chunk_bytes(4096);
    settings.segment_interval = Duration::from_millis(20);
    settings
}

/// Inserts `value` into `t`.
pub(crate) fn insert(value: i64) -> Transaction {
    Transaction::from(Statement::with_params(
        "INSERT INTO t (value) VALUES (?1)",
        [value.into()],
    ))
}

/// One data node with an archive.
pub(crate) struct Node {
    pub(crate) replication: Replication,
    pub(crate) streams: ChangeStreams,
    pub(crate) archive: Archive,
    _membership: ManualMembership,
}

impl Node {
    /// Starts a node over `root`, archiving every database to `store` by `settings`.
    pub(crate) fn start(
        root: &Path,
        store: Arc<dyn ObjectStore>,
        settings: ArchiveSettings,
    ) -> Self {
        let _ = tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .with_test_writer()
            .try_init();
        let (membership, manual) = Membership::manual();
        let replication = start_replication(root, sqlite(), membership);
        let archive = Archive::with_settings(
            ARCHIVE,
            PREFIX,
            every_database(),
            settings,
            store,
            ArchiveParts {
                replication: replication.clone(),
                engine: Arc::new(sqlite()),
                clock: Arc::new(SystemClock),
                execution: Execution::Threads,
            },
        )
        .expect("sqlite is a frame source");
        let streams = ChangeStreams::start(ChangeParts {
            sinks: vec![archive.sink()],
            replication: replication.clone(),
            state_dir: root.join("changes"),
            settings: ChangeSettings {
                retry_initial: Duration::from_millis(10),
                retry_max: Duration::from_millis(100),
            },
        });
        Self {
            replication,
            streams,
            archive,
            _membership: manual,
        }
    }

    pub(crate) fn registry(&self) -> &Registry {
        self.replication.registry()
    }

    /// Creates `database` with one replica, serves it as primary at epoch 1, and runs `schema`.
    pub(crate) async fn create(&self, database: &DatabaseName, schema: &str) -> TestResult {
        guarded("creating the database", self.registry().create(database)).await??;
        self.serve(database, 1)?;
        self.execute(database, Transaction::from(Statement::new(schema)))
            .await
    }

    /// Serves `database` as primary at `epoch`.
    pub(crate) fn serve(&self, database: &DatabaseName, epoch: u64) -> TestResult {
        self.replication.serve_primary(
            database.clone(),
            PrimaryRole::new(ReplicationFactor::ONE, LeaseEpoch::new(epoch)),
        )?;
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

    /// Waits until the archive has everything acknowledged.
    pub(crate) async fn await_archived(&self) -> TestResult {
        if guarded(
            "the archive catching up",
            self.streams.await_caught_up(HANG_GUARD),
        )
        .await?
        {
            Ok(())
        } else {
            Err("the archive didn't catch up".into())
        }
    }

    /// The database's position now.
    pub(crate) async fn position(
        &self,
        database: &DatabaseName,
    ) -> Result<Option<fleetringd::engine::Position>, Box<dyn Error>> {
        Ok(self.registry().open(database).await?.position().await?)
    }

    /// What the archive holds of `database`.
    pub(crate) async fn listing(&self, database: &DatabaseName) -> Result<Listing, Box<dyn Error>> {
        Ok(Listing::read(
            self.archive.store().as_ref(),
            &self.archive.keys(database),
            database,
        )
        .await?)
    }

    /// Stops the streams and the node.
    pub(crate) async fn stop(self) {
        self.streams.stop(Duration::ZERO).await;
        self.replication.stop();
        self.registry().close_all().await;
    }
}

/// Every database.
pub(crate) fn every_database() -> DatabaseMatchers {
    DatabaseMatchers::new(["*".parse().expect("a star")])
}

/// Starts replication over `root` with `engine`.
pub(crate) fn start_replication(
    root: &Path,
    engine: impl DatabaseEngine + Clone,
    membership: Membership,
) -> Replication {
    let config = EngineConfig {
        execution: Execution::Threads,
        max_open: NonZeroUsize::new(64).expect("nonzero"),
        page_size: PAGE_SIZE,
        ..EngineConfig::default()
    };
    let databases = root.join("databases");
    let registry_engine = engine.clone();
    let node: NodeId = "node".parse().expect("a node id");
    Replication::start(
        ReplicationParts {
            node,
            engine: Arc::new(engine),
            membership,
            peers: Arc::new(Peers::with_network(Network::default())),
            addresses: Arc::new(BTreeMap::<NodeId, String>::new()),
            authority: Arc::new(BTreeMap::<DatabaseName, EpochHolders>::new()),
            clock: Arc::new(SystemClock),
            execution: Execution::Threads,
            state_dir: root.join("replication"),
            settings: ReplicationSettings {
                retry_interval: Duration::from_millis(50),
                ..ReplicationSettings::new()
            },
        },
        move |sink| Registry::new(registry_engine, sink, databases, config),
    )
}
