//! Shared fixtures: registries over each engine, and the same workload in each engine's language.

use std::error::Error;
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::time::Duration;

use fleetring_control::DatabaseName;
use fleetringd::engine::{
    EngineConfig, Execution, MemoryEngine, SqliteEngine, Statement, Transaction, Value,
};
use fleetringd::{RecordingSink, Registry};
use tempfile::TempDir;

pub(crate) type TestResult = Result<(), Box<dyn Error>>;

/// The page size the tests give SQLite databases, small so that WAL files stay small.
pub(crate) const PAGE_SIZE: u32 = 512;

/// A database name.
pub(crate) fn name(text: &str) -> DatabaseName {
    text.parse().expect("valid database name")
}

/// An engine configuration for tests.
pub(crate) fn config(execution: Execution, max_open: usize, idle: Duration) -> EngineConfig {
    EngineConfig {
        execution,
        max_open: NonZeroUsize::new(max_open).expect("max_open > 0"),
        idle_timeout: idle,
        page_size: PAGE_SIZE,
        ..EngineConfig::default()
    }
}

/// Which engine a fixture runs on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Kind {
    Sqlite,
    Memory,
}

/// A registry over one engine, with its sink and data directory.
#[derive(Debug)]
pub(crate) struct Fixture {
    pub(crate) kind: Kind,
    pub(crate) registry: Registry,
    pub(crate) sink: RecordingSink,
    pub(crate) dir: TempDir,
}

impl Fixture {
    pub(crate) fn new(kind: Kind, config: EngineConfig) -> Self {
        let dir = tempfile::tempdir().expect("temp dir");
        let sink = RecordingSink::new();
        let root = dir.path().join("databases");
        let registry = match kind {
            Kind::Sqlite => Registry::new(
                SqliteEngine::new(config.page_size).expect("valid page size"),
                sink.clone(),
                root,
                config,
            ),
            Kind::Memory => Registry::new(MemoryEngine::new(), sink.clone(), root, config),
        };
        Self {
            kind,
            registry,
            sink,
            dir,
        }
    }

    pub(crate) fn path(&self, relative: &str) -> PathBuf {
        self.dir.path().join(relative)
    }

    /// Creates a database ready for [`put`](Self::put).
    pub(crate) async fn create(&self, db: &DatabaseName) -> TestResult {
        let handle = self.registry.create(db).await?;
        if self.kind == Kind::Sqlite {
            handle
                .execute(Transaction::from(Statement::new(
                    "CREATE TABLE kv (k TEXT PRIMARY KEY, v)",
                )))
                .await?;
        }
        Ok(())
    }

    /// Stores `value` under `key`.
    pub(crate) fn put(&self, key: &str, value: impl Into<Value>) -> Statement {
        let params = [Value::from(key), value.into()];
        match self.kind {
            Kind::Sqlite => {
                Statement::with_params("INSERT OR REPLACE INTO kv (k, v) VALUES (?1, ?2)", params)
            }
            Kind::Memory => Statement::with_params("put", params),
        }
    }

    /// Reads the value under `key`.
    pub(crate) fn get(&self, key: &str) -> Statement {
        match self.kind {
            Kind::Sqlite => Statement::with_params("SELECT v FROM kv WHERE k = ?1", [key.into()]),
            Kind::Memory => Statement::with_params("get", [key.into()]),
        }
    }

    /// A statement that always fails.
    pub(crate) fn failing(&self) -> Statement {
        match self.kind {
            Kind::Sqlite => Statement::new("INSERT INTO missing_table VALUES (1)"),
            Kind::Memory => Statement::new("fail"),
        }
    }

    /// A query that takes about `millis` milliseconds.
    pub(crate) fn slow_query(&self, millis: i64) -> Statement {
        match self.kind {
            // Counting with a recursive CTE runs at roughly ten million rows a second.
            Kind::Sqlite => Statement::with_params(
                "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c WHERE x < ?1)
                 SELECT count(*) FROM c",
                [Value::Integer(millis * 10_000)],
            ),
            Kind::Memory => Statement::with_params("sleep", [Value::Integer(millis)]),
        }
    }

    /// The value under `key`, if any.
    pub(crate) async fn value(
        &self,
        db: &DatabaseName,
        key: &str,
    ) -> Result<Option<Value>, Box<dyn Error>> {
        let rows = self.registry.query(db, self.get(key)).await?;
        Ok(rows
            .rows
            .into_iter()
            .next()
            .and_then(|row| row.into_iter().next()))
    }
}

/// The path of a SQLite database's WAL inside a fixture's databases root.
pub(crate) fn wal_file(root: &Path, db: &str) -> PathBuf {
    root.join("databases").join(db).join("data.db-wal")
}
