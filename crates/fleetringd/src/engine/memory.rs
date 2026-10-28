//! An in-memory, self-replicating engine for tests and simulations.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard};
use std::thread;
use std::time::Duration;

use fleetring::NodeId;

use crate::engine::{
    Database, DatabaseCapability, DatabaseEngine, EngineCapability, EngineError, EngineKind,
    Executed, OpenMode, Position, ReplicaPosition, Rows, SelfReplicating, Snapshot, Statement,
    Transaction, Value,
};

/// An in-memory key-value engine that replicates itself.
///
/// It exists to prove that nothing above the [`DatabaseEngine`] trait assumes SQLite or its WAL.
/// Databases live in memory, keyed by location, and survive closing and reopening for as long as
/// any clone of the engine lives. Every replica named at construction holds every committed
/// transaction as soon as it commits.
///
/// Statements are commands, with their arguments as parameters:
///
/// - `put` with a text key and any value stores the value;
/// - `delete` with a text key removes it;
/// - `get` with a text key returns a `value` column, with one row if the key exists;
/// - `scan` returns every `key` and `value` in key order;
/// - `sleep` with an integer number of milliseconds blocks the calling thread;
/// - `fail` always fails.
///
/// Positions are the count of committed write transactions, as 8 big-endian bytes.
///
/// # Examples
///
/// ```
/// use fleetringd::engine::{
///     DatabaseEngine, MemoryEngine, OpenMode, Statement, Transaction, Value,
/// };
///
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let engine = MemoryEngine::new();
/// let mut db = engine.open("tenant".as_ref(), OpenMode::Create)?;
/// let put = Statement::with_params("put", [Value::from("k"), Value::from(7)]);
/// db.execute(&Transaction::from(put))?;
/// let rows = db.query(&Statement::with_params("get", [Value::from("k")]))?;
/// assert_eq!(rows.rows, vec![vec![Value::Integer(7)]]);
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone, Default)]
pub struct MemoryEngine {
    store: Arc<Mutex<BTreeMap<PathBuf, Stored>>>,
    replicas: Arc<[NodeId]>,
}

impl MemoryEngine {
    /// An engine with no replicas.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// An engine whose databases replicate to `replicas`.
    #[must_use]
    pub fn with_replicas(replicas: impl IntoIterator<Item = NodeId>) -> Self {
        Self {
            store: Arc::default(),
            replicas: replicas.into_iter().collect(),
        }
    }
}

impl DatabaseEngine for MemoryEngine {
    fn kind(&self) -> EngineKind {
        EngineKind::Memory
    }

    fn capability(&self) -> EngineCapability<'_> {
        EngineCapability::SelfReplicating
    }

    fn open(&self, location: &Path, mode: OpenMode) -> Result<Box<dyn Database>, EngineError> {
        let mut store = lock(&self.store);
        match (mode, store.contains_key(location)) {
            (OpenMode::Existing, false) => return Err(EngineError::NotFound),
            (OpenMode::Create, true) => return Err(EngineError::AlreadyExists),
            (OpenMode::Create, false) => {
                store.insert(location.to_owned(), Stored::default());
            }
            (OpenMode::Existing, true) => {}
        }
        Ok(Box::new(MemoryDatabase {
            location: location.to_owned(),
            store: Arc::clone(&self.store),
            replicas: Arc::clone(&self.replicas),
        }))
    }
}

/// One database's committed state.
#[derive(Debug, Clone, Default)]
struct Stored {
    rows: BTreeMap<String, Value>,
    version: u64,
}

impl Stored {
    fn position(&self) -> Option<Position> {
        (self.version > 0).then(|| Position::new(self.version.to_be_bytes()))
    }
}

#[derive(Debug)]
struct MemoryDatabase {
    location: PathBuf,
    store: Arc<Mutex<BTreeMap<PathBuf, Stored>>>,
    replicas: Arc<[NodeId]>,
}

impl MemoryDatabase {
    fn stored(&self) -> Result<Stored, EngineError> {
        lock(&self.store)
            .get(&self.location)
            .cloned()
            .ok_or(EngineError::NotFound)
    }
}

impl Database for MemoryDatabase {
    fn execute(&mut self, transaction: &Transaction) -> Result<Executed, EngineError> {
        let mut working = self.stored()?;
        let mut rows_affected = 0;
        for statement in transaction.statements() {
            rows_affected += apply(&mut working.rows, statement)?;
        }
        if rows_affected > 0 {
            working.version += 1;
            lock(&self.store).insert(self.location.clone(), working);
        }
        Ok(Executed { rows_affected })
    }

    fn query(&mut self, statement: &Statement) -> Result<Rows, EngineError> {
        let stored = self.stored()?;
        read(&stored.rows, statement)
    }

    fn snapshot(&mut self, destination: &Path) -> Result<Snapshot, EngineError> {
        let mut store = lock(&self.store);
        let stored = store
            .get(&self.location)
            .cloned()
            .ok_or(EngineError::NotFound)?;
        let position = stored.position();
        store.insert(destination.to_owned(), stored);
        Ok(Snapshot { position })
    }

    fn capability(&mut self) -> DatabaseCapability<'_> {
        DatabaseCapability::SelfReplicating(self)
    }

    fn close(self: Box<Self>) -> Result<(), EngineError> {
        Ok(())
    }
}

impl SelfReplicating for MemoryDatabase {
    fn position(&mut self) -> Result<Option<Position>, EngineError> {
        Ok(self.stored()?.position())
    }

    fn replica_positions(&mut self) -> Result<Vec<ReplicaPosition>, EngineError> {
        let position = self.stored()?.position();
        Ok(self
            .replicas
            .iter()
            .map(|node| ReplicaPosition {
                node: node.clone(),
                position: position.clone(),
            })
            .collect())
    }
}

/// Runs one command against `rows`, and returns how many rows it changed.
fn apply(rows: &mut BTreeMap<String, Value>, statement: &Statement) -> Result<u64, EngineError> {
    match (statement.sql(), statement.params()) {
        ("put", [Value::Text(key), value]) => {
            rows.insert(key.clone(), value.clone());
            Ok(1)
        }
        ("delete", [Value::Text(key)]) => Ok(u64::from(rows.remove(key).is_some())),
        _ => read(rows, statement).map(|_| 0),
    }
}

/// Runs one read-only command against `rows`.
fn read(rows: &BTreeMap<String, Value>, statement: &Statement) -> Result<Rows, EngineError> {
    match (statement.sql(), statement.params()) {
        ("get", [Value::Text(key)]) => Ok(Rows {
            columns: vec!["value".to_owned()],
            rows: rows
                .get(key)
                .map(|value| vec![value.clone()])
                .into_iter()
                .collect(),
        }),
        ("scan", []) => Ok(Rows {
            columns: vec!["key".to_owned(), "value".to_owned()],
            rows: rows
                .iter()
                .map(|(key, value)| vec![Value::Text(key.clone()), value.clone()])
                .collect(),
        }),
        ("sleep", [Value::Integer(millis)]) => {
            thread::sleep(Duration::from_millis(u64::try_from(*millis).unwrap_or(0)));
            Ok(Rows::default())
        }
        ("put" | "delete", _) => Err(failed(format!("`{}` writes", statement.sql()))),
        ("fail", _) => Err(failed("`fail` always fails".to_owned())),
        (command, params) => Err(failed(format!(
            "unknown command `{command}` with {} parameters",
            params.len()
        ))),
    }
}

fn failed(message: String) -> EngineError {
    EngineError::Statement { message }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().expect("memory engine lock poisoned")
}
