//! The open databases of one data node: opened on first use, closed when idle, and capped.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard, Weak};
use std::{io, mem};

use fleetring_control::DatabaseName;
use tokio::sync::{oneshot, watch};

use crate::actor::{self, ActorConfig, ChangeRecording, DatabaseError, DatabaseHandle, Stored};
use crate::changes::CaptureRules;
use crate::engine::{
    DatabaseEngine, EngineConfig, EngineError, Execution, OpenMode, Rows, Statement, Transaction,
};
use crate::layout;
use crate::sink::ReplicationSink;

/// Every open database on a data node, each owned by its own actor.
///
/// A database opens on first use and closes after [`EngineConfig::idle_timeout`] without
/// requests. At most [`EngineConfig::max_open`] are open at once: opening one more first closes
/// the least recently used, after it finishes the requests it already accepted. A database never
/// has two actors at once, so it never has two writers.
///
/// Databases live under `root`, one directory each. Cloning a registry is cheap and shares it.
///
/// A database that a logical change stream reads ([`with_recorded_changes`]) records its row
/// changes from the moment it opens, in the strongest mode the streams ask for (FR-039).
///
/// [`with_recorded_changes`]: Self::with_recorded_changes
#[derive(Debug, Clone)]
pub struct Registry {
    inner: Arc<Inner>,
}

#[derive(Debug)]
struct Inner {
    engine: Arc<dyn DatabaseEngine>,
    sink: Arc<dyn ReplicationSink>,
    root: PathBuf,
    config: EngineConfig,
    /// The databases that record row changes, and in which mode.
    recorded: CaptureRules,
    state: Mutex<State>,
}

#[derive(Debug, Default)]
struct State {
    entries: BTreeMap<DatabaseName, Entry>,
    /// Databases whose files are being deleted; a request for one finds no database.
    removing: BTreeSet<DatabaseName>,
    clock: u64,
    next_id: u64,
}

#[derive(Debug)]
struct Entry {
    id: u64,
    handle: DatabaseHandle,
    closed: watch::Receiver<bool>,
    status: Status,
}

#[derive(Debug)]
enum Status {
    Open {
        last_used: u64,
        close: oneshot::Sender<()>,
    },
    Closing,
}

impl Entry {
    fn begin_closing(&mut self) {
        if let Status::Open { close, .. } = mem::replace(&mut self.status, Status::Closing) {
            let _ = close.send(());
        }
    }
}

/// What a caller must do before it can use a database.
enum Next {
    Use(DatabaseHandle),
    WaitForClose(watch::Receiver<bool>),
}

impl Registry {
    /// A registry with no open databases, keeping databases under `root`.
    #[must_use]
    pub fn new(
        engine: impl DatabaseEngine,
        sink: impl ReplicationSink,
        root: impl Into<PathBuf>,
        config: EngineConfig,
    ) -> Self {
        Self {
            inner: Arc::new(Inner {
                engine: Arc::new(engine),
                sink: Arc::new(sink),
                root: root.into(),
                config,
                recorded: CaptureRules::none(),
                state: Mutex::default(),
            }),
        }
    }

    /// The same registry, with every database that `recorded` names recording its row changes
    /// from the moment it opens, in the mode the rules give it, for logical change streams
    /// (FR-039).
    ///
    /// Call it before any database opens; a registry with open databases keeps them as they
    /// are.
    ///
    /// # Panics
    ///
    /// If the registry has been cloned, so that its settings can no longer change.
    #[must_use]
    pub fn with_recorded_changes(self, recorded: CaptureRules) -> Self {
        let mut inner = Arc::into_inner(self.inner)
            .expect("a registry's recorded databases are set before it is shared");
        inner.recorded = recorded;
        Self {
            inner: Arc::new(inner),
        }
    }

    /// Creates a database and leaves it open.
    ///
    /// # Errors
    ///
    /// [`DatabaseError::Open`] with [`EngineError::AlreadyExists`] if the database exists, or
    /// another reason the engine couldn't create it.
    pub async fn create(&self, name: &DatabaseName) -> Result<DatabaseHandle, DatabaseError> {
        let handle = self.acquire(name, OpenMode::Create).await?;
        handle.ready().await?;
        Ok(handle)
    }

    /// Opens an existing database, or returns it if it is already open.
    ///
    /// # Errors
    ///
    /// [`DatabaseError::Open`] with [`EngineError::NotFound`] if the database doesn't exist, or
    /// another reason the engine couldn't open it.
    pub async fn open(&self, name: &DatabaseName) -> Result<DatabaseHandle, DatabaseError> {
        let handle = self.acquire(name, OpenMode::Existing).await?;
        handle.ready().await?;
        Ok(handle)
    }

    /// Runs `transaction` on an existing database, opening it if needed, and waits for
    /// replication to acknowledge it.
    ///
    /// # Errors
    ///
    /// As [`DatabaseHandle::execute`], and as [`open`](Self::open).
    pub async fn execute(
        &self,
        name: &DatabaseName,
        transaction: Transaction,
    ) -> Result<Stored, DatabaseError> {
        loop {
            let handle = self.acquire(name, OpenMode::Existing).await?;
            if let Some(permit) = handle.reserve().await {
                return handle.execute_reserved(permit, transaction).await;
            }
        }
    }

    /// Runs a read-only query on an existing database, opening it if needed.
    ///
    /// # Errors
    ///
    /// As [`DatabaseHandle::query`], and as [`open`](Self::open).
    pub async fn query(
        &self,
        name: &DatabaseName,
        statement: Statement,
    ) -> Result<Rows, DatabaseError> {
        loop {
            let handle = self.acquire(name, OpenMode::Existing).await?;
            if let Some(permit) = handle.reserve().await {
                return handle.query_reserved(permit, statement).await;
            }
        }
    }

    /// How many databases are open or still closing.
    ///
    /// # Panics
    ///
    /// If the registry's lock was poisoned by a panic in another thread.
    #[must_use]
    pub fn open_count(&self) -> usize {
        self.inner.lock().entries.len()
    }

    /// Whether `name` is open, and not closing.
    ///
    /// # Panics
    ///
    /// If the registry's lock was poisoned by a panic in another thread.
    #[must_use]
    pub fn is_open(&self, name: &DatabaseName) -> bool {
        self.inner.lock().entries.get(name).is_some_and(|entry| {
            matches!(entry.status, Status::Open { .. }) && !entry.handle.is_closed()
        })
    }

    /// Closes every open database, after each finishes the requests it already accepted.
    ///
    /// # Panics
    ///
    /// If the registry's lock was poisoned by a panic in another thread.
    pub async fn close_all(&self) {
        let waits: Vec<watch::Receiver<bool>> = {
            let mut state = self.inner.lock();
            state
                .entries
                .values_mut()
                .map(|entry| {
                    entry.begin_closing();
                    entry.closed.clone()
                })
                .collect()
        };
        for closed in waits {
            wait_closed(closed).await;
        }
    }

    /// Closes `name` if it is open, after it finishes the requests it already accepted, and
    /// waits until it has closed.
    ///
    /// A request that arrives later opens it again.
    ///
    /// # Panics
    ///
    /// If the registry's lock was poisoned by a panic in another thread.
    pub async fn close(&self, name: &DatabaseName) {
        let closed = self.inner.lock().entries.get_mut(name).map(|entry| {
            entry.begin_closing();
            entry.closed.clone()
        });
        if let Some(closed) = closed {
            wait_closed(closed).await;
        }
    }

    /// Closes `name` if it is open, then deletes its files, so that no copy of it remains on this
    /// node. A request that arrives meanwhile, or after, finds no database until one is created
    /// again.
    ///
    /// # Errors
    ///
    /// The I/O error if the files can't be deleted; the database stays closed.
    ///
    /// # Panics
    ///
    /// If the registry's lock was poisoned by a panic in another thread.
    pub async fn remove(&self, name: &DatabaseName) -> io::Result<()> {
        self.inner.lock().removing.insert(name.clone());
        self.close(name).await;
        let location = self.location(name);
        let removed = match self.inner.config.execution {
            Execution::Inline => remove_dir_if_present(&location),
            Execution::Threads => {
                tokio::task::spawn_blocking(move || remove_dir_if_present(&location))
                    .await
                    .map_err(io::Error::other)
                    .and_then(|removed| removed)
            }
        };
        self.inner.lock().removing.remove(name);
        removed
    }

    /// The directory that holds `name`'s storage, whether or not it is open.
    pub(crate) fn location(&self, name: &DatabaseName) -> PathBuf {
        self.inner.root.join(layout::database_dir(name))
    }

    /// The directory every database's storage lives under.
    pub(crate) fn root(&self) -> &Path {
        &self.inner.root
    }

    /// Returns a handle to the database's actor, starting one if the database isn't open.
    async fn acquire(
        &self,
        name: &DatabaseName,
        mode: OpenMode,
    ) -> Result<DatabaseHandle, DatabaseError> {
        loop {
            match self.next(name, mode)? {
                Next::Use(handle) => return Ok(handle),
                Next::WaitForClose(closed) => wait_closed(closed).await,
            }
        }
    }

    fn next(&self, name: &DatabaseName, mode: OpenMode) -> Result<Next, DatabaseError> {
        let mut state = self.inner.lock();
        if state.removing.contains(name) {
            return Err(DatabaseError::Open {
                name: name.clone(),
                source: Arc::new(EngineError::NotFound),
            });
        }
        state.clock += 1;
        let now = state.clock;
        if let Some(entry) = state.entries.get_mut(name) {
            return match &mut entry.status {
                Status::Open { last_used, .. } if !entry.handle.is_closed() => {
                    if mode == OpenMode::Create {
                        return Err(DatabaseError::Open {
                            name: name.clone(),
                            source: Arc::new(EngineError::AlreadyExists),
                        });
                    }
                    *last_used = now;
                    Ok(Next::Use(entry.handle.clone()))
                }
                _ => Ok(Next::WaitForClose(entry.closed.clone())),
            };
        }
        if state.entries.len() < self.inner.config.max_open.get() {
            let handle = self.start(&mut state, name, mode, now);
            return Ok(Next::Use(handle));
        }
        let victim = state
            .entries
            .values_mut()
            .filter_map(|entry| match entry.status {
                Status::Open { last_used, .. } => Some((last_used, entry)),
                Status::Closing => None,
            })
            .min_by_key(|(last_used, _)| *last_used)
            .map(|(_, entry)| entry);
        if let Some(victim) = victim {
            victim.begin_closing();
            return Ok(Next::WaitForClose(victim.closed.clone()));
        }
        // Every entry is already closing: wait for one of them.
        match state.entries.values().next() {
            Some(closing) => Ok(Next::WaitForClose(closing.closed.clone())),
            None => Ok(Next::Use(self.start(&mut state, name, mode, now))),
        }
    }

    fn start(
        &self,
        state: &mut State,
        name: &DatabaseName,
        mode: OpenMode,
        now: u64,
    ) -> DatabaseHandle {
        let id = state.next_id;
        state.next_id += 1;
        let inner = &self.inner;
        let config = ActorConfig {
            name: name.clone(),
            engine: Arc::clone(&inner.engine),
            sink: Arc::clone(&inner.sink),
            location: self.location(name),
            mode,
            execution: inner.config.execution,
            idle_timeout: inner.config.idle_timeout,
            mailbox: inner.config.mailbox.get(),
            thread_name: format!("fleetring-db-{id}"),
            changes: match inner.recorded.mode_for(name) {
                Some(mode) => ChangeRecording::On(mode),
                None => ChangeRecording::Off,
            },
        };
        let registry = Arc::downgrade(inner);
        let exiting = name.clone();
        let spawned = actor::spawn(config, move || forget(&registry, &exiting, id));
        let handle = spawned.handle.clone();
        state.entries.insert(
            name.clone(),
            Entry {
                id,
                handle: spawned.handle,
                closed: spawned.closed,
                status: Status::Open {
                    last_used: now,
                    close: spawned.close,
                },
            },
        );
        handle
    }
}

impl Inner {
    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().expect("registry lock poisoned")
    }
}

/// Removes a closed actor's entry, unless a newer actor already replaced it.
fn forget(registry: &Weak<Inner>, name: &DatabaseName, id: u64) {
    if let Some(inner) = registry.upgrade() {
        let mut state = inner.lock();
        if state.entries.get(name).is_some_and(|entry| entry.id == id) {
            state.entries.remove(name);
        }
    }
}

/// Deletes `dir` and everything in it, if it exists.
fn remove_dir_if_present(dir: &Path) -> io::Result<()> {
    match std::fs::remove_dir_all(dir) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}

async fn wait_closed(mut closed: watch::Receiver<bool>) {
    // An error means the actor is gone, which is as closed as it gets.
    drop(closed.wait_for(|closed| *closed).await);
}

const _: () = {
    const fn assert_debug_send_sync<T: std::fmt::Debug + Send + Sync>() {}
    assert_debug_send_sync::<DatabaseError>();
    assert_debug_send_sync::<DatabaseHandle>();
    assert_debug_send_sync::<Registry>();
};
