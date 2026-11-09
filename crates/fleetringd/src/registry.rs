//! The open databases of one data node: opened on first use, closed when idle, and capped.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard, Weak};
use std::{io, mem};

use fleetring_control::DatabaseName;
use tokio::sync::{oneshot, watch};
use tokio::time::Instant;

use crate::actor::{
    self, ActorConfig, Admission, Admit, ChangeRecording, DatabaseError, DatabaseHandle, Published,
    SessionCalled, SessionId, SessionLimits, SessionPublished, SessionTurn, StatementLimit, Stored,
};
use crate::changes::CaptureRules;
use crate::engine::{
    CaptureMode, DatabaseEngine, EngineConfig, EngineError, Execution, OpenMode, Rows, Schema,
    SchemaRequest, Statement, Transaction,
};
use crate::layout;
use crate::publisher::BatchPublisher;

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
    publisher: Arc<dyn BatchPublisher>,
    root: PathBuf,
    config: EngineConfig,
    /// The databases that record row changes, and in which mode.
    recorded: CaptureRules,
    /// How long a SQL session's transaction may hold a database's writer.
    sessions: SessionLimits,
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
        publisher: impl BatchPublisher,
        root: impl Into<PathBuf>,
        config: EngineConfig,
    ) -> Self {
        Self {
            inner: Arc::new(Inner {
                engine: Arc::new(engine),
                publisher: Arc::new(publisher),
                root: root.into(),
                config,
                recorded: CaptureRules::none(),
                sessions: SessionLimits::default(),
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

    /// The same registry, with SQL sessions held to `limits` (ADR-0037, point 14).
    ///
    /// Call it before any database opens; a registry with open databases keeps them as they
    /// are.
    ///
    /// # Panics
    ///
    /// If the registry has been cloned, so that its settings can no longer change.
    #[must_use]
    pub fn with_sessions(self, limits: SessionLimits) -> Self {
        let mut inner = Arc::into_inner(self.inner)
            .expect("a registry's session limits are set before it is shared");
        inner.sessions = limits;
        Self {
            inner: Arc::new(inner),
        }
    }

    /// The mode `name` records its row changes in on this node, or `None` if no logical change
    /// stream here reads it, so that it records nothing.
    #[must_use]
    pub(crate) fn capture_mode(&self, name: &DatabaseName) -> Option<CaptureMode> {
        self.inner.recorded.mode_for(name)
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
        self.commit(name, transaction).await?.stored().await
    }

    /// Runs `transaction` on an existing database, opening it if needed, and returns once its
    /// batch, if it committed one, is in replication's hands; [`Published::stored`] waits for
    /// the acknowledgement.
    ///
    /// # Errors
    ///
    /// As [`execute`](Self::execute), but for the acknowledgement.
    pub(crate) async fn commit(
        &self,
        name: &DatabaseName,
        transaction: Transaction,
    ) -> Result<Published, DatabaseError> {
        let admit: Admit<'_> = Box::new(|| Some(Box::new(())));
        Ok(self.commit_until(name, transaction, None, admit).await?.0)
    }

    /// As [`execute`](Self::execute), but the transaction's statements stop at `limit`: they run
    /// for `limit.budget` from when the transaction starts, and never past `limit.latest`, which
    /// also bounds how long it may wait to start, in the mailbox or behind a SQL session's
    /// transaction. Stopped, the transaction rolls back (FR-078).
    ///
    /// # Errors
    ///
    /// As [`execute`](Self::execute); a [`DatabaseError::Engine`] with
    /// [`EngineError::Interrupted`] if the limit stopped it, when nothing committed.
    pub async fn execute_until(
        &self,
        name: &DatabaseName,
        transaction: Transaction,
        limit: StatementLimit,
    ) -> Result<Stored, DatabaseError> {
        let admit: Admit<'_> = Box::new(|| Some(Box::new(())));
        self.commit_until(name, transaction, Some(limit), admit)
            .await?
            .0
            .stored()
            .await
    }

    /// As [`commit`](Self::commit), with the transaction's statements stopped at `limit` if
    /// there is one, and `admit` asked right before it starts: refused, the transaction never
    /// runs; admitted, it returns with what keeps it admitted.
    pub(crate) async fn commit_until<'a>(
        &self,
        name: &DatabaseName,
        transaction: Transaction,
        limit: Option<StatementLimit>,
        admit: Admit<'a>,
    ) -> Result<(Published, Option<Admission<'a>>), DatabaseError> {
        loop {
            let handle = self.acquire(name, OpenMode::Existing).await?;
            if let Some(permit) = handle.reserve().await {
                return handle
                    .commit_reserved(permit, transaction, limit, admit)
                    .await;
            }
        }
    }

    /// Runs one session's call, `turn`, on an existing database, opening it if needed, and waits
    /// for replication to acknowledge every transaction the call committed (ADR-0037). The
    /// call's statements stop at `limit`, if there is one, counted from when the call starts, and
    /// when this future is dropped (FR-078).
    ///
    /// # Errors
    ///
    /// [`DatabaseError::TransactionTimedOut`] or [`DatabaseError::TransactionLost`] if the session
    /// is holding a transaction the database no longer has; [`DatabaseError::Busy`] if another
    /// session's transaction holds the writer and too many writes wait for it;
    /// [`DatabaseError::NoSessions`] if the engine can't run sessions; and as
    /// [`execute`](Self::execute).
    pub async fn session_call(
        &self,
        name: &DatabaseName,
        turn: SessionTurn,
        limit: Option<StatementLimit>,
    ) -> Result<SessionCalled, DatabaseError> {
        let admit: Admit<'_> = Box::new(|| Some(Box::new(())));
        self.session_publish(name, turn, limit, admit)
            .await?
            .0
            .stored()
            .await
    }

    /// As [`session_call`](Self::session_call), but asks `admit` right before the call starts
    /// (refused, it never runs), and returns once the transactions the call committed are in
    /// replication's hands, with what keeps it admitted; [`SessionPublished::stored`] waits for
    /// their acknowledgements.
    pub(crate) async fn session_publish<'a>(
        &self,
        name: &DatabaseName,
        work: SessionTurn,
        limit: Option<StatementLimit>,
        admit: Admit<'a>,
    ) -> Result<(SessionPublished, Option<Admission<'a>>), DatabaseError> {
        loop {
            let handle = self.acquire(name, OpenMode::Existing).await?;
            if let Some(permit) = handle.reserve().await {
                return handle
                    .session_call_reserved(permit, work, limit, admit)
                    .await;
            }
        }
    }

    /// How SQL sessions are held to their limits here.
    pub(crate) fn session_limits(&self) -> SessionLimits {
        self.inner.sessions
    }

    /// Ends SQL session `session` on `name`: rolls back the transaction it holds there, if any.
    /// A database that isn't open holds none, and isn't opened for it.
    pub async fn session_end(&self, name: &DatabaseName, session: SessionId) {
        let handle = {
            let state = self.inner.lock();
            state
                .entries
                .get(name)
                .filter(|entry| matches!(entry.status, Status::Open { .. }))
                .map(|entry| entry.handle.clone())
        };
        if let Some(handle) = handle
            && let Err(error) = handle.session_end(session).await
        {
            tracing::debug!(database = %name, %session, %error, "ending a session found the database closed");
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
        self.query_within(name, statement, None).await
    }

    /// As [`query`](Self::query), but the query stops at `deadline`, waiting in the database's
    /// mailbox included (FR-078).
    ///
    /// # Errors
    ///
    /// As [`query`](Self::query); a [`DatabaseError::Engine`] with
    /// [`EngineError::Interrupted`] if the deadline stopped it.
    pub async fn query_until(
        &self,
        name: &DatabaseName,
        statement: Statement,
        deadline: Instant,
    ) -> Result<Rows, DatabaseError> {
        self.query_within(name, statement, Some(deadline)).await
    }

    async fn query_within(
        &self,
        name: &DatabaseName,
        statement: Statement,
        deadline: Option<Instant>,
    ) -> Result<Rows, DatabaseError> {
        loop {
            let handle = self.acquire(name, OpenMode::Existing).await?;
            if let Some(permit) = handle.reserve().await {
                return handle.query_reserved(permit, statement, deadline).await;
            }
        }
    }

    /// The schema of an existing database, opening it if needed.
    ///
    /// # Errors
    ///
    /// As [`DatabaseHandle::schema`], and as [`open`](Self::open).
    pub async fn schema(
        &self,
        name: &DatabaseName,
        request: SchemaRequest,
    ) -> Result<Schema, DatabaseError> {
        self.schema_within(name, request, None).await
    }

    /// As [`schema`](Self::schema), but the read stops at `deadline` (FR-078).
    ///
    /// # Errors
    ///
    /// As [`schema`](Self::schema); a [`DatabaseError::Engine`] with
    /// [`EngineError::Interrupted`] if the deadline stopped it.
    pub async fn schema_until(
        &self,
        name: &DatabaseName,
        request: SchemaRequest,
        deadline: Instant,
    ) -> Result<Schema, DatabaseError> {
        self.schema_within(name, request, Some(deadline)).await
    }

    async fn schema_within(
        &self,
        name: &DatabaseName,
        request: SchemaRequest,
        deadline: Option<Instant>,
    ) -> Result<Schema, DatabaseError> {
        loop {
            let handle = self.acquire(name, OpenMode::Existing).await?;
            if let Some(permit) = handle.reserve().await {
                return handle.schema_reserved(permit, request, deadline).await;
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
            publisher: Arc::clone(&inner.publisher),
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
            sessions: inner.sessions,
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
