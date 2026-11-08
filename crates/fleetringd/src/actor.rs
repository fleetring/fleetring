//! The per-database actor: one task that owns an open database (ADR-0013, point 6).
//!
//! The actor serializes writes, sends reads to the engine's read path, and turns each committed
//! write transaction of a frame-source engine into exactly one batch for the
//! [`BatchPublisher`]. It closes the database when it has been idle for the configured time, or
//! when the registry evicts it. A database that logical change streams read also records its row
//! changes from the moment it opens (FR-039).
//!
//! A write, a query, or a schema read carries an [`Interrupt`] that stops its statements on the
//! database's thread (FR-078). The caller's side of the handle raises it: at the request's
//! deadline, from a `tokio::time` timer, and when the caller stops waiting for the reply, so a
//! cancelled call frees the database too. The thread only checks the flag, so time never leaves
//! `tokio::time` and a simulation stays deterministic. Under [`Execution::Inline`] a statement
//! blocks the task that would raise it, so there only a request whose deadline passed, or whose
//! caller left, while it waited in the mailbox is stopped, before it starts.

mod executor;

use std::collections::BTreeMap;
use std::fmt;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use fleetring_control::DatabaseName;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::time::{self, Instant};

use self::executor::{Executor, OpenRequest};
use crate::changes::MarkHome;
use crate::engine::{
    CaptureMode, CheckpointMarker, Database, DatabaseCapability, DatabaseEngine, DatabaseSnapshot,
    EngineError, Executed, Execution, FrameBatch, FrameSource, Interrupt, Interruption,
    LogicalChanges, OpenMode, PinnedSnapshot, Position, ReadLimit, RecordedTransaction,
    ReplicaPosition, RowChange, Rows, Schema, SchemaRequest, Shape, Statement, Transaction,
};
use crate::publisher::{
    Acknowledged, Acknowledgement, BatchPublisher, CommittedBatch, ReplicationError,
};

type Reply<T> = oneshot::Sender<Result<T, DatabaseError>>;

/// A request in an actor's mailbox.
pub(crate) enum Request {
    Ready(Reply<()>),
    Execute(
        Transaction,
        Interrupt,
        Reply<(Executed, Option<Acknowledgement>)>,
    ),
    Query(Statement, Interrupt, Reply<Rows>),
    Schema(SchemaRequest, Interrupt, Reply<Schema>),
    Position(Reply<Option<Position>>),
    CommittedSince(Option<Position>, Reply<Option<FrameBatch>>),
    CommittedBetween(Option<Position>, Position, Reply<Option<FrameBatch>>),
    RowChanges(Option<Position>, Position, Reply<Vec<RowChange>>),
    Transactions(
        Option<Position>,
        Position,
        ReadLimit,
        Reply<Vec<RecordedTransaction>>,
    ),
    ReleaseChanges(Position, Reply<()>),
    SinkMarks(Reply<Option<BTreeMap<String, Position>>>),
    StageSinkMark(String, Position, Reply<MarkHome>),
    Checkpoint(Reply<CheckpointMarker>),
    Snapshot(PathBuf, Reply<DatabaseSnapshot>),
    PinSnapshot(Reply<PinnedSnapshot>),
    PinSnapshotAt(Option<Position>, Reply<PinnedSnapshot>),
    ReplicaPositions(Reply<Vec<ReplicaPosition>>),
}

impl Request {
    /// Answers the request with `error` without running it.
    fn fail(self, error: DatabaseError) {
        match self {
            Self::Ready(reply) => {
                let _ = reply.send(Err(error));
            }
            Self::Execute(_, _, reply) => {
                let _ = reply.send(Err(error));
            }
            Self::Query(_, _, reply) => {
                let _ = reply.send(Err(error));
            }
            Self::Schema(_, _, reply) => {
                let _ = reply.send(Err(error));
            }
            Self::Position(reply) => {
                let _ = reply.send(Err(error));
            }
            Self::CommittedSince(_, reply) => {
                let _ = reply.send(Err(error));
            }
            Self::CommittedBetween(_, _, reply) => {
                let _ = reply.send(Err(error));
            }
            Self::Transactions(_, _, _, reply) => {
                let _ = reply.send(Err(error));
            }
            Self::RowChanges(_, _, reply) => {
                let _ = reply.send(Err(error));
            }
            Self::ReleaseChanges(_, reply) => {
                let _ = reply.send(Err(error));
            }
            Self::SinkMarks(reply) => {
                let _ = reply.send(Err(error));
            }
            Self::StageSinkMark(_, _, reply) => {
                let _ = reply.send(Err(error));
            }
            Self::Checkpoint(reply) => {
                let _ = reply.send(Err(error));
            }
            Self::Snapshot(_, reply) => {
                let _ = reply.send(Err(error));
            }
            Self::PinSnapshot(reply) => {
                let _ = reply.send(Err(error));
            }
            Self::PinSnapshotAt(_, reply) => {
                let _ = reply.send(Err(error));
            }
            Self::ReplicaPositions(reply) => {
                let _ = reply.send(Err(error));
            }
        }
    }
}

impl fmt::Debug for Request {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Ready(_) => "Ready",
            Self::Execute(..) => "Execute",
            Self::Query(..) => "Query",
            Self::Schema(..) => "Schema",
            Self::Position(_) => "Position",
            Self::CommittedSince(..) => "CommittedSince",
            Self::CommittedBetween(..) => "CommittedBetween",
            Self::RowChanges(..) => "RowChanges",
            Self::Transactions(..) => "Transactions",
            Self::ReleaseChanges(..) => "ReleaseChanges",
            Self::SinkMarks(_) => "SinkMarks",
            Self::StageSinkMark(..) => "StageSinkMark",
            Self::Checkpoint(_) => "Checkpoint",
            Self::Snapshot(..) => "Snapshot",
            Self::PinSnapshot(_) => "PinSnapshot",
            Self::PinSnapshotAt(..) => "PinSnapshotAt",
            Self::ReplicaPositions(_) => "ReplicaPositions",
        })
    }
}

/// The error returned by a request to an open database.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum DatabaseError {
    /// The database closed before it accepted the request, which never ran. Open it again.
    #[error("database {name} is closed")]
    Closed {
        /// The database.
        name: DatabaseName,
    },
    /// The database stopped after accepting the request, so it may or may not have run.
    #[error("database {name} stopped before replying")]
    Interrupted {
        /// The database.
        name: DatabaseName,
    },
    /// The database couldn't be opened.
    #[error("database {name} could not be opened")]
    Open {
        /// The database.
        name: DatabaseName,
        /// Why.
        source: Arc<EngineError>,
    },
    /// The database refuses writes because it can no longer read its own log. The write that
    /// found out may have committed.
    #[error("database {name} can no longer capture commits")]
    Failed {
        /// The database.
        name: DatabaseName,
        /// Why.
        source: Arc<EngineError>,
    },
    /// The operation needs an engine of another shape.
    #[error("{operation} needs a {needed} engine")]
    Unsupported {
        /// The operation.
        operation: &'static str,
        /// The shape it needs.
        needed: Shape,
    },
    /// The engine refused or failed the request.
    #[error(transparent)]
    Engine(#[from] EngineError),
    /// The write committed locally, but replication couldn't store it.
    #[error(transparent)]
    Replication(#[from] ReplicationError),
    /// Replication refused the write before it ran, so nothing committed (ADR-0023). Retry it
    /// once the reason has passed.
    #[error("database {name} refused the write: {source}")]
    Refused {
        /// The database.
        name: DatabaseName,
        /// Why.
        source: ReplicationError,
    },
}

impl DatabaseError {
    /// The engine error behind this error, if there is one.
    #[must_use]
    pub fn engine_error(&self) -> Option<&EngineError> {
        match self {
            Self::Open { source, .. } | Self::Failed { source, .. } => Some(source),
            Self::Engine(source) => Some(source),
            _ => None,
        }
    }
}

/// A write transaction that committed, and how durably replication stored it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct Stored {
    /// The rows the transaction's statements inserted, updated, or deleted.
    pub rows_affected: u64,
    /// How replication acknowledged the transaction's batch, or `None` if it committed no batch
    /// because it changed nothing or the engine replicates itself.
    pub acknowledged: Option<Acknowledged>,
}

/// A write transaction that has run, and whose batch, if it committed one, replication has
/// taken: it is part of the database's log, and only its acknowledgement is outstanding.
pub(crate) struct Published {
    executed: Executed,
    acknowledgement: Option<Acknowledgement>,
}

impl Published {
    /// Waits for replication to acknowledge the batch, if the transaction committed one.
    ///
    /// # Errors
    ///
    /// [`DatabaseError::Replication`] if replication couldn't store the batch; the write
    /// committed locally.
    pub(crate) async fn stored(self) -> Result<Stored, DatabaseError> {
        let acknowledged = match self.acknowledgement {
            Some(acknowledgement) => Some(acknowledgement.wait().await?),
            None => None,
        };
        Ok(Stored {
            rows_affected: self.executed.rows_affected,
            acknowledged,
        })
    }
}

impl fmt::Debug for Published {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Published")
            .field("executed", &self.executed)
            .field("replicated", &self.acknowledgement.is_some())
            .finish()
    }
}

/// A cheap, cloneable handle to an open database's actor.
///
/// A handle stops working when its database closes, after an idle timeout or an eviction:
/// requests then fail with [`DatabaseError::Closed`], and the caller opens the database again
/// through the [`Registry`](crate::Registry).
#[derive(Debug, Clone)]
pub struct DatabaseHandle {
    name: DatabaseName,
    shape: Shape,
    sender: mpsc::Sender<Request>,
}

impl DatabaseHandle {
    /// The database's name.
    #[must_use]
    pub fn name(&self) -> &DatabaseName {
        &self.name
    }

    /// How the database replicates. Callers check it before frame-source or self-replicating
    /// requests.
    #[must_use]
    pub fn shape(&self) -> Shape {
        self.shape
    }

    /// Whether the database has closed, so that this handle no longer works.
    #[must_use]
    pub fn is_closed(&self) -> bool {
        self.sender.is_closed()
    }

    /// Runs `transaction` as one write transaction, then waits for replication to acknowledge
    /// its batch.
    ///
    /// Dropping the returned future before the transaction has run stops its statements, and
    /// the transaction then rolls back (FR-078). It has no deadline of its own.
    ///
    /// # Errors
    ///
    /// [`DatabaseError`] if the database is closed, replication refuses the write before it
    /// runs or a statement fails (nothing commits), or replication fails afterwards (the write
    /// committed locally).
    pub async fn execute(&self, transaction: Transaction) -> Result<Stored, DatabaseError> {
        let permit = self.reserve().await.ok_or_else(|| self.closed())?;
        self.execute_reserved(permit, transaction, None).await
    }

    /// Runs a read-only query on the read path, against the latest commit.
    ///
    /// Dropping the returned future before the query has run stops it (FR-078). It has no
    /// deadline of its own.
    ///
    /// # Errors
    ///
    /// [`DatabaseError`] if the database is closed or the query fails.
    pub async fn query(&self, statement: Statement) -> Result<Rows, DatabaseError> {
        let permit = self.reserve().await.ok_or_else(|| self.closed())?;
        self.query_reserved(permit, statement, None).await
    }

    /// The database's schema, as the engine reports it (ADR-0041, point 6), read on the read
    /// path against the latest commit.
    ///
    /// # Errors
    ///
    /// [`DatabaseError`] if the database is closed or the engine can't report its schema; an
    /// engine that can't describe itself fails with [`EngineError::Unsupported`].
    pub async fn schema(&self, request: SchemaRequest) -> Result<Schema, DatabaseError> {
        let permit = self.reserve().await.ok_or_else(|| self.closed())?;
        self.schema_reserved(permit, request, None).await
    }

    /// The position just past the last committed transaction, or `None` for an empty log.
    ///
    /// # Errors
    ///
    /// [`DatabaseError`] if the database is closed or the engine can't report it.
    pub async fn position(&self) -> Result<Option<Position>, DatabaseError> {
        self.call(Request::Position).await
    }

    /// Every committed transaction after `after`, as one batch, for a follower catching up.
    ///
    /// # Errors
    ///
    /// [`DatabaseError::Unsupported`] unless the engine is a frame source, and [`DatabaseError`]
    /// if the database is closed or `after` doesn't fit its log.
    pub async fn committed_since(
        &self,
        after: Option<Position>,
    ) -> Result<Option<FrameBatch>, DatabaseError> {
        self.call(|reply| Request::CommittedSince(after, reply))
            .await
    }

    /// Every committed transaction after `after` up to `until`, as one batch, for a change
    /// stream that may not read past the write quorum's acknowledgements.
    ///
    /// # Errors
    ///
    /// [`DatabaseError::Unsupported`] unless the engine is a frame source, and [`DatabaseError`]
    /// if the database is closed or the positions don't fit its log.
    pub(crate) async fn committed_between(
        &self,
        after: Option<Position>,
        until: Position,
    ) -> Result<Option<FrameBatch>, DatabaseError> {
        self.call(|reply| Request::CommittedBetween(after, until, reply))
            .await
    }

    /// The row changes of the transactions after `after` up to `until`, in commit order.
    ///
    /// # Errors
    ///
    /// [`DatabaseError::Unsupported`] unless the engine records row changes, and
    /// [`DatabaseError`] if the database is closed or the changes can't be read.
    pub(crate) async fn row_changes(
        &self,
        after: Option<Position>,
        until: Position,
    ) -> Result<Vec<RowChange>, DatabaseError> {
        self.call(|reply| Request::RowChanges(after, until, reply))
            .await
    }

    /// The recorded transactions after `after` up to `until`, whole and in commit order, as many
    /// as `limit` allows, for a watcher (ADR-0045). The check that the engine still holds them
    /// and the read run as one request, so no prune falls between them.
    ///
    /// # Errors
    ///
    /// [`DatabaseError::Unsupported`] unless the engine records row changes; an engine error
    /// [`EngineError::ChangesReleased`] if it no longer holds every one after `after`; and
    /// [`DatabaseError`] if the database is closed or the changes can't be read.
    pub(crate) async fn recorded_transactions(
        &self,
        after: Option<Position>,
        until: Position,
        limit: ReadLimit,
    ) -> Result<Vec<RecordedTransaction>, DatabaseError> {
        self.call(|reply| Request::Transactions(after, until, limit, reply))
            .await
    }

    /// Lets the engine forget recorded row changes that every change stream has delivered, up to
    /// `before`.
    ///
    /// # Errors
    ///
    /// [`DatabaseError::Unsupported`] unless the engine records row changes, and
    /// [`DatabaseError`] if the database is closed.
    pub(crate) async fn release_changes(&self, before: Position) -> Result<(), DatabaseError> {
        self.call(|reply| Request::ReleaseChanges(before, reply))
            .await
    }

    /// Every sink mark the database holds, by sink name, or `None` if its engine can't keep marks
    /// inside the database (ADR-0028).
    ///
    /// # Errors
    ///
    /// [`DatabaseError`] if the database is closed or the marks can't be read.
    pub(crate) async fn sink_marks(
        &self,
    ) -> Result<Option<BTreeMap<String, Position>>, DatabaseError> {
        self.call(Request::SinkMarks).await
    }

    /// Stages `delivered` as `sink`'s mark, which the database's next write transaction records,
    /// and says where the position is kept: [`MarkHome::Local`], with nothing staged, if the
    /// engine can't keep marks (ADR-0028).
    ///
    /// # Errors
    ///
    /// [`DatabaseError`] if the database is closed.
    pub(crate) async fn stage_sink_mark(
        &self,
        sink: String,
        delivered: Position,
    ) -> Result<MarkHome, DatabaseError> {
        self.call(|reply| Request::StageSinkMark(sink, delivered, reply))
            .await
    }

    /// Checkpoints a frame-source database's log, and returns the marker for followers.
    ///
    /// This is the only way a database checkpoints: the engine never does on its own.
    ///
    /// # Errors
    ///
    /// [`DatabaseError::Unsupported`] unless the engine is a frame source, and [`DatabaseError`]
    /// if the database is closed or the checkpoint can't complete.
    pub async fn checkpoint(&self) -> Result<CheckpointMarker, DatabaseError> {
        self.call(Request::Checkpoint).await
    }

    /// Writes a consistent copy of the database into `destination`.
    ///
    /// # Errors
    ///
    /// [`DatabaseError`] if the database is closed or the copy can't be written.
    pub async fn snapshot(
        &self,
        destination: impl Into<PathBuf>,
    ) -> Result<DatabaseSnapshot, DatabaseError> {
        let destination = destination.into();
        self.call(|reply| Request::Snapshot(destination, reply))
            .await
    }

    /// Pins a consistent copy of a frame-source database for a follower to read without
    /// blocking writes; see [`FrameSource::pin_snapshot`].
    ///
    /// # Errors
    ///
    /// [`DatabaseError::Unsupported`] unless the engine is a frame source, and [`DatabaseError`]
    /// if the database is closed or the copy can't be pinned.
    pub async fn pin_snapshot(&self) -> Result<PinnedSnapshot, DatabaseError> {
        self.call(Request::PinSnapshot).await
    }

    /// Pins a consistent copy of a frame-source database that holds its log up to `at`, for an
    /// archive's base snapshot; see [`FrameSource::pin_snapshot_at`].
    ///
    /// # Errors
    ///
    /// [`DatabaseError::Unsupported`] unless the engine is a frame source, and [`DatabaseError`]
    /// if the database is closed, `at` isn't a commit boundary of its current log, or the copy
    /// can't be pinned.
    pub async fn pin_snapshot_at(
        &self,
        at: Option<Position>,
    ) -> Result<PinnedSnapshot, DatabaseError> {
        self.call(|reply| Request::PinSnapshotAt(at, reply)).await
    }

    /// How far each replica of a self-replicating database has got.
    ///
    /// # Errors
    ///
    /// [`DatabaseError::Unsupported`] unless the engine replicates itself, and
    /// [`DatabaseError`] if the database is closed or the engine can't report them.
    pub async fn replica_positions(&self) -> Result<Vec<ReplicaPosition>, DatabaseError> {
        self.call(Request::ReplicaPositions).await
    }

    /// Waits until the database has opened.
    pub(crate) async fn ready(&self) -> Result<(), DatabaseError> {
        self.call(Request::Ready).await
    }

    /// Reserves a mailbox slot, or returns `None` if the database has closed.
    pub(crate) async fn reserve(&self) -> Option<mpsc::Permit<'_, Request>> {
        self.sender.reserve().await.ok()
    }

    pub(crate) async fn execute_reserved(
        &self,
        permit: mpsc::Permit<'_, Request>,
        transaction: Transaction,
        deadline: Option<Instant>,
    ) -> Result<Stored, DatabaseError> {
        self.commit_reserved(permit, transaction, deadline)
            .await?
            .stored()
            .await
    }

    /// Runs `transaction`, stopping its statements at `deadline` or once this future is
    /// dropped, and returns once its batch, if it committed one, is in replication's hands,
    /// without waiting for the acknowledgement.
    pub(crate) async fn commit_reserved(
        &self,
        permit: mpsc::Permit<'_, Request>,
        transaction: Transaction,
        deadline: Option<Instant>,
    ) -> Result<Published, DatabaseError> {
        let (executed, acknowledgement) = self
            .interruptible(permit, deadline, |interrupt, reply| {
                Request::Execute(transaction, interrupt, reply)
            })
            .await?;
        Ok(Published {
            executed,
            acknowledgement,
        })
    }

    /// Runs `statement`, stopping it at `deadline` or once this future is dropped.
    pub(crate) async fn query_reserved(
        &self,
        permit: mpsc::Permit<'_, Request>,
        statement: Statement,
        deadline: Option<Instant>,
    ) -> Result<Rows, DatabaseError> {
        self.interruptible(permit, deadline, |interrupt, reply| {
            Request::Query(statement, interrupt, reply)
        })
        .await
    }

    /// Reads the schema, stopping at `deadline` or once this future is dropped.
    pub(crate) async fn schema_reserved(
        &self,
        permit: mpsc::Permit<'_, Request>,
        request: SchemaRequest,
        deadline: Option<Instant>,
    ) -> Result<Schema, DatabaseError> {
        self.interruptible(permit, deadline, |interrupt, reply| {
            Request::Schema(request, interrupt, reply)
        })
        .await
    }

    /// Sends the request `request` makes with a fresh [`Interrupt`], and waits for its reply.
    ///
    /// The interrupt is raised with [`Interruption::Deadline`] once `deadline` passes, by a
    /// `tokio::time` timer, and with [`Interruption::Cancelled`] if this future is dropped before
    /// the reply arrives, because its caller stopped waiting. Either way the reply that follows
    /// says what happened: a statement that finished first keeps its result.
    async fn interruptible<T>(
        &self,
        permit: mpsc::Permit<'_, Request>,
        deadline: Option<Instant>,
        request: impl FnOnce(Interrupt, Reply<T>) -> Request,
    ) -> Result<T, DatabaseError> {
        let interrupt = Interrupt::new();
        // A deadline already past stops the request before it starts, even where the
        // database's calls run inline, so that the timer below can't fire first.
        if deadline.is_some_and(|deadline| deadline <= Instant::now()) {
            interrupt.raise(Interruption::Deadline);
        }
        let (reply, mut result) = oneshot::channel();
        permit.send(request(interrupt.clone(), reply));
        // Once the reply has arrived the statement is done, so raising the flag then is harmless.
        let _cancel_on_drop = RaiseOnDrop(interrupt.clone());
        if let Some(deadline) = deadline {
            tokio::select! {
                replied = &mut result => return self.replied(replied),
                () = time::sleep_until(deadline) => interrupt.raise(Interruption::Deadline),
            }
        }
        self.replied(result.await)
    }

    async fn call<T>(&self, request: impl FnOnce(Reply<T>) -> Request) -> Result<T, DatabaseError> {
        let permit = self.reserve().await.ok_or_else(|| self.closed())?;
        let (reply, result) = oneshot::channel();
        permit.send(request(reply));
        self.await_reply(result).await
    }

    async fn await_reply<T>(
        &self,
        result: oneshot::Receiver<Result<T, DatabaseError>>,
    ) -> Result<T, DatabaseError> {
        self.replied(result.await)
    }

    fn replied<T>(
        &self,
        replied: Result<Result<T, DatabaseError>, oneshot::error::RecvError>,
    ) -> Result<T, DatabaseError> {
        replied.map_err(|_| DatabaseError::Interrupted {
            name: self.name.clone(),
        })?
    }

    fn closed(&self) -> DatabaseError {
        DatabaseError::Closed {
            name: self.name.clone(),
        }
    }
}

/// Raises its interrupt with [`Interruption::Cancelled`] when dropped.
struct RaiseOnDrop(Interrupt);

impl Drop for RaiseOnDrop {
    fn drop(&mut self) {
        self.0.raise(Interruption::Cancelled);
    }
}

/// What the registry needs to start an actor.
#[derive(Debug)]
pub(crate) struct ActorConfig {
    pub(crate) name: DatabaseName,
    pub(crate) engine: Arc<dyn DatabaseEngine>,
    pub(crate) publisher: Arc<dyn BatchPublisher>,
    pub(crate) location: PathBuf,
    pub(crate) mode: OpenMode,
    pub(crate) execution: Execution,
    pub(crate) idle_timeout: Duration,
    pub(crate) mailbox: usize,
    pub(crate) thread_name: String,
    /// Whether the database records row changes for logical change streams.
    pub(crate) changes: ChangeRecording,
}

/// Whether a database records its row changes from the moment it opens, and how much of each
/// row (FR-039).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) enum ChangeRecording {
    /// No logical change stream reads the database.
    #[default]
    Off,
    /// A logical change stream reads it, so every write records its row changes in this mode.
    On(CaptureMode),
}

/// A started actor, as the registry holds it.
#[derive(Debug)]
pub(crate) struct Spawned {
    pub(crate) handle: DatabaseHandle,
    /// Asks the actor to finish its queued requests and close.
    pub(crate) close: oneshot::Sender<()>,
    /// Turns true once the actor has closed its database.
    pub(crate) closed: watch::Receiver<bool>,
}

/// Starts an actor for one database. `on_exit` runs after the database has closed, just before
/// `closed` turns true.
pub(crate) fn spawn(config: ActorConfig, on_exit: impl FnOnce() + Send + 'static) -> Spawned {
    let (sender, mailbox) = mpsc::channel(config.mailbox);
    let (close, close_requested) = oneshot::channel();
    let (closed_tx, closed) = watch::channel(false);
    let handle = DatabaseHandle {
        name: config.name.clone(),
        shape: config.engine.capability().shape(),
        sender,
    };
    tokio::spawn(async move {
        run(config, mailbox, close_requested).await;
        on_exit();
        let _ = closed_tx.send(true);
    });
    Spawned {
        handle,
        close,
        closed,
    }
}

async fn run(
    config: ActorConfig,
    mut mailbox: mpsc::Receiver<Request>,
    close_requested: oneshot::Receiver<()>,
) {
    let ActorConfig {
        name,
        engine,
        publisher,
        location,
        mode,
        execution,
        idle_timeout,
        thread_name,
        changes,
        ..
    } = config;
    let opened = Executor::open(OpenRequest {
        engine,
        location,
        mode,
        execution,
        thread_name,
    })
    .await;
    let executor = match opened {
        Ok(executor) => executor,
        Err(error) => {
            let source = Arc::new(error);
            tracing::debug!(database = %name, error = %source, "database could not be opened");
            mailbox.close();
            while let Some(request) = mailbox.recv().await {
                request.fail(DatabaseError::Open {
                    name: name.clone(),
                    source: Arc::clone(&source),
                });
            }
            return;
        }
    };
    let mut actor = Actor {
        name,
        executor,
        publisher,
        last: None,
        failed: None,
    };
    actor.start(changes).await;
    actor
        .serve(&mut mailbox, close_requested, idle_timeout)
        .await;
    mailbox.close();
    while let Some(request) = mailbox.recv().await {
        actor.handle(request).await;
    }
    let name = actor.name.clone();
    if let Err(error) = actor.executor.close().await {
        tracing::error!(database = %name, %error, "closing the database failed");
    }
}

/// The state an actor keeps while its database is open.
struct Actor {
    name: DatabaseName,
    executor: Executor,
    publisher: Arc<dyn BatchPublisher>,
    /// The end of the last batch captured, for a frame source.
    last: Option<Position>,
    /// Why the database stopped taking writes, if it has.
    failed: Option<Arc<EngineError>>,
}

impl Actor {
    /// Finds where the log ends, so the first write's batch holds only that write, and starts
    /// recording row changes if a logical change stream reads the database.
    async fn start(&mut self, changes: ChangeRecording) {
        let name = self.name.clone();
        let position = self
            .executor
            .run(move |database| {
                if let ChangeRecording::On(mode) = changes {
                    match database.logical_changes() {
                        Some(logical) => logical.start_recording(mode)?,
                        None => tracing::warn!(
                            database = %name,
                            "a logical change stream reads this database, but its engine can't record row changes"
                        ),
                    }
                }
                match database.capability() {
                    DatabaseCapability::FrameSource(frames) => frames.position(),
                    _ => Ok(None),
                }
            })
            .await;
        match position {
            Ok(Ok(position)) => self.last = position,
            Ok(Err(error)) => drop(self.fail(error)),
            Err(stopped) => drop(self.fail(EngineError::Storage(stopped.into()))),
        }
    }

    async fn serve(
        &mut self,
        mailbox: &mut mpsc::Receiver<Request>,
        mut close_requested: oneshot::Receiver<()>,
        idle_timeout: Duration,
    ) {
        loop {
            let request = tokio::select! {
                biased;
                _ = &mut close_requested => return,
                received = time::timeout(idle_timeout, mailbox.recv()) => match received {
                    Ok(Some(request)) => request,
                    Ok(None) | Err(_) => return,
                },
            };
            self.handle(request).await;
        }
    }

    async fn handle(&mut self, request: Request) {
        match request {
            Request::Ready(reply) => {
                let _ = reply.send(Ok(()));
            }
            Request::Execute(transaction, interrupt, reply) => {
                let result = self.execute(transaction, interrupt).await;
                let _ = reply.send(result);
            }
            Request::Query(statement, interrupt, reply) => {
                let result = self
                    .call(move |database| Ok(database.query(&statement, &interrupt)?))
                    .await;
                let _ = reply.send(result);
            }
            Request::Schema(request, interrupt, reply) => {
                let result = self
                    .call(move |database| Ok(database.schema(&request, &interrupt)?))
                    .await;
                let _ = reply.send(result);
            }
            Request::Position(reply) => {
                let last = self.last.clone();
                let result = self
                    .call(move |database| match database.capability() {
                        DatabaseCapability::FrameSource(_) => Ok(last),
                        DatabaseCapability::SelfReplicating(engine) => Ok(engine.position()?),
                    })
                    .await;
                let _ = reply.send(result);
            }
            Request::CommittedSince(after, reply) => {
                let result = self
                    .call(move |database| {
                        Ok(frame_source(database, "reading committed batches")?
                            .committed_since(after.as_ref())?)
                    })
                    .await;
                let _ = reply.send(result);
            }
            Request::CommittedBetween(after, until, reply) => {
                let result = self
                    .call(move |database| {
                        Ok(frame_source(database, "reading committed batches")?
                            .committed_between(after.as_ref(), &until)?)
                    })
                    .await;
                let _ = reply.send(result);
            }
            Request::RowChanges(after, until, reply) => {
                let result = self
                    .call(move |database| {
                        Ok(logical_changes(database, "reading row changes")?
                            .changes_between(after.as_ref(), &until)?)
                    })
                    .await;
                let _ = reply.send(result);
            }
            Request::Transactions(after, until, limit, reply) => {
                let result = self
                    .call(move |database| {
                        Ok(
                            logical_changes(database, "watching changes")?.transactions_after(
                                after.as_ref(),
                                &until,
                                limit,
                            )?,
                        )
                    })
                    .await;
                let _ = reply.send(result);
            }
            Request::ReleaseChanges(before, reply) => {
                let result = self
                    .call(move |database| {
                        Ok(logical_changes(database, "releasing row changes")?.release(&before)?)
                    })
                    .await;
                let _ = reply.send(result);
            }
            Request::SinkMarks(reply) => {
                let result = self
                    .call(|database| {
                        database
                            .sink_marks()
                            .map(|marks| marks.marks())
                            .transpose()
                            .map_err(DatabaseError::from)
                    })
                    .await;
                let _ = reply.send(result);
            }
            Request::StageSinkMark(sink, delivered, reply) => {
                let result = self
                    .call(move |database| match database.sink_marks() {
                        Some(marks) => {
                            marks.stage(&sink, &delivered);
                            Ok(MarkHome::Replicated)
                        }
                        None => Ok(MarkHome::Local),
                    })
                    .await;
                let _ = reply.send(result);
            }
            Request::Checkpoint(reply) => {
                let result = self
                    .call(|database| Ok(frame_source(database, "checkpointing")?.checkpoint()?))
                    .await;
                let _ = reply.send(result);
            }
            Request::Snapshot(destination, reply) => {
                let result = self
                    .call(move |database| Ok(database.snapshot(&destination)?))
                    .await;
                let _ = reply.send(result);
            }
            Request::PinSnapshot(reply) => {
                let result = self
                    .call(|database| {
                        Ok(frame_source(database, "pinning a snapshot")?.pin_snapshot()?)
                    })
                    .await;
                let _ = reply.send(result);
            }
            Request::PinSnapshotAt(at, reply) => {
                let result = self
                    .call(move |database| {
                        Ok(frame_source(database, "pinning a snapshot")?
                            .pin_snapshot_at(at.as_ref())?)
                    })
                    .await;
                let _ = reply.send(result);
            }
            Request::ReplicaPositions(reply) => {
                let result = self
                    .call(|database| match database.capability() {
                        DatabaseCapability::SelfReplicating(engine) => {
                            Ok(engine.replica_positions()?)
                        }
                        _ => Err(DatabaseError::Unsupported {
                            operation: "reading replica positions",
                            needed: Shape::SelfReplicating,
                        }),
                    })
                    .await;
                let _ = reply.send(result);
            }
        }
    }

    /// Runs one write transaction, unless `interrupt` stops it, and captures its batch, if it
    /// committed one.
    async fn execute(
        &mut self,
        transaction: Transaction,
        interrupt: Interrupt,
    ) -> Result<(Executed, Option<Acknowledgement>), DatabaseError> {
        if let Some(source) = &self.failed {
            return Err(DatabaseError::Failed {
                name: self.name.clone(),
                source: Arc::clone(source),
            });
        }
        // Refused here, nothing runs: the engine never sees the transaction.
        self.publisher
            .admit(&self.name)
            .map_err(|source| DatabaseError::Refused {
                name: self.name.clone(),
                source,
            })?;
        let last = self.last.clone();
        let outcome = self
            .executor
            .run(move |database| {
                let result = database.execute(&transaction, &interrupt);
                // Capture runs whatever the outcome, so a commit is never missed. A rolled-back
                // transaction, an interrupted one included, leaves no committed frames, so it
                // yields no batch.
                let captured = match database.capability() {
                    DatabaseCapability::FrameSource(frames) => {
                        Some(frames.committed_since(last.as_ref()))
                    }
                    _ => None,
                };
                (result, captured)
            })
            .await;
        let (result, captured) = match outcome {
            Ok(outcome) => outcome,
            Err(stopped) => {
                self.fail(EngineError::Storage(stopped.into()));
                return Err(DatabaseError::Interrupted {
                    name: self.name.clone(),
                });
            }
        };
        let acknowledgement = match captured {
            None | Some(Ok(None)) => None,
            Some(Ok(Some(batch))) => {
                self.last = Some(batch.end.clone());
                Some(self.publisher.publish(CommittedBatch {
                    database: self.name.clone(),
                    batch,
                }))
            }
            Some(Err(error)) => {
                let source = self.fail(error);
                return Err(DatabaseError::Failed {
                    name: self.name.clone(),
                    source,
                });
            }
        };
        Ok((result?, acknowledgement))
    }

    async fn call<T: Send + 'static>(
        &mut self,
        call: impl FnOnce(&mut dyn Database) -> Result<T, DatabaseError> + Send + 'static,
    ) -> Result<T, DatabaseError> {
        match self.executor.run(call).await {
            Ok(result) => result,
            Err(stopped) => {
                self.fail(EngineError::Storage(stopped.into()));
                Err(DatabaseError::Interrupted {
                    name: self.name.clone(),
                })
            }
        }
    }

    fn fail(&mut self, error: EngineError) -> Arc<EngineError> {
        tracing::error!(database = %self.name, %error, "database stopped taking writes");
        let source = Arc::new(error);
        self.failed = Some(Arc::clone(&source));
        source
    }
}

fn logical_changes<'a>(
    database: &'a mut dyn Database,
    operation: &'static str,
) -> Result<&'a mut dyn LogicalChanges, DatabaseError> {
    database
        .logical_changes()
        .ok_or(DatabaseError::Unsupported {
            operation,
            needed: Shape::FrameSource,
        })
}

fn frame_source<'a>(
    database: &'a mut dyn Database,
    operation: &'static str,
) -> Result<&'a mut dyn FrameSource, DatabaseError> {
    match database.capability() {
        DatabaseCapability::FrameSource(frames) => Ok(frames),
        _ => Err(DatabaseError::Unsupported {
            operation,
            needed: Shape::FrameSource,
        }),
    }
}
