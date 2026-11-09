//! The per-database actor: one task that owns an open database (ADR-0013, point 6).
//!
//! The actor serializes writes, sends reads to the engine's read path, and turns each committed
//! write transaction of a frame-source engine into exactly one batch for the
//! [`BatchPublisher`]. It closes the database when it has been idle for the configured time, or
//! when the registry evicts it. A database that logical change streams read also records its row
//! changes from the moment it opens (FR-039).
//!
//! A write, a query, a schema read, or a session's call carries an [`Interrupt`] that stops its
//! statements on the database's thread (FR-078). Before such a request starts, the actor asks its
//! caller, who may refuse it, as a node does once it no longer serves the database at the
//! request's lease, and who arms the statement deadline from that moment: time a write spends
//! waiting for a session's transaction doesn't count against its statements, and a separate,
//! later bound keeps it from waiting forever ([`StatementLimit`]). The caller's side of the handle raises it: at the request's
//! deadline, from a `tokio::time` timer, and when the caller stops waiting for the reply, so a
//! cancelled call frees the database too. The thread only checks the flag, so time never leaves
//! `tokio::time` and a simulation stays deterministic. Under [`Execution::Inline`] a statement
//! blocks the task that would raise it, so there only a request whose deadline passed, or whose
//! caller left, while it waited in the mailbox is stopped, before it starts.
//!
//! The actor also runs SQL sessions' calls (ADR-0037). A session that begins a transaction holds
//! the database's writer until it commits, rolls back, or ends: meanwhile other writes, and
//! other sessions' calls that may write, wait in the actor, up to
//! [`SessionLimits::max_waiting_writes`] of them, and reads go on. A transaction open longer than
//! [`SessionLimits::transaction_timeout`] is rolled back, its time kept by `tokio::time`, and a
//! session that ends rolls back what it left open. A session that thinks it is in a transaction
//! the database no longer holds, because it timed out or the database closed, is told so, so
//! that its later statements never run outside the transaction they were meant for.

mod executor;

use std::collections::{BTreeMap, BTreeSet, VecDeque};
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
    Autocommit, CallResult, CaptureMode, CheckpointMarker, Database, DatabaseCapability,
    DatabaseEngine, DatabaseSnapshot, EngineCapability, EngineError, Executed, Execution,
    ForkMarker, ForkReset, FrameBatch, FrameSource, Interrupt, Interruption, LogicalChanges,
    OpenMode, PinnedSnapshot, Position, ReadLimit, RecordedTransaction, ReplicaPosition, RowChange,
    Rows, Schema, SchemaRequest, SessionAccess, SessionCall, Shape, Statement, Transaction,
    WriteIntent,
};
use crate::publisher::{
    Acknowledged, Acknowledgement, BatchPublisher, CommittedBatch, ReplicationError,
};

type Reply<T> = oneshot::Sender<Result<T, DatabaseError>>;

/// A SQL session's identity on one node, unique for the node's life (ADR-0037).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SessionId(u64);

impl SessionId {
    /// The session numbered `id`.
    #[must_use]
    pub fn new(id: u64) -> Self {
        Self(id)
    }
}

impl fmt::Display for SessionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// How long a session may hold a database's writer, and how many writes may wait for it
/// (`[sessions]`, ADR-0037 point 14).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SessionLimits {
    /// A session's transaction open longer than this is rolled back.
    pub transaction_timeout: Duration,
    /// How many writes may wait, per database, for a session's transaction to end; the next is
    /// refused with [`DatabaseError::Busy`]. One principal's sessions may take at most half.
    pub max_waiting_writes: usize,
    /// How many sessions one principal may hold open on a node.
    pub max_sessions_per_principal: usize,
    /// How long a session may go without a call before its node ends it.
    pub idle_timeout: Duration,
}

impl Default for SessionLimits {
    /// 30 seconds, 64 waiting writes, 256 sessions per principal, and 60 seconds idle.
    fn default() -> Self {
        Self {
            transaction_timeout: Duration::from_secs(30),
            max_waiting_writes: 64,
            max_sessions_per_principal: 256,
            idle_timeout: Duration::from_secs(60),
        }
    }
}

/// One call of a SQL session, as the actor runs it.
#[derive(Debug, Clone, PartialEq)]
pub struct SessionTurn {
    session: SessionId,
    /// Who opened the session: the principal its token or certificate names.
    principal: String,
    call: SessionCall,
    access: SessionAccess,
    /// Whether the session thinks it is in a transaction, from its last call's answer.
    holding: Autocommit,
}

impl SessionTurn {
    /// Call `call` of session `session`, which `principal` opened with `access`; `holding` is
    /// whether the session thinks it is in a transaction, from its last call's answer.
    #[must_use]
    pub fn new(
        session: SessionId,
        principal: impl Into<String>,
        call: SessionCall,
        access: SessionAccess,
        holding: Autocommit,
    ) -> Self {
        Self {
            session,
            principal: principal.into(),
            call,
            access,
            holding,
        }
    }
}

/// How long a request's statements may run (FR-078): for `budget` once the request starts, and
/// never past `latest`, which also bounds how long it may wait to start, in the mailbox or behind
/// a session's transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StatementLimit {
    /// How long the statements may run once the request starts.
    pub budget: Duration,
    /// When the statements stop, however soon the request started; a request still waiting
    /// then never runs.
    pub latest: Instant,
}

impl StatementLimit {
    /// Statements stop at `deadline`, wherever the time went.
    #[must_use]
    pub fn until(deadline: Instant) -> Self {
        Self {
            budget: Duration::MAX,
            latest: deadline,
        }
    }

    /// When statements that start at `started` stop.
    fn deadline(self, started: Instant) -> Instant {
        started
            .checked_add(self.budget)
            .map_or(self.latest, |end| end.min(self.latest))
    }
}

/// What admits a request when it starts, and keeps it admitted while it runs (a drain waits for
/// it), or `None` to refuse it, which then never runs.
pub(crate) type Admit<'a> = Box<dyn FnOnce() -> Option<Admission<'a>> + Send + 'a>;

/// What keeps a started request admitted: dropped once its write is in replication's hands.
pub(crate) type Admission<'a> = Box<dyn Send + 'a>;

/// What a caller hands the actor with a request: its interrupt, and its say on starting it.
pub(crate) struct Ticket {
    interrupt: Interrupt,
    /// Asked, with where to answer, right before the request starts.
    start: Option<oneshot::Sender<oneshot::Sender<bool>>>,
}

impl fmt::Debug for Ticket {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Ticket")
            .field("interrupt", &self.interrupt)
            .finish_non_exhaustive()
    }
}

impl Ticket {
    /// Asks the caller whether the request may start now, and returns its interrupt if it may,
    /// or why not: the caller has gone or refused it, or its interrupt is raised.
    async fn start(mut self, name: &DatabaseName) -> Result<Interrupt, DatabaseError> {
        let refused = |interrupt: &Interrupt| match interrupt.raised() {
            Some(why) => DatabaseError::Engine(EngineError::Interrupted(why)),
            None => DatabaseError::NotAdmitted { name: name.clone() },
        };
        let Some(start) = self.start.take() else {
            return Ok(self.interrupt);
        };
        let (go, decision) = oneshot::channel();
        if start.send(go).is_err() || !decision.await.unwrap_or(false) {
            // A caller that has gone raised the interrupt as it went.
            return Err(refused(&self.interrupt));
        }
        Ok(self.interrupt)
    }
}

/// A session call that ran, with the acknowledgements of the transactions it committed, in
/// commit order.
type SessionRan = (CallResult, Vec<Acknowledgement>);

/// A request in an actor's mailbox.
pub(crate) enum Request {
    Ready(Reply<()>),
    Execute(
        Transaction,
        Ticket,
        Reply<(Executed, Option<Acknowledgement>)>,
    ),
    Query(Statement, Ticket, Reply<Rows>),
    Schema(SchemaRequest, Ticket, Reply<Schema>),
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
    SessionCall(SessionTurn, Ticket, Reply<SessionRan>),
    SessionEnd(SessionId, Reply<()>),
    ForkMarker(Reply<Option<ForkMarker>>),
    ForkReset(ForkMarker, Reply<(ForkReset, Option<Acknowledgement>)>),
}

impl Request {
    /// Whether the request needs the writer to itself, so that it waits while a session's
    /// transaction holds it: a write, or a checkpoint, which rewrites the database file.
    /// Snapshots read only the committed log, which an open transaction never changes, so a
    /// follower's catch-up never waits for one.
    fn needs_writer(&self) -> bool {
        matches!(
            self,
            Self::Execute(..) | Self::Checkpoint(_) | Self::ForkReset(..)
        )
    }

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
            Self::SessionCall(_, _, reply) => {
                let _ = reply.send(Err(error));
            }
            Self::SessionEnd(_, reply) => {
                let _ = reply.send(Err(error));
            }
            Self::ForkMarker(reply) => {
                let _ = reply.send(Err(error));
            }
            Self::ForkReset(_, reply) => {
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
            Self::SessionCall(..) => "SessionCall",
            Self::SessionEnd(..) => "SessionEnd",
            Self::ForkMarker(_) => "ForkMarker",
            Self::ForkReset(..) => "ForkReset",
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
    /// A session's transaction holds the database's writer, and as many writes as may wait for
    /// it already do; the request never ran (ADR-0037, point 14).
    #[error("database {name} has too many writes waiting for a transaction; retry")]
    Busy {
        /// The database.
        name: DatabaseName,
    },
    /// The session's transaction was open longer than the session limit, and was rolled back;
    /// the call never ran.
    #[error("database {name} rolled back a transaction open longer than the session limit")]
    TransactionTimedOut {
        /// The database.
        name: DatabaseName,
    },
    /// The session's transaction is gone: the database closed, which rolled it back; the call
    /// never ran.
    #[error("database {name} no longer holds the session's transaction; it was rolled back")]
    TransactionLost {
        /// The database.
        name: DatabaseName,
    },
    /// The caller refused the request when it was about to start, because the node no longer
    /// serves the database at the request's lease; it never ran.
    #[error("database {name} is no longer served here; retry")]
    NotAdmitted {
        /// The database.
        name: DatabaseName,
    },
    /// The database's engine can't run SQL sessions (ADR-0037, point 16).
    #[error("database {name}'s engine can't run sql sessions")]
    NoSessions {
        /// The database.
        name: DatabaseName,
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

/// A SQL session's call that ran, and how durably replication stored what it committed.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct SessionCalled {
    /// What the call produced, and whether the session is in a transaction after it.
    pub result: CallResult,
    /// How replication acknowledged the last transaction the call committed, or `None` if it
    /// committed none or the engine replicates itself.
    pub acknowledged: Option<Acknowledged>,
}

/// A session's call that has run, whose committed transactions replication has taken.
pub(crate) struct SessionPublished {
    result: CallResult,
    acknowledgements: Vec<Acknowledgement>,
}

impl SessionPublished {
    /// What the call produced.
    pub(crate) fn result(&self) -> &CallResult {
        &self.result
    }

    /// Waits for replication to acknowledge every transaction the call committed.
    ///
    /// # Errors
    ///
    /// [`DatabaseError::Replication`] if replication couldn't store one; the call's transactions
    /// committed locally.
    pub(crate) async fn stored(self) -> Result<SessionCalled, DatabaseError> {
        let mut acknowledged = None;
        for acknowledgement in self.acknowledgements {
            acknowledged = Some(acknowledgement.wait().await?);
        }
        Ok(SessionCalled {
            result: self.result,
            acknowledged,
        })
    }
}

impl fmt::Debug for SessionPublished {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SessionPublished")
            .field("result", &self.result)
            .field("committed", &self.acknowledgements.len())
            .finish()
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

    /// The marker of the last fork reset the database committed, if it holds one (ADR-0046,
    /// point 6.1); see [`Database::fork_marker`](crate::engine::Database::fork_marker).
    ///
    /// # Errors
    ///
    /// [`DatabaseError`] if the database is closed, its engine can't fork, or the marker can't
    /// be read.
    pub async fn fork_marker(&self) -> Result<Option<ForkMarker>, DatabaseError> {
        self.call(Request::ForkMarker).await
    }

    /// Runs a fork's reset for `marker`, unless the database holds its marker already, and
    /// puts the transaction it wrote, if any, in replication's hands (ADR-0046, point 6.1); see
    /// [`Database::fork_reset`](crate::engine::Database::fork_reset). Returns what it did; the
    /// caller waits for the quorum it needs.
    ///
    /// # Errors
    ///
    /// [`DatabaseError`] if the database is closed, its engine can't fork, or the reset fails;
    /// nothing is written then.
    pub async fn fork_reset(&self, marker: ForkMarker) -> Result<ForkReset, DatabaseError> {
        self.call(|reply| Request::ForkReset(marker, reply))
            .await
            .map(|(reset, _acknowledgement)| reset)
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

    /// Runs one call of SQL session `session` with `access`, stopping its statements at
    /// `deadline` or once this future is dropped (FR-078), and returns once the transactions it
    /// committed are in replication's hands. `holding` is whether the session thinks it is in a
    /// transaction, from its last call's answer.
    pub(crate) async fn session_call_reserved<'a>(
        &self,
        permit: mpsc::Permit<'_, Request>,
        work: SessionTurn,
        limit: Option<StatementLimit>,
        admit: Admit<'a>,
    ) -> Result<(SessionPublished, Option<Admission<'a>>), DatabaseError> {
        let ((result, acknowledgements), admission) = self
            .interruptible(permit, limit, admit, |ticket, reply| {
                Request::SessionCall(work, ticket, reply)
            })
            .await?;
        Ok((
            SessionPublished {
                result,
                acknowledgements,
            },
            admission,
        ))
    }

    /// Ends SQL session `session` here: rolls back the transaction it holds, if any.
    pub(crate) async fn session_end(&self, session: SessionId) -> Result<(), DatabaseError> {
        self.call(|reply| Request::SessionEnd(session, reply)).await
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
        limit: Option<StatementLimit>,
    ) -> Result<Stored, DatabaseError> {
        let (published, _) = self
            .commit_reserved(permit, transaction, limit, Box::new(|| Some(Box::new(()))))
            .await?;
        published.stored().await
    }

    /// Runs `transaction`, admitted by `admit` when it starts, stopping its statements at
    /// `limit` or once this future is dropped, and returns once its batch, if it committed one,
    /// is in replication's hands, without waiting for the acknowledgement, with what keeps it
    /// admitted.
    pub(crate) async fn commit_reserved<'a>(
        &self,
        permit: mpsc::Permit<'_, Request>,
        transaction: Transaction,
        limit: Option<StatementLimit>,
        admit: Admit<'a>,
    ) -> Result<(Published, Option<Admission<'a>>), DatabaseError> {
        let ((executed, acknowledgement), admission) = self
            .interruptible(permit, limit, admit, |ticket, reply| {
                Request::Execute(transaction, ticket, reply)
            })
            .await?;
        Ok((
            Published {
                executed,
                acknowledgement,
            },
            admission,
        ))
    }

    /// Runs `statement`, stopping it at `deadline` or once this future is dropped.
    pub(crate) async fn query_reserved(
        &self,
        permit: mpsc::Permit<'_, Request>,
        statement: Statement,
        deadline: Option<Instant>,
    ) -> Result<Rows, DatabaseError> {
        let limit = deadline.map(StatementLimit::until);
        let (rows, _) = self
            .interruptible(
                permit,
                limit,
                Box::new(|| Some(Box::new(()))),
                |ticket, reply| Request::Query(statement, ticket, reply),
            )
            .await?;
        Ok(rows)
    }

    /// Reads the schema, stopping at `deadline` or once this future is dropped.
    pub(crate) async fn schema_reserved(
        &self,
        permit: mpsc::Permit<'_, Request>,
        request: SchemaRequest,
        deadline: Option<Instant>,
    ) -> Result<Schema, DatabaseError> {
        let limit = deadline.map(StatementLimit::until);
        let (schema, _) = self
            .interruptible(
                permit,
                limit,
                Box::new(|| Some(Box::new(()))),
                |ticket, reply| Request::Schema(request, ticket, reply),
            )
            .await?;
        Ok(schema)
    }

    /// Sends the request `request` makes with a fresh [`Interrupt`], and waits for its reply.
    ///
    /// Right before the request starts, the actor asks: `admit` then admits it, or refuses it,
    /// and it never runs. Its statements then stop at `limit`, counted from that moment: a
    /// `tokio::time` timer raises the interrupt with [`Interruption::Deadline`]. A request that
    /// waits past `limit.latest` to start is refused. The interrupt is raised with
    /// [`Interruption::Cancelled`] if this future is dropped before the reply arrives, because
    /// its caller stopped waiting. Either way the reply that follows says what happened: a
    /// statement that finished first keeps its result.
    async fn interruptible<'a, T>(
        &self,
        permit: mpsc::Permit<'_, Request>,
        limit: Option<StatementLimit>,
        admit: Admit<'a>,
        request: impl FnOnce(Ticket, Reply<T>) -> Request,
    ) -> Result<(T, Option<Admission<'a>>), DatabaseError> {
        let interrupt = Interrupt::new();
        // A deadline already past stops the request before it starts, even where the
        // database's calls run inline, so that the timer below can't fire first.
        if limit.is_some_and(|limit| limit.latest <= Instant::now()) {
            interrupt.raise(Interruption::Deadline);
        }
        let (reply, mut result) = oneshot::channel();
        let (start, mut starting) = oneshot::channel();
        let ticket = Ticket {
            interrupt: interrupt.clone(),
            start: Some(start),
        };
        permit.send(request(ticket, reply));
        // Raised if this future is dropped before the reply arrives: the caller stopped waiting.
        let cancel_on_drop = RaiseOnDrop::new(&interrupt);
        // Waiting to start, at most until the limit's latest moment.
        let mut waiting_until = limit.map(|limit| limit.latest);
        let go = loop {
            tokio::select! {
                replied = &mut result => {
                    cancel_on_drop.disarm();
                    return self.replied(replied).map(|answer| (answer, None));
                }
                go = &mut starting => break go.ok(),
                () = expiry(waiting_until) => {
                    interrupt.raise(Interruption::Deadline);
                    waiting_until = None;
                }
            }
        };
        let mut admission = None;
        if let Some(go) = go {
            let admitted = if interrupt.raised().is_some() {
                None
            } else {
                admit()
            };
            let started = admitted.is_some();
            admission = admitted;
            let _ = go.send(started);
            if let (true, Some(limit)) = (started, limit) {
                let deadline = limit.deadline(Instant::now());
                tokio::select! {
                    replied = &mut result => {
                        cancel_on_drop.disarm();
                        return self.replied(replied).map(|answer| (answer, admission));
                    }
                    () = time::sleep_until(deadline) => interrupt.raise(Interruption::Deadline),
                }
            }
        }
        let replied = result.await;
        cancel_on_drop.disarm();
        self.replied(replied).map(|answer| (answer, admission))
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
struct RaiseOnDrop(Option<Interrupt>);

impl RaiseOnDrop {
    fn new(interrupt: &Interrupt) -> Self {
        Self(Some(interrupt.clone()))
    }

    /// The reply arrived: the call wasn't cancelled.
    fn disarm(mut self) {
        self.0 = None;
    }
}

impl Drop for RaiseOnDrop {
    fn drop(&mut self) {
        if let Some(interrupt) = &self.0 {
            interrupt.raise(Interruption::Cancelled);
        }
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
    /// How long a session's transaction may hold the writer, and how many writes may wait.
    pub(crate) sessions: SessionLimits,
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
        sessions,
        ..
    } = config;
    let opened = Executor::open(OpenRequest {
        engine: Arc::clone(&engine),
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
        engine,
        executor,
        publisher,
        last: None,
        failed: None,
        limits: sessions,
        holder: None,
        waiting: VecDeque::new(),
        expired: BTreeSet::new(),
    };
    actor.start(changes).await;
    actor
        .serve(&mut mailbox, close_requested, idle_timeout)
        .await;
    // A transaction left open goes with the database; what waited for it runs first.
    actor.end_transaction(Ending::Closing).await;
    actor.run_waiting().await;
    mailbox.close();
    while let Some(request) = mailbox.recv().await {
        actor.dispatch(request).await;
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
    /// How long a session's transaction may hold the writer, and how many writes may wait.
    limits: SessionLimits,
    /// The session whose transaction holds the writer, if one does.
    holder: Option<Holder>,
    /// The requests waiting for the holder's transaction to end, in arrival order.
    waiting: VecDeque<Request>,
    /// The sessions whose transactions were rolled back for running too long, until they hear.
    expired: BTreeSet<SessionId>,
    /// The engine, which says how a captured batch splits into transactions.
    engine: Arc<dyn DatabaseEngine>,
}

/// Why a session's transaction ends without its commit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Ending {
    /// Its session ended, which rolls back what it left open.
    SessionEnded,
    /// It was open longer than [`SessionLimits::transaction_timeout`].
    TimedOut,
    /// The database is closing.
    Closing,
}

/// The session whose transaction holds a database's writer.
#[derive(Debug, Clone, Copy)]
struct Holder {
    session: SessionId,
    /// When its transaction is rolled back if it is still open.
    deadline: Instant,
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
            // A database whose writer a transaction holds isn't idle, and the transaction's own
            // limit applies instead.
            let holding = self.holder.map(|holder| holder.deadline);
            let idle = match holding {
                Some(_) => None,
                None => Some(idle_timeout),
            };
            let request = tokio::select! {
                biased;
                _ = &mut close_requested => return,
                () = expiry(holding) => {
                    self.end_transaction(Ending::TimedOut).await;
                    self.run_waiting().await;
                    continue;
                }
                received = receive(mailbox, idle) => match received {
                    Some(request) => request,
                    None => return,
                },
            };
            self.dispatch(request).await;
        }
    }

    /// Handles `request`, then runs what waited for a transaction that has ended.
    async fn dispatch(&mut self, request: Request) {
        self.handle(request).await;
        self.run_waiting().await;
    }

    /// Runs the requests that waited for a transaction, in order, while no transaction holds the
    /// writer. One of them may begin a transaction, and the rest then wait again.
    async fn run_waiting(&mut self) {
        while self.holder.is_none() && !self.waiting.is_empty() {
            let waiting = std::mem::take(&mut self.waiting);
            for request in waiting {
                self.handle(request).await;
            }
        }
    }

    /// Queues `request` until the holder's transaction ends, or refuses it if as many writes as
    /// may wait already do.
    ///
    /// One principal's sessions may take at most half the places, so that a principal re-queuing
    /// `BEGIN` from many sessions can't keep everyone else's writes out.
    fn wait(&mut self, request: Request) {
        let full = self.waiting.len() >= self.limits.max_waiting_writes;
        let over_share = match &request {
            Request::SessionCall(turn, _, _) => {
                let share = (self.limits.max_waiting_writes / 2).max(1);
                let queued = self
                    .waiting
                    .iter()
                    .filter(|waiting| {
                        matches!(waiting, Request::SessionCall(other, _, _) if other.principal == turn.principal)
                    })
                    .count();
                queued >= share
            }
            _ => false,
        };
        if full || over_share {
            request.fail(DatabaseError::Busy {
                name: self.name.clone(),
            });
        } else {
            self.waiting.push_back(request);
        }
    }

    async fn handle(&mut self, request: Request) {
        if self.holder.is_some() && request.needs_writer() {
            self.wait(request);
            return;
        }
        match request {
            Request::SessionCall(work, ticket, reply) => {
                let other = self.holder.filter(|holder| holder.session != work.session);
                if other.is_none() {
                    let result = self.session_call(work, ticket).await;
                    let _ = reply.send(result);
                    return;
                }
                if work.holding == Autocommit::InTransaction {
                    let _ = reply.send(Err(self.lost(work.session)));
                    return;
                }
                // A read-only session never needs the writer: it runs now, and the engine
                // refuses whatever it may not run.
                if work.access == SessionAccess::ReadOnly {
                    let result = self.session_read(work, ticket).await;
                    let _ = reply.send(result);
                    return;
                }
                // Another session's transaction holds the writer: a call that only reads runs
                // now, against the latest commit; one that may write waits.
                let call = work.call.clone();
                let intent = self
                    .call(move |database| match database.sql_sessions() {
                        Some(sessions) => Ok(sessions.intent(&call)?),
                        None => Ok(WriteIntent::MayWrite),
                    })
                    .await;
                match intent {
                    Ok(WriteIntent::ReadOnly) => {
                        let result = self.session_read(work, ticket).await;
                        let _ = reply.send(result);
                    }
                    Ok(WriteIntent::MayWrite) => {
                        self.wait(Request::SessionCall(work, ticket, reply));
                    }
                    Err(error) => {
                        let _ = reply.send(Err(error));
                    }
                }
            }
            Request::SessionEnd(session, reply) => {
                if self.holder.is_some_and(|holder| holder.session == session) {
                    self.end_transaction(Ending::SessionEnded).await;
                }
                self.expired.remove(&session);
                let _ = reply.send(Ok(()));
            }
            Request::Ready(reply) => {
                let _ = reply.send(Ok(()));
            }
            Request::Execute(transaction, ticket, reply) => {
                let result = self.execute(transaction, ticket).await;
                let _ = reply.send(result);
            }
            Request::Query(statement, ticket, reply) => {
                let result = match ticket.start(&self.name).await {
                    Ok(interrupt) => {
                        self.call(move |database| Ok(database.query(&statement, &interrupt)?))
                            .await
                    }
                    Err(error) => Err(error),
                };
                let _ = reply.send(result);
            }
            Request::Schema(request, ticket, reply) => {
                let result = match ticket.start(&self.name).await {
                    Ok(interrupt) => {
                        self.call(move |database| Ok(database.schema(&request, &interrupt)?))
                            .await
                    }
                    Err(error) => Err(error),
                };
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
            Request::ForkMarker(reply) => {
                let result = self.call(|database| Ok(database.fork_marker()?)).await;
                let _ = reply.send(result);
            }
            Request::ForkReset(marker, reply) => {
                let result = self.fork_reset(marker).await;
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
        ticket: Ticket,
    ) -> Result<(Executed, Option<Acknowledgement>), DatabaseError> {
        if let Some(source) = &self.failed {
            return Err(DatabaseError::Failed {
                name: self.name.clone(),
                source: Arc::clone(source),
            });
        }
        let interrupt = ticket.start(&self.name).await?;
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

    /// Runs a fork's reset, and captures and publishes the transaction it wrote, if any, as a
    /// write's batch is (ADR-0046, point 6.1).
    async fn fork_reset(
        &mut self,
        marker: ForkMarker,
    ) -> Result<(ForkReset, Option<Acknowledgement>), DatabaseError> {
        if let Some(source) = &self.failed {
            return Err(DatabaseError::Failed {
                name: self.name.clone(),
                source: Arc::clone(source),
            });
        }
        let last = self.last.clone();
        let outcome = self
            .executor
            .run(move |database| {
                let result = database.fork_reset(&marker);
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

    /// Why a session that thinks it is in a transaction isn't: it ran too long, or the database
    /// closed since.
    fn lost(&mut self, session: SessionId) -> DatabaseError {
        let name = self.name.clone();
        if self.expired.remove(&session) {
            DatabaseError::TransactionTimedOut { name }
        } else {
            DatabaseError::TransactionLost { name }
        }
    }

    /// Runs one call of a session that holds the writer, or of any session while none does, and
    /// captures and publishes each transaction it committed.
    async fn session_call(
        &mut self,
        work: SessionTurn,
        ticket: Ticket,
    ) -> Result<SessionRan, DatabaseError> {
        let SessionTurn {
            session,
            call,
            access,
            holding,
            ..
        } = work;
        if let Some(source) = &self.failed {
            return Err(DatabaseError::Failed {
                name: self.name.clone(),
                source: Arc::clone(source),
            });
        }
        let holds = self.holder.is_some_and(|holder| holder.session == session);
        if holding == Autocommit::InTransaction && !holds {
            return Err(self.lost(session));
        }
        if access == SessionAccess::ReadWrite {
            let checked = call.clone();
            let intent = self
                .call(move |database| match database.sql_sessions() {
                    Some(sessions) => Ok(sessions.intent(&checked)?),
                    None => Ok(WriteIntent::ReadOnly),
                })
                .await?;
            // Refused here, nothing runs: below the replica floor, a call that may write never
            // reaches the engine (ADR-0023).
            if intent == WriteIntent::MayWrite {
                self.publisher
                    .admit(&self.name)
                    .map_err(|source| DatabaseError::Refused {
                        name: self.name.clone(),
                        source,
                    })?;
            }
        }
        let interrupt = ticket.start(&self.name).await?;
        let last = self.last.clone();
        let engine = Arc::clone(&self.engine);
        let outcome = self
            .executor
            .run(move |database| {
                let sessions = database.sql_sessions()?;
                let result = sessions.run(&call, access, &interrupt);
                let autocommit = sessions.autocommit();
                // Capture runs whatever the outcome, so a commit is never missed.
                let captured = capture_each(database, engine.as_ref(), last);
                Some((result, autocommit, captured))
            })
            .await;
        let (result, autocommit, captured) = match outcome {
            Ok(Some(outcome)) => outcome,
            Ok(None) => {
                return Err(DatabaseError::NoSessions {
                    name: self.name.clone(),
                });
            }
            Err(stopped) => {
                self.fail(EngineError::Storage(stopped.into()));
                self.holder = None;
                return Err(DatabaseError::Interrupted {
                    name: self.name.clone(),
                });
            }
        };
        let acknowledgements = match captured {
            None => Vec::new(),
            Some(Ok(batches)) => batches
                .into_iter()
                .map(|batch| {
                    self.last = Some(batch.end.clone());
                    self.publisher.publish(CommittedBatch {
                        database: self.name.clone(),
                        batch,
                    })
                })
                .collect(),
            Some(Err(error)) => {
                let source = self.fail(error);
                return Err(DatabaseError::Failed {
                    name: self.name.clone(),
                    source,
                });
            }
        };
        match (autocommit, holds) {
            (Autocommit::InTransaction, false) => {
                self.holder = Some(Holder {
                    session,
                    deadline: Instant::now() + self.limits.transaction_timeout,
                });
            }
            (Autocommit::On, true) => self.holder = None,
            _ => {}
        }
        Ok((result?, acknowledgements))
    }

    /// Runs one call of a session that doesn't hold the writer while another session's
    /// transaction does: a call that only reads, run as a read-only session's.
    async fn session_read(
        &mut self,
        work: SessionTurn,
        ticket: Ticket,
    ) -> Result<SessionRan, DatabaseError> {
        let SessionTurn { call, .. } = work;
        let interrupt = ticket.start(&self.name).await?;
        let result = self
            .call(move |database| match database.sql_sessions() {
                Some(sessions) => Ok(sessions.run(&call, SessionAccess::ReadOnly, &interrupt)?),
                None => Err(EngineError::Unsupported { what: "sessions" }.into()),
            })
            .await?;
        Ok((result, Vec::new()))
    }

    /// Rolls back the holder's transaction, if a session holds one.
    async fn end_transaction(&mut self, ending: Ending) {
        let Some(holder) = self.holder.take() else {
            return;
        };
        match ending {
            Ending::SessionEnded => {}
            Ending::TimedOut => {
                self.expired.insert(holder.session);
                tracing::warn!(database = %self.name, session = %holder.session, "rolled back a session's transaction open longer than the session limit");
            }
            Ending::Closing => {
                tracing::info!(database = %self.name, session = %holder.session, "rolled back a session's transaction as the database closed");
            }
        }
        let rolled_back = self
            .call(|database| match database.sql_sessions() {
                Some(sessions) => Ok(sessions.roll_back()?),
                None => Ok(()),
            })
            .await;
        if let Err(error) = rolled_back {
            tracing::error!(database = %self.name, %error, "rolling back a session's transaction failed");
        }
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

/// Every transaction committed after `last`, as one batch each, in commit order: the batches a
/// session's call publishes, so that each transaction is replicated and acknowledged as an
/// `Execute`'s is. `None` for an engine that replicates itself.
fn capture_each(
    database: &mut dyn Database,
    engine: &dyn DatabaseEngine,
    last: Option<Position>,
) -> Option<Result<Vec<FrameBatch>, EngineError>> {
    let DatabaseCapability::FrameSource(frames) = database.capability() else {
        return None;
    };
    let captured = (|| {
        let Some(batch) = frames.committed_since(last.as_ref())? else {
            return Ok(Vec::new());
        };
        let EngineCapability::FrameSource(source) = engine.capability() else {
            return Ok(vec![batch]);
        };
        let transactions = source.transactions(&batch)?;
        if transactions.len() <= 1 {
            return Ok(vec![batch]);
        }
        let mut batches = Vec::with_capacity(transactions.len());
        let mut after = last;
        for transaction in transactions {
            if let Some(one) = frames.committed_between(after.as_ref(), &transaction.end)? {
                batches.push(one);
            }
            after = Some(transaction.end);
        }
        Ok(batches)
    })();
    Some(captured)
}

/// Waits until `deadline`, or forever without one.
async fn expiry(deadline: Option<Instant>) {
    match deadline {
        Some(deadline) => time::sleep_until(deadline).await,
        None => std::future::pending().await,
    }
}

/// The next request in `mailbox`, or `None` once it closes or, with `idle`, stays empty that
/// long.
async fn receive(mailbox: &mut mpsc::Receiver<Request>, idle: Option<Duration>) -> Option<Request> {
    match idle {
        Some(idle) => time::timeout(idle, mailbox.recv()).await.ok().flatten(),
        None => mailbox.recv().await,
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
