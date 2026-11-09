//! The `DatabaseEngine` trait and its replication capabilities (ADR-0016).

use std::collections::BTreeMap;
use std::error::Error;
use std::fmt;
use std::ops::Range;
use std::path::Path;

use crate::engine::{
    CaptureMode, ChangesAfter, CheckpointMarker, DatabaseSnapshot, Executed, FrameBatch, Interrupt,
    Interruption, LogicalSupport, PinnedSnapshot, Position, ReadLimit, RecordedTransaction,
    ReplicaPosition, RowChange, Rows, Schema, SchemaRequest, SqlSessions, Statement, Transaction,
};

/// A storage engine that opens databases (ADR-0015, ADR-0016).
///
/// Every engine opens or creates, executes, queries, snapshots, and closes databases. How a
/// database replicates depends on the engine's [`capability`](Self::capability): a frame source
/// hands each committed transaction to `fleetringd` to ship, while a self-replicating engine
/// replicates itself and only reports positions. Callers match on the capability and never
/// assume frames.
pub trait DatabaseEngine: fmt::Debug + Send + Sync + 'static {
    /// The engine's kind, which every database records (ADR-0021).
    fn kind(&self) -> EngineKind;

    /// How the engine's databases replicate.
    fn capability(&self) -> EngineCapability<'_>;

    /// Whether the engine's databases can record and decode row changes for a logical change
    /// stream (FR-039). An engine that can't still feeds physical streams if it is a frame
    /// source. The default is [`LogicalSupport::Unsupported`].
    fn logical_support(&self) -> LogicalSupport {
        LogicalSupport::Unsupported
    }

    /// Opens or creates the database stored at `location`, a directory the engine owns.
    ///
    /// # Errors
    ///
    /// [`EngineError::NotFound`] or [`EngineError::AlreadyExists`] if `mode` rules the database's
    /// state out, and [`EngineError::Storage`] if the engine can't open it.
    fn open(&self, location: &Path, mode: OpenMode) -> Result<Box<dyn Database>, EngineError>;
}

/// An open database: the single owner of its storage while it is open.
///
/// Every method blocks. The actor calls them on the database's own thread, or inline under
/// simulation (ADR-0020).
///
/// [`execute`](Self::execute), [`query`](Self::query), and [`schema`](Self::schema) take an
/// [`Interrupt`] that the caller raises at its deadline or when it goes away (FR-078). The
/// engine checks it before it starts and while the user's statements run, often enough that a
/// statement stops soon after it is raised, and then fails the call with
/// [`EngineError::Interrupted`]. An interrupted write transaction rolls back and commits
/// nothing. Once the statements have all run, the engine finishes the call whatever the
/// interrupt says, so a transaction is never cut between its last statement and its commit.
/// The engine never reads a clock: time stays with whoever raises the interrupt.
pub trait Database: fmt::Debug + Send {
    /// Runs `transaction` as one write transaction, unless `interrupt` stops it first.
    ///
    /// # Errors
    ///
    /// [`EngineError::Statement`] if a statement fails, and [`EngineError::Interrupted`] if
    /// `interrupt` stops one, each after rolling the transaction back; [`EngineError::Storage`]
    /// if storage fails.
    fn execute(
        &mut self,
        transaction: &Transaction,
        interrupt: &Interrupt,
    ) -> Result<Executed, EngineError>;

    /// Runs a read-only query against the latest committed state, unless `interrupt` stops it
    /// first.
    ///
    /// # Errors
    ///
    /// [`EngineError::Statement`] if the query fails or tries to write, and
    /// [`EngineError::Interrupted`] if `interrupt` stops it.
    fn query(&mut self, statement: &Statement, interrupt: &Interrupt) -> Result<Rows, EngineError>;

    /// The database's schema (ADR-0041, point 6): its tables, views, indexes, triggers, and
    /// virtual tables, with the engine's own definitions and row counts capped by the request.
    /// It reads the latest committed state, as [`query`](Self::query) does, and must not hold
    /// up writes. Engines that can't describe themselves keep the default.
    ///
    /// # Errors
    ///
    /// [`EngineError::Unsupported`] if the engine can't describe its databases,
    /// [`EngineError::Interrupted`] if `interrupt` stops the read, and [`EngineError::Storage`]
    /// if the catalog can't be read.
    fn schema(
        &mut self,
        request: &SchemaRequest,
        interrupt: &Interrupt,
    ) -> Result<Schema, EngineError> {
        let _ = (request, interrupt);
        Err(EngineError::Unsupported { what: "schema" })
    }

    /// Writes a consistent copy of the database into `destination`, a directory that a follower
    /// can later open as its location.
    ///
    /// # Errors
    ///
    /// [`EngineError::Storage`] if the copy can't be written.
    fn snapshot(&mut self, destination: &Path) -> Result<DatabaseSnapshot, EngineError>;

    /// How this database replicates, with access to the matching operations.
    fn capability(&mut self) -> DatabaseCapability<'_>;

    /// The database's row-change recorder, if its engine supports logical change streams
    /// ([`DatabaseEngine::logical_support`]). The default is `None`.
    fn logical_changes(&mut self) -> Option<&mut dyn LogicalChanges> {
        None
    }

    /// The database's SQL sessions, if its engine runs them (ADR-0037). The default is `None`,
    /// and a `Database.Session` call on such a database is refused with `FAILED_PRECONDITION`.
    fn sql_sessions(&mut self) -> Option<&mut dyn SqlSessions> {
        None
    }

    /// The database's recorder of sink marks, if its engine can keep them inside the database
    /// (ADR-0028). The default is `None`: the engine's change streams then keep their positions
    /// on this node only.
    fn sink_marks(&mut self) -> Option<&mut dyn SinkMarks> {
        None
    }

    /// Closes the database. It doesn't checkpoint: the log stays as it is for the next open.
    ///
    /// # Errors
    ///
    /// [`EngineError::Storage`] if closing fails.
    fn close(self: Box<Self>) -> Result<(), EngineError>;
}

/// An engine's replication shape, with access to its engine-wide operations.
#[derive(Debug, Clone, Copy)]
#[non_exhaustive]
pub enum EngineCapability<'a> {
    /// `fleetringd` ships the engine's committed batches and has followers apply them.
    FrameSource(&'a dyn FrameSourceEngine),
    /// The engine replicates itself; `fleetringd` only places, fences, and reads positions.
    SelfReplicating,
}

impl EngineCapability<'_> {
    /// The shape alone, without access to operations.
    #[must_use]
    pub fn shape(&self) -> Shape {
        match self {
            Self::FrameSource(_) => Shape::FrameSource,
            Self::SelfReplicating => Shape::SelfReplicating,
        }
    }
}

/// An open database's replication shape, with access to the matching operations.
#[derive(Debug)]
#[non_exhaustive]
pub enum DatabaseCapability<'a> {
    /// The database yields committed batches.
    FrameSource(&'a mut dyn FrameSource),
    /// The database replicates itself and reports its replicas' positions.
    SelfReplicating(&'a mut dyn SelfReplicating),
}

impl DatabaseCapability<'_> {
    /// The shape alone, without access to operations.
    #[must_use]
    pub fn shape(&self) -> Shape {
        match self {
            Self::FrameSource(_) => Shape::FrameSource,
            Self::SelfReplicating(_) => Shape::SelfReplicating,
        }
    }
}

/// The two ways a database replicates (ADR-0016, point 1).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[non_exhaustive]
pub enum Shape {
    /// `fleetringd` ships committed batches to followers.
    FrameSource,
    /// The engine replicates itself.
    SelfReplicating,
}

impl fmt::Display for Shape {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::FrameSource => "frame-source",
            Self::SelfReplicating => "self-replicating",
        })
    }
}

/// The frame-source operations on an open primary.
pub trait FrameSource: fmt::Debug + Send {
    /// The position just past the last committed transaction, or `None` for an empty log.
    ///
    /// # Errors
    ///
    /// [`EngineError::Storage`] if the log can't be read.
    fn position(&mut self) -> Result<Option<Position>, EngineError>;

    /// Every committed transaction after `after` as one batch, or `None` if there is none.
    ///
    /// `after` is a position this engine handed out, or `None` to read the whole current log.
    ///
    /// # Errors
    ///
    /// [`EngineError::InvalidPosition`] if `after` doesn't fit the log, including a position
    /// from an earlier log generation that the engine can't vouch for continuing from, and
    /// [`EngineError::Storage`] if the log can't be read. A follower at such a position can only
    /// catch up from a snapshot.
    fn committed_since(
        &mut self,
        after: Option<&Position>,
    ) -> Result<Option<FrameBatch>, EngineError>;

    /// Every committed transaction after `after` up to `until`, as one batch, or `None` if
    /// there is none.
    ///
    /// `until` is a position this engine handed out as a batch end, at or before the log's end.
    /// A change stream reads this way, so that it never hands out a transaction past the write
    /// quorum's acknowledgements (FR-039).
    ///
    /// # Errors
    ///
    /// As [`committed_since`](Self::committed_since), and [`EngineError::InvalidPosition`] if
    /// `until` isn't a commit boundary of the log after `after`.
    fn committed_between(
        &mut self,
        after: Option<&Position>,
        until: &Position,
    ) -> Result<Option<FrameBatch>, EngineError>;

    /// Checkpoints the log into the database, and returns the marker followers checkpoint at.
    ///
    /// Engines never checkpoint on their own: this is the only way the log restarts.
    ///
    /// # Errors
    ///
    /// [`EngineError::CheckpointBlocked`] if the checkpoint couldn't complete, and
    /// [`EngineError::Storage`] if it failed.
    fn checkpoint(&mut self) -> Result<CheckpointMarker, EngineError>;

    /// Pins a consistent copy of the database for a follower to read while writes go on.
    ///
    /// The call itself is quick; reading the copy happens elsewhere, through the returned
    /// [`PinnedSnapshot`], without the database's thread. While it lives, the engine keeps its
    /// files in place, so [`checkpoint`](Self::checkpoint) may fail with
    /// [`EngineError::CheckpointBlocked`].
    ///
    /// # Errors
    ///
    /// [`EngineError::Storage`] if the log or the files can't be read.
    fn pin_snapshot(&mut self) -> Result<PinnedSnapshot, EngineError>;

    /// Pins a consistent copy that holds the log up to `at` and nothing after it: a commit
    /// boundary of the current log, or the position the current log continues from, or `None`
    /// for the main file alone, as of the last checkpoint.
    ///
    /// An archive pins at a position its change stream reached, which a pump reaches only
    /// through acknowledged batches, so the copy never holds a write no quorum acknowledged
    /// (ADR-0029). The copy is held in place as [`pin_snapshot`](Self::pin_snapshot)'s is, and
    /// [`FrameSourceEngine::open_follower`] opens it as a copy that the log's batches after `at`
    /// continue.
    ///
    /// # Errors
    ///
    /// [`EngineError::InvalidPosition`] if `at` isn't a commit boundary between the current
    /// log's start and its end, and [`EngineError::Storage`] if the files can't be read.
    fn pin_snapshot_at(&mut self, at: Option<&Position>) -> Result<PinnedSnapshot, EngineError>;
}

/// The frame-source operations that don't need an open primary.
pub trait FrameSourceEngine: fmt::Debug + Send + Sync {
    /// Opens the follower copy at `location`, which a [`Database::snapshot`] created.
    ///
    /// A follower never opens the database itself; it only mirrors the primary's log.
    ///
    /// # Errors
    ///
    /// [`EngineError::NotFound`] if `location` holds no copy, and [`EngineError::Storage`] if it
    /// can't be read.
    fn open_follower(&self, location: &Path) -> Result<Box<dyn FrameFollower>, EngineError>;

    /// The transactions `batch` holds, in commit order: where each ends, and which of the
    /// payload's bytes are its own. The ranges are disjoint and in order, and the last
    /// transaction ends at the batch's end.
    ///
    /// The replication log's hash chain (FR-044) hashes each transaction's bytes, so the ranges
    /// must hold exactly the bytes of the log: a batch read back from the log as several
    /// transactions must hash the same as the batches that shipped them one at a time. Bytes of
    /// the log that belong to no transaction, such as a log generation's header, go with the
    /// transaction that follows them; bytes that describe the batch itself, such as its framing,
    /// are left out of every range.
    ///
    /// # Errors
    ///
    /// [`EngineError::InvalidPayload`] if the payload is malformed.
    fn transactions(&self, batch: &FrameBatch) -> Result<Vec<BatchTransaction>, EngineError>;
}

/// One transaction inside a [`FrameBatch`], as [`FrameSourceEngine::transactions`] reports it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BatchTransaction {
    /// The position just past the transaction.
    pub end: Position,
    /// The transaction's bytes inside the batch's payload.
    pub bytes: Range<usize>,
}

/// A follower's copy of a frame-source database, which applies the primary's batches.
pub trait FrameFollower: fmt::Debug + Send {
    /// The position just past the last applied transaction, or `None` before the first batch.
    fn position(&self) -> Option<Position>;

    /// The position the copy's current log continues from: where the last checkpoint the copy
    /// followed ended, or `None` if its log has never restarted. The copy can read its log back
    /// from here on ([`committed_since`](Self::committed_since)), and from no earlier position.
    fn log_start(&self) -> Option<Position>;

    /// Applies `batch` durably, and returns the new position. A follower acknowledges a batch
    /// only after this returns.
    ///
    /// # Errors
    ///
    /// [`EngineError::InvalidPayload`] if the batch is malformed or doesn't continue the copy, and
    /// [`EngineError::Storage`] if it can't be written.
    fn apply(&mut self, batch: &FrameBatch) -> Result<Position, EngineError>;

    /// Checkpoints the copy at the primary's `marker`.
    ///
    /// # Errors
    ///
    /// [`EngineError::InvalidPosition`] unless the copy is exactly at the marker, and
    /// [`EngineError::Storage`] if the checkpoint fails.
    fn checkpoint(&mut self, marker: &CheckpointMarker) -> Result<(), EngineError>;

    /// Every transaction the copy holds after `after` as one batch, or `None` if there is none.
    ///
    /// A promotion's candidate fetches the frames it lacks from the most advanced fenced member
    /// this way (ADR-0018, point 4). `after` is a position of this log, or `None` to read the
    /// whole current log.
    ///
    /// # Errors
    ///
    /// [`EngineError::InvalidPosition`] if `after` doesn't fit the copy's log, and
    /// [`EngineError::Storage`] if it can't be read.
    fn committed_since(&self, after: Option<&Position>) -> Result<Option<FrameBatch>, EngineError>;

    /// Every transaction the copy holds after `after` up to `until`, as one batch, or `None` if
    /// there is none.
    ///
    /// Verifying the log's hash chain reads the copy back this way, up to the position being
    /// verified (FR-044).
    ///
    /// # Errors
    ///
    /// As [`committed_since`](Self::committed_since), and [`EngineError::InvalidPosition`] if
    /// `until` isn't a transaction boundary of the copy's log after `after`.
    fn committed_between(
        &self,
        after: Option<&Position>,
        until: &Position,
    ) -> Result<Option<FrameBatch>, EngineError>;

    /// Discards everything the copy holds past `position`, durably, so that it is at `position`.
    ///
    /// A member whose log runs past the position a new primary adopted truncates it this way
    /// before it accepts the new epoch's frames (ADR-0018, point 5).
    ///
    /// # Errors
    ///
    /// [`EngineError::InvalidPosition`] if `position` isn't a transaction boundary of the copy's
    /// current log, for example because it is in an older generation, and
    /// [`EngineError::Storage`] if the copy can't be written.
    fn truncate_to(&mut self, position: &Position) -> Result<(), EngineError>;
}

/// Row-change recording on an open database, for logical change streams (FR-039, ADR-0024).
///
/// Once recording starts, the engine keeps the row changes of every later write transaction as
/// durably as the transaction itself, keyed by the log position the transaction started from,
/// until [`release`](Self::release) lets it forget them. A change stream then decodes the
/// changes of any batch it reads from the log, after a restart and on a promoted follower too.
/// How much of each row is kept is the [`CaptureMode`] recording started in.
pub trait LogicalChanges: fmt::Debug + Send {
    /// Starts recording row changes of every later write transaction, in `mode`.
    ///
    /// Recording that has already started keeps its mode.
    ///
    /// # Errors
    ///
    /// [`EngineError::Storage`] if the recorder can't be set up.
    fn start_recording(&mut self, mode: CaptureMode) -> Result<(), EngineError>;

    /// The row changes of the transactions after `after` up to `until`, in commit order, and in
    /// statement order within a transaction.
    ///
    /// `after` is `None` for the current log's beginning, as a reader with no position reads it.
    ///
    /// # Errors
    ///
    /// [`EngineError::Storage`] if the recorded changes can't be read.
    fn changes_between(
        &mut self,
        after: Option<&Position>,
        until: &Position,
    ) -> Result<Vec<RowChange>, EngineError>;

    /// Lets the engine forget the recorded changes of transactions that ended at or before
    /// `before`, which every change stream has delivered.
    ///
    /// # Errors
    ///
    /// [`EngineError::Storage`] if the engine can't record the release.
    fn release(&mut self, before: &Position) -> Result<(), EngineError>;

    /// The recorded transactions after `after` up to `until`, in commit order, each whole, with
    /// the position it started from and the position to resume after it, for a watcher
    /// (ADR-0045). At most `limit` of them, but always one whole transaction if there is one.
    ///
    /// `after` is `None` for the current log's beginning. Unlike
    /// [`changes_between`](Self::changes_between), it never returns less than was recorded: if
    /// the engine no longer holds every recorded transaction after `after`, it refuses.
    ///
    /// # Errors
    ///
    /// [`EngineError::ChangesReleased`] with the oldest position the engine holds changes from,
    /// if it doesn't hold every recorded change after `after`;
    /// [`EngineError::InvalidPosition`] if `after` isn't a position of this engine;
    /// [`EngineError::Storage`] if the recorded changes can't be read; and
    /// [`EngineError::Unsupported`], the default, for an engine that can't tell its recorded
    /// transactions apart.
    fn transactions_after(
        &mut self,
        after: Option<&Position>,
        until: &Position,
        limit: ReadLimit,
    ) -> Result<Vec<RecordedTransaction>, EngineError> {
        let _ = (after, until, limit);
        Err(EngineError::Unsupported {
            what: "watching changes",
        })
    }
}

/// Delivered positions of change sinks, kept inside the database so that they replicate,
/// fence, and survive with it (ADR-0028).
///
/// A mark is staged after its sink has taken a batch, and recorded by the next write
/// transaction, before the transaction's own statements; the engine never starts a
/// transaction just to record one.
pub trait SinkMarks: fmt::Debug + Send {
    /// Every recorded mark, by sink name, as this copy holds them.
    ///
    /// # Errors
    ///
    /// [`EngineError::Storage`] if the marks can't be read.
    fn marks(&mut self) -> Result<BTreeMap<String, Position>, EngineError>;

    /// Stages `delivered` as `sink`'s mark, replacing any mark staged for it before. The next
    /// write transaction records it; one that rolls back leaves it staged.
    fn stage(&mut self, sink: &str, delivered: &Position);
}

/// The operations on an open database whose engine replicates itself.
pub trait SelfReplicating: fmt::Debug + Send {
    /// The position just past the last committed transaction, or `None` for an empty log.
    ///
    /// # Errors
    ///
    /// [`EngineError::Storage`] if the engine can't report it.
    fn position(&mut self) -> Result<Option<Position>, EngineError>;

    /// How far each of the database's replicas has got.
    ///
    /// # Errors
    ///
    /// [`EngineError::Storage`] if the engine can't report them.
    fn replica_positions(&mut self) -> Result<Vec<ReplicaPosition>, EngineError>;
}

/// Which engine a database runs on (ADR-0021).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[non_exhaustive]
pub enum EngineKind {
    /// SQLite, replicated by shipping WAL frames (ADR-0016).
    Sqlite,
    /// The in-memory test engine, which replicates itself.
    Memory,
}

impl fmt::Display for EngineKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Sqlite => "sqlite",
            Self::Memory => "memory",
        })
    }
}

/// Whether [`DatabaseEngine::open`] may create the database.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub enum OpenMode {
    /// The database must already exist.
    #[default]
    Existing,
    /// The database must not exist yet; it is created empty.
    Create,
}

/// The error an engine returns.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum EngineError {
    /// The database doesn't exist.
    #[error("database does not exist")]
    NotFound,
    /// The database already exists.
    #[error("database already exists")]
    AlreadyExists,
    /// A statement failed, and its transaction rolled back.
    #[error("statement failed: {message}")]
    Statement {
        /// The engine's description of the failure.
        message: String,
    },
    /// A statement began, committed, or rolled back a transaction itself.
    #[error("statements must not control transactions")]
    TransactionControl,
    /// The call's [`Interrupt`] stopped it before it finished; a write transaction rolled back
    /// (FR-078).
    #[error("statement interrupted: {0}")]
    Interrupted(Interruption),
    /// A position isn't one this engine handed out for this log.
    #[error("position {position} does not fit the log")]
    InvalidPosition {
        /// The position.
        position: Position,
    },
    /// A replication payload is malformed or doesn't continue the copy.
    #[error("invalid replication payload")]
    InvalidPayload(#[source] Box<dyn Error + Send + Sync>),
    /// A checkpoint couldn't complete.
    #[error("checkpoint could not complete")]
    CheckpointBlocked,
    /// Storage failed.
    #[error("storage failed")]
    Storage(#[source] Box<dyn Error + Send + Sync>),
    /// The engine doesn't offer the operation.
    #[error("the engine doesn't support {what}")]
    Unsupported {
        /// What was asked for.
        what: &'static str,
    },
    /// The recorded row changes after a position are no longer all held: some were released
    /// and pruned, or were never recorded (ADR-0045, point 4).
    #[error("the recorded changes {} are no longer held", ChangesAfter(position))]
    ChangesReleased {
        /// The position asked for.
        position: Position,
        /// The oldest position the engine holds every recorded change after, or `None` if it
        /// holds none from before the log's current end.
        oldest: Option<Position>,
    },
}
