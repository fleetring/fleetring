//! The `DatabaseEngine` trait and its replication capabilities (ADR-0016).

use std::error::Error;
use std::fmt;
use std::path::Path;

use crate::engine::{
    CheckpointMarker, Executed, FrameBatch, PinnedSnapshot, Position, ReplicaPosition, Rows,
    Snapshot, Statement, Transaction,
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
pub trait Database: fmt::Debug + Send {
    /// Runs `transaction` as one write transaction.
    ///
    /// # Errors
    ///
    /// [`EngineError::Statement`] if a statement fails, after rolling the transaction back, and
    /// [`EngineError::Storage`] if storage fails.
    fn execute(&mut self, transaction: &Transaction) -> Result<Executed, EngineError>;

    /// Runs a read-only query against the latest committed state.
    ///
    /// # Errors
    ///
    /// [`EngineError::Statement`] if the query fails or tries to write.
    fn query(&mut self, statement: &Statement) -> Result<Rows, EngineError>;

    /// Writes a consistent copy of the database into `destination`, a directory that a follower
    /// can later open as its location.
    ///
    /// # Errors
    ///
    /// [`EngineError::Storage`] if the copy can't be written.
    fn snapshot(&mut self, destination: &Path) -> Result<Snapshot, EngineError>;

    /// How this database replicates, with access to the matching operations.
    fn capability(&mut self) -> DatabaseCapability<'_>;

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
}

/// A follower's copy of a frame-source database, which applies the primary's batches.
pub trait FrameFollower: fmt::Debug + Send {
    /// The position just past the last applied transaction, or `None` before the first batch.
    fn position(&self) -> Option<Position>;

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
}
