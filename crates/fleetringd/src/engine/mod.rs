//! Storage engines behind the [`DatabaseEngine`] trait (ADR-0015, ADR-0016).
//!
//! Everything above this module sees only opaque [`Position`]s, which compare bytewise in log
//! order, and opaque [`Payload`]s. An engine is either a frame source, whose committed batches
//! `fleetringd` ships to followers, or self-replicating; callers ask through
//! [`DatabaseEngine::capability`] and [`Database::capability`].
//!
//! - [`SqliteEngine`] is the frame source for plain SQLite. WAL details stay inside it, and user
//!   statements are confined to the database they name.
//! - [`MemoryEngine`] is a self-replicating test engine that has no WAL at all.
//!
//! A statement stops when its [`Interrupt`] is raised. The caller raises it, at a deadline it
//! keeps on its own clock or when it goes away, so engines never read a clock (FR-078).

mod adapter;
mod changes;
mod config;
mod interrupt;
mod memory;
mod pinned;
mod position;
mod schema;
mod sqlite;
mod statement;

pub use adapter::{
    BatchTransaction, Database, DatabaseCapability, DatabaseEngine, EngineCapability, EngineError,
    EngineKind, FrameFollower, FrameSource, FrameSourceEngine, LogicalChanges, OpenMode,
    SelfReplicating, Shape, SinkMarks,
};
pub use changes::{
    CaptureMode, ChangesAfter, Column, LogicalSupport, ParseCaptureModeError, ReadLimit,
    RecordedTransaction, RowChange, RowOperation,
};
pub use config::{EngineConfig, Execution};
pub use interrupt::{Interrupt, Interruption};
pub use memory::MemoryEngine;
pub use pinned::{PinnedSnapshot, SnapshotFile, SnapshotGuard};
pub use position::{
    CheckpointMarker, DatabaseSnapshot, FrameBatch, Payload, Position, ReplicaPosition,
};
pub use schema::{
    Internal, MAX_COUNT_LIMIT, MAX_SCHEMA_OBJECTS, ObjectKind, RowCount, Schema, SchemaObject,
    SchemaRequest,
};
pub use sqlite::{InvalidPageSizeError, SqliteEngine, SqliteFollower};
pub use statement::{Executed, Rows, Statement, Transaction, Value};

const _: () = {
    const fn assert_debug_send_sync<T: std::fmt::Debug + Send + Sync>() {}
    const fn assert_debug_send<T: std::fmt::Debug + Send + ?Sized>() {}
    assert_debug_send_sync::<BatchTransaction>();
    assert_debug_send_sync::<CheckpointMarker>();
    assert_debug_send_sync::<CaptureMode>();
    assert_debug_send_sync::<Column>();
    assert_debug_send_sync::<LogicalSupport>();
    assert_debug_send_sync::<ChangesAfter<'static>>();
    assert_debug_send_sync::<ReadLimit>();
    assert_debug_send_sync::<RecordedTransaction>();
    assert_debug_send_sync::<RowChange>();
    assert_debug_send_sync::<RowOperation>();
    assert_debug_send_sync::<EngineConfig>();
    assert_debug_send_sync::<EngineError>();
    assert_debug_send_sync::<EngineKind>();
    assert_debug_send_sync::<Executed>();
    assert_debug_send_sync::<Execution>();
    assert_debug_send_sync::<FrameBatch>();
    assert_debug_send_sync::<InvalidPageSizeError>();
    assert_debug_send_sync::<Internal>();
    assert_debug_send_sync::<Interrupt>();
    assert_debug_send_sync::<Interruption>();
    assert_debug_send_sync::<ObjectKind>();
    assert_debug_send_sync::<RowCount>();
    assert_debug_send_sync::<Schema>();
    assert_debug_send_sync::<SchemaObject>();
    assert_debug_send_sync::<SchemaRequest>();
    assert_debug_send_sync::<MemoryEngine>();
    assert_debug_send_sync::<OpenMode>();
    assert_debug_send_sync::<ParseCaptureModeError>();
    assert_debug_send_sync::<Payload>();
    assert_debug_send_sync::<PinnedSnapshot>();
    assert_debug_send_sync::<Position>();
    assert_debug_send_sync::<ReplicaPosition>();
    assert_debug_send_sync::<Rows>();
    assert_debug_send_sync::<Shape>();
    assert_debug_send_sync::<DatabaseSnapshot>();
    assert_debug_send_sync::<SnapshotFile>();
    assert_debug_send_sync::<SqliteEngine>();
    assert_debug_send_sync::<SqliteFollower>();
    assert_debug_send_sync::<Statement>();
    assert_debug_send_sync::<Transaction>();
    assert_debug_send_sync::<Value>();
    assert_debug_send::<dyn Database>();
    assert_debug_send::<dyn FrameFollower>();
    assert_debug_send::<dyn LogicalChanges>();
    assert_debug_send::<dyn SinkMarks>();
};
