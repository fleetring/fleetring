//! Storage engines behind the [`DatabaseEngine`] trait (ADR-0015, ADR-0016).
//!
//! Everything above this module sees only opaque [`Position`]s, which compare bytewise in log
//! order, and opaque [`Payload`]s. An engine is either a frame source, whose committed batches
//! `fleetringd` ships to followers, or self-replicating; callers ask through
//! [`DatabaseEngine::capability`] and [`Database::capability`].
//!
//! - [`SqliteEngine`] is the frame source for plain SQLite. WAL details stay inside it.
//! - [`MemoryEngine`] is a self-replicating test engine that has no WAL at all.

mod adapter;
mod config;
mod memory;
mod position;
mod sqlite;
mod statement;

pub use adapter::{
    Database, DatabaseCapability, DatabaseEngine, EngineCapability, EngineError, EngineKind,
    FrameFollower, FrameSource, FrameSourceEngine, OpenMode, SelfReplicating, Shape,
};
pub use config::{EngineConfig, Execution};
pub use memory::MemoryEngine;
pub use position::{CheckpointMarker, FrameBatch, Payload, Position, ReplicaPosition, Snapshot};
pub use sqlite::{InvalidPageSizeError, SqliteEngine, SqliteFollower};
pub use statement::{Executed, Rows, Statement, Transaction, Value};

const _: () = {
    const fn assert_debug_send_sync<T: std::fmt::Debug + Send + Sync>() {}
    const fn assert_debug_send<T: std::fmt::Debug + Send + ?Sized>() {}
    assert_debug_send_sync::<CheckpointMarker>();
    assert_debug_send_sync::<EngineConfig>();
    assert_debug_send_sync::<EngineError>();
    assert_debug_send_sync::<EngineKind>();
    assert_debug_send_sync::<Executed>();
    assert_debug_send_sync::<Execution>();
    assert_debug_send_sync::<FrameBatch>();
    assert_debug_send_sync::<InvalidPageSizeError>();
    assert_debug_send_sync::<MemoryEngine>();
    assert_debug_send_sync::<OpenMode>();
    assert_debug_send_sync::<Payload>();
    assert_debug_send_sync::<Position>();
    assert_debug_send_sync::<ReplicaPosition>();
    assert_debug_send_sync::<Rows>();
    assert_debug_send_sync::<Shape>();
    assert_debug_send_sync::<Snapshot>();
    assert_debug_send_sync::<SqliteEngine>();
    assert_debug_send_sync::<SqliteFollower>();
    assert_debug_send_sync::<Statement>();
    assert_debug_send_sync::<Transaction>();
    assert_debug_send_sync::<Value>();
    assert_debug_send::<dyn Database>();
    assert_debug_send::<dyn FrameFollower>();
};
