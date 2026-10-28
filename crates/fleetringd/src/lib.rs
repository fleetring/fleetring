//! The FleetRing daemon (ADR-0015).
//!
//! `fleetringd` keeps each database on a primary with followers, on machines you own. The
//! `fleetring` crate decides where each database should live; the daemon makes it so. One binary
//! runs as a data node, as the control node, or as both.
//!
//! The library exposes the daemon's building blocks so that tests, including the simulation
//! harness (ADR-0020), run the same code as the binary.

mod actor;
pub mod clock;
mod config;
pub mod control;
mod daemon;
mod data_dir;
pub mod engine;
mod environment;
mod layout;
mod membership;
mod registry;
#[cfg(feature = "simulation")]
pub mod sim;
mod sink;
pub mod transport;

pub use actor::{DatabaseError, DatabaseHandle};
pub use config::{
    Config, ConfigError, ControlSettings, HEARTBEATS_PER_TIMEOUT, MembershipSettings, NodeConfig,
    Role,
};
pub use daemon::{Daemon, StartError};
pub use data_dir::{DataDir, DataDirError};
pub use environment::{Environment, Network};
pub use membership::{Membership, MembershipTasks, MembershipView};
pub use registry::Registry;
pub use sink::{Acknowledgement, CommittedBatch, RecordingSink, ReplicationError, ReplicationSink};

// Public types must stay shareable across threads; a refactor that breaks this fails to compile.
const _: () = {
    const fn assert_debug_send_sync<T: std::fmt::Debug + Send + Sync>() {}
    assert_debug_send_sync::<clock::SimClock>();
    assert_debug_send_sync::<clock::SystemClock>();
    assert_debug_send_sync::<control::Committed>();
    assert_debug_send_sync::<control::ControlClient>();
    assert_debug_send_sync::<control::ControlHandle>();
    assert_debug_send_sync::<control::ControlNode>();
    assert_debug_send_sync::<control::ControlOptions>();
    assert_debug_send_sync::<control::ControlService>();
    assert_debug_send_sync::<control::LogError>();
    assert_debug_send_sync::<control::OpenError>();
    assert_debug_send_sync::<control::SubmitError>();
    assert_debug_send_sync::<ControlSettings>();
    assert_debug_send_sync::<Daemon>();
    assert_debug_send_sync::<Environment>();
    assert_debug_send_sync::<Network>();
    assert_debug_send_sync::<Membership>();
    assert_debug_send_sync::<MembershipSettings>();
    assert_debug_send_sync::<MembershipTasks>();
    assert_debug_send_sync::<MembershipView>();
    assert_debug_send_sync::<StartError>();

    // The log owns a SQLite connection, which moves between threads but isn't shared.
    const fn assert_debug_send<T: std::fmt::Debug + Send>() {}
    assert_debug_send::<control::CommandLog>();
};
