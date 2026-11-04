//! The FleetRing daemon (ADR-0015).
//!
//! `fleetringd` keeps each database on a primary with followers, on machines you own. The
//! `fleetring` crate decides where each database should live; the daemon makes it so. One binary
//! runs as a data node, as the control node, or as both.
//!
//! The library exposes the daemon's building blocks so that tests, including the simulation
//! harness (ADR-0020), run the same code as the binary.

mod actor;
pub mod auth;
pub mod changes;
pub mod clock;
mod config;
pub mod control;
mod daemon;
pub mod data;
mod data_dir;
pub mod durability;
pub mod engine;
mod environment;
mod layout;
mod membership;
mod publisher;
mod registry;
pub mod replication;
#[cfg(feature = "simulation")]
pub mod sim;
pub mod tls;
pub mod transport;

pub use actor::{DatabaseError, DatabaseHandle, Stored};
pub use config::{
    Config, ConfigError, ControlSettings, HEARTBEATS_PER_TIMEOUT, LEASE_RENEWALS_PER_DURATION,
    MembershipSettings, NodeConfig, NodeRole, OnShutdown, Plaintext, PrincipalConfig,
    ReplicationConfig, TlsConfig, TokenForwarding,
};
pub use daemon::{Daemon, StartError};
pub use data_dir::{DataDir, DataDirError};
pub use environment::{Environment, Network};
pub use membership::{
    DatabaseStatuses, ManualMembership, Membership, MembershipTasks, MembershipView,
};
pub use publisher::{
    Acknowledged, Acknowledgement, BatchPublisher, CommittedBatch, RecordingPublisher,
    ReplicationError,
};
pub use registry::Registry;

// Public types must stay shareable across threads; a refactor that breaks this fails to compile.
const _: () = {
    const fn assert_debug_send_sync<T: std::fmt::Debug + Send + Sync>() {}
    assert_debug_send_sync::<auth::Action>();
    assert_debug_send_sync::<auth::BearerToken>();
    assert_debug_send_sync::<auth::Binding>();
    assert_debug_send_sync::<auth::BindingDenied>();
    assert_debug_send_sync::<auth::Caller>();
    assert_debug_send_sync::<auth::Credential>();
    assert_debug_send_sync::<auth::Denied>();
    assert_debug_send_sync::<auth::ForNodes>();
    assert_debug_send_sync::<auth::Grant>();
    assert_debug_send_sync::<auth::Needed>();
    assert_debug_send_sync::<auth::Requirement>();
    assert_debug_send_sync::<auth::Rpc>();
    assert_debug_send_sync::<auth::IssueError>();
    assert_debug_send_sync::<auth::ParseScopeError>();
    assert_debug_send_sync::<auth::Scope>();
    assert_debug_send_sync::<auth::Scopes>();
    assert_debug_send_sync::<auth::Target<'static>>();
    assert_debug_send_sync::<auth::TokenError>();
    assert_debug_send_sync::<auth::TokenSigner>();
    assert_debug_send_sync::<auth::TokenVerifier>();
    assert_debug_send_sync::<auth::Unbound>();
    assert_debug_send_sync::<auth::Verified>();
    assert_debug_send_sync::<auth::VerifyingKey>();
    assert_debug_send_sync::<changes::ChangeStreams>();
    assert_debug_send_sync::<clock::SimClock>();
    assert_debug_send_sync::<clock::SystemClock>();
    assert_debug_send_sync::<control::Committed>();
    assert_debug_send_sync::<control::ControlClient>();
    assert_debug_send_sync::<control::ControlHandle>();
    assert_debug_send_sync::<control::ControlNode>();
    assert_debug_send_sync::<control::ControlOptions>();
    assert_debug_send_sync::<control::ControlService>();
    assert_debug_send_sync::<control::LogError>();
    assert_debug_send_sync::<control::NoControl>();
    assert_debug_send_sync::<control::OpenError>();
    assert_debug_send_sync::<control::SubmitError>();
    assert_debug_send_sync::<ControlSettings>();
    assert_debug_send_sync::<Daemon>();
    assert_debug_send_sync::<data::Client>();
    assert_debug_send_sync::<data::DatabaseSpec>();
    assert_debug_send_sync::<data::DurabilityChange>();
    assert_debug_send_sync::<data::LeaseUpdate>();
    assert_debug_send_sync::<data::LeaseUpdates>();
    assert_debug_send_sync::<data::Location>();
    assert_debug_send_sync::<data::NodeStatus>();
    assert_debug_send_sync::<data::ReplicaInfo>();
    assert_debug_send_sync::<data::ServedDatabase>();
    assert_debug_send_sync::<data::Verification>();
    assert_debug_send_sync::<data::Written>();
    assert_debug_send_sync::<Environment>();
    assert_debug_send_sync::<Network>();
    assert_debug_send_sync::<ManualMembership>();
    assert_debug_send_sync::<Membership>();
    assert_debug_send_sync::<MembershipSettings>();
    assert_debug_send_sync::<MembershipTasks>();
    assert_debug_send_sync::<MembershipView>();
    assert_debug_send_sync::<OnShutdown>();
    assert_debug_send_sync::<ReplicationConfig>();
    assert_debug_send_sync::<Plaintext>();
    assert_debug_send_sync::<StartError>();
    assert_debug_send_sync::<Stored>();
    assert_debug_send_sync::<TlsConfig>();
    assert_debug_send_sync::<tls::Accepted>();
    assert_debug_send_sync::<tls::Authority>();
    assert_debug_send_sync::<tls::AuthorityError>();
    assert_debug_send_sync::<tls::FleetName>();
    assert_debug_send_sync::<tls::Conn>();
    assert_debug_send_sync::<tls::HandshakeError>();
    assert_debug_send_sync::<tls::Identity>();
    assert_debug_send_sync::<tls::IdentityError>();
    assert_debug_send_sync::<tls::Issued>();
    assert_debug_send_sync::<tls::NameError>();
    assert_debug_send_sync::<tls::Peer>();
    assert_debug_send_sync::<tls::Principal>();
    assert_debug_send_sync::<tls::Raw>();
    assert_debug_send_sync::<tls::Security>();
    assert_debug_send_sync::<tls::SubjectUri>();
    assert_debug_send_sync::<tls::Validity>();
    assert_debug_send_sync::<transport::Listener>();

    // The log owns a SQLite connection, which moves between threads but isn't shared.
    const fn assert_debug_send<T: std::fmt::Debug + Send>() {}
    assert_debug_send::<control::CommandLog>();
};
