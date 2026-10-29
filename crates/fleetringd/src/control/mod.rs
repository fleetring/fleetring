//! The control node (ADR-0017): a durable command log, one writer, and the gRPC `Control`
//! service, plus the client data nodes use to reach it.
//!
//! - [`CommandLog`] stores each stamped command in SQLite and makes it durable before it is
//!   applied, snapshots the state periodically, and replays the log at startup.
//! - [`ControlNode`] runs the single writer: it stamps each command with a
//!   [`Clock`](crate::clock::Clock), logs it, applies it with `fleetring_control::apply`, and
//!   publishes the new state. [`ControlHandle`] submits commands and reads the published state.
//! - [`ControlService`] serves the `Control` gRPC service over a handle.
//! - [`ControlClient`] calls the service, retrying and following leader redirects
//!   ([`not_leader`]).
//! - [`Failover`] ticks the state machine on a timer, starts promotions for expired leases, and
//!   tells each promotion's candidate (ADR-0018).
//! - [`NoControl`] is what a node that runs no control plane serves in the service's place: every
//!   call fails with `FAILED_PRECONDITION` and names the control node.

mod absent;
mod client;
mod failover;
mod log;
mod node;
mod redirect;
mod service;

pub use absent::NoControl;
pub use client::ControlClient;
pub use failover::{Failover, FailoverSettings};
pub use log::{CommandLog, LogError, LogIndex, Recovered};
pub use node::{
    Committed, ControlHandle, ControlNode, ControlOptions, OpenError, RecoverySummary, StorageIo,
    SubmitError,
};
pub use redirect::{LEADER_METADATA_KEY, leader_hint, not_leader};
pub use service::{
    ControlService, MISSING_DATABASE_KEY, MembershipStream, missing_database, rejection_status,
};
pub(crate) use service::{member_from, node_to_proto};
