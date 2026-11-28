//! The control node (ADR-0017): a control group that replicates the control log (ADR-0035,
//! ADR-0040), and the gRPC `Control` service, plus the client data nodes use to reach it.
//!
//! - [`ControlStore`] is `control.db` format 2, the control group's Raft log and control
//!   snapshots, which migrates a format-1 log on open (format 1 is read only for that).
//! - [`ControlNode`] runs the control group: one voter by default, which elects itself at once.
//!   Its leader screens each command in `screen`, which adds the commands that enable a fleet
//!   feature on its first use and refuses, unlogged, what must never be logged; stamps it with a
//!   [`Clock`](crate::clock::Clock) and its node ID; and proposes it. Every member applies the
//!   committed log with `fleetring_control::apply` and publishes the new state. `raft` holds the
//!   group's openraft types, storage, state machine, and proposer. [`ControlHandle`] submits
//!   commands and reads the published state.
//! - [`ControlService`] serves the `Control` gRPC service over a handle, forks included
//!   (ADR-0046): what replicas report about forks, a fork's status in a placement, and a fork
//!   operation's status cross the protocol in `fork`. It also delegates agents' tokens
//!   (ADR-0047): `delegation` resolves a request's child, scopes, and times, and the service
//!   records the delegation before it signs with the control node's issuer pair.
//! - [`ControlClient`] calls the service, retrying and following leader redirects
//!   ([`not_leader`](fleetring_client::redirect::not_leader)).
//! - [`Failover`] ticks the state machine on a timer, starts promotions for expired leases, and
//!   tells each promotion's candidate (ADR-0018), on a ready leader only, after a settle window
//!   (ADR-0040 point 6). Beside it, a sweep of its own drops databases whose expiry has passed
//!   (ADR-0041, point 4).
//! - Drop notices tell the replicas of a database the control node dropped or expired, so that
//!   they delete their copies (ADR-0042).
//! - [`NoControl`] is what a node that runs no control plane serves in the service's place: every
//!   call fails with `FAILED_PRECONDITION` and names the control node.

mod absent;
mod client;
mod delegation;
mod failover;
pub(crate) mod fork;
mod format1;
mod node;
mod notices;
pub mod raft;
mod screen;
mod service;
mod store;
#[cfg(test)]
pub(crate) mod test_support;

use std::path::Path;

pub use absent::NoControl;
pub use client::ControlClient;
pub use failover::{Failover, FailoverSettings};
pub use fork::SeedProgress;
pub(crate) use format1::file_format;
pub use format1::{LogError, LogIndex, Recovered};
pub use node::{
    Bootstrap, Committed, ControlGroup, ControlHandle, ControlNode, ControlOptions,
    GROUP_CHANGE_WAIT, GroupError, GroupMember, GroupStatus, MAX_VOTERS, OpenError,
    RecoverySummary, StorageIo, SubmitError,
};
pub use raft::Readiness;
pub use service::{
    ControlService, DurabilityDefaults, MIN_EXPIRY, MISSING_DATABASE_KEY, MembershipStream,
    missing_database, rejection_status,
};
pub(crate) use service::{member_from, node_to_proto};
/// The control log format this build reads and writes: format 2, the control group's store.
pub(crate) use store::STORE_FORMAT as FORMAT_VERSION;
pub use store::{ControlSnapshot, ControlStore};

/// Recovers a format-1 control log at `path` with format 1's own code, as 0.9.x did: the state
/// after its last command. For the 0.9.0 fixture's tests; this build never writes format 1.
///
/// # Errors
///
/// [`LogError`] if the file can't be opened or recovered.
#[doc(hidden)]
pub fn format1_recover(path: &Path) -> Result<Recovered, LogError> {
    format1::CommandLog::open(path, fleetring_control::ControlConfig::new())?.recover()
}

/// Creates an empty format-1 control log at `path`, as 0.9.x did on a first start. For the
/// migration's tests.
///
/// # Errors
///
/// [`LogError`] if the file can't be created.
#[doc(hidden)]
pub fn format1_create(path: &Path) -> Result<(), LogError> {
    format1::CommandLog::open(path, fleetring_control::ControlConfig::new()).map(drop)
}
