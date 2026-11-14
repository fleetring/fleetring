//! The control node (ADR-0017): a durable command log, one writer, and the gRPC `Control`
//! service, plus the client data nodes use to reach it.
//!
//! - [`CommandLog`] stores each stamped command in SQLite and makes it durable before it is
//!   applied, snapshots the state periodically, and replays the log at startup.
//! - [`ControlNode`] runs the single writer: it stamps each command with a
//!   [`Clock`](crate::clock::Clock), logs it, applies it with `fleetring_control::apply`, and
//!   publishes the new state. [`ControlHandle`] submits commands and reads the published state.
//! - [`ControlService`] serves the `Control` gRPC service over a handle, forks included
//!   (ADR-0046): what replicas report about forks, a fork's status in a placement, and a fork
//!   operation's status cross the protocol in `fork`. It also delegates agents' tokens
//!   (ADR-0047): `delegation` resolves a request's child, scopes, and times, and the service
//!   records the delegation before it signs with the control node's issuer pair.
//! - [`ControlClient`] calls the service, retrying and following leader redirects
//!   ([`not_leader`](fleetring_client::redirect::not_leader)).
//! - [`Failover`] ticks the state machine on a timer, starts promotions for expired leases, and
//!   tells each promotion's candidate (ADR-0018). Beside it, a sweep of its own drops databases
//!   whose expiry has passed (ADR-0041, point 4).
//! - Drop notices tell the replicas of a database the control node dropped or expired, so that
//!   they delete their copies (ADR-0042).
//! - [`NoControl`] is what a node that runs no control plane serves in the service's place: every
//!   call fails with `FAILED_PRECONDITION` and names the control node.

mod absent;
mod client;
mod delegation;
mod failover;
pub(crate) mod fork;
mod log;
mod node;
mod notices;
mod service;
#[cfg(test)]
mod test_support;

pub use absent::NoControl;
pub use client::ControlClient;
pub use failover::{Failover, FailoverSettings};
pub use fork::SeedProgress;
pub(crate) use log::FORMAT_VERSION;
pub use log::{CommandLog, LogError, LogIndex, Recovered};
pub use node::{
    Committed, ControlHandle, ControlNode, ControlOptions, OpenError, RecoverySummary, StorageIo,
    SubmitError,
};
pub use service::{
    ControlService, DurabilityDefaults, MIN_EXPIRY, MISSING_DATABASE_KEY, MembershipStream,
    missing_database, rejection_status,
};
pub(crate) use service::{member_from, node_to_proto};
