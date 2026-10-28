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

mod client;
mod log;
mod node;
mod redirect;
mod service;

pub use client::ControlClient;
pub use log::{CommandLog, LogError, LogIndex, Recovered};
pub use node::{
    Committed, ControlHandle, ControlNode, ControlOptions, OpenError, RecoverySummary, StorageIo,
    SubmitError,
};
pub use redirect::{LEADER_METADATA_KEY, leader_hint, not_leader};
pub use service::{ControlService, MembershipStream, rejection_status};
