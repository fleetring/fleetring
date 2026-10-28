//! The FleetRing control plane's state machine (ADR-0017).
//!
//! Every change to control state is a command, applied in order by one pure function. The crate
//! does no I/O and reads no clock: each command carries the time the control node stamped on it,
//! so replaying a command log anywhere gives the same state. That is what lets the control plane
//! move from one node to Raft without changing its logic.
//!
//! - [`State`] holds the members with their placement attributes and liveness, the membership
//!   epoch, and every database with its engine, epoch, lease, and any promotion in progress.
//! - [`Command`] is a stamped [`Action`]: join, leave, heartbeat, create a database, acquire or
//!   renew a lease, start a promotion, record a fenced member, or complete a promotion.
//! - [`apply`] checks a command against the rules of ADR-0017 and ADR-0018 and returns an
//!   [`Outcome`]: what changed, or a [`Rejection`] that left the state untouched.
//!
//! # Rules
//!
//! - **Time** comes only from command stamps. A stamp earlier than the last accepted one counts as
//!   the last accepted one.
//! - **Liveness:** a member is down once the time passes its last heartbeat plus the heartbeat
//!   timeout. Down members stay in the ring; liveness only filters lease and promotion candidates.
//! - **Membership epoch** grows on every change to the members or their placement attributes.
//! - **Replica sets** are the ring's replica sets over all members. With R the replica set's size,
//!   W = ⌊R/2⌋ + 1 and Q = R − W + 1.
//! - **Epochs** only grow. A database's first lease is granted at epoch 1; every later holder
//!   comes from a promotion, which may start only after the current lease has expired plus the
//!   failover margin, issues the next epoch, and completes only once Q replica-set members, the
//!   candidate included, are fenced at it.
//! - **Candidates** are live replica-set members. A promotion picks the one that reported the most
//!   advanced position, preferring the earliest in the ring's preference order on ties.
//! - **Positions** are opaque bytes compared lexicographically (ADR-0016).
//!
//! # Examples
//!
//! ```
//! use fleetring::ReplicationFactor;
//! use fleetring_control::{
//!     apply, Action, Command, ControlConfig, Engine, Member, Rejection, State, Timestamp,
//! };
//!
//! let mut state = State::new(ControlConfig::new());
//! let at = Timestamp::from_millis(0);
//! for id in ["a", "b", "c"] {
//!     apply(&mut state, Command::new(at, Action::Join { member: Member::new(id.parse()?) }))?;
//! }
//! let db = "tenant:acme".parse()?;
//! apply(&mut state, Command::new(at, Action::CreateDatabase {
//!     name: db,
//!     replication_factor: ReplicationFactor::new(3).ok_or("zero")?,
//!     engine: Engine::Sqlite,
//! }))?;
//! let quorums = state.quorums(&"tenant:acme".parse()?)?;
//! assert_eq!((quorums.write(), quorums.promotion()), (2, 2));
//!
//! // A rejected command leaves the state as it was.
//! let before = state.clone();
//! let again = apply(&mut state, Command::new(at, Action::Leave { id: "z".parse()? }));
//! assert!(matches!(again, Err(Rejection::UnknownMember { .. })));
//! assert_eq!(state, before);
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```
#![deny(clippy::float_arithmetic)]

mod apply;
mod command;
mod config;
mod database;
mod member;
mod state;
mod time;

pub use apply::{Applied, JoinKind, Outcome, Rejection, apply};
pub use command::{Action, Command};
pub use config::ControlConfig;
pub use database::{
    Database, DatabaseName, Engine, Lease, LeaseEpoch, ParseDatabaseNameError, Position, Promotion,
    Quorums,
};
pub use member::{HostedReplica, Liveness, Member, Report, Role};
pub use state::State;
pub use time::{Millis, Timestamp};

// Public types must stay shareable across threads; a refactor that breaks this fails to compile.
const _: () = {
    const fn assert_debug_send_sync<T: std::fmt::Debug + Send + Sync>() {}
    assert_debug_send_sync::<Action>();
    assert_debug_send_sync::<Applied>();
    assert_debug_send_sync::<Command>();
    assert_debug_send_sync::<ControlConfig>();
    assert_debug_send_sync::<Database>();
    assert_debug_send_sync::<DatabaseName>();
    assert_debug_send_sync::<Engine>();
    assert_debug_send_sync::<HostedReplica>();
    assert_debug_send_sync::<JoinKind>();
    assert_debug_send_sync::<Lease>();
    assert_debug_send_sync::<LeaseEpoch>();
    assert_debug_send_sync::<Liveness>();
    assert_debug_send_sync::<Member>();
    assert_debug_send_sync::<Millis>();
    assert_debug_send_sync::<Outcome>();
    assert_debug_send_sync::<ParseDatabaseNameError>();
    assert_debug_send_sync::<Position>();
    assert_debug_send_sync::<Promotion>();
    assert_debug_send_sync::<Quorums>();
    assert_debug_send_sync::<Rejection>();
    assert_debug_send_sync::<Report>();
    assert_debug_send_sync::<Role>();
    assert_debug_send_sync::<State>();
    assert_debug_send_sync::<Timestamp>();
};
