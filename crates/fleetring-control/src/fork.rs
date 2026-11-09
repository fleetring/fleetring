//! Forks in control state (ADR-0046): the fork record and its identity, operations, what
//! replicas report about forks, and the driver that moves forks on.
//!
//! A fork is a new database that copies another, its source. [`Action::ForkDatabase`] creates
//! it with a new [`Incarnation`](crate::Incarnation), [`ForkState::Seeding`] at attempt 1. Its
//! replicas then stage the copy, and report it in heartbeats; [`Action::CompleteFork`] makes it
//! [`ForkState::Seeded`] once a write quorum holds the same staged copy, and only then may it
//! take a lease, first on one of those holders. Its primary runs the fork-reset write, and
//! [`Action::MarkForkReady`] records it as [`ForkState::Ready`]. [`Action::RetryForkSeed`]
//! starts another attempt with a new seeder, and [`Action::FailFork`] fails it for good.
//! `DropDatabase` cancels it.
//!
//! Every command after the creation carries the fork's [`ForkIdentity`], and `apply` refuses one
//! that doesn't name the current fork before any other check. [`actions`] proposes these
//! commands from the state alone.
//!
//! [`Action::ForkDatabase`]: crate::Action::ForkDatabase
//! [`Action::CompleteFork`]: crate::Action::CompleteFork
//! [`Action::MarkForkReady`]: crate::Action::MarkForkReady
//! [`Action::RetryForkSeed`]: crate::Action::RetryForkSeed
//! [`Action::FailFork`]: crate::Action::FailFork

pub(crate) mod apply;
mod driver;
mod identity;
mod operation;
mod record;
mod report;
pub(crate) mod rules;

pub use driver::actions;
pub use identity::{ChainHash, ForkIdentity, OperationId, ParseOperationIdError};
pub use operation::{
    CancelCause, ForkExpiry, ForkIntent, IntentField, MAX_FORK_OPERATIONS, OperationOutcome,
    OperationRecord,
};
pub use record::{Fork, ForkFailure, ForkLimits, ForkState, RetryCause};
pub use report::{
    CopyLossReason, CopyLost, ResetReport, SeedFailure, SeedFailureReason, StagedCopy,
};
