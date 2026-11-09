//! Forks in control state (ADR-0046): the fork record and its identity, operations, what
//! replicas report about forks, and the driver that moves forks on.
//!
//! A fork is a new database that copies another, its source. [`Action::ForkDatabase`] creates
//! it with the [`Incarnation`](crate::Incarnation) the command carries, [`ForkState::Seeding`]
//! at attempt 1. Its replicas then stage the copy and report it in heartbeats;
//! [`Action::CompleteFork`] makes it [`ForkState::Seeded`] once a completion quorum holds the
//! same staged copy. Those holders are its replica set until it is ready, and only one that still
//! reports the copy may take its first lease. Its primary runs the fork-reset write, and
//! [`Action::MarkForkReady`] records it as [`ForkState::Ready`] once the bootstrap quorum
//! reports it. [`Action::RetryForkSeed`] starts another attempt with another seeder,
//! [`Action::RecordForkDiskRefusal`] records a replica without room for the copy,
//! [`Action::DropForkHolder`] removes a holder that lost its copy or left, seeding again below
//! the completion quorum, and [`Action::FailFork`] fails it for good. `DropDatabase` and
//! `ExpireDatabase` cancel it before it is ready.
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
//! [`Action::RecordForkDiskRefusal`]: crate::Action::RecordForkDiskRefusal
//! [`Action::DropForkHolder`]: crate::Action::DropForkHolder

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
pub use rules::completion_quorum;
