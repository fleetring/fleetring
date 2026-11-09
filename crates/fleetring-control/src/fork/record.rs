//! A fork's record in control state: where it came from, its limits, and its state.

use std::collections::BTreeSet;
use std::fmt;

use fleetring::NodeId;
use serde::{Deserialize, Serialize};

use crate::fork::{ChainHash, OperationId};
use crate::{DatabaseName, Incarnation, LeaseEpoch, Millis, Position, PrincipalName, Timestamp};

/// How many attempts a fork may make, by default.
const DEFAULT_MAX_ATTEMPTS: u32 = 5;

/// How long a fork may stay seeding, by default: one hour.
const DEFAULT_DEADLINE: Millis = Millis::new(60 * 60 * 1_000);

/// How long one attempt may run before the driver starts another, by default: 30 minutes.
const DEFAULT_ATTEMPT_TIMEOUT: Millis = Millis::new(30 * 60 * 1_000);

/// How long a seeder may be down before the driver starts another attempt without it, by
/// default: 2 minutes.
const DEFAULT_SEEDER_TIMEOUT: Millis = Millis::new(2 * 60 * 1_000);

/// How long an operation's record outlives its final outcome, by default: 7 days.
const DEFAULT_OPERATION_RETENTION: Millis = Millis::new(7 * 24 * 60 * 60 * 1_000);

/// A fork's budgets and timeouts (ADR-0046, point 7).
///
/// The control node takes them from its configuration and puts them in the `ForkDatabase`
/// command, so the fork records the limits it was made under and a replay applies the same
/// ones, whatever the configuration is by then.
///
/// # Examples
///
/// ```
/// use fleetring_control::Millis;
/// use fleetring_control::fork::ForkLimits;
///
/// let limits = ForkLimits::default();
/// assert_eq!(limits.max_attempts, 5);
/// assert_eq!(limits.deadline, Millis::new(3_600_000));
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ForkLimits {
    /// The most attempts a fork makes before it fails with
    /// [`ForkFailure::RetriesExhausted`].
    pub max_attempts: u32,
    /// How long after its request a fork may stay seeding before it fails with
    /// [`ForkFailure::DeadlineExceeded`].
    pub deadline: Millis,
    /// How long one attempt may run before the driver starts the next.
    pub attempt_timeout: Millis,
    /// How long an attempt's seeder may be down before the driver starts the next attempt
    /// without it.
    pub seeder_timeout: Millis,
    /// How long the operation's record, and so its deduplication, outlives its final outcome.
    pub operation_retention: Millis,
    /// The free bytes a replica must keep beyond the fork's copy for the control plane's
    /// best-effort disk admission to count it (ADR-0046, point 7.3), or `None` for the default,
    /// 1 GiB. The proposing node's `[fork] admission_min_free_bytes`, so that a dev node, which
    /// keeps a small margin, admits as its data side does.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub admission_min_free_bytes: Option<u64>,
}

impl ForkLimits {
    /// The defaults of ADR-0046, point 7: 5 attempts within an hour, 30 minutes an attempt, a
    /// new seeder after 2 minutes down, and records kept for 7 days.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            max_attempts: DEFAULT_MAX_ATTEMPTS,
            deadline: DEFAULT_DEADLINE,
            attempt_timeout: DEFAULT_ATTEMPT_TIMEOUT,
            seeder_timeout: DEFAULT_SEEDER_TIMEOUT,
            operation_retention: DEFAULT_OPERATION_RETENTION,
            admission_min_free_bytes: None,
        }
    }

    /// The free bytes a replica must keep beyond the fork's copy for the control plane's disk
    /// admission to count it: [`admission_min_free_bytes`](Self::admission_min_free_bytes), or
    /// 1 GiB by default.
    #[must_use]
    pub fn admission_floor(&self) -> u64 {
        self.admission_min_free_bytes
            .unwrap_or(super::rules::ADMISSION_HEADROOM)
    }
}

impl Default for ForkLimits {
    fn default() -> Self {
        Self::new()
    }
}

/// A database made by a fork: where it came from, who asked, under which limits, and how far it
/// has got (ADR-0046, point 2).
///
/// The record stays for the database's life: once the fork is ready it is the provenance that
/// `forked_from` shows. It grants nothing.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Fork {
    operation: OperationId,
    source: DatabaseName,
    source_incarnation: Incarnation,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    requested_by: Option<PrincipalName>,
    requested_at: Timestamp,
    #[serde(default, skip_serializing_if = "is_zero")]
    source_bytes: u64,
    limits: ForkLimits,
    state: ForkState,
}

fn is_zero(value: &u64) -> bool {
    *value == 0
}

impl Fork {
    pub(crate) fn new(
        operation: OperationId,
        source: DatabaseName,
        source_incarnation: Incarnation,
        requested_by: Option<PrincipalName>,
        requested_at: Timestamp,
        limits: ForkLimits,
        state: ForkState,
    ) -> Self {
        Self {
            operation,
            source,
            source_incarnation,
            requested_by,
            requested_at,
            source_bytes: 0,
            limits,
            state,
        }
    }

    /// Records the source's size as its replicas reported it when the fork was requested.
    #[must_use]
    pub(crate) fn with_source_bytes(mut self, source_bytes: u64) -> Self {
        self.source_bytes = source_bytes;
        self
    }

    /// Returns the operation that made the fork.
    #[must_use]
    pub fn operation(&self) -> OperationId {
        self.operation
    }

    /// Returns the database the fork copies.
    #[must_use]
    pub fn source(&self) -> &DatabaseName {
        &self.source
    }

    /// Returns the incarnation of the source the fork copies. A source dropped or recreated
    /// since is a different database, and the fork never copies it.
    #[must_use]
    pub fn source_incarnation(&self) -> Incarnation {
        self.source_incarnation
    }

    /// Returns the principal that asked for the fork, if the caller had a name. It grants
    /// nothing.
    #[must_use]
    pub fn requested_by(&self) -> Option<&PrincipalName> {
        self.requested_by.as_ref()
    }

    /// Returns when the fork was requested.
    #[must_use]
    pub fn requested_at(&self) -> Timestamp {
        self.requested_at
    }

    /// Returns the source's size in bytes as its replicas last reported it before the fork was
    /// requested, or 0 if none had. Control-plane admission reserves it on each replica while
    /// the fork is seeding.
    #[must_use]
    pub fn source_bytes(&self) -> u64 {
        self.source_bytes
    }

    /// Returns the limits the fork was made under.
    #[must_use]
    pub fn limits(&self) -> ForkLimits {
        self.limits
    }

    /// Returns how far the fork has got.
    #[must_use]
    pub fn state(&self) -> &ForkState {
        &self.state
    }

    pub(crate) fn set_state(&mut self, state: ForkState) {
        self.state = state;
    }

    /// Returns whether a completion quorum holds the copy: [`ForkState::Seeded`] or
    /// [`ForkState::Ready`].
    #[must_use]
    pub fn is_seeded(&self) -> bool {
        matches!(
            self.state,
            ForkState::Seeded { .. } | ForkState::Ready { .. }
        )
    }

    /// Returns whether the fork's reset is committed: [`ForkState::Ready`].
    #[must_use]
    pub fn is_ready(&self) -> bool {
        matches!(self.state, ForkState::Ready { .. })
    }
}

/// How far a fork has got (ADR-0046, points 2 and 3).
///
/// `Seeding` → `Seeded` → `Ready`, or `Seeding` → `Failed`; a `Seeded` fork whose holders fall
/// below its completion quorum seeds again. A seeding fork takes no lease; a seeded one takes
/// its first lease only on a holder that still reports the copy, keeps its holders as its
/// replica set, and serves no client until it is ready.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum ForkState {
    /// An attempt is copying the source: its seeder fetches a snapshot from the source's
    /// primary, and the other replicas copy from the seeder.
    Seeding {
        /// The attempt, from 1.
        attempt: u32,
        /// The replica that fetches from the source in this attempt.
        seeder: NodeId,
        /// Replicas passed over as seeders because they failed as one: a preference, not a
        /// ban. When every live replica that didn't refuse on disk is here, they are chosen
        /// again, in preference order.
        #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
        seeder_excluded: BTreeSet<NodeId>,
        /// Replicas that refused the copy for want of disk. They are never seeder or holder,
        /// and only they count toward [`ForkFailure::InsufficientDisk`].
        #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
        disk_refused: BTreeSet<NodeId>,
        /// When this attempt started.
        attempt_started_at: Timestamp,
        /// When the fork fails if it is still seeding.
        deadline: Timestamp,
        /// Why the last attempt ended, if this isn't the first: the cause a failure at the
        /// deadline keeps.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        last_cause: Option<RetryCause>,
    },
    /// A completion quorum of the fork's replicas durably holds the same staged copy. Until the
    /// fork is ready, the holders are its replica set, whatever placement says. It is not ready.
    Seeded {
        /// The attempt that fetched the copy.
        attempt: u32,
        /// The source position the copy holds.
        at: Position,
        /// The copy's chain hash at that position.
        chain_hash: ChainHash,
        /// The replicas that reported the staged copy.
        holders: BTreeSet<NodeId>,
        /// The database's epoch when the fork was seeded: while it still is, no lease has been
        /// issued since, and the next lease is the first.
        lease_from: LeaseEpoch,
        /// When the fork was recorded as seeded.
        completed_at: Timestamp,
    },
    /// The fork's primary committed the fork-reset write at the bootstrap quorum. Clients,
    /// `db watch`, and sinks are served; from here on it is an ordinary database, and its
    /// replica set follows placement.
    Ready {
        /// The source position the copy holds.
        at: Position,
        /// The copy's chain hash at that position.
        chain_hash: ChainHash,
        /// When the fork was recorded as seeded.
        completed_at: Timestamp,
        /// The lease epoch whose primary committed the reset.
        reset_epoch: LeaseEpoch,
        /// The position of the reset write in the fork's own log.
        reset_position: Position,
        /// When the fork was recorded as ready.
        ready_at: Timestamp,
    },
    /// The fork failed for good: it is never leased or served, and only `db drop` removes it.
    Failed {
        /// Why.
        reason: ForkFailure,
        /// How many attempts it made.
        attempts: u32,
        /// When it failed.
        failed_at: Timestamp,
    },
}

impl ForkState {
    /// Returns the state's name, as status output shows it.
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Seeding { .. } => "seeding",
            Self::Seeded { .. } => "seeded",
            Self::Ready { .. } => "ready",
            Self::Failed { .. } => "failed",
        }
    }
}

/// Why a fork failed (ADR-0046, point 2).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum ForkFailure {
    /// The source was dropped while the fork was seeding.
    SourceGone,
    /// The source was dropped and recreated while the fork was seeding: a different database.
    SourceRecreated,
    /// The fork used every attempt its limits allow.
    RetriesExhausted {
        /// Why the last attempt ended.
        last: RetryCause,
    },
    /// The fork was still seeding at its deadline.
    DeadlineExceeded {
        /// Why the last attempt ended, or the attempt timeout if the first was still running.
        last: RetryCause,
    },
    /// The source can't be copied within the source's pin limits.
    SourceTooLarge,
    /// Fewer replicas than the fork's completion quorum have room for the copy: only replicas
    /// that refused for want of disk count against it, never seeders that timed out.
    InsufficientDisk,
    /// The source's primary refused to seed it for its engine.
    EngineUnsupported,
}

impl fmt::Display for ForkFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SourceGone => f.write_str("the source was dropped"),
            Self::SourceRecreated => f.write_str("the source was dropped and recreated"),
            Self::RetriesExhausted { last } => {
                write!(f, "every attempt failed; the last because {last}")
            }
            Self::DeadlineExceeded { last } => {
                write!(f, "the fork was still seeding at its deadline; {last}")
            }
            Self::SourceTooLarge => f.write_str("the source is too large to copy"),
            Self::InsufficientDisk => f.write_str("too few replicas have room for the copy"),
            Self::EngineUnsupported => f.write_str("the source's engine can't fork"),
        }
    }
}

/// Why an attempt ended and another began, kept as the real cause of a failure (ADR-0046,
/// point 2).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum RetryCause {
    /// The seeder was down for the seeder timeout, or left the fleet.
    SeederDown,
    /// The attempt ran for the attempt timeout.
    AttemptTimedOut,
    /// The seeder's transfer broke after its own retries.
    TransferFailed,
    /// The source's pin expired before the transfer finished.
    PinExpired,
    /// A holder of a seeded fork lost its staged copy before the first lease.
    HolderLost,
    /// Holders of a seeded fork left the fleet.
    HoldersLeft,
    /// The seeder refused the copy for want of disk.
    SeederDiskRefused,
}

impl fmt::Display for RetryCause {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::SeederDown => "the seeder was down",
            Self::AttemptTimedOut => "the attempt timed out",
            Self::TransferFailed => "the seeder's transfer failed",
            Self::PinExpired => "the source's pin expired",
            Self::HolderLost => "a holder lost its copy",
            Self::HoldersLeft => "holders left the fleet",
            Self::SeederDiskRefused => "the seeder had no room for the copy",
        })
    }
}
