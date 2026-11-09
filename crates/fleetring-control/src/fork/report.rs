//! What a replica reports about a fork in its heartbeats (ADR-0046, point 4.5).
//!
//! Each report carries the fork identity it is about. The control plane compares it with the
//! current fork's, and a report for another identity counts for nothing.

use std::fmt;

use serde::{Deserialize, Serialize};

use crate::fork::{ChainHash, ForkFailure, ForkIdentity, RetryCause};
use crate::{Incarnation, LeaseEpoch, Position};

/// A replica durably holds a staged copy of a fork: written, verified, synced, and renamed into
/// place.
///
/// # Examples
///
/// ```
/// use fleetring_control::{Incarnation, Position};
/// use fleetring_control::fork::{ChainHash, ForkIdentity, OperationId, StagedCopy};
///
/// let fork = ForkIdentity::new("t".parse()?, Incarnation::new(3), OperationId::from_bytes([1; 16]));
/// let staged = StagedCopy::new(fork, 1, Position::new([9]), ChainHash::new([4]), Incarnation::new(1));
/// assert_eq!(staged.attempt(), 1);
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StagedCopy {
    fork: ForkIdentity,
    attempt: u32,
    position: Position,
    chain_hash: ChainHash,
    source_incarnation: Incarnation,
}

impl StagedCopy {
    /// Describes a staged copy of `fork`, fetched in `attempt`, that holds the source at
    /// `position` with `chain_hash`, from the source's `source_incarnation`.
    #[must_use]
    pub fn new(
        fork: ForkIdentity,
        attempt: u32,
        position: Position,
        chain_hash: ChainHash,
        source_incarnation: Incarnation,
    ) -> Self {
        Self {
            fork,
            attempt,
            position,
            chain_hash,
            source_incarnation,
        }
    }

    /// Returns the fork the copy is of.
    #[must_use]
    pub fn fork(&self) -> &ForkIdentity {
        &self.fork
    }

    /// Returns the attempt that fetched the copy.
    #[must_use]
    pub fn attempt(&self) -> u32 {
        self.attempt
    }

    /// Returns the source position the copy holds.
    #[must_use]
    pub fn position(&self) -> &Position {
        &self.position
    }

    /// Returns the copy's chain hash at that position.
    #[must_use]
    pub fn chain_hash(&self) -> &ChainHash {
        &self.chain_hash
    }

    /// Returns the incarnation of the source the copy came from.
    #[must_use]
    pub fn source_incarnation(&self) -> Incarnation {
        self.source_incarnation
    }
}

/// Why a replica couldn't seed a fork.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum SeedFailureReason {
    /// The replica hasn't room for the copy. The driver records the refusal: the replica is
    /// never seeder or holder for this fork, and the fork fails only when fewer than its
    /// completion quorum of replicas remain with room.
    InsufficientDisk,
    /// The source can't be copied within the source's pin limits. Permanent.
    SourceTooLarge,
    /// The source's primary can't seed the source's engine. Permanent.
    EngineUnsupported,
    /// The transfer broke after the replica's own retries. Worth another attempt.
    TransferFailed,
    /// The source's pin expired before the transfer finished. Worth another attempt.
    PinExpired,
}

impl SeedFailureReason {
    /// Returns whether another attempt may succeed where this one failed.
    #[must_use]
    pub const fn is_retryable(self) -> bool {
        matches!(self, Self::TransferFailed | Self::PinExpired)
    }

    /// Returns why the fork fails if this reason ends it for good, or `None` for a reason that
    /// ends only an attempt, or only this replica's part.
    pub(crate) const fn as_failure(self) -> Option<ForkFailure> {
        match self {
            Self::SourceTooLarge => Some(ForkFailure::SourceTooLarge),
            Self::EngineUnsupported => Some(ForkFailure::EngineUnsupported),
            Self::InsufficientDisk | Self::TransferFailed | Self::PinExpired => None,
        }
    }

    /// Returns the cause of the next attempt when the seeder reports this reason, if it is
    /// worth one.
    pub(crate) const fn as_retry(self) -> Option<RetryCause> {
        match self {
            Self::TransferFailed => Some(RetryCause::TransferFailed),
            Self::PinExpired => Some(RetryCause::PinExpired),
            Self::InsufficientDisk | Self::SourceTooLarge | Self::EngineUnsupported => None,
        }
    }
}

impl fmt::Display for SeedFailureReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::InsufficientDisk => "insufficient disk",
            Self::SourceTooLarge => "source too large",
            Self::EngineUnsupported => "engine unsupported",
            Self::TransferFailed => "transfer failed",
            Self::PinExpired => "pin expired",
        })
    }
}

/// A replica couldn't seed a fork in one attempt.
///
/// # Examples
///
/// ```
/// use fleetring_control::Incarnation;
/// use fleetring_control::fork::{ForkIdentity, OperationId, SeedFailure, SeedFailureReason};
///
/// let fork = ForkIdentity::new("t".parse()?, Incarnation::new(3), OperationId::from_bytes([1; 16]));
/// let failure = SeedFailure::new(fork, 2, SeedFailureReason::InsufficientDisk);
/// assert!(!failure.reason().is_retryable());
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SeedFailure {
    fork: ForkIdentity,
    attempt: u32,
    reason: SeedFailureReason,
}

impl SeedFailure {
    /// Describes a failure to seed `fork` in `attempt`, for `reason`.
    #[must_use]
    pub fn new(fork: ForkIdentity, attempt: u32, reason: SeedFailureReason) -> Self {
        Self {
            fork,
            attempt,
            reason,
        }
    }

    /// Returns the fork the failure is about.
    #[must_use]
    pub fn fork(&self) -> &ForkIdentity {
        &self.fork
    }

    /// Returns the attempt that failed.
    #[must_use]
    pub fn attempt(&self) -> u32 {
        self.attempt
    }

    /// Returns why.
    #[must_use]
    pub fn reason(&self) -> SeedFailureReason {
        self.reason
    }
}

/// Why a holder's staged copy is lost.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum CopyLossReason {
    /// The staged file or its sidecar is missing.
    Missing,
    /// The staged file fails its size, position, or checksum check.
    Damaged,
}

/// A holder found its staged copy of a fork missing or damaged (ADR-0046, points 4.5 and 6.2).
///
/// # Examples
///
/// ```
/// use fleetring_control::Incarnation;
/// use fleetring_control::fork::{CopyLossReason, CopyLost, ForkIdentity, OperationId};
///
/// let fork = ForkIdentity::new("t".parse()?, Incarnation::new(3), OperationId::from_bytes([1; 16]));
/// let lost = CopyLost::new(fork, 1, CopyLossReason::Damaged);
/// assert_eq!(lost.attempt(), 1);
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CopyLost {
    fork: ForkIdentity,
    attempt: u32,
    reason: CopyLossReason,
}

impl CopyLost {
    /// Describes the loss of the staged copy of `fork` fetched in `attempt`, for `reason`.
    #[must_use]
    pub fn new(fork: ForkIdentity, attempt: u32, reason: CopyLossReason) -> Self {
        Self {
            fork,
            attempt,
            reason,
        }
    }

    /// Returns the fork the copy was of.
    #[must_use]
    pub fn fork(&self) -> &ForkIdentity {
        &self.fork
    }

    /// Returns the attempt that fetched the copy.
    #[must_use]
    pub fn attempt(&self) -> u32 {
        self.attempt
    }

    /// Returns why the copy is lost.
    #[must_use]
    pub fn reason(&self) -> CopyLossReason {
        self.reason
    }
}

/// Whether a fork's reset write has committed at the fork's bootstrap quorum.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ResetProgress {
    /// The primary is running the reset.
    Running,
    /// The reset and its marker are committed at the fork's bootstrap quorum.
    Committed,
}

/// A fork's primary reports its fork-reset write (ADR-0046, point 6): running, or committed at
/// the bootstrap quorum with the marker for this identity. Every replica that holds the
/// committed marker reports it, and `MarkForkReady` needs the bootstrap quorum of reports.
///
/// # Examples
///
/// ```
/// use fleetring_control::{Incarnation, LeaseEpoch, Position};
/// use fleetring_control::fork::{ForkIdentity, OperationId, ResetReport};
///
/// let fork = ForkIdentity::new("t".parse()?, Incarnation::new(3), OperationId::from_bytes([1; 16]));
/// let reset = ResetReport::committed(fork, LeaseEpoch::new(1), Position::new([2]));
/// assert!(reset.is_committed());
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResetReport {
    fork: ForkIdentity,
    epoch: LeaseEpoch,
    position: Position,
    progress: ResetProgress,
}

impl ResetReport {
    /// The primary of `fork` at `epoch` is running the reset, which it wrote at `position`.
    #[must_use]
    pub fn running(fork: ForkIdentity, epoch: LeaseEpoch, position: Position) -> Self {
        Self {
            fork,
            epoch,
            position,
            progress: ResetProgress::Running,
        }
    }

    /// The primary of `fork` at `epoch` holds the reset marker for the identity, committed at
    /// the fork's bootstrap quorum at `position`.
    #[must_use]
    pub fn committed(fork: ForkIdentity, epoch: LeaseEpoch, position: Position) -> Self {
        Self {
            fork,
            epoch,
            position,
            progress: ResetProgress::Committed,
        }
    }

    /// Returns the fork the report is about.
    #[must_use]
    pub fn fork(&self) -> &ForkIdentity {
        &self.fork
    }

    /// Returns the lease epoch of the primary that wrote the reset.
    #[must_use]
    pub fn epoch(&self) -> LeaseEpoch {
        self.epoch
    }

    /// Returns the reset write's position in the fork's log.
    #[must_use]
    pub fn position(&self) -> &Position {
        &self.position
    }

    /// Returns whether the reset is committed at the fork's bootstrap quorum.
    #[must_use]
    pub fn is_committed(&self) -> bool {
        self.progress == ResetProgress::Committed
    }
}

/// A ready fork's primary reports that placement's write quorum holds its whole log, and that
/// it counts both its recorded holders' and placement's write quorums toward every write from
/// now on (ADR-0046, point 3.10, as built). `ReleaseForkHolders` needs this report from the
/// lease holder at the current epoch before the fork's replica set leaves its holders.
///
/// # Examples
///
/// ```
/// use fleetring_control::{Incarnation, LeaseEpoch};
/// use fleetring_control::fork::{ForkIdentity, OperationId, PlacementCaughtUp};
///
/// let fork = ForkIdentity::new("t".parse()?, Incarnation::new(3), OperationId::from_bytes([1; 16]));
/// let caught_up = PlacementCaughtUp::new(fork, LeaseEpoch::new(2));
/// assert_eq!(caught_up.epoch(), LeaseEpoch::new(2));
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlacementCaughtUp {
    fork: ForkIdentity,
    epoch: LeaseEpoch,
}

impl PlacementCaughtUp {
    /// The primary of `fork` at `epoch` has placement's write quorum holding its log.
    #[must_use]
    pub fn new(fork: ForkIdentity, epoch: LeaseEpoch) -> Self {
        Self { fork, epoch }
    }

    /// Returns the fork the report is about.
    #[must_use]
    pub fn fork(&self) -> &ForkIdentity {
        &self.fork
    }

    /// Returns the lease epoch of the primary that reports it.
    #[must_use]
    pub fn epoch(&self) -> LeaseEpoch {
        self.epoch
    }
}
