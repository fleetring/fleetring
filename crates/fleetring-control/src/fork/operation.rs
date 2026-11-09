//! Fork operations: the caller's intent a resubmission is compared with, and the record of each
//! operation's resolved state and outcome (ADR-0046, point 5).

use std::fmt;

use fleetring::ReplicationFactor;
use serde::{Deserialize, Serialize};

use crate::fork::{ChainHash, ForkFailure};
use crate::{
    DatabaseName, Description, Durability, Incarnation, Labels, MetadataSpec, Millis, Position,
    PrincipalName, Timestamp,
};

/// The most operation records control state keeps. A new fork past it is refused until `Tick`
/// forgets records past their retention (ADR-0046, point 5.3).
pub const MAX_FORK_OPERATIONS: usize = 10_000;

/// When a fork expires, as the caller asked: never, a span after the fork is made, or at a time.
///
/// A span is part of the caller's intent and is resolved to a time once, when the operation is
/// first applied, so a resubmission of the same span later matches it and never moves the
/// expiry.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum ForkExpiry {
    /// The fork never expires.
    #[default]
    Never,
    /// The fork expires this long after the operation is first applied.
    After(Millis),
    /// The fork expires at this time.
    At(Timestamp),
}

impl ForkExpiry {
    /// Returns whether the fork never expires.
    #[must_use]
    pub fn is_never(&self) -> bool {
        matches!(self, Self::Never)
    }

    /// Returns the expiry this resolves to for an operation first applied at `now`: a span
    /// resolves to `resolved`, the time the control service resolved it to, or else to `now`
    /// plus the span.
    pub(crate) fn resolve(self, now: Timestamp, resolved: Option<Timestamp>) -> Option<Timestamp> {
        match self {
            Self::Never => None,
            Self::After(span) => Some(resolved.unwrap_or_else(|| now.saturating_add(span))),
            Self::At(at) => Some(at),
        }
    }
}

/// What the caller asked for in a fork request, exactly as given: what a resubmission with the
/// same operation ID must repeat (ADR-0046, point 5.2).
///
/// It holds nothing resolved against control state: not the source's incarnation, not an
/// absolute expiry for a relative one, not the fork's incarnation. Those are resolved once, when
/// the operation is first applied, and kept in its [`OperationRecord`].
///
/// # Examples
///
/// ```
/// use fleetring::ReplicationFactor;
/// use fleetring_control::Millis;
/// use fleetring_control::fork::{ForkExpiry, ForkIntent};
///
/// let mut intent = ForkIntent::new("scratch".parse()?, "main".parse()?, ReplicationFactor::ONE);
/// intent.expiry = ForkExpiry::After(Millis::new(3_600_000));
/// assert_eq!(intent.first_difference(&intent.clone()), None);
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ForkIntent {
    /// The fork's name.
    pub name: DatabaseName,
    /// The database to copy, by name.
    pub source: DatabaseName,
    /// The source incarnation the caller requires, if it named one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expect_source_incarnation: Option<Incarnation>,
    /// How many replicas the fork should have.
    pub replication_factor: ReplicationFactor,
    /// The fork's durability settings as given, or `None` for the default:
    /// [`Acks::Quorum`](crate::Acks::Quorum) with the floor at W.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub durability: Option<Durability>,
    /// The fork's description, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<Description>,
    /// The fork's labels.
    #[serde(default, skip_serializing_if = "Labels::is_empty")]
    pub labels: Labels,
    /// When the fork expires.
    #[serde(default, skip_serializing_if = "ForkExpiry::is_never")]
    pub expiry: ForkExpiry,
    /// The principal asking, as the control node names the caller. It is the fork's creator
    /// and the description's author, and only it may resubmit the operation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub requested_by: Option<PrincipalName>,
}

impl ForkIntent {
    /// The intent to fork `source` as `name` with `replication_factor` replicas, and nothing
    /// else: default durability, no metadata, no expiry, no named requester.
    #[must_use]
    pub fn new(
        name: DatabaseName,
        source: DatabaseName,
        replication_factor: ReplicationFactor,
    ) -> Self {
        Self {
            name,
            source,
            expect_source_incarnation: None,
            replication_factor,
            durability: None,
            description: None,
            labels: Labels::new(),
            expiry: ForkExpiry::Never,
            requested_by: None,
        }
    }

    /// Returns the first field, in declaration order, in which `other` asks for something
    /// else, or `None` if it asks for the same. The requester isn't compared: a resubmission
    /// from another principal is refused before its intent is looked at.
    #[must_use]
    pub fn first_difference(&self, other: &Self) -> Option<IntentField> {
        [
            (self.name == other.name, IntentField::Name),
            (self.source == other.source, IntentField::Source),
            (
                self.expect_source_incarnation == other.expect_source_incarnation,
                IntentField::ExpectSourceIncarnation,
            ),
            (
                self.replication_factor == other.replication_factor,
                IntentField::ReplicationFactor,
            ),
            (self.durability == other.durability, IntentField::Durability),
            (
                self.description == other.description,
                IntentField::Description,
            ),
            (self.labels == other.labels, IntentField::Labels),
            (self.expiry == other.expiry, IntentField::Expiry),
        ]
        .into_iter()
        .find_map(|(same, field)| (!same).then_some(field))
    }

    /// Returns the durability settings in effect: as given, or the default.
    #[must_use]
    pub fn resolved_durability(&self) -> Durability {
        self.durability
            .unwrap_or_else(|| Durability::quorum(self.replication_factor))
    }

    /// Returns the metadata the fork is created with, its expiry resolved to `expires_at`.
    pub(crate) fn metadata(&self, expires_at: Option<Timestamp>) -> MetadataSpec {
        MetadataSpec {
            description: self.description.clone(),
            labels: self.labels.clone(),
            expires_at,
            created_by: self.requested_by.clone(),
        }
    }
}

/// A field of a [`ForkIntent`], as a conflict names it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum IntentField {
    /// The fork's name.
    Name,
    /// The source's name.
    Source,
    /// The source incarnation required.
    ExpectSourceIncarnation,
    /// The replication factor.
    ReplicationFactor,
    /// The durability settings.
    Durability,
    /// The description.
    Description,
    /// The labels.
    Labels,
    /// The expiry.
    Expiry,
}

impl fmt::Display for IntentField {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Name => "fork name",
            Self::Source => "source",
            Self::ExpectSourceIncarnation => "expected source incarnation",
            Self::ReplicationFactor => "replication factor",
            Self::Durability => "durability",
            Self::Description => "description",
            Self::Labels => "labels",
            Self::Expiry => "expiry",
        })
    }
}

/// Why a fork that wasn't ready was removed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum CancelCause {
    /// `DropDatabase`: `db drop`, the cancellation path (ADR-0046, point 5.5).
    Dropped,
    /// `ExpireDatabase`: its expiry passed.
    Expired,
}

/// What became of a fork operation (ADR-0046, point 5.3). `Ready`, `Failed`, and `Cancelled`
/// are terminal; the others aren't.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum OperationOutcome {
    /// The fork is seeding, in this attempt.
    Pending {
        /// The current attempt.
        attempt: u32,
    },
    /// A completion quorum holds the copy; the fork's reset hasn't been recorded yet.
    Seeded,
    /// The fork is ready.
    Ready {
        /// The source position the fork holds.
        at: Position,
        /// The copy's chain hash at that position.
        chain_hash: ChainHash,
    },
    /// The fork failed.
    Failed {
        /// Why.
        reason: ForkFailure,
    },
    /// The fork was removed before it was ready.
    Cancelled {
        /// What removed it.
        cause: CancelCause,
    },
}

impl OperationOutcome {
    /// Returns whether the outcome is terminal: `Ready`, `Failed`, or `Cancelled`. Only then
    /// does the record's retention start.
    #[must_use]
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::Ready { .. } | Self::Failed { .. } | Self::Cancelled { .. }
        )
    }
}

/// A fork operation's record: the caller's intent, what it resolved to when first applied, and
/// its outcome.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OperationRecord {
    intent: ForkIntent,
    source_incarnation: Incarnation,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    expires_at: Option<Timestamp>,
    target_incarnation: Incarnation,
    outcome: OperationOutcome,
    updated_at: Timestamp,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    finished_at: Option<Timestamp>,
    retention: Millis,
}

impl OperationRecord {
    pub(crate) fn new(
        intent: ForkIntent,
        source_incarnation: Incarnation,
        expires_at: Option<Timestamp>,
        target_incarnation: Incarnation,
        updated_at: Timestamp,
        retention: Millis,
    ) -> Self {
        Self {
            intent,
            source_incarnation,
            expires_at,
            target_incarnation,
            outcome: OperationOutcome::Pending { attempt: 1 },
            updated_at,
            finished_at: None,
            retention,
        }
    }

    /// Returns what the caller asked for.
    #[must_use]
    pub fn intent(&self) -> &ForkIntent {
        &self.intent
    }

    /// Returns the fork's name.
    #[must_use]
    pub fn target(&self) -> &DatabaseName {
        &self.intent.name
    }

    /// Returns the fork's incarnation.
    #[must_use]
    pub fn target_incarnation(&self) -> Incarnation {
        self.target_incarnation
    }

    /// Returns the source incarnation the operation resolved to when it was first applied.
    #[must_use]
    pub fn source_incarnation(&self) -> Incarnation {
        self.source_incarnation
    }

    /// Returns the expiry the operation resolved to when it was first applied, if any.
    #[must_use]
    pub fn expires_at(&self) -> Option<Timestamp> {
        self.expires_at
    }

    /// Returns what became of the operation.
    #[must_use]
    pub fn outcome(&self) -> &OperationOutcome {
        &self.outcome
    }

    /// Returns when the outcome last changed.
    #[must_use]
    pub fn updated_at(&self) -> Timestamp {
        self.updated_at
    }

    /// Returns when the outcome became terminal, or `None` while it is pending.
    #[must_use]
    pub fn finished_at(&self) -> Option<Timestamp> {
        self.finished_at
    }

    /// Returns when the record is forgotten, or `None` while its outcome is pending: its
    /// terminal outcome's time plus its retention.
    #[must_use]
    pub fn forgotten_at(&self) -> Option<Timestamp> {
        self.finished_at
            .map(|finished| finished.saturating_add(self.retention))
    }

    /// Returns whether the record is past its retention at `at`.
    pub(crate) fn is_forgotten_at(&self, at: Timestamp) -> bool {
        self.forgotten_at().is_some_and(|forgotten| at >= forgotten)
    }

    /// Records `outcome` at `at`. A terminal outcome never changes, so a later one is ignored.
    pub(crate) fn set_outcome(&mut self, outcome: OperationOutcome, at: Timestamp) {
        if self.outcome.is_terminal() {
            return;
        }
        if outcome.is_terminal() {
            self.finished_at = Some(at);
        }
        self.outcome = outcome;
        self.updated_at = at;
    }
}
