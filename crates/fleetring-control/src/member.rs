//! Fleet members: placement attributes, liveness, and what each member reports.

use fleetring::{Host, Node, NodeId, Weight, Zone};
use serde::{Deserialize, Serialize};

use crate::ReplicasCaughtUp;
use crate::fork::{CopyLost, PlacementCaughtUp, ResetReport, SeedFailure, StagedCopy};
use crate::{DatabaseName, LeaseEpoch, Position, Software, Timestamp};

/// A member's placement attributes, the ID, zone, host, and weight that the ring places by, and
/// the address its peers reach it at.
///
/// The address isn't a placement attribute: [`Member::to_node`] leaves it out, so it never moves
/// a key. It is still part of the membership, so a member that rejoins with a new address bumps
/// the membership epoch, and every node learns the address with the next membership update. It
/// arrives inside the `Join` command, so replaying the log restores it.
///
/// The [`Software`] the member runs is recorded with it too, but unlike the address it never
/// changes the membership epoch: a control node of a release that ignores it, such as 0.3.0
/// after a rollback, must replay the log to the same epoch (ADR-0038). The control node sends a
/// membership update when it changes all the same. A member that reported none runs
/// [`Software::baseline`].
///
/// # Examples
///
/// ```
/// use fleetring::Weight;
/// use fleetring_control::Member;
///
/// let member = Member::new("nas".parse()?)
///     .with_zone("garage".parse()?)
///     .with_weight(Weight::new(2).expect("2 is nonzero"))
///     .with_address("10.0.0.7:7400");
/// let node = member.to_node();
/// assert_eq!(node.id().as_str(), "nas");
/// assert_eq!(node.weight().get(), 2);
/// assert_eq!(member.address(), Some("10.0.0.7:7400"));
/// # Ok::<(), fleetring::ParseLabelError>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Member {
    id: NodeId,
    zone: Option<Zone>,
    host: Option<Host>,
    weight: Weight,
    // Absent from states and logs written before members had addresses.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    address: Option<String>,
    // Absent from states and logs written before members reported their software.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    software: Option<Software>,
}

impl Member {
    /// Creates a member of weight 1 in the default zone, on a host of its own.
    #[must_use]
    pub fn new(id: NodeId) -> Self {
        Self {
            id,
            zone: None,
            host: None,
            weight: Weight::ONE,
            address: None,
            software: None,
        }
    }

    /// Places the member in `zone`.
    #[must_use]
    pub fn with_zone(mut self, zone: Zone) -> Self {
        self.zone = Some(zone);
        self
    }

    /// Places the member on `host`.
    #[must_use]
    pub fn with_host(mut self, host: Host) -> Self {
        self.host = Some(host);
        self
    }

    /// Sets the member's weight.
    #[must_use]
    pub fn with_weight(mut self, weight: Weight) -> Self {
        self.weight = weight;
        self
    }

    /// Sets the address peers reach the member at, as `host:port`.
    #[must_use]
    pub fn with_address(mut self, address: impl Into<String>) -> Self {
        self.address = Some(address.into());
        self
    }

    /// Returns whether `other` has the same ID, placement attributes, and address: everything
    /// but the software, which never changes the membership epoch.
    #[must_use]
    pub fn same_attributes(&self, other: &Self) -> bool {
        self.id == other.id
            && self.zone == other.zone
            && self.host == other.host
            && self.weight == other.weight
            && self.address == other.address
    }

    /// Records the software the member runs.
    #[must_use]
    pub fn with_software(mut self, software: Software) -> Self {
        self.software = Some(software);
        self
    }

    /// Returns the software the member reported, or `None` if it reported none, as 0.3.0 nodes
    /// don't.
    #[must_use]
    pub fn software(&self) -> Option<&Software> {
        self.software.as_ref()
    }

    /// Returns the software the member runs: what it reported, or [`Software::baseline`] if it
    /// reported nothing.
    #[must_use]
    pub fn running(&self) -> Software {
        self.software.clone().unwrap_or_else(Software::baseline)
    }

    /// Returns the member's ID.
    #[must_use]
    pub fn id(&self) -> &NodeId {
        &self.id
    }

    /// Returns the member's zone, or `None` for the default zone.
    #[must_use]
    pub fn zone(&self) -> Option<&Zone> {
        self.zone.as_ref()
    }

    /// Returns the member's host label, or `None` if it is its own host.
    #[must_use]
    pub fn host(&self) -> Option<&Host> {
        self.host.as_ref()
    }

    /// Returns the member's weight.
    #[must_use]
    pub fn weight(&self) -> Weight {
        self.weight
    }

    /// Returns the address peers reach the member at, as `host:port`, if it advertised one.
    #[must_use]
    pub fn address(&self) -> Option<&str> {
        self.address.as_deref()
    }

    /// Returns the ring node with the member's placement attributes.
    #[must_use]
    pub fn to_node(&self) -> Node {
        let mut node = Node::new(self.id.clone()).with_weight(self.weight);
        if let Some(zone) = &self.zone {
            node = node.with_zone(zone.clone());
        }
        if let Some(host) = &self.host {
            node = node.with_host(host.clone());
        }
        node
    }
}

impl From<&Node> for Member {
    fn from(node: &Node) -> Self {
        Self {
            id: node.id().clone(),
            zone: node.zone().cloned(),
            host: node.host().cloned(),
            weight: node.weight(),
            address: None,
            software: None,
        }
    }
}

/// Whether a member has sent a heartbeat recently enough to count as live.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Liveness {
    /// The member's last heartbeat is within the heartbeat timeout.
    Live,
    /// The heartbeat timeout has passed since the member's last heartbeat.
    Down,
}

/// The role a member says it plays for a database it hosts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
#[non_exhaustive]
pub enum ReplicaRole {
    /// The member accepts writes for the database.
    Primary,
    /// The member stores the database's log as a standby.
    Follower,
}

/// One database a member hosts, as reported in a heartbeat.
///
/// Besides its role and position, a replica may report its size and, for a fork, its staged
/// copy, a failure to seed, or its reset write (ADR-0046, point 4.5). Those are recorded only
/// once the `fork` feature is enabled, so that control state stays as it was until then.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HostedReplica {
    database: DatabaseName,
    role: ReplicaRole,
    position: Position,
    // Absent from logs written before replicas reported it.
    #[serde(default, skip_serializing_if = "is_zero")]
    log_epoch: LeaseEpoch,
    // Absent from logs and states written before forks existed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    size_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    staged: Option<StagedCopy>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    copy_lost: Option<CopyLost>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    seed_failure: Option<SeedFailure>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    reset: Option<ResetReport>,
    // Absent from logs and states written before ready forks released their holders.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    placement_caught_up: Option<PlacementCaughtUp>,
    // Absent from logs and states written before membership changes kept replica sets.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    replicas_caught_up: Option<ReplicasCaughtUp>,
}

fn is_zero(epoch: &LeaseEpoch) -> bool {
    *epoch == LeaseEpoch::ZERO
}

impl HostedReplica {
    /// Describes a replica of `database` that plays `role` and has stored the log up to
    /// `position`.
    #[must_use]
    pub fn new(database: DatabaseName, role: ReplicaRole, position: Position) -> Self {
        Self {
            database,
            role,
            position,
            log_epoch: LeaseEpoch::ZERO,
            size_bytes: None,
            staged: None,
            copy_lost: None,
            seed_failure: None,
            reset: None,
            placement_caught_up: None,
            replicas_caught_up: None,
        }
    }

    /// Says that the last frames of the replica's log came from the primary of `log_epoch`.
    ///
    /// Promotions rank candidates by this epoch before their positions, because positions on
    /// logs written by different primaries don't compare (ADR-0018). A replica that doesn't
    /// report it counts as epoch 0.
    #[must_use]
    pub fn with_log_epoch(mut self, log_epoch: LeaseEpoch) -> Self {
        self.log_epoch = log_epoch;
        self
    }

    /// Says how many bytes the replica's copy takes, for fork admission (ADR-0046, point 7.3).
    #[must_use]
    pub fn with_size_bytes(mut self, size_bytes: u64) -> Self {
        self.size_bytes = Some(size_bytes);
        self
    }

    /// Says that the replica durably holds a staged copy of a fork (ADR-0046, point 4.3).
    #[must_use]
    pub fn with_staged(mut self, staged: StagedCopy) -> Self {
        self.staged = Some(staged);
        self
    }

    /// Says that the replica's staged copy of a fork is missing or damaged (ADR-0046, point
    /// 4.5).
    #[must_use]
    pub fn with_copy_lost(mut self, copy_lost: CopyLost) -> Self {
        self.copy_lost = Some(copy_lost);
        self
    }

    /// Says that the replica couldn't seed a fork (ADR-0046, point 4.5).
    #[must_use]
    pub fn with_seed_failure(mut self, seed_failure: SeedFailure) -> Self {
        self.seed_failure = Some(seed_failure);
        self
    }

    /// Says how the replica's reset write of a fork stands (ADR-0046, point 6).
    #[must_use]
    pub fn with_reset(mut self, reset: ResetReport) -> Self {
        self.reset = Some(reset);
        self
    }

    /// Says that the replica, a ready fork's primary, has placement's write quorum holding its
    /// log (ADR-0046, point 3.10, as built).
    #[must_use]
    pub fn with_placement_caught_up(mut self, caught_up: PlacementCaughtUp) -> Self {
        self.placement_caught_up = Some(caught_up);
        self
    }

    /// Says that the replica, the primary of a database whose replica set a membership change
    /// kept, has placement's write quorum holding its log (ADR-0031, amendment 2).
    #[must_use]
    pub fn with_replicas_caught_up(mut self, caught_up: ReplicasCaughtUp) -> Self {
        self.replicas_caught_up = Some(caught_up);
        self
    }

    /// Returns the epoch of the primary that wrote the last frames of the replica's log.
    #[must_use]
    pub fn log_epoch(&self) -> LeaseEpoch {
        self.log_epoch
    }

    /// Returns whether placement's write quorum holds the log of the database the replica
    /// leads, while a membership change keeps its old replica set, if it reported it.
    #[must_use]
    pub fn replicas_caught_up(&self) -> Option<&ReplicasCaughtUp> {
        self.replicas_caught_up.as_ref()
    }

    /// Returns the database.
    #[must_use]
    pub fn database(&self) -> &DatabaseName {
        &self.database
    }

    /// Returns the role the member plays for the database.
    #[must_use]
    pub fn role(&self) -> ReplicaRole {
        self.role
    }

    /// Returns the position the member has stored the log up to.
    #[must_use]
    pub fn position(&self) -> &Position {
        &self.position
    }

    /// Returns the bytes the replica's copy takes, if it said.
    #[must_use]
    pub fn size_bytes(&self) -> Option<u64> {
        self.size_bytes
    }

    /// Returns the staged copy of a fork the replica holds, if it reported one.
    #[must_use]
    pub fn staged(&self) -> Option<&StagedCopy> {
        self.staged.as_ref()
    }

    /// Returns the loss of the replica's staged copy of a fork, if it reported one.
    #[must_use]
    pub fn copy_lost(&self) -> Option<&CopyLost> {
        self.copy_lost.as_ref()
    }

    /// Returns the replica's failure to seed a fork, if it reported one.
    #[must_use]
    pub fn seed_failure(&self) -> Option<&SeedFailure> {
        self.seed_failure.as_ref()
    }

    /// Returns how the replica's reset write of a fork stands, if it reported one.
    #[must_use]
    pub fn reset(&self) -> Option<&ResetReport> {
        self.reset.as_ref()
    }

    /// Returns whether placement's write quorum holds the log of the ready fork the replica
    /// leads, if it reported it.
    #[must_use]
    pub fn placement_caught_up(&self) -> Option<&PlacementCaughtUp> {
        self.placement_caught_up.as_ref()
    }

    /// Returns the report, keeping what it says about forks only if `forks` says so: before the
    /// `fork` feature is enabled those fields are dropped, so control state stays as it was. The
    /// same holds of a hand-over report before the `handover` feature is enabled.
    pub(crate) fn into_report(
        self,
        at: Timestamp,
        forks: ForkReports,
        handover: HandoverReports,
    ) -> (DatabaseName, Report) {
        let report = Report {
            role: self.role,
            position: self.position,
            at,
            log_epoch: self.log_epoch,
            size_bytes: self.size_bytes,
            staged: self.staged,
            copy_lost: self.copy_lost,
            seed_failure: self.seed_failure,
            reset: self.reset,
            placement_caught_up: self.placement_caught_up,
            replicas_caught_up: match handover {
                HandoverReports::Record => self.replicas_caught_up,
                HandoverReports::Drop => None,
            },
        };
        let report = match forks {
            ForkReports::Record => report,
            ForkReports::Drop => Report {
                size_bytes: None,
                staged: None,
                copy_lost: None,
                seed_failure: None,
                reset: None,
                placement_caught_up: None,
                ..report
            },
        };
        (self.database, report)
    }
}

/// Whether a heartbeat records what replicas report about forks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ForkReports {
    /// The `fork` feature is enabled: record them.
    Record,
    /// It isn't: drop them, so control state stays as it was before forks existed.
    Drop,
}

/// Whether a heartbeat records what primaries report about a hand-over (ADR-0031, amendment 2).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum HandoverReports {
    /// The `handover` feature is enabled: record them.
    Record,
    /// It isn't: drop them, so control state stays as it was before hand-overs existed.
    Drop,
}

/// What a member last reported about one database it hosts.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Report {
    role: ReplicaRole,
    position: Position,
    at: Timestamp,
    // Absent from states written before replicas reported it.
    #[serde(default, skip_serializing_if = "is_zero")]
    log_epoch: LeaseEpoch,
    // Absent from logs and states written before forks existed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    size_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    staged: Option<StagedCopy>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    copy_lost: Option<CopyLost>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    seed_failure: Option<SeedFailure>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    reset: Option<ResetReport>,
    // Absent from logs and states written before ready forks released their holders.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    placement_caught_up: Option<PlacementCaughtUp>,
    // Absent from logs and states written before membership changes kept replica sets.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    replicas_caught_up: Option<ReplicasCaughtUp>,
}

impl Report {
    /// Returns the role the member reported.
    #[must_use]
    pub fn role(&self) -> ReplicaRole {
        self.role
    }

    /// Returns the log position the member reported.
    #[must_use]
    pub fn position(&self) -> &Position {
        &self.position
    }

    /// Returns when the report arrived, as the effective time of its heartbeat.
    #[must_use]
    pub fn at(&self) -> Timestamp {
        self.at
    }

    /// Returns the epoch of the primary that wrote the last frames of the member's log, or
    /// [`LeaseEpoch::ZERO`] if it didn't say.
    #[must_use]
    pub fn log_epoch(&self) -> LeaseEpoch {
        self.log_epoch
    }

    /// Returns the bytes the member's copy takes, if it said.
    #[must_use]
    pub fn size_bytes(&self) -> Option<u64> {
        self.size_bytes
    }

    /// Returns the staged copy of a fork the member holds, if it reported one.
    #[must_use]
    pub fn staged(&self) -> Option<&StagedCopy> {
        self.staged.as_ref()
    }

    /// Returns the loss of the member's staged copy of a fork, if it reported one.
    #[must_use]
    pub fn copy_lost(&self) -> Option<&CopyLost> {
        self.copy_lost.as_ref()
    }

    /// Returns the member's failure to seed a fork, if it reported one.
    #[must_use]
    pub fn seed_failure(&self) -> Option<&SeedFailure> {
        self.seed_failure.as_ref()
    }

    /// Returns how the member's reset write of a fork stands, if it reported one.
    #[must_use]
    pub fn reset(&self) -> Option<&ResetReport> {
        self.reset.as_ref()
    }

    /// Returns whether placement's write quorum holds the log of the ready fork the member
    /// leads, if it reported it.
    #[must_use]
    pub fn placement_caught_up(&self) -> Option<&PlacementCaughtUp> {
        self.placement_caught_up.as_ref()
    }

    /// Returns whether placement's write quorum holds the log of the database the member
    /// leads, while a membership change keeps its old replica set, if it reported it.
    #[must_use]
    pub fn replicas_caught_up(&self) -> Option<&ReplicasCaughtUp> {
        self.replicas_caught_up.as_ref()
    }
}
