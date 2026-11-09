//! Commands: the only way to change control state.

use std::collections::BTreeSet;

use fleetring::{NodeId, ReplicationFactor};
use serde::{Deserialize, Serialize};

use crate::fork::{
    ChainHash, ForkFailure, ForkIdentity, ForkIntent, ForkLimits, OperationId, RetryCause,
};
use crate::{
    DatabaseName, Durability, Engine, Feature, HostedReplica, Incarnation, LeaseEpoch, Member,
    MetadataChange, MetadataSpec, Position, PrincipalName, Revoked, Software, Timestamp,
};

/// The features of a command that needs database metadata.
const METADATA: &[Feature] = &[Feature::Metadata];

/// The features of a command that needs database metadata and sets an expiry or acts on one.
const METADATA_AND_EXPIRY: &[Feature] = &[Feature::Metadata, Feature::Expiry];

/// The features of a command that drives a fork.
const FORK: &[Feature] = &[Feature::Fork];

/// The features of a fork request, which records metadata.
const FORK_AND_METADATA: &[Feature] = &[Feature::Fork, Feature::Metadata];

/// The features of a fork request that sets an expiry.
const FORK_METADATA_AND_EXPIRY: &[Feature] = &[Feature::Fork, Feature::Metadata, Feature::Expiry];

/// One change to control state, stamped with the control node's time.
///
/// The control node stamps each command before it logs it, and [`apply`](crate::apply) reads
/// time only from that stamp, so replaying the log gives the same result anywhere.
///
/// # Examples
///
/// ```
/// use fleetring_control::{Action, Command, Member, Timestamp};
///
/// let join = Command::new(
///     Timestamp::from_millis(1_000),
///     Action::Join { member: Member::new("a".parse()?) },
/// );
/// assert_eq!(join.at.as_millis(), 1_000);
/// # Ok::<(), fleetring::ParseLabelError>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Command {
    /// When the control node stamped the command.
    pub at: Timestamp,
    /// What the command does.
    pub action: Action,
}

impl Command {
    /// Returns the command that performs `action`, stamped `at`.
    #[must_use]
    pub fn new(at: Timestamp, action: Action) -> Self {
        Self { at, action }
    }
}

/// What a [`Command`] does.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum Action {
    /// Adds a member, or updates a known member's placement attributes and marks it live.
    ///
    /// A member that rejoins under its old ID keeps that ID, and its earlier reports are cleared
    /// until its next heartbeat.
    Join {
        /// The member and its placement attributes.
        member: Member,
    },

    /// Removes a member.
    Leave {
        /// The member's ID.
        id: NodeId,
    },

    /// Marks a member live and replaces its reports with the databases it hosts now.
    Heartbeat {
        /// The member's ID.
        id: NodeId,
        /// Every database the member hosts, with its role and log position.
        replicas: Vec<HostedReplica>,
        /// What to do with a report for a database the control plane doesn't know.
        ///
        /// Heartbeats logged before this field existed carry none and replay as
        /// [`UnknownReports::Reject`], the rule they were applied with.
        #[serde(default = "UnknownReports::before_the_field")]
        unknown_reports: UnknownReports,
        /// The software the member runs, when it differs from what control state records for
        /// it, as after the control node was upgraded beside members that joined it earlier
        /// (ADR-0038). Recording a change grows the membership epoch, as a join with new
        /// software does.
        ///
        /// Heartbeats logged before this field existed carry none, and change no software.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        software: Option<Software>,
        /// The free bytes on the member's data volume, for the control plane's best-effort fork
        /// admission (ADR-0046, point 7.3). Recorded only once the `fork` feature is enabled, so
        /// that control state stays as it was until then.
        ///
        /// Heartbeats logged before this field existed carry none.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        disk_free_bytes: Option<u64>,
    },

    /// Records a new database.
    CreateDatabase {
        /// The database's name.
        name: DatabaseName,
        /// How many replicas it should have.
        replication_factor: ReplicationFactor,
        /// The engine it runs on.
        engine: Engine,
        /// Its durability settings, or `None` for [`Acks::Quorum`](crate::Acks::Quorum) with the
        /// floor at W. Creations logged before the field existed carry none.
        #[serde(default)]
        durability: Option<Durability>,
        /// What the creation records as the database's metadata, or `None` for none. Needs
        /// [`Feature::Metadata`], and [`Feature::Expiry`] too if it sets an expiry. Creations
        /// logged before the field existed carry none, and a creation without it logs exactly
        /// what one did before.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        metadata: Option<MetadataSpec>,
        /// The database's incarnation: 64 random bits the proposing control node draws, so that
        /// a name dropped and created again names another life of it (ADR-0046, point 1).
        /// Stored once [`Feature::Fork`] is enabled, when 0 is refused, and ignored until then,
        /// so that a fleet that can still roll back stores nothing new. Creations logged before
        /// the field existed carry 0.
        #[serde(default, skip_serializing_if = "Incarnation::is_legacy")]
        incarnation: Incarnation,
    },

    /// Changes a database's metadata with a patch (ADR-0041, point 2): only the description,
    /// labels, and expiry the change names, removing labels before it sets them. A database
    /// created without metadata gets it at its first update, with no creator and no creation
    /// time. Every accepted update grows the metadata's version by one.
    ///
    /// Needs [`Feature::Metadata`], and [`Feature::Expiry`] too if it touches the expiry.
    UpdateDatabase {
        /// The database's name.
        name: DatabaseName,
        /// What changes.
        change: MetadataChange,
        /// The principal making the change, recorded as the description's author when the
        /// change sets the description. Recorded only; nothing is decided by it.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        by: Option<PrincipalName>,
        /// Apply the change only if the metadata is at this version: 0 for a database without
        /// metadata.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        if_version: Option<u64>,
    },

    /// Drops a database whose expiry has passed at the command's time, exactly as
    /// [`Action::DropDatabase`] drops one. An expiry extended or cleared since the command was
    /// made wins: the command is then rejected.
    ///
    /// Needs [`Feature::Metadata`] and [`Feature::Expiry`].
    ExpireDatabase {
        /// The database's name.
        name: DatabaseName,
    },

    /// Records new durability settings for a database, which take effect when its next lease is
    /// granted: at the end of the next promotion, or at its first lease if none has been
    /// granted yet (ADR-0023).
    ///
    /// Until then the current primary keeps the settings it was granted the lease under, so its
    /// guarantee never changes under it. The promotion that moves to the next epoch fences under
    /// the current settings' Q, which keeps every write the current epoch acknowledged.
    ChangeDurability {
        /// The database.
        database: DatabaseName,
        /// The settings to take effect at the next epoch.
        durability: Durability,
    },

    /// Removes a database for good: its record, its lease, any promotion in progress, and every
    /// member's report of it.
    ///
    /// The lease holder learns of the drop when its next renewal is refused as an unknown
    /// database, and followers when their streams end and the control plane no longer knows the
    /// database. Logs written before this action existed never contain it.
    DropDatabase {
        /// The database's name.
        name: DatabaseName,
    },

    /// Grants a database's first lease, at epoch 1.
    ///
    /// Later leases come only from promotions, which fence a quorum first.
    AcquireLease {
        /// The database.
        database: DatabaseName,
        /// The live replica-set member that asks to be primary.
        node: NodeId,
    },

    /// Extends the current lease by the lease duration.
    RenewLease {
        /// The database.
        database: DatabaseName,
        /// The node that holds the lease.
        node: NodeId,
        /// The epoch the node holds the lease at.
        epoch: LeaseEpoch,
    },

    /// Issues a new epoch and picks a candidate to fail over to.
    StartPromotion {
        /// The database.
        database: DatabaseName,
    },

    /// Records that a replica-set member has fenced at a promotion's epoch.
    RecordFenced {
        /// The database.
        database: DatabaseName,
        /// The fenced member.
        node: NodeId,
        /// The epoch it fenced at.
        epoch: LeaseEpoch,
        /// The log position it reported when it fenced.
        position: Position,
    },

    /// Grants the lease to a promotion's candidate once a promotion quorum is fenced.
    CompletePromotion {
        /// The database.
        database: DatabaseName,
        /// The promotion's epoch.
        epoch: LeaseEpoch,
    },

    /// Changes nothing but the time.
    ///
    /// The control node submits one when no other command has arrived for a while, so that
    /// liveness and lease expiry, which are judged at the time of the last command, move on
    /// without traffic.
    Tick,

    /// Marks a member as draining: it gets no new lease and is never a promotion's candidate,
    /// and it may hand its leases over with [`Action::TransferLease`].
    ///
    /// A member stays draining until it leaves or joins again. Logs written before this action
    /// existed never contain it.
    Drain {
        /// The member's ID.
        id: NodeId,
    },

    /// Clears a member's draining mark, so that it may take leases and be a promotion's candidate
    /// again, without a restart. Clearing the mark of a member that isn't draining changes
    /// nothing.
    ///
    /// Needs [`Feature::Undrain`]: release 0.3.0 can't read this action in a log (ADR-0038).
    Undrain {
        /// The member's ID.
        id: NodeId,
    },

    /// Hands a database's lease from its draining holder to another replica, without waiting for
    /// the lease to expire: issues the next epoch and starts a promotion whose candidate is
    /// `target`, or, if none is named, the most advanced live replica that isn't draining.
    ///
    /// The candidate then fences a quorum and completes the promotion exactly as after an
    /// expiry (ADR-0022), so a transfer is as safe as a failover. Logs written before this
    /// action existed never contain it.
    TransferLease {
        /// The database.
        database: DatabaseName,
        /// The replica to hand the lease to, if the holder has one in mind: usually the follower
        /// that has acknowledged everything it committed.
        target: Option<NodeId>,
    },

    /// Adds a bearer token, or every token a signing key issued, to the denylist nodes check
    /// before they trust a token (ADR-0025).
    ///
    /// The entry is kept until `until`, then forgotten: a token's expiry, after which nodes
    /// refuse it anyway, or [`Timestamp::MAX`] for a key. Revoking an entry again keeps the later
    /// time. The state records identifiers only, never a token or a key. Logs written before this
    /// action existed never contain it.
    RevokeToken {
        /// The token or key.
        revoked: Revoked,
        /// When the entry may be forgotten.
        until: Timestamp,
    },

    /// Creates a database as a copy of another, seeded by an operation with a client-chosen ID
    /// (ADR-0046, points 3.1 and 5).
    ///
    /// The source is resolved to its current incarnation and must be ready, and must be the
    /// incarnation the caller expects if it names one; a relative expiry is resolved against
    /// the command's time. The fork gets the incarnation the command carries, the source's engine, and the
    /// intent's own durability and metadata (nothing is copied from the source), and starts
    /// seeding at attempt 1 with the first live member of its replica set as the seeder. It is
    /// refused early when fewer than its completion quorum of replicas have room for the copy, as far
    /// as their heartbeats say.
    ///
    /// A resubmission with a known operation ID resolves nothing again: from the same requester
    /// with the same intent it changes nothing and returns the operation's outcome; from another
    /// requester, or with another intent, it is refused. Needs [`Feature::Fork`] and [`Feature::Metadata`], and [`Feature::Expiry`]
    /// too if it sets an expiry. The first one accepted starts incarnations.
    ForkDatabase {
        /// The operation's ID, chosen by the client.
        operation: OperationId,
        /// What the caller asked for, exactly as given.
        intent: ForkIntent,
        /// The fork's incarnation: 64 random bits the proposing control node draws, never 0. A
        /// resubmission's is ignored.
        incarnation: Incarnation,
        /// The absolute expiry the control service resolved from a relative one in the intent,
        /// as it resolves `--expires-in` (ADR-0041). A resubmission's is ignored.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        resolved_expiry: Option<Timestamp>,
        /// The fork's budgets and timeouts, from the control node's configuration.
        limits: ForkLimits,
    },

    /// Records that a completion quorum of a fork's replicas durably holds the same staged copy
    /// (ADR-0046, point 3.2): the fork becomes seeded, its holders become its replica set until
    /// it is ready, and one of them may take its first lease.
    ///
    /// Accepted only for the current fork identity, seeding at exactly `attempt`, while the
    /// source still exists at the fork's source incarnation, when every holder is in the
    /// current replica set, didn't refuse on disk, and last reported this staged copy at log
    /// epoch 0, and when the holders reach the completion quorum C = max(⌊R/2⌋ + 1, W,
    /// `min_replicas`). Proposed by the control node's fork driver only. Needs
    /// [`Feature::Fork`].
    CompleteFork {
        /// The fork.
        fork: ForkIdentity,
        /// The attempt that fetched the copy.
        attempt: u32,
        /// The source position the copy holds.
        at: Position,
        /// The copy's chain hash.
        chain_hash: ChainHash,
        /// The replicas that hold it.
        holders: BTreeSet<NodeId>,
    },

    /// Ends a fork's current attempt and starts the next with a new seeder, or fails the fork
    /// if its attempts or its time have run out (ADR-0046, point 3.3).
    ///
    /// `exclude_seeder` joins the replicas passed over as seeders, a preference rather than a
    /// ban: the next seeder is the first live member of the replica set, in preference order,
    /// that is neither passed over nor disk-refused, or, if every live one that isn't
    /// disk-refused was passed over, the first of those. Past the budget the fork fails with
    /// [`ForkFailure::RetriesExhausted`], and past the deadline with
    /// [`ForkFailure::DeadlineExceeded`], each keeping `cause`. Accepted only for the current
    /// fork identity, seeding at `attempt`. Proposed by the control node's fork driver only.
    /// Needs [`Feature::Fork`].
    RetryForkSeed {
        /// The fork.
        fork: ForkIdentity,
        /// The attempt that ended.
        attempt: u32,
        /// Why it ended.
        cause: RetryCause,
        /// The seeder to pass over from now on, because it failed as one.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        exclude_seeder: Option<NodeId>,
    },

    /// Records that a replica of a seeding fork refused the copy for want of disk (ADR-0046,
    /// point 3.4): it is never seeder or holder for this fork. If it was the seeder, the next
    /// attempt starts as with [`Action::RetryForkSeed`]; if fewer replicas than the completion
    /// quorum remain with room, the fork fails with [`ForkFailure::InsufficientDisk`]. Seeder
    /// exclusions never count toward that. Accepted only for the current fork identity, seeding
    /// at `attempt`. Proposed by the control node's fork driver only. Needs [`Feature::Fork`].
    RecordForkDiskRefusal {
        /// The fork.
        fork: ForkIdentity,
        /// The current attempt.
        attempt: u32,
        /// The replica that refused.
        node: NodeId,
    },

    /// Removes a holder from a seeded fork (ADR-0046, point 3.6): one whose latest report says
    /// its staged copy is lost, before the first lease, or one that is no longer a fleet member,
    /// at any time before the fork is ready. If fewer holders than the completion quorum would
    /// remain, then before any lease since completion the fork seeds again at the next attempt,
    /// with a fresh deadline, any promotion cleared and its epoch kept; after one, the holder
    /// stays and the command is refused, since the fork may hold acknowledged writes that seeding
    /// again would discard. Accepted only for the current fork identity, seeded at `attempt`.
    /// Proposed by the control node's fork driver only. Needs [`Feature::Fork`].
    DropForkHolder {
        /// The fork.
        fork: ForkIdentity,
        /// The attempt that seeded it.
        attempt: u32,
        /// The holder.
        holder: NodeId,
        /// Why: [`RetryCause::HolderLost`] or [`RetryCause::HoldersLeft`].
        cause: RetryCause,
    },

    /// Fails a seeding fork for a permanent reason (ADR-0046, point 3.5). Accepted only for the
    /// current fork identity at its current attempt. Proposed by the control node's fork driver
    /// only. Needs [`Feature::Fork`].
    FailFork {
        /// The fork.
        fork: ForkIdentity,
        /// The current attempt.
        attempt: u32,
        /// Why it fails.
        reason: ForkFailure,
    },

    /// Records that a seeded fork's primary committed the fork-reset write (ADR-0046, point
    /// 3.7): the fork becomes ready, and its replica set follows placement again. It mirrors the
    /// marker in the database, which decides.
    ///
    /// Accepted only for the current fork identity, while seeded, for an epoch the database
    /// has issued, when at least the bootstrap quorum W_boot = max(⌊R/2⌋ + 1, W) of its replica
    /// set last reported the reset committed for this identity at that epoch and position.
    /// Proposed by the control node's fork driver only. Needs [`Feature::Fork`].
    MarkForkReady {
        /// The fork.
        fork: ForkIdentity,
        /// The epoch of the primary that committed the reset.
        epoch: LeaseEpoch,
        /// The reset write's position.
        reset_position: Position,
    },
}

impl Action {
    /// Returns the fleet features the action needs, in order: an action that needs features is
    /// accepted only once every member supports each of them, and the first one accepted
    /// enables them all (ADR-0038, point 4).
    ///
    /// # Examples
    ///
    /// ```
    /// use fleetring_control::{Action, ExpiryChange, Feature, MetadataChange, Timestamp};
    ///
    /// assert_eq!(Action::Undrain { id: "a".parse()? }.features(), [Feature::Undrain]);
    /// assert!(Action::Tick.features().is_empty());
    /// let extend = Action::UpdateDatabase {
    ///     name: "agent-42-task".parse()?,
    ///     change: MetadataChange {
    ///         expiry: ExpiryChange::Set(Timestamp::from_millis(90_000)),
    ///         ..MetadataChange::default()
    ///     },
    ///     by: None,
    ///     if_version: None,
    /// };
    /// assert_eq!(extend.features(), [Feature::Metadata, Feature::Expiry]);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    #[must_use]
    pub fn features(&self) -> &'static [Feature] {
        match self {
            Self::Undrain { .. } => &[Feature::Undrain],
            Self::CreateDatabase {
                metadata: Some(spec),
                ..
            } => {
                if spec.expires_at.is_some() {
                    METADATA_AND_EXPIRY
                } else {
                    METADATA
                }
            }
            Self::UpdateDatabase { change, .. } => {
                if change.touches_expiry() {
                    METADATA_AND_EXPIRY
                } else {
                    METADATA
                }
            }
            Self::ExpireDatabase { .. } => METADATA_AND_EXPIRY,
            Self::ForkDatabase { intent, .. } => {
                if !intent.expiry.is_never() {
                    FORK_METADATA_AND_EXPIRY
                } else {
                    FORK_AND_METADATA
                }
            }
            Self::CompleteFork { .. }
            | Self::RetryForkSeed { .. }
            | Self::RecordForkDiskRefusal { .. }
            | Self::FailFork { .. }
            | Self::DropForkHolder { .. }
            | Self::MarkForkReady { .. } => FORK,
            _ => &[],
        }
    }

    /// Returns the fork identity that fences the action: that of a command that drives an
    /// existing fork, which `apply` checks before anything else (ADR-0046, point 3).
    #[must_use]
    pub fn fork_identity(&self) -> Option<&ForkIdentity> {
        match self {
            Self::CompleteFork { fork, .. }
            | Self::RetryForkSeed { fork, .. }
            | Self::RecordForkDiskRefusal { fork, .. }
            | Self::FailFork { fork, .. }
            | Self::DropForkHolder { fork, .. }
            | Self::MarkForkReady { fork, .. } => Some(fork),
            _ => None,
        }
    }
}

/// What a heartbeat does with a report for a database the control plane doesn't know.
///
/// A node reports every database it hosts. One the control plane doesn't know, such as a
/// database dropped while the node was away, mustn't fail the whole heartbeat: the node would
/// read as down, and its other reports would be lost.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UnknownReports {
    /// Reject the whole heartbeat. This was the only rule before the choice existed, and it stays
    /// so that logs written then replay the same way. New heartbeats never use it.
    Reject,
    /// Record the heartbeat and the known reports, and name the unknown databases in the outcome.
    Skip,
}

impl UnknownReports {
    /// The rule for a logged heartbeat that predates the field.
    fn before_the_field() -> Self {
        Self::Reject
    }
}
