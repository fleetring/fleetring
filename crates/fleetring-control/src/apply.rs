//! The state machine's transition function and its outcomes.

use std::collections::{BTreeMap, BTreeSet};
use std::iter;

use fleetring::{Epoch, NodeId, ReplicationFactor};
use serde::{Deserialize, Serialize};

use crate::command::UnknownReports;
use crate::fork::apply as forks;
use crate::fork::{
    CancelCause, ForkFailure, ForkIdentity, IntentField, OperationId, OperationOutcome, RetryCause,
};
use crate::member::ForkReports;
use crate::state::{MemberEntry, build_ring, quorums_for};
use crate::{
    Action, Command, Database, DatabaseName, DescriptionChange, Durability, DurabilityError,
    Engine, ExpiryChange, Feature, HostedReplica, Incarnation, Lease, LeaseEpoch, MAX_LABELS,
    Member, Metadata, MetadataChange, MetadataSpec, Position, PrincipalName, Promotion, Quorums,
    Release, Revoked, Software, State, Timestamp,
};

/// The result of applying a command: what changed, or why nothing did.
pub type Outcome = Result<Applied, Rejection>;

/// Applies `command` to `state` and reports what happened.
///
/// This is the only function that changes control state. It reads no clock, randomness, or
/// environment, so applying the same commands to the same state always gives the same states and
/// outcomes.
///
/// **Time.** A command runs at its stamp, or at [`State::now`] if the stamp is earlier: a stamp
/// that goes backwards counts as the last accepted time. Accepting a command moves
/// [`State::now`] to the time it ran at.
///
/// **Rejections** leave the state exactly as it was, including [`State::now`].
///
/// **Features.** An action that needs fleet features ([`Action::features`]) is checked against
/// the gate first ([`State::check_features`]); the first one accepted enables every feature it
/// needs for good (ADR-0038, point 4). A join is checked against the release window and the
/// enabled features; [`State::check_admission`] makes both checks without applying anything.
///
/// **Expiry** is judged at the command's time alone: no clock is read, so an expiry passes at
/// the same command in every replay.
///
/// **Forks** (ADR-0046): a command that drives an existing fork names its fork identity
/// ([`Action::fork_identity`]), and is refused with [`Rejection::StaleForkIdentity`] before any
/// other check, the feature gate included, unless that identity is the current fork's.
///
/// # Errors
///
/// Returns a [`Rejection`] when the command breaks a rule of ADR-0017 or ADR-0018; each variant
/// documents its rule.
///
/// # Examples
///
/// ```
/// use fleetring::ReplicationFactor;
/// use fleetring_control::{
///     apply, Action, Applied, Command, ControlConfig, Engine, LeaseEpoch, Member, State,
///     Timestamp,
/// };
///
/// let mut state = State::new(ControlConfig::new());
/// let at = Timestamp::from_millis(1_000);
/// let db = "tenant:acme".parse()?;
/// apply(&mut state, Command::new(at, Action::Join { member: Member::new("a".parse()?) }))?;
/// apply(&mut state, Command::new(at, Action::CreateDatabase {
///     name: db,
///     replication_factor: ReplicationFactor::ONE,
///     engine: Engine::Sqlite,
///     durability: None,
///     metadata: None,
///     incarnation: fleetring_control::Incarnation::LEGACY,
/// }))?;
/// let granted = apply(&mut state, Command::new(at, Action::AcquireLease {
///     database: "tenant:acme".parse()?,
///     node: "a".parse()?,
/// }))?;
/// let Applied::LeaseGranted { lease, .. } = granted else { panic!("expected a lease") };
/// assert_eq!(lease.epoch(), LeaseEpoch::new(1));
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
pub fn apply(state: &mut State, command: Command) -> Outcome {
    let now = command.at.max(state.now);
    if let Some(identity) = command.action.fork_identity() {
        crate::fork::rules::current_fork(state, identity)?;
    }
    let features = command.action.features();
    for feature in features {
        state.check_feature(*feature)?;
    }
    let applied = match command.action {
        Action::Join { member } => join(state, now, member),
        Action::Leave { id } => leave(state, id),
        Action::Heartbeat {
            id,
            replicas,
            unknown_reports,
            software,
            disk_free_bytes,
        } => heartbeat(
            state,
            now,
            Reported {
                id,
                replicas,
                unknown_reports,
                software,
                disk_free_bytes,
            },
        ),
        Action::CreateDatabase {
            name,
            replication_factor,
            engine,
            durability,
            metadata,
            incarnation,
        } => create_database(
            state,
            now,
            Creation {
                name,
                replication_factor,
                engine,
                durability,
                metadata,
                incarnation,
            },
        ),
        Action::UpdateDatabase {
            name,
            change,
            by,
            if_version,
        } => update_database(state, now, name, change, by, if_version),
        Action::ExpireDatabase { name } => expire_database(state, now, name),
        Action::ChangeDurability {
            database,
            durability,
        } => change_durability(state, database, durability),
        Action::DropDatabase { name } => drop_database(state, now, name),
        Action::AcquireLease { database, node } => acquire_lease(state, now, database, node),
        Action::RenewLease {
            database,
            node,
            epoch,
        } => renew_lease(state, now, &database, &node, epoch),
        Action::StartPromotion { database } => start_promotion(state, now, database),
        Action::RecordFenced {
            database,
            node,
            epoch,
            position,
        } => record_fenced(state, database, node, epoch, position),
        Action::CompletePromotion { database, epoch } => {
            complete_promotion(state, now, database, epoch)
        }
        Action::Tick => {
            state.revocations.forget_through(now);
            forks::tick(state, now);
            Ok(Applied::Ticked { at: now })
        }
        Action::Drain { id } => drain(state, id),
        Action::Undrain { id } => undrain(state, id),
        Action::TransferLease { database, target } => transfer_lease(state, now, database, target),
        Action::RevokeToken { revoked, until } => revoke_token(state, now, revoked, until),
        Action::ForkDatabase {
            operation,
            intent,
            incarnation,
            resolved_expiry,
            limits,
        } => forks::fork_database(
            state,
            now,
            operation,
            intent,
            forks::Proposed {
                incarnation,
                resolved_expiry,
                limits,
            },
        ),
        Action::CompleteFork {
            fork,
            attempt,
            at,
            chain_hash,
            holders,
        } => forks::complete_fork(state, now, &fork, attempt, at, chain_hash, holders),
        Action::RetryForkSeed {
            fork,
            attempt,
            cause,
            exclude_seeder,
        } => forks::retry_fork_seed(state, now, &fork, attempt, cause, exclude_seeder),
        Action::RecordForkDiskRefusal {
            fork,
            attempt,
            node,
        } => forks::record_disk_refusal(state, now, &fork, attempt, node),
        Action::DropForkHolder {
            fork,
            attempt,
            holder,
            cause,
        } => forks::drop_fork_holder(state, now, &fork, attempt, holder, cause),
        Action::FailFork {
            fork,
            attempt,
            reason,
        } => forks::fail_fork(state, now, &fork, attempt, reason),
        Action::MarkForkReady {
            fork,
            epoch,
            reset_position,
        } => forks::mark_fork_ready(state, now, &fork, epoch, reset_position),
    }?;
    state.features.extend(features.iter().copied());
    state.now = now;
    Ok(applied)
}

/// What an accepted command did.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum Applied {
    /// A member joined, rejoined, or changed its placement attributes.
    MemberJoined {
        /// The member's ID.
        id: NodeId,
        /// The membership epoch after the join.
        membership_epoch: Epoch,
        /// Whether the member is new, rejoined unchanged, or changed its attributes.
        kind: JoinKind,
    },

    /// A member left.
    MemberLeft {
        /// The member's ID.
        id: NodeId,
        /// The membership epoch after the member left.
        membership_epoch: Epoch,
    },

    /// A heartbeat was recorded.
    HeartbeatRecorded {
        /// The member's ID.
        id: NodeId,
        /// The reported databases the control plane doesn't know, left out of the reports.
        unknown: Vec<DatabaseName>,
    },

    /// A database was created.
    DatabaseCreated {
        /// The database's name.
        database: DatabaseName,
    },

    /// New durability settings were recorded for a database, to take effect at its next lease.
    DurabilityChanged {
        /// The database's name.
        database: DatabaseName,
        /// The settings that take effect at the next lease.
        durability: Durability,
        /// The settings in effect until then.
        current: Durability,
    },

    /// A database was dropped, with its lease and any promotion.
    DatabaseDropped {
        /// The database's name.
        database: DatabaseName,
        /// The epoch the database was at when it was dropped. A node still serving it at that
        /// epoch stops once its next renewal is refused.
        epoch: LeaseEpoch,
    },

    /// A database's first lease was granted.
    LeaseGranted {
        /// The database.
        database: DatabaseName,
        /// The new lease.
        lease: Lease,
    },

    /// A lease was renewed.
    LeaseRenewed {
        /// The database.
        database: DatabaseName,
        /// The lease with its new expiry.
        lease: Lease,
    },

    /// A promotion started and issued a new epoch.
    PromotionStarted {
        /// The database.
        database: DatabaseName,
        /// The new epoch.
        epoch: LeaseEpoch,
        /// The chosen candidate.
        candidate: NodeId,
        /// The log position the candidate last reported.
        position: Position,
    },

    /// A member was recorded as fenced at a promotion's epoch.
    FenceRecorded {
        /// The database.
        database: DatabaseName,
        /// The fenced member.
        node: NodeId,
        /// How many current replica-set members are fenced now.
        fenced: u8,
        /// The promotion quorum Q.
        needed: u8,
    },

    /// A promotion completed and its candidate holds the lease.
    PromotionCompleted {
        /// The database.
        database: DatabaseName,
        /// The new lease.
        lease: Lease,
        /// How many replica-set members were fenced, the candidate included.
        fenced: u8,
        /// The replica set's quorums at completion.
        quorums: Quorums,
        /// The most advanced position among the fenced members. The new primary adopts this
        /// log before it accepts writes (ADR-0018, point 4).
        adopted: Position,
    },

    /// Time moved on, and nothing else changed.
    Ticked {
        /// The state's time after the tick.
        at: Timestamp,
    },

    /// A member is draining: it gets no new lease, and it may hand its leases over.
    MemberDraining {
        /// The member's ID.
        id: NodeId,
    },

    /// A member's draining mark is cleared: it may take leases again.
    MemberUndrained {
        /// The member's ID.
        id: NodeId,
    },

    /// A bearer token or signing key is on the denylist.
    TokenRevoked {
        /// The token or key.
        revoked: Revoked,
        /// When the entry may be forgotten: the later of this and any earlier revocation's.
        until: Timestamp,
    },

    /// A database's metadata was patched.
    DatabaseUpdated {
        /// The database.
        database: DatabaseName,
        /// The metadata's version after the update.
        version: u64,
    },

    /// A database whose expiry had passed was dropped, as `DropDatabase` drops one.
    DatabaseExpired {
        /// The database.
        database: DatabaseName,
        /// The epoch the database was at when it was dropped. A node still serving it at that
        /// epoch stops once its next renewal is refused.
        epoch: LeaseEpoch,
        /// The expiry that had passed.
        expires_at: Timestamp,
    },

    /// A fork was created and started seeding at attempt 1.
    ForkCreated {
        /// The fork.
        database: DatabaseName,
        /// Its incarnation.
        incarnation: Incarnation,
        /// The operation that made it.
        operation: OperationId,
        /// The first attempt's seeder.
        seeder: NodeId,
    },

    /// A fork request repeated a known operation; nothing changed.
    ForkResent {
        /// The operation.
        operation: OperationId,
        /// The fork it made.
        database: DatabaseName,
        /// The fork's incarnation.
        incarnation: Incarnation,
        /// What has become of it.
        outcome: OperationOutcome,
    },

    /// A fork's completion quorum holds its copy: it is seeded.
    ForkSeeded {
        /// The fork.
        database: DatabaseName,
        /// The replicas that hold the copy, the only ones that may take its first lease.
        holders: BTreeSet<NodeId>,
    },

    /// A fork's attempt ended and the next began.
    ForkRetried {
        /// The fork.
        database: DatabaseName,
        /// The new attempt.
        attempt: u32,
        /// Its seeder.
        seeder: NodeId,
        /// Why the last attempt ended.
        cause: RetryCause,
    },

    /// A replica of a seeding fork refused the copy for want of disk; the attempt goes on
    /// without it.
    ForkDiskRefusalRecorded {
        /// The fork.
        database: DatabaseName,
        /// The replica.
        node: NodeId,
    },

    /// A holder left a seeded fork's holders, and enough remain.
    ForkHolderDropped {
        /// The fork.
        database: DatabaseName,
        /// The holder.
        holder: NodeId,
        /// Why.
        cause: RetryCause,
    },

    /// A fork failed for good.
    ForkFailed {
        /// The fork.
        database: DatabaseName,
        /// Why.
        reason: ForkFailure,
    },

    /// A fork's reset is committed: it is ready.
    ForkReady {
        /// The fork.
        database: DatabaseName,
        /// The epoch of the primary that committed the reset.
        epoch: LeaseEpoch,
        /// The reset write's position.
        reset_position: Position,
    },
}

/// How a join changed the membership.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum JoinKind {
    /// The member is new. The membership epoch grew.
    New,
    /// A known member rejoined with the same attributes. The membership epoch didn't change.
    Rejoined,
    /// A known member rejoined with new attributes. The membership epoch grew.
    Updated,
}

/// Why a command was rejected. A rejected command changes nothing.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum Rejection {
    /// The node is not a member.
    #[error("node '{id}' is not a member")]
    UnknownMember {
        /// The node's ID.
        id: NodeId,
    },

    /// The database doesn't exist.
    #[error("database '{database}' does not exist")]
    UnknownDatabase {
        /// The database.
        database: DatabaseName,
    },

    /// A database with this name already exists.
    #[error("database '{database}' already exists")]
    DatabaseExists {
        /// The database.
        database: DatabaseName,
    },

    /// The fleet has no members, so no database has a replica set.
    #[error("the fleet has no members")]
    NoMembers,

    /// The durability settings don't fit the database's replication factor.
    #[error("invalid durability for database '{database}': {source}")]
    InvalidDurability {
        /// The database.
        database: DatabaseName,
        /// What is wrong with the settings.
        source: DurabilityError,
    },

    /// The join would make a ring that can't be built, such as a host in two zones.
    #[error("join would make an invalid ring: {reason}")]
    InvalidTopology {
        /// Why the ring can't be built.
        reason: String,
    },

    /// The membership epoch can't grow any further.
    #[error("membership epoch is exhausted")]
    MembershipEpochExhausted,

    /// A heartbeat reports the same database twice.
    #[error("heartbeat reports database '{database}' more than once")]
    DuplicateReport {
        /// The database.
        database: DatabaseName,
    },

    /// The node is down, so it can't take a lease.
    #[error("node '{id}' is down")]
    MemberDown {
        /// The node's ID.
        id: NodeId,
    },

    /// The node is not in the database's replica set.
    #[error("node '{id}' is not in the replica set of database '{database}'")]
    NotInReplicaSet {
        /// The node's ID.
        id: NodeId,
        /// The database.
        database: DatabaseName,
    },

    /// The database already has a lease; a new holder needs a promotion.
    #[error("database '{database}' already has a lease; a new holder needs a promotion")]
    LeaseAlreadyIssued {
        /// The database.
        database: DatabaseName,
    },

    /// The database has no lease yet.
    #[error("database '{database}' has no lease")]
    NoLease {
        /// The database.
        database: DatabaseName,
    },

    /// The node doesn't hold the database's lease.
    #[error("node '{id}' does not hold the lease of database '{database}'")]
    NotLeaseHolder {
        /// The node's ID.
        id: NodeId,
        /// The database.
        database: DatabaseName,
    },

    /// The command names an epoch that is not the current one.
    #[error("epoch {given} of database '{database}' is stale; the current epoch is {current}")]
    StaleEpoch {
        /// The database.
        database: DatabaseName,
        /// The epoch the command named.
        given: LeaseEpoch,
        /// The epoch the command needed to name.
        current: LeaseEpoch,
    },

    /// The database's epoch counter can't grow any further.
    #[error("epochs of database '{database}' are exhausted")]
    LeaseEpochExhausted {
        /// The database.
        database: DatabaseName,
    },

    /// The current lease hasn't expired by the failover margin yet.
    #[error("lease of database '{database}' can be replaced only after {available_after}")]
    LeaseNotExpired {
        /// The database.
        database: DatabaseName,
        /// The lease's expiry plus the failover margin. A promotion may start once the time is
        /// past it.
        available_after: Timestamp,
    },

    /// A promotion is already in progress and its candidate is live and not stalled.
    #[error("database '{database}' already has a promotion in progress")]
    PromotionInProgress {
        /// The database.
        database: DatabaseName,
    },

    /// No promotion is in progress.
    #[error("database '{database}' has no promotion in progress")]
    NoPromotion {
        /// The database.
        database: DatabaseName,
    },

    /// No live replica-set member has reported a position for the database.
    #[error("database '{database}' has no live candidate with a reported position")]
    NoCandidate {
        /// The database.
        database: DatabaseName,
    },

    /// The member is already fenced at this promotion's epoch.
    #[error("node '{id}' is already fenced for database '{database}'")]
    AlreadyFenced {
        /// The node's ID.
        id: NodeId,
        /// The database.
        database: DatabaseName,
    },

    /// The promotion's candidate is down or has left.
    #[error("candidate '{id}' is down")]
    CandidateDown {
        /// The candidate's ID.
        id: NodeId,
    },

    /// The promotion's candidate hasn't fenced itself yet.
    #[error("candidate '{id}' is not fenced")]
    CandidateNotFenced {
        /// The candidate's ID.
        id: NodeId,
    },

    /// Fewer than Q replica-set members are fenced.
    #[error("promotion needs {needed} fenced members; {fenced} are fenced")]
    QuorumNotReached {
        /// How many replica-set members are fenced.
        fenced: u8,
        /// The promotion quorum Q.
        needed: u8,
    },

    /// The node is draining, so it can't take a lease or be a promotion's candidate.
    #[error("node '{id}' is draining")]
    MemberDraining {
        /// The node's ID.
        id: NodeId,
    },

    /// Only a draining holder may hand a lease over before it expires.
    #[error("node '{id}' holds the lease of database '{database}' and is not draining")]
    HolderNotDraining {
        /// The holder's ID.
        id: NodeId,
        /// The database.
        database: DatabaseName,
    },

    /// The joining member's release is more than one release line from a member's, or in another
    /// major: a fleet runs at most two adjacent release lines at once (ADR-0038, point 1).
    #[error(
        "node '{id}' runs fleetringd {release}, which can't run beside node '{member}' on \
         {member_release}: a fleet runs at most two adjacent release lines at once"
    )]
    IncompatibleVersion {
        /// The joining node's ID.
        id: NodeId,
        /// The joining node's release.
        release: Release,
        /// A member whose release line is too far from it.
        member: NodeId,
        /// That member's release.
        member_release: Release,
    },

    /// The joining member lacks a fleet feature the fleet has enabled, so it couldn't read or
    /// act on commands the fleet already uses.
    #[error(
        "node '{id}' runs fleetringd {release}, which lacks the fleet feature '{feature}' this \
         fleet uses; it needs a release that supports it"
    )]
    MissingFeature {
        /// The joining node's ID.
        id: NodeId,
        /// The joining node's release.
        release: Release,
        /// The enabled feature it lacks.
        feature: Feature,
    },

    /// The command needs a fleet feature that a member doesn't support yet, so applying it would
    /// leave that member behind.
    #[error(
        "the fleet can't use '{feature}' yet: node '{id}' runs fleetringd {release}, which \
         doesn't support it; upgrade every member first"
    )]
    FeatureUnsupported {
        /// The feature the command needs.
        feature: Feature,
        /// The first member, in ID order, that doesn't support it.
        id: NodeId,
        /// That member's release.
        release: Release,
    },

    /// The command needs a fleet feature that isn't enabled, and no member has joined yet, so
    /// no member can be shown to support it. Enabling it then would refuse every later join of
    /// a release that doesn't report it.
    #[error("the fleet can't use '{feature}' yet: no member has joined")]
    FeatureWithoutMembers {
        /// The feature the command needs.
        feature: Feature,
    },

    /// A revocation would be forgotten at once: the token it names has already expired.
    #[error("revocation of {revoked} ends at {until}, which has passed")]
    RevocationPassed {
        /// The token or key.
        revoked: Revoked,
        /// When the entry would have been forgotten.
        until: Timestamp,
    },

    /// A conditional update named a version the database's metadata isn't at.
    #[error("metadata of database '{database}' is at version {current}, not {expected}")]
    MetadataConflict {
        /// The database.
        database: DatabaseName,
        /// The version the update required.
        expected: u64,
        /// The metadata's version: 0 for a database without metadata.
        current: u64,
    },

    /// An update would leave a database with more labels than it may carry.
    #[error("database '{database}' would have {count} labels; the maximum is {max}")]
    TooManyLabels {
        /// The database.
        database: DatabaseName,
        /// How many labels the update would leave.
        count: usize,
        /// The most a database may carry.
        max: usize,
    },

    /// An expiry is at or before the command's time, so the database would expire at once.
    #[error("expiry {expires_at} of database '{database}' has already passed")]
    ExpiryPassed {
        /// The database.
        database: DatabaseName,
        /// The expiry asked for.
        expires_at: Timestamp,
    },

    /// An `ExpireDatabase` named a database whose expiry hasn't passed at the command's time, or
    /// that has none: it was extended or cleared after the command was made.
    #[error("database '{database}' has not expired")]
    NotExpired {
        /// The database.
        database: DatabaseName,
        /// The database's expiry, if it has one.
        expires_at: Option<Timestamp>,
    },

    /// A command meant for a fork names a fork identity that isn't the current one: the fork
    /// was dropped, or its name now belongs to another database (ADR-0046, point 3). Checked
    /// before anything else; the command changes nothing.
    #[error("fork {fork} is not the current database of that name")]
    StaleForkIdentity {
        /// The identity the command named.
        fork: ForkIdentity,
    },

    /// A fork request reused a known operation ID, from its requester, asking for something
    /// else (ADR-0046, point 5.2).
    #[error("operation {operation} was requested with a different {field}")]
    OperationConflict {
        /// The operation.
        operation: OperationId,
        /// The first field of the intent that differs.
        field: IntentField,
    },

    /// A fork request reused an operation ID another principal requested. Nothing about the
    /// operation is revealed (ADR-0046, point 5.2).
    #[error("operation {operation} isn't yours")]
    OperationNotYours {
        /// The operation.
        operation: OperationId,
    },

    /// Control state holds as many fork operations as it may; a new one waits until records
    /// past their retention are forgotten (ADR-0046, point 5.3).
    #[error("too many fork operations: at most {max} are kept; retry later")]
    TooManyOperations {
        /// The most kept.
        max: usize,
    },

    /// A fork's source is itself a fork that isn't ready yet.
    #[error("source '{source_database}' is not ready")]
    SourceNotReady {
        /// The source.
        source_database: DatabaseName,
    },

    /// A fork's source isn't the incarnation the command expects: it was dropped, or dropped
    /// and recreated.
    #[error(
        "source '{source_database}' is not at incarnation {expected}{}",
        match current {
            Some(current) => format!("; it is at {current}"),
            None => "; it was dropped".to_owned(),
        }
    )]
    SourceIncarnationMismatch {
        /// The source.
        source_database: DatabaseName,
        /// The incarnation the command expects.
        expected: Incarnation,
        /// The source's incarnation, or `None` if it no longer exists.
        current: Option<Incarnation>,
    },

    /// Fewer replicas of a new fork than its completion quorum have room for the copy, as far as
    /// their heartbeats say (ADR-0046, point 7.3).
    #[error("fork '{database}' needs room for its copy on {need} replicas; {admitting} have it")]
    InsufficientDisk {
        /// The fork.
        database: DatabaseName,
        /// How many replicas have room.
        admitting: usize,
        /// The fork's completion quorum.
        need: u8,
    },

    /// The fork isn't seeding at the attempt the command names: it moved on to another
    /// attempt, completed, or failed.
    #[error("fork '{database}' is not seeding at attempt {attempt}")]
    ForkNotSeeding {
        /// The fork.
        database: DatabaseName,
        /// The attempt the command named.
        attempt: u32,
    },

    /// The fork isn't seeded: it is still seeding, or already ready, or failed.
    #[error("fork '{database}' is not seeded")]
    ForkNotSeeded {
        /// The fork.
        database: DatabaseName,
    },

    /// A fork's completion names fewer holders than its completion quorum.
    #[error("a fork needs {need} replicas holding its staged copy; {have} were named")]
    NotEnoughHolders {
        /// How many holders the command named.
        have: usize,
        /// The fork's completion quorum.
        need: u8,
    },

    /// A holder's latest report isn't the staged copy the completion claims.
    #[error("node '{id}' has not reported the claimed staged copy of fork '{database}'")]
    HolderReportMismatch {
        /// The holder.
        id: NodeId,
        /// The fork.
        database: DatabaseName,
    },

    /// A fork that isn't seeded takes no lease (ADR-0046, point 3.9).
    #[error("fork '{database}' is not seeded and takes no lease")]
    LeaseOnUnseededFork {
        /// The fork.
        database: DatabaseName,
    },

    /// A fork's replica set is smaller than its completion quorum: the fleet has too few
    /// members for its replication factor and durability, whatever their disks hold.
    #[error(
        "fork '{database}' needs {needed} replicas to complete; the fleet places it on {available}"
    )]
    TooFewReplicas {
        /// The fork.
        database: DatabaseName,
        /// The fork's completion quorum.
        needed: u8,
        /// The replicas placement gives it.
        available: usize,
    },

    /// Fewer members of the fork's replica set than its bootstrap quorum reported the reset
    /// committed at the epoch and position the command names.
    #[error("too few replicas reported the reset of fork '{database}' committed")]
    ResetNotReported {
        /// The fork.
        database: DatabaseName,
    },

    /// A creation's incarnation is the legacy 0, which a creation may carry only before forks
    /// are enabled, or a database or a fork operation records it already. The control node
    /// draws another and asks again.
    #[error("incarnation {incarnation} is unavailable")]
    IncarnationUnavailable {
        /// The incarnation.
        incarnation: Incarnation,
    },

    /// A seeded fork's first lease goes only to a holder whose latest report still shows the
    /// staged copy the fork completed with, at log epoch 0 (ADR-0046, point 3.9).
    #[error("node '{id}' no longer reports the staged copy of fork '{database}'")]
    StagedCopyNotReported {
        /// The holder.
        id: NodeId,
        /// The fork.
        database: DatabaseName,
    },

    /// The source's engine can't fork (ADR-0046, point 9).
    #[error("databases on {engine} can't be forked yet")]
    EngineCannotFork {
        /// The engine.
        engine: Engine,
    },

    /// A completion names a holder that refused the copy for want of disk.
    #[error("node '{id}' refused the copy of fork '{database}' for want of disk")]
    DiskRefusedHolder {
        /// The holder.
        id: NodeId,
        /// The fork.
        database: DatabaseName,
    },

    /// A holder may leave a seeded fork's holders only if it is no longer a member, or, before
    /// the first lease, if its latest report says its copy is lost.
    #[error("node '{id}' is a member holding its copy of fork '{database}'")]
    HolderNotDroppable {
        /// The holder.
        id: NodeId,
        /// The fork.
        database: DatabaseName,
    },

    /// A fork's durability can't change until it is ready: its completion counted holders
    /// under the settings it was made with.
    #[error("fork '{database}' is not ready; its durability can't change yet")]
    ForkNotReady {
        /// The fork.
        database: DatabaseName,
    },
}

fn join(state: &mut State, now: Timestamp, member: Member) -> Outcome {
    check_join_software(state, &member)?;
    let id = member.id().clone();
    // Software isn't compared: a rejoin on a new release with the same attributes keeps the
    // membership epoch, so that a control node of a release that ignores software, such as
    // 0.3.0 after a rollback, replays the log to the same epoch (ADR-0038, point 2).
    let kind = match state.members.get(&id) {
        None => JoinKind::New,
        Some(entry) if entry.member.same_attributes(&member) => JoinKind::Rejoined,
        Some(_) => JoinKind::Updated,
    };
    let membership_epoch = if kind == JoinKind::Rejoined {
        state.membership_epoch
    } else {
        let next = next_membership_epoch(state.membership_epoch)?;
        let others = state
            .members
            .values()
            .filter(|entry| entry.member.id() != &id)
            .map(|entry| entry.member.to_node());
        build_ring(others.chain(iter::once(member.to_node())), next).map_err(|error| {
            Rejection::InvalidTopology {
                reason: error.to_string(),
            }
        })?;
        next
    };

    state.membership_epoch = membership_epoch;
    state.members.insert(
        id.clone(),
        MemberEntry {
            member,
            last_heartbeat: now,
            reports: BTreeMap::new(),
            draining: false,
            disk_free_bytes: None,
        },
    );
    Ok(Applied::MemberJoined {
        id,
        membership_epoch,
        kind,
    })
}

/// Checks the software of a joining `member`, as [`State::check_admission`] and `join` both do.
///
/// A member that rejoins on the software it is recorded with changes no release in the fleet, so
/// it is never refused for it: a heartbeat may have recorded software a join would refuse.
pub(crate) fn check_join_software(state: &State, member: &Member) -> Result<(), Rejection> {
    let recorded = state
        .members
        .get(member.id())
        .map(|entry| entry.member.software());
    if recorded == Some(member.software()) {
        return Ok(());
    }
    check_software(state, member.id(), &member.running())
}

/// Checks that a member `id` running `software` may join: it supports every enabled feature,
/// and its release line is within the window of every other member's, live or down (ADR-0038,
/// point 3).
fn check_software(state: &State, id: &NodeId, software: &Software) -> Result<(), Rejection> {
    if let Some(feature) = state
        .features
        .iter()
        .copied()
        .find(|feature| !software.supports(*feature))
    {
        return Err(Rejection::MissingFeature {
            id: id.clone(),
            release: software.release().clone(),
            feature,
        });
    }
    for (other, entry) in &state.members {
        if other == id {
            continue;
        }
        let theirs = entry.member.running();
        if !software.release().runs_beside(theirs.release()) {
            return Err(Rejection::IncompatibleVersion {
                id: id.clone(),
                release: software.release().clone(),
                member: other.clone(),
                member_release: theirs.release().clone(),
            });
        }
    }
    Ok(())
}

fn leave(state: &mut State, id: NodeId) -> Outcome {
    if !state.members.contains_key(&id) {
        return Err(Rejection::UnknownMember { id });
    }
    let membership_epoch = next_membership_epoch(state.membership_epoch)?;

    state.members.remove(&id);
    state.membership_epoch = membership_epoch;
    Ok(Applied::MemberLeft {
        id,
        membership_epoch,
    })
}

/// What a heartbeat reports.
struct Reported {
    id: NodeId,
    replicas: Vec<HostedReplica>,
    unknown_reports: UnknownReports,
    software: Option<Software>,
    disk_free_bytes: Option<u64>,
}

fn heartbeat(state: &mut State, now: Timestamp, reported: Reported) -> Outcome {
    let Reported {
        id,
        replicas,
        unknown_reports,
        software,
        disk_free_bytes,
    } = reported;
    let Some(entry) = state.members.get(&id) else {
        return Err(Rejection::UnknownMember { id });
    };
    // A heartbeat is never refused for its software: that would make a live node look down. It
    // only corrects the record, and leaves the membership epoch as it is, as a join does.
    let software = software.filter(|software| entry.member.software() != Some(software));
    let mut seen = BTreeSet::new();
    let mut unknown = Vec::new();
    for replica in &replicas {
        if !seen.insert(replica.database()) {
            return Err(Rejection::DuplicateReport {
                database: replica.database().clone(),
            });
        }
        if state.database(replica.database()).is_some() {
            continue;
        }
        match unknown_reports {
            UnknownReports::Skip => unknown.push(replica.database().clone()),
            UnknownReports::Reject => {
                return Err(Rejection::UnknownDatabase {
                    database: replica.database().clone(),
                });
            }
        }
    }
    // What replicas say about forks is recorded only once forks are enabled, so that control
    // state stays byte for byte as it was until then (ADR-0046, point 8).
    let (forks, disk_free_bytes) = if state.is_enabled(Feature::Fork) {
        (ForkReports::Record, disk_free_bytes)
    } else {
        (ForkReports::Drop, None)
    };
    let reports = replicas
        .into_iter()
        .filter(|replica| !unknown.contains(replica.database()))
        .map(|replica| replica.into_report(now, forks))
        .collect();

    let Some(entry) = state.members.get_mut(&id) else {
        return Err(Rejection::UnknownMember { id });
    };
    entry.last_heartbeat = now;
    entry.reports = reports;
    entry.disk_free_bytes = disk_free_bytes;
    if let Some(software) = software {
        entry.member = entry.member.clone().with_software(software);
    }
    Ok(Applied::HeartbeatRecorded { id, unknown })
}

/// What a `CreateDatabase` asks for.
struct Creation {
    name: DatabaseName,
    replication_factor: ReplicationFactor,
    engine: Engine,
    durability: Option<Durability>,
    metadata: Option<MetadataSpec>,
    incarnation: Incarnation,
}

fn create_database(state: &mut State, now: Timestamp, creation: Creation) -> Outcome {
    let Creation {
        name,
        replication_factor,
        engine,
        durability,
        metadata,
        incarnation,
    } = creation;
    if state.databases.contains_key(&name) {
        return Err(Rejection::DatabaseExists { database: name });
    }
    let durability = durability.unwrap_or_else(|| Durability::quorum(replication_factor));
    durability
        .check(replication_factor)
        .map_err(|source| Rejection::InvalidDurability {
            database: name.clone(),
            source,
        })?;
    if let Some(expires_at) = metadata.as_ref().and_then(|spec| spec.expires_at) {
        check_expiry(&name, now, expires_at)?;
    }
    // Incarnations start with the `fork` feature, so that a fleet that can still roll back
    // stores nothing new (ADR-0046, point 1.2): until then one in the command is ignored.
    let incarnation = if state.is_enabled(Feature::Fork) {
        forks::check_incarnation(state, incarnation)?;
        incarnation
    } else {
        Incarnation::LEGACY
    };
    let metadata = metadata.map(|spec| Metadata::created(spec, now));
    state.databases.insert(
        name.clone(),
        Database::new(
            name.clone(),
            replication_factor,
            engine,
            durability,
            metadata,
        )
        .with_incarnation(incarnation),
    );
    Ok(Applied::DatabaseCreated { database: name })
}

/// Rejects an expiry at or before `now`, which would expire the database at once.
pub(crate) fn check_expiry(
    database: &DatabaseName,
    now: Timestamp,
    expires_at: Timestamp,
) -> Result<(), Rejection> {
    if expires_at <= now {
        return Err(Rejection::ExpiryPassed {
            database: database.clone(),
            expires_at,
        });
    }
    Ok(())
}

/// Patches a database's metadata, starting from none if it has none: label removals, then label
/// sets, then the description and the expiry. The version grows by one.
fn update_database(
    state: &mut State,
    now: Timestamp,
    name: DatabaseName,
    change: MetadataChange,
    by: Option<PrincipalName>,
    if_version: Option<u64>,
) -> Outcome {
    let db = state.database_or_reject(&name)?;
    let current = db.metadata().map_or(0, Metadata::version);
    if let Some(expected) = if_version.filter(|expected| *expected != current) {
        return Err(Rejection::MetadataConflict {
            database: name,
            expected,
            current,
        });
    }
    let mut metadata = db.metadata().cloned().unwrap_or_else(Metadata::empty);
    let MetadataChange {
        description,
        set_labels,
        remove_labels,
        expiry,
    } = change;
    let labels = metadata
        .labels()
        .patched(&remove_labels, &set_labels)
        .map_err(|count| Rejection::TooManyLabels {
            database: name.clone(),
            count,
            max: MAX_LABELS,
        })?;
    metadata.set_labels(labels);
    match description {
        DescriptionChange::Keep => {}
        DescriptionChange::Set(text) => metadata.set_description(Some(text), by),
        DescriptionChange::SetWithoutAuthor(text) => metadata.set_description(Some(text), None),
        DescriptionChange::Clear => metadata.set_description(None, None),
    }
    match expiry {
        ExpiryChange::Keep => {}
        ExpiryChange::Set(expires_at) => {
            check_expiry(&name, now, expires_at)?;
            metadata.set_expires_at(Some(expires_at));
        }
        ExpiryChange::Clear => metadata.set_expires_at(None),
    }
    let version = current.saturating_add(1);
    metadata.set_version(version);

    database_mut(state, &name)?.set_metadata(metadata);
    Ok(Applied::DatabaseUpdated {
        database: name,
        version,
    })
}

/// Drops a database whose expiry is at or before `now`, as `DropDatabase` drops one.
fn expire_database(state: &mut State, now: Timestamp, name: DatabaseName) -> Outcome {
    let db = state.database_or_reject(&name)?;
    let expires_at = db.metadata().and_then(Metadata::expires_at);
    let Some(expires_at) = expires_at.filter(|expires_at| *expires_at <= now) else {
        return Err(Rejection::NotExpired {
            database: name,
            expires_at,
        });
    };
    let Some(database) = remove_database(state, now, &name, CancelCause::Expired) else {
        return Err(Rejection::UnknownDatabase { database: name });
    };
    Ok(Applied::DatabaseExpired {
        database: name,
        epoch: database.epoch(),
        expires_at,
    })
}

fn change_durability(state: &mut State, database: DatabaseName, durability: Durability) -> Outcome {
    let db = state.database_or_reject(&database)?;
    if !db.is_ready() {
        return Err(Rejection::ForkNotReady { database });
    }
    durability
        .check(db.replication_factor())
        .map_err(|source| Rejection::InvalidDurability {
            database: database.clone(),
            source,
        })?;
    let current = db.durability();
    let db = database_mut(state, &database)?;
    db.set_pending_durability(durability);
    Ok(Applied::DurabilityChanged {
        database,
        durability,
        current,
    })
}

fn drop_database(state: &mut State, now: Timestamp, name: DatabaseName) -> Outcome {
    let Some(database) = remove_database(state, now, &name, CancelCause::Dropped) else {
        return Err(Rejection::UnknownDatabase { database: name });
    };
    Ok(Applied::DatabaseDropped {
        database: name,
        epoch: database.epoch(),
    })
}

/// Removes the database, with its lease and any promotion, and every member's report of it, so
/// that no stale report outlives it; returns it, or `None` if there is no such database. Reports
/// are replaced wholesale by each heartbeat anyway; dropping them here keeps a snapshot taken
/// right after the drop identical to one taken after the next heartbeats. A fork dropped before
/// it was ready has its operation cancelled for `cause` (ADR-0046, point 3.8).
fn remove_database(
    state: &mut State,
    now: Timestamp,
    name: &DatabaseName,
    cause: CancelCause,
) -> Option<Database> {
    let database = state.databases.remove(name)?;
    for entry in state.members.values_mut() {
        entry.reports.remove(name);
    }
    forks::dropped(state, &database, now, cause);
    Some(database)
}

fn acquire_lease(
    state: &mut State,
    now: Timestamp,
    database: DatabaseName,
    node: NodeId,
) -> Outcome {
    let db = state.database_or_reject(&database)?;
    if db.fork().is_some_and(|fork| !fork.is_seeded()) {
        return Err(Rejection::LeaseOnUnseededFork { database });
    }
    if db.lease().is_some() {
        return Err(Rejection::LeaseAlreadyIssued { database });
    }
    let epoch = db
        .epoch()
        .next()
        .ok_or_else(|| Rejection::LeaseEpochExhausted {
            database: database.clone(),
        })?;
    require_member(state, &node)?;
    if !state.is_live(&node, now) {
        return Err(Rejection::MemberDown { id: node });
    }
    if state.is_draining(&node) {
        return Err(Rejection::MemberDraining { id: node });
    }
    if !state.replica_set(&database)?.contains(&node) {
        return Err(Rejection::NotInReplicaSet { id: node, database });
    }
    forks::check_lease(state, db, &node)?;
    let lease = Lease::new(
        node,
        epoch,
        now.saturating_add(state.config.lease_duration()),
    );

    let db = database_mut(state, &database)?;
    db.set_epoch(epoch);
    db.apply_pending_durability();
    db.set_lease(lease.clone());
    Ok(Applied::LeaseGranted { database, lease })
}

fn renew_lease(
    state: &mut State,
    now: Timestamp,
    database: &DatabaseName,
    node: &NodeId,
    epoch: LeaseEpoch,
) -> Outcome {
    let db = state.database_or_reject(database)?;
    let Some(lease) = db.lease() else {
        return Err(Rejection::NoLease {
            database: database.clone(),
        });
    };
    if epoch != db.epoch() || epoch != lease.epoch() {
        return Err(Rejection::StaleEpoch {
            database: database.clone(),
            given: epoch,
            current: db.epoch(),
        });
    }
    if lease.holder() != node {
        return Err(Rejection::NotLeaseHolder {
            id: node.clone(),
            database: database.clone(),
        });
    }
    require_member(state, node)?;
    let expires_at = now.saturating_add(state.config.lease_duration());

    let db = database_mut(state, database)?;
    let Some(lease) = db.lease_mut() else {
        return Err(Rejection::NoLease {
            database: database.clone(),
        });
    };
    lease.set_expires_at(expires_at);
    Ok(Applied::LeaseRenewed {
        database: database.clone(),
        lease: lease.clone(),
    })
}

fn start_promotion(state: &mut State, now: Timestamp, database: DatabaseName) -> Outcome {
    let PromotionPlan {
        epoch,
        candidate,
        position,
    } = plan_promotion(state, now, &database)?;

    let db = database_mut(state, &database)?;
    db.set_epoch(epoch);
    db.set_promotion(Some(Promotion::new(epoch, candidate.clone(), now)));
    Ok(Applied::PromotionStarted {
        database,
        epoch,
        candidate,
        position,
    })
}

/// What a promotion of one database would do if it started now.
#[derive(Debug)]
pub(crate) struct PromotionPlan {
    pub(crate) epoch: LeaseEpoch,
    pub(crate) candidate: NodeId,
    pub(crate) position: Position,
}

/// Checks whether a promotion of `database` may start at `now`, and picks its epoch and
/// candidate, without changing anything.
///
/// The candidate is the live replica-set member whose log is the most advanced: the highest log
/// epoch, then the highest position, and the earliest in preference order on ties. A draining
/// member is never the candidate.
pub(crate) fn plan_promotion(
    state: &State,
    now: Timestamp,
    database: &DatabaseName,
) -> Result<PromotionPlan, Rejection> {
    let config = state.config;
    let db = state.database_or_reject(database)?;
    let Some(lease) = db.lease() else {
        return Err(Rejection::NoLease {
            database: database.clone(),
        });
    };
    let available_after = lease.expires_at().saturating_add(config.failover_margin());
    if now <= available_after {
        return Err(Rejection::LeaseNotExpired {
            database: database.clone(),
            available_after,
        });
    }
    promotion_in_progress(state, now, db)?;
    let epoch = next_epoch(db)?;
    let (candidate, position) = best_candidate(state, now, database, None)?;
    Ok(PromotionPlan {
        epoch,
        candidate,
        position,
    })
}

/// Rejects a new promotion while one is in progress whose candidate is live and hasn't stalled.
fn promotion_in_progress(state: &State, now: Timestamp, db: &Database) -> Result<(), Rejection> {
    let config = state.config;
    if let Some(promotion) = db.promotion() {
        let stalled_after = promotion.started_at().saturating_add(
            config
                .lease_duration()
                .saturating_add(config.failover_margin()),
        );
        if state.is_live(promotion.candidate(), now) && now <= stalled_after {
            return Err(Rejection::PromotionInProgress {
                database: db.name().clone(),
            });
        }
    }
    Ok(())
}

fn next_epoch(db: &Database) -> Result<LeaseEpoch, Rejection> {
    db.epoch()
        .next()
        .ok_or_else(|| Rejection::LeaseEpochExhausted {
            database: db.name().clone(),
        })
}

/// Picks the live replica-set member of `database` with the most advanced log, by log epoch and
/// then position, preferring the earliest in preference order on ties. A draining member is
/// skipped, and so is `excluded`: a transfer never hands a lease back to its holder.
fn best_candidate(
    state: &State,
    now: Timestamp,
    database: &DatabaseName,
    excluded: Option<&NodeId>,
) -> Result<(NodeId, Position), Rejection> {
    let mut best: Option<(&NodeId, LeaseEpoch, &Position)> = None;
    let db = state.database_or_reject(database)?;
    let replicas = state.replica_set(database)?;
    for id in &replicas {
        if Some(id) == excluded || !state.is_live(id, now) || state.is_draining(id) {
            continue;
        }
        let Some(rank) = state
            .report(id, database)
            .and_then(|report| crate::fork::rules::candidate_rank(db, report))
        else {
            continue;
        };
        // Strictly greater, so the earliest in preference order wins ties (ADR-0018, point 7).
        if best.is_none_or(|(_, log_epoch, position)| rank > (log_epoch, position)) {
            best = Some((id, rank.0, rank.1));
        }
    }
    let Some((candidate, _, position)) = best else {
        return Err(Rejection::NoCandidate {
            database: database.clone(),
        });
    };
    Ok((candidate.clone(), position.clone()))
}

fn revoke_token(state: &mut State, now: Timestamp, revoked: Revoked, until: Timestamp) -> Outcome {
    if until <= now {
        return Err(Rejection::RevocationPassed { revoked, until });
    }
    state.revocations.forget_through(now);
    let until = state.revocations.insert(revoked.clone(), until);
    Ok(Applied::TokenRevoked { revoked, until })
}

fn drain(state: &mut State, id: NodeId) -> Outcome {
    let Some(entry) = state.members.get_mut(&id) else {
        return Err(Rejection::UnknownMember { id });
    };
    entry.draining = true;
    Ok(Applied::MemberDraining { id })
}

fn undrain(state: &mut State, id: NodeId) -> Outcome {
    let Some(entry) = state.members.get_mut(&id) else {
        return Err(Rejection::UnknownMember { id });
    };
    entry.draining = false;
    Ok(Applied::MemberUndrained { id })
}

/// Starts a promotion for `database` at the next epoch without waiting for its lease to expire,
/// because the holder is draining. The candidate is `target` if given, which must be a live
/// replica-set member that isn't draining, and otherwise the most advanced such member.
fn transfer_lease(
    state: &mut State,
    now: Timestamp,
    database: DatabaseName,
    target: Option<NodeId>,
) -> Outcome {
    let db = state.database_or_reject(&database)?;
    let Some(lease) = db.lease() else {
        return Err(Rejection::NoLease { database });
    };
    let holder = lease.holder().clone();
    if !state.is_draining(&holder) {
        return Err(Rejection::HolderNotDraining {
            id: holder,
            database,
        });
    }
    promotion_in_progress(state, now, db)?;
    let epoch = next_epoch(db)?;
    let (candidate, position) = match target {
        Some(target) => {
            require_member(state, &target)?;
            if !state.replica_set(&database)?.contains(&target) {
                return Err(Rejection::NotInReplicaSet {
                    id: target,
                    database,
                });
            }
            if target == holder || state.is_draining(&target) {
                return Err(Rejection::MemberDraining { id: target });
            }
            if !state.is_live(&target, now) {
                return Err(Rejection::MemberDown { id: target });
            }
            let position = state
                .report(&target, &database)
                .map(|report| report.position().clone())
                .unwrap_or_default();
            (target, position)
        }
        None => best_candidate(state, now, &database, Some(&holder))?,
    };

    let db = database_mut(state, &database)?;
    db.set_epoch(epoch);
    db.set_promotion(Some(
        Promotion::new(epoch, candidate.clone(), now).handed_over_by(holder),
    ));
    Ok(Applied::PromotionStarted {
        database,
        epoch,
        candidate,
        position,
    })
}

fn record_fenced(
    state: &mut State,
    database: DatabaseName,
    node: NodeId,
    epoch: LeaseEpoch,
    position: Position,
) -> Outcome {
    let db = state.database_or_reject(&database)?;
    let promotion = current_promotion(db, &database, epoch)?;
    require_member(state, &node)?;
    let replicas = state.replica_set(&database)?;
    if !replicas.contains(&node) {
        return Err(Rejection::NotInReplicaSet { id: node, database });
    }
    if promotion.fenced_position(&node).is_some() {
        return Err(Rejection::AlreadyFenced { id: node, database });
    }
    let fenced = promotion
        .fenced()
        .filter(|(id, _)| replicas.contains(id))
        .count()
        .saturating_add(1);
    let fenced = u8::try_from(fenced).unwrap_or(u8::MAX);
    let needed = quorums_for(replicas.len(), db.durability().acks()).promotion();

    let db = database_mut(state, &database)?;
    let Some(promotion) = db.promotion_mut() else {
        return Err(Rejection::NoPromotion { database });
    };
    promotion.record_fenced(node.clone(), position);
    Ok(Applied::FenceRecorded {
        database,
        node,
        fenced,
        needed,
    })
}

fn complete_promotion(
    state: &mut State,
    now: Timestamp,
    database: DatabaseName,
    epoch: LeaseEpoch,
) -> Outcome {
    let db = state.database_or_reject(&database)?;
    let promotion = current_promotion(db, &database, epoch)?;
    let candidate = promotion.candidate().clone();
    if !state.is_live(&candidate, now) {
        return Err(Rejection::CandidateDown { id: candidate });
    }
    let replicas = state.replica_set(&database)?;
    if !replicas.contains(&candidate) {
        return Err(Rejection::NotInReplicaSet {
            id: candidate,
            database,
        });
    }
    if promotion.fenced_position(&candidate).is_none() {
        return Err(Rejection::CandidateNotFenced { id: candidate });
    }
    // The promotion fences under the settings the old epoch's writes were acknowledged with, so
    // every write it acknowledged reaches a fenced member; the new settings apply from the new
    // lease on.
    let quorums = quorums_for(replicas.len(), db.durability().acks());
    let fenced: Vec<&Position> = promotion
        .fenced()
        .filter(|(id, _)| replicas.contains(id))
        .map(|(_, position)| position)
        .collect();
    let fenced_count = u8::try_from(fenced.len()).unwrap_or(u8::MAX);
    if fenced_count < quorums.promotion() {
        return Err(Rejection::QuorumNotReached {
            fenced: fenced_count,
            needed: quorums.promotion(),
        });
    }
    let adopted = fenced.into_iter().max().cloned().unwrap_or_default();
    let lease = Lease::new(
        candidate,
        epoch,
        now.saturating_add(state.config.lease_duration()),
    );

    let db = database_mut(state, &database)?;
    db.apply_pending_durability();
    db.set_lease(lease.clone());
    db.set_promotion(None);
    Ok(Applied::PromotionCompleted {
        database,
        lease,
        fenced: fenced_count,
        quorums,
        adopted,
    })
}

/// Returns the database's promotion if `epoch` is its epoch.
fn current_promotion<'db>(
    db: &'db Database,
    database: &DatabaseName,
    epoch: LeaseEpoch,
) -> Result<&'db Promotion, Rejection> {
    let Some(promotion) = db.promotion() else {
        return Err(Rejection::NoPromotion {
            database: database.clone(),
        });
    };
    if promotion.epoch() != epoch {
        return Err(Rejection::StaleEpoch {
            database: database.clone(),
            given: epoch,
            current: promotion.epoch(),
        });
    }
    Ok(promotion)
}

fn require_member(state: &State, id: &NodeId) -> Result<(), Rejection> {
    if state.members.contains_key(id) {
        Ok(())
    } else {
        Err(Rejection::UnknownMember { id: id.clone() })
    }
}

fn database_mut<'s>(
    state: &'s mut State,
    database: &DatabaseName,
) -> Result<&'s mut Database, Rejection> {
    state
        .databases
        .get_mut(database)
        .ok_or_else(|| Rejection::UnknownDatabase {
            database: database.clone(),
        })
}

fn next_membership_epoch(epoch: Epoch) -> Result<Epoch, Rejection> {
    epoch
        .get()
        .checked_add(1)
        .map(Epoch::new)
        .ok_or(Rejection::MembershipEpochExhausted)
}
