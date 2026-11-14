//! Control state and the queries the control plane answers from it.

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use fleetring::{BuildRingError, Epoch, Node, NodeId, ReplicationFactor, Ring};
use serde::{Deserialize, Serialize};

use crate::apply::{check_join_software, plan_promotion};
use crate::delegation::apply::{self as delegations, Resend, Times};
use crate::delegation::{Delegation, DelegationId, DelegationOperation, RateKey};
use crate::fork::apply::{live_operation, resend};
use crate::fork::{ForkState, OperationId, OperationRecord};
use crate::{
    Acks, Action, ControlConfig, Database, DatabaseName, Feature, Liveness, Member, Quorums,
    Rejection, Report, Revocations, Timestamp,
};

/// Everything the control plane knows: members, their liveness, and databases with their leases.
///
/// A state changes only through [`apply`](crate::apply). It serializes with serde so the control
/// node can snapshot it; deserializing a snapshot restores it exactly. Placement is never stored:
/// [`State::ring`] computes it from the membership.
///
/// # Examples
///
/// ```
/// use fleetring_control::{apply, Action, Command, ControlConfig, Member, State, Timestamp};
///
/// let mut state = State::new(ControlConfig::new());
/// assert!(state.ring().is_err());
/// let join = Action::Join { member: Member::new("a".parse()?) };
/// apply(&mut state, Command::new(Timestamp::from_millis(5), join))?;
/// assert_eq!(state.ring()?.node_count(), 1);
/// assert_eq!(state.membership_epoch().get(), 1);
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct State {
    pub(crate) config: ControlConfig,
    pub(crate) now: Timestamp,
    pub(crate) membership_epoch: Epoch,
    pub(crate) members: BTreeMap<NodeId, MemberEntry>,
    pub(crate) databases: BTreeMap<DatabaseName, Database>,
    /// Revoked bearer tokens and signing keys. Absent from states written before revocation.
    #[serde(default, skip_serializing_if = "Revocations::is_empty")]
    pub(crate) revocations: Revocations,
    /// The fleet features enabled for good. Absent from states written before features.
    ///
    /// Unlike a member's [`Software`](crate::Software), this decodes strictly: a release that
    /// doesn't know an enabled feature mustn't run the fleet, so its control node fails to load
    /// the state rather than drop the name (ADR-0041, amendment 1).
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    pub(crate) features: BTreeSet<Feature>,
    /// Fork operations, by ID, until their retention has passed (ADR-0046, point 5.3). Absent
    /// until the first fork.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub(crate) fork_operations: BTreeMap<OperationId, OperationRecord>,
    /// Recorded delegations, by delegation ID, until each is forgotten or revoked (ADR-0047,
    /// point 6.1). Absent until the first delegation.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub(crate) delegations: BTreeMap<DelegationId, Delegation>,
    /// Delegation operations, by ID, separate from fork operations (ADR-0047, point 6.2).
    /// Absent until the first delegation.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub(crate) delegation_operations: BTreeMap<OperationId, DelegationOperation>,
    /// The effective times of the commands each rate window counts, oldest first (ADR-0047,
    /// point 6.2). Absent until the first delegation.
    #[serde(
        default,
        skip_serializing_if = "BTreeMap::is_empty",
        with = "rate_windows_as_pairs"
    )]
    pub(crate) rate_windows: BTreeMap<RateKey, VecDeque<Timestamp>>,
}

/// The rate windows as `[key, times]` pairs in key order: JSON map keys must be strings, and a
/// [`RateKey`] is a structure.
mod rate_windows_as_pairs {
    use std::collections::{BTreeMap, VecDeque};

    use serde::{Deserialize, Deserializer, Serializer};

    use crate::Timestamp;
    use crate::delegation::RateKey;

    pub(super) fn serialize<S: Serializer>(
        windows: &BTreeMap<RateKey, VecDeque<Timestamp>>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        serializer.collect_seq(windows.iter())
    }

    pub(super) fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<BTreeMap<RateKey, VecDeque<Timestamp>>, D::Error> {
        let pairs = Vec::<(RateKey, VecDeque<Timestamp>)>::deserialize(deserializer)?;
        Ok(pairs.into_iter().collect())
    }
}

/// A member with its liveness and its latest reports.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct MemberEntry {
    pub(crate) member: Member,
    pub(crate) last_heartbeat: Timestamp,
    pub(crate) reports: BTreeMap<DatabaseName, Report>,
    /// Whether the member is handing its leases over before it leaves. Absent from states
    /// written before members could drain.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub(crate) draining: bool,
    /// The free bytes on the member's data volume, as its last heartbeat said once the `fork`
    /// feature was enabled. Absent until then.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) disk_free_bytes: Option<u64>,
}

impl State {
    /// Returns an empty state that runs under `config` for its whole life.
    #[must_use]
    pub fn new(config: ControlConfig) -> Self {
        Self {
            config,
            now: Timestamp::ZERO,
            membership_epoch: Epoch::default(),
            members: BTreeMap::new(),
            databases: BTreeMap::new(),
            revocations: Revocations::new(),
            features: BTreeSet::new(),
            fork_operations: BTreeMap::new(),
            delegations: BTreeMap::new(),
            delegation_operations: BTreeMap::new(),
            rate_windows: BTreeMap::new(),
        }
    }

    /// Returns the fleet features enabled for good, in order (ADR-0038, point 4).
    pub fn features(&self) -> impl ExactSizeIterator<Item = Feature> + '_ {
        self.features.iter().copied()
    }

    /// Returns whether `feature` is enabled: a command that needed it was accepted once every
    /// member supported it.
    #[must_use]
    pub fn is_enabled(&self, feature: Feature) -> bool {
        self.features.contains(&feature)
    }

    /// Checks `action` against the feature gate alone: it passes if each feature it needs is
    /// enabled, or supported by every member, live or down, of a fleet that has at least one.
    ///
    /// [`apply`](crate::apply) makes the same check. The control node also makes it before it
    /// logs a command, so that a refused command never reaches the log, where a control node of
    /// an older release, after a rollback, couldn't read it.
    ///
    /// # Errors
    ///
    /// [`Rejection::FeatureUnsupported`] naming the first feature, in [`Action::features`]'s
    /// order, that isn't enabled and that a member lacks, and the first member, in ID order,
    /// that lacks it; [`Rejection::FeatureWithoutMembers`] for a feature that isn't enabled
    /// while the fleet has no members.
    ///
    /// # Examples
    ///
    /// ```
    /// use fleetring_control::{
    ///     apply, Action, Command, ControlConfig, Feature, Member, Rejection, Release, Software,
    ///     State, Timestamp,
    /// };
    ///
    /// let mut state = State::new(ControlConfig::new());
    /// let at = Timestamp::from_millis(0);
    /// let new = Software::new(Release::new(0, 4, 0), 1).with_feature(Feature::Undrain);
    /// let join = |id: &str, software: Option<Software>| -> Result<Action, Box<dyn std::error::Error>> {
    ///     let member = Member::new(id.parse()?);
    ///     Ok(Action::Join { member: match software { Some(s) => member.with_software(s), None => member } })
    /// };
    /// apply(&mut state, Command::new(at, join("a", Some(new.clone()))?))?;
    /// apply(&mut state, Command::new(at, join("b", None)?))?; // 0.3.0 reports nothing
    /// let undrain = Action::Undrain { id: "a".parse()? };
    /// assert!(matches!(
    ///     state.check_features(&undrain),
    ///     Err(Rejection::FeatureUnsupported { feature: Feature::Undrain, .. })
    /// ));
    /// apply(&mut state, Command::new(at, join("b", Some(new))?))?;
    /// assert!(state.check_features(&undrain).is_ok());
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn check_features(&self, action: &Action) -> Result<(), Rejection> {
        action
            .features()
            .iter()
            .try_for_each(|feature| self.check_feature(*feature))
    }

    /// Checks every rule [`apply`](crate::apply) refuses `action` for because of software: the
    /// feature gate ([`State::check_features`]) and, for a join, the release window and the
    /// enabled features (ADR-0038, point 3). A join on the software the member is recorded with
    /// always passes, as `apply` lets it rejoin.
    ///
    /// The control node makes this check before it logs a command, so that a command refused for
    /// software never reaches the log. A control node of an older release, after a rollback,
    /// decodes the log without the software, so it would accept such a command: a refused join
    /// would make a new member there.
    ///
    /// # Errors
    ///
    /// [`Rejection::FeatureUnsupported`] as [`State::check_features`] returns it;
    /// [`Rejection::MissingFeature`] for a join that lacks an enabled feature; and
    /// [`Rejection::IncompatibleVersion`] for a join whose release line is outside the window of
    /// another member's, live or down.
    ///
    /// # Examples
    ///
    /// ```
    /// use fleetring_control::{
    ///     apply, Action, Command, ControlConfig, Member, Rejection, Release, Software, State,
    ///     Timestamp,
    /// };
    ///
    /// let mut state = State::new(ControlConfig::new());
    /// let join = Action::Join { member: Member::new("old".parse()?) }; // 0.3.0 reports nothing
    /// apply(&mut state, Command::new(Timestamp::from_millis(0), join))?;
    /// let far = Action::Join {
    ///     member: Member::new("far".parse()?).with_software(Software::new(Release::new(0, 5, 0), 1)),
    /// };
    /// assert!(matches!(
    ///     state.check_admission(&far),
    ///     Err(Rejection::IncompatibleVersion { .. })
    /// ));
    /// assert!(state.check_admission(&Action::Tick).is_ok());
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn check_admission(&self, action: &Action) -> Result<(), Rejection> {
        self.check_features(action)?;
        if let Action::Join { member } = action {
            check_join_software(self, member)?;
        }
        Ok(())
    }

    /// Checks one fleet feature against the gate, as [`State::check_features`] checks each
    /// feature an action needs: it passes if `feature` is enabled, or supported by every member,
    /// live or down, of a fleet that has at least one.
    ///
    /// A control node asks it before it has built the command a request would propose, so that
    /// a request for a feature the fleet can't use yet is refused before anything else is
    /// resolved (ADR-0047, point 5.11).
    ///
    /// # Errors
    ///
    /// [`Rejection::FeatureUnsupported`] naming the first member, in ID order, that lacks the
    /// feature while it isn't enabled; [`Rejection::FeatureWithoutMembers`] while it isn't
    /// enabled and the fleet has no members.
    pub fn check_feature(&self, feature: Feature) -> Result<(), Rejection> {
        if self.is_enabled(feature) {
            return Ok(());
        }
        // With no members, "every member supports it" would hold of any feature, even one no
        // release reports yet, and enabling it would refuse every later join (ADR-0041,
        // amendment 3). This changes no replay: until this rule, the only gated action was
        // `Undrain`, which names a member and so was already rejected in an empty fleet.
        if self.members.is_empty() {
            return Err(Rejection::FeatureWithoutMembers { feature });
        }
        for (id, entry) in &self.members {
            let software = entry.member.running();
            if !software.supports(feature) {
                return Err(Rejection::FeatureUnsupported {
                    feature,
                    id: id.clone(),
                    release: software.release().clone(),
                });
            }
        }
        Ok(())
    }

    /// Returns the timing rules the state runs under.
    #[must_use]
    pub fn config(&self) -> &ControlConfig {
        &self.config
    }

    /// Returns the effective time of the last accepted command.
    ///
    /// A command stamped earlier than this runs at this time instead, so time never goes
    /// backwards inside the state.
    #[must_use]
    pub fn now(&self) -> Timestamp {
        self.now
    }

    /// Returns the denylist of revoked bearer tokens and signing keys.
    #[must_use]
    pub fn revocations(&self) -> &Revocations {
        &self.revocations
    }

    /// Returns the membership epoch, which grows with every change to the set of members or
    /// their placement attributes.
    #[must_use]
    pub fn membership_epoch(&self) -> Epoch {
        self.membership_epoch
    }

    /// Returns every member, in ID order.
    pub fn members(&self) -> impl ExactSizeIterator<Item = &Member> {
        self.members.values().map(|entry| &entry.member)
    }

    /// Returns the member with `id`, if it is in the fleet.
    #[must_use]
    pub fn member(&self, id: &NodeId) -> Option<&Member> {
        self.members.get(id).map(|entry| &entry.member)
    }

    /// Returns when the member with `id` last joined or sent a heartbeat.
    #[must_use]
    pub fn last_heartbeat(&self, id: &NodeId) -> Option<Timestamp> {
        self.members.get(id).map(|entry| entry.last_heartbeat)
    }

    /// Returns whether the member with `id` is live at `at`, or `None` if it isn't a member.
    ///
    /// A member is down once `at` is later than its last heartbeat plus the heartbeat timeout.
    #[must_use]
    pub fn liveness(&self, id: &NodeId, at: Timestamp) -> Option<Liveness> {
        self.members
            .get(id)
            .map(|entry| self.liveness_of(entry, at))
    }

    /// Returns whether the member with `id` is draining: handing its leases over before it
    /// leaves. A draining member gets no new lease and is never a promotion's candidate. A node
    /// that isn't a member isn't draining.
    #[must_use]
    pub fn is_draining(&self, id: &NodeId) -> bool {
        self.members.get(id).is_some_and(|entry| entry.draining)
    }

    /// Returns what the member with `id` last reported about `database`.
    #[must_use]
    pub fn report(&self, id: &NodeId, database: &DatabaseName) -> Option<&Report> {
        self.members.get(id)?.reports.get(database)
    }

    /// Returns the free bytes on the member's data volume as its last heartbeat said, if it said
    /// and the `fork` feature was enabled by then.
    #[must_use]
    pub fn disk_free_bytes(&self, id: &NodeId) -> Option<u64> {
        self.members.get(id)?.disk_free_bytes
    }

    /// Returns the record of fork operation `operation`, unless it is unknown or past its
    /// retention at `at`, or at [`State::now`] if `at` is earlier (ADR-0046, point 5.3).
    #[must_use]
    pub fn fork_operation(
        &self,
        operation: &OperationId,
        at: Timestamp,
    ) -> Option<&OperationRecord> {
        live_operation(self, operation, at.max(self.now))
    }

    /// Returns the record of fork operation `operation` as [`State::fork_operation`] does, but
    /// only while the database it made exists at the incarnation it made: what a principal with
    /// `admin` on the target, rather than the requester, may read (ADR-0046, point 5.4). Once the
    /// target is dropped and its name reused, `admin` on the new database reveals nothing about
    /// the earlier operation.
    #[must_use]
    pub fn fork_operation_of_target(
        &self,
        operation: &OperationId,
        at: Timestamp,
    ) -> Option<&OperationRecord> {
        self.fork_operation(operation, at).filter(|record| {
            self.database(record.target())
                .is_some_and(|database| database.incarnation() == record.target_incarnation())
        })
    }

    /// Returns every fork operation's record, in ID order, including any past its retention
    /// that no `Tick` has forgotten yet.
    pub fn fork_operations(
        &self,
    ) -> impl ExactSizeIterator<Item = (&OperationId, &OperationRecord)> {
        self.fork_operations.iter()
    }

    /// Answers `action` without applying it if it is a resend of a known fork operation:
    /// `None` for any other action, including a `ForkDatabase` with an operation ID that is
    /// unknown or past its retention at `at`; otherwise the operation's record, or why the
    /// resend is refused.
    ///
    /// `apply` answers a resend the same way and changes nothing else. The control node asks
    /// first, so that a resend is never logged (ADR-0046, point 5.2).
    ///
    /// # Errors
    ///
    /// [`Rejection::OperationNotYours`] for a resend from another requester, revealing nothing
    /// about the operation, and [`Rejection::OperationConflict`] for a different request with
    /// a known ID.
    #[must_use]
    pub fn fork_resend(
        &self,
        action: &Action,
        at: Timestamp,
    ) -> Option<Result<&OperationRecord, Rejection>> {
        let Action::ForkDatabase {
            operation, intent, ..
        } = action
        else {
            return None;
        };
        let record = self.fork_operation(operation, at)?;
        Some(resend(record, *operation, intent))
    }

    /// Returns every recorded delegation, in ID order, including any past its `kept_until` that
    /// nothing has pruned yet; [`Delegation::is_live`] tells which are live (ADR-0047, point
    /// 6.1).
    pub fn delegations(&self) -> impl ExactSizeIterator<Item = (&DelegationId, &Delegation)> {
        self.delegations.iter()
    }

    /// Returns the delegation recorded as `id`, unless it is unknown or past its `kept_until` at
    /// `at`, or at [`State::now`] if `at` is earlier.
    #[must_use]
    pub fn delegation(&self, id: &DelegationId, at: Timestamp) -> Option<&Delegation> {
        let now = at.max(self.now);
        self.delegations
            .get(id)
            .filter(|record| record.is_kept(now))
    }

    /// Returns delegation operation `operation`, unless it is unknown or past its `kept_until`
    /// at `at`, or at [`State::now`] if `at` is earlier (ADR-0047, point 6.2).
    #[must_use]
    pub fn delegation_operation(
        &self,
        operation: &OperationId,
        at: Timestamp,
    ) -> Option<&DelegationOperation> {
        let now = at.max(self.now);
        self.delegation_operations
            .get(operation)
            .filter(|record| record.is_kept(now))
    }

    /// Returns every delegation operation, in ID order, including any past its `kept_until`
    /// that nothing has pruned yet (ADR-0047, point 6.2).
    pub fn delegation_operations(
        &self,
    ) -> impl ExactSizeIterator<Item = (&OperationId, &DelegationOperation)> {
        self.delegation_operations.iter()
    }

    /// Returns every rate window with the effective times it holds, oldest first, including
    /// times out of their window that nothing has pruned yet (ADR-0047, point 6.2).
    pub fn rate_windows(
        &self,
    ) -> impl ExactSizeIterator<Item = (&RateKey, impl ExactSizeIterator<Item = Timestamp> + '_)>
    {
        self.rate_windows
            .iter()
            .map(|(key, times)| (key, times.iter().copied()))
    }

    /// Answers `action` without applying it if it resends a known delegation operation: `None`
    /// for any other action, including a `RecordDelegation` whose operation is unknown or past
    /// its `kept_until`; otherwise the live delegation it recorded, with its ID, or why the
    /// resend is refused. `at` is the stamp the command would carry.
    ///
    /// [`apply`](crate::apply) answers a resend the same way and changes nothing but the time.
    /// The control node may ask first, so that it signs a resend without logging it (ADR-0047,
    /// point 5.7).
    ///
    /// # Errors
    ///
    /// [`Rejection::OperationNotYours`] for another requester, revealing nothing about the
    /// operation; [`Rejection::DelegationConflict`] for another canonical request with the
    /// same ID, or for a copy of the caller's token signed by another key than the one the
    /// delegation was minted from; [`Rejection::DelegationRevoked`] once a revocation tombstoned the operation or
    /// the denylist refuses its delegation; and [`Rejection::DelegationExpired`] once the
    /// delegation is no longer live.
    #[must_use]
    pub fn delegation_resend(
        &self,
        action: &Action,
        at: Timestamp,
    ) -> Option<Result<(&DelegationId, &Delegation), Rejection>> {
        let Action::RecordDelegation {
            operation,
            requested_by,
            request,
            caller,
            ..
        } = action
        else {
            return None;
        };
        let times = Times {
            at,
            now: at.max(self.now),
        };
        let asked = Resend {
            operation: *operation,
            requested_by,
            request,
            caller_key: &caller.kid,
        };
        delegations::resend(self, times, &asked)
    }

    /// Returns every database, in name order.
    pub fn databases(&self) -> impl ExactSizeIterator<Item = &Database> {
        self.databases.values()
    }

    /// Returns the database named `name`, if it exists.
    #[must_use]
    pub fn database(&self, name: &DatabaseName) -> Option<&Database> {
        self.databases.get(name)
    }

    /// Builds the ring over every member, live or down, at the membership epoch.
    ///
    /// # Errors
    ///
    /// Returns [`BuildRingError::NoNodes`] if the fleet has no members. `apply` rejects joins
    /// that would make any other error possible.
    pub fn ring(&self) -> Result<Ring, BuildRingError> {
        membership_ring(
            self.members.values().map(|entry| &entry.member),
            self.membership_epoch,
        )
    }

    /// Returns a database's replica set: the ring's replica set over all members, in preference
    /// order.
    ///
    /// Liveness doesn't change the replica set; it only filters candidates for leases and
    /// promotions (ADR-0006, ADR-0018).
    ///
    /// A seeded fork's replica set is frozen at its holders, in ID order, until it is ready:
    /// placement changes don't move a copy that isn't ready yet (ADR-0046, point 3.10). A ready
    /// fork keeps them until they are released, once its primary reports that placement's write
    /// quorum holds its log, and from then on it follows the ring like any database.
    ///
    /// Any other database whose replica set a membership change moved while it had a lease
    /// keeps the set it moved off, in its preference order ([`Database::kept_replicas`]), until
    /// its primary reports that placement's write quorum holds its log (ADR-0031, amendment 2):
    /// a membership change never changes the replica set promotions fence, only a release does.
    ///
    /// # Errors
    ///
    /// Returns [`Rejection::UnknownDatabase`] if there is no such database, and
    /// [`Rejection::NoMembers`] if the fleet is empty.
    pub fn replica_set(&self, database: &DatabaseName) -> Result<Vec<NodeId>, Rejection> {
        let db = self.database_or_reject(database)?;
        match db.fork().map(|fork| fork.state()) {
            Some(ForkState::Seeded { .. })
                if crate::mutation::Mutation::MoveSeededReplicaSet.is_active() =>
            {
                self.placed_replicas(db)
            }
            Some(
                ForkState::Seeded { holders, .. }
                | ForkState::Ready {
                    holders: Some(holders),
                    ..
                },
            ) => Ok(holders.iter().cloned().collect()),
            _ => match db.kept_replicas() {
                Some(kept) => Ok(kept.to_vec()),
                None => self.placed_replicas(db),
            },
        }
    }

    /// Returns where placement puts `db`: the ring's replica set over all members, ignoring a
    /// seeded fork's frozen holders and a replica set a membership change kept.
    pub(crate) fn placed_replicas(&self, db: &Database) -> Result<Vec<NodeId>, Rejection> {
        let ring = self.ring().map_err(|_| Rejection::NoMembers)?;
        Ok(ring
            .replicas_n(db.name().as_str(), db.replication_factor())
            .to_ids())
    }

    /// Returns the quorums for a database's current replica set.
    ///
    /// # Errors
    ///
    /// Fails as [`State::replica_set`] does.
    pub fn quorums(&self, database: &DatabaseName) -> Result<Quorums, Rejection> {
        let acks = self.database_or_reject(database)?.durability().acks();
        let replicas = self.replica_set(database)?;
        Ok(quorums_for(replicas.len(), acks))
    }

    /// Returns every database a `StartPromotion` stamped `at` would start a promotion for, in
    /// name order.
    ///
    /// That is every database whose lease expired more than the failover margin before `at`,
    /// with no promotion in progress, or one whose candidate is down or has stalled, and with a
    /// live candidate. The control node asks this on a timer, so that a failover starts without
    /// waiting for traffic and without logging commands that would only be rejected.
    ///
    /// # Examples
    ///
    /// ```
    /// use fleetring::ReplicationFactor;
    /// use fleetring_control::{
    ///     apply, Action, Command, ControlConfig, Engine, HostedReplica, Member, Position, ReplicaRole,
    ///     State, Timestamp, UnknownReports,
    /// };
    ///
    /// let mut state = State::new(ControlConfig::new());
    /// let run = |state: &mut State, millis, action| {
    ///     apply(state, Command::new(Timestamp::from_millis(millis), action))
    /// };
    /// for id in ["a", "b"] {
    ///     run(&mut state, 0, Action::Join { member: Member::new(id.parse()?) })?;
    /// }
    /// let db = || "tenant:acme".parse();
    /// run(&mut state, 0, Action::CreateDatabase {
    ///     name: db()?,
    ///     replication_factor: ReplicationFactor::new(2).ok_or("zero")?,
    ///     engine: Engine::Sqlite,
    ///     durability: None,
    ///     metadata: None,
    ///     incarnation: fleetring_control::Incarnation::LEGACY,
    /// })?;
    /// run(&mut state, 0, Action::AcquireLease { database: db()?, node: "a".parse()? })?;
    /// // "b" keeps reporting its copy; "a" goes quiet.
    /// let report = HostedReplica::new(db()?, ReplicaRole::Follower, Position::new([1]));
    /// let heartbeat = Action::Heartbeat {
    ///     id: "b".parse()?,
    ///     replicas: vec![report],
    ///     unknown_reports: UnknownReports::Skip,
    ///     software: None,
    ///     disk_free_bytes: None,
    /// };
    /// run(&mut state, 12_000, heartbeat)?;
    /// assert!(state.promotions_due(Timestamp::from_millis(11_000)).is_empty());
    /// assert_eq!(state.promotions_due(Timestamp::from_millis(12_001)), vec![db()?]);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    #[must_use]
    pub fn promotions_due(&self, at: Timestamp) -> Vec<DatabaseName> {
        let at = at.max(self.now);
        self.databases
            .keys()
            .filter(|name| plan_promotion(self, at, name).is_ok())
            .cloned()
            .collect()
    }

    /// Returns every database an `ExpireDatabase` stamped `at` would drop, ordered by expiry and
    /// then by name.
    ///
    /// That is every database whose expiry is at or before `at`, or before [`State::now`] if
    /// `at` is earlier, as [`apply`](crate::apply) judges it. The control node asks this on a
    /// timer and submits a command for each; `apply` decides again by the command's own time, so
    /// an expiry extended in between wins.
    ///
    /// # Examples
    ///
    /// ```
    /// use fleetring::ReplicationFactor;
    /// use fleetring_control::{
    ///     apply, Action, Command, ControlConfig, Engine, Feature, Member, MetadataSpec, Release,
    ///     Software, State, Timestamp,
    /// };
    ///
    /// let mut state = State::new(ControlConfig::new());
    /// let run = |state: &mut State, millis, action| {
    ///     apply(state, Command::new(Timestamp::from_millis(millis), action))
    /// };
    /// let software = Feature::ALL
    ///     .into_iter()
    ///     .fold(Software::new(Release::new(0, 6, 0), 1), Software::with_feature);
    /// let member = Member::new("a".parse()?).with_software(software);
    /// run(&mut state, 0, Action::Join { member })?;
    /// for (name, expires_at) in [("scratch-b", 60_000), ("scratch-a", 60_000), ("memory", 0)] {
    ///     let spec = MetadataSpec {
    ///         expires_at: (expires_at > 0).then(|| Timestamp::from_millis(expires_at)),
    ///         ..MetadataSpec::default()
    ///     };
    ///     run(&mut state, 0, Action::CreateDatabase {
    ///         name: name.parse()?,
    ///         replication_factor: ReplicationFactor::ONE,
    ///         engine: Engine::Sqlite,
    ///         durability: None,
    ///         metadata: Some(spec),
    ///         incarnation: fleetring_control::Incarnation::LEGACY,
    ///     })?;
    /// }
    /// assert!(state.expired_databases(Timestamp::from_millis(59_999)).is_empty());
    /// let due = state.expired_databases(Timestamp::from_millis(60_000));
    /// assert_eq!(due, ["scratch-a".parse()?, "scratch-b".parse()?]);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    #[must_use]
    pub fn expired_databases(&self, at: Timestamp) -> Vec<DatabaseName> {
        let at = at.max(self.now);
        let mut due: Vec<(Timestamp, &DatabaseName)> = self
            .databases
            .values()
            .filter_map(|database| {
                let expires_at = database.metadata()?.expires_at()?;
                (expires_at <= at).then_some((expires_at, database.name()))
            })
            .collect();
        due.sort_unstable();
        due.into_iter().map(|(_, name)| name.clone()).collect()
    }

    pub(crate) fn database_or_reject(&self, name: &DatabaseName) -> Result<&Database, Rejection> {
        self.databases
            .get(name)
            .ok_or_else(|| Rejection::UnknownDatabase {
                database: name.clone(),
            })
    }

    pub(crate) fn liveness_of(&self, entry: &MemberEntry, at: Timestamp) -> Liveness {
        let deadline = entry
            .last_heartbeat
            .saturating_add(self.config.heartbeat_timeout());
        if at > deadline {
            Liveness::Down
        } else {
            Liveness::Live
        }
    }

    pub(crate) fn is_live(&self, id: &NodeId, at: Timestamp) -> bool {
        self.liveness(id, at) == Some(Liveness::Live)
    }
}

/// Builds the ring the control plane places by: `members` at `epoch`, with the default strategy
/// and seed.
///
/// [`State::ring`] calls it, and so must every node that rebuilds the ring from a membership it
/// was sent, so that all of them compute the same fingerprint. The order of `members` doesn't
/// matter.
///
/// # Errors
///
/// [`BuildRingError::NoNodes`] if `members` is empty, or another [`BuildRingError`] if the
/// members can't form a ring, which `apply` never lets a membership reach.
///
/// # Examples
///
/// ```
/// use fleetring_control::{apply, membership_ring, Action, Command, ControlConfig, Member, State, Timestamp};
///
/// let mut state = State::new(ControlConfig::new());
/// let members = [Member::new("a".parse()?), Member::new("b".parse()?)];
/// for member in &members {
///     let join = Action::Join { member: member.clone() };
///     apply(&mut state, Command::new(Timestamp::from_millis(0), join))?;
/// }
/// let rebuilt = membership_ring(members.iter().rev(), state.membership_epoch())?;
/// assert_eq!(rebuilt.fingerprint(), state.ring()?.fingerprint());
/// assert_eq!(rebuilt.epoch(), state.membership_epoch());
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
pub fn membership_ring<'a>(
    members: impl IntoIterator<Item = &'a Member>,
    epoch: Epoch,
) -> Result<Ring, BuildRingError> {
    build_ring(members.into_iter().map(Member::to_node), epoch)
}

/// Builds a ring over `nodes` at `epoch` with the default strategy and seed.
pub(crate) fn build_ring(
    nodes: impl IntoIterator<Item = Node>,
    epoch: Epoch,
) -> Result<Ring, BuildRingError> {
    Ring::builder().epoch(epoch).nodes(nodes).build()
}

/// Returns the quorums for a replica set of `len` members in `acks`. `len` is at least 1 and at
/// most 255 because it is bounded by a [`ReplicationFactor`].
pub(crate) fn quorums_for(len: usize, acks: Acks) -> Quorums {
    let size = u8::try_from(len)
        .ok()
        .and_then(ReplicationFactor::new)
        .unwrap_or(ReplicationFactor::ONE);
    Quorums::with_acks(size, acks)
}
