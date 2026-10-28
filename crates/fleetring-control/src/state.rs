//! Control state and the queries the control plane answers from it.

use std::collections::BTreeMap;

use fleetring::{BuildRingError, Epoch, Node, NodeId, ReplicationFactor, Ring};
use serde::{Deserialize, Serialize};

use crate::{
    ControlConfig, Database, DatabaseName, Liveness, Member, Quorums, Rejection, Report, Timestamp,
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
}

/// A member with its liveness and its latest reports.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct MemberEntry {
    pub(crate) member: Member,
    pub(crate) last_heartbeat: Timestamp,
    pub(crate) reports: BTreeMap<DatabaseName, Report>,
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
        }
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

    /// Returns the member with `id`, if it is in the cluster.
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

    /// Returns what the member with `id` last reported about `database`.
    #[must_use]
    pub fn report(&self, id: &NodeId, database: &DatabaseName) -> Option<&Report> {
        self.members.get(id)?.reports.get(database)
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
    /// Returns [`BuildRingError::NoNodes`] if the cluster has no members. `apply` rejects joins
    /// that would make any other error possible.
    pub fn ring(&self) -> Result<Ring, BuildRingError> {
        build_ring(
            self.members.values().map(|entry| entry.member.to_node()),
            self.membership_epoch,
        )
    }

    /// Returns a database's replica set: the ring's replica set over all members, in preference
    /// order.
    ///
    /// Liveness doesn't change the replica set; it only filters candidates for leases and
    /// promotions (ADR-0006, ADR-0018).
    ///
    /// # Errors
    ///
    /// Returns [`Rejection::UnknownDatabase`] if there is no such database, and
    /// [`Rejection::NoMembers`] if the cluster is empty.
    pub fn replica_set(&self, database: &DatabaseName) -> Result<Vec<NodeId>, Rejection> {
        let db = self.database_or_reject(database)?;
        let ring = self.ring().map_err(|_| Rejection::NoMembers)?;
        Ok(ring
            .replicas_n(database.as_str(), db.replication_factor())
            .to_ids())
    }

    /// Returns the quorums for a database's current replica set.
    ///
    /// # Errors
    ///
    /// Fails as [`State::replica_set`] does.
    pub fn quorums(&self, database: &DatabaseName) -> Result<Quorums, Rejection> {
        let replicas = self.replica_set(database)?;
        Ok(quorums_for(replicas.len()))
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

/// Builds a ring over `nodes` at `epoch` with the default strategy and seed.
pub(crate) fn build_ring(
    nodes: impl IntoIterator<Item = Node>,
    epoch: Epoch,
) -> Result<Ring, BuildRingError> {
    Ring::builder().epoch(epoch).nodes(nodes).build()
}

/// Returns the quorums for a replica set of `len` members, which is at least 1 and at most 255
/// because it is bounded by a [`ReplicationFactor`].
pub(crate) fn quorums_for(len: usize) -> Quorums {
    let size = u8::try_from(len)
        .ok()
        .and_then(ReplicationFactor::new)
        .unwrap_or(ReplicationFactor::ONE);
    Quorums::new(size)
}
