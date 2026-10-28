//! Commands: the only way to change control state.

use fleetring::{NodeId, ReplicationFactor};
use serde::{Deserialize, Serialize};

use crate::{DatabaseName, Engine, HostedReplica, LeaseEpoch, Member, Position, Timestamp};

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
    },

    /// Records a new database.
    CreateDatabase {
        /// The database's name.
        name: DatabaseName,
        /// How many replicas it should have.
        replication_factor: ReplicationFactor,
        /// The engine it runs on.
        engine: Engine,
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
}
