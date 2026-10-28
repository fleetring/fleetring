//! Cluster members: placement attributes, liveness, and what each member reports.

use fleetring::{Host, Node, NodeId, Weight, Zone};
use serde::{Deserialize, Serialize};

use crate::{DatabaseName, Position, Timestamp};

/// A member's placement attributes, the ID, zone, host, and weight that the ring places by, and
/// the address its peers reach it at.
///
/// The address isn't a placement attribute: [`Member::to_node`] leaves it out, so it never moves
/// a key. It is still part of the membership, so a member that rejoins with a new address bumps
/// the membership epoch, and every node learns the address with the next membership update. It
/// arrives inside the `Join` command, so replaying the log restores it.
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
        }
    }
}

/// Whether a member has sent a heartbeat recently enough to count as up.
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
pub enum Role {
    /// The member accepts writes for the database.
    Primary,
    /// The member stores the database's log as a standby.
    Follower,
}

/// One database a member hosts, as reported in a heartbeat.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HostedReplica {
    database: DatabaseName,
    role: Role,
    position: Position,
}

impl HostedReplica {
    /// Describes a replica of `database` that plays `role` and has stored the log up to
    /// `position`.
    #[must_use]
    pub fn new(database: DatabaseName, role: Role, position: Position) -> Self {
        Self {
            database,
            role,
            position,
        }
    }

    /// Returns the database.
    #[must_use]
    pub fn database(&self) -> &DatabaseName {
        &self.database
    }

    /// Returns the role the member plays for the database.
    #[must_use]
    pub fn role(&self) -> Role {
        self.role
    }

    /// Returns the position the member has stored the log up to.
    #[must_use]
    pub fn position(&self) -> &Position {
        &self.position
    }

    pub(crate) fn into_report(self, at: Timestamp) -> (DatabaseName, Report) {
        (
            self.database,
            Report {
                role: self.role,
                position: self.position,
                at,
            },
        )
    }
}

/// What a member last reported about one database it hosts.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Report {
    role: Role,
    position: Position,
    at: Timestamp,
}

impl Report {
    /// Returns the role the member reported.
    #[must_use]
    pub fn role(&self) -> Role {
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
}
