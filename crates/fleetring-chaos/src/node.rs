//! The five nodes of a run, and the names and addresses the harness gives them (ADR-0036,
//! point 2).

use std::fmt;
use std::net::Ipv4Addr;

use fleetring::NodeId;
use fleetringd::NodeRole;

/// The Docker network the nodes share.
pub const NETWORK: &str = "fleetring-chaos";
/// The network's subnet.
pub const SUBNET: &str = "10.77.0.0/24";
/// The network's gateway: the host's address on it.
pub const GATEWAY: &str = "10.77.0.1";
/// The port every node serves on.
pub const PORT: u16 = 7400;
/// The fleet's name, which its certificates carry.
pub const FLEET: &str = "chaos";
/// The principal the harness's clients act as.
pub const PRINCIPAL: &str = "chaos";
/// The image the nodes run unless `--image` names another.
pub const DEFAULT_IMAGE: &str = "fleetring-chaos:local";

/// One of the five nodes: its name, container, address, and node role.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Node {
    /// `n1`, the control node.
    N1,
    /// `n2`, a data node.
    N2,
    /// `n3`, a data node.
    N3,
    /// `n4`, a data node.
    N4,
    /// `n5`, a data node.
    N5,
}

impl Node {
    /// Every node, in order.
    pub const ALL: [Node; 5] = [Node::N1, Node::N2, Node::N3, Node::N4, Node::N5];

    /// The control node: `n1`, which runs node role `both`.
    pub const CONTROL: Node = Node::N1;

    /// Returns the node's ID: `n1` to `n5`.
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            Self::N1 => "n1",
            Self::N2 => "n2",
            Self::N3 => "n3",
            Self::N4 => "n4",
            Self::N5 => "n5",
        }
    }

    /// Returns the name of the node's container: `fleetring-chaos-n1` to `fleetring-chaos-n5`.
    #[must_use]
    pub fn container(self) -> String {
        format!("{NETWORK}-{}", self.name())
    }

    /// Returns the node's address on the network: `10.77.0.11` to `10.77.0.15`.
    #[must_use]
    pub fn ip(self) -> Ipv4Addr {
        let host = match self {
            Self::N1 => 11,
            Self::N2 => 12,
            Self::N3 => 13,
            Self::N4 => 14,
            Self::N5 => 15,
        };
        Ipv4Addr::new(10, 77, 0, host)
    }

    /// Returns the address peers and clients dial the node at, as `host:port`.
    #[must_use]
    pub fn address(self) -> String {
        format!("{}:{PORT}", self.ip())
    }

    /// Returns the node's role: `both` for the control node, `data` for the others.
    #[must_use]
    pub fn role(self) -> NodeRole {
        match self {
            Self::N1 => NodeRole::Both,
            Self::N2 | Self::N3 | Self::N4 | Self::N5 => NodeRole::Data,
        }
    }

    /// Returns the node whose ID is `id`, or `None` if it isn't one of the five.
    #[must_use]
    pub fn from_node_id(id: &NodeId) -> Option<Node> {
        Self::ALL
            .into_iter()
            .find(|node| node.name() == id.as_str())
    }
}

impl fmt::Display for Node {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nodes_have_distinct_names_and_addresses() {
        for (index, node) in Node::ALL.into_iter().enumerate() {
            let number = index + 1;
            assert_eq!(node.name(), format!("n{number}"));
            assert_eq!(node.container(), format!("fleetring-chaos-n{number}"));
            assert_eq!(node.address(), format!("10.77.0.1{number}:7400"));
            let id: NodeId = node.name().parse().expect("node ID");
            assert_eq!(Node::from_node_id(&id), Some(node));
        }
        let other: NodeId = "n6".parse().expect("node ID");
        assert_eq!(Node::from_node_id(&other), None);
    }

    #[test]
    fn only_the_control_node_runs_both_roles() {
        assert_eq!(Node::CONTROL.role(), NodeRole::Both);
        for node in &Node::ALL[1..] {
            assert_eq!(node.role(), NodeRole::Data, "{node}");
        }
    }
}
