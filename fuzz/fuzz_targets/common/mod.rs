//! Ring inputs the fuzzer can generate, and the placement invariants every ring must keep.

use std::collections::BTreeSet;
use std::num::NonZeroU32;

use arbitrary::Arbitrary;
use fleetring::{Node, NodeId, ReplicationFactor, Ring, Strategy, VnodeParams, Weight};

/// A ring description the fuzzer controls. Most labels are valid so most rings build, but
/// `raw_id` lets arbitrary strings exercise label validation too.
#[derive(Debug, Arbitrary)]
pub struct RingInput {
    pub nodes: Vec<NodeInput>,
    pub vnode_tokens: Option<u8>,
    pub seed: u64,
    pub replication: u8,
}

#[derive(Debug, Arbitrary)]
pub struct NodeInput {
    pub id: u16,
    pub raw_id: Option<String>,
    pub weight: u16,
    pub zone: Option<u8>,
    pub host: Option<u8>,
}

impl RingInput {
    /// Builds the ring, or returns `None` when the input is invalid in a way `build()` must
    /// reject cleanly (an error, never a panic).
    pub fn build(&self) -> Option<Ring> {
        let mut nodes = Vec::new();
        for input in self.nodes.iter().take(48) {
            let id = match &input.raw_id {
                Some(raw) => raw.parse::<NodeId>().ok()?,
                None => format!("n{}", input.id).parse().ok()?,
            };
            let mut node = Node::new(id).with_weight(Weight::new(1 + u32::from(input.weight % 512))?);
            if let Some(zone) = input.zone {
                node = node.with_zone(format!("z{}", zone % 6).parse().ok()?);
            }
            if let Some(host) = input.host {
                // Hosts are named after their zone so most inputs respect "one host, one zone";
                // the rest exercise the HostInMultipleZones error.
                node = node.with_host(format!("h{}", host % 12).parse().ok()?);
            }
            nodes.push(node);
        }
        let strategy = match self.vnode_tokens {
            None => Strategy::Rendezvous,
            Some(tokens) => {
                let per_weight = NonZeroU32::new(1 + u32::from(tokens % 8))?;
                Strategy::Vnodes(VnodeParams::new(per_weight))
            }
        };
        let replication = ReplicationFactor::new(1 + self.replication % 8)?;
        Ring::builder()
            .strategy(strategy)
            .seed(self.seed)
            .replication_factor(replication)
            .nodes(nodes)
            .build()
            .ok()
    }
}

/// Asserts the ADR-0006 invariants for one key: distinct, bounded, prefix-consistent replica
/// sets, a full preference permutation, and filtering that excludes rejected nodes.
pub fn check_key(ring: &Ring, key: &[u8]) {
    let nodes = ring.node_count();
    let factor = usize::from(ring.replication_factor().get());
    let replicas = ring.replicas(key);
    assert_eq!(replicas.len(), factor.min(nodes), "replica count");
    let ids: BTreeSet<&NodeId> = replicas.iter().map(Node::id).collect();
    assert_eq!(ids.len(), replicas.len(), "replicas are distinct");
    assert_eq!(ring.primary(key), replicas.primary(), "primary is the first replica");
    assert_eq!(replicas.is_under_replicated(), nodes < factor);

    let preference: Vec<&NodeId> = ring.preference(key).map(Node::id).collect();
    assert_eq!(preference.len(), nodes, "preference covers every node");
    let all: BTreeSet<&NodeId> = preference.iter().copied().collect();
    assert_eq!(all.len(), nodes, "preference has no duplicates");
    assert!(replicas.iter().map(Node::id).eq(preference.iter().copied().take(replicas.len())), "replicas prefix the preference list");

    let down = replicas.primary().id().clone();
    match ring.replicas_where(key, |node| node.id() != &down) {
        None => assert_eq!(nodes, 1, "only a single-node ring can lose every node"),
        Some(filtered) => {
            assert!(!filtered.contains(&down), "filtered replicas exclude the rejected node");
            assert_eq!(filtered.len(), factor.min(nodes - 1));
        }
    }
}
