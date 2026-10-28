//! Liveness filters and bounded loads keep their guarantees on arbitrary rings: filters exclude
//! rejected nodes, and bounded placements never exceed capacity unless flagged as saturated
//! (ADR-0006, ADR-0007).

#![no_main]

mod common;

use std::collections::BTreeSet;

use arbitrary::Arbitrary;
use fleetring::{BoundedLoads, Headroom, Node, NodeId};
use libfuzzer_sys::fuzz_target;

#[derive(Debug, Arbitrary)]
struct Input {
    ring: common::RingInput,
    key: Vec<u8>,
    down: u64,
    loads: Vec<u64>,
    headroom: u16,
    item: u64,
}

fuzz_target!(|input: Input| {
    let Some(ring) = input.ring.build() else {
        return;
    };
    common::check_key(&ring, &input.key);

    // Liveness: bit i of `down` marks the i-th node (canonical order) as down.
    let is_down = |node: &Node| {
        let index = ring.nodes().iter().position(|n| n.id() == node.id()).expect("ring node");
        input.down.rotate_left(u32::try_from(index % 64).expect("small")) & 1 == 1
    };
    if let Some(filtered) = ring.replicas_where(&input.key, |node| !is_down(node)) {
        assert!(filtered.iter().all(|node| !is_down(node)), "filters exclude down nodes");
    }

    // Bounded loads: loads cycle over the nodes; capacity holds unless saturated.
    if input.loads.is_empty() {
        return;
    }
    let loads = ring
        .nodes()
        .iter()
        .zip(input.loads.iter().cycle())
        .map(|(node, &load)| (node.id().clone(), load));
    let policy = BoundedLoads::new(Headroom::from_percent(input.headroom));
    let view = policy.load_view(&ring, loads).expect("loads name ring nodes exactly once");
    let placed = view.replicas(&input.key, input.item);
    let replicas = placed.replicas();
    let factor = usize::from(ring.replication_factor().get());
    assert_eq!(replicas.len(), factor.min(ring.node_count()), "bounded placements are full");
    let ids: BTreeSet<&NodeId> = replicas.iter().map(Node::id).collect();
    assert_eq!(ids.len(), replicas.len(), "bounded replicas are distinct");
    if !placed.is_saturated() {
        for node in replicas {
            let load = u128::from(view.load(node.id()).expect("ring node"));
            let capacity = view.capacity(node.id(), input.item).expect("ring node");
            assert!(load + u128::from(input.item) <= capacity, "over capacity without saturation");
        }
    }
    assert_eq!(placed.is_home(), *replicas == ring.replicas(&input.key), "is_home matches home placement");
});
