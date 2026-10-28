//! Bounded-load placement through the public API (FR-011, ADR-0007).

use std::collections::BTreeSet;
use std::iter;

use fleetring::{
    BoundedLoads, Headroom, Host, LoadView, LoadViewError, Node, NodeId, ReplicationFactor, Ring,
};
use proptest::prelude::*;

use crate::common::corpus::{Corpus, MEDIUM};
use crate::common::fixtures;
use crate::common::stats::expected_shares;

type TestResult = Result<(), Box<dyn std::error::Error>>;

/// The strategies that the policy must work with, unchanged.
fn strategies() -> Vec<fleetring::Strategy> {
    vec![fleetring::Strategy::Rendezvous, crate::vnodes::vnodes(16)]
}

fn build_ring(nodes: Vec<Node>, strategy: fleetring::Strategy, replication: u8) -> Ring {
    Ring::builder()
        .strategy(strategy)
        .replication_factor(
            ReplicationFactor::new(replication).expect("test replication factors are nonzero"),
        )
        .nodes(nodes)
        .build()
        .expect("test memberships are valid")
}

/// No loads at all: every node has load 0.
fn no_loads() -> iter::Empty<(&'static str, u64)> {
    iter::empty()
}

/// Position of `node` in the ring's canonical order.
fn index_of(ring: &Ring, node: &Node) -> usize {
    ring.nodes()
        .binary_search_by(|probe| probe.id().cmp(node.id()))
        .expect("the node belongs to the ring")
}

/// Whether `node` has room for a database of `item_load`, by the view's own load and capacity.
fn has_room(view: &LoadView<'_>, node: &Node, item_load: u64) -> bool {
    let load = view
        .load(node.id())
        .expect("the node is in the view's ring");
    let capacity = view
        .capacity(node.id(), item_load)
        .expect("the node is in the view's ring");
    u128::from(load) + u128::from(item_load) <= capacity
}

/// The host a node runs on: its host label, or the node itself when it has none.
fn host_of(node: &Node) -> (Option<&Host>, Option<&NodeId>) {
    match node.host() {
        Some(host) => (Some(host), None),
        None => (None, Some(node.id())),
    }
}

/// Counts the distinct values.
fn distinct<T: Ord>(values: impl IntoIterator<Item = T>) -> usize {
    values.into_iter().collect::<BTreeSet<T>>().len()
}

/// The placement ADR-0007 defines, built from the ring's public API: the replica set among the
/// nodes with room, then the best remaining nodes of the unfiltered preference list. Returns the
/// replicas' IDs and whether any slot had to be filled that way.
fn reference_placement(view: &LoadView<'_>, key: &[u8], item_load: u64) -> (Vec<NodeId>, bool) {
    let ring = view.ring();
    let slots = ring.replicas(key).len();
    let mut ids = ring
        .replicas_where(key, |node| has_room(view, node, item_load))
        .map(|replicas| replicas.to_ids())
        .unwrap_or_default();
    let saturated = ids.len() < slots;
    for node in ring.preference(key) {
        if ids.len() == slots {
            break;
        }
        if !ids.contains(node.id()) {
            ids.push(node.id().clone());
        }
    }
    (ids, saturated)
}

/// Loads that are often tiny, so a node's load often lands right at its capacity, with both
/// extremes mixed in.
fn any_load() -> impl Strategy<Value = u64> {
    prop_oneof![0..8u64, 0..10_000u64, any::<u64>(), Just(u64::MAX)]
}

/// Database sizes from nothing to `u64::MAX`.
fn any_item_load() -> impl Strategy<Value = u64> {
    prop_oneof![0..4u64, 0..10_000u64, any::<u64>(), Just(u64::MAX)]
}

fn any_headroom() -> impl Strategy<Value = Headroom> {
    prop_oneof![Just(25u16), 0..=200u16, any::<u16>()].prop_map(Headroom::from_percent)
}

/// A random ring with loads for some of its nodes, and a database to place on it.
#[derive(Debug, Clone)]
struct Scenario {
    nodes: Vec<Node>,
    strategy: fleetring::Strategy,
    replication: u8,
    /// Loads by canonical position. `None` leaves the node out of the loads.
    loads: Vec<Option<u64>>,
    headroom: Headroom,
    item_load: u64,
    key: String,
}

impl Scenario {
    fn ring(&self) -> Ring {
        build_ring(self.nodes.clone(), self.strategy, self.replication)
    }

    /// The listed loads, in canonical order.
    fn loads(&self, ring: &Ring) -> Vec<(NodeId, u64)> {
        ring.nodes()
            .iter()
            .zip(&self.loads)
            .filter_map(|(node, load)| load.map(|load| (node.id().clone(), load)))
            .collect()
    }

    fn view<'ring>(&self, ring: &'ring Ring) -> LoadView<'ring> {
        BoundedLoads::new(self.headroom)
            .load_view(ring, self.loads(ring))
            .expect("the loads name each of the ring's nodes at most once")
    }
}

fn scenarios() -> impl Strategy<Value = Scenario> {
    (
        fixtures::memberships_up_to(16),
        proptest::sample::select(strategies()),
        1u8..=6,
        proptest::collection::vec(proptest::option::of(any_load()), 16),
        any_headroom(),
        any_item_load(),
        "[a-z]{1,12}:[0-9]{1,6}",
    )
        .prop_map(
            |(nodes, strategy, replication, loads, headroom, item_load, key)| Scenario {
                nodes,
                strategy,
                replication,
                loads,
                headroom,
                item_load,
                key,
            },
        )
}

#[test]
fn zero_loads_place_every_key_at_home() -> TestResult {
    let policy = BoundedLoads::default();
    let named = [
        ("single", fixtures::single()),
        ("homelab", fixtures::homelab()),
        ("three_zones(4)", fixtures::three_zones(4)),
        ("skewed_zones", fixtures::skewed_zones()),
        ("fleet(12, 3)", fixtures::fleet(12, 3)),
        ("fleet_1000", fixtures::fleet_1000()),
    ];
    for strategy in strategies() {
        for (label, nodes) in named.clone() {
            let ring = Ring::builder().strategy(strategy).nodes(nodes).build()?;
            let unlisted = policy.load_view(&ring, no_loads())?;
            let zeros = policy.load_view(&ring, ring.nodes().iter().map(|node| (node.id(), 0)))?;
            // Count loads: when nothing is placed yet, any node can take one database.
            let cases = [(&unlisted, 0), (&unlisted, 1), (&zeros, 0), (&zeros, 1)];
            for key in Corpus::Agents.keys(250) {
                let home = ring.replicas(&key);
                for &(view, item_load) in &cases {
                    let placed = view.replicas(&key, item_load);
                    assert!(
                        placed.is_home() && !placed.is_saturated(),
                        "{label}, item {item_load}: {placed:?}"
                    );
                    assert_eq!(placed.replicas(), &home, "{label}, item {item_load}");
                }
            }
        }
    }
    Ok(())
}

#[test]
fn databases_that_fit_nowhere_go_home_saturated() -> TestResult {
    // Equal weights give every node the same capacity, so with equal loads either every node has
    // room or none does. A database bigger than any node's capacity therefore fits nowhere.
    for strategy in strategies() {
        for nodes in [fixtures::three_zones(4), fixtures::skewed_zones()] {
            let ring = Ring::builder().strategy(strategy).nodes(nodes).build()?;
            let view = BoundedLoads::default().load_view(&ring, no_loads())?;
            for key in Corpus::Agents.keys(250) {
                for item_load in [2, 1_000_000, u64::MAX] {
                    let placed = view.replicas(&key, item_load);
                    assert!(placed.is_saturated() && placed.is_home(), "{placed:?}");
                    assert_eq!(placed.replicas(), &ring.replicas(&key));
                }
            }
        }
    }
    Ok(())
}

#[test]
fn saturated_placements_put_the_nodes_without_room_last() -> TestResult {
    // The home lab weighs 1, 2, and 4. Three replicas of 100 on an empty ring give laptop-1 a
    // capacity of ⌈1.25 · 300 · 1/7⌉ = 54, too little for the database even though it holds
    // nothing. The other two have room, so they come first, in the order failover would use.
    for strategy in strategies() {
        let ring = Ring::builder()
            .strategy(strategy)
            .nodes(fixtures::homelab())
            .build()?;
        let view = BoundedLoads::default().load_view(&ring, no_loads())?;
        assert_eq!(view.capacity("laptop-1", 100), Some(54));
        assert_eq!(view.capacity("laptop-2", 100), Some(108));
        assert_eq!(view.capacity("nas", 100), Some(215));
        for key in Corpus::Agents.keys(250) {
            let placed = view.replicas(&key, 100);
            assert!(placed.is_saturated(), "{placed:?}");
            let with_room = ring
                .replicas_where(&key, |node| node.id().as_str() != "laptop-1")
                .ok_or("two nodes have room")?;
            let ids = placed.replicas().to_ids();
            assert_eq!(ids[..2], with_room.to_ids()[..]);
            assert_eq!(ids[2].as_str(), "laptop-1");
            assert_eq!(placed.is_home(), placed.replicas() == &ring.replicas(&key));
        }
    }
    Ok(())
}

#[test]
fn views_report_loads_capacities_and_their_ring() -> TestResult {
    let ring = Ring::builder().nodes(fixtures::homelab()).build()?;
    let view = BoundedLoads::new(Headroom::from_percent(25))
        .load_view(&ring, [("nas", 30), ("laptop-1", 10)])?;
    assert!(std::ptr::eq(view.ring(), &ring));
    assert_eq!(view.load("laptop-1"), Some(10));
    assert_eq!(view.load("laptop-2"), Some(0));
    assert_eq!(view.load("nas"), Some(30));
    assert_eq!(view.load("vps"), None);
    assert_eq!(view.total_load(), 40);

    // Three replicas of 5 raise the total to 55, and the weights are 1, 2, and 4 out of 7.
    assert_eq!(view.capacity("laptop-1", 5), Some(10)); // ⌈9.82⌉
    assert_eq!(view.capacity("laptop-2", 5), Some(20)); // ⌈19.64⌉
    assert_eq!(view.capacity("nas", 5), Some(40)); // ⌈39.29⌉
    assert_eq!(view.capacity("vps", 5), None);
    assert_eq!(view.capacity("laptop-1", 0), Some(8)); // ⌈1.25 · 40 · 1/7⌉ = ⌈7.14⌉

    // laptop-1 would reach 15, over its capacity of 10, so the database fills its slot last.
    let placed = view.replicas("agent:1", 5);
    assert!(placed.is_saturated());
    assert_eq!(
        placed.replicas().get(2).map(|node| node.id().as_str()),
        Some("laptop-1")
    );
    assert_eq!(placed.clone().into_replicas(), *placed.replicas());

    let debug = format!("{view:?}");
    assert!(debug.starts_with("LoadView { ring: Ring {"), "{debug}");
    assert!(
        debug.ends_with(", headroom: Headroom(25), total_load: 40, .. }"),
        "{debug}"
    );
    Ok(())
}

#[test]
fn the_default_policy_has_25_percent_headroom() {
    assert_eq!(
        BoundedLoads::default(),
        BoundedLoads::new(Headroom::from_percent(25))
    );
    assert_eq!(BoundedLoads::default().headroom().percent(), 25);
    assert_eq!(Headroom::default().percent(), 25);
}

#[test]
fn unknown_node_ids_are_rejected() -> TestResult {
    let ring = Ring::builder().nodes(fixtures::homelab()).build()?;
    let error = BoundedLoads::default()
        .load_view(&ring, [("nas", 4), ("vps", 1)])
        .err()
        .ok_or("vps is not in the ring")?;
    assert_eq!(
        error,
        LoadViewError::UnknownNode {
            id: "vps".to_owned()
        }
    );
    assert_eq!(error.to_string(), "node id 'vps' is not in the ring");
    Ok(())
}

#[test]
fn node_ids_listed_twice_are_rejected() -> TestResult {
    let ring = Ring::builder().nodes(fixtures::homelab()).build()?;
    let error = BoundedLoads::default()
        .load_view(&ring, [("nas", 4), ("laptop-1", 2), ("nas", 4)])
        .err()
        .ok_or("nas is listed twice")?;
    assert_eq!(
        error,
        LoadViewError::DuplicateNode {
            id: "nas".to_owned()
        }
    );
    assert_eq!(
        error.to_string(),
        "node id 'nas' appears more than once in the loads"
    );
    Ok(())
}

#[test]
fn the_first_bad_id_in_the_loads_is_reported() -> TestResult {
    let ring = Ring::builder().nodes(fixtures::homelab()).build()?;
    let policy = BoundedLoads::default();
    let error = policy
        .load_view(&ring, [("nas", 1), ("vps", 2), ("nas", 3)])
        .err()
        .ok_or("the loads are invalid")?;
    assert_eq!(
        error,
        LoadViewError::UnknownNode {
            id: "vps".to_owned()
        }
    );
    let error = policy
        .load_view(&ring, [("nas", 1), ("nas", 3), ("vps", 2)])
        .err()
        .ok_or("the loads are invalid")?;
    assert_eq!(
        error,
        LoadViewError::DuplicateNode {
            id: "nas".to_owned()
        }
    );
    Ok(())
}

#[test]
fn unknown_ids_are_escaped_in_messages() -> TestResult {
    // Callers pass IDs as plain strings, which need not be valid labels.
    let ring = Ring::builder().nodes(fixtures::homelab()).build()?;
    let id = "Laptop 1\n'x'";
    let error = BoundedLoads::default()
        .load_view(&ring, [(id, 1)])
        .err()
        .ok_or("the id is not in the ring")?;
    assert_eq!(error, LoadViewError::UnknownNode { id: id.to_owned() });
    assert_eq!(
        error.to_string(),
        r"node id 'Laptop 1\n\'x\'' is not in the ring"
    );
    Ok(())
}

/// What placing a corpus one database at a time did on one ring.
#[derive(Debug)]
struct Run {
    /// Placements that put a replica on a node without room.
    saturated: usize,
    /// Placements that differ from home placement.
    away_from_home: usize,
    /// The largest ratio of a node's final load to its fair share of the final total.
    max_load_ratio: f64,
}

/// Places the first `keys` UUID keys one at a time, with count loads and 25% headroom, and checks
/// each placement against the view it was made from.
fn simulate(ring: &Ring, keys: usize) -> Result<Run, LoadViewError> {
    let policy = BoundedLoads::new(Headroom::from_percent(25));
    let replication = usize::from(ring.replication_factor().get());
    let mut loads = vec![0; ring.node_count()];
    let mut saturated = 0;
    let mut away_from_home = 0;
    for key in Corpus::Uuids.keys(keys) {
        let view = policy.load_view(
            ring,
            ring.nodes().iter().map(Node::id).zip(loads.iter().copied()),
        )?;
        if saturated == 0 {
            // Until a placement saturates, every node stays within its capacity at the current
            // total.
            for (node, &load) in ring.nodes().iter().zip(&loads) {
                let capacity = view.capacity(node.id(), 0).expect("a ring node");
                assert!(
                    u128::from(load) <= capacity,
                    "{} holds {load}, over its capacity of {capacity}",
                    node.id()
                );
            }
        }
        let room: Vec<bool> = ring
            .nodes()
            .iter()
            .map(|node| has_room(&view, node, 1))
            .collect();
        let placed = view.replicas(&key, 1);
        if placed.is_saturated() {
            saturated += 1;
        } else {
            for node in placed.replicas() {
                assert!(
                    room[index_of(ring, node)],
                    "{} has no room, yet holds a replica of {}",
                    node.id(),
                    placed.replicas()
                );
            }
        }
        // Replicas spread over as many zones as the nodes with room allow.
        let zones_with_room = ring
            .nodes()
            .iter()
            .zip(&room)
            .filter(|&(_, &room)| room)
            .map(|(node, _)| node.zone());
        let spread = replication.min(distinct(zones_with_room));
        let zones = distinct(placed.replicas().iter().take(spread).map(Node::zone));
        assert_eq!(zones, spread, "{}", placed.replicas());

        if !placed.is_home() {
            away_from_home += 1;
        }
        for node in placed.replicas() {
            loads[index_of(ring, node)] += 1;
        }
    }
    let total = to_f64(loads.iter().sum());
    let shares = expected_shares(&fixtures::weights(ring.nodes()));
    let max_load_ratio = loads
        .iter()
        .zip(&shares)
        .map(|(&load, share)| to_f64(load) / (total * share))
        .fold(0.0, f64::max);
    Ok(Run {
        saturated,
        away_from_home,
        max_load_ratio,
    })
}

/// Converts a load to `f64` exactly; simulated loads stay far below 2³².
fn to_f64(load: u64) -> f64 {
    f64::from(u32::try_from(load).expect("simulated loads fit in u32"))
}

#[test]
fn one_at_a_time_placement_keeps_every_node_within_its_capacity() -> TestResult {
    let named = [
        ("homelab", fixtures::homelab()),
        ("three_zones(4)", fixtures::three_zones(4)),
        ("fleet(12, 3)", fixtures::fleet(12, 3)),
    ];
    for strategy in strategies() {
        for (label, nodes) in named.clone() {
            let ring = Ring::builder().strategy(strategy).nodes(nodes).build()?;
            let run = simulate(&ring, MEDIUM)?;
            println!(
                "{label}, {strategy:?}: {} saturated, {} away from home, max load ratio {:.4}",
                run.saturated, run.away_from_home, run.max_load_ratio
            );
            let replication = usize::from(ring.replication_factor().get());
            if ring.node_count() >= 2 * replication {
                assert_eq!(run.saturated, 0, "{label}, {strategy:?}: {run:?}");
                // Within capacity means within ⌈1.25 × fair share⌉, and every fair share here
                // ends at 10,000 or more.
                assert!(
                    run.max_load_ratio <= 1.25 + 1e-3,
                    "{label}, {strategy:?}: {run:?}"
                );
            }
        }
    }
    Ok(())
}

proptest! {
    #[test]
    fn placements_match_the_adr_definition(scenario in scenarios()) {
        let ring = scenario.ring();
        let view = scenario.view(&ring);
        let placed = view.replicas(&scenario.key, scenario.item_load);
        let (want, saturated) =
            reference_placement(&view, scenario.key.as_bytes(), scenario.item_load);
        prop_assert_eq!(placed.replicas().to_ids(), want);
        prop_assert_eq!(placed.is_saturated(), saturated);
        let home = ring.replicas(&scenario.key);
        prop_assert_eq!(placed.is_home(), placed.replicas() == &home);
        prop_assert_eq!(placed.replicas().len(), home.len());
    }

    #[test]
    fn nodes_without_room_hold_replicas_only_in_saturated_placements(scenario in scenarios()) {
        let ring = scenario.ring();
        let view = scenario.view(&ring);
        let placed = view.replicas(&scenario.key, scenario.item_load);
        let crowded: Vec<&NodeId> = placed
            .replicas()
            .iter()
            .filter(|node| !has_room(&view, node, scenario.item_load))
            .map(Node::id)
            .collect();
        prop_assert_eq!(!crowded.is_empty(), placed.is_saturated(), "{:?}", crowded);
    }

    #[test]
    fn zones_and_hosts_spread_among_the_nodes_with_room(
        nodes in prop_oneof![
            Just(fixtures::three_zones(4)),
            Just(fixtures::fleet(12, 3)),
            Just(fixtures::skewed_zones()),
            fixtures::memberships_up_to(16),
        ],
        strategy in proptest::sample::select(strategies()),
        replication in 1u8..=6,
        loads in proptest::collection::vec(any_load(), 32),
        headroom in any_headroom(),
        item_load in any_item_load(),
        key in "[a-z]{1,12}:[0-9]{1,6}",
    ) {
        let ring = build_ring(nodes, strategy, replication);
        let view = BoundedLoads::new(headroom)
            .load_view(&ring, ring.nodes().iter().map(Node::id).zip(loads.iter().copied()))
            .expect("one load per ring node");
        let placed = view.replicas(&key, item_load);
        let replicas: Vec<&Node> = placed.replicas().iter().collect();
        let with_room: Vec<&Node> =
            ring.nodes().iter().filter(|node| has_room(&view, node, item_load)).collect();
        let r = usize::from(replication);

        // The first min(R, zones with room) replicas sit in distinct zones (ADR-0006).
        let zones = r.min(distinct(with_room.iter().map(|node| node.zone())));
        let spread = distinct(replicas[..zones].iter().map(|node| node.zone()));
        prop_assert_eq!(spread, zones, "{}", placed.replicas());

        // Likewise for hosts: the first min(R, hosts with room) replicas are on distinct hosts.
        let hosts = r.min(distinct(with_room.iter().map(|node| host_of(node))));
        let spread = distinct(replicas[..hosts].iter().map(|node| host_of(node)));
        prop_assert_eq!(spread, hosts, "{}", placed.replicas());
    }

    #[test]
    fn zero_loads_place_random_memberships_at_home(
        nodes in fixtures::memberships_up_to(16),
        strategy in proptest::sample::select(strategies()),
        replication in 1u8..=6,
        headroom in any_headroom(),
        item_load in 0..=1u64,
        key in "[a-z]{1,12}:[0-9]{1,6}",
    ) {
        let ring = build_ring(nodes, strategy, replication);
        let view = BoundedLoads::new(headroom).load_view(&ring, no_loads()).expect("no loads");
        let placed = view.replicas(&key, item_load);
        prop_assert!(placed.is_home() && !placed.is_saturated(), "{:?}", placed);
        prop_assert_eq!(placed.replicas(), &ring.replicas(&key));
    }

    #[test]
    fn views_ignore_the_order_of_their_loads(
        scenario in scenarios(),
        order in Just((0..16).collect::<Vec<usize>>()).prop_shuffle(),
    ) {
        let ring = scenario.ring();
        let loads = scenario.loads(&ring);
        let shuffled: Vec<(NodeId, u64)> =
            order.iter().filter_map(|&position| loads.get(position).cloned()).collect();
        let policy = BoundedLoads::new(scenario.headroom);
        let view = policy.load_view(&ring, loads).expect("valid loads");
        let other = policy.load_view(&ring, shuffled).expect("valid loads");
        prop_assert_eq!(view.total_load(), other.total_load());
        for node in ring.nodes() {
            prop_assert_eq!(view.load(node.id()), other.load(node.id()));
        }
        prop_assert_eq!(
            view.replicas(&scenario.key, scenario.item_load),
            other.replicas(&scenario.key, scenario.item_load)
        );
    }

    #[test]
    fn single_node_rings_always_place_at_home(
        load in any_load(),
        headroom in any_headroom(),
        item_load in any_item_load(),
        replication in 1u8..=6,
    ) {
        // `fleetring up` on one laptop: the capacity of the only node covers the whole total.
        let ring = build_ring(fixtures::single(), fleetring::Strategy::default(), replication);
        let view = BoundedLoads::new(headroom)
            .load_view(&ring, [("laptop", load)])
            .expect("laptop is the ring's node");
        let placed = view.replicas("agent:1", item_load);
        prop_assert!(placed.is_home() && !placed.is_saturated(), "{:?}", placed);
    }
}
