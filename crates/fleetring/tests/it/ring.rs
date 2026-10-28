use std::error::Error;

use fleetring::{
    BuildRingError, Epoch, Host, Node, NodeId, ReplicationFactor, Ring, RingBuilder, StableHasher,
    Weight, Xxh3, Zone,
};
use proptest::prelude::*;

type TestResult = Result<(), Box<dyn Error>>;

fn id(label: &str) -> NodeId {
    label.parse().expect("test ids are valid")
}

fn zone(label: &str) -> Zone {
    label.parse().expect("test zones are valid")
}

fn host(label: &str) -> Host {
    label.parse().expect("test hosts are valid")
}

fn node(label: &str) -> Node {
    Node::new(id(label))
}

fn weight(value: u32) -> Weight {
    Weight::new(value).expect("test weights are nonzero")
}

fn replication(value: u8) -> ReplicationFactor {
    ReplicationFactor::new(value).expect("test replication factors are nonzero")
}

fn ids<H>(ring: &Ring<H>) -> Vec<&str> {
    ring.nodes().iter().map(|node| node.id().as_str()).collect()
}

fn rejected(nodes: impl IntoIterator<Item = Node>) -> BuildRingError {
    Ring::builder()
        .nodes(nodes)
        .build()
        .expect_err("the nodes are invalid")
}

/// XXH3 with a salt mixed into the seed: a hasher whose value matters for equality.
#[derive(Debug, Clone, PartialEq)]
struct SaltedXxh3 {
    salt: u64,
}

impl StableHasher for SaltedXxh3 {
    fn algorithm(&self) -> &'static str {
        "salted-xxh3-test"
    }

    fn hash(&self, bytes: &[u8], seed: u64) -> u64 {
        Xxh3.hash(bytes, seed ^ self.salt)
    }
}

/// Weights that are usually small, so ties are common, with both extremes mixed in.
fn any_weight() -> impl Strategy<Value = Weight> {
    prop_oneof![1..4u32, Just(u32::MAX), 1..=u32::MAX].prop_map(weight)
}

/// A node drawn from small label pools, so duplicate IDs and hosts in two zones are common.
fn any_node() -> impl Strategy<Value = Node> {
    let zone_label = proptest::option::of("z[0-2]");
    let host_label = proptest::option::of("h[0-2]");
    ("[a-h]{1,2}", any_weight(), zone_label, host_label).prop_map(
        |(label, weight, zone_label, host_label)| {
            let mut node = node(&label).with_weight(weight);
            if let Some(label) = zone_label {
                node = node.with_zone(zone(&label));
            }
            if let Some(label) = host_label {
                node = node.with_host(host(&label));
            }
            node
        },
    )
}

/// Nodes that always build: distinct IDs, with host `hN` always in zone number `N % 3`, where
/// zone number 0 is the default zone.
fn valid_nodes() -> impl Strategy<Value = Vec<Node>> {
    let member = (any_weight(), 0..3u8, proptest::option::of(0..4u8));
    proptest::collection::btree_map("[a-z0-9.:_-]{1,8}", member, 1..24).prop_map(|members| {
        members
            .into_iter()
            .map(|(label, (weight, zone_number, host_number))| {
                let zone_number = host_number.map_or(zone_number, |number| number % 3);
                let mut node = node(&label).with_weight(weight);
                if zone_number > 0 {
                    node = node.with_zone(zone(&format!("z{zone_number}")));
                }
                if let Some(number) = host_number {
                    node = node.with_host(host(&format!("h{number}")));
                }
                node
            })
            .collect()
    })
}

/// Pairs each generated list with a shuffled copy of itself.
fn with_shuffle(
    nodes: impl Strategy<Value = Vec<Node>>,
) -> impl Strategy<Value = (Vec<Node>, Vec<Node>)> {
    nodes.prop_flat_map(|nodes| (Just(nodes.clone()), Just(nodes).prop_shuffle()))
}

/// What a build produced, in a comparable form: the canonical nodes and the `Debug` summary, or
/// the error.
fn outcome(nodes: Vec<Node>) -> Result<(Vec<Node>, String), BuildRingError> {
    Ring::builder()
        .seed(11)
        .nodes(nodes)
        .build()
        .map(|ring| (ring.nodes().to_vec(), format!("{ring:?}")))
}

/// The error `build` must report for `nodes`, found the slow and obvious way, or `None` if the
/// nodes are valid.
fn expected_error(nodes: &[Node]) -> Option<BuildRingError> {
    if nodes.is_empty() {
        return Some(BuildRingError::NoNodes);
    }
    let mut sorted: Vec<&Node> = nodes.iter().collect();
    sorted.sort_by(|a, b| a.id().cmp(b.id()));
    for pair in sorted.windows(2) {
        if pair[0].id() == pair[1].id() {
            return Some(BuildRingError::DuplicateNode {
                id: pair[0].id().clone(),
            });
        }
    }
    for (position, later) in sorted.iter().enumerate() {
        let Some(host) = later.host() else {
            continue;
        };
        let earlier: Vec<&&Node> = sorted[..position]
            .iter()
            .filter(|node| node.host() == Some(host))
            .collect();
        if earlier.iter().any(|node| node.zone() != later.zone()) {
            return Some(BuildRingError::HostInMultipleZones {
                host: host.clone(),
                first: earlier[0].zone().cloned(),
                second: later.zone().cloned(),
            });
        }
    }
    None
}

proptest! {
    #[test]
    fn permutations_of_the_same_nodes_build_identical_rings(
        (nodes, shuffled) in with_shuffle(valid_nodes()),
    ) {
        let ring = Ring::builder().nodes(nodes.clone()).build().expect("valid nodes");
        let other = Ring::builder().nodes(shuffled).build().expect("valid nodes");
        prop_assert_eq!(ring.nodes(), other.nodes());
        prop_assert_eq!(format!("{ring:?}"), format!("{other:?}"));

        let mut sorted = nodes;
        sorted.sort_by(|a, b| a.id().cmp(b.id()));
        prop_assert_eq!(ring.nodes(), sorted.as_slice());
    }

    #[test]
    fn permutations_of_any_nodes_produce_the_same_outcome(
        (nodes, shuffled) in with_shuffle(proptest::collection::vec(any_node(), 0..12)),
    ) {
        prop_assert_eq!(outcome(nodes), outcome(shuffled));
    }

    #[test]
    fn build_never_panics_and_rejects_exactly_the_invalid_inputs(
        nodes in proptest::collection::vec(any_node(), 0..12),
        seed in any::<u64>(),
        replication_factor in 1..=u8::MAX,
        epoch in any::<u64>(),
    ) {
        let result = Ring::builder()
            .seed(seed)
            .replication_factor(replication(replication_factor))
            .epoch(Epoch::new(epoch))
            .nodes(nodes.clone())
            .build();
        match result {
            Ok(ring) => {
                prop_assert_eq!(expected_error(&nodes), None);
                let mut sorted = nodes;
                sorted.sort_by(|a, b| a.id().cmp(b.id()));
                prop_assert_eq!(ring.nodes(), sorted.as_slice());
                prop_assert_eq!(ring.seed(), seed);
                prop_assert_eq!(ring.replication_factor().get(), replication_factor);
                prop_assert_eq!(ring.epoch(), Epoch::new(epoch));
            }
            Err(error) => prop_assert_eq!(Some(error), expected_error(&nodes)),
        }
    }

    #[test]
    fn to_builder_rebuilds_the_same_ring(
        nodes in valid_nodes(),
        salt in any::<u64>(),
        seed in any::<u64>(),
        replication_factor in 1..=u8::MAX,
        epoch in any::<u64>(),
    ) {
        let ring = Ring::builder()
            .hasher(SaltedXxh3 { salt })
            .seed(seed)
            .replication_factor(replication(replication_factor))
            .epoch(Epoch::new(epoch))
            .nodes(nodes)
            .build()
            .expect("valid nodes");
        let rebuilt = ring.to_builder().build().expect("a ring's own nodes are valid");
        prop_assert_eq!(rebuilt.nodes(), ring.nodes());
        prop_assert_eq!(format!("{rebuilt:?}"), format!("{ring:?}"));
    }
}

#[test]
fn an_empty_builder_is_rejected() {
    let error = Ring::builder()
        .build()
        .expect_err("a ring needs at least one node");
    assert_eq!(error, BuildRingError::NoNodes);
    assert_eq!(error.to_string(), "ring has no nodes");
}

#[test]
fn a_shared_id_is_rejected_even_with_different_settings() {
    let error = rejected([
        node("nas"),
        node("vps"),
        node("nas").with_weight(weight(4)).with_zone(zone("garage")),
    ]);
    assert_eq!(error, BuildRingError::DuplicateNode { id: id("nas") });
    assert_eq!(error.to_string(), "node id 'nas' appears more than once");
}

#[test]
fn a_host_in_two_named_zones_is_rejected() {
    let error = rejected([
        node("disk-1")
            .with_host(host("nas"))
            .with_zone(zone("garage")),
        node("disk-2")
            .with_host(host("nas"))
            .with_zone(zone("attic")),
    ]);
    assert_eq!(
        error,
        BuildRingError::HostInMultipleZones {
            host: host("nas"),
            first: Some(zone("garage")),
            second: Some(zone("attic")),
        }
    );
    assert_eq!(
        error.to_string(),
        "host 'nas' is in zone 'garage' and in zone 'attic'; a host must be in exactly one zone"
    );
}

#[test]
fn the_default_zone_counts_as_a_zone_for_hosts() {
    let error = rejected([
        node("disk-1").with_host(host("nas")),
        node("disk-2")
            .with_host(host("nas"))
            .with_zone(zone("garage")),
    ]);
    assert_eq!(
        error,
        BuildRingError::HostInMultipleZones {
            host: host("nas"),
            first: None,
            second: Some(zone("garage")),
        }
    );
    assert_eq!(
        error.to_string(),
        "host 'nas' is in the default zone and in zone 'garage'; a host must be in exactly one zone"
    );

    let error = rejected([
        node("disk-1")
            .with_host(host("nas"))
            .with_zone(zone("garage")),
        node("disk-2").with_host(host("nas")),
    ]);
    assert_eq!(
        error.to_string(),
        "host 'nas' is in zone 'garage' and in the default zone; a host must be in exactly one zone"
    );
}

#[cfg(target_pointer_width = "64")]
#[test]
fn too_many_nodes_names_the_count_and_the_limit() {
    // Unreachable without 2³² nodes in memory; ring.rs unit-tests the check itself.
    let error = BuildRingError::TooManyNodes {
        count: 4_294_967_296,
        max: 4_294_967_295,
    };
    assert_eq!(
        error.to_string(),
        "ring has 4294967296 nodes; the maximum is 4294967295"
    );
}

#[test]
fn duplicates_are_reported_before_host_conflicts_and_the_smallest_id_first() {
    let nodes = [
        node("b"),
        node("c").with_host(host("h")).with_zone(zone("z1")),
        node("a"),
        node("d").with_host(host("h")),
        node("b"),
        node("a"),
    ];
    let forward = rejected(nodes.clone());
    let backward = rejected(nodes.into_iter().rev());
    assert_eq!(forward, BuildRingError::DuplicateNode { id: id("a") });
    assert_eq!(backward, forward);
}

#[test]
fn hosts_shared_within_one_zone_are_accepted() -> TestResult {
    let ring = Ring::builder()
        .node(node("disk-1").with_host(host("nas")).with_zone(zone("z1")))
        .node(node("disk-2").with_host(host("nas")).with_zone(zone("z1")))
        .node(node("disk-3").with_host(host("box")))
        .node(node("disk-4").with_host(host("box")))
        .build()?;
    assert_eq!(ring.node_count(), 4);
    assert_eq!(ring.zone_count(), 2);
    Ok(())
}

#[test]
fn builders_start_from_the_documented_defaults() -> TestResult {
    let ring = Ring::builder().node(node("solo")).build()?;
    assert_eq!(ring.hasher(), &Xxh3);
    assert_eq!(ring.seed(), 0);
    assert_eq!(ring.strategy(), fleetring::Strategy::Rendezvous);
    assert_eq!(ring.replication_factor(), ReplicationFactor::default());
    assert_eq!(ring.epoch(), Epoch::default());
    assert_eq!(ring.node_count(), 1);
    assert_eq!(ring.zone_count(), 1);

    let summary = format!("{ring:?}");
    let from_new = RingBuilder::new().node(node("solo")).build()?;
    let from_default = RingBuilder::default().node(node("solo")).build()?;
    assert_eq!(format!("{from_new:?}"), summary);
    assert_eq!(format!("{from_default:?}"), summary);
    assert_eq!(
        fleetring::Strategy::default(),
        fleetring::Strategy::Rendezvous
    );
    Ok(())
}

#[test]
fn debug_summarizes_the_ring_without_listing_nodes() -> TestResult {
    let ring = Ring::builder()
        .seed(7)
        .epoch(Epoch::new(3))
        .node(node("laptop-1").with_zone(zone("home")))
        .node(node("nas"))
        .build()?;
    assert_eq!(
        format!("{ring:?}"),
        "Ring { hasher: Xxh3, seed: 7, strategy: Rendezvous, \
         replication_factor: ReplicationFactor(3), epoch: Epoch(3), node_count: 2, \
         zone_count: 2, .. }"
    );
    Ok(())
}

#[test]
fn nodes_are_kept_in_byte_order_of_their_ids() -> TestResult {
    let ring = Ring::builder()
        .nodes(["node_9", "node-2", "node.0", "node-10", "node-1"].map(node))
        .build()?;
    assert_eq!(
        ids(&ring),
        ["node-1", "node-10", "node-2", "node.0", "node_9"]
    );
    Ok(())
}

#[test]
fn nodes_are_found_by_any_form_of_their_id() -> TestResult {
    let ring = Ring::builder()
        .nodes([node("b").with_weight(weight(2)), node("a"), node("c")])
        .build()?;
    assert_eq!(ring.node("b").map(Node::weight), Some(weight(2)));
    assert_eq!(ring.node(String::from("a")), ring.nodes().first());
    let c = id("c");
    assert_eq!(ring.node(&c), ring.nodes().last());
    assert_eq!(ring.node(c), ring.nodes().last());
    assert_eq!(ring.node("d"), None);
    assert_eq!(ring.node("B"), None, "lookups don't fold case");
    assert_eq!(ring.node(""), None);
    Ok(())
}

#[test]
fn the_default_zone_counts_as_one_zone() -> TestResult {
    let ring = Ring::builder()
        .node(node("a").with_zone(zone("z1")))
        .node(node("b"))
        .node(node("c"))
        .node(node("d").with_zone(zone("z1")))
        .build()?;
    assert_eq!(ring.zone_count(), 2);
    Ok(())
}

#[test]
fn switching_hashers_keeps_every_other_setting() -> TestResult {
    let ring = Ring::builder()
        .seed(9)
        .strategy(fleetring::Strategy::Rendezvous)
        .replication_factor(replication(5))
        .epoch(Epoch::new(4))
        .node(node("a"))
        .hasher(SaltedXxh3 { salt: 1 })
        .build()?;
    assert_eq!(ring.hasher(), &SaltedXxh3 { salt: 1 });
    assert_eq!(ring.seed(), 9);
    assert_eq!(ring.replication_factor(), replication(5));
    assert_eq!(ring.epoch(), Epoch::new(4));
    assert_eq!(ids(&ring), ["a"]);
    Ok(())
}

#[test]
fn to_builder_keeps_the_hasher_settings_and_nodes() -> TestResult {
    let ring = Ring::builder()
        .hasher(SaltedXxh3 { salt: 3 })
        .seed(42)
        .strategy(fleetring::Strategy::Rendezvous)
        .replication_factor(replication(2))
        .epoch(Epoch::new(9))
        .node(node("nas").with_weight(weight(4)).with_zone(zone("garage")))
        .node(
            node("laptop-1")
                .with_host(host("desk"))
                .with_zone(zone("home")),
        )
        .node(
            node("laptop-2")
                .with_host(host("desk"))
                .with_zone(zone("home")),
        )
        .build()?;
    let rebuilt = ring.to_builder().build()?;
    assert_eq!(rebuilt.nodes(), ring.nodes());
    assert_eq!(rebuilt.hasher(), &SaltedXxh3 { salt: 3 });
    assert_eq!(rebuilt.seed(), 42);
    assert_eq!(rebuilt.strategy(), fleetring::Strategy::Rendezvous);
    assert_eq!(rebuilt.replication_factor(), replication(2));
    assert_eq!(rebuilt.epoch(), Epoch::new(9));
    assert_eq!(rebuilt.zone_count(), 2);
    assert_eq!(format!("{rebuilt:?}"), format!("{ring:?}"));

    let copy = ring.clone();
    assert_eq!(copy.nodes(), ring.nodes());
    assert_eq!(format!("{copy:?}"), format!("{ring:?}"));
    Ok(())
}

#[test]
fn remove_node_drops_only_the_node_with_that_id() -> TestResult {
    let ring = Ring::builder()
        .nodes(["a", "b", "c"].map(node))
        .remove_node("b")
        .build()?;
    assert_eq!(ids(&ring), ["a", "c"]);
    Ok(())
}

#[test]
fn remove_node_accepts_any_form_of_the_id() -> TestResult {
    let c = id("c");
    let ring = Ring::builder()
        .nodes(["a", "b", "c", "d", "e"].map(node))
        .remove_node("a")
        .remove_node(String::from("b"))
        .remove_node(&c)
        .remove_node(id("d"))
        .build()?;
    assert_eq!(ids(&ring), ["e"]);
    assert_eq!(ring.node(c), None);
    Ok(())
}

#[test]
fn removing_an_absent_node_changes_nothing() -> TestResult {
    let ring = Ring::builder()
        .nodes(["a", "b"].map(node))
        .remove_node("c")
        .remove_node("A")
        .build()?;
    assert_eq!(ids(&ring), ["a", "b"]);
    Ok(())
}

#[test]
fn remove_node_removes_every_copy_of_a_duplicated_id() -> TestResult {
    let ring = Ring::builder()
        .node(node("a"))
        .node(node("b"))
        .node(node("a").with_weight(weight(2)))
        .remove_node("a")
        .build()?;
    assert_eq!(ids(&ring), ["b"]);
    Ok(())
}

#[test]
fn removing_the_last_node_leaves_nothing_to_build() {
    let error = Ring::builder()
        .node(node("a"))
        .remove_node("a")
        .build()
        .expect_err("no nodes are left");
    assert_eq!(error, BuildRingError::NoNodes);
}

#[test]
fn a_node_can_be_replaced_by_removing_it_first() -> TestResult {
    let ring = Ring::builder()
        .nodes(["a", "b"].map(node))
        .remove_node("b")
        .node(node("b").with_weight(weight(7)))
        .build()?;
    assert_eq!(ids(&ring), ["a", "b"]);
    assert_eq!(ring.node("b").map(Node::weight), Some(weight(7)));
    Ok(())
}

#[test]
fn draining_a_node_builds_the_next_ring_without_it() -> TestResult {
    let ring = Ring::builder()
        .seed(5)
        .epoch(Epoch::new(1))
        .nodes(["a", "b", "c"].map(node))
        .build()?;
    let next = ring
        .to_builder()
        .remove_node("b")
        .epoch(Epoch::new(2))
        .build()?;
    assert_eq!(ids(&next), ["a", "c"]);
    assert_eq!(next.epoch(), Epoch::new(2));
    assert_eq!(next.seed(), ring.seed());
    assert_eq!(ids(&ring), ["a", "b", "c"], "the old ring is unchanged");
    Ok(())
}
