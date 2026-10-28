//! Placement through the public API: weight proportionality (FR-008) and replica-set behavior
//! (FR-009).

use fleetring::{Node, ReplicationFactor, Ring, Weight};
use proptest::prelude::*;

use crate::common::corpus::{Corpus, LARGE, MEDIUM};
use crate::common::fixtures;
use crate::common::stats::{assert_balanced, tally};

type TestResult = Result<(), Box<dyn std::error::Error>>;

/// Position of `node` in the ring's canonical order.
fn index_of(ring: &Ring, node: &Node) -> usize {
    ring.nodes()
        .binary_search_by(|probe| probe.id().cmp(node.id()))
        .expect("the node belongs to the ring")
}

fn primary_counts(ring: &Ring, corpus: Corpus, keys: usize) -> Vec<u64> {
    let primaries = corpus
        .keys(keys)
        .map(|key| index_of(ring, ring.primary(&key)));
    tally(ring.node_count(), primaries)
}

/// Rings whose primaries weighted rendezvous must spread exactly in proportion to weight.
fn weighted_rings() -> Result<Vec<(&'static str, Ring)>, Box<dyn std::error::Error>> {
    let equal: Vec<Node> = (0..10)
        .map(|i| Ok(Node::new(format!("node-{i}").parse()?)))
        .collect::<Result<_, fleetring::ParseLabelError>>()?;
    let lopsided = vec![
        Node::new("light".parse()?),
        Node::new("heavy".parse()?).with_weight(Weight::new(1000).ok_or("zero")?),
    ];
    Ok(vec![
        ("10 equal nodes", Ring::builder().nodes(equal).build()?),
        (
            "homelab 1/2/4",
            Ring::builder().nodes(fixtures::homelab()).build()?,
        ),
        (
            "12 nodes weighing 1..=4",
            Ring::builder().nodes(fixtures::fleet(12, 3)).build()?,
        ),
        (
            "weights 1 and 1000",
            Ring::builder().nodes(lopsided).build()?,
        ),
    ])
}

#[test]
fn primaries_follow_weights_on_every_corpus() -> TestResult {
    for (label, ring) in weighted_rings()? {
        let weights = fixtures::weights(ring.nodes());
        for corpus in Corpus::ALL {
            let counts = primary_counts(&ring, corpus, MEDIUM);
            assert_balanced(&counts, &weights, &format!("{label}, {corpus} keys"));
        }
    }
    Ok(())
}

#[test]
#[ignore = "1M keys per corpus; runs in the release statistics job"]
fn primaries_follow_weights_at_one_million_keys() -> TestResult {
    for (label, ring) in weighted_rings()? {
        let weights = fixtures::weights(ring.nodes());
        for corpus in Corpus::ALL {
            let counts = primary_counts(&ring, corpus, LARGE);
            assert_balanced(&counts, &weights, &format!("{label}, {corpus} keys"));
        }
    }
    Ok(())
}

#[test]
fn replica_sets_have_the_documented_shape() -> TestResult {
    let ring = Ring::builder().nodes(fixtures::three_zones(3)).build()?;
    for key in Corpus::Agents.keys(1_000) {
        let replicas = ring.replicas(&key);
        assert_eq!(replicas.len(), 3);
        assert!(!replicas.is_under_replicated());
        assert_eq!(replicas.primary(), ring.primary(&key));
        assert_eq!(replicas.followers().count(), 2);
        let zones: Vec<_> = replicas.iter().map(Node::zone).collect();
        assert!(
            zones[0] != zones[1] && zones[1] != zones[2] && zones[0] != zones[2],
            "{replicas}"
        );
        let preference: Vec<&Node> = ring.preference(&key).collect();
        assert_eq!(preference.len(), ring.node_count());
        assert!(
            replicas.iter().eq(preference.iter().copied().take(3)),
            "{replicas}"
        );
    }
    Ok(())
}

#[test]
fn small_rings_are_under_replicated_not_errors() -> TestResult {
    let ring = Ring::builder().node(Node::new("solo".parse()?)).build()?;
    let replicas = ring.replicas("agent:1");
    assert_eq!(replicas.len(), 1);
    assert!(replicas.is_under_replicated());
    assert_eq!(replicas.to_string(), "[solo]");
    Ok(())
}

#[test]
fn display_lists_ids_in_preference_order() -> TestResult {
    let ring = Ring::builder().nodes(fixtures::homelab()).build()?;
    let replicas = ring.replicas("tenant:acme");
    let ids: Vec<String> = replicas.iter().map(|node| node.id().to_string()).collect();
    assert_eq!(replicas.to_string(), format!("[{}]", ids.join(", ")));
    Ok(())
}

#[test]
fn rejecting_every_node_yields_none() -> TestResult {
    let ring = Ring::builder().nodes(fixtures::homelab()).build()?;
    assert!(ring.replicas_where("agent:1", |_| false).is_none());
    assert_eq!(ring.preference_where("agent:1", |_| false).count(), 0);
    Ok(())
}

/// Both placement strategies, with few vnode tokens so walks revisit nodes often.
fn strategies() -> impl proptest::strategy::Strategy<Value = fleetring::Strategy> {
    prop_oneof![
        Just(fleetring::Strategy::Rendezvous),
        Just(crate::vnodes::vnodes(16)),
    ]
}

proptest! {
    #[test]
    fn filtering_nodes_equals_removing_them(
        nodes in fixtures::memberships_up_to(16),
        down in proptest::collection::vec(any::<bool>(), 16),
        key in "[a-z]{1,12}:[0-9]{1,6}",
        strategy in strategies(),
    ) {
        let ring = Ring::builder().strategy(strategy).nodes(nodes.clone()).build().expect("valid membership");
        let is_down = |node: &Node| down[index_of(&ring, node)];
        let survivors: Vec<Node> = ring.nodes().iter().filter(|n| !is_down(n)).cloned().collect();
        let filtered = ring.replicas_where(&key, |node| !is_down(node));
        let filtered_list: Vec<_> = ring.preference_where(&key, |node| !is_down(node)).map(Node::id).cloned().collect();
        if survivors.is_empty() {
            prop_assert!(filtered.is_none());
        } else {
            let rebuilt = Ring::builder().strategy(strategy).nodes(survivors).build().expect("survivors are valid");
            prop_assert_eq!(filtered.expect("some node is up").to_ids(), rebuilt.replicas(&key).to_ids());
            let rebuilt_list: Vec<_> = rebuilt.preference(&key).map(Node::id).cloned().collect();
            prop_assert_eq!(filtered_list, rebuilt_list);
        }
    }

    #[test]
    fn smaller_replication_factors_are_prefixes(
        nodes in fixtures::memberships_up_to(16),
        key in "[a-z]{1,12}:[0-9]{1,6}",
        r in 1u8..=8,
        strategy in strategies(),
    ) {
        let ring = Ring::builder().strategy(strategy).nodes(nodes).build().expect("valid membership");
        let short = ring.replicas_n(&key, ReplicationFactor::new(r).expect("nonzero")).to_ids();
        let long = ring.replicas_n(&key, ReplicationFactor::new(r + 1).expect("nonzero")).to_ids();
        prop_assert_eq!(&short[..], &long[..short.len()]);
        prop_assert_eq!(short.len(), usize::from(r).min(ring.node_count()));
    }

    #[test]
    fn placement_ignores_the_order_nodes_were_added(
        nodes in fixtures::memberships_up_to(16),
        key in "[a-z]{1,12}:[0-9]{1,6}",
        strategy in strategies(),
    ) {
        let forward = Ring::builder().strategy(strategy).nodes(nodes.clone()).build().expect("valid membership");
        let backward = Ring::builder().strategy(strategy).nodes(nodes.into_iter().rev()).build().expect("valid membership");
        prop_assert_eq!(forward.replicas(&key), backward.replicas(&key));
    }
}
