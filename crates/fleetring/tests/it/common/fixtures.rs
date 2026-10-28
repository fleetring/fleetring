//! Named memberships, and a proptest strategy for random ones.
//!
//! Every membership follows the topology rules of ADR-0006: node IDs are unique, and each host
//! label stays inside one zone. A node without a zone is in the default zone, and a node without
//! a host label is its own host.

use std::collections::BTreeSet;
use std::str::FromStr;

use fleetring::{Node, ParseLabelError, Weight};
use proptest::arbitrary::any;
use proptest::collection::vec;
use proptest::sample::Index;
use proptest::strategy::Strategy;

/// The weights that [`fleet`] cycles through within each zone.
const FLEET_WEIGHTS: [u32; 4] = [1, 2, 3, 4];

/// The most nodes that [`memberships`] generates.
const MAX_NODES: usize = 64;

/// The largest weight that [`memberships_up_to`] generates.
const MAX_WEIGHT: u32 = 1000;

/// The most zone labels that a generated membership uses, besides the default zone.
const MAX_ZONES: usize = 6;

/// The most host labels that a generated membership uses in each zone.
const MAX_HOSTS_PER_ZONE: usize = 4;

/// One node, `laptop`: the smallest ring, as when `fleetring up` runs on a single laptop.
pub(crate) fn single() -> Vec<Node> {
    vec![node("laptop", 1)]
}

/// A home lab: `laptop-1` (weight 1), `laptop-2` (weight 2), and `nas` (weight 4), all in the
/// default zone and each its own host.
pub(crate) fn homelab() -> Vec<Node> {
    vec![node("laptop-1", 1), node("laptop-2", 2), node("nas", 4)]
}

/// Zones `z1`, `z2`, and `z3` with `per_zone` nodes of weight 1 each, named `z1-n1` and so on.
///
/// # Panics
///
/// Panics if `per_zone` is zero.
pub(crate) fn three_zones(per_zone: usize) -> Vec<Node> {
    assert!(per_zone > 0, "each zone needs at least one node");
    zones(&[per_zone; 3])
}

/// Zones `z1`, `z2`, and `z3` with 10, 10, and 2 nodes of weight 1.
///
/// Strict zone diversity puts one replica of every key in the small zone (ADR-0006).
pub(crate) fn skewed_zones() -> Vec<Node> {
    zones(&[10, 10, 2])
}

/// A fleet of `nodes` nodes dealt round-robin into zones `z1` to `z{zones}`.
///
/// Within each zone, consecutive pairs of nodes share a host (`z1-h1` holds `z1-n1` and `z1-n2`),
/// and weights cycle through 1, 2, 3, 4.
///
/// # Panics
///
/// Panics unless every zone gets a node: `1 <= zones <= nodes`.
pub(crate) fn fleet(nodes: usize, zones: usize) -> Vec<Node> {
    assert!(
        (1..=nodes).contains(&zones),
        "{nodes} nodes cannot fill {zones} zones"
    );
    (0..nodes)
        .map(|i| {
            let zone = i % zones + 1;
            let rank = i / zones;
            node(
                &format!("z{zone}-n{}", rank + 1),
                FLEET_WEIGHTS[rank % FLEET_WEIGHTS.len()],
            )
            .with_zone(label(&format!("z{zone}")))
            .with_host(label(&format!("z{zone}-h{}", rank / 2 + 1)))
        })
        .collect()
}

/// A [`fleet`] of 1000 nodes over 8 zones: 125 nodes on 63 hosts in each zone.
pub(crate) fn fleet_1000() -> Vec<Node> {
    fleet(1000, 8)
}

/// Returns each node's weight, in order, for the helpers in [`super::stats`].
pub(crate) fn weights(nodes: &[Node]) -> Vec<u32> {
    nodes.iter().map(|node| node.weight().get()).collect()
}

/// Random memberships of 1 to 64 nodes; see [`memberships_up_to`].
pub(crate) fn memberships() -> impl Strategy<Value = Vec<Node>> {
    memberships_up_to(MAX_NODES)
}

/// Random memberships of 1 to `max_nodes` nodes, with unique IDs and weights from 1 to 1000.
///
/// IDs are short random strings; a repeated ID drops the later node. A membership uses up to six
/// zone labels (`z1`, `z2`, …) and up to four host labels per zone, and none, some, or all of its
/// nodes have a zone label, and likewise a host label. A host label names its zone (`z2-h1`, or
/// `h1` in the default zone), so no host spans two zones.
///
/// Counterexamples shrink toward a few weight-1 nodes with short IDs and no labels.
///
/// # Panics
///
/// Panics if `max_nodes` is zero.
pub(crate) fn memberships_up_to(max_nodes: usize) -> impl Strategy<Value = Vec<Node>> {
    assert!(max_nodes > 0, "a membership needs at least one node");
    let topology = (
        0..=MAX_ZONES,
        any::<bool>(),
        0..=MAX_HOSTS_PER_ZONE,
        any::<bool>(),
    );
    let nodes = vec(
        (
            "[a-z][a-z0-9]{0,7}",
            1..=MAX_WEIGHT,
            any::<Index>(),
            any::<Index>(),
        ),
        1..=max_nodes,
    );
    (topology, nodes).prop_map(|((zones, default_zone, hosts, own_hosts), nodes)| {
        let mut ids = BTreeSet::new();
        nodes
            .iter()
            .filter(|(id, ..)| ids.insert(id.as_str()))
            .map(|(id, weight, zone, host)| {
                let zone = label_number(*zone, zones, default_zone);
                let host = label_number(*host, hosts, own_hosts);
                generated_node(id, *weight, zone, host)
            })
            .collect()
    })
}

/// Maps `pick` to a label number from 1 to `labels`, or to `None` for no label.
///
/// There is no label when `labels` is zero. Otherwise `unlabeled` adds a slot that means no
/// label, so a membership either labels every node or mixes labeled and unlabeled ones.
fn label_number(pick: Index, labels: usize, unlabeled: bool) -> Option<usize> {
    if labels == 0 {
        return None;
    }
    let slot = pick.index(labels + usize::from(unlabeled));
    (slot < labels).then_some(slot + 1)
}

/// Builds a node for [`memberships_up_to`] from its zone and host label numbers.
fn generated_node(id: &str, weight: u32, zone: Option<usize>, host: Option<usize>) -> Node {
    let mut generated = node(id, weight);
    if let Some(zone) = zone {
        generated = generated.with_zone(label(&format!("z{zone}")));
    }
    if let Some(host) = host {
        let host = match zone {
            Some(zone) => format!("z{zone}-h{host}"),
            None => format!("h{host}"),
        };
        generated = generated.with_host(label(&host));
    }
    generated
}

/// Weight-1 nodes in zones `z1`, `z2`, …, with `sizes[k]` nodes in zone `k + 1`.
fn zones(sizes: &[usize]) -> Vec<Node> {
    sizes
        .iter()
        .zip(1..)
        .flat_map(|(&size, zone)| {
            (1..=size).map(move |i| {
                node(&format!("z{zone}-n{i}"), 1).with_zone(label(&format!("z{zone}")))
            })
        })
        .collect()
}

/// Returns a node in the default zone, on its own host.
fn node(id: &str, weight: u32) -> Node {
    let weight = Weight::new(weight).unwrap_or_else(|| panic!("node {id} has weight 0"));
    Node::new(label(id)).with_weight(weight)
}

/// Parses a label that the harness built, so it is valid by construction.
fn label<T: FromStr<Err = ParseLabelError>>(text: &str) -> T {
    text.parse()
        .unwrap_or_else(|error| panic!("harness label {text:?} is invalid: {error}"))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use fleetring::{Host, Zone};
    use proptest::strategy::ValueTree;
    use proptest::test_runner::{Config, TestCaseError, TestError, TestRng, TestRunner};
    use proptest::{prop_assert, proptest};

    use super::*;

    /// How many of a membership's nodes carry some label.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
    enum Labeling {
        Unlabeled,
        Mixed,
        Labeled,
    }

    /// Classifies a membership by how many of its nodes satisfy `has_label`.
    fn labeling(nodes: &[Node], has_label: impl Fn(&Node) -> bool) -> Labeling {
        match nodes.iter().filter(|node| has_label(node)).count() {
            0 => Labeling::Unlabeled,
            count if count == nodes.len() => Labeling::Labeled,
            _ => Labeling::Mixed,
        }
    }

    /// Checks the rules of ADR-0006: node IDs are unique, and each host label stays in one zone.
    fn check_topology(nodes: &[Node]) -> Result<(), String> {
        let mut ids = BTreeSet::new();
        let mut host_zones = BTreeMap::new();
        for node in nodes {
            if !ids.insert(node.id()) {
                return Err(format!("node id {} repeats", node.id()));
            }
            if let Some(host) = node.host() {
                let zone = *host_zones.entry(host).or_insert(node.zone());
                if zone != node.zone() {
                    return Err(format!(
                        "host {host} spans zones {zone:?} and {:?}",
                        node.zone()
                    ));
                }
            }
        }
        Ok(())
    }

    /// Counts nodes per zone label, with `None` for the default zone.
    fn zone_sizes(nodes: &[Node]) -> BTreeMap<Option<&str>, usize> {
        let mut sizes = BTreeMap::new();
        for node in nodes {
            *sizes.entry(node.zone().map(Zone::as_str)).or_default() += 1;
        }
        sizes
    }

    #[test]
    fn named_fixtures_follow_the_topology_rules() {
        let fixtures = [
            ("single", single()),
            ("homelab", homelab()),
            ("three_zones(4)", three_zones(4)),
            ("skewed_zones", skewed_zones()),
            ("fleet(7, 3)", fleet(7, 3)),
            ("fleet_1000", fleet_1000()),
        ];
        for (name, nodes) in fixtures {
            assert_eq!(check_topology(&nodes), Ok(()), "{name}");
        }
    }

    #[test]
    fn small_fixtures_match_their_descriptions() {
        let ids = |nodes: &[Node]| -> Vec<String> {
            nodes.iter().map(|node| node.id().to_string()).collect()
        };
        assert_eq!(ids(&single()), ["laptop"]);
        let homelab = homelab();
        assert_eq!(ids(&homelab), ["laptop-1", "laptop-2", "nas"]);
        assert_eq!(weights(&homelab), [1, 2, 4]);
        assert!(
            homelab
                .iter()
                .all(|node| node.zone().is_none() && node.host().is_none())
        );
    }

    #[test]
    fn zone_fixtures_have_the_documented_sizes() {
        assert_eq!(
            zone_sizes(&three_zones(4)),
            BTreeMap::from([(Some("z1"), 4), (Some("z2"), 4), (Some("z3"), 4)])
        );
        let skewed = skewed_zones();
        assert_eq!(
            zone_sizes(&skewed),
            BTreeMap::from([(Some("z1"), 10), (Some("z2"), 10), (Some("z3"), 2)])
        );
        assert!(
            skewed
                .iter()
                .all(|node| node.weight() == Weight::ONE && node.host().is_none())
        );
    }

    #[test]
    fn fleet_1000_spans_8_zones_with_paired_hosts_and_weights_1_to_4() {
        let nodes = fleet_1000();
        assert_eq!(nodes.len(), 1000);
        let sizes = zone_sizes(&nodes);
        assert_eq!(sizes.len(), 8, "{sizes:?}");
        assert!(sizes.values().all(|&size| size == 125), "{sizes:?}");

        let mut host_sizes: BTreeMap<&Host, usize> = BTreeMap::new();
        for node in &nodes {
            let host = node.host().expect("every fleet node has a host label");
            *host_sizes.entry(host).or_default() += 1;
        }
        assert_eq!(host_sizes.len(), 8 * 63);
        assert!(host_sizes.values().all(|&size| size <= 2));

        let weights: BTreeSet<u32> = weights(&nodes).into_iter().collect();
        assert_eq!(weights, BTreeSet::from(FLEET_WEIGHTS));
    }

    proptest! {
        #[test]
        fn generated_memberships_follow_the_topology_rules(nodes in memberships()) {
            prop_assert!((1..=MAX_NODES).contains(&nodes.len()), "{} nodes", nodes.len());
            prop_assert!(nodes.iter().all(|node| (1..=MAX_WEIGHT).contains(&node.weight().get())));
            let zones: BTreeSet<&Zone> = nodes.iter().filter_map(Node::zone).collect();
            prop_assert!(zones.len() <= MAX_ZONES, "{zones:?}");
            let mut zone_hosts: BTreeMap<Option<&Zone>, BTreeSet<&Host>> = BTreeMap::new();
            for node in &nodes {
                if let Some(host) = node.host() {
                    zone_hosts.entry(node.zone()).or_default().insert(host);
                }
            }
            prop_assert!(
                zone_hosts.values().all(|hosts| hosts.len() <= MAX_HOSTS_PER_ZONE),
                "{zone_hosts:?}"
            );
            check_topology(&nodes).map_err(TestCaseError::fail)?;
        }

        #[test]
        fn memberships_up_to_caps_the_node_count(nodes in memberships_up_to(3)) {
            prop_assert!((1..=3).contains(&nodes.len()), "{} nodes", nodes.len());
            check_topology(&nodes).map_err(TestCaseError::fail)?;
        }
    }

    #[test]
    fn generated_memberships_cover_every_mix_of_zone_and_host_labels() {
        let strategy = memberships();
        let mut runner = TestRunner::deterministic();
        let mut mixes = BTreeSet::new();
        for _ in 0..256 {
            let nodes = strategy
                .new_tree(&mut runner)
                .expect("the strategy never rejects")
                .current();
            mixes.insert((
                labeling(&nodes, |node| node.zone().is_some()),
                labeling(&nodes, |node| node.host().is_some()),
            ));
        }
        assert_eq!(mixes.len(), 9, "{mixes:?}");
    }

    #[test]
    fn failing_properties_shrink_to_small_readable_memberships() {
        let config = Config {
            // This failure is deliberate, so keep it out of proptest-regressions/.
            failure_persistence: None,
            max_shrink_iters: 10_000,
            ..Config::default()
        };
        let rng = TestRng::deterministic_rng(config.rng_algorithm);
        let mut runner = TestRunner::new_with_rng(config, rng);
        let result = runner.run(&memberships(), |nodes| {
            prop_assert!(nodes.len() < 4, "{} nodes", nodes.len());
            Ok(())
        });
        let Err(TestError::Fail(_, minimal)) = result else {
            panic!("a property that fails from 4 nodes up did not fail: {result:?}");
        };
        assert_eq!(minimal.len(), 4, "{minimal:#?}");
        assert!(
            minimal.iter().all(|node| node.id().as_str().len() <= 2
                && node.weight() == Weight::ONE
                && node.zone().is_none()
                && node.host().is_none()),
            "{minimal:#?}"
        );
    }
}
