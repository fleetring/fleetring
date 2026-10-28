//! Ring diagnostics and expected shares (FR-015).

use std::num::NonZeroU32;

use fleetring::{Diagnostic, Node, ReplicationFactor, Ring, Severity, Strategy, VnodeParams};

use crate::common::corpus::Corpus;
use crate::common::fixtures;
use crate::vnodes::vnodes;

type TestResult = Result<(), Box<dyn std::error::Error>>;

fn samples(count: u32) -> NonZeroU32 {
    NonZeroU32::new(count).expect("nonzero sample count")
}

#[test]
fn small_zones_that_must_hold_a_replica_of_everything_are_flagged() -> TestResult {
    // Zones of 10, 10, and 2 nodes at R = 3: every key keeps one replica in the 2-node zone.
    let ring = Ring::builder().nodes(fixtures::skewed_zones()).build()?;
    let diagnostics = ring.diagnostics();
    let skews: Vec<_> = diagnostics
        .iter()
        .filter_map(|diagnostic| match diagnostic {
            Diagnostic::ZoneReplicaSkew {
                zone,
                weight_share_bps,
                replica_share_bps,
            } => Some((zone.clone(), *weight_share_bps, *replica_share_bps)),
            _ => None,
        })
        .collect();
    assert_eq!(skews.len(), 1, "{diagnostics:?}");
    let (zone, weight_share, replica_share) = skews[0].clone();
    let small_zone = zone.ok_or("the skewed zone has a label")?;
    let small_nodes: Vec<&Node> = ring
        .nodes()
        .iter()
        .filter(|node| node.zone() == Some(&small_zone))
        .collect();
    assert_eq!(small_nodes.len(), 2);
    assert_eq!(weight_share, 2 * 10_000 / 22);

    // An independent count over a harness corpus agrees within one percentage point.
    let mut in_small_zone = 0u64;
    let mut replicas = 0u64;
    for key in Corpus::Agents.keys(20_000) {
        for node in &ring.replicas(&key) {
            replicas += 1;
            in_small_zone += u64::from(node.zone() == Some(&small_zone));
        }
    }
    let counted = in_small_zone * 10_000 / replicas;
    assert!(
        counted.abs_diff(u64::from(replica_share)) <= 100,
        "diagnostic says {replica_share} bps, counting says {counted} bps"
    );
    Ok(())
}

#[test]
fn healthy_rings_report_nothing() -> TestResult {
    for nodes in [fixtures::three_zones(3), fixtures::three_zones(5)] {
        let ring = Ring::builder().nodes(nodes).build()?;
        assert_eq!(ring.diagnostics(), [], "three equal zones at R = 3");
    }
    Ok(())
}

#[test]
fn only_the_deepest_shortfall_is_reported() -> TestResult {
    let solo = Ring::builder().node(Node::new("solo".parse()?)).build()?;
    assert_eq!(
        solo.diagnostics(),
        [Diagnostic::UnderReplicated {
            nodes: 1,
            replication_factor: ReplicationFactor::default(),
        }]
    );

    let shared_host = Ring::builder()
        .nodes(["a", "b", "c"].map(|id| {
            Node::new(id.parse().expect("valid id")).with_host("nas".parse().expect("valid host"))
        }))
        .build()?;
    let diagnostics = shared_host.diagnostics();
    assert_eq!(diagnostics.len(), 1);
    assert_eq!(
        diagnostics[0].to_string(),
        "ring has 1 host but replicates 3 ways, so some replicas share a host"
    );

    let homelab = Ring::builder().nodes(fixtures::homelab()).build()?;
    let diagnostics = homelab.diagnostics();
    assert_eq!(diagnostics.len(), 1);
    assert_eq!(diagnostics[0].severity(), Severity::Info);
    assert_eq!(
        diagnostics[0].to_string(),
        "ring has 1 zone but replicates 3 ways, so some replicas share a zone"
    );
    Ok(())
}

#[test]
fn low_token_counts_are_flagged_per_node_and_warnings_sort_first() -> TestResult {
    // homelab weighs 1, 2, and 4; at 16 tokens per weight only `nas` (weight 4) reaches 64 tokens.
    let ring = Ring::builder()
        .strategy(vnodes(16))
        .nodes(fixtures::homelab())
        .build()?;
    let diagnostics = ring.diagnostics();
    let low: Vec<String> = diagnostics
        .iter()
        .filter(|d| matches!(d, Diagnostic::LowTokenCount { .. }))
        .map(ToString::to_string)
        .collect();
    assert_eq!(
        low,
        [
            "node 'laptop-1' has 16 tokens; vnode balance needs at least 64",
            "node 'laptop-2' has 32 tokens; vnode balance needs at least 64",
        ]
    );
    let severities: Vec<Severity> = diagnostics.iter().map(Diagnostic::severity).collect();
    let mut sorted = severities.clone();
    sorted.sort();
    assert_eq!(severities, sorted, "warnings come before info");
    assert_eq!(
        severities.last(),
        Some(&Severity::Info),
        "the single zone is info"
    );
    Ok(())
}

#[test]
fn primary_shares_are_exact_and_replica_shares_are_complete() -> TestResult {
    let rendezvous = Ring::builder().nodes(fixtures::homelab()).build()?;
    let shares = rendezvous.expected_shares(samples(2_000));
    let primary: Vec<u32> = shares.iter().map(|share| share.primary_bps).collect();
    // homelab weights by canonical ID: laptop-1 = 1, laptop-2 = 2, nas = 4.
    assert_eq!(primary, [10_000 / 7, 20_000 / 7, 40_000 / 7]);
    let replicas: u32 = shares.iter().map(|share| share.replica_bps).sum();
    assert!(
        (9_997..=10_000).contains(&replicas),
        "replica shares sum to {replicas}"
    );

    let vnode_ring = Ring::builder()
        .strategy(Strategy::Vnodes(VnodeParams::default()))
        .nodes(fixtures::three_zones(4))
        .build()?;
    let shares = vnode_ring.expected_shares(samples(500));
    let primary: u32 = shares.iter().map(|share| share.primary_bps).sum();
    assert!(
        (9_988..=10_000).contains(&primary),
        "token arcs sum to {primary}"
    );
    Ok(())
}
