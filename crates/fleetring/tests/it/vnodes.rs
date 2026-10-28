//! The vnode strategy through the public API (FR-010).

use std::num::NonZeroU32;

use fleetring::{BuildRingError, Node, Ring, Strategy, VnodeParams, Weight};

use crate::common::corpus::{Corpus, MEDIUM};
use crate::common::fixtures;
use crate::common::stats::{expected_shares, max_relative_deviation, tally};

type TestResult = Result<(), Box<dyn std::error::Error>>;

pub(crate) fn vnodes(tokens_per_weight: u32) -> Strategy {
    let tokens = NonZeroU32::new(tokens_per_weight).expect("tokens per weight is nonzero");
    Strategy::Vnodes(VnodeParams::new(tokens))
}

fn heavy_node(weight: u32) -> Result<Node, Box<dyn std::error::Error>> {
    Ok(Node::new("big".parse()?).with_weight(Weight::new(weight).ok_or("zero weight")?))
}

#[test]
fn the_token_budget_is_exactly_two_to_the_twenty_second() -> TestResult {
    // 1024 × 4096 = 2²² tokens is allowed; one more unit of weight is not.
    Ring::builder()
        .strategy(vnodes(4096))
        .node(heavy_node(1024)?)
        .build()?;
    let error = Ring::builder()
        .strategy(vnodes(4096))
        .node(heavy_node(1025)?)
        .build()
        .expect_err("one weight unit over the budget");
    assert_eq!(
        error,
        BuildRingError::TooManyTokens {
            total: 1025 * 4096,
            max: 1 << 22
        }
    );
    assert_eq!(
        error.to_string(),
        "ring needs 4198400 tokens; the maximum is 4194304"
    );
    Ok(())
}

#[test]
fn extreme_weights_report_too_many_tokens_instead_of_overflowing() -> TestResult {
    let error = Ring::builder()
        .strategy(vnodes(u32::MAX))
        .node(heavy_node(u32::MAX)?)
        .node(Node::new("small".parse()?).with_weight(Weight::new(u32::MAX).ok_or("zero")?))
        .build()
        .expect_err("far over the budget");
    assert_eq!(
        error,
        BuildRingError::TooManyTokens {
            total: u64::MAX,
            max: 1 << 22
        }
    );
    Ok(())
}

#[test]
fn default_tokens_balance_equal_nodes_within_the_adr_bound() -> TestResult {
    // ADR-0004: per-node error is about 1/√tokens; the ticket bounds the worst node by 5/√256.
    let ring = Ring::builder()
        .strategy(Strategy::Vnodes(VnodeParams::default()))
        .nodes(fixtures::three_zones(4))
        .build()?;
    let nodes = ring.nodes();
    let primaries = Corpus::Uuids.keys(MEDIUM).map(|key| {
        let primary = ring.primary(&key);
        nodes
            .binary_search_by(|node| node.id().cmp(primary.id()))
            .expect("the primary belongs to the ring")
    });
    let counts = tally(ring.node_count(), primaries);
    let deviation = max_relative_deviation(&counts, &expected_shares(&fixtures::weights(nodes)));
    assert!(
        deviation < 5.0 / 16.0,
        "worst node is off by {deviation:.3}"
    );
    Ok(())
}

#[test]
fn vnode_rings_reject_nothing_rendezvous_accepts_within_budget() -> TestResult {
    for nodes in [
        fixtures::homelab(),
        fixtures::skewed_zones(),
        fixtures::fleet(40, 4),
    ] {
        let ring = Ring::builder().strategy(vnodes(16)).nodes(nodes).build()?;
        let replicas = ring.replicas("agent:sam-researcher");
        assert_eq!(replicas.len(), 3.min(ring.node_count()));
        assert_eq!(
            ring.preference("agent:sam-researcher").count(),
            ring.node_count()
        );
    }
    Ok(())
}
