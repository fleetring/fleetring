//! Shards cache keys over weighted nodes, the way a Redis-style cluster spreads its keyspace.
//!
//! A cache keeps one copy of each key, so the ring has a replication factor of 1 and each key's
//! preferred primary is its owner. Weights express capacity: `cache-4` has four times the memory
//! of `cache-1`, so it should own four times as many keys. Under weighted rendezvous hashing, the
//! default strategy, each node's expected share of keys is exactly its share of the total weight.
//!
//! The example prints those exact shares, from `Ring::expected_shares`, next to a count over
//! 100,000 keys.
//!
//! ```sh
//! cargo run -p fleetring --example kv_sharding
//! ```

use std::collections::BTreeMap;
use std::error::Error;
use std::num::NonZeroU32;

use fleetring::{Node, NodeId, ReplicationFactor, Ring, Weight};

/// How many keys to place.
const KEYS: u64 = 100_000;

fn main() -> Result<(), Box<dyn Error>> {
    let ring = Ring::builder()
        .replication_factor(ReplicationFactor::ONE)
        .node(cache("cache-1", 1)?)
        .node(cache("cache-2", 2)?)
        .node(cache("cache-3", 1)?)
        .node(cache("cache-4", 4)?)
        .build()?;

    let mut owned: BTreeMap<&NodeId, u64> = BTreeMap::new();
    for i in 0..KEYS {
        let owner = ring.primary(format!("user:{i}:session"));
        *owned.entry(owner.id()).or_default() += 1;
    }

    // Primary shares are exact. The sample count only matters for replica shares, which equal
    // primary shares at a replication factor of 1.
    let shares = ring.expected_shares(NonZeroU32::new(1_000).ok_or("zero samples")?);

    println!("{KEYS} keys over {} nodes\n", ring.node_count());
    println!("node     weight  expected  counted");
    for (node, share) in ring.nodes().iter().zip(&shares) {
        let count = owned.get(node.id()).copied().unwrap_or(0);
        println!(
            "{:<7}  {:>6}  {:>8}  {:>7}",
            node.id(),
            node.weight(),
            percent(u64::from(share.primary_bps)),
            percent(count * 10_000 / KEYS),
        );
    }
    Ok(())
}

/// Returns a cache node with the given weight.
fn cache(id: &str, weight: u32) -> Result<Node, Box<dyn Error>> {
    Ok(Node::new(id.parse()?).with_weight(Weight::new(weight).ok_or("zero weight")?))
}

/// Formats basis points as a percentage with two decimals, using integers only.
fn percent(bps: u64) -> String {
    format!("{}.{:02}%", bps / 100, bps % 100)
}
