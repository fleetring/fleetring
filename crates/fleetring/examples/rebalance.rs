//! Plans the rebalance that follows adding a broker to a message queue.
//!
//! A queue spreads a topic's partitions over its brokers, three replicas each: a leader and two
//! followers. Adding a fifth broker means building the next ring, then diffing the current ring
//! against it over every partition. The diff yields only the partitions whose replicas change,
//! and adding one broker swaps at most one replica of each, always to the new broker.
//!
//! The example prints the first steps of the migration plan, then the movement summary: how many
//! partitions change, how many change leaders, and how many replicas each broker gains or loses.
//!
//! ```sh
//! cargo run -p fleetring --example rebalance
//! ```

use std::error::Error;
use std::num::NonZeroU32;

use fleetring::{Epoch, Node, Ring};

/// Partitions in the topic.
const PARTITIONS: u32 = 512;

fn main() -> Result<(), Box<dyn Error>> {
    let mut brokers = Vec::new();
    for id in ["broker-1", "broker-2", "broker-3", "broker-4"] {
        brokers.push(Node::new(id.parse()?));
    }
    let current = Ring::builder()
        .nodes(brokers)
        .epoch(Epoch::new(1))
        .build()?;

    // Membership changes build a new ring with a newer epoch; the current ring never changes.
    let target = current
        .to_builder()
        .node(Node::new("broker-5".parse()?))
        .epoch(Epoch::new(2))
        .build()?;
    let partitions: Vec<String> = (0..PARTITIONS).map(|p| format!("orders:{p}")).collect();

    // Each step copies first, moves the leader once the new replica has caught up, and drops
    // last, so no partition ever has fewer replicas than it started with.
    println!(
        "first changes, epoch {} to {}:",
        current.epoch(),
        target.epoch()
    );
    for change in current.diff(&target, &partitions).take(8) {
        let mut steps = Vec::new();
        for node in change.added() {
            steps.push(format!("copy to {}", node.id()));
        }
        if change.primary_changed() {
            let (from, to) = (change.from().primary().id(), change.to().primary().id());
            steps.push(format!("move the leader from {from} to {to}"));
        }
        for node in change.removed() {
            steps.push(format!("drop from {}", node.id()));
        }
        println!("  {:<10} {}", change.key(), steps.join(", "));
    }

    // The diff is lazy, so summarizing a fresh one walks every partition again.
    let summary = current.diff(&target, &partitions).summarize();
    println!("\n{summary}");

    // Before the key list is at hand, estimate the same fractions from synthetic keys.
    let estimate = current.estimate_movement(&target, NonZeroU32::new(10_000).ok_or("zero")?);
    let changed_bps = estimate.keys_changed() * 10_000 / estimate.keys_examined();
    println!(
        "\nestimated from {} synthetic keys: {}.{:02}% of keys change",
        estimate.keys_examined(),
        changed_bps / 100,
        changed_bps % 100,
    );
    Ok(())
}
