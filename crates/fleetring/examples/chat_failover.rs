//! Routes chat rooms to their home nodes, and fails over deterministically when nodes go down.
//!
//! Each room lives on a home node, with a standby in another zone. A health checker tells every
//! router which nodes are down, and routers skip those nodes at lookup time with a filter. They
//! never rebuild the ring for it: liveness is not membership, so a node that drops out for a
//! minute moves no data, and its rooms return home as soon as it is back.
//!
//! Filtering out the down nodes gives exactly the placement of a ring built without them, so
//! every router that sees the same health report picks the same fallback, without coordinating.
//!
//! ```sh
//! cargo run -p fleetring --example chat_failover
//! ```

use std::collections::BTreeSet;
use std::error::Error;

use fleetring::{Node, ReplicationFactor, Ring};

/// The chat servers and their zones.
const SERVERS: [(&str, &str); 6] = [
    ("chat-a1", "us-east-1a"),
    ("chat-a2", "us-east-1a"),
    ("chat-b1", "us-east-1b"),
    ("chat-b2", "us-east-1b"),
    ("chat-c1", "us-east-1c"),
    ("chat-c2", "us-east-1c"),
];

/// The rooms to route.
const ROOMS: [&str; 6] = [
    "room:general",
    "room:random",
    "room:rust",
    "room:ops",
    "room:design",
    "room:support",
];

fn main() -> Result<(), Box<dyn Error>> {
    let ring = build_ring(SERVERS)?;
    println!("home placement: [home, standby]");
    for room in ROOMS {
        println!("  {room:<13} {}", ring.replicas(room));
    }

    // The health checker reports two servers down. A filter accepts any predicate over nodes.
    let down = BTreeSet::from(["chat-a2", "chat-c2"]);
    let is_up = |node: &Node| !down.contains(node.id().as_str());

    // The filter applies before replicas spread across zones, so when a zone's best server is
    // down, that zone's next-best server takes its place and the standby stays in another zone.
    let names: Vec<&str> = down.iter().copied().collect();
    println!("\nwith {} down:", names.join(" and "));
    for room in ROOMS {
        let live = ring
            .replicas_where(room, is_up)
            .ok_or("every server is down")?;
        let status = if live == ring.replicas(room) {
            "unchanged"
        } else {
            "failed over"
        };
        println!("  {room:<13} {live}  {status}");
    }

    // The full preference list among live servers: where the room goes if more servers fail.
    // Each zone's best live server comes before any zone's second one.
    let order: Vec<&str> = ring
        .preference_where("room:general", is_up)
        .map(|node| node.id().as_str())
        .collect();
    println!("\nroom:general failover order: {}", order.join(" -> "));

    // Another router builds its own ring from the same membership, listed in another order, and
    // picks the same fallback for every room.
    let mut reversed = SERVERS;
    reversed.reverse();
    let other_router = build_ring(reversed)?;
    let agree = ROOMS
        .iter()
        .all(|room| ring.replicas_where(room, is_up) == other_router.replicas_where(room, is_up));
    println!("\nanother router agrees on every room: {agree}");

    // Failover equals placement on a ring built without the down servers. That is why routers
    // can fail over on their own, and why a filter never needs to move data.
    let survivors = down
        .iter()
        .fold(ring.to_builder(), |builder, id| builder.remove_node(id))
        .build()?;
    let same = ROOMS
        .iter()
        .all(|room| ring.replicas_where(room, is_up) == Some(survivors.replicas(room)));
    println!("same as a ring without the down servers: {same}");
    Ok(())
}

/// Builds a ring with two replicas per room, home and standby, from `(id, zone)` pairs.
fn build_ring(servers: [(&str, &str); 6]) -> Result<Ring, Box<dyn Error>> {
    let mut builder =
        Ring::builder().replication_factor(ReplicationFactor::new(2).ok_or("zero replicas")?);
    for (id, zone) in servers {
        builder = builder.node(Node::new(id.parse()?).with_zone(zone.parse()?));
    }
    Ok(builder.build()?)
}
