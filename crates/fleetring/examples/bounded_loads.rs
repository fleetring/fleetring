//! Places new LLM conversations on GPU nodes by KV-cache memory, with bounded loads.
//!
//! An inference router pins each conversation to one GPU node, so every turn reuses the KV cache
//! that node already holds. Hashing balances the number of conversations per node, but not their
//! size: one conversation may carry a long document and use a hundred times the memory of
//! another. Bounded loads corrects for that when a conversation starts. Given the bytes of KV
//! cache each node holds, it skips the nodes that would end up more than 25% above their
//! weighted share of the total, and picks the next node in the conversation's preference list.
//!
//! The result depends on the loads, so other routers can't recompute it from the ring. A router
//! records an override for every conversation placed away from its home node, and adds each
//! placement to the loads before placing the next one.
//!
//! ```sh
//! cargo run -p fleetring --example bounded_loads
//! ```

use std::error::Error;

use fleetring::{BoundedLoads, Headroom, Node, ReplicationFactor, Ring, Weight};

/// Bytes in a gibibyte.
const GIB: u64 = 1 << 30;

fn main() -> Result<(), Box<dyn Error>> {
    // Weights follow GPU memory: the 80 GiB cards weigh twice as much as the 40 GiB cards.
    let ring = Ring::builder()
        .replication_factor(ReplicationFactor::ONE)
        .node(gpu("gpu-1", 2)?)
        .node(gpu("gpu-2", 2)?)
        .node(gpu("gpu-3", 1)?)
        .node(gpu("gpu-4", 1)?)
        .build()?;
    let policy = BoundedLoads::new(Headroom::from_percent(25));

    // KV-cache bytes each node holds now. A few long conversations keep gpu-2 busy.
    let mut loads = [
        ("gpu-1", 30 * GIB),
        ("gpu-2", 70 * GIB),
        ("gpu-3", 14 * GIB),
        ("gpu-4", 16 * GIB),
    ];

    // New conversations and the KV cache each is expected to need.
    let arrivals = [
        ("chat:1001", 2 * GIB),
        ("chat:1002", GIB),
        ("chat:1003", 6 * GIB),
        ("chat:1004", 2 * GIB),
        ("chat:1005", GIB),
        ("chat:1006", 3 * GIB),
        ("chat:1007", 2 * GIB),
        ("chat:1008", GIB),
    ];

    println!("conversation  KV cache  home   placed");
    for (conversation, bytes) in arrivals {
        // Take a fresh snapshot for every placement, so each one sees the bytes placed before it.
        let view = policy.load_view(&ring, loads)?;
        let placed = view.replicas(conversation, bytes);
        let node = placed.replicas().primary();
        let note = if placed.is_home() {
            "home"
        } else {
            "away from home: record an override"
        };
        println!(
            "{conversation:<12}  {:>4} GiB  {:<5}  {:<5}  {note}",
            bytes / GIB,
            ring.primary(conversation).id(),
            node.id(),
        );
        // Record the placement, so the next snapshot counts its bytes.
        for (id, load) in &mut loads {
            if *id == node.id().as_str() {
                *load += bytes;
            }
        }
    }

    // A node has room while its load plus the new conversation stays within its capacity: its
    // weighted share of the total after the placement, plus the headroom. gpu-2 is over.
    let view = policy.load_view(&ring, loads)?;
    println!("\nnode   weight  KV cache  capacity for a 2 GiB conversation");
    for node in ring.nodes() {
        let id = node.id();
        let load = view.load(id).ok_or("unknown node")?;
        let capacity = view.capacity(id, 2 * GIB).ok_or("unknown node")?;
        println!(
            "{id:<5}  {:>6}  {:>4} GiB  {:>4} GiB",
            node.weight(),
            load / GIB,
            capacity / u128::from(GIB),
        );
    }

    // A conversation too large for any node's headroom still gets a node: its home node. The
    // placement reports that it saturated, so the router can alert or throttle.
    let large = view.replicas("chat:1010", 48 * GIB);
    println!(
        "\nchat:1010, 48 GiB: placed on {}, home: {}, saturated: {}",
        large.replicas().primary().id(),
        large.is_home(),
        large.is_saturated(),
    );
    Ok(())
}

/// Returns a GPU node with the given weight.
fn gpu(id: &str, weight: u32) -> Result<Node, Box<dyn Error>> {
    Ok(Node::new(id.parse()?).with_weight(Weight::new(weight).ok_or("zero weight")?))
}
