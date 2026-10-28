//! Places tenant databases across three zones, the way FleetRing does.
//!
//! FleetRing runs one SQLite database per tenant on machines you own. For each database, the
//! ring picks three nodes: the preferred primary, which takes writes once its lease confirms it,
//! and two followers that replicate it. Replicas spread across zones first, so losing a whole
//! zone never loses a database, and a node's weight sets its share of primaries.
//!
//! Placement needs nothing but the ring and the key, so every router, client, and daemon that
//! holds the same ring computes the same answer locally, with no lookup service.
//!
//! ```sh
//! cargo run -p fleetring --example database_fleet
//! ```

use std::error::Error;

use fleetring::{Node, ReplicationFactor, Ring, Weight};

fn main() -> Result<(), Box<dyn Error>> {
    // Six nodes in three zones. The `eu-west-1c` machines have twice the disk of the others, so
    // they get twice the weight.
    let ring = Ring::builder()
        .node(node("db-1", "eu-west-1a", 1)?)
        .node(node("db-2", "eu-west-1a", 1)?)
        .node(node("db-3", "eu-west-1b", 1)?)
        .node(node("db-4", "eu-west-1b", 1)?)
        .node(node("db-5", "eu-west-1c", 2)?)
        .node(node("db-6", "eu-west-1c", 2)?)
        .build()?;
    println!(
        "{} nodes in {} zones, {} replicas per database\n",
        ring.node_count(),
        ring.zone_count(),
        ring.replication_factor(),
    );

    println!("{:<16}  {:<17}  followers", "database", "primary");
    let tenants = [
        "tenant:acme",
        "tenant:globex",
        "tenant:initech",
        "tenant:umbrella",
        "tenant:hooli",
        "tenant:stark",
    ];
    for tenant in tenants {
        let replicas = ring.replicas(tenant);
        let followers: Vec<String> = replicas.followers().map(describe).collect();
        let primary = describe(replicas.primary());
        println!("{tenant:<16}  {primary:<17}  {}", followers.join(", "));
    }

    // A tenant on a premium plan gets five replicas. The set for a replication factor is always a
    // prefix of the set for a larger one, so the upgrade keeps the existing three replicas and
    // only copies the database to two more nodes.
    let premium = ReplicationFactor::new(5).ok_or("zero replicas")?;
    println!(
        "\ntenant:acme with {premium} replicas: {}",
        ring.replicas_n("tenant:acme", premium),
    );
    Ok(())
}

/// Returns a node in `zone` with the given weight.
fn node(id: &str, zone: &str, weight: u32) -> Result<Node, Box<dyn Error>> {
    Ok(Node::new(id.parse()?)
        .with_zone(zone.parse()?)
        .with_weight(Weight::new(weight).ok_or("zero weight")?))
}

/// Formats a node as `id (zone)`.
fn describe(node: &Node) -> String {
    match node.zone() {
        Some(zone) => format!("{} ({zone})", node.id()),
        None => node.id().to_string(),
    }
}
