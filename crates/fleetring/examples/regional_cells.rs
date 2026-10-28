//! Keeps EU and US tenants in their regions, and spreads every other tenant over both.
//!
//! A SaaS fleet runs in two regions. Tenants with a residency contract must stay on machines in
//! their region; every other tenant may live in either. A cells layer expresses exactly that:
//!
//! - **Residency rules first.** A directory sends `eu:` keys to the EU cell and `us:` keys to the
//!   US cell, and an exact rule pins one large tenant to the EU cell.
//! - **Then a ring.** Every other key is hashed to a cell by a selector ring whose nodes are the
//!   cells. The US cell has twice the machines, so it has twice the weight.
//! - **Zone-aware rings inside.** Each cell is an ordinary ring that spreads a key's three
//!   replicas across its region's zones.
//!
//! The example prints where tenants land, fails a node over inside its cell, then plans two
//! changes: growing a cell, which moves keys only inside it, and reweighting a cell, which moves
//! keys only between cells.
//!
//! ```sh
//! cargo run -p fleetring --example regional_cells
//! ```

use std::error::Error;

use fleetring::{CellPlacement, Cells, Directory, Node, Ring, StableHasher, Weight, Xxh3};

fn main() -> Result<(), Box<dyn Error>> {
    let eu = region(
        "eu",
        &["eu-west-1a", "eu-west-1b", "eu-central-1a"],
        &["eu-1", "eu-2", "eu-3", "eu-4", "eu-5", "eu-6"],
    )?;
    let us = region(
        "us",
        &["us-east-1a", "us-east-1b", "us-west-2a"],
        &[
            "us-1", "us-2", "us-3", "us-4", "us-5", "us-6", "us-7", "us-8", "us-9", "us-10",
            "us-11", "us-12",
        ],
    )?;
    let fleet = build_fleet(eu.clone(), us.clone(), 1)?;
    println!(
        "{} cells, fingerprint {}\n",
        fleet.cell_count(),
        fleet.fingerprint()
    );

    println!("{:<18}  {:<4}  replicas", "tenant", "cell");
    for tenant in [
        "eu:tenant:acme",
        "eu:tenant:globex",
        "us:tenant:initech",
        "tenant:umbrella",
        "tenant:hooli",
        "tenant:tyrell",
        "tenant:soylent",
        "tenant:cyberdyne",
        // Pinned to the EU cell by an exact rule.
        "tenant:wayne",
    ] {
        let placed = fleet.replicas(tenant);
        println!(
            "{tenant:<18}  {:<4}  {}",
            cell_name(&placed),
            describe(&placed)
        );
    }

    // Liveness is a filter inside the key's cell, so failover keeps zones diverse and never
    // leaves the region.
    let key = "eu:tenant:acme";
    let down = fleet.primary(key).id().clone();
    let failover = fleet
        .replicas_where(key, |node| node.id() != &down)
        .ok_or("every EU node is down")?;
    println!("\n{key} with {down} down: {}", describe(&failover));
    let region_down = fleet.replicas_where(key, |node| !node.id().as_str().starts_with("eu-"));
    assert!(
        region_down.is_none(),
        "a residency key never leaves its cell"
    );
    println!("{key} with the whole EU cell down: unavailable, never moved to the US");

    let keys: Vec<String> = (0..3_000)
        .map(|i| match i % 3 {
            0 => format!("eu:tenant:{i}"),
            1 => format!("us:tenant:{i}"),
            _ => format!("tenant:{i}"),
        })
        .collect();

    // A new EU machine changes keys only inside the EU cell: cell weights are explicit, so
    // growing a cell never pulls keys in from another one.
    let grown_eu = eu
        .to_builder()
        .node(Node::new("eu-7".parse()?).with_zone("eu-west-1a".parse()?))
        .build()?;
    let grown = build_fleet(grown_eu, us.clone(), 1)?;
    println!(
        "\nadding eu-7 to the EU cell:\n{}",
        fleet.diff(&grown, &keys).summarize()
    );

    // Doubling the EU cell's weight is a deliberate cross-cell migration, and only keys without
    // a residency rule move: from the US cell to the EU cell.
    let reweighted = build_fleet(eu, us, 2)?;
    println!(
        "\ndoubling the EU cell's weight:\n{}",
        fleet.diff(&reweighted, &keys).summarize()
    );
    Ok(())
}

/// Builds the layer: residency rules, then a selector ring that weights the EU cell `eu_weight`
/// and the US cell 2.
fn build_fleet(eu: Ring, us: Ring, eu_weight: u32) -> Result<Cells, Box<dyn Error>> {
    let rules = Directory::new()
        .prefix("eu:", "eu".parse()?)
        .prefix("us:", "us".parse()?)
        .exact("tenant:wayne", "eu".parse()?);
    let selector = Ring::builder()
        .seed(seed(""))
        .node(Node::new("eu".parse()?).with_weight(Weight::new(eu_weight).ok_or("zero weight")?))
        .node(Node::new("us".parse()?).with_weight(Weight::new(2).ok_or("zero weight")?))
        .build()?;
    Ok(Cells::builder()
        .directory(rules)
        .ring_selector(selector)
        .cell("eu".parse()?, eu)
        .cell("us".parse()?, us)
        .build()?)
}

/// Builds a cell's ring: `nodes` dealt round-robin into `zones`, under the seed of its path.
fn region(path: &str, zones: &[&str], nodes: &[&str]) -> Result<Ring, Box<dyn Error>> {
    let mut builder = Ring::builder().seed(seed(path));
    for (id, zone) in nodes.iter().zip(zones.iter().cycle()) {
        builder = builder.node(Node::new(id.parse()?).with_zone(zone.parse()?));
    }
    Ok(builder.build()?)
}

/// Derives a ring's seed from the path of the cell that holds it, the derivation `Cells`
/// recommends: every layer then hashes keys with a seed of its own.
fn seed(path: &str) -> u64 {
    Xxh3.hash(path.as_bytes(), 0)
}

/// The key's cell, or `-` for a placement without one.
fn cell_name(placed: &CellPlacement<'_>) -> String {
    placed
        .cell()
        .map_or_else(|| "-".to_owned(), ToString::to_string)
}

/// Formats a placement's replicas as `id (zone)`, primary first.
fn describe(placed: &CellPlacement<'_>) -> String {
    let replicas: Vec<String> = placed
        .replicas()
        .iter()
        .map(|node| match node.zone() {
            Some(zone) => format!("{} ({zone})", node.id()),
            None => node.id().to_string(),
        })
        .collect();
    replicas.join(", ")
}
