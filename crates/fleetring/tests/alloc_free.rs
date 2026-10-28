//! Lookups must not allocate for typical shapes (ADR-0006, point 7), because routers call them
//! on every request. This binary installs dhat's counting allocator, so it runs without the
//! libtest harness.

#[cfg(not(target_family = "wasm"))]
#[global_allocator]
static ALLOCATOR: dhat::Alloc = dhat::Alloc;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    #[cfg(not(target_family = "wasm"))]
    check_lookups_do_not_allocate()?;
    Ok(())
}

#[cfg(not(target_family = "wasm"))]
fn check_lookups_do_not_allocate() -> Result<(), Box<dyn std::error::Error>> {
    use std::hint::black_box;

    use fleetring::{Node, ReplicationFactor, Ring, Strategy, VnodeParams, Weight};

    // 1000 nodes over 8 zones, pairs of nodes sharing a host, mixed weights so that rendezvous
    // takes the logarithm path.
    let mut nodes = Vec::with_capacity(1000);
    for i in 0u32..1000 {
        let zone = i % 8;
        let node = Node::new(format!("node-{i:04}").parse()?)
            .with_zone(format!("zone-{zone}").parse()?)
            .with_host(format!("host-{zone}-{:03}", i / 16).parse()?)
            .with_weight(Weight::new(1 + i % 4).ok_or("zero weight")?);
        nodes.push(node);
    }
    let rendezvous = Ring::builder().nodes(nodes.clone()).build()?;
    let vnodes = Ring::builder()
        .strategy(Strategy::Vnodes(VnodeParams::default()))
        .nodes(nodes)
        .build()?;
    let cells = two_level_cells()?;
    let keys: Vec<String> = (0..500).map(|i| format!("agent:{i}")).collect();
    let factors: Vec<ReplicationFactor> = (1..=8).filter_map(ReplicationFactor::new).collect();

    let _profiler = dhat::Profiler::builder().testing().build();
    let before = dhat::HeapStats::get();
    for ring in [&rendezvous, &vnodes] {
        for key in &keys {
            for &factor in &factors {
                black_box(ring.replicas_n(key, factor));
            }
            black_box(ring.primary(key));
        }
    }
    let after = dhat::HeapStats::get();
    dhat::assert_eq!(
        after.total_blocks,
        before.total_blocks,
        "replicas() allocated on a 1000-node, 8-zone ring"
    );

    let before = dhat::HeapStats::get();
    for key in &keys {
        black_box(cells.replicas(key));
        black_box(cells.primary(key));
        black_box(cells.cell_of(key));
        black_box(cells.replicas_where(key, |node| !node.id().as_str().ends_with("-00")));
    }
    let after = dhat::HeapStats::get();
    dhat::assert_eq!(
        after.total_blocks,
        before.total_blocks,
        "cell lookups allocated on a two-level cells tree"
    );
    println!(
        "alloc_free: 0 allocations across {} ring lookups (rendezvous and vnodes) and {} cell lookups",
        keys.len() * factors.len().saturating_add(1) * 2,
        keys.len() * 4
    );
    Ok(())
}

/// Four regions, each a cells layer of four zone rings with 50 nodes of mixed weights, so every
/// placement path holds two cells. A directory rule sends keys starting with `agent:1` to
/// `region-0`, so lookups take both the directory path and the selector-ring path.
#[cfg(not(target_family = "wasm"))]
fn two_level_cells()
-> Result<fleetring::Cells<fleetring::Cells<fleetring::Ring>>, Box<dyn std::error::Error>> {
    use fleetring::{Cells, Directory, Node, Ring, StableHasher, Weight, Xxh3};

    // Every layer hashes with a seed of its own, derived from its path.
    let seed = |path: &str| Xxh3.hash(path.as_bytes(), 0);
    let mut regions = Cells::builder();
    let mut region_selector = Ring::builder().seed(seed(""));
    for r in 0..4 {
        let region = format!("region-{r}");
        let mut zones = Cells::builder();
        let mut zone_selector = Ring::builder().seed(seed(&region));
        for z in 0..4 {
            let zone = format!("{region}.zone-{z}");
            let mut ring = Ring::builder().seed(seed(&zone));
            for n in 0u32..50 {
                let node = Node::new(format!("{zone}.node-{n:02}").parse()?)
                    .with_weight(Weight::new(1 + n % 4).ok_or("zero weight")?);
                ring = ring.node(node);
            }
            zone_selector = zone_selector.node(Node::new(zone.parse()?));
            zones = zones.cell(zone.parse()?, ring.build()?);
        }
        region_selector = region_selector.node(Node::new(region.parse()?));
        regions = regions.cell(
            region.parse()?,
            zones.ring_selector(zone_selector.build()?).build()?,
        );
    }
    Ok(regions
        .directory(Directory::new().prefix("agent:1", "region-0".parse()?))
        .ring_selector(region_selector.build()?)
        .build()?)
}
