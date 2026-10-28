//! `Ring::replicas` must not allocate for typical shapes (ADR-0006, point 7), because routers
//! call it on every request. This binary installs dhat's counting allocator, so it runs
//! without the libtest harness.

#[cfg(not(target_family = "wasm"))]
#[global_allocator]
static ALLOCATOR: dhat::Alloc = dhat::Alloc;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    #[cfg(not(target_family = "wasm"))]
    check_replicas_do_not_allocate()?;
    Ok(())
}

#[cfg(not(target_family = "wasm"))]
fn check_replicas_do_not_allocate() -> Result<(), Box<dyn std::error::Error>> {
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
    println!(
        "alloc_free: 0 allocations across {} lookups (rendezvous and vnodes)",
        keys.len() * factors.len().saturating_add(1) * 2
    );
    Ok(())
}
