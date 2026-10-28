//! Lookup and build benchmarks (FR-016). Run with `cargo bench -p fleetring --bench lookup`.
//!
//! Budgets live in `docs/perf.md`. Shared CI runners are too noisy to gate on these numbers, so
//! CI only checks that the benches build.

use divan::Bencher;
use fleetring::{Node, Ring, Strategy, VnodeParams, Weight};

fn main() {
    divan::main();
}

/// Node counts for every benchmark.
const SIZES: &[usize] = &[3, 10, 100, 1000];

/// How each benchmark ring is built.
#[derive(Debug, Clone, Copy)]
enum Shape {
    /// Rendezvous, every node weight 1, so ranking never computes a logarithm.
    RendezvousUniform,
    /// Rendezvous with weights 1..=4, so every draw takes the fixed-point logarithm.
    RendezvousMixed,
    /// The vnode ring at the default 256 tokens per unit of weight, weights 1..=4.
    Vnodes,
}

fn ring(shape: Shape, nodes: usize) -> Ring {
    let mixed = !matches!(shape, Shape::RendezvousUniform);
    let members = (0..nodes).map(|i| {
        let weight = if mixed { 1 + i % 4 } else { 1 };
        Node::new(format!("node-{i:04}").parse().expect("valid id"))
            .with_zone(format!("zone-{}", i % 8).parse().expect("valid zone"))
            .with_weight(
                Weight::new(u32::try_from(weight).expect("small weight")).expect("nonzero"),
            )
    });
    let strategy = match shape {
        Shape::RendezvousUniform | Shape::RendezvousMixed => Strategy::Rendezvous,
        Shape::Vnodes => Strategy::Vnodes(VnodeParams::default()),
    };
    Ring::builder()
        .strategy(strategy)
        .nodes(members)
        .build()
        .expect("valid ring")
}

fn keys() -> Vec<String> {
    (0..1024).map(|i| format!("agent:{i:05}")).collect()
}

fn bench_lookups(bencher: Bencher, shape: Shape, nodes: usize, primary_only: bool) {
    let ring = ring(shape, nodes);
    let keys = keys();
    let mut next = 0;
    bencher.bench_local(|| {
        next = (next + 1) % keys.len();
        let key = divan::black_box(&keys[next]);
        if primary_only {
            divan::black_box(ring.primary(key));
        } else {
            divan::black_box(ring.replicas(key));
        }
    });
}

mod replicas_r3 {
    use super::{Bencher, SIZES, Shape, bench_lookups};

    #[divan::bench(args = SIZES)]
    fn rendezvous_uniform(bencher: Bencher, nodes: usize) {
        bench_lookups(bencher, Shape::RendezvousUniform, nodes, false);
    }

    #[divan::bench(args = SIZES)]
    fn rendezvous_mixed(bencher: Bencher, nodes: usize) {
        bench_lookups(bencher, Shape::RendezvousMixed, nodes, false);
    }

    #[divan::bench(args = SIZES)]
    fn vnodes(bencher: Bencher, nodes: usize) {
        bench_lookups(bencher, Shape::Vnodes, nodes, false);
    }
}

mod primary_r1 {
    use super::{Bencher, SIZES, Shape, bench_lookups};

    #[divan::bench(args = SIZES)]
    fn rendezvous_mixed(bencher: Bencher, nodes: usize) {
        bench_lookups(bencher, Shape::RendezvousMixed, nodes, true);
    }

    #[divan::bench(args = SIZES)]
    fn vnodes(bencher: Bencher, nodes: usize) {
        bench_lookups(bencher, Shape::Vnodes, nodes, true);
    }
}

mod preference {
    use super::{Bencher, SIZES, Shape, keys, ring};

    #[divan::bench(args = SIZES)]
    fn rendezvous_mixed(bencher: Bencher, nodes: usize) {
        let ring = ring(Shape::RendezvousMixed, nodes);
        let keys = keys();
        let mut next = 0;
        bencher.bench_local(|| {
            next = (next + 1) % keys.len();
            divan::black_box(ring.preference(divan::black_box(&keys[next])).count())
        });
    }
}

mod build {
    use super::{Bencher, SIZES, Shape, ring};

    #[divan::bench(args = SIZES)]
    fn rendezvous(bencher: Bencher, nodes: usize) {
        bencher.bench(|| ring(Shape::RendezvousMixed, nodes));
    }

    #[divan::bench(args = SIZES, sample_count = 20)]
    fn vnodes(bencher: Bencher, nodes: usize) {
        bencher.bench(|| ring(Shape::Vnodes, nodes));
    }
}

mod diff {
    use fleetring::Node;

    use super::{Bencher, Shape, ring};

    /// Keys per diff; divan reports the time per diff, so divide by this for per-key cost.
    const KEYS: usize = 10_000;

    #[divan::bench(args = [10, 100], sample_count = 20)]
    fn add_one_node(bencher: Bencher, nodes: usize) {
        let current = ring(Shape::RendezvousMixed, nodes);
        let target = current
            .to_builder()
            .node(Node::new("node-new".parse().expect("valid id")))
            .build()
            .expect("valid ring");
        let keys: Vec<String> = (0..KEYS).map(|i| format!("agent:{i:05}")).collect();
        bencher
            .counter(divan::counter::ItemsCount::new(KEYS))
            .bench_local(|| current.diff(&target, &keys).summarize());
    }
}

mod bounded {
    use fleetring::{BoundedLoads, Headroom};

    use super::{Bencher, SIZES, Shape, keys, ring};

    #[divan::bench(args = SIZES)]
    fn replicas(bencher: Bencher, nodes: usize) {
        let ring = ring(Shape::RendezvousMixed, nodes);
        let loads = ring.nodes().iter().enumerate().map(|(i, node)| {
            (
                node.id().clone(),
                u64::try_from(i).expect("small index") * 1_000,
            )
        });
        let policy = BoundedLoads::new(Headroom::from_percent(25));
        let view = policy
            .load_view(&ring, loads)
            .expect("loads name ring nodes");
        let keys = keys();
        let mut next = 0;
        bencher.bench_local(|| {
            next = (next + 1) % keys.len();
            divan::black_box(view.replicas(divan::black_box(&keys[next]), 1))
        });
    }
}
