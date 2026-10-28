# Performance

Budgets and measurements for the `fleetring` placement crate (FR-016). The rendezvous default (ADR-0004) costs O(N) per lookup, and these numbers show it fits FleetRing's fleets with room to spare.

Run the benchmarks with:

```sh
cargo bench -p fleetring --bench lookup
```

Shared CI runners are too noisy to gate on timings, so CI only builds the benches. Re-measure on reference hardware before a release and update this page.

## Budgets

| Path | Budget | Measured (median) | Verdict |
|---|---|---|---|
| Rendezvous `replicas`, R = 3, N = 100, mixed weights | ≤ 2 µs | 1.55 µs | met |
| Rendezvous `replicas`, R = 3, N = 1000, mixed weights | ≤ 20 µs | 10.3 µs | met |
| Vnode `replicas`, R = 3, N = 1000 | ≤ 1 µs | 0.42 µs | met |
| Vnode `build`, N = 1000 at 256 tokens per weight (weights 1–4, 640k tokens) | ≤ 100 ms | 19 ms | met |
| `diff` throughput, N = 100, adding one node | ≥ 100k keys/s | 350k keys/s | met |

## Measurements

- **Hardware:** Apple M-series laptop, macOS, release profile, Rust 1.99.1.
- **Date:** 2026-10-27.
- **Rings:** nodes spread over 8 zones. "Uniform" means every weight is 1. "Mixed" means weights cycle 1–4, so rendezvous takes the fixed-point logarithm for every draw.

| Benchmark | N = 3 | N = 10 | N = 100 | N = 1000 |
|---|---|---|---|---|
| `replicas` (R = 3), rendezvous, uniform weights | 87 ns | 221 ns | 1.25 µs | 8.56 µs |
| `replicas` (R = 3), rendezvous, mixed weights | 96 ns | 276 ns | 1.55 µs | 10.3 µs |
| `replicas` (R = 3), vnodes | 102 ns | 77 ns | 112 ns | 424 ns |
| `primary`, rendezvous, mixed weights | 79 ns | 177 ns | 1.05 µs | 9.04 µs |
| `primary`, vnodes | 30 ns | 33 ns | 49 ns | 161 ns |
| `preference` (full list), rendezvous, mixed weights | 208 ns | 510 ns | 4.69 µs | 55.0 µs |
| `build`, rendezvous | 0.62 µs | 1.81 µs | 19.0 µs | 156 µs |
| `build`, vnodes at 256 tokens per weight | 29 µs | 119 µs | 1.43 ms | 19.0 ms |
| `LoadView::replicas` (bounded loads), rendezvous, mixed weights | 346 ns | 551 ns | 3.67 µs | 23.2 µs |
| `diff` + `summarize`, per key, rendezvous, mixed weights | — | 0.55 µs | 2.85 µs | — |

## Notes

- **Rendezvous cost:** about 8.5 ns per node with uniform weights. That's one 16-byte XXH3 hash plus the champion scan.
  - Mixed weights add about 1.7 ns per node for the table-based logarithm. ADR-0003 records why the table replaced repeated squaring, which costs 105.6 ns per call.
- **Vnode lookups:** they are nearly flat in N because of the binary search. N = 3 is slower than N = 10 because R = 3 makes the walk find *every* node, including the lightest one, which owns the fewest tokens. With 10 nodes, three distinct zones turn up within a few tokens.
- **Bounded loads:** a placement runs two selections, home placement and then the filtered one, plus the capacity checks, so it costs about twice a plain `replicas` call.
- **Allocation:** `replicas` and `primary` allocate nothing for R ≤ 8, and `tests/alloc_free.rs` enforces this. `preference` allocates O(N) by design.
