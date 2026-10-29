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
| Cells `replicas`, R = 3, 100 cells × 100 nodes, rendezvous selector and cells (FR-020) | ≤ 5 µs | 2.3–3.5 µs | met |

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

### Cells

FR-020's layout, from `cargo bench -p fleetring --bench lookup -- cells`, measured 2026-10-28 on the same laptop: 100 cells of 100 nodes each, 10,000 nodes in all. Each cell's ring spreads its nodes over 8 zones with weights cycling 1–4, and the selector ring weighs every cell 1. Each lookup reaches one of the 100 cell rings, so the two cells benches take 1000 samples of 20 lookups each.

| Benchmark | Median |
|---|---|
| `Cells::replicas` (R = 3), rendezvous selector ring and cell rings | 2.19 µs |
| `Cells::replicas` (R = 3), vnode selector ring and cell rings, 256 tokens per weight | 0.30 µs |
| `Ring::replicas` (R = 3), one flat rendezvous ring of all 10,000 nodes, for comparison | 87 µs |

## Notes

- **Rendezvous cost:** about 8.5 ns per node with uniform weights. That's one 16-byte XXH3 hash plus the champion scan.
  - Mixed weights add about 1.7 ns per node for the table-based logarithm. ADR-0003 records why the table replaced repeated squaring, which costs 105.6 ns per call.
- **Vnode lookups:** they are nearly flat in N because of the binary search. N = 3 is slower than N = 10 because R = 3 makes the walk find *every* node, including the lightest one, which owns the fewest tokens. With 10 nodes, three distinct zones turn up within a few tokens.
- **Bounded loads:** a placement runs two selections, home placement and then the filtered one, plus the capacity checks, so it costs about twice a plain `replicas` call.
- **Cells:** a lookup costs one selector lookup plus one cell lookup. With rendezvous, that is a 100-way `primary` on the selector ring, a 100-way `replicas` in the cell (1.55 µs), and a binary search for the cell's ID. Splitting 10,000 nodes into 100 cells makes rendezvous lookups about 40 times faster than one flat ring, as ADR-0012 predicted.
  - The cells numbers were measured on an idle machine before the 0.1.0 release.
- **Allocation:** `replicas` and `primary` allocate nothing for R ≤ 8, and `tests/alloc_free.rs` enforces this. `preference` allocates O(N) by design.

## fleetringd replication

FR-030's replicated writes, from `cargo test -p fleetringd --release --test replication -- --ignored --nocapture`, measured 2026-10-28 on an Apple M1 Max laptop (macOS, APFS, Rust 1.99.1).

- **Setup:** three data nodes in one process, each with its own registry, replication, and `Replication` server on loopback TCP, and a control daemon for membership. The primary runs SQLite with `synchronous = FULL` and 512-byte pages; each follower appends every batch to its mirrored WAL and fsyncs before acknowledging.
- **Workload:** 2,000 small transactions per row, each an `INSERT OR REPLACE` of one 64-byte row, split evenly across concurrent writers on one database. Latency is from `execute` to its acknowledgement: commit, capture, shipping, and the write quorum (W = 2 of R = 3, so one follower). R = 1 is the same path with no followers.

| R | Checkpoint every | Writers | Throughput (tx/s) | p50 ack | p99 ack |
|---|---|---|---|---|---|
| 1 | 4 MiB | 1 | 147 | 6.3 ms | 13.5 ms |
| 3 | 4 MiB | 1 | 101 | 9.5 ms | 19.9 ms |
| 3 | 4 MiB | 4 | 91 | 42.3 ms | 66.4 ms |
| 3 | 4 MiB | 16 | 116 | 152 ms | 220 ms |
| 3 | 4 MiB | 64 | 175 | 338 ms | 763 ms |
| 1 | 256 KiB | 1 | 131 | 6.4 ms | 21.0 ms |
| 3 | 256 KiB | 1 | 100 | 9.5 ms | 20.7 ms |
| 3 | 256 KiB | 4 | 95 | 40.7 ms | 74.3 ms |
| 3 | 256 KiB | 16 | 119 | 133 ms | 260 ms |
| 3 | 256 KiB | 64 | 152 | 339 ms | 849 ms |

- **The primary's own commit dominates.** With no followers a commit already takes about 6 ms, nearly all of it SQLite's fsync. Replication adds about 3 ms: one loopback round trip plus the follower's fsync.
- **One database doesn't scale with writers**, as ADR-0016 predicts: the actor commits one transaction at a time, each with its own fsync, so more writers mostly queue. Batching on the replication side (commits that arrive while 8 batches are in flight join one batch) lifts 64 writers to about 1.7 times one writer's throughput. Fleet throughput comes from many databases, each with its own actor.
- **Checkpoint threshold:** at this size it barely matters. When these numbers were taken, capture reread the whole WAL after every commit (FR-026), which a 4 MiB threshold kept cheap from the page cache; `fleetring-wal` now reads only the tail after the actor's position (FR-039), so the threshold no longer bears on the write path.
- Laptop numbers on a shared machine; re-measure on reference hardware before relying on them.


## Change capture

FR-039's logical change capture (ADR-0024), from `cargo bench -p fleetringd --bench capture`, measured 2026-10-29 on an Apple M1 Max laptop (macOS, APFS, Rust 1.99.1, release profile).

- **Setup:** one SQLite database opened as the daemon opens it (`synchronous = FULL`, 4 KiB pages, WAL), with the write path run directly on the engine adapter: one transaction, the read of the log's new frames the actor does after every commit, and a checkpoint whenever the WAL passes the daemon's default 4 MiB threshold. A recorded database releases each batch at once, as a caught-up sink would, so pruning the change table is part of the cost. No replication.
- **Workload:** inserts of a 4-column row (integer key, text, real, 16-byte blob) in transactions of 1 and 100 rows, with capture off and in each mode. 40 samples of 5 iterations; medians below, with the ratio to capture off.

| Transaction | Capture off | `id` | `after` | `full` |
|---|---|---|---|---|
| 1 row | 82 µs | 92 µs (1.13×) | 92 µs (1.12×) | 97 µs (1.19×) |
| 100 rows | 179 µs | 294 µs (1.64×) | 435 µs (2.4×) | 424 µs (2.4×) |

- **The fixed cost per transaction is one extra page.** A captured transaction dirties the change table's leaf page, which is one more 4 KiB frame in the WAL and in the fsync; the recorder's own statements (one cached `max(change_id)` read at begin and at commit, one cached insert for the commit record, a prune every 64 transactions) cost a few microseconds. A trigger that inserts one tiny row into any second table costs the same 10–30 µs on this machine, so this is the floor for capture into a table.
- **The per-row cost is the trigger, not the JSON.** `id` adds about 1.2 µs per row, which is SQLite firing a trigger and inserting a short row; `after` and `full` add about 1.3 µs more per row for building and storing the row's JSON (about 150 bytes here, so the change table also takes more pages). Building the JSON in a Rust scalar function instead of SQL's `json_array`, `typeof`, and `hex` measured the same, so SQL builds it. Inserts record only the new row in either mode, so `full` costs what `after` does; updates in `full` write two rows and the updated-column list, so expect them closer to twice `after`.
- **What changed from the first measurement.** The first numbers (2.2× on one row, 3.7× on 100 rows) were dominated by the actor re-reading and re-checksumming the whole WAL after every commit, which capture tripled in size; `fleetring-wal` now reads only the tail after the position it already has, which cut the capture-off write from 142 µs to 82 µs as well. The change table also lost its index: `change_id`s are contiguous and `start` never decreases along them, so range bounds are a binary search of point lookups, and a captured transaction dirties one page, not two.
- The slowest samples (several milliseconds) are the checkpoints. Laptop numbers on a shared machine; re-measure on reference hardware before relying on them.
