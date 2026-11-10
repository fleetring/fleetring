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

### Sink marks

FR-041's sink marks (ADR-0028), from `cargo bench -p fleetringd --bench capture -- marks`, measured 2026-11-03 on the same laptop (Rust 1.99.1, release profile) while the machine was heavily loaded by other builds (load average near 900), so only the fastest sample of each case is comparable; medians ran two to five times higher and moved between runs.

- **Setup:** the `insert` cases above, with a 20-byte sink mark staged before every write, which is the worst case: a stream that delivers after each commit. The write that carries it upserts one row of `_fleetring_sinks` before the transaction's own statements. Logical capture off and in `after`, 1 and 100 rows. Fastest of 300 samples, over four runs, with the same case of `insert` beside it.

| Transaction | Capture | `insert` | `marks` |
|---|---|---|---|
| 1 row | off | 80 µs | 73 µs |
| 1 row | `after` | 86 µs | 128 µs |
| 100 rows | off | 173 µs | 161 µs |
| 100 rows | `after` | 410 µs | 401 µs |

- **A mark is one more frame.** The row lives on the table's single leaf page, so the transaction dirties one more 4 KiB page, which is one more frame in the log, in the batch shipped to followers, and in the fsync (`a_mark_adds_one_frame_to_the_write_it_rides_on`). That is the same fixed cost a captured transaction pays for its change-table page, 10 µs when measured on an idle machine under [Change capture](#change-capture); the noise here, with `marks` faster than `insert` in three of four cases, is larger than that, so treat the table as "no cost visible at this noise" rather than a measurement of it, and re-measure on an idle machine.
- **Only the writes that follow a delivery pay it.** A pump stages a mark after each delivery and the next write records it, so a database with idle streams pays nothing, and one that is written faster than its sinks deliver pays one page per delivery, not per write.

## Hash chain

FR-044's hash chain over the replication log, from `cargo bench -p fleetringd --bench capture -- chain`, measured 2026-10-29 on an Apple M1 Max laptop (macOS, Rust 1.99.1, release profile) while other builds shared the machine, so these are the medians of 100 samples rather than idle-machine numbers.

- **Setup:** one SQLite database as the daemon opens it (4 KiB pages, WAL), one captured batch of 1 or 100 inserted rows (the same rows as the change-capture bench), hashed onto the chain as the primary's stream does before it ships the batch: the engine delimits the batch's transactions from its frame headers, without copying, and BLAKE3 runs over each transaction's bytes with the hash before it folded in.

| Batch | Payload | Chain cost (median) | Raw BLAKE3 over the frame |
|---|---|---|---|
| 1 row, 1 frame | 4.1 KiB | 2.9 µs | 2.5 µs |
| 100 rows, 1 frame | 4.1 KiB | 3.7 µs | 2.5 µs |

- **The hash is the cost.** Delimiting the transactions and the chain's bookkeeping add a few hundred nanoseconds; the rest is BLAKE3 at about 1.6 GB/s single-threaded, so a batch costs roughly 0.6 µs per 4 KiB frame plus 0.5 µs. The transaction's bytes are hashed on their own and the digest chained, rather than fed after the chain's prefix: 80 bytes of prefix pushed every frame off BLAKE3's 1 KiB chunk boundaries and doubled the cost.
- **None of it lands on the commit.** The database's thread captures and publishes a batch as before; the first stream to ship the batch hashes it on its own task, and the other streams reuse the hash. Against the 82 µs one-row commit measured under [Change capture](#change-capture), the hash would cost 3.5% at one row and 2.1% at 100 rows even inline; against a replicated acknowledgement of about 3 ms ([fleetringd replication](#fleetringd-replication)), the few microseconds it adds to shipping are under 0.2%. A follower hashes each batch once, on the blocking thread that applies it, before the apply's fsync.
- **Verification** (`db verify`) reads a replica's log back from its last recorded anchor, at most one checkpoint's worth of batches, and hashes it at the same rate, so a 4 MiB log verifies in a few milliseconds.
- Laptop numbers on a shared machine; re-measure on reference hardware before relying on them.


## Request path

FR-063's trace layer, metrics, and request IDs on every RPC (ADR-0034), measured 2026-11-04 on an Apple M1 Max laptop (macOS, Rust 1.99.1, release profile) while other builds kept the load average near 11 on its 10 cores, so the end-to-end numbers move by more than the effect between runs; the micro-benchmark isolates it.

- **The layer alone,** from `cargo bench -p fleetringd --bench observe`: one unary request through a service that answers at once with a message and `grpc-status` trailers, read to the end, with the daemon's log subscriber installed and writing to nowhere. `bare` calls the service; `traced` calls it through `TraceLayer`. Medians of 200 samples of 200 calls.

| Case | Subscriber at `info` (the default) | at `debug` |
|---|---|---|
| `bare` | 0.48 µs | 0.50 µs |
| `traced`, `code` and `duration_ms` recorded at every level | 2.19 µs | 2.90 µs |
| `traced`, recorded only at `debug` (shipped) | 1.82 µs | 2.92 µs |

- **What a request pays:** about 1.35 µs at `info`: the request ID, the `rpc` span with its three fields, the in-flight gauge up and down, the request counter and duration histogram, two clock reads, and a second boxed body that watches for the trailers. Recording `code` and `duration_ms` into the span cost another 0.37 µs at `info`, where only the `debug` line `rpc finished` reads them, so they are recorded only when `debug` is on (the plan's fallback for this risk). At `debug` each request also formats that line, about 2.4 µs in all.
- **End to end,** from `cargo test -p fleetringd --release --test observe -- --ignored --nocapture rpc_latency`: one daemon running both roles over TLS, 4,000 calls per case on one HTTP/2 connection, split across the callers. `Locate` touches no storage, so it is the request path alone; `Execute` writes one 64-byte row with its fsync, replication factor 1. Medians of three runs on each side; "after" is before the fallback above.

| RPC | Callers | Before: calls/s | After: calls/s | Before: p50 | After: p50 |
|---|---|---|---|---|---|
| `Locate` | 1 | 15,605 | 14,622 | 60 µs | 64 µs |
| `Locate` | 16 | 31,532 | 29,947 | 493 µs | 522 µs |
| `Execute` | 1 | 5,593 | 5,507 | 144 µs | 154 µs |
| `Execute` | 16 | 6,636 | 6,996 | 1–2 ms | 1 ms |

- **Reading it:** the layer's 1.35 µs is about 2% of the cheapest RPC's 60 µs and under 1% of a local write; against a replicated acknowledgement of about 3 ms ([fleetringd replication](#fleetringd-replication)) it is 0.05%. Streams (`Replicate`, `Snapshot`, `WatchMembership`, `WatchLeases`) pay it once per stream, not per batch, and a batch costs two counter increments. The sampler reads the replication state's locks once per `[admin] sample_interval_ms` (10 s), off the write path.
- **The `capture` bench didn't move,** as nothing on its path changed (`cargo bench -p fleetringd --bench capture -- insert`, fastest of 40 samples, before → after): 1 row with capture off 67.9 → 70.1 µs, 100 rows 188.1 → 182.8 µs; in `after` 130.2 → 134.0 µs and 412.7 → 418.8 µs.
- Laptop numbers on a shared machine; re-measure on reference hardware before relying on them.

## Forks

FR-087's measurements of a fork (ADR-0046, point 7), from `cargo bench -p fleetringd --bench fork`, which prints the tables below. They set the defaults of ADR-0046's point 7 and the daemon's `[fork]` table.

- **Setup:** a control daemon and three data daemons in one process, over TLS on loopback, with the shipped defaults (heartbeats every second, `[fork]` as shipped) except a disk-admission margin of 64 MiB and 0%, so that the laptop's disk admits the copies. The source has three replicas, acknowledging at W = 2 (`acks = quorum`); the fork has three, so its completion quorum C and bootstrap quorum W_boot are 2. The source is filled with 1 MiB transactions of random 64 KiB blobs, then four writers write 64-byte rows to it for 10 seconds with no fork, the baseline, and again from the fork request until the fork is ready. Every data directory is on the same disk.
- **Columns:** *to `Seeded`* and *`Seeded` to `Ready`* are the control node's own times (`requested_at`, `completed_at`, `ready_at`). *Pins, held* is the source primary's `fleetring_fork_pin_seconds` count and sum. *WAL held by the pin* is the peak of `fleetring_fork_pinned_wal_bytes`, sampled every 20 ms; *WAL file growth* is how far the source's `data.db-wal` grew during the fork. *Staged per node* is each node's `fleetring_fork_seed_bytes`: the bytes it wrote into its staged copy. *Fleet disk* sums every file under the three data directories, sampled every 20 ms; *after* is 3 s after readiness. *Fork's control entries* are the control-log commands that name the fork, and *all control entries* every command logged from its request to its readiness, nearly all heartbeats.

### Laptop

Measured 2026-11-09 on an Apple M1 Max (8 performance and 2 efficiency cores), 64 GiB of RAM, an Apple AP1024R SSD (1 TB, APFS), macOS 26.5.2, Rust 1.99.1, bench (release) profile. Another release build and a simulation sweep shared the machine, with load averages between 9 and 13 on its 10 cores, so the write latencies are well above [fleetringd replication](#fleetringd-replication)'s and their tails are the machine's, not the fork's.

| Source | Copy | To `Seeded` | `Seeded` to `Ready` | Pins, held | WAL held by the pin, peak; WAL file growth during the fork | Staged per node | Fleet disk: before, peak, after | Per-node peak |
|---|---|---|---|---|---|---|---|---|
| 10 MiB | 11.8 MiB | 1.3 s | 3.0 s | 1, 120 ms | 0.0 MiB, 0.2 MiB | 11.8 MiB on each of the 3 | 35.5 MiB, 74.0 MiB, 66.8 MiB | 26.0 MiB, 25.0 MiB, 26.0 MiB |
| 100 MiB | 102.3 MiB | 4.4 s | 2.0 s | 1, 1.7 s | 0.3 MiB, 1.0 MiB | 102.3 MiB on each of the 3 | 306.9 MiB, 617.1 MiB, 617.1 MiB | 205.7 MiB on each |
| 1 GiB | 1.0 GiB | 36.9 s | 4.0 s | 1, 17.4 s | 2.8 MiB, 2.9 MiB | 1.0 GiB on each of the 3 | 3.0 GiB, 6.1 GiB, 6.1 GiB | 2.0 GiB on each |

| Source | Baseline writes: tx/s, p50, p99 | During the fork: tx/s, p50, p99 | Slowest write: baseline; during the fork, and when it began | Fork's control entries | All control entries during the fork (in the baseline's 10 s) |
|---|---|---|---|---|---|
| 10 MiB | 41, 93.1 ms, 172.5 ms | 29, 97.7 ms, 1306.7 ms | 224.9 ms; 1307.5 ms, 1.4 s after the request | 5: `fork_database`, `complete_fork`, `acquire_lease`, `renew_lease`, `mark_fork_ready` | 34 (58) |
| 100 MiB | 36, 94.4 ms, 1160.7 ms | 38, 102.5 ms, 157.6 ms | 1249.7 ms; 189.4 ms, 5.8 s after | 5: the same | 46 (60) |
| 1 GiB | 35, 90.1 ms, 2023.8 ms | 35, 107.3 ms, 234.0 ms | 2103.7 ms; 968.5 ms, 18.2 s after | 6: the same, with 2 `renew_lease` | 244 (59) |

In every run the ring made the source's primary, `data-2`, the fork's seeder, so the seeder's stream crossed loopback to its own process.

### CI Linux runner

Measured on 2026-11-10 by `.github/workflows/fork-measurements.yml` (run 42766193417, PR #167) on `ubuntu-latest`: an AMD EPYC 9V74 with 4 vCPUs, 15 GiB of RAM, and a 150 GB NVMe disk. The setup is the laptop's. In every run the source's primary, data-2, was also the seeder. Label a pull request `measure-forks` to measure again.

| Source | Copy | To `Seeded` | `Seeded` to `Ready` | Pins, held | WAL held by the pin, peak; WAL file growth during the fork | Staged per node | Fleet disk: before, peak, after | Per-node peak |
|---|---|---|---|---|---|---|---|---|
| 10 MiB | 12.1 MiB | 1.6 s | 3.0 s | 1, 18 ms | 0.0 MiB; 2.7 MiB | 12.1 MiB ×3 | 38.0, 84.2, 75.5 MiB | 28.1 MiB |
| 100 MiB | 103.9 MiB | 3.8 s | 3.0 s | 1, 234 ms | 0.6 MiB; 0.1 MiB | 103.9 MiB ×3 | 317.0, 630.4, 620.3 MiB | 210.1 MiB |
| 1 GiB | 1.0 GiB | 20.0 s | 3.0 s | 1, 1.7 s | 6.7 MiB; 5.2 MiB | 1.0 GiB ×3 | 3.0, 6.1, 6.1 GiB | 2.0 GiB |

| Source | Baseline writes: tx/s, p50, p99 | During the fork: tx/s, p50, p99 | Slowest write: baseline; during the fork, and when it began | Fork's control entries | All control entries during the fork (in the baseline's 10 s) |
|---|---|---|---|---|---|
| 10 MiB | 1935, 1.9 ms, 8.7 ms | 1882, 1.8 ms, 10.1 ms | 120.2 ms; 105.5 ms at 0.7 s | 5 | 35 (64) |
| 100 MiB | 1561, 1.8 ms, 17.3 ms | 882, 1.9 ms, 72.6 ms | 197.4 ms; 740.7 ms at 1.7 s | 5 | 48 (62) |
| 1 GiB | 1388, 1.8 ms, 17.9 ms | 460, 2.0 ms, 84.1 ms | 387.8 ms; 2240.1 ms at 2.7 s | 5 | 146 (69) |

- **The runner was faster than ADR-0046 point 7 assumed.** It assumed 15 MiB/s; the runner reached `Seeded` at 1 GiB in 20 s, about 51 MiB/s end to end. So point 7's defaults have more headroom than its reasoning claimed.
- **On an idle machine, a fork costs the source's writes throughput, not latency.** p50 stayed at about 2 ms. During a 1 GiB fork the source took a third of its baseline transactions, and its p99 rose to 84 ms, because the pinned stream and the copies share the disk with the writers.

### Reading it

- **Streaming the copy is the cost.** The pin lasts as long as the source's primary takes to stream the copy to the seeder: 59 MiB/s at 1 GiB and 60 MiB/s at 100 MiB, with TLS, the stream's checksum, and the seeder's unpacking and fsync on the same disk. The peers then copy the seeder's staged copy at about the same rate, unpinned, so *to `Seeded`* is about twice the pin: 36.9 s at 1 GiB, 28 MiB/s end to end.
- **`Seeded` to `Ready` doesn't grow with the size.** It is the first lease, the fork-reset write at W_boot, and the reset reports, which ride the next heartbeats: 2 to 4 s at one heartbeat a second, whatever the size.
- **The disk holds one copy per holder, never two.** A staged copy is renamed into the database's place when its holder opens or follows the fork, so each node's peak is its source replica and its fork copy, and the fleet's peak is the final state. A copy is a little larger than its fill: SQLite's pages, and the WAL up to the pinned position.
- **The source's WAL barely grows while pinned.** At 35 writes a second, each one 4 KiB frame, a 17 s pin held 2.8 MiB; even the fastest sustained rate measured on this laptop, the fill's 1.1 MiB/s, would hold about 680 MiB over a full 10-minute pin.
- **What the fork does to the source's writes is within this machine's noise.** p50 rose by 5 to 17 ms (5% to 19%) and throughput stayed level at 100 MiB and 1 GiB. Stalls of 1 to 2 s happened with no fork as well, as other builds' fsyncs queued ahead of the writers'; the one at 10 MiB fell when the fork was seeded and its replicas opened it on the same disk. Re-measure on an idle machine before relying on the tails.
- **A fork takes five control-log commands, plus a lease renewal for each lease period it outlasts.** Everything else logged meanwhile is the three nodes' heartbeats, at the baseline's rate.
- Laptop numbers on a shared machine; re-measure on reference hardware before relying on them.
