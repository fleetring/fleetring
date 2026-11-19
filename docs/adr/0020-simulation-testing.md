# ADR-0020: Deterministic simulation testing with turmoil

- **Status:** Accepted (2026-10-28); point 2 amended on 2026-11-19 (FR-122, accepted): what a seed replays exactly, the build it needs, and what still differs between runs
- **Date:** 2026-10-28
- **Related:** ADR-0013, ADR-0016, ADR-0017, ADR-0018, ADR-0019

## Context

The hard bugs in a replicated system are interleavings: a partition during a promotion, a crash between an fsync and an acknowledgement, a heartbeat that arrives after a lease expired. Unit tests don't find them, and real clusters reproduce them rarely. Deterministic simulation runs a whole cluster in one process, on a simulated network and clock, with a seed that replays any failure exactly. `docs/mvp.md` asks for it from day one.

## Decision

1. **turmoil hosts.** Each node, control or data, is a turmoil host running the real `fleetringd` code over simulated TCP and simulated time.
2. **Determinism.**
   - One seed drives turmoil's network and the workload's randomness.
   - SQLite runs inline on the simulation thread, through the engine adapter's simulation mode, instead of on dedicated threads.
   - Daemon code takes time only from `tokio::time` and randomness only from an injected, seeded generator.
3. **Faults.** Partitions and repairs, message delay and loss, host crashes and restarts (a host's data directory survives a crash, its memory doesn't), and control node outages.
4. **Invariants checked after every run:**
   - no acknowledged write is lost;
   - at most one primary acknowledges writes in any epoch;
   - once faults heal, every follower's mirror converges to its primary's;
   - replaying the control log reproduces the control state.
5. **Runs.** A fixed set of seeds runs in every CI build and finishes in minutes. A scheduled job sweeps random seeds. Any failing seed becomes a regression test.

## Limits

- Disk faults, such as lost fsyncs and torn pages, aren't simulated: each host's data directory is a real temporary directory. A fault-injecting storage seam can add them later.
- turmoil simulates TCP, not the internals of HTTP/2, which tonic handles unchanged.

## Alternatives considered

- **madsim.** It gives stronger determinism by replacing Tokio, time, and randomness, but every dependency then needs a madsim-compatible version, which is an ongoing maintenance cost.
- **Hand-rolled network and clock traits.** Full control, but more code to write and keep correct, and the daemon would run on fakes rather than on Tokio.

## Consequences

- Daemon code must not read the wall clock or a thread-local RNG directly. A test or lint enforces it.
- The simulation harness is the first daemon ticket after the skeleton (FR-024), so every later ticket adds its invariants to it.

## Amendment, 2026-11-19 (FR-122): what makes a seed replay

*Accepted on 2026-11-19.* The text above is unchanged.

A seed didn't always replay its run. Seed 2026 of `recreated::late_drop_notice_then_primary_lost`, before FR-121's fix, passed in some runs and failed in others on the same binary. Running one scenario twice on one seed and comparing the runs found three sources:

1. **Tokio's own randomness.** Tokio picks among the ready branches of a `select!`, and the order a `watch` channel wakes its receivers, from a generator each runtime seeds. turmoil seeds it from the simulation's seed only when Tokio is built with `--cfg tokio_unstable`; otherwise each run draws a fresh seed. CI's simulation jobs set the flag, but a plain `cargo test` didn't: with FR-121's fix reverted, six runs of the seed without the flag gave six different traces, diverging within the first second, while six runs with it gave identical traces and all failed the same way. **The workspace's `.cargo/config.toml` now sets `--cfg tokio_unstable` for every build**, `cargo test` included. A `RUSTFLAGS` variable replaces configured flags, so a job or developer that sets it adds the cfg itself; release builds set `RUSTFLAGS`, so release binaries don't carry it.
2. **The real volume's free space.** A data node reports its volume's free bytes in every heartbeat, and a fork's admission compares against them. In the simulation that was the test machine's disk, which other programs change, so two runs logged different heartbeats. A simulated node now believes its volume has 1 TiB free of 2 TiB, unless a test's fault hooks say otherwise. The harness no longer gives every data host a fault-hook volume when built with `fault-hooks`, since a hook volume subtracts what the data directory has grown by, which varies (below); only a test that sets one gets one.
3. **File modification times in the harness.** The fleet re-read a copy for fork markers only when a file's size or modification time changed. Modification times come from the wall clock, and on a file system with coarse timestamps a copy rewritten in place at the same size within one tick keeps its stamp, so when a marker was seen could depend on the machine's speed. The fleet now compares a digest of the files' contents. No run was seen to diverge from this; it was removed because it reads the wall clock.

**What a seed replays.** Built with the cfg, a seed replays the same trace, every event at the same simulated time, the same history, and the same control log, in one process and across processes. `sim::determinism` runs a write-and-failover scenario twice on two fixed seeds and asserts all three; it fails in a build without the cfg rather than compare less. `Replay::supported()` still falls back to comparing only the test's own events, in order, in such a build.

**What still differs, and how tests cope.**

- *SQLite's WAL salts.* SQLite draws each log generation's salts from its own generator, which every connection in the process shares and which is seeded from the operating system; tests run in parallel in one process, so seeding it would not make one test's draws repeat. A SQLite position spells the salts (ADR-0016), so positions, chain hashes and the replication records that hold them differ in those bytes between runs, and so do the records' sizes, since their JSON spells bytes in decimal. Positions still compare by checkpoint sequence number first, so behaviour depends on the salts only when two logs that diverged share a sequence number; no scenario has been seen to. `sim::determinism` compares control logs with the salts masked.
- *A fault-hook volume* subtracts what the data directory has grown by, so in a test that sets one, the reported free space can differ by a few bytes between runs, for the reason above.
