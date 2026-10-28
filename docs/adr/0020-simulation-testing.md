# ADR-0020: Deterministic simulation testing with turmoil

- **Status:** Proposed (2026-10-28)
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
