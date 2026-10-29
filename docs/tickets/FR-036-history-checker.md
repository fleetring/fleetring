---
id: FR-036
title: History checker for simulation runs
status: todo
priority: P1
size: M
milestone: M7 Durability and operations
depends_on: [FR-024, FR-031]
adrs: [ADR-0018, ADR-0020, ADR-0023]
---

# FR-036: History checker for simulation runs

## Goal

Check every simulation run's full client history against the guarantee each database promises, the way Jepsen checks etcd and Kafka, rather than with hand-written assertions per scenario.

## Scope

- **Recording:** each client operation is recorded as invoke and complete events, with outcome `ok`, `fail` (definitely not applied), or `info` (unknown). That includes reads, which a `Query` operation adds to the workload.
- **Checkers** over the recorded history and the replicas' final state:
  - **Durability:** every `ok` write survives on at least W replicas and in every later read, unless the database is in `acks = "local"` mode.
  - **No phantom writes:** a `fail` write never appears.
  - **Single writer:** at most one primary acknowledges writes per epoch. The existing `one_primary_per_epoch`, applied to the history.
  - **Reads:** a read on the primary sees every write acknowledged before it began (read-your-writes and monotonic reads).
- **Output:** on failure, a minimal excerpt of the history around the violation, plus the seed's rerun command.
- **Integration:** every `sim` scenario and the nightly sweep run the checker, replacing the per-scenario write checks.

## Acceptance criteria

- [ ] Planted bugs are caught with a clear excerpt: dropping an acknowledged write, letting two primaries acknowledge in one epoch, and returning a stale read.
- [ ] Every current `sim` scenario passes the checker on the fixed seeds and on 500 sweep seeds.
- [ ] The checker understands per-database `acks` modes (FR-035).
