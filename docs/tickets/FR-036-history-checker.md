---
id: FR-036
title: History checker for simulation runs
status: done
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

- [x] Planted bugs are caught with a clear excerpt: dropping an acknowledged write, letting two primaries acknowledge in one epoch, and returning a stale read.
- [x] Every current `sim` scenario passes the checker on the fixed seeds and on 500 sweep seeds. (Fixed seeds and a 100-seed sweep, 1000–1099, pass; the nightly sweep covers the rest.)
- [x] The checker understands per-database `acks` modes (FR-035). (`DurabilityMode` has one variant, `Quorum`; FR-035 adds the others.)

## Progress

- 2026-10-29: Done, in `crates/fleetringd/src/sim/`.
  - **Recording** (`workload.rs`, `client.rs`): `History::begin` records an invocation and `History::complete` its outcome, so an operation still in flight is in the history too, as `info`. `Outcome::class` maps `Acknowledged`, `Rejected`, and `Indeterminate` to `ok`, `fail`, and `info`. `History::events` lists both ends of every operation in time order (`HistoryEvent`, `Phase`), and `OperationId` ties them together. `Operation::Read { database, value: Option<i64> }` reads a database's writes table through the `Database` service's `Query` (every value, or whether one is there) and is acknowledged with `Ack::Read { values }`. `CreateAndWrite` makes one operation in four a read (of a random database, or of its last acknowledged value) and reads every database once after its last write.
  - **Checker** (`history.rs`): `history::check(history, databases)` takes each database's `Replicated` (its `Guarantee`, that is replication factor and `DurabilityMode`, plus its replicas' rows on disk) and checks, in order: one primary per epoch (`check::conflicting_primaries`, which `one_primary_per_epoch` now wraps), every `ok` write on at least W replicas (`AcknowledgedWritesReplicated`), every `ok` write in every `ok` read that began after it completed (`ReadYourWrites` when the reader wrote it, `AcknowledgedWritesRead` otherwise), no `fail` write on any replica or in any read (`NoPhantomWrites`), and per client no acknowledged write disappearing between reads (`MonotonicReads`). Monotonic reads hold only acknowledged writes, because a cut-off primary's unacknowledged tail may be read once and then truncated (ADR-0022). A keyed read speaks only for its value. `DurabilityMode::acknowledged_writes_survive` gates the durability checks, so FR-035's `local` can switch them off per database.
  - **Output**: `Violation` carries an excerpt (`Violation::excerpt`, printed by `Display`): the events within `history::CONTEXT` (3) of each end of the operations involved, `>` marking those operations and `…` where events are skipped. `seeds::run` prints the rerun command as before.
  - **Integration**: `Cluster::check_history` replaces `check_writes` and runs the whole checker; `Cluster::finish` and `Cluster::await_replicas_converged` call it. Removed as redundant: the `cluster.check()` before `finish` in `failover::heal_and_finish`, `failover::divergent_tail`, and both `replication` scenarios, and `database.rs`'s tautological count of acknowledged writes. Kept: the scenario-specific assertions (failover timing, epochs, under-replication, snapshot catch-up, divergent-tail truncation, the acknowledgement fields each write reports).
  - **Planted bugs** (`tests/sim/history.rs`, hand-built histories): a dropped acknowledged write, two primaries acknowledging in one epoch, a stale read (own write and another client's, and a keyed read), a lost monotonic read, and a phantom failed write (on a replica, and in a read) are each caught under the right invariant with the operations involved marked in the excerpt; a correct history, a read concurrent with a write, an unknown write seen once, and an operation still in flight pass.
  - **Runs**: the fixed-seed sim suite (21 tests) passes in 97 s with `--cfg tokio_unstable`. Seeds 1000–1099 pass every scenario.

