---
id: FR-040
title: Turso adapter: a self-replicating engine whose change stream reads `turso_cdc`
status: todo
priority: P2
size: L
milestone: M8 Access and integration
depends_on: [FR-039]
adrs: [ADR-0016, ADR-0021, ADR-0024]
---

# FR-040: Turso adapter: a self-replicating engine whose change stream reads `turso_cdc`

## Goal

Run databases on Turso (the Rust rewrite of SQLite, with its own MVCC log and sync) behind the same `DatabaseEngine` trait as SQLite, with placement, leases, fencing, and change streams unchanged above the adapter.

## Scope

- **Shape: self-replicating** (ADR-0016, point 1). Turso replicates itself through its sync protocol; the adapter implements `SelfReplicating` (`position`, `replica_positions`) and ships no frames. Positions are Turso's log positions, encoded fixed-width big-endian so that they compare bytewise in log order.
- **Logical changes through `LogicalChanges`** (ADR-0024). `start_recording(mode)` maps the engine-neutral `CaptureMode` onto `PRAGMA capture_data_changes_conn('<mode>')`: `id` → `id`, `after` → `after`, `full` → `full`. `changes_between` reads `turso_cdc` by `change_txn_id` ranges that the adapter relates to its positions, and decodes Turso's records into `RowChange`s; `release` prunes delivered rows. The decoder's output is the same as the SQLite adapter's, so sinks don't know the engine.
- **No frames, no WAL module.** Nothing in `fleetring-wal` is used. `EngineKind::Turso` is recorded on every database it holds (ADR-0021).
- **`db exec` and `db query`** work as on SQLite; `turso_cdc` is reserved the way `_fleetring_cdc` is.
- **Not in scope:** migrating a database between SQLite and Turso (ADR-0021's logical copy), and Turso's own S3 durability as a sink.

## Acceptance criteria

- [ ] A Turso database is created, written, queried, and dropped through the data plane, and its control state records `engine = turso`.
- [ ] A logical file sink receives the same `RowChange`s for the same statements as it does from SQLite, in each capture mode.
- [ ] Leases and fencing work with Turso's positions: a fenced primary refuses writes, and promotion ranks replicas by their reported positions.
- [ ] The adapter's tests run only when the `turso` feature is on, so the default build carries no Turso dependency.

## Progress

- Not started.
