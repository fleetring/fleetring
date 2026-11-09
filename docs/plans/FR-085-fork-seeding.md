# FR-085 (#135): seeding forks, the fork-reset write, and readiness

This plan splits #135, with #154 bundled, into stages that can be built and reviewed apart. It adds nothing to ADR-0046: where the two differ, the ADR wins, and the difference is a bug in this plan.

## What FR-084 (#134) left for this ticket

FR-084 landed the control-state half: `fleetring-control`'s incarnations, fork records, operations, `fork::actions`, and the driver loop in `control/failover.rs`. The daemon-facing half of its scope did not land, and this ticket takes it:

- the proto: `Placement.incarnation` and `ForkStatus`, the heartbeat report fields, `disk_free_bytes`, `size_bytes`, and the RPCs `Control.ForkDatabase` and `Control.GetForkOperation` with `ForkOperationStatus`;
- the `control/service.rs` handlers, and both RPCs in `auth::RPCS`;
- incarnations drawn from the injected RNG in `CreateDatabase` and `CreateDatabaseWithMetadata` (the `TODO(#135)` comments), with a redraw on `IncarnationUnavailable`;
- heartbeat decoding into `fleetring_control::Report` (staged, copy lost, seed failure, reset), and `disk_free_bytes` (the other `TODO(#135)`);
- `fleetring_client::fork_database(...)` and `fork_operation(id)`;
- simulation hooks that feed staged, failure and reset reports, delayed and duplicated, without data nodes (for FR-087).

`software.rs` reports `Feature::Fork` only at the end of stage C, once data nodes seed and gate forks.

## Stages

| Stage | What | Depends on | Owns these files |
|---|---|---|---|
| A | The protocol and control-plane wiring above, plus every proto field the later stages need, plus the `[fork]` config section | — | `fleetring.proto`, `control/service.rs`, `auth/mod.rs` (RPCS), `fleetring-client`, `config.rs` (`[fork]`), `sim/` hooks, `transport.rs` conversions |
| B | The adapter's `fork_reset` and `fork_marker`, and #154's page-size check | — | `engine/adapter.rs`, `engine/sqlite.rs`, `engine/memory.rs`, `engine/changes.rs`, `durability/restore.rs`, `fleetring-wal` (page-size helper), the snapshot install path |
| C | The data path: the `seeding` binding, the source's pin, the seeder and its peers, staging, reservations, reports, deletion on signals, opening, readiness, and the real-daemon tests | A, B | `auth/binding.rs`, `replication/binding.rs`, `replication/fork.rs` (new), `replication/snapshot.rs`, `replication/primary.rs`, `replication/follower.rs`, `replication/mod.rs`, `data/`, `changes/`, `durability/` (archive gating), `software.rs`, `tests/fork.rs` (new) |
| R | Fable's independent review of A + B + C, and the fixes | C | — |

A and B run in parallel. C starts from A and B merged.

### Interfaces between stages

- **Proto (A defines, C uses).** All fields additive and optional:
  - `SnapshotRequest`: `fork`, `fork_incarnation` (fixed64), `operation` (bytes, 16), `fork_attempt`, `source_incarnation` (fixed64), `staged` (bool).
  - `SnapshotChunk`: `total_bytes` on the first chunk; `source_incarnation` on the last.
  - `DropCopyRequest`: `incarnation`, `operation` (ADR-0046 point 4.6).
  - `DatabaseStatus` in `HeartbeatRequest`: `staged`, `copy_lost`, `seed_failure`, `reset`, `seed_progress`, `size_bytes`; `HeartbeatRequest.disk_free_bytes`.
  - `Placement`: `incarnation` (fixed64), `fork` (`ForkStatus`).
  - Incarnations travel as `fixed64`; text prints them as 16 lowercase hex digits and JSON as that string (point 1.4). Operation IDs travel as 16 bytes and print as 26 lowercase base32 characters (point 5.1, `OperationId`'s `Display`).
- **Adapter (B defines, C uses).** On the frame-source engine trait (`engine/adapter.rs`):
  - `fn fork_reset(&mut self, marker: &ForkMarker) -> Result<ForkReset, EngineError>`: one transaction; if `_fleetring_fork` holds a row with this marker's identity, does nothing and answers `ForkReset::AlreadyDone`; otherwise clears the change record (ADR-0024) and the sink positions (ADR-0028), replaces `_fleetring_fork`'s contents with this one row, and answers `ForkReset::Written { position }`. Change capture doesn't record it.
  - `fn fork_marker(&mut self) -> Result<Option<ForkMarker>, EngineError>`.
  - `ForkMarker { fork_incarnation, operation, source_incarnation, at }` is engine-neutral: the incarnations as `u64`, the operation as 16 bytes, `at` as an opaque `Position`. The table's encoding is the adapter's.
  - The memory engine keeps the marker in memory with the same conditional semantics.
- **Page size (B).** `fleetring_wal::header_page_size(&[u8]) -> Option<u32>` (or the adapter's equivalent), and a check before opening a restored or seeded copy, and when accepting a WAL batch or a snapshot, that the WAL header's page size equals the database file's and the configured `[engine] page_size`; a mismatch is refused with an error naming the source (archive, peer, snapshot), and nothing is opened.

## Stage C, in order

1. The `seeding` binding (point 4.1, 4.4): both checks, each comparing the full fork identity; `PERMISSION_DENIED` with `fleetring-binding: seeding`; client certificates refused.
2. The source side (point 4.1, 4.2): the open-copy incarnation check and stamp; `acknowledged` as the W-th highest acknowledged position; `pin_snapshot_at(Some(acknowledged))`; `UNAVAILABLE` with retry-after on `InvalidPosition` and the held checkpoint; `max_pin_ms`, `max_pin_wal_bytes`, `max_pin_ms_per_hour`, `max_pins_per_node`, `max_pins_per_database`; `ABORTED` and release; pin duration and WAL growth measured.
3. The seeder and peers (`replication/fork.rs`, point 4.3 to 4.6, 7.1, 7.2): the staging directory, sidecar, fsyncs and rename; the reservation ledger and disk admission; `transfer_retries`; reports; the start-up scan; deletion only on positive signals; staged orphans in `orphan list` and `orphan remove`.
4. Opening (point 6.2): the verified-copy check before the first lease and after it; `copy_lost`; followers installing from their staged copy; `Database.Create` and `Database.Restore` refusing a placement with a fork that isn't ready.
5. The reset and readiness (point 6.1 to 6.4): `fork_reset` on a primary whose copy lacks the marker; the commit at W_boot; `reset` reports from the primary and followers; the readiness gate on data calls, `WatchChanges`, sink pumps, and archive base snapshots; refusing to serve with `Ready` in view and no marker.
6. Metrics and audit lines (point 11).
7. `Feature::Fork` reported by this build.
8. Fault hooks for the tests (hold the reset; pause a seeder after staging; crash points around the reset), compiled only for tests as the existing fault hooks are.
9. The acceptance tests of #135 in `crates/fleetringd/tests/fork.rs`, against three real daemons.

## Gates for every stage

```sh
cargo fmt --all
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
RUSTDOCFLAGS="-D warnings" cargo doc --no-deps --all-features
cargo +1.88 check --workspace --exclude fleetringd --exclude fleetring-client --exclude fleetring-wal-sqlite-tests --all-features --all-targets
cargo +1.89 check -p fleetringd -p fleetring-client -p fleetring-wal-sqlite-tests --all-features --all-targets
```

Plus the simulation suite and the rolling-upgrade check before the PR.
