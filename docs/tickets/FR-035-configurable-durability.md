---
id: FR-035
title: Configurable durability (acks, min_replicas, per database)
status: done
priority: P1
size: L
milestone: M7 Durability and operations
depends_on: [FR-030, FR-031]
adrs: [ADR-0018, ADR-0022, ADR-0023]
---

# FR-035: Configurable durability (`acks`, `min_replicas`, per database)

## Goal

Let operators choose each database's durability, as `acks` and `min.insync.replicas` do in Kafka, and refuse writes cleanly when too few replicas are live (ADR-0023).

## Scope

- **Settings:** `[replication] acks = "quorum" | "all" | "local"` (default `quorum`) and `min_replicas` (default W), validated against R. `ack_timeout` is unchanged.
- **Control state:** a database records `acks` and `min_replicas` at creation. The fields are additive and replay-safe, so old logs load with the defaults. `CreateDatabase`, `db create --acks … --min-replicas …`, and `Placement` and `db locate` carry them.
- **Quorums:** `Quorums` derives W and Q from R and the mode, with Q = R − W + 1 for `quorum` and `all`. In `local` mode, promotion fences a majority and adopts the most advanced log, and may lose acknowledged writes.
- **Refusal before commit:** a pre-commit check in the actor or sink. A write arriving with fewer than `min_replicas` reachable replicas fails with `UNAVAILABLE` and commits nothing.
- **Changing durability:** an explicit control command that takes effect at the database's next epoch.
- **Readiness:** keep the bounded wait for every live replica when a node starts serving (after create, restart, or promotion), and serve only if the floor is met.

## Acceptance criteria

- [x] With `acks = "all"`, a write is acknowledged only after every replica fsyncs it, and stopping one replica blocks writes.
- [x] With `min_replicas` above the live replica count, writes fail with `UNAVAILABLE`, and the primary's database is unchanged.
- [x] With `acks = "local"`, writes are acknowledged after one fsync, and a simulated failover may lose acknowledged writes. The history checker (FR-036) flags losses only in this mode.
- [x] Two databases in one cluster with different modes keep their own guarantees under the simulation faults.
- [x] Old control logs replay unchanged, with every database read as `quorum` with `min_replicas` = W.
- [x] `db create` and `db locate` show the settings.

## Progress

- 2026-10-29: Done. ADR-0023 accepted.
  - **Control plane** (`fleetring-control`): `Acks` (`quorum`, `all`, `local`) and `Durability` (the mode plus `min_replicas`, validated as W ≤ `min_replicas` ≤ R, defaulting to W). `Quorums::with_acks` derives W and Q per mode: a majority and R − W + 1; R and 1; 1 and a majority. `Database` records its durability and any pending change; records written before the fields load as `quorum` with the floor at W (`DatabaseRecord`). `CreateDatabase` carries an optional `durability`, and `ChangeDurability` records a pending setting that `AcquireLease` and `CompletePromotion` apply, so a primary's guarantee never changes under it; the promotion fences under the old setting's Q. `State::quorums`, `RecordFenced`, and `CompletePromotion` use the database's mode. Rejections: `InvalidDurability`.
  - **Protocol**: `Acks` enum; `acks` and `min_replicas` on `Placement`, `CreateDatabaseRequest`, and the new `ChangeDurability` RPC. All additive.
  - **Refusal before commit** (`sink.rs`, `actor.rs`, `replication/`): `ReplicationSink::admit` runs before the actor hands a transaction to the engine. `ReplicatedSink::admit` refuses with `ReplicationError::BelowFloor` when fewer than `min_replicas` replicas, the primary included, are connected and live, and the actor answers `DatabaseError::Refused`, which the `Database` service maps to `UNAVAILABLE`. The post-commit `UnderReplicated` and the `ack_timeout` failure (`ABORTED`) stay for writes that passed the floor. `PrimaryRole` carries the `Durability`, and the quorum W comes from the mode. In `local`, the primary alone is the quorum and acknowledges after its own fsync.
  - **Readiness** (`data/node.rs`): the bounded wait now waits for the floor as well as every live follower; a lease that arrives without settings (a promotion's) learns them from the control plane before it is served.
  - **Configuration and CLI**: `[replication] acks` and `min_replicas` are the control node's defaults for creations that name none (a default floor outside W ≤ floor ≤ R is clamped); `db create --acks --min-replicas`, `db set-durability`, and the `acks` and `min replicas` lines of `db locate` and the `ACKS` and `MIN` columns of `db list`.
  - **Simulation**: `DurabilityMode` has `All` and `Local`; `Operation::Create` and `Ack::Opened` carry the mode, and `Cluster::check_history` holds each database to the guarantee its creation reported. `CreateAndWrite::with_durability` and `with_durability_for` choose modes.
  - **Tests**: control scenarios (`tests/it/durability.rs`: quorums per mode, the floor's bounds, promotion quorums per mode, a change waiting for the next lease, old logs and snapshots loading with the defaults); real TCP with a held-ack proxy (`tests/replication/durability.rs`: `all` waits for every replica and refuses without one, the floor refuses before anything runs with byte-identical files, `local` acknowledges after the primary's own fsync); the daemon and CLI (`tests/database.rs`: `UNAVAILABLE` below the floor with unchanged files, cluster defaults and clamping, `db create`/`locate`/`list`/`set-durability`); the control service (`tests/control.rs`); simulation (`tests/sim/durability.rs`: a `local` primary's cut-off tail is lost and flagged only under `quorum`, `all` acknowledges nothing while a replica is down, and two databases with different modes keep their own guarantees); the planted-bug checker (`tests/sim/history.rs`).
  - **Follow-ups**: a dynamic in-sync set for `all` (ADR-0023's alternatives); `Placement` doesn't yet carry a pending change, so `db locate` shows only the settings in effect; `min_replicas` is validated against R, not the current replica-set size, so a database whose set is smaller than its floor refuses writes until members return.
