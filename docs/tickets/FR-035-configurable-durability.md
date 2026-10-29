---
id: FR-035
title: Configurable durability (acks, min_replicas, per database)
status: todo
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

- [ ] With `acks = "all"`, a write is acknowledged only after every replica fsyncs it, and stopping one replica blocks writes.
- [ ] With `min_replicas` above the live replica count, writes fail with `UNAVAILABLE`, and the primary's database is unchanged.
- [ ] With `acks = "local"`, writes are acknowledged after one fsync, and a simulated failover may lose acknowledged writes. The history checker (FR-036) flags losses only in this mode.
- [ ] Two databases in one cluster with different modes keep their own guarantees under the simulation faults.
- [ ] Old control logs replay unchanged, with every database read as `quorum` with `min_replicas` = W.
- [ ] `db create` and `db locate` show the settings.
