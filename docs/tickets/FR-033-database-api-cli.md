---
id: FR-033
title: Database API and CLI for fleetringd v0.1
status: todo
priority: P1
size: M
milestone: M6 Replication and failover
depends_on: [FR-026, FR-028]
adrs: [ADR-0015, ADR-0019]
---

# FR-033: Database API and CLI for `fleetringd` v0.1

## Goal

Create databases, run SQL on their primaries, and see where they live, before the router exists (MVP step 4).

## Scope

- **`Database` gRPC service:** `Execute` and `Query` on a database's primary. A request that reaches another node is forwarded or redirected to the current primary.
- **Creation:** `CreateDatabase` places the database with the ring and opens it on its primary and followers.
- **CLI:** `fleetring db create`, `fleetring db exec`, `fleetring db locate` (primary, followers, epoch, and positions), and `fleetring status` (members, liveness, and ring fingerprint).
- Each write reports how many replicas acknowledged it and whether the database is under-replicated.

## Acceptance criteria

- [ ] On a three-node local cluster, `db create`, `db exec`, and `db locate` work end to end, and `db exec` keeps working through a primary failover after a retry.
- [ ] `fleetring status` shows the same ring fingerprint on every node.
- [ ] A request sent to a node that isn't the primary is redirected correctly.
