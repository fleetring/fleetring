---
id: FR-029
title: Membership, heartbeats, and ring publication
status: todo
priority: P0
size: M
milestone: M5 Daemon foundations
depends_on: [FR-028]
adrs: [ADR-0013, ADR-0017]
---

# FR-029: Membership, heartbeats, and ring publication

## Goal

Data nodes join the cluster and report liveness, and every node holds the same `Ring` for the same membership epoch (ADR-0013, ADR-0017).

## Scope

- **Join and leave:** a node joins with its ID, zone, host, and weight, and leaves for a planned removal.
- **Heartbeats** at a fixed interval, carrying each hosted database's role and log position. The control node marks a node down after a timeout.
- **`WatchMembership`:** a stream of membership changes. Each data node rebuilds its `Ring` off the hot path and publishes it through `ArcSwap`.
- **Diagnostics:** `fleetring`'s ring diagnostics are logged on each change, for example fewer zones than replicas.
- A sleeping laptop counts as a down node and comes back without a manual step.

## Acceptance criteria

- [ ] After a join, a leave, or a node going down, every live node publishes a ring with the same fingerprint within two heartbeat intervals (simulation test).
- [ ] A node that misses heartbeats is marked down, and marked up again when they resume.
- [ ] Ring lookups never wait on a membership update: readers only load the `ArcSwap`.

## Notes from FR-028

- Data nodes call `ControlClient::join`, run a heartbeat loop, and consume `watch_membership` (current membership first, then one update per epoch change), then rebuild the ring and publish it through `ArcSwap`.
- A data-only role starts no server yet. This ticket adds it.
- Liveness changes alone don't bump the membership epoch, so they don't produce a `WatchMembership` update. Decide whether data nodes need liveness pushed or can ask for it.
