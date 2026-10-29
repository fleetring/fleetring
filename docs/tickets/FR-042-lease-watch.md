---
id: FR-042
title: "`WatchLeases`: a stream of the leases this node gains and loses"
status: todo
priority: P2
size: M
milestone: M8 Access and integration
depends_on: [FR-031, FR-037]
adrs: [ADR-0018, ADR-0019, ADR-0022]
---

# FR-042: `WatchLeases`: a stream of the leases this node gains and loses

## Context

A worker that runs next to a data node (a queue consumer, a scheduler, a cache warmer) wants to act only on the databases that node is primary for, and to stop the moment the lease moves. Today it can only poll `status --node` or `db locate`, which is late by a polling interval and costs a control round trip per database. The node already knows: `Replication::served` is a `watch` channel of the databases it serves with their epochs, and `WatchMembership` on the control service shows the shape a server-streamed view takes over gRPC (ADR-0019). Epochs matter to the subscriber as much as ownership does: a worker that holds work from epoch 3 must drop it when it hears epoch 4, and must never act on a lease it heard about under a lower epoch than the one it last saw (ADR-0018).

## Scope

- **`WatchLeases` on the Node service**, server-streamed like `WatchMembership`: the first message carries every database the node currently holds with its epoch, and every later message one `LeaseEvent { database, epoch, kind: Gained | Lost }`. Lost carries the epoch that ended. A stream opened on a data-only node answers for that node; the control node answers `FAILED_PRECONDITION` with the data nodes' addresses, as other data calls do.
- **Fed from `Replication::served`**, so events follow the node's own view of its leases, after the lease is recorded durably and before the node accepts writes under it, and after it steps down and before it closes the database.
- **Additive proto:** new messages and one new RPC; nothing existing changes shape.
- **CLI:** `fleetringd watch-leases --node <addr>` prints events as they arrive, for operators and for tests.
- **Not in scope:** leases without a database behind them (FR-043), and a cluster-wide stream across nodes; a client that wants every node subscribes to each.

## Definition of done

- [ ] Opening the stream returns the current leases, and a gained lease appears on it before the first write under that epoch is acknowledged; a lost lease appears before the node stops answering for the database.
- [ ] Events for one database arrive in epoch order, and a subscriber that reconnects gets the current set again and no stale event.
- [ ] Simulation: under failover (`tests/sim`), the union of `Gained` events across nodes for a database names exactly the lease holders the control state recorded, epoch by epoch, with no two nodes holding `Gained` for the same epoch at once.
- [ ] `fleetringd watch-leases` is documented in the daemon README; the proto change is additive and the compatibility test passes.
