---
id: FR-042
title: "`WatchLeases`: a stream of the leases this node gains and loses"
status: done
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

- [x] Opening the stream returns the current leases, and a gained lease appears on it before the first write under that epoch is acknowledged; a lost lease appears before the node stops answering for the database.
- [x] Events for one database arrive in epoch order, and a subscriber that reconnects gets the current set again and no stale event.
- [x] Simulation: under failover (`tests/sim`), the union of `Gained` events across nodes for a database names exactly the lease holders the control state recorded, epoch by epoch, with no two nodes holding `Gained` for the same epoch at once.
- [x] `fleetringd watch-leases` is documented in the daemon README; the proto change is additive and the compatibility test passes.

## Progress

- 2026-10-29: Done.
  - **Proto (additive):** `Database.WatchLeases(WatchLeasesRequest) returns (stream LeaseUpdate)`. A `LeaseUpdate` is a `oneof`: `LeaseSnapshot { node_id, repeated HeldLease { database, epoch } }` first, then `LeaseEvent { database, epoch, kind: GAINED | LOST }`. No existing message changed; `tests/transport.rs` still passes.
  - **Ordering, made structural in `Replication` (`replication/mod.rs`):** `served` (the watch the change streams already follow) now changes under the `primaries` lock and ahead of `primaries`: `serve_primary` inserts the database into `served` before the primary that can take a batch, and every step down (`stop_primary`, `stop`, `demote`, `step_down` on a fence, `discard`) goes through `Inner::withdraw_primary`, which removes it from `served` before the primary. `demote`, `step_down`, and `discard` never updated `served` before, so a demoted node left its change-stream pumps running and would have reported no `Lost`. `DataNode::forget` and `forget_any` call `Replication::withdraw` before they drop the lease from the table that routes requests, so `Lost` precedes the first redirect. `tests/replication/leases.rs` churns a primary through 4,000 serve-and-stop rounds while another worker reads the primary and the watch around each other; under the old order it fails every run, under the new one a primary is never seen without its lease nor a staying lease without its primary.
  - **Stream (`data/leases.rs`):** a task per stream reads a `LeaseWatch` (a `watch` receiver over `served`), sends the snapshot, then the events `lease_events` derives by comparing successive sets (a database whose epoch changed yields `Lost` then `Gained`). Updates wait in a bounded channel (256); when it is full the task sends `RESOURCE_EXHAUSTED` and ends, so the subscriber reconnects for a fresh snapshot and nothing buffers without bound. Changes between two reads of the watch fold together: a late reader sees the net change, never an event out of epoch order. The stream ends when the subscriber leaves or replication stops.
  - **Client and CLI:** `Client::watch_leases(address) -> LeaseUpdates`, whose `next()` yields `LeaseUpdate::Snapshot { node, leases }` or `LeaseUpdate::Event(LeaseEvent)`; `fleetringd watch-leases --node <host:port>` prints `held <db> epoch N` for the snapshot, then `gained <db> epoch N` and `lost <db> epoch N` lines as they arrive. A node that hosts no data answers `UNIMPLEMENTED`, as it does every database call (the control role serves no `Database` service, so there is no `FAILED_PRECONDITION` with addresses to copy).
  - **Simulation:** a lease watcher client host per data host (`<node>-leases`) follows the node's stream for the whole run, reconnects when it ends, and every second replaces a silent stream with a fresh one, swapping only once the replacement's snapshot arrived, so a crashed host's stalled stream never hides an event. Snapshot entries and events land in the history as `LeaseObservation`s and in the trace. `Invariant::LeaseEventsMatchControl` (`check::lease_events_match_control`, run by `Cluster::finish`) replays the control log into a holder per `(database, epoch)` and checks that a node reports holding a database only at an epoch control leased to it, that no two nodes report one epoch, that each stream's events for a database are in epoch order, and that every epoch a node acknowledged writes under was reported held or gained by that node. `tests/sim/failover.rs` also asserts the watchers saw at least three gains across the failovers.
  - **Tests:** `data::leases` unit tests (snapshot then ordered events, `RESOURCE_EXHAUSTED` when behind and a fresh snapshot after, the stream ends with replication), `data::convert` round trips, `tests/database.rs::watch_leases_streams_the_leases_each_node_gains_and_loses` (three daemons: empty snapshots, `Gained` at epoch 1 on the node that created the database, `Lost` 1 and `Gained` 2 across a drain, a write acknowledged at epoch 2 by the node that reported it, fresh streams replay nothing, the control node answers `UNIMPLEMENTED`), `tests/replication/leases.rs`, `sim::check` unit tests, and the whole sim suite under the new invariant.
  - **Not done:** a cluster-wide stream and leases without a database (FR-043), as scoped.
