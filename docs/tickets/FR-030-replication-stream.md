---
id: FR-030
title: Replication stream and follower mirroring
status: done
priority: P0
size: L
milestone: M6 Replication and failover
depends_on: [FR-023, FR-026, FR-029]
adrs: [ADR-0016, ADR-0018]
---

# FR-030: Replication stream and follower mirroring

## Goal

Every committed transaction reaches the database's followers as a frame batch, and clients are acknowledged only once the write quorum holds it (ADR-0016, ADR-0018).

## Scope

- **Primary:** one `Replicate` stream per follower, sending batches from the follower's last acknowledged position, with flow control. A write's reply waits for W − 1 follower acknowledgements.
- **Follower:** validates each batch (epoch, position continuity, checksums), appends it to its mirrored WAL with `fleetring-wal`, fsyncs, and acknowledges its new position.
- **Checkpoints:** when the WAL passes a size threshold and every follower has acknowledged all of it, the primary checkpoints and ships a marker, and followers checkpoint their mirrors at it. A follower that lags past a limit stops blocking checkpoints and is sent to snapshot catch-up (FR-032).
- **Under-replication:** a database with fewer than W − 1 reachable followers rejects writes with a clear error rather than acknowledging them unsafely.
- **Batching and pipelining (ADR-0016, point 11):** commits that arrive while a batch is in flight join the next batch and are confirmed together, and several batches may be in flight per follower.
- **Engine-neutral:** this ticket handles opaque positions and payloads only; SQLite specifics stay in the adapter (FR-025, FR-026).
- **Positions** go into heartbeats (FR-029), for promotion decisions (FR-031).

## Acceptance criteria

- [x] After any workload, a caught-up follower's database is byte-identical to the primary's once both checkpoint.
- [x] No write is acknowledged before W replicas have fsynced it (tested with a follower that delays its acknowledgements).
- [x] Simulation: with partitions and follower restarts during writes, every acknowledged write is on at least W replicas, and every follower converges once the network heals.
- [x] Throughput and p99 acknowledgement latency for small transactions are measured and recorded in `docs/perf.md`.

## Notes from FR-026

- **Plug in:** implement `ReplicationSink` and pass it to `Registry::new`. Its `Acknowledgement` completes once W − 1 followers have acknowledged. The actor publishes right after each commit without blocking; the writer waits on the acknowledgement, so batches can pipeline.
- **Primary side:**
  - `DatabaseHandle::committed_since(Some(follower_ack))` catches a follower up;
  - `checkpoint()` returns the marker to ship (RESTART, not TRUNCATE; see ADR-0016 point 7);
  - `position()` goes into heartbeats;
  - `snapshot(dir)` gives a new follower its initial copy. Followers must start from a snapshot, never from an empty file.
- **Follower side:** take the `EngineCapability::FrameSource` capability, call `open_follower(dir)`, then `apply(&batch)`, which fsyncs before returning, and acknowledge the returned position. Call `checkpoint(&marker)` on markers. These calls block, so run them on a blocking thread.
- **`Daemon` doesn't own a `Registry` yet.** Wire it in here or in FR-033.
- **Known cost:** capture rereads the whole WAL on every commit. The checkpoint threshold keeps that bounded; an incremental reader in `fleetring-wal` would remove it.
- **Two `Position` types:** `fleetringd::engine::Position` and `fleetring_control::Position`, with conversions between them. Consider one shared type in a small common crate.

## Notes from FR-029

- Heartbeats send an empty database list today. Let `Membership::start` take a source of `DatabaseStatus` entries (role and position per hosted database). The control plane answers a heartbeat naming an unknown database with `NOT_FOUND`, which the membership loop currently treats as "rejoin", so tell the two apart.
- Add the `Replication` and `Database` services to the daemon's router in `daemon.rs::serve` with `add_optional_service`. Use `Membership::view()` for placement and peer liveness.

## Notes from FR-024

- **Simulation:** add a `Write` operation and its acknowledgement in `sim/workload.rs`, its executor in `sim/client.rs::execute`, and the "no acknowledged write lost" check at the marked extension point in `sim/check.rs`.
- **`transport::Peers`** still builds its own TCP endpoints. Give it a constructor that takes a `Network` and uses `Network::channel`, so replication runs under simulation.
- **The data-role registry** must follow `config.engine.execution`, which the simulated `Environment` forces to `inline`.

## Progress

- 2026-10-28: Done, with the daemon wiring left to FR-033.
  - **Entry point:** `replication::Replication::start(parts, |sink| Registry::new(engine, sink, root, config))` builds the `ReplicatedSink` the registry publishes to, and returns a cheap, cloneable `Replication`.
    - `ReplicationParts` holds the node ID, the engine, `Membership`, a `Peers` pool (`Peers::with_network`), a `PeerAddresses` source (node ID to `host:port`), a `Clock`, the `Execution`, a state directory, and `ReplicationSettings`.
    - `server()` returns the `Replication` gRPC server for the router, and `service()` the bare service for callers that wrap it.
    - `serve_primary(name, PrimaryRole { replication_factor, epoch })` makes the node a database's primary; `stop_primary`, `stop`, `primary_status`, `follower_status`, and `checkpoint` round it out.
    - `Replication` implements `DatabaseStatuses`, which `Membership::start_reporting` sends in every heartbeat.
  - **Protocol:** one `Replicate` stream per follower in the ring's `replicas_n(name, rf)`. The stream opens with the database and epoch, and its `fleetring-primary` metadata names the primary. The follower records the epoch durably (refusing a lower one), fetches its first copy if it has none, then acknowledges its position. Each batch's `start` is exactly the position it continues from, so continuity is a byte comparison.
  - **Write quorum:** an acknowledgement completes once W − 1 followers acknowledge a position at or past the batch's end, with W from `Quorums` over the replica set. If fewer followers are connected and live, it fails at once with `ReplicationError::UnderReplicated`; after `ack_timeout` it fails too. The write stays committed on the primary and replicates later.
  - **Pipelining:** up to `max_in_flight` batches per follower. A published batch is sent as captured when it is the only one pending; several pending batches are read back as one with `committed_since`.
  - **Checkpoints:** due once the batches published since the last pass `checkpoint_threshold` bytes and every follower either holds everything or can get the rest from the retained batches. The primary runs RESTART and sends the marker to each follower holding the log up to it. A follower that makes no progress for `lag_limit` stops blocking; once the log restarts past it, it is flagged `needs_snapshot`.
  - **Initial copy:** a follower without a copy calls `Snapshot` on the primary. The primary streams `DatabaseHandle::snapshot` as a plain archive of the snapshot directory's files; the follower unpacks it into a staging directory under the registry root, syncs it, and renames it into place.
  - **Heartbeats:** `Membership::start_reporting` takes a `DatabaseStatuses` source. `NOT_FOUND` for an unknown database now carries `fleetring-missing-database` metadata (`control::missing_database`); the heartbeat drops that database and is resent, and only an unknown node rejoins. *(Superseded 2026-10-29: the control plane now accepts such a heartbeat and returns the unknown names; see FR-029.)*
  - **Simulation:** `Operation::Write` and the `Writes` workload, `check::acknowledged_writes_replicated` and `check::replicas_converged`, `Cluster::check_writes`, `Cluster::await_replicas_converged`, and the write check in `Cluster::finish`. Data hosts run a registry and replication beside the daemon on port 7401 (`sim::HostPorts`), and the writer sends writes to the primary's registry in process.
  - **Tests:** `tests/replication` (byte-identical followers across automatic checkpoints, quorum timing with held-back acknowledgements, under-replication, epochs and continuity, self-replicating engines, heartbeat reports) and `tests/sim/replication.rs`. Measurements are in `docs/perf.md`.
- 2026-10-28: Integrated into the daemon, on top of FR-033.
  - **Daemon:** the data role builds `Replication::start` around its `Registry` (state under `<data_dir>/replication`, the environment's clock, network, and execution), serves `replication.server()` next to `Control` and `Database`, and starts membership with `start_reporting`, so heartbeats carry every database's role and position. `Membership` implements `PeerAddresses` from the addresses members advertise. `UnreplicatedSink` and `daemon::replication_sink()` are gone. A new `[replication]` table (`ReplicationConfig`) sets `ReplicationSettings`; `Daemon::replication()` exposes it.
  - **Primaries:** `DataNode` calls `serve_primary` at the lease's epoch and replication factor when it takes a database's first lease, and when it relearns its lease after a restart. It then waits up to `ack_timeout` (`Replication::await_writable`) for enough followers to connect, so the first write after a create or restart doesn't fail as under-replicated. `Placement` gains `replication_factor` (proto field 7).
  - **Acknowledgement counts:** an `Acknowledgement` completes with `Acknowledged { acknowledged_by, reachable, replicas }`: the replicas holding the batch when its quorum was reached (W or more), those reachable when it was published, and R. `DatabaseHandle::execute` and `Registry::execute` return `Stored { rows_affected, acknowledged }`. `ExecuteResponse.acknowledged_by` and `replicas` come from it, and `under_replicated` is `reachable < R`. A write without a batch reports `Replication::holders`. An under-replicated or timed-out write stays `ABORTED`: it committed on the primary and may replicate later.
  - **Simulation:** one write path, FR-033's `Operation::Write { database, value }` and `Ack::Written { acknowledged_by, replicas, under_replicated }`, through the `Database` service. `sim::replica`, `HostPorts`, port 7401, `ClusterBuilder::replicated`, and the `Writes` workload are gone; `ClusterBuilder::replication` sets each host's `[replication]` table. The write checks find databases and their W from acknowledged `Operation::Create`s and read the `writes` table from each host's copy. `tests/sim/replication.rs` creates its database through the data plane with `CreateAndWrite` and aims its faults at whichever node became primary.
  - **Tests:** `tests/database.rs` adds `writes_replicate_to_every_follower_through_the_daemons` (R = 3 over TCP: every write acknowledged by 2 or 3, followers byte-identical to the primary after a checkpoint, rows read from each follower's file, and heartbeat roles in `locate`), and its other tests now expect real counts.
- 2026-10-28: `DataNode` now waits for all live replicas. A node that starts serving a database, after a create, after a restart, or once a promotion completes, waits up to `ack_timeout` (`Replication::await_replicas`) until every follower the membership view counts live, and at least a write quorum, is connected, so the first write isn't reported under-replicated while every replica is up. It shares `await_followers` with `await_writable`; on timeout it logs at info if a quorum is connected and warns otherwise. `DataNode::start_serving` records the lease before the wait, so renewals go on throughout, and `route` holds requests for a database that is still settling until the wait ends. After a restart the lease keeper starts each database in a task of its own, so renewals and promotions never wait on it.

## Notes for FR-031

- `Fence` returns `UNIMPLEMENTED`. The durable highest epoch per database lives in `<state_dir>/epochs`; fencing can reuse `Epochs::admit` and report `follower_status().position`.
- `serve_primary` takes the epoch from the caller; nothing checks it against a lease yet. A follower refuses a stream at a lower epoch but accepts a stream at the same epoch from any node.
- A follower whose log runs past the primary's (a divergent tail) makes `committed_since` fail with `InvalidPosition`; the stream logs it and retries forever. Truncation belongs here.
- A follower copy lives at the registry location the primary would use, so promotion opens it in place. Nothing stops the registry from opening a database this node follows; the service only refuses streams for databases open here.

- From the daemon integration: a restarted primary serves its database again only when the first request for it arrives (`DataNode::route` relearns the lease), so its followers get no stream until then. Lease renewal should call `serve_primary` as soon as the node knows it holds a lease, and `stop_primary` when it loses one.
- `DataNode` keeps serving at the epoch it learned; a newer epoch for the same database has to replace the role (`serve_primary` again) or stop it.

## Notes for FR-032

- The initial copy is a whole-directory archive in memory, without a checksum, taken through the actor (blocking writes while it runs). Replace `replication/archive.rs` and the `snapshot` handler with the chunked, checksummed, online-backup transfer; the follower already stages and renames.
- A follower flagged `needs_snapshot` is never re-seeded: the stream pauses for `lag_limit` and tries again. Re-seed it by moving its copy aside and letting the stream bootstrap again, or by a dedicated message.
- The latest checkpoint marker is kept in memory only. After a primary restart, a follower behind an earlier checkpoint isn't detected, and `committed_since` would start it on the new generation. Persist the marker, or have the engine refuse positions from older generations it can't vouch for.

## Notes for FR-033

Done in the daemon integration; see Progress.

- Wire `Replication::start` into `Daemon` in place of a bare `Registry`, add `replication.server()` to the router with `add_optional_service`, and start membership with `Membership::start_reporting(member, client, settings, replication.clone())`.
- Replace `sim::replica` (the data role beside the daemon, on port 7401) with the daemon's own, and implement `PeerAddresses` over the advertised addresses.
- `ExecuteResponse.under_replicated` maps to `DatabaseError::Replication(ReplicationError::UnderReplicated { .. })`.

