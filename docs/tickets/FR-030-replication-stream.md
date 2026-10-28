---
id: FR-030
title: Replication stream and follower mirroring
status: todo
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

- [ ] After any workload, a caught-up follower's database is byte-identical to the primary's once both checkpoint.
- [ ] No write is acknowledged before W replicas have fsynced it (tested with a follower that delays its acknowledgements).
- [ ] Simulation: with partitions and follower restarts during writes, every acknowledged write is on at least W replicas, and every follower converges once the network heals.
- [ ] Throughput and p99 acknowledgement latency for small transactions are measured and recorded in `docs/perf.md`.

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
