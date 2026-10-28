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
