---
id: FR-026
title: Engine adapter and per-database actor
status: todo
priority: P0
size: L
milestone: M5 Daemon foundations
depends_on: [FR-022, FR-025]
adrs: [ADR-0013, ADR-0015, ADR-0016]
---

# FR-026: Engine adapter and per-database actor

## Goal

Give each open database one owner that serializes writes, keeps SQLite off the async executor, and captures each committed transaction as a frame batch (ADR-0013, ADR-0016).

## Scope

- **`DatabaseEngine` trait (ADR-0016):** capability-based. Every engine opens, creates, executes, queries, snapshots, and closes. A frame-source engine also yields committed batches since a position and applies batches; a self-replicating engine reports replica positions instead. Positions are opaque bytes with an adapter-defined total order, and payloads are opaque bytes. v0.1 implements the frame-source shape for plain SQLite with `rusqlite` and bundled SQLite.
- **Connection settings:** `journal_mode=WAL`, `wal_autocheckpoint=0`, and `synchronous=FULL`, with the page size fixed at creation.
- **Actor:** a Tokio task with a bounded mailbox for each open database. SQLite runs on a dedicated thread per open database, or inline under simulation. Writes are serialized, and reads use a separate read connection.
- **Commit capture:** after each write transaction, the actor reads the new frames with `fleetring-wal` and passes the batch to replication (FR-030). Until replication exists, batches go to a local sink for tests.
- **Lifecycle:** open on first use, close when idle, and cap the number of open databases with least-recently-used eviction.

## Acceptance criteria

- [ ] Each committed write transaction produces exactly one batch, holding exactly that transaction's frames.
- [ ] No checkpoint happens unless the actor asks for one: the WAL keeps growing across thousands of writes until then.
- [ ] A slow query on one database doesn't delay writes to another.
- [ ] One daemon creates and writes to 10,000 databases with the open-database cap set to 256.

## Notes

- Design the trait so that later adapters fit without changing callers: libSQL or Turso as frame sources, Turso MVCC, Postgres as self-replicating, and logical export, import, and change streams for engine-changing migrations (ADR-0021). A test adapter that is not SQLite should exist from the start, to prove nothing above the trait assumes the WAL.
