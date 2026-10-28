---
id: FR-026
title: Engine adapter and per-database actor
status: done
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

- [x] Each committed write transaction produces exactly one batch, holding exactly that transaction's frames.
- [x] No checkpoint happens unless the actor asks for one: the WAL keeps growing across thousands of writes until then.
- [x] A slow query on one database doesn't delay writes to another.
- [x] One daemon creates and writes to 10,000 databases with the open-database cap set to 256.

## Notes

- Design the trait so that later adapters fit without changing callers: libSQL or Turso as frame sources, Turso MVCC, Postgres as self-replicating, and logical export, import, and change streams for engine-changing migrations (ADR-0021). A test adapter that is not SQLite should exist from the start, to prove nothing above the trait assumes the WAL.

## Progress

- 2026-10-28: Done.
  - **`fleetringd::engine`:** opaque `Position` (bytes, `Ord` is lexicographic) and `Payload`. `DatabaseEngine` opens or creates; `Database` executes, queries, snapshots, and closes. Callers match on `EngineCapability` and `DatabaseCapability`, each either `FrameSource` (committed batches since a position, checkpoint and its marker, and `FrameSourceEngine::open_follower` for the follower's `apply`) or `SelfReplicating` (replica positions).
  - **`SqliteEngine`:** `rusqlite` 0.37 with bundled SQLite. One write connection and one `query_only` read connection, both kept open; `journal_mode=WAL`, `wal_autocheckpoint=0`, `synchronous=FULL`, the page size fixed at creation, and no checkpoint on close. After each write transaction the actor reads the new frames with `read_committed_file`; positions are `Position::to_bytes` and payloads `Batch::to_bytes`. `SqliteFollower` mirrors batches with `Mirror`.
  - **Checkpoints** run only when asked, as `wal_checkpoint(RESTART)` with `journal_size_limit = 0`, rather than ADR-0016's `TRUNCATE`: the WAL keeps its header, so its checkpoint sequence survives the registry closing and reopening the database, and the next write still trims the file. A truncated WAL would restart the sequence at 0 on reopen and break position order. This needs an amendment to ADR-0016, point 7.
  - **`MemoryEngine`:** an in-memory, self-replicating test engine with no WAL. The engine-neutral actor tests run on both engines.
  - **Actor:** a Tokio task per open database with a bounded mailbox. Engine calls run on a dedicated thread, or inline with `engine.execution = "inline"` for simulation. A failed or rolled-back transaction publishes nothing. Each commit publishes one batch to a `ReplicationSink`, and the writer waits for the returned `Acknowledgement`, so the actor doesn't wait for replication.
  - **`Registry`:** opens on first use, closes after `idle_timeout_secs`, and caps open databases at `max_open` with least-recently-used eviction. An evicted database finishes the requests it accepted before it closes, and never has two actors at once. Databases live under one directory each, with names escaped so that case-insensitive file systems can't collide.
  - **Config:** `[engine]` with `execution`, `max_open` (256), `idle_timeout_secs` (300), `mailbox` (64), and `page_size` (4096).
  - **Tests:** `tests/actor`, 17 tests in about 6 seconds in debug, plus the 10,000-database run behind `#[ignore]`: about 10 seconds, bound by creating and fsyncing 30,000 files. A 600-database run with the cap at 16 runs by default.
  - **Later:** capture rereads the WAL on each commit, which is O(WAL size). FR-030's checkpoint threshold bounds it; an incremental cursor in `fleetring-wal` would remove it.
