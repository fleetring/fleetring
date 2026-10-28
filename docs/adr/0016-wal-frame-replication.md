# ADR-0016: Replication through engine adapters, starting with SQLite WAL shipping

- **Status:** Accepted (2026-10-28)
- **Date:** 2026-10-28
- **Related:** ADR-0006, ADR-0010, ADR-0013, ADR-0015, ADR-0018, ADR-0021

## Context

Each database has one primary and R − 1 followers, chosen by the ring (ADR-0006). Writes go to the primary, and followers must hold every acknowledged write so that a failover loses nothing.

Engines replicate in different ways. In WAL mode, SQLite writes each transaction as checksummed page frames ending in a commit frame, a ready-made replication stream as long as the primary controls checkpoints. libSQL and Turso can hand over commits from inside the engine, and Turso's MVCC engine has its own log and can keep redundancy in S3. Postgres replicates itself through streaming replication. The leases, quorums, and failover in ADR-0018 should work the same over all of them, so nothing above the engine adapter may depend on one engine's log format.

## Decision

### Engine adapters

1. **Capability-based adapters.** Each engine implements the `DatabaseEngine` trait (ADR-0015) in one of two shapes:
   - **Frame source:** the engine hands `fleetringd` each committed transaction as a batch, and `fleetringd` ships it to followers and has their engines apply it. Plain SQLite (by reading the WAL) and later libSQL or Turso (by capturing commits inside the engine) take this shape.
   - **Self-replicating:** the engine replicates itself, as Postgres streaming replication or Turso's own sync does. `fleetringd` still places the database, holds its lease, and fences, and asks the engine for each replica's position, but it never touches the payload.
2. **Positions and payloads are engine-defined.** Everything above the adapter, meaning ADR-0018's quorums and fencing, the control state, the protocol, and migration, sees only:
   - a **log position**: opaque bytes from the engine, encoded so that comparing the bytes lexicographically gives log order, as a fixed-width big-endian Postgres LSN does. Control state and failover compare positions without knowing the engine;
   - a **batch payload**: opaque bytes that the engine produces and applies.

   SQLite's WAL generations, offsets, and salts stay inside the SQLite adapter.
3. **Durability sinks are separate from replicas.** Replicas give availability and failover; a durability sink, such as S3 snapshots and log archiving for point-in-time restore, gives backups. Sinks get their own trait later (MVP step 5), so "replicas plus S3", "S3 only", or an engine's own S3 mode can plug in without changing replication.

### The SQLite adapter (v0.1)

4. **The primary's WAL is the replication log.** The database's actor owns the only write connection and sets `journal_mode=WAL`, `wal_autocheckpoint=0`, and `synchronous=FULL`. After each write transaction commits, it reads the new frames from the `-wal` file, up to and including the last commit frame, and ships them as one batch.
5. **SQLite positions** are `(generation, offset)` inside the opaque position. The generation changes whenever a checkpoint restarts the WAL, which changes the header's salts. Frames are validated with SQLite's cumulative checksums before they are shipped and again before they are applied.
6. **Followers mirror the WAL byte for byte.** A follower appends each batch to its own `-wal` file and fsyncs it before acknowledging. A new generation starts with the primary's WAL header. A follower doesn't open the database; on promotion it removes any stale `-shm` and opens it normally, and SQLite's WAL recovery replays exactly the frames up to the last valid commit frame.
7. **The primary checkpoints, and followers follow.** When the WAL passes a size threshold and every follower has acknowledged all of it, the primary runs `wal_checkpoint(RESTART)`, with `journal_size_limit = 0` so the next commit trims the file, and ships a checkpoint marker, and each follower checkpoints its mirror at the marker. (*Corrected 2026-10-28, FR-026:* not TRUNCATE. A WAL truncated to zero bytes and then reopened restarts SQLite's checkpoint sequence at 0, so new positions would sort before old ones.) A follower that lags past a limit stops blocking checkpoints and catches up from a snapshot instead.
8. **Snapshot catch-up.** A new follower, or one too far behind for the current generation, receives a consistent copy of the database files, pinned so no checkpoint can change them while they are read, then the log from the copy's position. (*Amended 2026-10-28, FR-032:* not SQLite's online backup API, which rewrites the copy's header, so the copy wouldn't be byte-identical, and leaves no WAL prefix for the follower's mirror to continue from.)
9. **No follower reads in v0.1.** Followers are warm standbys.

### For every frame-source engine

10. **Acknowledged means stored by the write quorum** (ADR-0018). A database whose live replicas can't reach W rejects writes rather than acknowledging them unsafely.
11. **Batching and pipelining.** Commits that arrive while a batch is in flight are grouped into the next batch and confirmed together, and several batches may be in flight to each follower at once. A replicated commit then costs about one network round trip, shared across concurrent writers, on top of the engine's own fsync.

## Why WAL shipping is robust without engine hooks

- **No frame is lost before it ships:** `fleetringd` owns every connection, so nothing else checkpoints or restarts the WAL.
- **Capture is cheap:** re-reading just-written frames hits the OS page cache. A replicated write is dominated by fsync and the network round trip.
- **A deposed primary can't acknowledge writes:** acknowledgement needs the write quorum, and fenced followers refuse batches from an old epoch (ADR-0018).
- **Torn appends are harmless:** SQLite's recovery ignores frames after the last valid commit frame.

Throughput: one database is limited by SQLite's single writer and one fsync per commit, not by shipping. Fleet throughput scales out across many small databases. An engine with concurrent writers, such as Turso's MVCC, plugs in as another adapter.

## Alternatives considered

- **libSQL's WAL hooks as the only path.** Capture inside the engine is more robust on followers, but v0.1 would depend on libSQL internals. It remains a later adapter.
- **Logical changesets** from SQLite's session extension. Engine-neutral, but they miss schema changes, and followers converge only approximately.
- **FUSE or a custom VFS**, as LiteFS does. It needs `unsafe` FFI or a FUSE mount, which is hard to run on laptops.
- **One fixed position format,** such as `(epoch, generation, offset)` everywhere. Simpler at first, but it would tie leases, the protocol, and migration to SQLite's WAL.

## Consequences

- No other process may write a database while `fleetringd` runs. The daemon holds an exclusive lock on its data directory.
- SQLite replication is physical, so a primary and its followers agree on page size and file format; the adapter checks both.
- Moving a database to a different engine, such as SQLite to Postgres, is a logical copy rather than a physical one (ADR-0021).
- Later adapters, each in its own ticket: libSQL or Turso as frame sources, Turso MVCC, Postgres as self-replicating, and S3 durability sinks.
