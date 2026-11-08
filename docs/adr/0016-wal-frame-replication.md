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
3. **Durability sinks are separate from replicas.** Replicas give availability and failover; a durability sink, such as S3 snapshots and log archiving for point-in-time restore, gives backups. Sinks get their own trait later (MVP step 5), so "replicas plus S3", "S3 only", or an engine's own S3 mode can plug in without changing replication. (*Amended 2026-11-03, FR-049:* [ADR-0029](0029-s3-durability-sink.md) builds them as archives, fed by a physical change stream, and puts the plug-in point at the object store rather than at a sink trait of their own.)

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
12. **A hash chain over the log** (*added 2026-10-29, FR-044*). Each replica keeps a BLAKE3 chain over the log in its engine-neutral replication state, never inside the engine's log: one hash per transaction, `hash = BLAKE3(prev || start || end || BLAKE3(bytes))`, where `start` and `end` are the opaque positions the transaction runs between (each length-prefixed) and `bytes` are the transaction's bytes as the engine delimits them inside a batch, hashed on their own so BLAKE3 takes them a whole chunk at a time (`FrameSourceEngine::transactions`), with a generation's header folded into the first transaction after it and the batch's own framing left out. Hashing per transaction, from the position the receiver continues at, makes a batch read back from the log hash the same as the batches that shipped its transactions one at a time, and leaving the shipping epoch out keeps two replicas with identical logs in agreement after a promotion re-ships old bytes. The primary's stream computes a batch's hash as it ships the batch, off the database's thread, and sends it with the batch; a follower recomputes it before applying and refuses a mismatch by asking for a snapshot, as a diverged copy does. Snapshots and promotion catch-up carry the hash at their position, so a seeded or promoted replica continues the chain. Only an anchor is durable, recorded at checkpoints and when a snapshot is installed; a restart recomputes the rest from the log, so the write path gains no write or fsync. A replica whose record doesn't fit its log derives an anchor from where its log begins, proves agreement from there on only, and takes the primary's hash on first contact. `db verify` reads each replica's log back from its anchor to a position and compares the hashes.

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

## Amendment (2026-11-08, FR-078, #109): statements stop at an interrupt

A statement ran on its primary until it finished, whatever its client did. A client's timeout cancelled only its call, so a recursive CTE that never ends held the database's thread, and for a write its writer, for good. Every engine adapter now takes a way to stop one.

1. **`execute`, `query`, and `schema` take an engine-neutral `Interrupt`.** It is a shared flag with a reason, `Deadline` or `Cancelled`, and no clock. The engine checks it before it starts a call and while the user's statements run, and once it is raised fails the call with `EngineError::Interrupted` and the reason. An interrupted write transaction rolls back and commits nothing, so it yields no batch and no row change. Once the statements have all run, the engine finishes the call, its own bookkeeping and commit included, whatever the flag says, so no transaction is cut between its last statement and its commit.
2. **Time stays above the adapter.** The actor's handle raises the flag: at the request's deadline, from a `tokio::time` timer, and when its caller drops the call. The engine never reads a clock, so a simulation stays deterministic (ADR-0020), and a deadline and a cancellation are one mechanism. Under inline execution the flag can only be raised between calls, so there it stops a request whose deadline passed, or whose caller left, while it waited.
3. **The SQLite adapter checks the flag from SQLite's progress handler,** every thousand virtual-machine instructions, installed only around user statements and schema reads, and maps `SQLITE_INTERRUPT` to `EngineError::Interrupted`. Its own statements around a write (change records, sink marks, `COMMIT`) run without it.
4. **A data node's deadline** is its `[limits] statement_timeout_ms` from when it takes the request, or the call's own `grpc-timeout` if earlier; `EngineError::Interrupted` reaches the client as `DEADLINE_EXCEEDED`, or `CANCELLED` for a cancelled call.
