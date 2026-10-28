# ADR-0016: Replicate each database by shipping its SQLite WAL frames

- **Status:** Proposed (2026-10-28)
- **Date:** 2026-10-28
- **Related:** ADR-0006, ADR-0010, ADR-0013, ADR-0015, ADR-0018

## Context

Each database has one primary and R − 1 followers, chosen by the ring (ADR-0006). Writes go to the primary, and followers must hold every acknowledged write so that a failover loses nothing. In WAL mode, SQLite already writes each transaction as a run of checksummed page frames that ends in a commit frame. That log is a ready-made replication stream, provided the primary controls when it is checkpointed.

## Decision

1. **The primary's WAL is the replication log.** The database's actor owns the only write connection and sets `journal_mode=WAL`, `wal_autocheckpoint=0`, and `synchronous=FULL`. After each write transaction commits, the actor reads the new frames from the `-wal` file, up to and including the last commit frame, and ships them as one batch.
2. **A position names every point in the log:** `(epoch, generation, offset)`. The generation changes whenever a checkpoint restarts the WAL, which changes the header's salts. Frames are validated with SQLite's own cumulative checksums before they are shipped, and again before they are applied.
3. **Acknowledged means stored by the write quorum.** The primary replies to a client only after W replicas, itself included, have durably stored the batch (ADR-0018). A database whose live replica set can't reach W rejects writes instead of acknowledging them unsafely.
4. **Followers mirror the WAL byte for byte.** A follower appends each batch to its own `-wal` file and fsyncs it before acknowledging. A new generation starts with the primary's WAL header. The follower doesn't open the database while it is a follower. On promotion it removes any stale `-shm` file and opens the database normally. SQLite's WAL recovery then replays exactly the frames up to the last valid commit frame, which is the same recovery SQLite trusts after any crash.
5. **The primary checkpoints, and followers follow.** When the WAL passes a size threshold and every follower has acknowledged all of it, the primary runs `wal_checkpoint(TRUNCATE)` and ships a checkpoint marker. Each follower checkpoints its mirror at the marker, then starts the new generation. A follower that falls behind past a limit stops blocking checkpoints and catches up from a snapshot instead.
6. **Snapshot catch-up.** A follower that is new, or too far behind to resume in the current generation, receives a consistent copy of the database from SQLite's online backup API, then the WAL from the copy's position.
7. **No follower reads in v0.1.** Followers are warm standbys. Serving reads would mean updating SQLite's WAL index while readers are open, which comes later.
8. **Engine adapter.** Capture and apply sit behind the `DatabaseEngine` trait (ADR-0015). Replication, leases, and migration only ever see batches of committed page frames and their positions. libSQL and Turso adapters can capture inside their engines later, behind the same trait.

## Why this is robust without engine hooks

- **No frame is lost before it ships:** `fleetringd` owns every connection, so nothing else checkpoints or restarts the WAL.
- **Capture is cheap:** re-reading frames that were just written hits the OS page cache. A replicated write is dominated by fsync and the network round trip.
- **A deposed primary can't acknowledge writes:** acknowledgement needs the write quorum, and fenced followers refuse batches from an old epoch (ADR-0018).
- **Torn appends are harmless:** SQLite's recovery ignores frames after the last valid commit frame, on a follower as on any crashed database.

## Alternatives considered

- **libSQL's WAL hooks.** They capture inside the engine and offer a frame injector for followers, but v0.1 would depend on libSQL internals with an uncertain long-term future. They remain a later adapter.
- **Logical changesets** from SQLite's session extension. They work with any engine, but they don't capture schema changes, and followers converge only approximately.
- **FUSE or a custom VFS**, as LiteFS does. Either intercepts every write, but a VFS needs `unsafe` FFI and a FUSE mount is hard to run on laptops.

## Consequences

- No other process may write a database while `fleetringd` runs. The daemon holds an exclusive lock on its data directory.
- Replication is physical, so a primary and its followers must agree on page size and file format. The adapter checks both when a follower starts.
- Mirrored WALs make followers converge byte for byte, which simplifies the later anti-entropy layer (ADR-0010).
