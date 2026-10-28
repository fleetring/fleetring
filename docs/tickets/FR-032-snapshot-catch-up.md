---
id: FR-032
title: Snapshot catch-up
status: done
priority: P1
size: M
milestone: M6 Replication and failover
depends_on: [FR-030]
adrs: [ADR-0016]
---

# FR-032: Snapshot catch-up

## Goal

A new follower, or one too far behind to resume in the current WAL generation, catches up from a consistent snapshot (ADR-0016, point 6).

## Scope

- The primary takes a consistent snapshot (a pinned copy of the files; see Progress and ADR-0016 point 8), without blocking writes while it runs, and records the position the snapshot corresponds to.
- `Snapshot` streams it in chunks with a checksum. The follower writes it to a temporary file, verifies it, and swaps it in atomically.
- The follower then resumes `Replicate` from the snapshot's position.
- The same path seeds a node newly added to a database's replica set, ahead of live migration in MVP step 3.

## Acceptance criteria

- [x] A follower added to a 100 MB database during a write workload catches up and stays current, and its database ends byte-identical to the primary's once both checkpoint.
- [x] An interrupted transfer resumes or restarts cleanly, and never leaves a partial database in place.
- [x] Simulation: a follower restarted after the primary has checkpointed past its position catches up through a snapshot.

## Progress

- 2026-10-28: Done.
  - **Consistent snapshots without blocking writes:** `FrameSource::pin_snapshot` (through `DatabaseHandle::pin_snapshot`) runs one quick call on the database's thread. It reads the committed WAL prefix's end and returns a `PinnedSnapshot`: the position, the open files with the length of each that belongs to the copy (`data.db-checkpoint`, `data.db`, and the WAL prefix, last), and a `SnapshotGuard`. While any snapshot of a database is pinned, its checkpoints fail with `CheckpointBlocked` (the pins are shared by every clone of the `SqliteEngine`), so the database file can't change and the WAL is only appended to. After reading, `PinnedSnapshot::confirm` checks that the WAL header is unchanged, which catches the one remaining case: a WAL fully checkpointed just before the pin restarts at the next commit and overwrites its prefix. Self-replicating engines don't implement it: `pin_snapshot` returns `DatabaseError::Unsupported`, and `Snapshot` and `seed` refuse them cleanly.
    - **Not the online backup API.** `sqlite3_backup` rewrites the destination's header (schema cookie and change counter), so the copy wouldn't be byte-identical to the primary, and it produces no WAL prefix for the follower's mirror to continue from. The pinned copy of the files is byte-identical and keeps ADR-0016's mirror invariant. ADR-0016 point 8 still names the backup API and needs an amendment.
  - **Transfer:** the `Snapshot` RPC streams the pinned files from a task of its own, in chunks of at most 1 MiB read on blocking threads (inline under simulation), as the archive in `replication/archive.rs` (per file: `u16` name length, name, `u64` length, content). The last chunk carries no data, the position, and an XXH3-128 checksum of the stream from the workspace's `twox-hash`; XXH3 detects corruption, not tampering. The stream gives up, releasing the pin, if the receiver goes away or takes longer than `open_timeout` to take a chunk.
  - **Install:** the follower writes each chunk into `.incoming-N` under the registry root as it arrives, checks offsets, refuses duplicate or non-plain names, syncs each file, and verifies the checksum at the end. Only then does it rename any old copy to `.retired-N`, rename the staging directory into place, fsync the parent, and remove the old copy. The database's place holds the old copy, nothing, or the new copy, never a mix. A failed transfer removes its staging directory, and `Replication::start` sweeps the ones a crash left. **Interrupted transfers restart from offset 0**; resuming would mean keeping the pin across connections.
  - **Re-seeding:** a new `Reseed { epoch }` message in `ReplicateMessage` (proto field 4). The primary sends it when its plan says `NeedsSnapshot` (the follower is behind the in-memory marker and the retained batches don't reach it), or when `committed_since` refuses the follower's position with `InvalidPosition` while the follower is behind the primary's end. The follower fetches the snapshot, swaps it in, reopens its copy, checks it is at the snapshot's position, and acknowledges it; the primary records that position and reopens the stream from it. The 60 s `lag_limit` pause after `NeedsSnapshot` is gone.
  - **Checkpoint marker:** `SqliteDatabase::checkpoint` writes the position it covers to `data.db-checkpoint` (staged, fsynced, renamed, directory fsynced) before running RESTART, and loads it on open. `committed_since` refuses an earlier-generation position unless it is exactly that marker, so a primary that restarted no longer starts a follower behind an older checkpoint on the new generation: the follower is re-seeded instead. A follower records the same marker when the primary's next generation reaches it, and snapshots carry the file, so a promoted copy vouches for its last checkpoint too.
  - **Seeding a new replica:** `Replication::seed(database, from, epoch)` fetches and installs a snapshot through the same path, ending any stream applying to the copy first, and returns the copy's position. It returns `SeedError` (`SelfReplicating`, `Primary`, `Failed`). `FollowerStatus::snapshots` counts the snapshots installed since the node started.
  - **Tests:**
    - `tests/replication/snapshot.rs`: a follower added (replica set grown from 2 to 3) to a 4 MiB database during a write workload seeds itself, catches up, stays current, and ends byte-identical to the primary and the other follower after a checkpoint; the 100 MB run is `#[ignore]`d (about 8 s in release). Transfers that are cut, corrupted (caught by the checksum), and held leave no copy and no staging directory in place; a corrupted `seed` keeps the old copy byte for byte; a clean one resumes replication from its position.
    - `tests/actor/pinned.rs`: a pinned snapshot holds off checkpoints but not writes, and its copy catches up and ends byte-identical; a snapshot whose WAL restarts under it fails `confirm`; a reopened primary refuses a position behind its last checkpoint and accepts the marker.
    - `src/replication/archive.rs` unit tests: round trips at every chunk size, corruption, truncation, and hostile names.
    - Simulation: `a_follower_restarted_past_a_checkpoint_catches_up_through_a_snapshot` crashes a follower, writes until the primary has checkpointed past it twice, restarts it, and checks that it re-seeded, converges, and that every acknowledged write holds, on the fixed seeds, with a replay comparison.

## Notes for later tickets

- ADR-0016 point 8 is amended: the snapshot is a pinned copy of the files, not the online backup API.
- Transfers restart rather than resume. Resuming needs the primary to keep a pin, or a second identical snapshot, across connections.
- A long transfer holds off the primary's checkpoints, so its WAL grows meanwhile.
- `PrimaryStatus::checkpointed` is still in memory only; after a restart it is `None` until the next checkpoint, although the engine enforces the persisted marker.
- `Replication::seed` takes the primary and epoch from the caller; live migration (MVP step 3) decides when to call it.
