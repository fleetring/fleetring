---
id: FR-032
title: Snapshot catch-up
status: todo
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

- The primary takes a consistent snapshot with SQLite's online backup API, without blocking writes while it runs, and records the position the snapshot corresponds to.
- `Snapshot` streams it in chunks with a checksum. The follower writes it to a temporary file, verifies it, and swaps it in atomically.
- The follower then resumes `Replicate` from the snapshot's position.
- The same path seeds a node newly added to a database's replica set, ahead of live migration in MVP step 3.

## Acceptance criteria

- [ ] A follower added to a 100 MB database during a write workload catches up and stays current, and its database ends byte-identical to the primary's once both checkpoint.
- [ ] An interrupted transfer resumes or restarts cleanly, and never leaves a partial database in place.
- [ ] Simulation: a follower restarted after the primary has checkpointed past its position catches up through a snapshot.
