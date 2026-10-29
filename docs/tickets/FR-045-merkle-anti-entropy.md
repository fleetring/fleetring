---
id: FR-045
title: Merkle anti-entropy over pages, so a returning follower fetches only what differs
status: todo
priority: P2
size: L
milestone: M9 Durability sinks and verification
depends_on: [FR-044, FR-032]
adrs: [ADR-0010, ADR-0016]
---

# FR-045: Merkle anti-entropy over pages, so a returning follower fetches only what differs

## Context

A follower that falls behind the log's retention is re-seeded from a full snapshot (FR-032), however little of the database actually changed while it was away. ADR-0010 deferred the standard answer: a hash tree over the database's pages, as Cassandra and Riak build over key ranges for anti-entropy repair, so two copies compare roots, descend only into subtrees that differ, and exchange the leaves that do. For FleetRing the leaves are database pages, which makes the comparison exact (a page is either identical or not) and the repair a page copy. The same tree gives `db verify` (FR-044) a proof that two copies are byte-identical, not only that their logs agree, which also catches a corruption that never went through the log.

## Scope

- **A page hash tree per database**, BLAKE3 over page content, with a fan-out that keeps the tree for a 10 GiB database at 4 KiB pages under a few megabytes. The primary maintains it incrementally from the frames it ships (each batch names the pages it changed), and persists it beside the database so a restart doesn't rebuild it. Followers maintain theirs from the batches they apply.
- **Repair instead of re-seed:** a follower whose position no longer fits the log compares its tree with the primary's, descending only where roots differ, then fetches the differing pages and the log from the primary's current position. Both copies must be at a checkpoint boundary (no pending WAL frames) for the comparison, which the existing snapshot pin provides.
- **`db verify --pages`:** compares every replica's tree root with the primary's and reports byte-identical or the differing page ranges.
- **Not in scope:** repair between two followers without the primary, and trees over anything but whole pages.

## Definition of done

- [ ] A follower that was away while 1% of a database's pages changed catches up by fetching about 1% of the pages, measured in the replication tests, and ends byte-identical to the primary (`db verify --pages` agrees and SQLite's `integrity_check` passes).
- [ ] The tree stays consistent with the database across checkpoints, restarts, and snapshot seeding; a rebuilt tree equals the maintained one.
- [ ] A page corrupted on one replica outside the log is found by `db verify --pages` and repaired by the same path.
- [ ] Tree maintenance cost per batch is measured in `docs/perf.md`.
