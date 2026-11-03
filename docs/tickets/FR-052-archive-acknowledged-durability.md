---
id: FR-052
title: A durability mode that acknowledges a write only once it is archived
status: todo
priority: P2
size: M
milestone: M9 Durability sinks and verification
depends_on: [FR-049, FR-035]
adrs: [ADR-0023, ADR-0029]
---

# FR-052: A durability mode that acknowledges a write only once it is archived

## Goal

FR-049's archive is asynchronous, so a disaster that destroys every replica loses the writes the archive hadn't stored yet. Some databases would rather pay for an object-store round trip on every commit than lose any acknowledged write. ADR-0029 left this out of v1. This ticket adds it as a per-database choice beside ADR-0023's durability levels.

## Scope

- A durability level, set like the existing ones (`ChangeDurability`), under which a write is acknowledged only after the archive has stored the segment that holds it, in addition to the replica quorum the database already requires.
- Batch archive writes so that concurrent commits share one object-store request. Measure the added commit latency against a local object store and S3, and record it in `docs/perf.md`.
- If the archive is unreachable, writes under this level fail with an error that names the archive, rather than being acknowledged without it. Status shows the archive lag and the failures.
- Simulation: with archive faults injected, no write acknowledged under this level is missing from a restore.

## Acceptance criteria

- [ ] A database at this level loses no acknowledged write when every replica is destroyed and it is restored from the archive.
- [ ] Commit latency at this level is measured and documented, with and without batching.
- [ ] Other databases on the same node are unaffected when the archive is slow or down.

## Progress

- Not started.
