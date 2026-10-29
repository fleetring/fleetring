---
id: FR-041
title: Replicated sink positions, so a new primary resumes a change stream without duplicates
status: todo
priority: P2
size: M
milestone: M8 Access and integration
depends_on: [FR-039]
adrs: [ADR-0017, ADR-0022, ADR-0024]
---

# FR-041: Replicated sink positions, so a new primary resumes a change stream without duplicates

## Goal

A sink's delivered position lives on the primary alone today (`<data_dir>/changes/<sink>/<database>`). After a failover the new primary starts its pumps from its log's beginning, so a sink receives again everything recorded since the last checkpoint; consumers dedupe by position. Carry the position with the database instead, after rqlite's replicated high-water mark, so that a new primary continues where the old one stopped.

## Scope

- **Where the position lives: decide between two places**, and record the decision as an ADR:
  - **Control state.** A `SinkDelivered { database, sink, position }` command through `fleetring-control`'s pure `apply`, batched so that a busy stream doesn't flood the control log; the pump reads the position on start from the control view it already has.
  - **The database.** A `_fleetring_sinks` table beside `_fleetring_cdc` (ADR-0024), written by the primary after each delivery inside a small write transaction, so it replicates with the data; the new primary reads it from its own copy. This costs one replicated write per delivery, which could be folded into the next user transaction the way pruning is.
- **Resuming.** On promotion, each pump starts from the recorded position if the new log still continues from it, and from the log's beginning (counting a reset, as today) if not.
- **Pruning.** `LogicalChanges::release` uses the replicated positions, so rows aren't dropped before a sink the old primary was feeding has them.
- **Not in scope:** exactly-once delivery. At-least-once stays; this ticket only removes the duplicates a failover causes.

## Acceptance criteria

- [ ] In `tests/sim/changes.rs`, a primary crash no longer makes the file sink on the new primary repeat records the old primary delivered; `check::change_stream_consistent` gains a duplicates bound of one batch per failover.
- [ ] A sink's position outlives the node that recorded it: after the old primary's data directory is deleted, the new primary still resumes from the right place.
- [ ] `status --node` shows whether a sink's position is replicated or local.

## Progress

- Not started.
