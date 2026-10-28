---
id: FR-025
title: WAL format library
status: todo
priority: P0
size: M
milestone: M5 Daemon foundations
depends_on: [FR-022]
adrs: [ADR-0016, ADR-0018]
---

# FR-025: WAL format library

## Goal

Read, validate, and mirror SQLite WAL files exactly, in a pure synchronous crate (ADR-0016). This is the SQLite adapter's internals: its types encode into the engine-defined position and payload, and never appear above the adapter.

## Scope

- **Format:** parse the 32-byte WAL header and the 24-byte frame headers. Validate the magic number, format version, page size, salts, and SQLite's cumulative checksums in both byte orders.
- **Reader:** given a WAL file and a starting position, return the complete transactions after it, up to the last commit frame, as one batch with its start and end positions. Never return part of a transaction.
- **Mirror writer:** append a validated batch to a follower's WAL file and fsync it; start a new generation from a shipped header; truncate back to a commit boundary (ADR-0018, point 5).
- **Positions:** a `(generation, offset)` type with a total order, plus the salts that identify a generation.
- **No SQLite linkage:** the crate works on bytes and files.

## Acceptance criteria

- [ ] Frames read from WAL files written by real SQLite (through `rusqlite` in tests) validate, and corrupting any byte of a frame is detected.
- [ ] A torn final transaction, cut mid-frame or before its commit frame, is never returned.
- [ ] Mirroring a primary's batches into a follower's WAL, then opening the follower with SQLite, gives the same contents, and `PRAGMA integrity_check` reports `ok`.
- [ ] Property test: any sequence of transactions and checkpoints on a primary, mirrored batch by batch, leaves the follower identical to the primary once both checkpoint.
