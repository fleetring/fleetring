---
id: FR-025
title: WAL format library
status: done
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

- [x] Frames read from WAL files written by real SQLite (through `rusqlite` in tests) validate, and corrupting any byte of a frame is detected.
- [x] A torn final transaction, cut mid-frame or before its commit frame, is never returned.
- [x] Mirroring a primary's batches into a follower's WAL, then opening the follower with SQLite, gives the same contents, and `PRAGMA integrity_check` reports `ok`.
- [x] Property test: any sequence of transactions and checkpoints on a primary, mirrored batch by batch, leaves the follower identical to the primary once both checkpoint.

## Progress

- 2026-10-28: Done.
  - **Format:** WAL and frame headers, both checksum byte orders, salts, and cumulative checksums.
  - **Reader:** `read_committed` returns only whole committed transactions. It ignores torn tails, frames left from an older generation, and frames SQLite spilled from a transaction still open.
  - **Payload:** a versioned batch payload that can't be decoded when truncated.
  - **`LogEnd`:** the follower's continuity and checksum rules with no I/O, so they're tested on every target.
  - **`Mirror`:** append with fsync, truncate back to a commit, and an explicit `start_generation` that the follower calls only after checkpointing its database.
- **Positions** encode as 20 fixed-width big-endian bytes (checkpoint sequence, salts, offset), so byte order equals log order (ADR-0016); a proptest checks it. Generation order relies on the checkpoint sequence, which holds because the adapter keeps its write connection open (SQLite resets the sequence when the WAL is deleted).
- **Tests:** 47 integration tests and a doctest, under a second in debug.
  - 39 byte-level tests in both checksum orders, with no file I/O, so they run on wasm and s390x.
  - 8 tests against real SQLite, non-wasm only.
  - A 24-case proptest mirroring transactions and checkpoints, which ends with byte-identical database files.
  - The wasm32-wasip1 test build is verified locally. CI's s390x job builds SQLite with `cross`.
- **Later:** the reader rescans the WAL on each call, which is O(WAL size) and fine while checkpoints keep the WAL small. An incremental cursor can replace it if profiling asks for one.
