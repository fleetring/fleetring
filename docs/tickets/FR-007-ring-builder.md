---
id: FR-007
title: Ring and RingBuilder with validation and canonical membership
status: todo
priority: P0
size: M
milestone: M2 Placement core
depends_on: [FR-003, FR-004]
adrs: [ADR-0001, ADR-0005, ADR-0006]
---

# FR-007: `Ring` and `RingBuilder` with validation and canonical membership

## Goal

Build the core immutable `Ring<H = Xxh3>` and a builder that validates every input in one place (ADR-0001, ADR-0005, ADR-0006).

## Scope

- **`RingBuilder<H>`** with consuming setters (`build` moves the node list):
  - `hasher(h2) -> RingBuilder<H2>`, `seed(u64)`, `strategy(Strategy)`, `replication_factor(ReplicationFactor)`, `epoch(Epoch)`;
  - `node(Node)`, `nodes(impl IntoIterator<Item = Node>)`, `remove_node(&NodeId)`;
  - `build() -> Result<Ring<H>, BuildRingError>`.
- **`Ring::builder()`** returns `RingBuilder<Xxh3>`, and `ring.to_builder()` returns a pre-filled builder (requires `H: Clone`).
- **`Strategy`:** `#[non_exhaustive]`, with only `Rendezvous` (the default) for now. FR-010 adds `Vnodes`.
- **Validation:** all of it happens in `build()`:
  - at least one node;
  - no duplicate `NodeId`;
  - every host label belongs to exactly one zone;
  - strategy-specific limits, which FR-010 wires in.
- **`BuildRingError`:** a `#[non_exhaustive]` enum built with thiserror, with variants `NoNodes`, `DuplicateNode { id }`, `HostInMultipleZones { host, zones }`, and `TooManyTokens { total, max }`.
- **Internals**
  - Nodes are sorted by `NodeId`.
  - Each node's `node_hash` is precomputed.
  - Zones and hosts are interned to small indices so diversity checks are cheap.
  - The membership digest is computed once and reused by FR-011 and FR-013.
- **Accessors:** `node(&NodeId)`, `nodes()` (which returns a named iterator type), `node_count()`, `zone_count()`, `replication_factor()`, `strategy()`, `seed()`, `epoch()`, `hasher()`.
- **Trait impls**
  - A compile-time assertion that `Ring<Xxh3>: Send + Sync`.
  - `Clone` when `H: Clone`.
  - A `Debug` impl that summarizes the ring rather than dumping its internals.

## Acceptance criteria

- [ ] Building from any permutation of the same nodes produces the same internal order and node hashes (proptest).
- [ ] Every `BuildRingError` variant can be reached from a test, and its `Display` names the offending value.
- [ ] `ring.to_builder().build()` reproduces an equal ring.
- [ ] `build()` never panics, whatever the input (proptest with arbitrary nodes).
- [ ] The rustdoc example builds a three-node ring with zones and shows how to handle errors with `?`.

## Notes

- `len()` is deliberately missing. "Length" is ambiguous for a ring (nodes or tokens?), so `node_count()` is explicit.
