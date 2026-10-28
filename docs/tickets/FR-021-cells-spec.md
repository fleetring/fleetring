---
id: FR-021
title: "`CellsSpec`: serializable cells layers"
status: done
priority: P2
size: M
milestone: M3 Change and interop
depends_on: [FR-013, FR-020]
adrs: [ADR-0008, ADR-0012]
---

# FR-021: `CellsSpec`: serializable cells layers

## Goal

Describe a cells layer portably, as `RingSpec` does for a ring, so a control plane can distribute cell layouts and every router rebuilds exactly the same layer.

## Scope

- **Serde for `Directory`**, behind the `serde` feature. Directory keys and prefixes are opaque bytes, so choose their JSON representation first and document it with the type. For example: a string when the bytes are valid UTF-8, and an explicit encoding otherwise.
- **`CellsSpec`:** the directory rules, the optional selector `RingSpec`, and each cell's spec (a `RingSpec` or a nested `CellsSpec`). `Cells::to_spec` writes one, and `Cells::from_spec` validates it and rebuilds the layer.
- **Round trip:** `Cells::from_spec(&cells.to_spec(), hasher)` has the same fingerprint as `cells`.

## Acceptance criteria

- [x] `Directory` and `CellsSpec` serialize and deserialize behind `serde`. Invalid input returns an error, never a panic.
- [x] Every cells case in `placement-v1.json` round-trips through `CellsSpec` with an unchanged fingerprint.
- [x] A fuzz target covers `CellsSpec` decoding, either `spec_json` or a new one.

## Notes

- Split out of FR-020 on 2026-10-28. FR-020 gave `CellId` serde support, but not `Directory`.
- The fingerprint encoding in spec §13 doesn't change. This ticket only adds a serialized form.

## Progress

- 2026-10-28: Implemented in `cells_spec.rs`, with `Directory` serde in `serde_impls.rs`.
  - `CellsSpec` mirrors `RingSpec`: a format version, the directory, an optional selector `RingSpec`, the cells sorted by ID (each a `RingSpec` or a nested `CellsSpec`, through `PlacementSpec`), and the layer's fingerprint. Unknown fields are ignored, as in ADR-0008.
  - `Cells::from_spec(&spec, hasher)` takes a hasher, like `Ring::from_spec`. It checks the format, the selector, each cell in ID order, the builder's rules, and the fingerprint, and errors carry the path of the failing cell.
  - The JSON form is documented in rustdoc, as `RingSpec`'s is, rather than in the placement spec: serialized forms aren't placement, and spec v1 is frozen. A test keeps the rustdoc example and a locked JSON fixture in step.
  - A rule's key or prefix is a JSON string when its bytes are valid UTF-8, and `{"hex": "…"}` in lowercase otherwise. `Directory` rejects unknown fields and duplicate rules: a directory field an older reader skipped would silently route keys differently.
- Tests: all four cells vectors round-trip through `CellsSpec` and JSON text with their fingerprints and placements unchanged, plus every error variant, 23 malformed directories, and a 32-case proptest over two-level layers with non-UTF-8 rules.
- Fuzzing: `spec_json` also decodes each input as `Cells<Ring>` and `Cells<Cells<Ring>>`. When a mutated spec fails only on stored fingerprints, the harness repairs them and retries, so mutations reach the builder. 6.1M executions in 2 minutes found nothing.
- Ships in the next release after 0.1.0. It adds API only.
