---
id: FR-021
title: "`CellsSpec`: serializable cells layers"
status: todo
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

- **Serde for `Directory`**, behind the `serde` feature. Directory keys and prefixes are opaque bytes, so choose their JSON representation first and record it in the spec. For example: a string when the bytes are valid UTF-8, and an explicit encoding otherwise.
- **`CellsSpec`:** the directory rules, the optional selector `RingSpec`, and each cell's spec (a `RingSpec` or a nested `CellsSpec`). `Cells::to_spec` writes one, and `Cells::from_spec` validates it and rebuilds the layer.
- **Round trip:** `Cells::from_spec(&cells.to_spec())` has the same fingerprint as `cells`.

## Acceptance criteria

- [ ] `Directory` and `CellsSpec` serialize and deserialize behind `serde`. Invalid input returns an error, never a panic.
- [ ] Every cells case in `placement-v1.json` round-trips through `CellsSpec` with an unchanged fingerprint.
- [ ] A fuzz target covers `CellsSpec` decoding, either `spec_json` or a new one.

## Notes

- Split out of FR-020 on 2026-10-28. FR-020 gave `CellId` serde support, but not `Directory`.
- The fingerprint encoding in spec §13 doesn't change. This ticket only adds a serialized form.
