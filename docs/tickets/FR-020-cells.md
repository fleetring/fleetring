---
id: FR-020
title: "Cells: nested placement with directories and per-cell rings"
status: todo
priority: P1
size: L
milestone: M3 Change and interop
depends_on: [FR-012, FR-013]
adrs: [ADR-0004, ADR-0006, ADR-0008, ADR-0012]
---

# FR-020: Cells: nested placement with directories and per-cell rings

## Goal

Implement ADR-0012. Keys are placed in two or more layers: a directory or ring picks the cell (region, cell, or tenant pool), and the cell's own ring picks zone- and host-aware replicas. Nesting composes to any depth without changing `Ring`.

## Scope

- **`Placement` trait** (crate-public; decide on sealing in review), implemented by `Ring<H>` and `Cells<P>`:
  - `replicas_for(&self, key: &[u8]) -> CellPlacement<'_>`
  - `fingerprint(&self) -> Fingerprint`
- **`CellId`:** a validated label with the same grammar as `NodeId`.
- **`Directory`:** exact-key and longest-prefix rules mapping to a `CellId`. Rules are deterministic and serializable (behind `serde`).
- **`Cells<P = Ring>` and `CellsBuilder`:**
  - Selector: a directory, a ring whose nodes are the cell IDs with explicit weights, or a directory with a ring fallback.
  - `cell(id, placement)` adds a cell.
  - `build()` validates that every selector target names a cell, that every cell is reachable, and that no cell ring shares its selector ring's seed.
- **`CellPlacement<'a>`:** the cell path (top to leaf) plus the leaf `ReplicaSet<'a>`.
- **Lookups:** `replicas`, `replicas_where` (the filter applies inside the cell), `primary`, and `preference` on the cell's ring.
- **Fingerprint:** a layer's fingerprint covers the directory rules, the selector ring's fingerprint, and each cell's ID, weight, and fingerprint, in ID order.
- **Diff:** `Cells::diff` separates cross-cell moves from in-cell moves.
- **Out of scope:** Across mode (one replica per cell) is specified in ADR-0012 and deferred to v0.2 (decided 2026-10-28).

## Acceptance criteria

- [ ] Within mode places every replica of a key in a single cell, and inside that cell the zone and host spreading from ADR-0006 holds.
- [ ] Directory rules win over the selector ring. Longest-prefix matching is deterministic, and exact rules win over prefixes.
- [ ] **Churn locality:** adding, removing, or reweighting a node inside a cell never changes which cell any key maps to. Proptest this over nested memberships.
- [ ] Changing one cell's selector weight moves keys only into or out of that cell, and the diff reports each move as cross-cell.
- [ ] Seed independence: `build()` rejects a cell ring that shares its selector ring's seed. A statistical test shows that with distinct seeds, vnode balance inside a cell matches a standalone ring's balance.
- [ ] Three-level nesting (`Cells<Cells<Ring>>`) works through the same API, and the result's path names each layer.
- [ ] The fingerprint changes when any layer changes, and stays the same when only epochs change.
- [ ] Spec v1 gains a composition section, and golden vectors cover directory, ring, and nested cases (coordinate with FR-014).
- [ ] Benchmark: 100 cells × 100 nodes. Record the result in `docs/perf.md`; the target is ≤ 5 µs per `replicas` lookup.

## Notes

- Cells partition keys; labels spread replicas. Don't add zone or host semantics to cells (ADR-0012, point 1).
- Keep `Ring`'s API unchanged. Nesting comes from composition, not from a generic member type.
