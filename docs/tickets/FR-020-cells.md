---
id: FR-020
title: "Cells: nested placement with directories and per-cell rings"
status: done
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

- [x] Within mode places every replica of a key in a single cell, and inside that cell the zone and host spreading from ADR-0006 holds.
- [x] Directory rules win over the selector ring. Longest-prefix matching is deterministic, and exact rules win over prefixes.
- [x] **Churn locality:** adding, removing, or reweighting a node inside a cell never changes which cell any key maps to. Proptest this over nested memberships.
- [x] Changing one cell's selector weight moves keys only into or out of that cell, and the diff reports each move as cross-cell.
- [x] Seed independence: `build()` rejects a cell ring that shares its selector ring's seed. A statistical test shows that with distinct seeds, vnode balance inside a cell matches a standalone ring's balance.
- [x] Three-level nesting (`Cells<Cells<Ring>>`) works through the same API, and the result's path names each layer.
- [x] The fingerprint changes when any layer changes, and stays the same when only epochs change.
- [x] Spec v1 gains a composition section, and golden vectors cover directory, ring, and nested cases (coordinate with FR-014).
- [x] Benchmark: 100 cells × 100 nodes. Record the result in `docs/perf.md`; the target is ≤ 5 µs per `replicas` lookup.

## Notes

- Cells partition keys; labels spread replicas. Don't add zone or host semantics to cells (ADR-0012, point 1).
- Keep `Ring`'s API unchanged. Nesting comes from composition, not from a generic member type.

## Progress

- 2026-10-28: Implemented in `cells.rs`, `directory.rs`, and `cell_diff.rs`:
  - `Placement` is sealed and implemented by `Ring` and `Cells`. `CellPlacement::cell()` returns an `Option`, because a bare `Ring` has an empty path.
  - Selection order: an exact rule, then the longest matching prefix, then the selector ring's preferred primary. A directory alone needs a catch-all (the empty prefix). A catch-all next to a selector ring is rejected as `ShadowedRing`, because the ring could never be reached.
  - Liveness filters apply only inside the key's cell. If every node in the cell is down, `replicas_where` returns `None`; a key never fails over to another cell.
  - `build()` applies the seed rule at every depth: no ring anywhere under a selector may share the selector's seed. This also covers grandchild rings, whose correlation would survive the layers in between.
  - `Cells::diff` yields `CellChange`s and a `CellMovementSummary` rather than reusing `Change` and `MovementSummary`: node IDs are unique only within one ring, and a cross-cell move relocates every replica.
- Fingerprint encoding (spec §13): a `CELL` tag and a mode byte, then the directory rules sorted bytewise, the selector ring's fingerprint, and each cell's ID and fingerprint in ID order. Epochs never appear, and the mode byte leaves room for Across mode.
- Vectors: four cases appended (a directory with a catch-all, a ring only, a directory and a ring with whole-cell outages, and a nested `Cells<Cells<Ring>>` with mixed strategies). The lock gained `53 f7051ba6e0dd9c3e`. The 49 frozen cases are unchanged, regenerating the file reproduces it byte for byte, and the Python reference matches every case.
- Statistics: with distinct seeds, a vnode cell's balance gives χ² = 3.66 against a limit of 45.97, the same verdict as a standalone ring. The shared-seed control gives χ² = 8625.
- Benchmark (100 cells × 100 nodes, R = 3): 2.3–3.5 µs per `replicas` lookup against the 5 µs target, where a flat 10,000-node rendezvous ring takes 90 µs. These were measured on a busy machine, so re-measure before the release.
- `tests/alloc_free.rs` also checks that cell lookups (`replicas`, `primary`, `cell_of`, and `replicas_where`) don't allocate on a two-level tree.
- `CellId` has serde support like the other labels. Serde for `Directory` moved to FR-021: byte keys need a JSON representation, which belongs with a `CellsSpec`.
