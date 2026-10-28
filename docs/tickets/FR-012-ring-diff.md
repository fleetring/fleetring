---
id: FR-012
title: Ring diff and movement summary for migration planning
status: done
priority: P1
size: M
milestone: M3 Change and interop
depends_on: [FR-009]
adrs: [ADR-0001, ADR-0006]
---

# FR-012: Ring diff and movement summary for migration planning

## Goal

Give `fleetringd` (live migration, MVP step 3) and the CLI (`node add --dry-run`) an exact, deterministic list of which databases move when the ring changes.

## Scope

- **`Ring::diff(&self, target: &Ring<H>, keys)`** takes `keys: impl IntoIterator<Item = K>` where `K: AsRef<[u8]>`. It returns an iterator that yields a `Change` only for keys whose replica set, or the order within it, changed.
- **`Change`** provides:
  - `key()`, `from()`, `to()`, where `from()` and `to()` return `ReplicaSet`s;
  - `added()` and `removed()`, which iterate over `&Node`;
  - `primary_changed()`, `is_reorder_only()`.
- **`MovementSummary`** can be built from changes or a diff.
  - **Counts:** keys examined, keys changed, primary changes, replicas added and removed per `NodeId`.
  - **Determinism:** the per-node maps are `BTreeMap`s so output order is stable.
  - **Display:** renders as a table.
- **`Ring::estimate_movement(&self, target, samples: NonZeroU32) -> MovementSummary`** estimates movement from a fixed internal synthetic corpus, for dry runs where no key list is available.
- **Efficiency:** each key is hashed once when both rings share a seed.

## Acceptance criteria

- [x] Unchanged keys are never yielded, and changed keys always are. Cross-check against a naive comparison of `replicas()` on both rings.
- [x] Adding one node:
  - every `Change` adds exactly that node and removes at most one node;
  - with equal weights and a single zone, the fraction of keys that change is within 5σ of R/(N+1).
- [x] Removing one node changes only keys whose replica set contained it.
- [x] `estimate_movement` is deterministic and lands within 1 percentage point of the exact diff over the 1M-key corpus.
- [x] Throughput is at least 100k keys/s at N = 100, single-threaded in release. Record the number in FR-016.

## Progress

- 2026-10-28: Done. A subagent built it in an isolated copy, and I merged and re-verified it.
  - **API:** `Ring::diff` returns a lazy `Diff`; `Diff::summarize` produces a `MovementSummary`. The other pieces are `MovementSummary::from_changes`, `Change` (`added`, `removed`, `primary_changed`, `is_reorder_only`), and `Ring::estimate_movement`.
- Measured:
  - Adding one node to 10 equal nodes at R = 3 changes 26.995% of keys, against a theoretical 27.273%.
  - The estimate lands within 0.24 percentage points of the exact diff.
  - Throughput is 350k keys/s at N = 100 (divan bench).
- Deviation: no "hash once" optimization. Hashing a key costs nanoseconds, while placing it costs a draw per node, and the two rings may hash differently.
