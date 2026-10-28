---
id: FR-014
title: Placement spec v1, golden vectors, and an independent reference check
status: done
priority: P1
size: L
milestone: M3 Change and interop
depends_on: [FR-002, FR-010, FR-011, FR-013]
adrs: [ADR-0002, ADR-0003, ADR-0004, ADR-0006, ADR-0007, ADR-0008]
---

# FR-014: Placement spec v1, golden vectors, and an independent reference check

## Goal

Make placement verifiable across platforms, crate versions, and languages, and prove the spec is complete by implementing it a second time in another language (ADR-0008).

## Scope

- **`docs/spec/placement-v1.md`** is normative and uses RFC 2119 keywords. Its sections:
  - conventions and encodings
  - XXH3-64 with a seed
  - node and key hashes
  - the rendezvous draw, and fixed-point −log₂ with pseudocode and reference values
  - the comparator and tie-breaks
  - vnode tokens and the ring walk
  - diversity passes, replica sets, and the prefix property
  - filter semantics
  - bounded-load capacity and saturation
  - the canonical encoding
  - the fingerprint and probe keys
  - versioning rules
- **`crates/fleetring/tests/vectors/placement-v1.json`**
  - Hasher vectors (from FR-003) and −log₂ vectors (from FR-005).
  - At least 40 placement cases, covering:
    - a single node
    - equal weights, and skewed weights (1 vs 1000)
    - vnodes with a small `tokens_per_weight`
    - zones <, =, and > R
    - shared hosts
    - seed 0 and a non-zero seed
    - empty keys, non-UTF-8 keys, and 1 KiB keys
    - liveness filters
    - bounded-load views, including saturation
- **Generator:** an `#[ignore]` test in `src/vectors.rs`, which can read private items such as the log table. Run `FLEETRING_WRITE_VECTORS=1 cargo test -p fleetring --all-features --lib vectors -- --ignored`. It only appends new cases and refuses to change existing ones; without the variable, it fails if the file is stale.
- **Conformance test:** embeds the vector file with `include_str!`, so it runs on WASI and emulated targets that have no filesystem access.
- **Freeze guard:** `placement-v1.lock` holds append-only checkpoints, `<count> <XXH3-64 digest>`, computed over the canonical form of the reference sections plus the first `count` cases. A test fails if any frozen case changes.
- **Independent reference:** `scripts/placement_ref.py` is a minimal implementation written only from the spec, using the PyPI `xxhash` package and plain integer math. A small CI job runs it against the vector file.

## Acceptance criteria

- [x] The Python reference passes every vector. This is the proof that the spec is complete and portable, and it lowers the risk for the v0.2 TypeScript and Python SDKs.
- [ ] The vectors pass on every CI target: Linux, macOS, Windows, i686, s390x, and wasm32-wasip1. *(They pass on aarch64-apple-darwin, and are embedded with `include_str!` for the other targets. Waiting on FR-002's first CI run.)*
- [x] Changing an existing expected value fails CI.
- [x] The spec header says `Status: Draft` until FR-019 changes it to `Final`.

## Progress

- 2026-10-28: Done. A subagent built it in an isolated copy, and I merged and re-verified it.
  - **Vectors:** 49 cases (30 rendezvous, 19 vnodes) with 892 key entries, 16 filter scenarios, and 25 bounded-load scenarios (300 placements, 108 of them saturated). The file also includes the 39 XXH3 vectors, all 1025 table entries, and 65 `L(x)` pairs. It even pins an exact rendezvous tie that a brute-force search found.
  - **Python reference:** `scripts/placement_ref.py` was written from the spec text and passes every vector. The new `spec-reference` CI job runs it.
- Spec fix found by the Python reference: §9 said a placement is saturated when it has fewer than R members, but the implementation uses r = min(R, N). The spec and ADR-0007 now say r, and `home` and N are defined. Rust behavior is unchanged.
- Freeze lock: `49 eef9c811be4bedef`.
