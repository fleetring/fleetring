---
id: FR-014
title: Placement spec v1, golden vectors, and an independent reference check
status: todo
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
- **Generator:** `cargo xtask vectors`, in a new `crates/xtask` with `publish = false` and an alias in `.cargo/config.toml`. It only appends new cases and refuses to change existing ones.
- **Conformance test:** embeds the vector file with `include_str!`, so it runs on WASI and emulated targets that have no filesystem access.
- **Freeze guard:** a test fails if any existing case's expected output changes. Check it against a committed SHA-256 of the canonicalized existing cases.
- **Independent reference:** `scripts/placement_ref.py` is a minimal implementation written only from the spec, using the PyPI `xxhash` package and plain integer math. A small CI job runs it against the vector file.

## Acceptance criteria

- [ ] The Python reference passes every vector. This is the proof that the spec is complete and portable, and it lowers the risk for the v0.2 TypeScript and Python SDKs.
- [ ] The vectors pass on every CI target: Linux, macOS, Windows, i686, s390x, and wasm32-wasip1.
- [ ] Changing an existing expected value fails CI.
- [ ] The spec header says `Status: Draft` until FR-019 changes it to `Final`.
