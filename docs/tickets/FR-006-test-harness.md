---
id: FR-006
title: Deterministic test harness, key corpora, and balance statistics
status: todo
priority: P0
size: S
milestone: M1 Hashing and model
depends_on: [FR-004]
adrs: [ADR-0008]
---

# FR-006: Deterministic test harness, key corpora, and balance statistics

## Goal

Build shared fixtures so property, statistical, and benchmark code all test against the same inputs. Statistical tests must be deterministic and never flaky (ADR-0008, point 7).

## Scope

- **Location:** `crates/fleetring/tests/common/`. This is internal test code, not public API. Benches can include it with `#[path = "../tests/common/mod.rs"]`.
- **PRNG:** an in-tree splitmix64. `rand` is not allowed (ADR-0009).
- **Key corpora**, each generated at 10k, 100k, and 1M keys:
  - sequential `tenant:{:06}`;
  - structured `agent:{word}-{word}-{n}`;
  - UUID-shaped strings;
  - random binary keys of 0–256 bytes.
- **Membership generators**
  - Proptest strategies: N ∈ [1, 64], weights ∈ [1, 1000], zones ∈ [1, 6], and hosts that may be shared within a zone.
  - Named fixtures: a single node; a home lab (3 laptops with weights 1/2/4); three equal zones; skewed zones (10/10/2); and a 1000-node fleet with 8 zones.
- **Statistics helpers**
  - Per-node counts and expected shares from weights.
  - Maximum relative deviation, and a per-node 5σ check (σ = √(m·p·(1−p))).
  - Chi-squared statistic with a hard-coded table of critical values for p = 10⁻⁶, df ≤ 128. No stats crate.
- **Assertion helper:** `assert_balanced` produces readable failures showing node, expected count, actual count, and z-score.
- **Corpus size:** default `cargo test` runs use the 100k corpora. 1M-key runs are `#[ignore]` and run in the release-mode CI job (FR-002).

## Acceptance criteria

- [ ] The corpora are byte-identical on every platform. A checksum test proves it on s390x, i686, and wasm. *(The checksum test exists and passes on aarch64-apple-darwin. Other platforms wait on FR-002's first CI run.)*
- [ ] The statistics helpers are unit-tested on known inputs. For example, a perfectly uniform distribution gives χ² = 0, and a single skewed bin fails.
- [ ] Generators shrink to small, readable counterexamples.
- [ ] Commit proptest regression files (`proptest-regressions/`). *(None exist yet, because no property has failed. Failures under `tests/it` write to `crates/fleetring/tests/proptest-regressions/`.)*
