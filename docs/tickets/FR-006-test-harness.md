---
id: FR-006
title: Deterministic test harness, key corpora, and balance statistics
status: done
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

- **Location:** `crates/fleetring/tests/it/common/`. All integration tests are one binary (`tests/it/main.rs`). This is internal test code, not public API. Benches can include it with `#[path = "../tests/it/common/mod.rs"]`.
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

- [x] The corpora are byte-identical on every platform. A checksum test proves it on s390x, i686, and wasm. *(The checksum tests pass on every CI target in run 40640646430.)*
- [x] The statistics helpers are unit-tested on known inputs. For example, a perfectly uniform distribution gives χ² = 0, and a single skewed bin fails.
- [x] Generators shrink to small, readable counterexamples.
- [x] Commit proptest regression files (`proptest-regressions/`). *(None exist yet, because no property has failed. Failures under `tests/it` write to `crates/fleetring/tests/proptest-regressions/`.)*

## Progress

- 2026-10-27: Done (built by a subagent in an isolated copy, then merged and re-verified).
  - Modules: `prng`, `corpus` (`Corpus::ALL`, sizes `SMALL`, `MEDIUM`, `LARGE`), `fixtures` (with `memberships()` strategies), and `stats` (`tally`, `balance`/`Balance`, `assert_balanced` with a pinned failure format).
- Corpus checksums (first 10,000 keys) are pinned for the cross-platform CI check:
  - sequential: `0x8d0a1f588e046af3`
  - agents: `0x2a86b5a649795fe1`
  - uuids: `0x9afbf7d2faaa7b44`
  - binary: `0x9a6c67add988dcb0`

  A separate Python implementation reproduces all four corpora byte for byte.
- Deviations:
  - **Critical value:** χ² critical values use Wilson–Hilferty at z = 4.7534 instead of a table. It is slightly lenient at small df, by 15% at df 1, and has no df cap.
  - **`binary` corpus:** apart from one empty first key, every key is 1–256 bytes long. With uniform lengths, repeated empty keys would pile onto one node and fail the 5σ check.
  - **Checksum fold:** multiply-xor rather than rotate-xor.
- Checked on correct placements: ideal weighted placements of every corpus pass `assert_balanced` at 100k keys, and also at 1M in the release `--ignored` job.
