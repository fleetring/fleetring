---
id: FR-016
title: Benchmarks and performance budget
status: done
priority: P1
size: S
milestone: M4 Hardening and release
depends_on: [FR-010, FR-012]
adrs: [ADR-0003, ADR-0004]
---

# FR-016: Benchmarks and performance budget

## Goal

Measure the hot paths, set budgets, and keep to them. Making rendezvous the default (O(N) per lookup, per ADR-0004) relies on these numbers.

## Scope

- **Divan benches**
  - `replicas`: both strategies × N ∈ {3, 10, 100, 1000} × R ∈ {1, 3}, with all-equal weights and with four weight classes.
  - `preference`: the full list.
  - `build`: N ∈ {10, 100, 1000}, with vnode rings up to 2²² tokens.
  - `diff`: 100k keys.
  - `neg_log2_q32`.
  - `LoadView::replicas`.
- **`docs/perf.md`:** the hardware used, a results table, the budgets, and whether each budget is met.
- **Initial budgets** (release builds on an Apple M-series machine and a GitHub x86 runner):

  | Path | Budget |
  |---|---|
  | rendezvous `replicas`, R = 3, N = 100 | ≤ 2 µs |
  | rendezvous `replicas`, R = 3, N = 1000 | ≤ 20 µs |
  | vnode `replicas`, R = 3, N = 1000 | ≤ 1 µs |
  | vnode `build`, N = 1000 × 256 tokens | ≤ 100 ms |
  | `diff`, N = 100 | ≥ 100k keys/s |

- **If rendezvous misses its budget**, apply FR-008's exactly-equivalent shortcuts first. If it's still more than 2× over, switch −log₂ to the table variant (ADR-0003). This has to happen before the spec freezes.
- **CI:** an optional smoke job runs the benches in quick mode. It doesn't gate on the numbers, because shared runners are too noisy.

## Acceptance criteria

- [x] `cargo bench -p fleetring` runs every bench.
- [x] `docs/perf.md` is filled in with measured numbers.
- [x] Every budget is met, or ADR-0003/0004 is amended with the decision taken instead.

## Progress

- 2026-10-27: `benches/lookup.rs` (divan) covers `replicas` (R = 3), `primary`, `preference`, and `build` for rendezvous with uniform and mixed weights and for vnodes, at N ∈ {3, 10, 100, 1000}. Results are in `docs/perf.md`, and every lookup and build budget is met:
  - rendezvous at N = 100: 1.55 µs, against a 2 µs budget;
  - rendezvous at N = 1000: 10.3 µs, against 20 µs;
  - vnodes at N = 1000: 0.42 µs, against 1 µs;
  - a vnode build at 1000 × 256: 19 ms, against 100 ms.
- Still open: benches for `diff` and bounded loads, after FR-011 and FR-012 merge. The divan bench for `neg_log2_q32` was replaced by FR-005's in-crate micro-benchmark, because the function is private.
- 2026-10-28: Added `diff` and bounded-load benches. Every budget is met, including `diff` at 350k keys/s at N = 100. `docs/perf.md` holds the full table.
