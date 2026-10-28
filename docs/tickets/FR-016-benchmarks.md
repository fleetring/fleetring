---
id: FR-016
title: Benchmarks and performance budget
status: todo
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

- [ ] `cargo bench -p fleetring` runs every bench.
- [ ] `docs/perf.md` is filled in with measured numbers.
- [ ] Every budget is met, or ADR-0003/0004 is amended with the decision taken instead.
