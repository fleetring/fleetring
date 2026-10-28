---
id: FR-005
title: Fixed-point −log₂ and exact score comparison
status: done
priority: P0
size: M
milestone: M1 Hashing and model
depends_on: [FR-001]
adrs: [ADR-0003]
---

# FR-005: Fixed-point −log₂ and exact score comparison

## Goal

Implement the integer-only arithmetic from ADR-0003 as a small, crate-private module with exhaustive tests. It must be bit-identical on every platform.

## Scope

- **`math` module (`pub(crate)`):**
  - `neg_log2_q32(x: u64) -> u64` computes `L(x) = −log₂((x + 1) / 2⁶⁴)` in Q32 fixed point. Handle `x = u64::MAX` (where `x + 1` overflows) explicitly or through `u128`.
  - A score type with a total `Ord`. It compares `L_a · w_b` against `L_b · w_a` in `u128`, then the raw draw (larger wins), then `NodeId` (bytewise; smaller wins).
  - `ceil_div_u128`, used by the bounded-load capacity formula (FR-011).
- **Module lint:** enable `#![warn(clippy::arithmetic_side_effects)]` in the module.
- **Choose the algorithm.** Implement both candidates from ADR-0003:
  - repeated squaring (the baseline);
  - a 1025-entry table with linear interpolation.

  Micro-benchmark both with divan and estimate the cost of a lookup at N = 100 and N = 1000. Pick one using the ADR rule and delete the other. Record the numbers in the PR, and amend ADR-0003 point 4 to state the outcome.

## Acceptance criteria

- [x] Boundary values:
  - `neg_log2_q32(u64::MAX) == 0`
  - `neg_log2_q32(0) == 64 << 32`
  - whenever `x + 1 == 2^k`, the result is exactly `(64 − k) << 32`.
- [x] **Monotone:** `x ≤ y` implies `L(x) ≥ L(y)`. Check this with a strided sweep of 2²⁰ inputs plus proptest pairs.
- [x] **Accurate:** results are within 740 Q32 units (1.7 × 10⁻⁷) of an `f64` reference computed in tests. That is the table's interpolation bound; the squaring baseline would have achieved 1 unit. Floats are allowed in tests. *(The bound changed when the table was chosen; see ADR-0003, point 4.)*
- [x] The score comparator is a total order: antisymmetric, transitive (proptest over triples), and consistent with the rule that equal weights compare by raw draw (ADR-0003 point 6).
- [x] Nothing overflows for any input: run proptest in debug mode, where overflow panics, with extreme weights and draws.
- [ ] Emit `x → L(x)` vectors for the spec kit (FR-014).

## Notes

- Floats are banned in this module and the rest of the library (`clippy::float_arithmetic`). Only `#[cfg(test)]` code may use them.

## Progress

- 2026-10-27: Done. Measured in release mode on an Apple M-series machine: repeated squaring took 105.6 ns per call, and the 1025-entry table with linear interpolation took 2.4 ns. Squaring would put a 100-node lookup at about 10 µs, roughly 5× the budget, so the **table** is pinned (ADR-0003, point 4, records the details).
- A `const fn` (48-bit repeated squaring, rounded) builds the table at compile time. A test proves each entry is correctly rounded: every entry is within 0.5 of the f64 value, and none sits within 2⁻¹⁴ units of a rounding boundary.
- Error is at most 740 Q32 units, and the measured worst case nearly reaches the theoretical 738. `L` is exact at powers of two and monotone across the full domain.
- Deviations: no `ceil_div_u128` helper, because `u128::div_ceil` covers it (FR-011). `Score.index` is a `usize`.
