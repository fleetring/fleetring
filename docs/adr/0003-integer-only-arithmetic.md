# ADR-0003: Placement arithmetic uses integers only

- **Status:** Accepted (2026-10-27)
- **Date:** 2026-10-27
- **Related:** ADR-0002, ADR-0004, ADR-0007, ADR-0008; tickets FR-005, FR-008, FR-011

## Context

Weighted rendezvous hashing (ADR-0004) gives each node a draw `u` that is uniform on (0, 1), then picks the node that minimizes `−ln(u) / weight`. This works as an exponential race: node *i* wins with probability `wᵢ / Σw`, and when one node's weight or membership changes, only that node's keys move. Most implementations compute this with `f64::ln`.

Floating point breaks determinism here:

- **`ln` varies by platform and language.** Rust's `f64::ln` resolves to the platform libm or an LLVM intrinsic. glibc, musl, Apple's libm, the MSVC CRT, and WASM engines aren't required to round it correctly, and their results can differ in the last bit. JavaScript's `Math.log` is "implementation-approximated", and Python's `math.log` calls the C library.
- **One-ULP differences flip winners.** When two nodes' scores are nearly tied, a one-ULP difference changes which one wins. Across millions of keys and many clients, a few databases end up with clients that disagree on where they live.
- **Overflow depends on build mode.** Rust panics on integer overflow in debug builds and wraps in release builds. If placement math overflows, its result depends on how it was compiled.

Prior art: Ceph CRUSH's `straw2` bucket is weighted rendezvous hashing with a fixed-point `ln` built from lookup tables. That's why the Linux kernel client and userspace compute identical placements.

## Decision

1. **No floating point in placement.** `#![deny(clippy::float_arithmetic)]` applies to the `fleetring` library code. Statistics helpers in tests may use floats.
2. **Weights are integers.** `Weight` is a nonzero `u32`. Capacity formulas (ADR-0007) use integer ratios.
3. **Fixed-point draw transform.** For a draw `x: u64` (ADR-0004), compute `L(x) = −log₂((x + 1) / 2⁶⁴)` in Q32 fixed point: 32 fractional bits, range [0, 64·2³²], stored in a `u64`. `L` is non-increasing in `x` by construction. Using log₂ instead of ln scales every score by the same constant, so winners don't change.
4. **The algorithm is pinned.** The exact `L` algorithm is part of placement spec v1 (ADR-0008).
   - **Baseline:** bit-by-bit repeated squaring. It runs 32 iterations of a 64×64→128-bit multiply, takes about ten lines with no tables, and is monotone by construction.
   - **Alternative:** a 1025-entry Q32 table with linear interpolation, the approach Ceph uses.
   - **Choice:** FR-005 benchmarks both. If the baseline misses the FR-016 lookup budget by more than 2×, we switch to the table before the spec freezes at 0.1.0.
   - **Outcome (FR-005, 2026-10-27): the table.** On an Apple M-series machine in release mode, squaring took 105.6 ns per call and the table 2.4 ns. At 100 nodes, squaring alone would cost about 10 µs per lookup, roughly 5× the 2 µs budget. Spec v1 pins:
     - `T[j] = round(2³² · log₂(1 + j/1024))` for `j` in `0..=1024`;
     - `v = x + 1`, `e = ⌊log₂ v⌋`, and the Q1.63 mantissa `m = v · 2^(63 − e)`;
     - the top 10 fractional bits of `m` index `T`, and the low 53 bits interpolate linearly, rounding down;
     - `L = (64 − e) · 2³² − fraction`, and `L = 0` when `x = u64::MAX`.

     The worst-case error is 738 Q32 units (1.7 × 10⁻⁷). That's a smooth bias far below statistical noise. `L` is still exact at powers of two and still monotone. Repeated squaring stays in the code, but only to build `T` at compile time.
5. **Comparison is exact.** For a given key, node *a* precedes node *b* when `L_a · w_b < L_b · w_a`, computed in `u128`. The largest product is 2³⁸ · 2³², so this can't overflow, and it compares `L_a/w_a` with `L_b/w_b` exactly. Ties go to the larger raw draw `x`, then the smaller `NodeId` (bytewise). The result is a total order that's identical on every platform.
6. **Exact shortcuts are allowed.** Because `L` is monotone, nodes of equal weight compare by raw draw alone, with no logarithm. Implementations may use this and similar shortcuts, such as memoizing `L` or bounding by the integer part of log₂, only if their output is bit-identical to the definition. The golden vectors (ADR-0008) catch violations.
7. **Arithmetic can't overflow.** Placement arithmetic widens to `u128` or uses explicit `wrapping_*` or `checked_*` operations. The math module enables `clippy::arithmetic_side_effects`, and CI runs the tests in both debug and release builds.

## Alternatives considered

- **`f64::ln`:** rejected because it's nondeterministic across platforms and languages.
- **A pinned pure-Rust libm (the `libm` crate):** deterministic across Rust targets, but it ties placement to one vendored libm version, and every non-Rust port would have to reproduce musl's `log` bit for bit.
- **Integer weights by repetition:** a node of weight *w* draws *w* times and keeps its best draw. It's exact with no logarithms, but costs O(Σw) per lookup.
- **Unweighted rendezvous with duplicate nodes standing in for weight:** has the same cost problem, and the duplicate identities complicate keeping replicas distinct.

## Consequences

- About 50 lines of math plus exhaustive tests, and a spec section that ports must follow.
- Ports need 64×64→128-bit multiplication. In JavaScript that means BigInt or 32-bit limbs.
- Before shortcuts, a lookup costs a few tens of nanoseconds per node. Clusters where every node has the same weight skip logarithms entirely.
- CI can verify weighted placement bit for bit across platforms (ADR-0009).
