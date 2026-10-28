//! Integer-only placement arithmetic (ADR-0003).
//!
//! Weighted rendezvous needs `−ln(u)` for a uniform draw `u`. Floating-point logarithms differ
//! between platforms and languages in the last bit, which is enough to flip a near-tie, so this
//! module computes a fixed-point `−log₂` with integer operations only. Every function here is
//! part of placement spec v1: changing its output changes placement.

#![warn(clippy::arithmetic_side_effects)]

use std::cmp::Ordering;

/// Fractional bits in the fixed-point logarithm (Q32).
const FRAC_BITS: u32 = 32;

/// The logarithm table covers `[1, 2]` in steps of `2^-TABLE_BITS`.
const TABLE_BITS: u32 = 10;

/// Bits of the Q1.63 mantissa below the table index, used for interpolation.
const REST_BITS: u32 = 63 - TABLE_BITS;

const TABLE_LEN: usize = (1 << TABLE_BITS) + 1;

/// `LOG2_TABLE[j] = round(2³² · log₂(1 + j / 1024))` for `j` in `0..=1024`.
///
/// Built at compile time from a 48-bit repeated-squaring logarithm, then rounded to 32 bits. A
/// unit test checks that no entry sits near a rounding boundary, so every entry equals the
/// correctly rounded mathematical value and ports can recompute or copy the table.
const LOG2_TABLE: [u64; TABLE_LEN] = log2_table();

/// Returns `−log₂((x + 1) / 2⁶⁴)` in Q32 fixed point.
///
/// The result lies in `[0, 64 · 2³²]`: `x = u64::MAX` maps to 0 (the best possible draw) and
/// `x = 0` maps to `64 · 2³²`. It is non-increasing in `x`, exact when `x + 1` is a power of
/// two, and otherwise within 740 Q32 units (about 1.7 × 10⁻⁷) of the exact value.
///
/// Algorithm (placement spec v1): let `v = x + 1`, `e = ⌊log₂ v⌋`, and `m = v · 2^(63 − e)`,
/// the Q1.63 mantissa. Its top 10 fractional bits index the table and the remaining 53 bits
/// interpolate linearly between neighboring entries, rounding down.
#[expect(
    clippy::arithmetic_side_effects,
    reason = "e ≤ 63, the interpolated step is below the table step, and the fraction is below 2³²"
)]
pub(crate) fn neg_log2_q32(x: u64) -> u64 {
    // x + 1 = 2⁶⁴ has a logarithm of exactly 64.
    let Some(v) = x.checked_add(1) else {
        return 0;
    };
    let integer = u64::from(v.ilog2());
    let mantissa = v << v.leading_zeros();
    let index = usize::try_from((mantissa >> REST_BITS) & ((1 << TABLE_BITS) - 1))
        .expect("a 10-bit index fits in usize");
    let rest = mantissa & ((1 << REST_BITS) - 1);
    let (low, high) = (LOG2_TABLE[index], LOG2_TABLE[index + 1]);
    let step = (u128::from(high - low) * u128::from(rest)) >> REST_BITS;
    let fraction = low + u64::try_from(step).expect("the interpolated step is below 2³²");
    ((64 - integer) << FRAC_BITS) - fraction
}

#[expect(
    clippy::arithmetic_side_effects,
    reason = "1024 + j < 2¹¹ has a logarithm in [10, 11], so the subtraction cannot underflow"
)]
#[expect(
    clippy::cast_possible_truncation,
    reason = "a rounded Q32 logarithm of a value in [1, 2] is at most 2³²"
)]
const fn log2_table() -> [u64; TABLE_LEN] {
    let mut table = [0; TABLE_LEN];
    let mut j = 0;
    while j < TABLE_LEN {
        // log₂(1 + j/1024) = log₂(1024 + j) − 10, to 48 bits, rounded half up to 32 bits.
        let q48 = log2_fixed(1024 + j as u128, 48) - (10 << 48);
        table[j] = ((q48 + (1 << 15)) >> 16) as u64;
        j += 1;
    }
    table
}

/// Returns `⌊2^bits · log₂(v)⌋` for `1 ≤ v ≤ 2⁶⁴`, computed bit by bit with repeated squaring.
///
/// The mantissa `m = v / 2^⌊log₂ v⌋` lies in `[1, 2)`. Squaring it doubles its logarithm, so
/// each squaring shifts out one fractional bit of `log₂ m`: the bit is 1 exactly when `m² ≥ 2`,
/// in which case `m²` is halved back into `[1, 2)`. Accurate to about 2⁻⁶⁰, far finer than the
/// 48 bits the table needs.
#[expect(
    clippy::arithmetic_side_effects,
    reason = "m < 2⁶³ keeps m² < 2¹²⁶, and shifts are by constants or by leading_zeros ≤ 127"
)]
const fn log2_fixed(v: u128, bits: u32) -> u128 {
    // Q1.62: the leading one of `v` moves to bit 62.
    let mut mantissa = (v << v.leading_zeros()) >> 65;
    let mut fraction = 0;
    let mut i = 0;
    while i < bits {
        mantissa = (mantissa * mantissa) >> 62;
        fraction <<= 1;
        if mantissa >> 63 != 0 {
            fraction |= 1;
            mantissa >>= 1;
        }
        i += 1;
    }
    ((v.ilog2() as u128) << bits) | fraction
}

/// One node's rendezvous draw for one key, with everything needed to rank it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Score {
    /// The raw 64-bit draw.
    pub(crate) draw: u64,
    /// `neg_log2_q32(draw)`. Only read when the compared nodes' weights differ, so rings whose
    /// nodes all weigh the same may leave it at zero.
    pub(crate) neg_log2: u64,
    /// The node's weight.
    pub(crate) weight: u32,
    /// The node's position in canonical `NodeId` order.
    pub(crate) index: usize,
}

/// Ranks two scores for the same key: `Less` means `a` is preferred.
///
/// Compares `neg_log2 / weight` exactly by cross-multiplying, then prefers the larger raw draw,
/// then the smaller index (that is, the smaller `NodeId`). The result is a total order.
///
/// Equal weights skip the logarithm: `neg_log2` is non-increasing in `draw`, so comparing draws
/// gives exactly the same order.
#[expect(
    clippy::arithmetic_side_effects,
    reason = "neg_log2 ≤ 2³⁸ and weight < 2³², so each product is below 2⁷⁰"
)]
pub(crate) fn cmp_scores(a: &Score, b: &Score) -> Ordering {
    let by_rate = if a.weight == b.weight {
        Ordering::Equal
    } else {
        let a_rate = u128::from(a.neg_log2) * u128::from(b.weight);
        let b_rate = u128::from(b.neg_log2) * u128::from(a.weight);
        a_rate.cmp(&b_rate)
    };
    by_rate
        .then_with(|| b.draw.cmp(&a.draw))
        .then_with(|| a.index.cmp(&b.index))
}

#[cfg(test)]
#[expect(
    clippy::arithmetic_side_effects,
    reason = "tests compute on small, bounded values"
)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    const UNIT: u64 = 1 << FRAC_BITS;

    #[test]
    fn extremes_and_powers_of_two_are_exact() {
        assert_eq!(neg_log2_q32(u64::MAX), 0);
        assert_eq!(neg_log2_q32(0), 64 * UNIT);
        for k in 0..64u32 {
            let x = (1u64 << k) - 1;
            assert_eq!(neg_log2_q32(x), (64 - u64::from(k)) * UNIT, "x + 1 = 2^{k}");
        }
    }

    #[test]
    fn is_non_increasing_across_a_strided_sweep() {
        // 2²⁰ points spread over the whole domain, plus every value near both ends.
        // Step in u64: the stride doesn't fit in a 32-bit usize (i686, wasm32).
        let stride = u64::MAX / (1 << 20);
        let mut previous = neg_log2_q32(0);
        for x in (0..1u64 << 20).map(|step| step * stride) {
            let current = neg_log2_q32(x);
            assert!(
                current <= previous,
                "L({x}) = {current} > previous {previous}"
            );
            previous = current;
        }
        for x in (0..4096).chain(u64::MAX - 4096..=u64::MAX) {
            assert!(
                neg_log2_q32(x) >= neg_log2_q32(x.saturating_add(1)),
                "x = {x}"
            );
        }
    }

    #[test]
    #[expect(
        clippy::cast_precision_loss,
        reason = "f64 is only the test's reference"
    )]
    fn is_within_740_units_of_the_exact_value() {
        // Linear interpolation of the concave log₂ undershoots by at most h²/(8 ln 2) with
        // h = 2⁻¹⁰, which is 738 units; table rounding and flooring add at most 1.5 more.
        let mut x = 0x9e37_79b9_7f4a_7c15_u64;
        let mut worst = 0.0f64;
        for _ in 0..200_000 {
            x = x.rotate_left(17).wrapping_mul(0xbf58_476d_1ce4_e5b9) ^ 0x94d0_49bb_1331_11eb;
            // (x + 1) loses precision in f64, but only ~2⁻⁵³ relative: far below one Q32 unit.
            let exact = 64.0 - ((x as f64) + 1.0).log2();
            let got = neg_log2_q32(x) as f64 / UNIT as f64;
            let error = (got - exact) * UNIT as f64;
            assert!(
                (-1.0..=740.0).contains(&error),
                "x = {x}: error {error} units"
            );
            worst = worst.max(error);
        }
        assert!(
            worst > 600.0,
            "the interpolation bound should be nearly reached: {worst}"
        );
    }

    #[test]
    #[expect(
        clippy::cast_precision_loss,
        reason = "f64 is only the test's reference"
    )]
    fn table_holds_correctly_rounded_logarithms() {
        assert_eq!(LOG2_TABLE[0], 0);
        assert_eq!(LOG2_TABLE[TABLE_LEN - 1], UNIT);
        for j in 0..TABLE_LEN {
            if j > 0 {
                assert!(
                    LOG2_TABLE[j] > LOG2_TABLE[j - 1],
                    "table must increase at {j}"
                );
            }
            let exact = (1.0 + j as f64 / 1024.0).log2() * UNIT as f64;
            let gap = (LOG2_TABLE[j] as f64 - exact).abs();
            assert!(gap <= 0.5 + 1e-6, "entry {j}: {} vs {exact}", LOG2_TABLE[j]);
            // The 48-bit value must not sit near a rounding boundary, or the rounded entry
            // could depend on how precisely it was computed.
            let q48 = log2_fixed(1024 + j as u128, 48) - (10 << 48);
            let below_boundary = (q48 & 0xffff).abs_diff(0x8000);
            assert!(below_boundary > 2, "entry {j} is ambiguous: {q48:#x}");
        }
    }

    #[test]
    fn repeated_squaring_matches_powers_of_two_and_known_values() {
        assert_eq!(log2_fixed(1, 32), 0);
        assert_eq!(log2_fixed(1 << 64, 32), 64 << 32);
        // log₂(3) = 1.584962500721156…; 0.5849625007211561 × 2³² = 2512394809.983…
        assert_eq!(log2_fixed(3, 32), (1 << 32) | 2_512_394_809);
    }

    fn score(draw: u64, weight: u32, index: usize) -> Score {
        Score {
            draw,
            neg_log2: neg_log2_q32(draw),
            weight,
            index,
        }
    }

    fn any_score() -> impl Strategy<Value = Score> {
        // Small draw and weight ranges make ties, and therefore tie-breaks, common.
        let draw = prop_oneof![any::<u64>(), 0..4u64, (u64::MAX - 4)..=u64::MAX];
        let weight = prop_oneof![1..4u32, Just(u32::MAX), 1..=u32::MAX];
        (draw, weight, 0..4usize).prop_map(|(draw, weight, index)| score(draw, weight, index))
    }

    proptest! {
        #[test]
        fn neg_log2_is_monotone(a in any::<u64>(), b in any::<u64>()) {
            let (low, high) = if a <= b { (a, b) } else { (b, a) };
            prop_assert!(neg_log2_q32(low) >= neg_log2_q32(high));
        }

        #[test]
        fn ranking_is_a_total_order(a in any_score(), b in any_score(), c in any_score()) {
            prop_assert_eq!(cmp_scores(&a, &b), cmp_scores(&b, &a).reverse());
            prop_assert_eq!(cmp_scores(&a, &a), Ordering::Equal);
            if cmp_scores(&a, &b).is_le() && cmp_scores(&b, &c).is_le() {
                prop_assert!(cmp_scores(&a, &c).is_le(), "{a:?} <= {b:?} <= {c:?}");
            }
        }

        #[test]
        fn equal_weights_rank_by_draw_exactly_as_by_logarithm(
            a in any::<u64>(), b in any::<u64>(), weight in 1..=u32::MAX,
        ) {
            let (sa, sb) = (score(a, weight, 0), score(b, weight, 1));
            let by_logarithm = (u128::from(sa.neg_log2) * u128::from(weight))
                .cmp(&(u128::from(sb.neg_log2) * u128::from(weight)))
                .then_with(|| b.cmp(&a))
                .then_with(|| sa.index.cmp(&sb.index));
            prop_assert_eq!(cmp_scores(&sa, &sb), by_logarithm);
        }

        #[test]
        fn heavier_nodes_win_identical_draws(draw in 0..u64::MAX, light in 1..u32::MAX) {
            // A strictly heavier node with the same draw has a smaller rate, unless L = 0.
            let heavy = score(draw, light + 1, 1);
            let light = score(draw, light, 0);
            prop_assert_eq!(cmp_scores(&heavy, &light), Ordering::Less);
        }
    }
}
