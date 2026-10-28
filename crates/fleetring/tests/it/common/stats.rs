//! Balance statistics for placement tests.
//!
//! A balance test counts how many keys of a fixed corpus land on each node, then checks the
//! counts against the node weights two ways: every node's z-score must stay within ±5σ, and
//! Pearson's χ² over all nodes must stay within its critical value at p = 10⁻⁶. The first check
//! catches one badly placed node, the second a small bias spread over many nodes. Floats are fine
//! here: only placement code must be integer-only (ADR-0003).

use std::fmt;

/// The largest |z| a node's count may show. A correct placement exceeds it with probability
/// about 6 × 10⁻⁷ per node.
const SIGMA_LIMIT: f64 = 5.0;

/// Φ⁻¹(1 − 10⁻⁶), the standard normal quantile at the χ² check's significance level.
const Z_CRITICAL: f64 = 4.753_424_308_817_086;

/// The fewest keys any node may expect. Below this, the normal and χ² approximations that both
/// checks rely on break down.
const MIN_EXPECTED: f64 = 5.0;

/// One node's observed and expected key counts.
#[derive(Debug, Clone, Copy)]
struct Bin {
    observed: f64,
    expected: f64,
    share: f64,
}

impl Bin {
    /// Returns how many standard deviations the count lies from expectation, where the standard
    /// deviation is the binomial σ = √(m·p·(1 − p)).
    fn z(self) -> f64 {
        let sigma = (self.expected * (1.0 - self.share)).sqrt();
        // A lone node expects every key, so its count matches exactly and σ is 0.
        if sigma > 0.0 {
            (self.observed - self.expected) / sigma
        } else {
            0.0
        }
    }
}

/// The result of a balance check: the worst node and the χ² statistic, with their limits.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct Balance {
    /// The index of the node with the largest |z|.
    pub(crate) worst_node: usize,
    /// The worst node's expected key count.
    pub(crate) expected: f64,
    /// The worst node's actual key count.
    pub(crate) actual: u64,
    /// The worst node's z-score.
    pub(crate) z: f64,
    /// Pearson's χ² over all nodes.
    pub(crate) chi_squared: f64,
    /// The degrees of freedom: one fewer than the number of nodes.
    pub(crate) df: u32,
    /// The χ² critical value for `df` at p = 10⁻⁶; see [`chi_squared_critical`].
    pub(crate) chi_squared_limit: f64,
}

impl Balance {
    /// Returns whether every node's count is within ±5σ of its expected count.
    pub(crate) fn within_sigma_limit(&self) -> bool {
        self.z.abs() <= SIGMA_LIMIT
    }

    /// Returns whether χ² is within its critical value.
    pub(crate) fn within_chi_squared_limit(&self) -> bool {
        self.chi_squared <= self.chi_squared_limit
    }

    /// Returns whether the counts pass both checks.
    pub(crate) fn is_balanced(&self) -> bool {
        self.within_sigma_limit() && self.within_chi_squared_limit()
    }
}

impl fmt::Display for Balance {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let verdict = |passes| if passes { "passes" } else { "fails" };
        let overall = if self.is_balanced() {
            "balanced"
        } else {
            "unbalanced"
        };
        writeln!(f, "{overall}")?;
        writeln!(
            f,
            "  worst node: index {}, expected {:.1} keys, got {}, z = {:+.2} ({} |z| <= {SIGMA_LIMIT})",
            self.worst_node,
            self.expected,
            self.actual,
            self.z,
            verdict(self.within_sigma_limit()),
        )?;
        write!(
            f,
            "  chi-squared: {:.2} with {} degrees of freedom ({} <= {:.2} at p = 1e-6)",
            self.chi_squared,
            self.df,
            verdict(self.within_chi_squared_limit()),
            self.chi_squared_limit,
        )
    }
}

/// Counts how often each index in `0..bins` occurs, such as the node each key lands on.
///
/// # Panics
///
/// Panics if an index is out of range.
pub(crate) fn tally(bins: usize, indices: impl IntoIterator<Item = usize>) -> Vec<u64> {
    let mut counts = vec![0; bins];
    for index in indices {
        let count = counts
            .get_mut(index)
            .unwrap_or_else(|| panic!("index {index} is out of range for {bins} bins"));
        *count += 1;
    }
    counts
}

/// Returns each node's expected share of keys, wᵢ / Σw.
///
/// # Panics
///
/// Panics if `weights` is empty or contains a zero.
pub(crate) fn expected_shares(weights: &[u32]) -> Vec<f64> {
    assert!(!weights.is_empty(), "there are no weights");
    if let Some(node) = weights.iter().position(|&weight| weight == 0) {
        panic!("node {node} has weight 0");
    }
    // Each partial sum is an integer below 2⁵³, so the total is exact.
    let total: f64 = weights.iter().copied().map(f64::from).sum();
    weights
        .iter()
        .map(|&weight| f64::from(weight) / total)
        .collect()
}

/// Returns Pearson's χ² statistic, Σ (Oᵢ − Eᵢ)² / Eᵢ, of the counts against the expected shares.
///
/// # Panics
///
/// Panics if the slices differ in length or no keys were counted.
pub(crate) fn chi_squared(observed: &[u64], expected_shares: &[f64]) -> f64 {
    bins(observed, expected_shares)
        .map(|bin| {
            let deviation = bin.observed - bin.expected;
            deviation * deviation / bin.expected
        })
        .sum()
}

/// Returns the χ² value that a correct placement exceeds with probability 10⁻⁶ at `df` degrees of
/// freedom.
///
/// Uses the Wilson–Hilferty approximation k·(1 − 2/(9k) + z·√(2/(9k)))³ with z = Φ⁻¹(1 − 10⁻⁶).
/// It overestimates the exact quantile, by 15% at k = 1, 3.3% at k = 7, and under 0.3% from
/// k = 63 on, so the check's false-alarm rate stays at or below 10⁻⁶. With 0 degrees of freedom,
/// χ² is always 0 and so is its critical value.
pub(crate) fn chi_squared_critical(df: u32) -> f64 {
    if df == 0 {
        return 0.0;
    }
    let k = f64::from(df);
    let a = 2.0 / (9.0 * k);
    let t = 1.0 - a + Z_CRITICAL * a.sqrt();
    k * t * t * t
}

/// Returns the largest relative deviation of any node's count from its expected count,
/// |Oᵢ − Eᵢ| / Eᵢ.
///
/// # Panics
///
/// Panics if the slices differ in length or no keys were counted.
pub(crate) fn max_relative_deviation(observed: &[u64], expected_shares: &[f64]) -> f64 {
    bins(observed, expected_shares)
        .map(|bin| (bin.observed - bin.expected).abs() / bin.expected)
        .fold(0.0, f64::max)
}

/// Returns each node's z-score: how many standard deviations its count lies from expectation,
/// with σᵢ = √(m·pᵢ·(1 − pᵢ)) for m keys.
///
/// # Panics
///
/// Panics if the slices differ in length or no keys were counted.
pub(crate) fn z_scores(observed: &[u64], expected_shares: &[f64]) -> Vec<f64> {
    bins(observed, expected_shares).map(Bin::z).collect()
}

/// Checks per-node key counts against expected shares, with the per-node 5σ check and the χ²
/// check at p = 10⁻⁶.
///
/// Use it directly when the shares don't come from weights, for example vnode arc lengths.
///
/// # Panics
///
/// Panics if the slices differ in length, the shares don't sum to 1, or any node expects fewer
/// than 5 keys.
pub(crate) fn balance(observed: &[u64], expected_shares: &[f64]) -> Balance {
    let share_sum: f64 = expected_shares.iter().sum();
    assert!(
        (share_sum - 1.0).abs() < 1e-9,
        "expected shares sum to {share_sum}, not 1"
    );
    let bins: Vec<Bin> = bins(observed, expected_shares).collect();
    if let Some((node, bin)) = bins
        .iter()
        .enumerate()
        .find(|(_, bin)| bin.expected < MIN_EXPECTED)
    {
        panic!(
            "node {node} expects {:.2} keys; use enough keys that every node expects at least {MIN_EXPECTED}",
            bin.expected
        );
    }
    let (worst_node, worst) = bins
        .iter()
        .enumerate()
        .max_by(|(_, a), (_, b)| a.z().abs().total_cmp(&b.z().abs()))
        .expect("bins() rejects empty input");
    let df = u32::try_from(bins.len() - 1).expect("node counts fit in u32");
    Balance {
        worst_node,
        expected: worst.expected,
        actual: observed[worst_node],
        z: worst.z(),
        chi_squared: chi_squared(observed, expected_shares),
        df,
        chi_squared_limit: chi_squared_critical(df),
    }
}

/// Asserts that per-node key counts are proportional to the node weights.
///
/// `label` names the case in the failure message, for example `"homelab/uuids"`. The message gives
/// the worst node's index, expected and actual counts, and z-score, plus χ² against its limit.
///
/// # Panics
///
/// Panics if a node's count is more than 5σ from its expected count or χ² exceeds its critical
/// value at p = 10⁻⁶, and on the inputs that [`expected_shares`] and [`balance`] reject.
#[track_caller]
pub(crate) fn assert_balanced(counts: &[u64], weights: &[u32], label: &str) {
    let balance = balance(counts, &expected_shares(weights));
    assert!(balance.is_balanced(), "{label}: {balance}");
}

/// Pairs each node's count with its expected count under `expected_shares`.
///
/// # Panics
///
/// Panics if the slices differ in length or no keys were counted.
fn bins(observed: &[u64], expected_shares: &[f64]) -> impl Iterator<Item = Bin> {
    assert_eq!(
        observed.len(),
        expected_shares.len(),
        "there must be one expected share per count"
    );
    let total = to_f64(observed.iter().sum());
    assert!(total > 0.0, "no keys were counted");
    observed
        .iter()
        .zip(expected_shares)
        .map(move |(&count, &share)| Bin {
            observed: to_f64(count),
            expected: total * share,
            share,
        })
}

/// Converts a key count to `f64` exactly; counts in these tests stay far below 2³².
fn to_f64(count: u64) -> f64 {
    f64::from(u32::try_from(count).expect("key counts in these tests fit in u32"))
}

#[cfg(test)]
mod tests {
    use fleetring::{StableHasher, Xxh3};

    use super::*;
    use crate::common::corpus::{self, Corpus};
    use crate::common::fixtures;

    /// Exact χ² quantiles at p = 10⁻⁶ as `(df, quantile)`, as `scipy.stats.chi2.isf(1e-6, df)`
    /// computes them.
    const EXACT_CRITICAL: [(u32, f64); 7] = [
        (1, 23.928_127),
        (2, 27.631_021),
        (7, 40.521_831),
        (15, 56.493_442),
        (63, 131.369_702),
        (127, 217.610_274),
        (999, 1_226.046_189),
    ];

    /// Places `hash` on a node with probability proportional to its weight, as an ideal placement
    /// would. `cumulative` holds the running totals of the weights.
    fn ideal_node(hash: u64, cumulative: &[u64]) -> usize {
        let total = *cumulative.last().expect("at least one node");
        let point = (u128::from(hash) * u128::from(total)) >> 64;
        cumulative.partition_point(|&end| u128::from(end) <= point)
    }

    /// Asserts that an ideal placement of the first `keys` keys of every corpus is balanced on the
    /// named fixtures, so the checks accept correct placements of these corpora.
    fn assert_ideal_placement_is_balanced(keys: usize) {
        let memberships = [
            ("single", fixtures::single()),
            ("homelab", fixtures::homelab()),
            ("skewed_zones", fixtures::skewed_zones()),
            ("fleet_1000", fixtures::fleet_1000()),
        ]
        .map(|(name, nodes)| {
            let weights = fixtures::weights(&nodes);
            let cumulative: Vec<u64> = weights
                .iter()
                .scan(0, |total, &weight| {
                    *total += u64::from(weight);
                    Some(*total)
                })
                .collect();
            (name, weights, cumulative)
        });
        for corpus in Corpus::ALL {
            let hashes: Vec<u64> = corpus.keys(keys).map(|key| Xxh3.hash(&key, 0)).collect();
            for (name, weights, cumulative) in &memberships {
                let counts = tally(
                    weights.len(),
                    hashes.iter().map(|&hash| ideal_node(hash, cumulative)),
                );
                assert_balanced(&counts, weights, &format!("{name}/{corpus}"));
            }
        }
    }

    #[test]
    fn tally_counts_each_index() {
        assert_eq!(tally(3, [0, 2, 2, 0, 2]), [2, 0, 3]);
        assert_eq!(tally(2, []), [0, 0]);
    }

    #[test]
    fn expected_shares_are_proportional_to_weight() {
        assert_eq!(expected_shares(&[1, 1, 2, 4]), [0.125, 0.125, 0.25, 0.5]);
        let shares = expected_shares(&[1, 2, 4]);
        assert!(
            (shares.iter().sum::<f64>() - 1.0).abs() < 1e-15,
            "{shares:?}"
        );
    }

    #[test]
    fn perfectly_proportional_counts_have_zero_chi_squared_and_pass() {
        let weights = [1, 1, 2, 4];
        let counts = [1_000, 1_000, 2_000, 4_000];
        let shares = expected_shares(&weights);
        assert_eq!(chi_squared(&counts, &shares), 0.0);
        assert_eq!(max_relative_deviation(&counts, &shares), 0.0);
        assert_eq!(z_scores(&counts, &shares), [0.0; 4]);
        assert!(balance(&counts, &shares).is_balanced());
        assert_balanced(&counts, &weights, "perfect");
    }

    #[test]
    fn a_single_skewed_node_fails_with_a_readable_report() {
        let mut counts = [10_000; 10];
        counts[3] = 11_000;
        let balance = balance(&counts, &expected_shares(&[1; 10]));
        assert!(!balance.within_sigma_limit() && !balance.within_chi_squared_limit());
        assert!(!balance.is_balanced());
        assert_eq!(
            balance.to_string(),
            "unbalanced\n  \
             worst node: index 3, expected 10100.0 keys, got 11000, z = +9.44 (fails |z| <= 5)\n  \
             chi-squared: 89.11 with 9 degrees of freedom (fails <= 45.97 at p = 1e-6)"
        );
    }

    #[test]
    fn chi_squared_catches_a_small_bias_spread_over_many_nodes() {
        // 100 equal nodes, each about 3.8σ off: no node stands out, but together they're skewed.
        let counts: Vec<u64> = (0..100)
            .map(|i| if i % 2 == 0 { 10_380 } else { 9_620 })
            .collect();
        let balance = balance(&counts, &expected_shares(&[1; 100]));
        assert!(balance.within_sigma_limit(), "{balance}");
        assert!(!balance.within_chi_squared_limit(), "{balance}");
        assert!(!balance.is_balanced());
    }

    #[test]
    fn the_sigma_check_catches_one_outlier_among_many_nodes() {
        // 1000 equal nodes expecting 1000 keys each (σ ≈ 31.6); node 0 is 6σ high and 190 others
        // are one key short. χ² rises by only about 36, far below its limit of 1226.
        let mut counts = vec![1_000; 1_000];
        counts[0] = 1_190;
        for count in &mut counts[1..=190] {
            *count -= 1;
        }
        let balance = balance(&counts, &expected_shares(&[1; 1_000]));
        assert_eq!(balance.worst_node, 0);
        assert!(!balance.within_sigma_limit(), "{balance}");
        assert!(balance.within_chi_squared_limit(), "{balance}");
        assert!(!balance.is_balanced());
    }

    #[test]
    fn a_single_node_is_balanced() {
        let balance = balance(&[42], &expected_shares(&[3]));
        assert!(balance.is_balanced(), "{balance}");
        assert_eq!((balance.z, balance.chi_squared, balance.df), (0.0, 0.0, 0));
    }

    #[test]
    fn z_scores_and_relative_deviation_follow_the_binomial_model() {
        let counts = [1_050, 950];
        let shares = expected_shares(&[1, 1]);
        // m = 2000 keys and p = 1/2, so σ = √(2000 · 1/2 · 1/2) = √500.
        let want = 50.0 / 500_f64.sqrt();
        let z = z_scores(&counts, &shares);
        assert!(
            (z[0] - want).abs() < 1e-12 && (z[1] + want).abs() < 1e-12,
            "{z:?}"
        );
        let deviation = max_relative_deviation(&counts, &shares);
        assert!((deviation - 0.05).abs() < 1e-12, "{deviation}");
    }

    #[test]
    fn critical_values_bound_the_exact_quantiles_from_above() {
        for (df, exact) in EXACT_CRITICAL {
            let approximate = chi_squared_critical(df);
            let excess = approximate / exact - 1.0;
            let allowed = if df < 63 { 0.15 } else { 0.003 };
            assert!(
                (0.0..allowed).contains(&excess),
                "df = {df}: {approximate} against exact {exact}"
            );
        }
        assert_eq!(chi_squared_critical(0), 0.0);
    }

    #[test]
    fn ideal_placement_of_every_corpus_is_balanced() {
        assert_ideal_placement_is_balanced(corpus::MEDIUM);
    }

    #[test]
    #[ignore = "1M keys per corpus; the release-mode statistical CI job runs it"]
    fn ideal_placement_of_every_large_corpus_is_balanced() {
        assert_ideal_placement_is_balanced(corpus::LARGE);
    }
}
