//! The harness's seeded random number generator.

/// A small, seeded generator (SplitMix64) for workloads and fault schedules.
///
/// The algorithm is fixed here rather than taken from a crate, so a seed replays the same run
/// after any dependency upgrade. It is not cryptographic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SimRng {
    state: u64,
}

impl SimRng {
    /// Returns a generator seeded with `seed`.
    #[must_use]
    pub const fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    /// Returns the next 64 random bits.
    pub fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Returns a number in `0..bound`, or 0 if `bound` is 0.
    ///
    /// It multiplies and keeps the high bits, so the bias is below `bound / 2^64`.
    pub fn below(&mut self, bound: u64) -> u64 {
        let wide = u128::from(self.next_u64()) * u128::from(bound);
        u64::try_from(wide >> 64).unwrap_or(0)
    }

    /// Returns an index into a slice of `len` items, or 0 if `len` is 0.
    pub fn index(&mut self, len: usize) -> usize {
        let len = u64::try_from(len).unwrap_or(u64::MAX);
        usize::try_from(self.below(len)).unwrap_or(0)
    }

    /// Returns `true` with probability `numerator / denominator`.
    pub fn chance(&mut self, numerator: u64, denominator: u64) -> bool {
        self.below(denominator) < numerator
    }

    /// Returns a new generator whose sequence is independent of this one's.
    pub fn fork(&mut self) -> Self {
        Self::new(self.next_u64())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_seed_gives_the_same_sequence() {
        let mut a = SimRng::new(7);
        let mut b = SimRng::new(7);
        let first: Vec<u64> = (0..8).map(|_| a.next_u64()).collect();
        let second: Vec<u64> = (0..8).map(|_| b.next_u64()).collect();
        assert_eq!(first, second);
        // SplitMix64's published first output for seed 0, so the algorithm can't drift.
        assert_eq!(SimRng::new(0).next_u64(), 0xE220_A839_7B1D_CDAF);
    }

    #[test]
    fn below_stays_in_range() {
        let mut rng = SimRng::new(1);
        for bound in [1_u64, 2, 3, 10, 1_000] {
            for _ in 0..100 {
                assert!(rng.below(bound) < bound, "bound {bound}");
            }
        }
        assert_eq!(rng.below(0), 0);
    }
}
