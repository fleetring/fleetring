//! SplitMix64, the PRNG behind the key corpora.
//!
//! The corpora must never change, so they can't use `rand`, whose algorithms may change between
//! releases (ADR-0009).

/// The increment between states: ⌊2⁶⁴/φ⌋, which is odd.
const GAMMA: u64 = 0x9e37_79b9_7f4a_7c15;

/// The first multiplier of the output function, David Stafford's "Mix13" variant of the
/// MurmurHash3 finalizer.
const MIX_1: u64 = 0xbf58_476d_1ce4_e5b9;

/// The second multiplier of the output function.
const MIX_2: u64 = 0x94d0_49bb_1331_11eb;

/// SplitMix64 (Steele, Lea, and Flood, 2014), as in Vigna's reference `splitmix64.c`.
///
/// Its output depends only on the seed, so a seed yields the same stream on every platform, Rust
/// release, and language.
#[derive(Debug, Clone)]
pub(crate) struct SplitMix64 {
    state: u64,
}

impl SplitMix64 {
    /// Creates a generator whose stream is fixed by `seed`.
    pub(crate) const fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    /// Returns the next 64 bits of the stream.
    pub(crate) fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(GAMMA);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(MIX_1);
        z = (z ^ (z >> 27)).wrapping_mul(MIX_2);
        z ^ (z >> 31)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matches_the_published_test_vector() {
        // Seed 1234567 is the test vector from Rosetta Code's SplitMix64 task.
        let mut rng = SplitMix64::new(1_234_567);
        let outputs: Vec<u64> = (0..5).map(|_| rng.next_u64()).collect();
        assert_eq!(
            outputs,
            [
                6_457_827_717_110_365_317,
                3_203_168_211_198_807_973,
                9_817_491_932_198_370_423,
                4_593_380_528_125_082_431,
                16_408_922_859_458_223_821,
            ]
        );
    }
}
