//! The fixed seed the property tests draw their cases from.

use proptest::test_runner::{Config, RngSeed};

/// Any value; changing it changes the cases every property tests.
const SEED: u64 = 0x5EED_F1EE_7000_0001;

/// Returns `config` drawing its cases from [`SEED`], so that every run on a target tests the
/// same cases and a property never flakes. A seed given in `PROPTEST_RNG_SEED` still wins, to
/// explore other cases.
pub(crate) fn fixed_seed(config: Config) -> Config {
    let rng_seed = match config.rng_seed {
        RngSeed::Random => RngSeed::Fixed(SEED),
        chosen @ RngSeed::Fixed(_) => chosen,
    };
    Config { rng_seed, ..config }
}
