//! The harness's seeded random number generator, [`SplitMix64`], shared with the control
//! group's runtime.

use crate::rng::SplitMix64;

/// The harness's name for [`SplitMix64`], kept so existing callers don't change.
pub type SimRng = SplitMix64;

/// Batons' secrets for a simulated router, from a seeded generator, so that a seed replays the
/// same batons (ADR-0020). Not unguessable: the simulation has no adversary.
#[derive(Debug)]
pub(crate) struct SimEntropy(std::sync::Mutex<SimRng>);

impl SimEntropy {
    /// Bytes from a generator seeded with `seed`.
    pub(crate) fn new(seed: u64) -> Self {
        Self(std::sync::Mutex::new(SimRng::new(seed)))
    }
}

impl crate::router::Entropy for SimEntropy {
    fn fill(&self, bytes: &mut [u8]) {
        let mut rng = self.0.lock().expect("entropy lock poisoned");
        for chunk in bytes.chunks_mut(8) {
            let word = rng.next_u64().to_le_bytes();
            chunk.copy_from_slice(&word[..chunk.len()]);
        }
    }
}
