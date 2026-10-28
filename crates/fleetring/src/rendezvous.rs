//! Weighted rendezvous hashing, the default strategy (ADR-0003, ADR-0004).
//!
//! For each key, node `i` draws `x = H(le64(node_hash) ‖ le64(key_hash), seed)`, and nodes are
//! ranked by `neg_log2_q32(x) / weight`, compared exactly (see [`crate::math`]). A node's draw
//! never depends on the other nodes, so adding, removing, or reweighting one node can only
//! insert, delete, or move that node in every key's order.

use crate::StableHasher;
use crate::math::{Score, cmp_scores, neg_log2_q32};
use crate::select::{CandidateOrder, Group, Members, Picks, extend_by_score};

/// One key's rendezvous order over a ring's members.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Rendezvous<'a, H> {
    members: Members<'a>,
    hasher: &'a H,
    seed: u64,
    key_hash: u64,
}

impl<'a, H: StableHasher> Rendezvous<'a, H> {
    pub(crate) fn new(members: Members<'a>, hasher: &'a H, seed: u64, key_hash: u64) -> Self {
        Self {
            members,
            hasher,
            seed,
            key_hash,
        }
    }

    /// Node `index`'s score for this key.
    fn score(&self, index: usize) -> Score {
        let member = self.members.entries[index];
        let mut input = [0u8; 16];
        input[..8].copy_from_slice(&member.node_hash.to_le_bytes());
        input[8..].copy_from_slice(&self.key_hash.to_le_bytes());
        let draw = self.hasher.hash(&input, self.seed);
        // Equal weights rank by draw alone (ADR-0003, point 6), so skip the logarithm.
        let neg_log2 = if self.members.uniform_weights {
            0
        } else {
            neg_log2_q32(draw)
        };
        Score {
            draw,
            neg_log2,
            weight: member.weight,
            index,
        }
    }
}

impl<H: StableHasher> CandidateOrder for Rendezvous<'_, H> {
    fn order<F: FnMut(usize) -> bool>(&self, mut eligible: F) -> Vec<usize> {
        let mut scores: Vec<Score> = (0..self.members.len())
            .filter(|&index| eligible(index))
            .map(|index| self.score(index))
            .collect();
        // `cmp_scores` is a total order, so an unstable sort is deterministic.
        scores.sort_unstable_by(cmp_scores);
        scores.into_iter().map(|score| score.index).collect()
    }

    fn extend_distinct<F: FnMut(usize) -> bool>(
        &self,
        picks: &mut Picks,
        k: usize,
        group: Group,
        eligible: F,
    ) {
        extend_by_score(
            &self.members,
            picks,
            k,
            group,
            eligible,
            |index| self.score(index),
            |a, b| cmp_scores(a, b).is_lt(),
        );
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;
    use crate::Xxh3;
    use crate::select::select;
    use crate::select::testing::{self, Fixture, StrategyUnderTest};

    struct UnderTest;

    impl StrategyUnderTest for UnderTest {
        fn order(
            &self,
            fixture: &Fixture,
            key_hash: u64,
            live: &dyn Fn(usize) -> bool,
        ) -> Vec<usize> {
            Rendezvous::new(fixture.members(), &Xxh3, 7, key_hash).order(live)
        }

        fn select(
            &self,
            fixture: &Fixture,
            key_hash: u64,
            r: usize,
            live: &dyn Fn(usize) -> bool,
        ) -> Vec<usize> {
            let order = Rendezvous::new(fixture.members(), &Xxh3, 7, key_hash);
            select(&order, r, live).to_vec()
        }
    }

    proptest! {
        #[test]
        fn selection_is_preference_prefix(
            fixture in testing::fixtures(), key_hash in any::<u64>(), mask in any::<u32>(),
        ) {
            testing::selection_is_preference_prefix(&UnderTest, &fixture, key_hash, mask)?;
        }

        #[test]
        fn adding_a_node_only_inserts_it(
            fixture in testing::fixtures(), key_hash in any::<u64>(), node in testing::new_nodes(),
        ) {
            testing::adding_a_node_only_inserts_it(&UnderTest, &fixture, key_hash, node)?;
        }

        #[test]
        fn adding_a_node_swaps_at_most_one_replica(
            fixture in testing::fixtures(), key_hash in any::<u64>(), r in 1usize..=5,
            node in testing::new_nodes(),
        ) {
            testing::adding_a_node_swaps_at_most_one_replica(&UnderTest, &fixture, key_hash, r, node)?;
        }

        #[test]
        fn reweighting_swaps_at_most_one_replica(
            fixture in testing::fixtures(), key_hash in any::<u64>(), r in 1usize..=5,
            node in any::<prop::sample::Index>(), weight in 1u32..=8,
        ) {
            let node = node.index(fixture.len());
            testing::reweighting_swaps_at_most_one_replica(&UnderTest, &fixture, key_hash, r, node, weight)?;
        }

        #[test]
        fn first_replicas_cover_distinct_zones(
            fixture in testing::fixtures(), key_hash in any::<u64>(), r in 1usize..=6,
        ) {
            testing::first_replicas_cover_distinct_zones(&UnderTest, &fixture, key_hash, r)?;
        }
    }
}
