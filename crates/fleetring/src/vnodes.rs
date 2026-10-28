//! The weighted virtual-node ring, the optional strategy (ADR-0004).
//!
//! Node `i` owns `weight × tokens_per_weight` tokens, and token `j` sits at
//! `H(le64(node_hash) ‖ le32(j), seed)` on a 64-bit circle. A key's candidate order walks
//! clockwise from its hash, taking each node at its first token. A node's tokens depend only on
//! its own identity and weight, so adding, removing, or reweighting a node only inserts,
//! deletes, or moves that node in every key's order.

use smallvec::SmallVec;

use crate::StableHasher;
use crate::select::{CandidateOrder, Group, Members, Picks};

/// The most tokens a ring may hold. At 12 bytes per token that is 48 MiB, which bounds memory
/// and build time for very large or heavily weighted rings.
pub(crate) const MAX_TOKENS: u64 = 1 << 22;

/// Returns how many tokens the members need at `tokens_per_weight`, saturating rather than
/// overflowing so callers can compare it with [`MAX_TOKENS`].
pub(crate) fn token_count(members: &Members<'_>, tokens_per_weight: u32) -> u64 {
    members.entries.iter().fold(0u64, |total, member| {
        total.saturating_add(u64::from(member.weight).saturating_mul(u64::from(tokens_per_weight)))
    })
}

/// A ring's tokens in clockwise order, with the node that owns each.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TokenTable {
    tokens: Box<[u64]>,
    owners: Box<[u32]>,
}

impl TokenTable {
    /// Builds the table. The caller has checked [`token_count`] against [`MAX_TOKENS`].
    pub(crate) fn build<H: StableHasher>(
        members: &Members<'_>,
        hasher: &H,
        seed: u64,
        tokens_per_weight: u32,
    ) -> Self {
        let total = token_count(members, tokens_per_weight);
        assert!(
            total <= MAX_TOKENS,
            "{total} tokens exceed the {MAX_TOKENS} limit build() enforces"
        );
        let mut entries: Vec<(u64, u32)> =
            Vec::with_capacity(usize::try_from(total).expect("at most 2²² tokens"));
        for (index, member) in members.entries.iter().enumerate() {
            let owner = u32::try_from(index).expect("fewer nodes than tokens");
            let count = u32::try_from(u64::from(member.weight) * u64::from(tokens_per_weight))
                .expect("at most 2²² tokens per node");
            let mut input = [0u8; 12];
            input[..8].copy_from_slice(&member.node_hash.to_le_bytes());
            for j in 0..count {
                input[8..].copy_from_slice(&j.to_le_bytes());
                entries.push((hasher.hash(&input, seed), owner));
            }
        }
        // Ties between tokens go to the lower index, i.e. the smaller NodeId. The order is
        // total, so an unstable sort is deterministic.
        entries.sort_unstable();
        let (tokens, owners): (Vec<u64>, Vec<u32>) = entries.into_iter().unzip();
        Self {
            tokens: tokens.into_boxed_slice(),
            owners: owners.into_boxed_slice(),
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.tokens.len()
    }

    /// Returns each node's arc of the circle: the total length of the arcs ending at its
    /// tokens. A key goes to the first token at or after its hash, so a node's share of primaries
    /// is exactly its arc over 2⁶⁴. The arcs of `nodes` nodes sum to 2⁶⁴.
    pub(crate) fn ownership(&self, nodes: usize) -> Vec<u128> {
        let mut owned = vec![0u128; nodes];
        let (Some(&first), Some(&last)) = (self.tokens.first(), self.tokens.last()) else {
            return owned;
        };
        for (position, &token) in self.tokens.iter().enumerate() {
            // Tokens are sorted, so only the first token's arc wraps: it runs from the last
            // token around to the first, which is the whole circle when every token is equal.
            let arc = match position.checked_sub(1) {
                Some(previous) => u128::from(token - self.tokens[previous]),
                None => (1 << 64) - u128::from(last - first),
            };
            let owner = usize::try_from(self.owners[position]).expect("node indices fit in usize");
            owned[owner] += arc;
        }
        owned
    }

    /// Owners of every token, clockwise from the first token at or after `key_hash`.
    fn walk(&self, key_hash: u64) -> impl Iterator<Item = usize> + '_ {
        let start = self.tokens.partition_point(|&token| token < key_hash);
        let start = if start == self.len() { 0 } else { start };
        self.owners[start..]
            .iter()
            .chain(&self.owners[..start])
            .map(|&owner| usize::try_from(owner).expect("node indices fit in usize"))
    }
}

/// One key's walk around a vnode ring.
#[derive(Debug, Clone, Copy)]
pub(crate) struct VnodeWalk<'a> {
    members: Members<'a>,
    table: &'a TokenTable,
    key_hash: u64,
}

impl<'a> VnodeWalk<'a> {
    pub(crate) fn new(members: Members<'a>, table: &'a TokenTable, key_hash: u64) -> Self {
        Self {
            members,
            table,
            key_hash,
        }
    }
}

impl CandidateOrder for VnodeWalk<'_> {
    fn order<F: FnMut(usize) -> bool>(&self, mut eligible: F) -> Vec<usize> {
        let mut seen = vec![false; self.members.len()];
        let mut order = Vec::with_capacity(self.members.len());
        for node in self.table.walk(self.key_hash) {
            if !seen[node] {
                seen[node] = true;
                if eligible(node) {
                    order.push(node);
                }
            }
        }
        order
    }

    fn extend_distinct<F: FnMut(usize) -> bool>(
        &self,
        picks: &mut Picks,
        k: usize,
        group: Group,
        mut eligible: F,
    ) {
        // A node owns many tokens; remember rejections so `eligible` runs once per node.
        let mut rejected: SmallVec<[usize; 8]> = SmallVec::new();
        let mut added = 0;
        for node in self.table.walk(self.key_hash) {
            if added == k {
                break;
            }
            if picks.contains(&node) || rejected.contains(&node) {
                continue;
            }
            let value = self.members.group(group, node);
            if group != Group::Node && picks.iter().any(|&p| self.members.group(group, p) == value)
            {
                continue;
            }
            if !eligible(node) {
                rejected.push(node);
                continue;
            }
            picks.push(node);
            added += 1;
        }
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;
    use crate::Xxh3;
    use crate::select::testing::{self, Fixture, StrategyUnderTest};
    use crate::select::{Member, select};
    use crate::test_util::TableHasher;

    /// Few tokens per weight unit, so walks often meet the same node several times.
    const TOKENS_PER_WEIGHT: u32 = 4;

    struct UnderTest;

    impl UnderTest {
        fn table(fixture: &Fixture) -> TokenTable {
            TokenTable::build(&fixture.members(), &Xxh3, 7, TOKENS_PER_WEIGHT)
        }
    }

    impl StrategyUnderTest for UnderTest {
        fn order(
            &self,
            fixture: &Fixture,
            key_hash: u64,
            live: &dyn Fn(usize) -> bool,
        ) -> Vec<usize> {
            let table = Self::table(fixture);
            VnodeWalk::new(fixture.members(), &table, key_hash).order(live)
        }

        fn select(
            &self,
            fixture: &Fixture,
            key_hash: u64,
            r: usize,
            live: &dyn Fn(usize) -> bool,
        ) -> Vec<usize> {
            let table = Self::table(fixture);
            select(
                &VnodeWalk::new(fixture.members(), &table, key_hash),
                r,
                live,
            )
            .to_vec()
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

    /// A fixture from `(node_hash, weight, zone, host)` tuples.
    fn fixture(nodes: &[(u64, u32, u32, u32)]) -> Fixture {
        Fixture {
            entries: nodes
                .iter()
                .map(|&(node_hash, weight, zone, host)| Member {
                    node_hash,
                    weight,
                    zone,
                    host,
                })
                .collect(),
        }
    }

    fn fixture_with_weight(fixture: &Fixture, weight: u32) -> Fixture {
        let mut heavier = fixture.clone();
        for member in &mut heavier.entries {
            member.weight = weight;
        }
        heavier
    }

    /// Two single-token nodes, `a` (index 0) and `b` (index 1), at the given positions.
    fn two_tokens(a_token: u64, b_token: u64) -> (Fixture, TokenTable) {
        let fixture = fixture(&[(1, 1, 0, 0), (2, 1, 0, 1)]);
        let token_input = |node_hash: u64| {
            let mut input = [0u8; 12];
            input[..8].copy_from_slice(&node_hash.to_le_bytes());
            input
        };
        let hasher = TableHasher::new()
            .with(token_input(1), a_token)
            .with(token_input(2), b_token);
        let table = TokenTable::build(&fixture.members(), &hasher, 0, 1);
        (fixture, table)
    }

    fn walk_order(fixture: &Fixture, table: &TokenTable, key_hash: u64) -> Vec<usize> {
        VnodeWalk::new(fixture.members(), table, key_hash).order(|_| true)
    }

    #[test]
    #[expect(clippy::cast_precision_loss, reason = "statistics in a test")]
    fn primaries_match_each_nodes_arc_of_the_circle() {
        // 12 equal nodes with 64 tokens each. A token owns the arc that ends at it, and a key
        // goes to the first token at or after its hash, so a node's primary share is exactly
        // its total arc length over 2⁶⁴.
        let nodes: Vec<(u64, u32, u32, u32)> = (0u32..12)
            .map(|i| (Xxh3.hash(&i.to_le_bytes(), 0), 1, 0, i))
            .collect();
        let fixture = fixture(&nodes);
        let table = TokenTable::build(&fixture.members(), &Xxh3, 7, 64);
        let owned = table.ownership(12);
        assert_eq!(owned.iter().sum::<u128>(), 1 << 64, "arcs tile the circle");

        let samples = 200_000u32;
        let mut counts = [0u64; 12];
        let mut key_hash = 0x9e37_79b9_7f4a_7c15_u64;
        for _ in 0..samples {
            key_hash = key_hash.wrapping_mul(0xbf58_476d_1ce4_e5b9).rotate_left(31)
                ^ 0x94d0_49bb_1331_11eb;
            let primary = VnodeWalk::new(fixture.members(), &table, key_hash).order(|_| true)[0];
            counts[primary] += 1;
        }
        for (node, (&count, &arc)) in counts.iter().zip(&owned).enumerate() {
            let share = arc as f64 / 2f64.powi(64);
            let expected = f64::from(samples) * share;
            let sigma = (expected * (1.0 - share)).sqrt();
            let z = (count as f64 - expected) / sigma;
            assert!(
                z.abs() <= 5.0,
                "node {node}: {count} vs {expected:.0}, z = {z:.2}"
            );
        }
    }

    #[test]
    fn walks_clockwise_from_the_key_and_wraps() {
        let (fixture, table) = two_tokens(100, 200);
        assert_eq!(
            walk_order(&fixture, &table, 150),
            [1, 0],
            "b's token is next"
        );
        assert_eq!(
            walk_order(&fixture, &table, 100),
            [0, 1],
            "a token at the key counts"
        );
        assert_eq!(
            walk_order(&fixture, &table, 250),
            [0, 1],
            "past the last token wraps"
        );
        assert_eq!(walk_order(&fixture, &table, 0), [0, 1]);
    }

    #[test]
    fn equal_tokens_go_to_the_smaller_node_id() {
        let (fixture, table) = two_tokens(500, 500);
        assert_eq!(walk_order(&fixture, &table, 500), [0, 1]);
        assert_eq!(
            walk_order(&fixture, &table, 501),
            [0, 1],
            "wraps to the tie"
        );
    }

    #[test]
    fn token_count_scales_with_weight_and_saturates() {
        let fixture = fixture(&[(1, 1, 0, 0), (2, 2, 0, 1), (3, 5, 0, 2)]);
        assert_eq!(token_count(&fixture.members(), 256), 8 * 256);
        assert_eq!(TokenTable::build(&fixture.members(), &Xxh3, 0, 3).len(), 24);
        let heavy = fixture_with_weight(&fixture, u32::MAX);
        assert_eq!(token_count(&heavy.members(), u32::MAX), u64::MAX);
    }
}
