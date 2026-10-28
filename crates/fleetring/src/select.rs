//! Topology-aware replica selection (ADR-0006).
//!
//! A strategy supplies each key's candidate order (ADR-0004). Selection rearranges that order
//! in three passes: one node per zone, then one node per unused host, then everything else.
//! The result is the key's preference list, and its first R entries are the replica set.

use smallvec::SmallVec;

/// One node's placement inputs, derived when a ring is built.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Member {
    /// `H(id, seed)`, the input to each of the node's draws and tokens (ADR-0005).
    pub(crate) node_hash: u64,
    pub(crate) weight: u32,
    /// The node's zone among the ring's zones. Nodes without a zone share one index.
    pub(crate) zone: u32,
    /// The node's host. A node without a host label has an index of its own.
    pub(crate) host: u32,
}

/// A ring's members in canonical `NodeId` order, with the counts selection needs.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Members<'a> {
    pub(crate) entries: &'a [Member],
    pub(crate) zone_count: usize,
    pub(crate) host_count: usize,
    /// Whether every node has the same weight, which lets rendezvous skip logarithms.
    pub(crate) uniform_weights: bool,
}

impl Members<'_> {
    pub(crate) fn len(&self) -> usize {
        self.entries.len()
    }

    /// The value `group` keeps distinct for node `index`.
    pub(crate) fn group(&self, group: Group, index: usize) -> usize {
        match group {
            Group::Zone => slot(self.entries[index].zone),
            Group::Host => slot(self.entries[index].host),
            Group::Node => index,
        }
    }
}

fn slot(index: u32) -> usize {
    usize::try_from(index).expect("zone and host indices fit in usize")
}

/// What a selection pass keeps distinct.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Group {
    Zone,
    Host,
    Node,
}

/// Node indices picked for a replica set, best first. Stored inline up to eight replicas, so
/// typical lookups don't allocate.
pub(crate) type Picks = SmallVec<[usize; 8]>;

/// A strategy's preference order over nodes for one key (ADR-0004, point 5).
///
/// Adding a node to the ring may only insert that node into the order, removing one may only
/// delete it, and reweighting one may only move it. Replica selection relies on nothing else.
pub(crate) trait CandidateOrder {
    /// Returns every node `eligible` accepts, best first.
    fn order<F: FnMut(usize) -> bool>(&self, eligible: F) -> Vec<usize>;

    /// Appends up to `k` nodes to `picks`, best first, keeping only the best node of each
    /// `group` value. Skips nodes that are already picked, that share a `group` value with an
    /// existing pick, or that `eligible` rejects. Calls `eligible` at most once per node.
    ///
    /// The result equals scanning [`order`](Self::order) and taking each node whose group
    /// value is still unused, stopping after `k`.
    fn extend_distinct<F: FnMut(usize) -> bool>(
        &self,
        picks: &mut Picks,
        k: usize,
        group: Group,
        eligible: F,
    );
}

/// Returns up to `r` replicas: the first `r` entries of the key's preference list.
///
/// Calls `eligible` at most three times per node.
pub(crate) fn select<O, F>(order: &O, r: usize, mut eligible: F) -> Picks
where
    O: CandidateOrder,
    F: FnMut(usize) -> bool,
{
    let mut picks = Picks::new();
    for group in [Group::Zone, Group::Host, Group::Node] {
        let missing = r.saturating_sub(picks.len());
        if missing == 0 {
            break;
        }
        order.extend_distinct(&mut picks, missing, group, &mut eligible);
    }
    picks
}

/// Rearranges a candidate order into the full preference list with the three diversity passes.
pub(crate) fn preference(members: &Members<'_>, order: &[usize]) -> Vec<usize> {
    let mut list = Vec::with_capacity(order.len());
    let mut taken = vec![false; members.len()];
    let mut zone_used = vec![false; members.zone_count];
    let mut host_used = vec![false; members.host_count];

    for &node in order {
        let zone = members.group(Group::Zone, node);
        if !zone_used[zone] {
            zone_used[zone] = true;
            host_used[members.group(Group::Host, node)] = true;
            taken[node] = true;
            list.push(node);
        }
    }
    for &node in order {
        let host = members.group(Group::Host, node);
        if !taken[node] && !host_used[host] {
            host_used[host] = true;
            taken[node] = true;
            list.push(node);
        }
    }
    list.extend(order.iter().copied().filter(|&node| !taken[node]));
    list
}

/// Shared [`CandidateOrder::extend_distinct`] for strategies that score each node
/// independently: keeps a small sorted list of the best `k` group champions in one pass.
///
/// Correct because the list's worst entry only ever improves. If a node's group lost its
/// champion to eviction earlier, any later node of that group that beats the current worst
/// entry also beats that evicted champion.
pub(crate) fn extend_by_score<S, C, F>(
    members: &Members<'_>,
    picks: &mut Picks,
    k: usize,
    group: Group,
    mut eligible: F,
    mut score: impl FnMut(usize) -> S,
    mut better: C,
) where
    S: Copy,
    C: FnMut(&S, &S) -> bool,
    F: FnMut(usize) -> bool,
{
    if k == 0 {
        return;
    }
    // (score, group value, node), best first.
    let mut best: SmallVec<[(S, usize, usize); 8]> = SmallVec::new();
    for node in 0..members.len() {
        if picks.contains(&node) {
            continue;
        }
        let value = members.group(group, node);
        if group != Group::Node && picks.iter().any(|&p| members.group(group, p) == value) {
            continue;
        }
        if !eligible(node) {
            continue;
        }
        let candidate = score(node);
        let same_group = if group == Group::Node {
            None
        } else {
            best.iter().position(|&(_, v, _)| v == value)
        };
        let replace = match same_group {
            Some(position) => position,
            None if best.len() < k => {
                best.push((candidate, value, node));
                let last = best.len() - 1;
                sift_up(&mut best, last, &mut better);
                continue;
            }
            None => best.len() - 1,
        };
        if better(&candidate, &best[replace].0) {
            best[replace] = (candidate, value, node);
            sift_up(&mut best, replace, &mut better);
        }
    }
    picks.extend(best.into_iter().map(|(_, _, node)| node));
}

/// Moves the improved entry at `position` toward the front until the list is sorted again.
fn sift_up<S>(
    best: &mut [(S, usize, usize)],
    mut position: usize,
    better: &mut impl FnMut(&S, &S) -> bool,
) {
    while position > 0 && better(&best[position].0, &best[position - 1].0) {
        best.swap(position, position - 1);
        position -= 1;
    }
}

/// Fixtures and strategy-independent properties shared by every strategy's tests.
#[cfg(test)]
pub(crate) mod testing {
    use proptest::prelude::*;
    use proptest::test_runner::TestCaseResult;

    use super::{Member, Members, preference};

    /// Owned members that a test can borrow as [`Members`].
    #[derive(Debug, Clone)]
    pub(crate) struct Fixture {
        pub(crate) entries: Vec<Member>,
    }

    impl Fixture {
        pub(crate) fn members(&self) -> Members<'_> {
            let count = |index: fn(&Member) -> u32| {
                self.entries
                    .iter()
                    .map(|member| usize::try_from(index(member)).expect("small index") + 1)
                    .max()
                    .unwrap_or(0)
            };
            Members {
                entries: &self.entries,
                zone_count: count(|member| member.zone),
                host_count: count(|member| member.host),
                uniform_weights: self
                    .entries
                    .windows(2)
                    .all(|pair| pair[0].weight == pair[1].weight),
            }
        }

        pub(crate) fn len(&self) -> usize {
            self.entries.len()
        }

        /// Appends a node, which therefore sorts last in canonical order.
        pub(crate) fn with_node(&self, node: NewNode) -> Self {
            let mut next = self.clone();
            next.entries.push(Member {
                node_hash: node.node_hash,
                weight: node.weight,
                zone: node.zone,
                host: node.zone * 8 + node.host,
            });
            next
        }
    }

    /// A node to add to a [`Fixture`].
    #[derive(Debug, Clone, Copy)]
    pub(crate) struct NewNode {
        node_hash: u64,
        weight: u32,
        zone: u32,
        host: u32,
    }

    /// Up to 24 nodes in up to 4 zones. Hosts are zone-local (host `h` of zone `z` is
    /// `z * 8 + h`) and often shared. Weights are uniform or mixed.
    pub(crate) fn fixtures() -> impl Strategy<Value = Fixture> {
        (1usize..=24, 1u32..=4, any::<bool>()).prop_flat_map(|(n, zones, uniform)| {
            let weight = if uniform {
                Just(1u32).boxed()
            } else {
                (1u32..=8).boxed()
            };
            proptest::collection::vec((any::<u64>(), weight, 0..zones, 0u32..4), n).prop_map(
                |nodes| Fixture {
                    entries: nodes
                        .into_iter()
                        .map(|(node_hash, weight, zone, host)| Member {
                            node_hash,
                            weight,
                            zone,
                            host: zone * 8 + host,
                        })
                        .collect(),
                },
            )
        })
    }

    pub(crate) fn new_nodes() -> impl Strategy<Value = NewNode> {
        (any::<u64>(), 1u32..=8, 0u32..4, 0u32..4).prop_map(|(node_hash, weight, zone, host)| {
            NewNode {
                node_hash,
                weight,
                zone,
                host,
            }
        })
    }

    /// A strategy as the shared properties see it: candidate orders and selections.
    pub(crate) trait StrategyUnderTest {
        fn order(
            &self,
            fixture: &Fixture,
            key_hash: u64,
            live: &dyn Fn(usize) -> bool,
        ) -> Vec<usize>;

        fn select(
            &self,
            fixture: &Fixture,
            key_hash: u64,
            r: usize,
            live: &dyn Fn(usize) -> bool,
        ) -> Vec<usize>;
    }

    fn everyone(_: usize) -> bool {
        true
    }

    /// Selection equals the prefix of the three-pass preference list, with and without a filter.
    pub(crate) fn selection_is_preference_prefix(
        strategy: &impl StrategyUnderTest,
        fixture: &Fixture,
        key_hash: u64,
        mask: u32,
    ) -> TestCaseResult {
        let live = |node: usize| mask.rotate_left(u32::try_from(node).expect("small")) & 1 == 1;
        for filter in [&everyone as &dyn Fn(usize) -> bool, &live] {
            let list = preference(
                &fixture.members(),
                &strategy.order(fixture, key_hash, filter),
            );
            for r in 1..=fixture.len() + 1 {
                let picks = strategy.select(fixture, key_hash, r, filter);
                prop_assert_eq!(&picks[..], &list[..r.min(list.len())], "r = {}", r);
            }
        }
        Ok(())
    }

    /// The order is a permutation, and adding a node only inserts it.
    pub(crate) fn adding_a_node_only_inserts_it(
        strategy: &impl StrategyUnderTest,
        fixture: &Fixture,
        key_hash: u64,
        node: NewNode,
    ) -> TestCaseResult {
        let order = strategy.order(fixture, key_hash, &everyone);
        let mut sorted = order.clone();
        sorted.sort_unstable();
        prop_assert_eq!(sorted, (0..fixture.len()).collect::<Vec<_>>());

        let grown = fixture.with_node(node);
        let added = fixture.len();
        let without_added: Vec<usize> = strategy
            .order(&grown, key_hash, &everyone)
            .into_iter()
            .filter(|&n| n != added)
            .collect();
        prop_assert_eq!(without_added, order);
        Ok(())
    }

    /// Adding a node swaps at most one replica, the joiner is the new node, the primary moves
    /// only to the new node, and removing the node again restores the original set.
    pub(crate) fn adding_a_node_swaps_at_most_one_replica(
        strategy: &impl StrategyUnderTest,
        fixture: &Fixture,
        key_hash: u64,
        r: usize,
        node: NewNode,
    ) -> TestCaseResult {
        let grown = fixture.with_node(node);
        let added = fixture.len();
        let before = strategy.select(fixture, key_hash, r, &everyone);
        let after = strategy.select(&grown, key_hash, r, &everyone);
        let gained: Vec<usize> = after
            .iter()
            .copied()
            .filter(|n| !before.contains(n))
            .collect();
        let lost: Vec<usize> = before
            .iter()
            .copied()
            .filter(|n| !after.contains(n))
            .collect();
        prop_assert!(
            gained.len() <= 1 && lost.len() <= 1,
            "{before:?} -> {after:?}"
        );
        prop_assert!(
            gained.iter().all(|&n| n == added),
            "only the new node can join"
        );
        if before.first() != after.first() {
            prop_assert_eq!(
                after.first(),
                Some(&added),
                "primary moves only to the new node"
            );
        }
        // Filtering the new node out is the same as never adding it (liveness equivalence).
        let filtered = strategy.select(&grown, key_hash, r, &|n| n != added);
        prop_assert_eq!(filtered, before);
        Ok(())
    }

    /// Reweighting one node swaps at most one replica: a heavier node can only join, a
    /// lighter one can only leave.
    pub(crate) fn reweighting_swaps_at_most_one_replica(
        strategy: &impl StrategyUnderTest,
        fixture: &Fixture,
        key_hash: u64,
        r: usize,
        node: usize,
        weight: u32,
    ) -> TestCaseResult {
        let mut reweighted = fixture.clone();
        reweighted.entries[node].weight = weight;
        let before = strategy.select(fixture, key_hash, r, &everyone);
        let after = strategy.select(&reweighted, key_hash, r, &everyone);
        let gained: Vec<usize> = after
            .iter()
            .copied()
            .filter(|n| !before.contains(n))
            .collect();
        let lost: Vec<usize> = before
            .iter()
            .copied()
            .filter(|n| !after.contains(n))
            .collect();
        prop_assert!(
            gained.len() <= 1 && lost.len() <= 1,
            "{before:?} -> {after:?}"
        );
        match weight.cmp(&fixture.entries[node].weight) {
            std::cmp::Ordering::Greater => prop_assert!(gained.iter().all(|&n| n == node)),
            std::cmp::Ordering::Less => prop_assert!(lost.iter().all(|&n| n == node)),
            std::cmp::Ordering::Equal => prop_assert_eq!(&after, &before),
        }
        Ok(())
    }

    /// The first min(R, zones) replicas sit in distinct zones, and the set has min(R, N) nodes.
    pub(crate) fn first_replicas_cover_distinct_zones(
        strategy: &impl StrategyUnderTest,
        fixture: &Fixture,
        key_hash: u64,
        r: usize,
    ) -> TestCaseResult {
        let picks = strategy.select(fixture, key_hash, r, &everyone);
        prop_assert_eq!(picks.len(), r.min(fixture.len()));
        let mut zones: Vec<u32> = fixture.entries.iter().map(|member| member.zone).collect();
        zones.sort_unstable();
        zones.dedup();
        let spread = r.min(zones.len());
        let mut seen: Vec<u32> = picks[..spread]
            .iter()
            .map(|&n| fixture.entries[n].zone)
            .collect();
        seen.sort_unstable();
        seen.dedup();
        prop_assert_eq!(seen.len(), spread);
        Ok(())
    }
}
