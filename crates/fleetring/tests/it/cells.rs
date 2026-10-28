//! Cells: nested placement through the public API (FR-020, ADR-0012).

use std::collections::BTreeSet;
use std::error::Error;
use std::num::NonZeroU32;

use fleetring::{
    BuildCellsError, CellId, CellPlacement, Cells, Directory, Epoch, Fingerprint, Node, Placement,
    ReplicationFactor, Ring, Strategy, VnodeParams, Weight,
};
use proptest::prelude::*;
use proptest::sample::Index;
use proptest::strategy::Strategy as _;

use crate::common::corpus::{Corpus, MEDIUM};
use crate::common::stats::{balance, tally};

type TestResult = Result<(), Box<dyn Error>>;

/// Keys for the quick checks: agent names, then sequential tenant names.
fn keys(n: usize) -> Vec<Vec<u8>> {
    Corpus::Agents
        .keys(n / 2)
        .chain(Corpus::Sequential.keys(n - n / 2))
        .collect()
}

fn cell(id: &str) -> CellId {
    id.parse().expect("test cell ids are valid")
}

fn weighted(id: &str, weight: u32) -> Node {
    Node::new(id.parse().expect("test ids are valid"))
        .with_weight(Weight::new(weight).expect("test weights are nonzero"))
}

fn vnodes(tokens_per_weight: u32) -> Strategy {
    let tokens = NonZeroU32::new(tokens_per_weight).expect("tokens per weight is nonzero");
    Strategy::Vnodes(VnodeParams::new(tokens))
}

/// A cell's ring: `zones` zones of `per_zone` weight-1 nodes, named `{prefix}-n1` onward so that
/// node IDs stay unique across cells.
fn cell_ring(prefix: &str, seed: u64, zones: usize, per_zone: usize) -> Ring {
    let nodes = (0..zones * per_zone).map(|i| {
        weighted(&format!("{prefix}-n{}", i + 1), 1).with_zone(
            format!("{prefix}-z{}", i / per_zone + 1)
                .parse()
                .expect("test zones are valid"),
        )
    });
    Ring::builder()
        .seed(seed)
        .nodes(nodes)
        .build()
        .expect("test rings are valid")
}

/// A selector ring whose nodes are the cells `cells`, with their weights.
fn selector(strategy: Strategy, seed: u64, cells: &[(&str, u32)]) -> Ring {
    Ring::builder()
        .strategy(strategy)
        .seed(seed)
        .nodes(cells.iter().map(|&(id, weight)| weighted(id, weight)))
        .build()
        .expect("test selector rings are valid")
}

/// A placement's path, as IDs.
fn path(placed: &CellPlacement<'_>) -> Vec<String> {
    placed.path().iter().map(|cell| cell.to_string()).collect()
}

/// Cells `a`, `b`, and `c`, each a ring of three zones of two nodes, picked by a rendezvous
/// selector ring with weights 1, 2, and 3, after a rule that pins `pinned:` keys to `a`.
fn three_cells() -> Result<Cells, BuildCellsError> {
    Cells::builder()
        .directory(Directory::new().prefix("pinned:", cell("a")))
        .ring_selector(selector(
            Strategy::Rendezvous,
            100,
            &[("a", 1), ("b", 2), ("c", 3)],
        ))
        .cell(cell("c"), cell_ring("c", 3, 3, 2))
        .cell(cell("a"), cell_ring("a", 1, 3, 2))
        .cell(cell("b"), cell_ring("b", 2, 3, 2))
        .build()
}

#[test]
fn every_replica_of_a_key_lands_in_its_cell_and_spreads_across_its_zones() -> TestResult {
    let fleet = three_cells()?;
    let mut cells_used = BTreeSet::new();
    for key in keys(2_000) {
        let placed = fleet.replicas(&key);
        let [leaf] = placed.path() else {
            panic!("{placed}: one layer gives one cell");
        };
        cells_used.insert(leaf.as_str());
        let ring = fleet.cell(leaf).ok_or("the path names a cell")?;
        assert_eq!(placed.replicas(), &ring.replicas(&key), "{placed}");
        assert!(
            placed
                .replicas()
                .iter()
                .all(|node| node.id().as_str().starts_with(&format!("{leaf}-"))),
            "{placed}: every replica is in cell {leaf}"
        );
        let zones: BTreeSet<_> = placed.replicas().iter().map(Node::zone).collect();
        assert_eq!(zones.len(), 3, "{placed}: three replicas in three zones");
        assert_eq!(fleet.cell_of(&key), *leaf);
        assert_eq!(fleet.primary(&key), placed.replicas().primary());
        assert!(fleet.preference(&key).eq(ring.preference(&key)), "{placed}");
    }
    assert_eq!(cells_used.len(), 3, "the keys reach every cell");
    Ok(())
}

#[test]
fn directory_rules_win_exact_first_then_the_longest_prefix_then_the_ring() -> TestResult {
    let ring = selector(
        Strategy::Rendezvous,
        100,
        &[("eu", 1), ("vip", 1), ("us", 1)],
    );
    let directory = Directory::new()
        .prefix("eu:", cell("eu"))
        .prefix("eu:vip:", cell("vip"))
        .exact("eu:vip:legacy", cell("eu"));
    let fleet = Cells::builder()
        .directory(directory)
        .ring_selector(ring.clone())
        .cell(cell("eu"), cell_ring("eu", 1, 1, 2))
        .cell(cell("vip"), cell_ring("vip", 2, 1, 2))
        .cell(cell("us"), cell_ring("us", 3, 1, 2))
        .build()?;
    for (key, want) in [
        ("eu:tenant:acme", "eu"),
        ("eu:vip:globex", "vip"),
        ("eu:vip:", "vip"),
        ("eu:vip", "eu"),
        ("eu:vip:legacy", "eu"),
        ("eu:vip:legacy:2", "vip"),
    ] {
        assert_eq!(fleet.cell_of(key).as_str(), want, "{key}");
    }
    // Rules win even where the ring would pick another cell, and every other key goes where the
    // ring's preferred primary says.
    let mut overridden = 0;
    for key in keys(500) {
        let mut prefixed = b"eu:".to_vec();
        prefixed.extend_from_slice(&key);
        assert_eq!(fleet.cell_of(&prefixed).as_str(), "eu");
        if ring.primary(&prefixed).id().as_str() != "eu" {
            overridden += 1;
        }
        assert_eq!(
            fleet.cell_of(&key).as_str(),
            ring.primary(&key).id().as_str()
        );
    }
    assert!(overridden > 0, "some rule overrides the ring");
    Ok(())
}

#[test]
fn a_directory_alone_needs_a_catch_all_and_sends_it_every_unmatched_key() -> TestResult {
    let build = |directory: Directory| {
        Cells::builder()
            .directory(directory)
            .cell(cell("eu"), cell_ring("eu", 1, 1, 2))
            .cell(cell("us"), cell_ring("us", 2, 1, 2))
            .build()
    };
    let rules = Directory::new()
        .prefix("eu:", cell("eu"))
        .exact("us", cell("us"));
    assert_eq!(
        build(rules.clone()).err(),
        Some(BuildCellsError::NoFallback)
    );
    let fleet = build(rules.catch_all(cell("us")))?;
    for key in keys(200) {
        assert_eq!(fleet.cell_of(&key).as_str(), "us");
    }
    assert_eq!(fleet.cell_of("eu:tenant").as_str(), "eu");
    assert_eq!(
        fleet.cell_of("EU:tenant").as_str(),
        "us",
        "rules are case-sensitive"
    );
    assert!(fleet.ring_selector().is_none());
    Ok(())
}

#[test]
fn filters_apply_inside_the_cell_and_never_move_a_key_to_another_cell() -> TestResult {
    let fleet = three_cells()?;
    for key in keys(300) {
        let home = fleet.replicas(&key);
        let leaf = home.cell().ok_or("a cell placement has a cell")?.clone();
        let in_cell = |node: &Node| node.id().as_str().starts_with(&format!("{leaf}-"));

        let other_cells_down = fleet
            .replicas_where(&key, |node| in_cell(node))
            .ok_or("the key's own cell is up")?;
        assert_eq!(other_cells_down, home, "nodes elsewhere don't matter");

        let primary = home.replicas().primary().id().clone();
        let failover = fleet
            .replicas_where(&key, |node| node.id() != &primary)
            .ok_or("five nodes of the cell are up")?;
        assert_eq!(failover.path(), home.path(), "failover stays in the cell");
        let ring = fleet.cell(&leaf).ok_or("the path names a cell")?;
        let expected = ring
            .replicas_where(&key, |node| node.id() != &primary)
            .ok_or("the ring has other nodes")?;
        assert_eq!(failover.replicas(), &expected);

        assert!(
            fleet.replicas_where(&key, |node| !in_cell(node)).is_none(),
            "a key whose cell is down has no replicas, though other cells are up"
        );
    }
    Ok(())
}

#[test]
fn changing_one_cells_weight_moves_keys_only_into_or_out_of_that_cell() -> TestResult {
    let keys = keys(2_000);
    for strategy in [Strategy::Rendezvous, vnodes(16)] {
        let build = |weight_of_b: u32| {
            Cells::builder()
                .ring_selector(selector(
                    strategy,
                    100,
                    &[("a", 2), ("b", weight_of_b), ("c", 2), ("d", 2)],
                ))
                .cell(cell("a"), cell_ring("a", 1, 3, 1))
                .cell(cell("b"), cell_ring("b", 2, 3, 1))
                .cell(cell("c"), cell_ring("c", 3, 3, 1))
                .cell(cell("d"), cell_ring("d", 4, 3, 1))
                .build()
        };
        let (current, heavier, lighter) = (build(2)?, build(3)?, build(1)?);

        let mut moved_in = 0;
        for change in current.diff(&heavier, &keys) {
            assert!(change.is_cross_cell(), "{strategy:?}: {change:?}");
            assert_eq!(
                change.to().cell(),
                Some(&cell("b")),
                "{strategy:?}: {change:?}"
            );
            assert_ne!(change.from().cell(), Some(&cell("b")), "{strategy:?}");
            assert!(change.primary_changed());
            assert_eq!(change.added().count(), change.to().replicas().len());
            assert_eq!(change.removed().count(), change.from().replicas().len());
            moved_in += 1;
        }
        let summary = current.diff(&heavier, &keys).summarize();
        assert!(moved_in > 0, "{strategy:?}: a heavier cell takes keys");
        assert_eq!(summary.keys_examined(), 2_000);
        assert_eq!(summary.keys_changed(), moved_in);
        assert_eq!(summary.cross_cell_moves(), moved_in);
        assert_eq!(summary.in_cell_changes(), 0);
        assert_eq!(summary.primary_changes(), moved_in);

        let summary = current.diff(&lighter, &keys).summarize();
        assert!(summary.cross_cell_moves() > 0, "{strategy:?}");
        assert_eq!(summary.in_cell_changes(), 0, "{strategy:?}");
        for change in current.diff(&lighter, &keys) {
            assert_eq!(
                change.from().cell(),
                Some(&cell("b")),
                "{strategy:?}: {change:?}"
            );
        }
    }
    Ok(())
}

#[test]
fn adding_a_node_to_a_cell_changes_keys_only_inside_that_cell() -> TestResult {
    let current = three_cells()?;
    let grown = current
        .cell("b")
        .ok_or("cell b exists")?
        .to_builder()
        .node(weighted("b-n7", 1).with_zone("b-z1".parse()?))
        .build()?;
    let target = current
        .to_builder()
        .remove_cell("b")
        .cell(cell("b"), grown)
        .build()?;
    let keys = keys(2_000);
    let changes: Vec<_> = current.diff(&target, &keys).collect();
    assert!(!changes.is_empty(), "the new node takes some replicas");
    for change in &changes {
        assert!(!change.is_cross_cell(), "{change:?}");
        assert_eq!(change.from().cell(), Some(&cell("b")));
        let added: Vec<&str> = change.added().map(|node| node.id().as_str()).collect();
        assert_eq!(added, ["b-n7"], "{change:?}");
        assert_eq!(change.removed().count(), 1, "{change:?}");
    }
    let summary = fleetring::CellMovementSummary::from_changes(&changes, 2_000);
    assert_eq!(summary, current.diff(&target, &keys).summarize());
    assert_eq!(summary.cross_cell_moves(), 0);
    assert_eq!(summary.in_cell_changes(), summary.keys_changed());
    Ok(())
}

/// A small membership for a leaf ring: one to five nodes with weights 1 to 4 in up to three
/// zones.
fn leaf_members() -> impl proptest::strategy::Strategy<Value = Vec<Node>> {
    proptest::collection::btree_map(0u8..32, (1u32..=4, 0u8..3), 1..=5).prop_map(|members| {
        members
            .into_iter()
            .map(|(n, (weight, zone))| {
                weighted(&format!("n{n}"), weight)
                    .with_zone(format!("z{zone}").parse().expect("valid zone"))
            })
            .collect()
    })
}

/// A leaf cell: its selector weight, whether its ring uses vnodes, and its membership.
type Leaf = (u32, bool, Vec<Node>);

/// A two-layer tree: up to three regions of up to three leaf cells, each region with its
/// weight in the top selector ring.
fn trees() -> impl proptest::strategy::Strategy<Value = Vec<(u32, Vec<Leaf>)>> {
    let leaf = (1u32..=4, any::<bool>(), leaf_members());
    let region = (1u32..=4, proptest::collection::vec(leaf, 1..=3));
    proptest::collection::vec(region, 1..=3)
}

/// A change to one node of a leaf ring's membership.
#[derive(Debug, Clone)]
enum Churn {
    Add { weight: u32, zone: u8 },
    Remove(Index),
    Reweight(Index, u32),
}

fn churn() -> impl proptest::strategy::Strategy<Value = Churn> {
    prop_oneof![
        (1u32..=4, 0u8..3).prop_map(|(weight, zone)| Churn::Add { weight, zone }),
        any::<Index>().prop_map(Churn::Remove),
        (any::<Index>(), 1u32..=8).prop_map(|(node, weight)| Churn::Reweight(node, weight)),
    ]
}

/// Builds a tree: the top layer pins `agent:brisk` keys to region `r0` and hashes the rest with
/// a selector ring under seed 1; region `r{i}` hashes keys to its cells `r{i}-c{j}` under seed
/// `2 + i`; and the cells' rings use seeds from 100 up, so every layer has its own seed.
fn build_tree(tree: &[(u32, Vec<Leaf>)], top_vnodes: bool) -> Result<Cells<Cells>, Box<dyn Error>> {
    let mut regions = Vec::new();
    for (i, (_, leaves)) in tree.iter().enumerate() {
        let ids: Vec<String> = (0..leaves.len()).map(|j| format!("r{i}-c{j}")).collect();
        let weights: Vec<(&str, u32)> = ids
            .iter()
            .zip(leaves)
            .map(|(id, (weight, ..))| (id.as_str(), *weight))
            .collect();
        let mut region = Cells::builder().ring_selector(selector(
            Strategy::Rendezvous,
            2 + u64::try_from(i)?,
            &weights,
        ));
        for (j, (id, (_, use_vnodes, nodes))) in ids.iter().zip(leaves).enumerate() {
            let ring = Ring::builder()
                .strategy(if *use_vnodes {
                    vnodes(16)
                } else {
                    Strategy::Rendezvous
                })
                .seed(100 + u64::try_from(10 * i + j)?)
                .nodes(nodes.iter().cloned())
                .build()?;
            region = region.cell(id.parse()?, ring);
        }
        regions.push((format!("r{i}"), region.build()?));
    }
    let weights: Vec<(&str, u32)> = regions
        .iter()
        .zip(tree)
        .map(|((id, _), (weight, _))| (id.as_str(), *weight))
        .collect();
    let strategy = if top_vnodes {
        vnodes(16)
    } else {
        Strategy::Rendezvous
    };
    let mut top = Cells::builder()
        .directory(Directory::new().prefix("agent:brisk", cell("r0")))
        .ring_selector(selector(strategy, 1, &weights));
    for (id, region) in regions {
        top = top.cell(id.parse()?, region);
    }
    Ok(top.build()?)
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    /// ADR-0012, point 3: cell weights are explicit, so no change inside a cell moves a key to
    /// another cell, at any depth.
    #[test]
    fn churn_inside_a_cell_never_changes_any_keys_path(
        tree in trees(),
        top_vnodes in any::<bool>(),
        region in any::<Index>(),
        leaf in any::<Index>(),
        churn in churn(),
    ) {
        let before = build_tree(&tree, top_vnodes).expect("valid tree");
        let mut changed = tree.clone();
        let leaves = &mut changed[region.index(tree.len())].1;
        let len = leaves.len();
        let nodes = &mut leaves[leaf.index(len)].2;
        match churn {
            Churn::Add { weight, zone } => nodes.push(
                weighted("added", weight).with_zone(format!("z{zone}").parse().expect("valid zone")),
            ),
            Churn::Remove(node) if nodes.len() > 1 => {
                let index = node.index(nodes.len());
                nodes.remove(index);
            }
            Churn::Remove(_) => return Ok(()),
            Churn::Reweight(node, weight) => {
                let index = node.index(nodes.len());
                nodes[index] = nodes[index].clone().with_weight(Weight::new(weight).expect("nonzero"));
            }
        }
        let after = build_tree(&changed, top_vnodes).expect("valid tree");
        for key in keys(64) {
            prop_assert_eq!(before.cell_of(&key), after.cell_of(&key));
            prop_assert_eq!(path(&before.replicas(&key)), path(&after.replicas(&key)));
        }
        let summary = before.diff(&after, keys(64)).summarize();
        prop_assert_eq!(summary.cross_cell_moves(), 0);
    }
}

#[test]
fn a_selector_ring_sharing_a_seed_with_any_ring_below_it_is_rejected() -> TestResult {
    let leaf = |seed| cell_ring("n", seed, 1, 2);
    let nested = |selector_seed, seeds: [u64; 2]| {
        Cells::builder()
            .ring_selector(selector(
                Strategy::Rendezvous,
                selector_seed,
                &[("x", 1), ("y", 1)],
            ))
            .cell(cell("x"), leaf(seeds[0]))
            .cell(cell("y"), leaf(seeds[1]))
            .build()
    };
    let shared = BuildCellsError::SharedSeed {
        cell: cell("eu"),
        seed: 7,
    };

    // The cell's own ring.
    let direct = Cells::builder()
        .ring_selector(selector(Strategy::Rendezvous, 7, &[("eu", 1)]))
        .cell(cell("eu"), leaf(7))
        .build();
    assert_eq!(direct.err(), Some(shared.clone()));

    // A nested layer's selector ring, a ring two layers down, and a ring under a nested layer
    // that selects by directory alone all count.
    let directory_only = Cells::builder()
        .directory(Directory::new().catch_all(cell("x")))
        .cell(cell("x"), leaf(7))
        .build()?;
    for inner in [nested(7, [8, 9])?, nested(8, [9, 7])?] {
        let top = Cells::builder()
            .ring_selector(selector(Strategy::Rendezvous, 7, &[("eu", 1)]))
            .cell(cell("eu"), inner)
            .build();
        assert_eq!(top.err(), Some(shared.clone()));
    }
    let top = Cells::builder()
        .ring_selector(selector(Strategy::Rendezvous, 7, &[("eu", 1)]))
        .cell(cell("eu"), directory_only)
        .build();
    assert_eq!(top.err(), Some(shared));

    // Siblings may share seeds, since they never see the same key, and a layer without a
    // selector ring hashes nothing.
    let siblings = Cells::builder()
        .ring_selector(selector(Strategy::Rendezvous, 7, &[("a", 1), ("b", 1)]))
        .cell(cell("a"), leaf(8))
        .cell(cell("b"), leaf(8))
        .build();
    assert!(siblings.is_ok());
    let unhashed = Cells::builder()
        .directory(Directory::new().prefix("a", cell("a")).catch_all(cell("b")))
        .cell(cell("a"), nested(8, [9, 10])?)
        .cell(cell("b"), nested(8, [9, 10])?)
        .build();
    assert!(unhashed.is_ok());
    Ok(())
}

/// The ring of cell `c1` in [`vnode_cells`]: 10 weight-1 nodes with 16 tokens each.
fn vnode_cell(seed: u64) -> Ring {
    Ring::builder()
        .strategy(vnodes(16))
        .seed(seed)
        .nodes((0..10).map(|i| weighted(&format!("n{i}"), 1)))
        .build()
        .expect("valid ring")
}

/// Each node's exact share of primaries from its token arcs, normalized to sum to 1.
fn arc_shares(ring: &Ring) -> Vec<f64> {
    let shares = ring.expected_shares(NonZeroU32::MIN);
    let total: f64 = shares
        .iter()
        .map(|share| f64::from(share.primary_bps))
        .sum();
    shares
        .iter()
        .map(|share| f64::from(share.primary_bps) / total)
        .collect()
}

/// Counts the primaries of `keys` on `ring`, by canonical node index.
fn primary_counts<'k>(ring: &Ring, keys: impl Iterator<Item = &'k Vec<u8>>) -> Vec<u64> {
    let nodes = ring.nodes();
    let primaries = keys.map(|key| {
        let primary = ring.primary(key);
        nodes
            .binary_search_by(|node| node.id().cmp(primary.id()))
            .expect("the primary belongs to the ring")
    });
    tally(nodes.len(), primaries)
}

/// ADR-0012, point 4: with distinct seeds, the keys that reach a vnode cell are spread over its
/// whole circle, so the cell balances exactly as the same ring does on its own. With the selector
/// ring's seed, which `build` rejects, the cell would see only the arcs it won one layer up.
#[test]
fn a_vnode_cell_balances_like_a_standalone_ring_only_with_its_own_seed() -> TestResult {
    // One token per cell, so each cell wins a few long arcs of the selector's circle.
    let selector = selector(vnodes(1), 1, &[("c1", 1), ("c2", 1), ("c3", 1), ("c4", 1)]);
    let standalone = vnode_cell(2);
    let fleet = Cells::builder()
        .ring_selector(selector.clone())
        .cell(cell("c1"), standalone.clone())
        .cell(cell("c2"), vnode_cell(3))
        .cell(cell("c3"), vnode_cell(4))
        .cell(cell("c4"), vnode_cell(5))
        .build()?;
    let keys: Vec<Vec<u8>> = Corpus::Uuids.keys(MEDIUM).collect();
    let shares = arc_shares(&standalone);

    let alone = balance(&primary_counts(&standalone, keys.iter()), &shares);
    assert!(alone.is_balanced(), "the standalone ring: {alone}");

    let reaching: Vec<&Vec<u8>> = keys
        .iter()
        .filter(|key| fleet.cell_of(key).as_str() == "c1")
        .collect();
    assert!(
        reaching.len() > 5_000,
        "only {} keys reach c1",
        reaching.len()
    );
    let counts = primary_counts(&standalone, reaching.iter().copied());
    let in_cells: Vec<u64> = {
        let nodes = standalone.nodes();
        let primaries = reaching.iter().map(|key| {
            let primary = fleet.primary(key);
            nodes
                .binary_search_by(|node| node.id().cmp(primary.id()))
                .expect("the primary belongs to cell c1")
        });
        tally(nodes.len(), primaries)
    };
    assert_eq!(in_cells, counts, "the cell places keys as its ring does");
    let within = balance(&in_cells, &shares);
    assert!(within.is_balanced(), "cell c1 inside the layer: {within}");

    // The control: the same keys on c1's ring under the selector ring's seed.
    let correlated = vnode_cell(1);
    let skewed = balance(
        &primary_counts(&correlated, reaching.iter().copied()),
        &arc_shares(&correlated),
    );
    assert!(
        !skewed.is_balanced(),
        "a shared seed skews the cell: {skewed}"
    );
    Ok(())
}

/// Regions `eu` and `us`, each a layer of two cells: `eu` hashes keys to `eu-1` and `eu-2` with a
/// vnode selector ring; `us` pins `us:big` keys to `us-2` and hashes the rest. The top layer pins
/// `eu:` and `us:` keys to their regions and hashes the rest.
#[derive(Clone)]
struct Fleet {
    top_rules: Directory,
    top_selector: Ring,
    eu_selector: Ring,
    eu_cells: [(&'static str, Ring); 2],
    us_rules: Directory,
    us_selector: Ring,
    us_cells: [(&'static str, Ring); 2],
}

impl Fleet {
    fn new() -> Self {
        Self {
            top_rules: Directory::new()
                .prefix("eu:", cell("eu"))
                .prefix("us:", cell("us")),
            top_selector: selector(Strategy::Rendezvous, 1, &[("eu", 1), ("us", 2)]),
            eu_selector: selector(vnodes(8), 2, &[("eu-1", 1), ("eu-2", 1)]),
            eu_cells: [
                ("eu-1", cell_ring("eu1", 10, 3, 1)),
                ("eu-2", cell_ring("eu2", 11, 2, 2)),
            ],
            us_rules: Directory::new().prefix("us:big", cell("us-2")),
            us_selector: selector(Strategy::Rendezvous, 3, &[("us-1", 3), ("us-2", 1)]),
            us_cells: [
                ("us-1", cell_ring("us1", 12, 3, 2)),
                ("us-2", cell_ring("us2", 13, 3, 1)),
            ],
        }
    }

    fn build(&self) -> Result<Cells<Cells>, BuildCellsError> {
        let eu = Cells::builder()
            .ring_selector(self.eu_selector.clone())
            .cells(
                self.eu_cells
                    .iter()
                    .map(|(id, ring)| (cell(id), ring.clone())),
            )
            .build()?;
        let us = Cells::builder()
            .directory(self.us_rules.clone())
            .ring_selector(self.us_selector.clone())
            .cells(
                self.us_cells
                    .iter()
                    .map(|(id, ring)| (cell(id), ring.clone())),
            )
            .build()?;
        Cells::builder()
            .directory(self.top_rules.clone())
            .ring_selector(self.top_selector.clone())
            .cell(cell("us"), us)
            .cell(cell("eu"), eu)
            .build()
    }
}

#[test]
fn three_levels_nest_through_the_same_api_and_name_every_layer_on_the_path() -> TestResult {
    let parts = Fleet::new();
    let fleet = parts.build()?;
    let mut paths = BTreeSet::new();
    for key in keys(1_000).into_iter().chain([
        b"eu:tenant:1".to_vec(),
        b"us:big:tenant".to_vec(),
        b"us:tenant:2".to_vec(),
    ]) {
        let placed = fleet.replicas(&key);
        let [region, leaf] = placed.path() else {
            panic!("{placed}: two layers give two cells");
        };
        let want_region = match &key[..] {
            [b'e', b'u', b':', ..] => "eu",
            [b'u', b's', b':', ..] => "us",
            _ => parts.top_selector.primary(&key).id().as_str(),
        };
        assert_eq!(region.as_str(), want_region, "{placed}");
        let inner = fleet.cell(region).ok_or("the path names a region")?;
        assert_eq!(inner.cell_of(&key), *leaf, "{placed}");
        let ring = inner.cell(leaf).ok_or("the path names a cell")?;
        assert_eq!(placed.replicas(), &ring.replicas(&key), "{placed}");
        assert_eq!(fleet.cell_of(&key), *leaf);
        assert_eq!(fleet.replicas_for(&key), placed, "the trait agrees");
        assert_eq!(inner.replicas(&key).path(), &placed.path()[1..]);
        paths.insert(path(&placed));
    }
    assert_eq!(paths.len(), 4, "every cell holds keys: {paths:?}");
    assert_eq!(
        fleet.replicas("us:big:tenant").path(),
        [&cell("us"), &cell("us-2")]
    );
    Ok(())
}

#[test]
fn layers_nest_deeper_with_paths_that_name_every_layer() -> TestResult {
    let fleet = Fleet::new().build()?;
    let planet = Cells::builder()
        .directory(Directory::new().catch_all(cell("earth")))
        .cell(cell("earth"), fleet.clone())
        .build()?;
    for key in keys(200) {
        let placed = planet.replicas(&key);
        assert_eq!(placed.path()[0], &cell("earth"));
        assert_eq!(&placed.path()[1..], fleet.replicas(&key).path());
        assert_eq!(placed.replicas(), fleet.replicas(&key).replicas());
        assert_eq!(
            placed.to_string(),
            format!("earth/{}", fleet.replicas(&key))
        );
    }
    Ok(())
}

#[test]
fn the_fingerprint_changes_with_every_layer_and_ignores_epochs() -> TestResult {
    /// A change to one part of a [`Fleet`].
    type Edit = fn(&mut Fleet) -> TestResult;
    let edits: [(&str, Edit); 8] = [
        ("a top-level rule", |fleet| {
            fleet.top_rules = fleet.top_rules.clone().exact("tenant:acme", cell("eu"));
            Ok(())
        }),
        ("a region's weight", |fleet| {
            fleet.top_selector = selector(Strategy::Rendezvous, 1, &[("eu", 1), ("us", 3)]);
            Ok(())
        }),
        ("the top selector ring's seed", |fleet| {
            fleet.top_selector = fleet.top_selector.to_builder().seed(99).build()?;
            Ok(())
        }),
        ("a cell's weight inside a region", |fleet| {
            fleet.eu_selector = selector(vnodes(8), 2, &[("eu-1", 1), ("eu-2", 2)]);
            Ok(())
        }),
        ("a rule inside a region", |fleet| {
            fleet.us_rules = fleet.us_rules.clone().exact("tenant:acme", cell("us-1"));
            Ok(())
        }),
        ("a node in a cell", |fleet| {
            let ring = &fleet.eu_cells[0].1;
            fleet.eu_cells[0].1 = ring
                .to_builder()
                .node(weighted("eu1-n9", 1).with_zone("eu1-z1".parse()?))
                .build()?;
            Ok(())
        }),
        ("a cell ring's seed", |fleet| {
            fleet.us_cells[1].1 = fleet.us_cells[1].1.to_builder().seed(98).build()?;
            Ok(())
        }),
        ("a cell ring's replication factor", |fleet| {
            let two = ReplicationFactor::new(2).ok_or("zero")?;
            let ring = &fleet.us_cells[0].1;
            fleet.us_cells[0].1 = ring.to_builder().replication_factor(two).build()?;
            Ok(())
        }),
    ];
    let base = Fleet::new();
    let mut variants = Vec::new();
    for (label, edit) in edits {
        let mut fleet = base.clone();
        edit(&mut fleet)?;
        variants.push((label, fleet));
    }

    let fingerprint = base.build()?.fingerprint();
    let mut seen = BTreeSet::from([fingerprint]);
    for (label, fleet) in &variants {
        let changed = fleet.build()?.fingerprint();
        assert_ne!(changed, fingerprint, "{label}");
        assert!(seen.insert(changed), "{label} collides with another change");
    }

    // Epochs never affect placement, at any layer.
    let mut later = base.clone();
    let epoch = |ring: &Ring| ring.to_builder().epoch(Epoch::new(42)).build();
    later.top_selector = epoch(&later.top_selector)?;
    later.eu_selector = epoch(&later.eu_selector)?;
    later.us_selector = epoch(&later.us_selector)?;
    for (_, ring) in later.eu_cells.iter_mut().chain(later.us_cells.iter_mut()) {
        *ring = epoch(ring)?;
    }
    assert_eq!(later.build()?.fingerprint(), fingerprint);

    // Nor does the order cells and rules were added in.
    let mut reordered = base.clone();
    reordered.eu_cells.reverse();
    reordered.us_cells.reverse();
    reordered.top_rules = Directory::new()
        .prefix("us:", cell("us"))
        .prefix("eu:", cell("eu"));
    assert_eq!(reordered.build()?.fingerprint(), fingerprint);
    Ok(())
}

#[test]
fn a_ring_is_a_placement_with_an_empty_path() -> TestResult {
    fn fingerprint_of<P: Placement>(placement: &P) -> Fingerprint {
        placement.fingerprint()
    }
    let ring = cell_ring("n", 1, 3, 1);
    let placed = ring.replicas_for(b"tenant:acme");
    assert!(placed.path().is_empty());
    assert_eq!(placed.cell(), None);
    assert_eq!(placed.replicas(), &ring.replicas("tenant:acme"));
    assert_eq!(placed.to_string(), ring.replicas("tenant:acme").to_string());
    assert_eq!(fingerprint_of(&ring), ring.fingerprint());
    let fleet = three_cells()?;
    assert_eq!(fingerprint_of(&fleet), fleet.fingerprint());
    Ok(())
}

#[test]
fn build_rejects_each_invalid_layer_with_a_precise_error() -> TestResult {
    let ring = |seed| cell_ring("n", seed, 1, 1);
    let cases: [(&str, Result<Cells, BuildCellsError>, BuildCellsError, &str); 7] = [
        (
            "no cells",
            Cells::builder()
                .directory(Directory::new().catch_all(cell("a")))
                .build(),
            BuildCellsError::NoCells,
            "cells layer has no cells",
        ),
        (
            "a duplicate cell",
            Cells::builder()
                .directory(Directory::new().catch_all(cell("a")))
                .cell(cell("b"), ring(1))
                .cell(cell("a"), ring(2))
                .cell(cell("b"), ring(3))
                .build(),
            BuildCellsError::DuplicateCell { id: cell("b") },
            "cell id 'b' appears more than once",
        ),
        (
            "no fallback",
            Cells::builder()
                .directory(Directory::new().prefix("a", cell("a")))
                .cell(cell("a"), ring(1))
                .build(),
            BuildCellsError::NoFallback,
            "keys that match no directory rule reach no cell; add a catch-all rule or a selector ring",
        ),
        (
            "a shadowed ring",
            Cells::builder()
                .directory(Directory::new().catch_all(cell("a")))
                .ring_selector(selector(Strategy::Rendezvous, 9, &[("a", 1)]))
                .cell(cell("a"), ring(1))
                .build(),
            BuildCellsError::ShadowedRing,
            "the directory's catch-all rule matches every key, so the selector ring is never used",
        ),
        (
            "unknown directory targets",
            Cells::builder()
                .directory(
                    Directory::new()
                        .prefix("p", cell("gone-2"))
                        .exact("k", cell("gone-1"))
                        .catch_all(cell("a")),
                )
                .cell(cell("a"), ring(1))
                .build(),
            BuildCellsError::UnknownDirectoryTarget {
                cell: cell("gone-1"),
            },
            "a directory rule names cell 'gone-1', which the layer doesn't have",
        ),
        (
            "unknown ring targets",
            Cells::builder()
                .ring_selector(selector(
                    Strategy::Rendezvous,
                    9,
                    &[("zz", 1), ("a", 1), ("gone", 1)],
                ))
                .cell(cell("a"), ring(1))
                .build(),
            BuildCellsError::UnknownRingTarget {
                node: "gone".parse()?,
            },
            "selector ring node 'gone' names a cell the layer doesn't have",
        ),
        (
            "an unreachable cell",
            Cells::builder()
                .directory(Directory::new().prefix("b", cell("b")))
                .ring_selector(selector(Strategy::Rendezvous, 9, &[("a", 1)]))
                .cell(cell("c"), ring(3))
                .cell(cell("a"), ring(1))
                .cell(cell("b"), ring(2))
                .build(),
            BuildCellsError::UnreachableCell { id: cell("c") },
            "cell 'c' is unreachable: no directory rule or selector ring node names it",
        ),
    ];
    for (label, result, error, message) in cases {
        let got = result.err().ok_or(label)?;
        assert_eq!(got, error, "{label}");
        assert_eq!(got.to_string(), message, "{label}");
    }
    Ok(())
}
