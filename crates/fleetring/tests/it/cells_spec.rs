//! Cells specs and their serde form through the public API (FR-021).

use std::collections::BTreeMap;
use std::error::Error;
use std::num::NonZeroU32;

use fleetring::{
    BuildCellsError, BuildRingError, CellId, Cells, CellsSpec, Directory, Epoch, Fingerprint,
    FromCellsSpecError, FromSpecError, Node, Placement, PlacementSpec, ReplicationFactor, Ring,
    RingSpec, Strategy, VnodeParams, Weight, Xxh3,
};
use proptest::prelude::*;
use proptest::strategy::Strategy as _;

use crate::common::corpus::Corpus;

type TestResult = Result<(), Box<dyn Error>>;

/// Every strategy a generated ring can use. Four tokens per weight unit keep vnode rings cheap
/// to rebuild; the vnode algorithm has its own suite.
const STRATEGIES: [Strategy; 2] = [
    Strategy::Rendezvous,
    Strategy::Vnodes(VnodeParams::new(NonZeroU32::new(4).expect("4 is nonzero"))),
];

fn cell(id: &str) -> CellId {
    id.parse().expect("test cell ids are valid")
}

fn weighted(id: &str, weight: u32) -> Node {
    Node::new(id.parse().expect("test ids are valid"))
        .with_weight(Weight::new(weight).expect("test weights are nonzero"))
}

/// A ring of weight-1 nodes with the IDs `ids`.
fn ring(seed: u64, ids: &[&str]) -> Ring {
    Ring::builder()
        .seed(seed)
        .nodes(ids.iter().map(|id| weighted(id, 1)))
        .build()
        .expect("test rings are valid")
}

/// A selector ring whose nodes are the cells `cells`, with their weights.
fn selector(seed: u64, cells: &[(&str, u32)]) -> Ring {
    Ring::builder()
        .seed(seed)
        .nodes(cells.iter().map(|&(id, weight)| weighted(id, weight)))
        .build()
        .expect("test selector rings are valid")
}

/// Cells `a`, `b`, and `c`, picked by a selector ring after a rule that pins `pinned:` keys to
/// `a`.
fn three_cells() -> Result<Cells, BuildCellsError> {
    Cells::builder()
        .directory(Directory::new().prefix("pinned:", cell("a")))
        .ring_selector(selector(100, &[("a", 1), ("b", 2), ("c", 3)]))
        .cell(cell("c"), ring(3, &["c-n1", "c-n2"]))
        .cell(cell("a"), ring(1, &["a-n1", "a-n2", "a-n3"]))
        .cell(cell("b"), ring(2, &["b-n1"]))
        .build()
}

/// Regions `eu` and `us`, each a layer of two cells. `eu` hashes keys to its cells with a
/// selector ring alone; `us` pins `us:big:` keys to `us-2` and a non-UTF-8 key to `us-1`, and
/// hashes the rest. The top layer pins `eu:` keys to `eu` and hashes the rest.
fn regions() -> Result<Cells<Cells>, BuildCellsError> {
    let eu = Cells::builder()
        .ring_selector(selector(2, &[("eu-1", 1), ("eu-2", 2)]))
        .cell(cell("eu-1"), ring(10, &["eu1-n1", "eu1-n2"]))
        .cell(cell("eu-2"), ring(11, &["eu2-n1", "eu2-n2", "eu2-n3"]))
        .build()?;
    let us = Cells::builder()
        .directory(
            Directory::new()
                .prefix("us:big:", cell("us-2"))
                .exact([0xff, 0xfe], cell("us-1")),
        )
        .ring_selector(selector(3, &[("us-1", 3), ("us-2", 1)]))
        .cell(cell("us-1"), ring(12, &["us1-n1"]))
        .cell(cell("us-2"), ring(13, &["us2-n1", "us2-n2"]))
        .build()?;
    Cells::builder()
        .directory(Directory::new().prefix("eu:", cell("eu")))
        .ring_selector(selector(1, &[("eu", 1), ("us", 1)]))
        .cell(cell("us"), us)
        .cell(cell("eu"), eu)
        .build()
}

/// Keys for the placement checks: agent names, sequential tenant names, and keys that the
/// fixtures' rules match.
fn keys() -> Vec<Vec<u8>> {
    let ruled = [
        &b"pinned:acme"[..],
        b"eu:acme",
        b"us:big:globex",
        &[0xff, 0xfe],
    ];
    Corpus::Agents
        .keys(100)
        .chain(Corpus::Sequential.keys(100))
        .chain(ruled.into_iter().map(<[u8]>::to_vec))
        .collect()
}

/// Asserts that two layers place every key of [`keys`] on the same path and replicas.
fn assert_same_placement<P: Placement>(left: &Cells<P>, right: &Cells<P>, context: &str) {
    for key in keys() {
        assert_eq!(
            left.replicas(&key),
            right.replicas(&key),
            "{context}: {key:?}"
        );
    }
}

/// Returns the spec of the layer that cell `id` of `spec` holds.
fn layer_mut<'a>(spec: &'a mut CellsSpec, id: &str) -> &'a mut CellsSpec {
    let entry = spec
        .cells
        .iter_mut()
        .find(|entry| entry.id.as_str() == id)
        .unwrap_or_else(|| panic!("the spec has a cell {id}"));
    match &mut entry.placement {
        PlacementSpec::Cells(layer) => layer,
        other => panic!("cell {id} is {other:?}, not a layer"),
    }
}

/// Returns the spec of the ring that cell `id` of `spec` holds.
fn ring_mut<'a>(spec: &'a mut CellsSpec, id: &str) -> &'a mut RingSpec {
    let entry = spec
        .cells
        .iter_mut()
        .find(|entry| entry.id.as_str() == id)
        .unwrap_or_else(|| panic!("the spec has a cell {id}"));
    match &mut entry.placement {
        PlacementSpec::Ring(ring) => ring,
        other => panic!("cell {id} is {other:?}, not a ring"),
    }
}

fn path(ids: &[&str]) -> Vec<CellId> {
    ids.iter().map(|id| cell(id)).collect()
}

#[test]
fn layers_rebuilt_from_their_specs_place_every_key_identically() -> TestResult {
    let one = three_cells()?;
    let spec = one.to_spec();
    assert_eq!(spec.fingerprint, one.fingerprint());
    let rebuilt: Cells = Cells::from_spec(&spec, Xxh3)?;
    assert_eq!(rebuilt.fingerprint(), one.fingerprint());
    assert_eq!(rebuilt.to_spec(), spec);
    assert_same_placement(&one, &rebuilt, "one layer");

    let two = regions()?;
    let spec = two.to_spec();
    assert_eq!(spec.fingerprint, two.fingerprint());
    let rebuilt: Cells<Cells> = Cells::from_spec(&spec, Xxh3)?;
    assert_eq!(rebuilt.fingerprint(), two.fingerprint());
    assert_eq!(rebuilt.to_spec(), spec);
    assert_same_placement(&two, &rebuilt, "two layers");
    Ok(())
}

#[test]
fn a_spec_holds_each_cell_in_id_order_with_its_own_spec() -> TestResult {
    let fleet = regions()?;
    let spec = fleet.to_spec();
    assert_eq!(spec.format, CellsSpec::FORMAT);
    assert_eq!(&spec.directory, fleet.directory());
    assert_eq!(spec.ring_selector, fleet.ring_selector().map(Ring::to_spec));
    let ids: Vec<&str> = spec.cells.iter().map(|entry| entry.id.as_str()).collect();
    assert_eq!(ids, ["eu", "us"]);
    for entry in &spec.cells {
        let region = fleet.cell(&entry.id).ok_or("the spec names a cell")?;
        assert_eq!(entry.placement, PlacementSpec::Cells(region.to_spec()));
        let PlacementSpec::Cells(inner) = &entry.placement else {
            return Err("a region is a layer".into());
        };
        for leaf in &inner.cells {
            let ring = region.cell(&leaf.id).ok_or("the spec names a cell")?;
            assert_eq!(leaf.placement, PlacementSpec::Ring(ring.to_spec()));
        }
    }
    Ok(())
}

#[test]
fn the_order_of_cells_in_a_spec_does_not_matter() -> TestResult {
    let fleet = regions()?;
    let spec = fleet.to_spec();
    let mut reversed = spec.clone();
    reversed.cells.reverse();
    for id in ["eu", "us"] {
        layer_mut(&mut reversed, id).cells.reverse();
    }
    assert_ne!(reversed, spec);
    let rebuilt: Cells<Cells> = Cells::from_spec(&reversed, Xxh3)?;
    assert_eq!(rebuilt.to_spec(), spec);
    Ok(())
}

#[test]
fn epochs_round_trip_without_affecting_any_fingerprint() -> TestResult {
    let later = |ring: &Ring| ring.to_builder().epoch(Epoch::new(42)).build();
    let base = three_cells()?;
    let mut builder = Cells::builder()
        .directory(base.directory().clone())
        .ring_selector(later(base.ring_selector().ok_or("a selector ring")?)?);
    for (id, ring) in base.cells() {
        builder = builder.cell(id.clone(), later(ring)?);
    }
    let fleet = builder.build()?;
    assert_eq!(fleet.fingerprint(), base.fingerprint());
    let rebuilt: Cells = Cells::from_spec(&fleet.to_spec(), Xxh3)?;
    let selector = rebuilt.ring_selector().ok_or("a selector ring")?;
    assert_eq!(selector.epoch(), Epoch::new(42));
    for (_, ring) in rebuilt.cells() {
        assert_eq!(ring.epoch(), Epoch::new(42));
    }
    Ok(())
}

#[test]
fn a_spec_whose_layer_changed_fails_the_fingerprint_check() -> TestResult {
    let fleet = regions()?;
    let spec = fleet.to_spec();

    let mut top = spec.clone();
    top.directory = top.directory.exact("tenant:acme", cell("eu"));
    let error = Cells::<Cells>::from_spec(&top, Xxh3).expect_err("a top-level rule changed");
    let FromCellsSpecError::FingerprintMismatch {
        stored, computed, ..
    } = error
    else {
        return Err(format!("expected a fingerprint mismatch, got {error:?}").into());
    };
    assert_eq!(stored, spec.fingerprint);
    assert_ne!(computed, stored);
    assert!(error.path().is_empty());
    assert_eq!(
        error.to_string(),
        format!(
            "cells spec stores fingerprint {stored}, but the layer it describes has fingerprint {computed}"
        )
    );

    let mut nested = spec.clone();
    let us = layer_mut(&mut nested, "us");
    us.directory = us.directory.clone().exact("tenant:acme", cell("us-2"));
    let error = Cells::<Cells>::from_spec(&nested, Xxh3).expect_err("a rule in us changed");
    assert!(
        matches!(error, FromCellsSpecError::FingerprintMismatch { .. }),
        "{error:?}"
    );
    assert_eq!(error.path(), path(&["us"]));
    assert!(
        error
            .to_string()
            .starts_with("cells spec of cell 'us' stores fingerprint "),
        "{error}"
    );

    let mut tampered = spec;
    tampered.fingerprint = Fingerprint::new(tampered.fingerprint.get() ^ 1);
    assert!(matches!(
        Cells::<Cells>::from_spec(&tampered, Xxh3),
        Err(FromCellsSpecError::FingerprintMismatch { .. })
    ));
    Ok(())
}

#[test]
fn an_unknown_format_is_rejected_before_anything_else() -> TestResult {
    let mut spec = regions()?.to_spec();
    spec.format = 2;
    // A cell is missing too, but the format is checked first.
    spec.cells.pop();
    let error = Cells::<Cells>::from_spec(&spec, Xxh3).expect_err("format 2 is unknown");
    assert_eq!(
        error,
        FromCellsSpecError::UnsupportedFormat {
            path: Vec::new(),
            found: 2
        }
    );
    assert_eq!(
        error.to_string(),
        "cells spec has unsupported format 2; the supported format is 1"
    );

    let mut spec = regions()?.to_spec();
    layer_mut(&mut spec, "eu").format = 0;
    let error = Cells::<Cells>::from_spec(&spec, Xxh3).expect_err("format 0 is unknown");
    assert_eq!(
        error,
        FromCellsSpecError::UnsupportedFormat {
            path: path(&["eu"]),
            found: 0
        }
    );
    assert_eq!(
        error.to_string(),
        "cells spec of cell 'eu' has unsupported format 0; the supported format is 1"
    );
    Ok(())
}

/// Asserts that `error` is `want`, displays as `message`, and has `source` as its source.
fn assert_error<S>(error: &FromCellsSpecError, want: &FromCellsSpecError, message: &str, source: &S)
where
    S: Error + PartialEq + 'static,
{
    assert_eq!(error, want);
    assert_eq!(error.to_string(), message);
    let cause = error.source().and_then(|cause| cause.downcast_ref::<S>());
    assert_eq!(cause, Some(source), "{error}");
}

#[test]
fn an_unknown_selector_target_is_rejected_with_the_path_of_its_layer() -> TestResult {
    let mut spec = regions()?.to_spec();
    layer_mut(&mut spec, "eu").ring_selector =
        Some(selector(2, &[("eu-1", 1), ("eu-2", 2), ("eu-9", 1)]).to_spec());
    let error = Cells::<Cells>::from_spec(&spec, Xxh3).expect_err("eu-9 is no cell");
    let cause = BuildCellsError::UnknownRingTarget {
        node: "eu-9".parse()?,
    };
    assert_error(
        &error,
        &FromCellsSpecError::InvalidLayer {
            path: path(&["eu"]),
            source: cause.clone(),
        },
        "cells spec of cell 'eu' describes an invalid layer",
        &cause,
    );
    assert_eq!(
        cause.to_string(),
        "selector ring node 'eu-9' names a cell the layer doesn't have"
    );
    Ok(())
}

#[test]
fn a_duplicate_cell_is_rejected_with_the_path_of_its_layer() -> TestResult {
    let mut spec = regions()?.to_spec();
    let us = layer_mut(&mut spec, "us");
    let twin = us.cells[0].clone();
    us.cells.push(twin);
    let error = Cells::<Cells>::from_spec(&spec, Xxh3).expect_err("us-1 appears twice");
    let cause = BuildCellsError::DuplicateCell { id: cell("us-1") };
    assert_error(
        &error,
        &FromCellsSpecError::InvalidLayer {
            path: path(&["us"]),
            source: cause.clone(),
        },
        "cells spec of cell 'us' describes an invalid layer",
        &cause,
    );
    assert_eq!(cause.to_string(), "cell id 'us-1' appears more than once");
    Ok(())
}

#[test]
fn a_shared_seed_is_rejected_at_the_layer_whose_selector_it_shares() -> TestResult {
    // A cell ring with its own layer's selector seed.
    let mut spec = regions()?.to_spec();
    *ring_mut(layer_mut(&mut spec, "eu"), "eu-1") = ring(2, &["eu1-n1", "eu1-n2"]).to_spec();
    let error = Cells::<Cells>::from_spec(&spec, Xxh3).expect_err("eu-1 has eu's seed");
    let cause = BuildCellsError::SharedSeed {
        cell: cell("eu-1"),
        seed: 2,
    };
    assert_error(
        &error,
        &FromCellsSpecError::InvalidLayer {
            path: path(&["eu"]),
            source: cause.clone(),
        },
        "cells spec of cell 'eu' describes an invalid layer",
        &cause,
    );

    // A ring two layers down with the top selector's seed. Region us is valid on its own, with a
    // spec that matches it, so the top layer rejects it.
    let fleet = regions()?;
    let us = fleet
        .cell("us")
        .ok_or("region us exists")?
        .to_builder()
        .remove_cell("us-2")
        .cell(cell("us-2"), ring(1, &["us2-n1", "us2-n2"]))
        .build()?;
    let mut spec = fleet.to_spec();
    *layer_mut(&mut spec, "us") = us.to_spec();
    let error = Cells::<Cells>::from_spec(&spec, Xxh3).expect_err("us/us-2 has the top seed");
    let cause = BuildCellsError::SharedSeed {
        cell: cell("us"),
        seed: 1,
    };
    assert_error(
        &error,
        &FromCellsSpecError::InvalidLayer {
            path: Vec::new(),
            source: cause.clone(),
        },
        "cells spec describes an invalid layer",
        &cause,
    );
    assert!(
        cause
            .to_string()
            .starts_with("cell 'us' holds a ring with seed 1")
    );
    Ok(())
}

#[test]
fn a_spec_that_nests_unlike_the_type_is_rejected_where_it_differs() -> TestResult {
    // Two layers loaded as one: the first cell in ID order is already too deep.
    let error = Cells::<Ring>::from_spec(&regions()?.to_spec(), Xxh3)
        .expect_err("the spec nests two layers");
    assert_eq!(
        error,
        FromCellsSpecError::DepthMismatch {
            path: path(&["eu"]),
            expected: 0,
            found: 1,
        }
    );
    assert_eq!(
        error.to_string(),
        "cell 'eu' is a cells layer of depth 1 in the spec, but the type expects a ring"
    );

    // One layer loaded as two.
    let error = Cells::<Cells>::from_spec(&three_cells()?.to_spec(), Xxh3)
        .expect_err("the spec nests one layer");
    assert_eq!(
        error,
        FromCellsSpecError::DepthMismatch {
            path: path(&["a"]),
            expected: 1,
            found: 0,
        }
    );
    assert_eq!(
        error.to_string(),
        "cell 'a' is a ring in the spec, but the type expects a cells layer of depth 1"
    );

    // A leaf that is a layer in its own right, inside an otherwise matching spec.
    let mut spec = regions()?.to_spec();
    let leaf = Cells::builder()
        .directory(Directory::new().catch_all(cell("x")))
        .cell(cell("x"), ring(20, &["x-n1"]))
        .build()?;
    layer_mut(&mut spec, "us").cells[1].placement = PlacementSpec::Cells(leaf.to_spec());
    let error = Cells::<Cells>::from_spec(&spec, Xxh3).expect_err("us/us-2 is too deep");
    assert_eq!(
        error,
        FromCellsSpecError::DepthMismatch {
            path: path(&["us", "us-2"]),
            expected: 0,
            found: 1,
        }
    );
    assert_eq!(
        error.to_string(),
        "cell 'us/us-2' is a cells layer of depth 1 in the spec, but the type expects a ring"
    );
    Ok(())
}

#[test]
fn an_invalid_ring_spec_is_rejected_with_the_path_of_its_cell() -> TestResult {
    let fleet = regions()?;

    // A seed changed after the spec was written.
    let mut spec = fleet.to_spec();
    let us2 = ring_mut(layer_mut(&mut spec, "us"), "us-2");
    us2.seed = 99;
    let stored = us2.fingerprint;
    let error = Cells::<Cells>::from_spec(&spec, Xxh3).expect_err("us/us-2's seed changed");
    let cause = FromSpecError::FingerprintMismatch {
        stored,
        computed: ring(99, &["us2-n1", "us2-n2"]).fingerprint(),
    };
    assert_error(
        &error,
        &FromCellsSpecError::InvalidRing {
            path: path(&["us", "us-2"]),
            source: cause.clone(),
        },
        "ring spec of cell 'us/us-2' is invalid",
        &cause,
    );

    // A node listed twice.
    let mut spec = fleet.to_spec();
    let eu1 = ring_mut(layer_mut(&mut spec, "eu"), "eu-1");
    let twin = eu1.nodes[0].clone();
    eu1.nodes.push(twin);
    let error = Cells::<Cells>::from_spec(&spec, Xxh3).expect_err("eu/eu-1 lists a node twice");
    let cause = FromSpecError::InvalidRing(BuildRingError::DuplicateNode {
        id: "eu1-n1".parse()?,
    });
    assert_error(
        &error,
        &FromCellsSpecError::InvalidRing {
            path: path(&["eu", "eu-1"]),
            source: cause.clone(),
        },
        "ring spec of cell 'eu/eu-1' is invalid",
        &cause,
    );

    // A ring written with another hasher.
    let mut spec = three_cells()?.to_spec();
    ring_mut(&mut spec, "b").hasher = "murmur3-64".to_owned();
    let error = Cells::<Ring>::from_spec(&spec, Xxh3).expect_err("b names another hasher");
    assert_eq!(
        error,
        FromCellsSpecError::InvalidRing {
            path: path(&["b"]),
            source: FromSpecError::HasherMismatch {
                spec: "murmur3-64".to_owned(),
                hasher: "xxh3-64".to_owned(),
            },
        }
    );
    Ok(())
}

#[test]
fn an_invalid_selector_ring_spec_is_rejected_with_the_path_of_its_layer() -> TestResult {
    let mut spec = regions()?.to_spec();
    if let Some(ring) = &mut layer_mut(&mut spec, "eu").ring_selector {
        ring.format = 2;
    }
    let error = Cells::<Cells>::from_spec(&spec, Xxh3).expect_err("eu's selector is format 2");
    let cause = FromSpecError::UnsupportedFormat { found: 2 };
    assert_error(
        &error,
        &FromCellsSpecError::InvalidSelector {
            path: path(&["eu"]),
            source: cause.clone(),
        },
        "selector ring spec of cell 'eu' is invalid",
        &cause,
    );

    let mut spec = regions()?.to_spec();
    if let Some(ring) = &mut spec.ring_selector {
        ring.nodes.clear();
    }
    let error = Cells::<Cells>::from_spec(&spec, Xxh3).expect_err("the top selector is empty");
    let cause = FromSpecError::InvalidRing(BuildRingError::NoNodes);
    assert_error(
        &error,
        &FromCellsSpecError::InvalidSelector {
            path: Vec::new(),
            source: cause.clone(),
        },
        "selector ring spec is invalid",
        &cause,
    );
    Ok(())
}

#[test]
fn cells_are_checked_in_id_order_whatever_order_the_spec_lists_them_in() -> TestResult {
    let mut spec = three_cells()?.to_spec();
    ring_mut(&mut spec, "b").seed ^= 1;
    ring_mut(&mut spec, "c").seed ^= 1;
    for _ in 0..2 {
        let error = Cells::<Ring>::from_spec(&spec, Xxh3).expect_err("b and c changed");
        assert_eq!(error.path(), path(&["b"]), "{error}");
        spec.cells.reverse();
    }
    Ok(())
}

/// The rules of a generated layer: exact rules and prefix rules over short byte strings, often
/// not UTF-8, each naming a cell by its index modulo the layer's cell count.
type Rules = (BTreeMap<Vec<u8>, usize>, BTreeMap<Vec<u8>, usize>);

fn rules() -> impl proptest::strategy::Strategy<Value = Rules> {
    let bytes = proptest::collection::vec(prop_oneof![b'a'..=b'c', any::<u8>()], 0..5);
    let rules = proptest::collection::btree_map(bytes, any::<usize>(), 0..4);
    (rules.clone(), rules)
}

/// A generated leaf ring: its strategy, replication factor, and epoch, and one to four nodes
/// with weights 1 to 4 in up to three zones.
type Leaf = (Strategy, u8, u64, Vec<Node>);

fn leaf() -> impl proptest::strategy::Strategy<Value = Leaf> {
    let members =
        proptest::collection::btree_map(0u8..16, (1u32..=4, 0u8..3), 1..=4).prop_map(|members| {
            members
                .into_iter()
                .map(|(n, (weight, zone))| {
                    weighted(&format!("n{n}"), weight)
                        .with_zone(format!("z{zone}").parse().expect("valid zone"))
                })
                .collect()
        });
    (
        proptest::sample::select(STRATEGIES.to_vec()),
        1u8..=3,
        any::<u64>(),
        members,
    )
}

/// A generated layer: its rules, its selector ring's strategy or `None` for a directory alone,
/// and one to three cells, each with its weight in the selector ring.
type Plan<C> = (Rules, Option<Strategy>, Vec<(u32, C)>);

fn plan<C: std::fmt::Debug>(
    cell: impl proptest::strategy::Strategy<Value = C>,
) -> impl proptest::strategy::Strategy<Value = Plan<C>> {
    (
        rules(),
        proptest::option::of(proptest::sample::select(STRATEGIES.to_vec())),
        proptest::collection::vec((1u32..=4, cell), 1..=3),
    )
}

/// Builds a generated layer over `cells`, given as `(ID, selector weight, placement)`.
///
/// With a selector ring, which hashes with `seed`, the rules lose any catch-all, which would hide
/// the ring. Without one, a catch-all sends unmatched keys to the first cell, and each cell gets
/// an exact rule of its own, longer than any generated key, so every cell stays reachable.
fn layer<P: Placement<Hasher = Xxh3>>(
    (exact, prefix): &Rules,
    selector: Option<Strategy>,
    seed: u64,
    cells: Vec<(CellId, u32, P)>,
) -> Cells<P> {
    let target = |index: &usize| cells[index % cells.len()].0.clone();
    let mut directory = Directory::new();
    for (key, index) in exact {
        directory = directory.exact(key, target(index));
    }
    for (bytes, index) in prefix {
        if selector.is_none() || !bytes.is_empty() {
            directory = directory.prefix(bytes, target(index));
        }
    }
    let mut builder = Cells::builder();
    match selector {
        Some(strategy) => {
            let ring = Ring::builder()
                .strategy(strategy)
                .seed(seed)
                .nodes(
                    cells
                        .iter()
                        .map(|(id, weight, _)| weighted(id.as_str(), *weight)),
                )
                .build()
                .expect("generated selector rings are valid");
            builder = builder.ring_selector(ring);
        }
        None => {
            directory = directory.catch_all(cells[0].0.clone());
            for (id, ..) in &cells {
                directory = directory.exact(format!("reach:{id}"), id.clone());
            }
        }
    }
    builder
        .directory(directory)
        .cells(cells.into_iter().map(|(id, _, placement)| (id, placement)))
        .build()
        .expect("generated layers are valid")
}

/// Two-layer trees of one to three regions of one to three leaf rings, with arbitrary directories
/// at both layers. The top selector ring hashes with seed `base`, region `i`'s with `base + 1 +
/// i`, and the ring of its cell `j` with `base + 4 + 3i + j`, so every ring has its own seed.
fn two_layers() -> impl proptest::strategy::Strategy<Value = Cells<Cells>> {
    (any::<u64>(), plan(plan(leaf()))).prop_map(|(base, (rules, selector, regions))| {
        let seed = |offset: usize| base.wrapping_add(u64::try_from(offset).expect("small offset"));
        let regions = regions
            .into_iter()
            .enumerate()
            .map(|(i, (weight, (rules, selector, leaves)))| {
                let leaves = leaves
                    .into_iter()
                    .enumerate()
                    .map(|(j, (weight, (strategy, factor, epoch, nodes)))| {
                        let ring = Ring::builder()
                            .strategy(strategy)
                            .seed(seed(4 + 3 * i + j))
                            .replication_factor(
                                ReplicationFactor::new(factor).expect("factor is nonzero"),
                            )
                            .epoch(Epoch::new(epoch))
                            .nodes(nodes)
                            .build()
                            .expect("generated leaf rings are valid");
                        (cell(&format!("r{i}-c{j}")), weight, ring)
                    })
                    .collect();
                let region = layer(&rules, selector, seed(1 + i), leaves);
                (cell(&format!("r{i}")), weight, region)
            })
            .collect();
        layer(&rules, selector, seed(0), regions)
    })
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]

    #[test]
    fn two_layers_round_trip_through_their_specs(fleet in two_layers()) {
        let spec = fleet.to_spec();
        prop_assert_eq!(spec.fingerprint, fleet.fingerprint());
        let rebuilt = Cells::<Cells>::from_spec(&spec, Xxh3).expect("a layer's own spec loads");
        prop_assert_eq!(rebuilt.fingerprint(), fleet.fingerprint());
        prop_assert_eq!(&rebuilt.to_spec(), &spec);
        let ruled = fleet
            .directory()
            .exact_rules()
            .chain(fleet.directory().prefix_rules())
            .map(|(bytes, _)| bytes.to_vec());
        for key in Corpus::Agents.keys(16).chain(ruled) {
            prop_assert_eq!(rebuilt.replicas(&key), fleet.replicas(&key), "{:?}", key);
        }
        #[cfg(feature = "serde")]
        {
            let json = serde_json::to_string(&spec).expect("a spec serializes");
            let parsed: CellsSpec = serde_json::from_str(&json).expect("a written spec parses");
            prop_assert_eq!(&parsed, &spec);
            let reloaded = Cells::<Cells>::from_spec(&parsed, Xxh3).expect("a parsed spec loads");
            prop_assert_eq!(reloaded.fingerprint(), fleet.fingerprint());
        }
    }
}

/// Tests that swap the hasher, using the `test-util` hasher double.
#[cfg(feature = "test-util")]
mod hashers {
    use fleetring::test_util::TableHasher;

    use super::*;

    /// A ring that hashes with an empty [`TableHasher`].
    fn table_ring(seed: u64, ids: &[&str]) -> Ring<TableHasher> {
        Ring::builder()
            .hasher(TableHasher::new())
            .seed(seed)
            .nodes(ids.iter().map(|id| weighted(id, 1)))
            .build()
            .expect("test rings are valid")
    }

    #[test]
    fn every_ring_loads_with_the_given_hasher_and_checks_its_name() -> TestResult {
        let fleet = Cells::builder()
            .ring_selector(table_ring(100, &["a", "b"]))
            .cell(cell("a"), table_ring(1, &["a-n1", "a-n2"]))
            .cell(cell("b"), table_ring(2, &["b-n1"]))
            .build()?;
        let spec = fleet.to_spec();
        let rebuilt = Cells::<Ring<TableHasher>>::from_spec(&spec, TableHasher::new())?;
        assert_eq!(rebuilt.fingerprint(), fleet.fingerprint());
        assert_eq!(rebuilt.to_spec(), spec);

        // The selector ring is checked first, so the mismatch names it.
        let error = Cells::<Ring>::from_spec(&spec, Xxh3).expect_err("the spec names table-test");
        assert_eq!(
            error,
            FromCellsSpecError::InvalidSelector {
                path: Vec::new(),
                source: FromSpecError::HasherMismatch {
                    spec: "table-test".to_owned(),
                    hasher: "xxh3-64".to_owned(),
                },
            }
        );
        Ok(())
    }
}

/// Tests of the serde form of directories and cells specs.
#[cfg(feature = "serde")]
mod json {
    use super::*;

    /// The layer of the `CellsSpec` docs: a directory alone, with a non-UTF-8 exact rule, a
    /// prefix rule, and a catch-all, over two one-node rings.
    fn documented_layer() -> Result<Cells, BuildCellsError> {
        Cells::builder()
            .directory(
                Directory::new()
                    .exact([0xff, 0x00], cell("vip"))
                    .prefix("vip:", cell("vip"))
                    .catch_all(cell("main")),
            )
            .cell(cell("vip"), ring(2, &["v1"]))
            .cell(cell("main"), ring(1, &["m1"]))
            .build()
    }

    /// [`documented_layer`] as `serde_json::to_string_pretty` writes its spec, which the
    /// `CellsSpec` docs show.
    const DOCUMENTED_JSON: &str = r#"{
  "format": 1,
  "directory": {
    "exact": [
      {
        "key": {
          "hex": "ff00"
        },
        "cell": "vip"
      }
    ],
    "prefix": [
      {
        "prefix": "",
        "cell": "main"
      },
      {
        "prefix": "vip:",
        "cell": "vip"
      }
    ]
  },
  "ring_selector": null,
  "cells": [
    {
      "id": "main",
      "placement": {
        "ring": {
          "format": 1,
          "epoch": 0,
          "hasher": "xxh3-64",
          "seed": 1,
          "strategy": {
            "kind": "rendezvous"
          },
          "replication_factor": 3,
          "nodes": [
            {
              "id": "m1",
              "weight": 1,
              "zone": null,
              "host": null
            }
          ],
          "fingerprint": "2e785be0a27abddf"
        }
      }
    },
    {
      "id": "vip",
      "placement": {
        "ring": {
          "format": 1,
          "epoch": 0,
          "hasher": "xxh3-64",
          "seed": 2,
          "strategy": {
            "kind": "rendezvous"
          },
          "replication_factor": 3,
          "nodes": [
            {
              "id": "v1",
              "weight": 1,
              "zone": null,
              "host": null
            }
          ],
          "fingerprint": "fa50a1e6a2ff3ce7"
        }
      }
    }
  ],
  "fingerprint": "2ec6a5648d7ab6a1"
}"#;

    #[test]
    fn the_documented_layer_serializes_to_the_documented_json_byte_for_byte() -> TestResult {
        let spec = documented_layer()?.to_spec();
        let written = serde_json::to_string_pretty(&spec)?;
        assert!(
            written == DOCUMENTED_JSON,
            "the documented layer now serializes as:\n{written}"
        );
        let parsed: CellsSpec = serde_json::from_str(DOCUMENTED_JSON)?;
        assert_eq!(parsed, spec);
        Ok(())
    }

    #[test]
    fn the_cells_spec_docs_show_the_documented_json() -> TestResult {
        let source = include_str!("../../src/cells_spec.rs").replace("\r\n", "\n");
        let block = source
            .split("/// ```json\n")
            .nth(1)
            .and_then(|rest| rest.split("/// ```\n").next())
            .ok_or("the CellsSpec docs have a JSON block")?;
        let shown: Vec<&str> = block
            .lines()
            .map(|line| line.strip_prefix("/// ").unwrap_or(line))
            .collect();
        assert_eq!(shown.join("\n"), DOCUMENTED_JSON);
        Ok(())
    }

    #[test]
    fn two_layers_round_trip_through_json_text() -> TestResult {
        let fleet = regions()?;
        let json = serde_json::to_string_pretty(&fleet.to_spec())?;
        let spec: CellsSpec = serde_json::from_str(&json)?;
        assert_eq!(spec, fleet.to_spec());
        let rebuilt: Cells<Cells> = Cells::from_spec(&spec, Xxh3)?;
        assert_eq!(rebuilt.fingerprint(), fleet.fingerprint());
        assert_same_placement(&fleet, &rebuilt, "two layers through JSON");
        Ok(())
    }

    #[test]
    fn rules_serialize_as_strings_and_non_utf8_bytes_as_hex() -> TestResult {
        let directory = Directory::new()
            .exact("tenant:acme", cell("eu"))
            .exact([0xff, 0x00, 0x41], cell("us"))
            .exact("", cell("eu"))
            .prefix("eu:", cell("eu"))
            .prefix([0xc3], cell("eu"))
            .prefix("caf\u{e9}:", cell("eu"))
            .catch_all(cell("us"));
        let json = serde_json::to_string(&directory)?;
        assert_eq!(
            json,
            concat!(
                r#"{"exact":["#,
                r#"{"key":"","cell":"eu"},"#,
                r#"{"key":"tenant:acme","cell":"eu"},"#,
                r#"{"key":{"hex":"ff0041"},"cell":"us"}"#,
                r#"],"prefix":["#,
                r#"{"prefix":"","cell":"us"},"#,
                r#"{"prefix":"café:","cell":"eu"},"#,
                r#"{"prefix":"eu:","cell":"eu"},"#,
                r#"{"prefix":{"hex":"c3"},"cell":"eu"}"#,
                r#"]}"#,
            )
        );
        let parsed: Directory = serde_json::from_str(&json)?;
        assert_eq!(parsed, directory);
        Ok(())
    }

    #[test]
    fn either_form_reads_any_rule_and_the_empty_prefix_is_the_catch_all() -> TestResult {
        let json = r#"{
            "exact": [{"key": {"hex": "65753a61636d65"}, "cell": "eu"}],
            "prefix": [{"prefix": {"hex": ""}, "cell": "us"}, {"prefix": "ÿ", "cell": "eu"}]
        }"#;
        let directory: Directory = serde_json::from_str(json)?;
        let want = Directory::new()
            .exact("eu:acme", cell("eu"))
            .catch_all(cell("us"))
            .prefix("\u{ff}", cell("eu"));
        assert_eq!(directory, want);
        assert_eq!(directory.lookup("eu:acme"), Some(&cell("eu")));
        assert_eq!(directory.lookup("anything else"), Some(&cell("us")));
        assert_eq!(directory.lookup([0xc3, 0xbf, 0x01]), Some(&cell("eu")));
        // Bytes that are valid UTF-8 always serialize in the string form.
        assert_eq!(
            serde_json::to_string(&directory)?,
            r#"{"exact":[{"key":"eu:acme","cell":"eu"}],"prefix":[{"prefix":"","cell":"us"},{"prefix":"ÿ","cell":"eu"}]}"#
        );
        Ok(())
    }

    #[test]
    fn malformed_directories_fail_with_an_error() {
        let rule =
            |key: &str| format!(r#"{{"exact": [{{"key": {key}, "cell": "eu"}}], "prefix": []}}"#);
        let cases = [
            (rule(r#"{"hex": "fff"}"#), "hex has odd length 3; each byte takes two digits"),
            (rule(r#"{"hex": "f"}"#), "hex has odd length 1; each byte takes two digits"),
            (
                rule(r#"{"hex": "FF"}"#),
                "hex has invalid byte 'F' at index 0; allowed are 0-9 and a-f",
            ),
            (
                rule(r#"{"hex": "0g"}"#),
                "hex has invalid byte 'g' at index 1; allowed are 0-9 and a-f",
            ),
            (
                rule(r#"{"hex": "é"}"#),
                r"hex has invalid byte '\xc3' at index 0; allowed are 0-9 and a-f",
            ),
            (
                rule(r#"{"hex": "0x00"}"#),
                "hex has invalid byte 'x' at index 1; allowed are 0-9 and a-f",
            ),
            (
                rule(r#"{"hex": "ff", "text": "x"}"#),
                "unknown field `text`, expected `hex`",
            ),
            (rule(r#"{"base64": "/w=="}"#), "unknown field `base64`, expected `hex`"),
            (rule("{}"), "missing field `hex`"),
            (rule(r#"{"hex": "ff", "hex": "00"}"#), "duplicate field `hex`"),
            (
                rule(r#"{"hex": 255}"#),
                "invalid type: integer `255`, expected lowercase hex digits, two per byte",
            ),
            (
                rule("12"),
                "invalid type: integer `12`, expected a string, or an object whose `hex` field holds lowercase hex digits",
            ),
            (rule("null"), "invalid type: null, expected a string, or an object"),
            (
                r#"{"exact": [{"key": "a", "cell": "eu", "note": "x"}], "prefix": []}"#.to_owned(),
                "unknown field `note`, expected `key` or `cell`",
            ),
            (
                r#"{"exact": [], "prefix": [{"key": "a", "cell": "eu"}]}"#.to_owned(),
                "unknown field `key`, expected `prefix` or `cell`",
            ),
            (
                r#"{"exact": [], "prefix": [], "suffix": []}"#.to_owned(),
                "unknown field `suffix`, expected `exact` or `prefix`",
            ),
            (r#"{"exact": []}"#.to_owned(), "missing field `prefix`"),
            (
                r#"{"exact": [{"key": "a"}], "prefix": []}"#.to_owned(),
                "missing field `cell`",
            ),
            (
                r#"{"exact": [{"key": "a", "cell": "EU"}], "prefix": []}"#.to_owned(),
                "cell id has invalid byte 'E' at index 0",
            ),
            (
                r#"{"exact": [{"key": "eu", "cell": "eu"}, {"key": {"hex": "6575"}, "cell": "us"}], "prefix": []}"#.to_owned(),
                r#"directory has two exact rules for key "eu""#,
            ),
            (
                r#"{"exact": [], "prefix": [{"prefix": "", "cell": "eu"}, {"prefix": "", "cell": "eu"}]}"#.to_owned(),
                r#"directory has two prefix rules for prefix """#,
            ),
            (
                r#"{"exact": [{"key": {"hex": "ff"}, "cell": "eu"}, {"key": {"hex": "ff"}, "cell": "us"}], "prefix": []}"#.to_owned(),
                r#"directory has two exact rules for key "\xff""#,
            ),
            (r#"{"exact": {}, "prefix": []}"#.to_owned(), "invalid type: map, expected a sequence"),
        ];
        for (json, message) in cases {
            let error =
                serde_json::from_str::<Directory>(&json).expect_err("the directory is malformed");
            assert!(error.to_string().contains(message), "{json}: {error}");
        }
    }

    /// Deserializes the JSON of [`regions`]'s spec after `edit` changes it.
    fn parse_edited(
        edit: impl FnOnce(&mut serde_json::Value),
    ) -> Result<CellsSpec, serde_json::Error> {
        let spec = regions().expect("the fixture is valid").to_spec();
        let mut value = serde_json::to_value(&spec).expect("a spec serializes");
        edit(&mut value);
        serde_json::from_value(value)
    }

    #[test]
    fn unknown_fields_outside_directories_are_ignored() -> TestResult {
        let spec = parse_edited(|value| {
            value["comment"] = "written by a newer daemon".into();
            value["cells"][0]["comment"] = "ignored in a cell".into();
            value["cells"][0]["placement"]["cells"]["comment"] = "and in a nested layer".into();
            value["ring_selector"]["comment"] = "and in a ring spec".into();
        })?;
        assert_eq!(spec, regions()?.to_spec());
        Ok(())
    }

    #[test]
    fn a_missing_selector_ring_reads_as_none() -> TestResult {
        let spec = three_cells()?.to_spec();
        let mut value = serde_json::to_value(&spec)?;
        value["ring_selector"] = serde_json::Value::Null;
        let parsed: CellsSpec = serde_json::from_value(value.clone())?;
        assert_eq!(parsed.ring_selector, None);
        if let Some(object) = value.as_object_mut() {
            object.remove("ring_selector");
        }
        let parsed: CellsSpec = serde_json::from_value(value)?;
        assert_eq!(parsed.ring_selector, None);
        Ok(())
    }

    #[test]
    fn malformed_cells_specs_fail_with_an_error() {
        type Edit = fn(&mut serde_json::Value);
        let cases: [(Edit, &str); 7] = [
            (
                |value| {
                    let ring =
                        value["cells"][0]["placement"]["cells"]["cells"][0]["placement"]["ring"]
                            .take();
                    value["cells"][0]["placement"]["cells"]["cells"][0]["placement"] =
                        serde_json::json!({ "vnode_ring": ring });
                },
                "unknown variant `vnode_ring`, expected `ring` or `cells`",
            ),
            (
                |value| {
                    let layer = value["cells"][0]["placement"]["cells"].clone();
                    value["cells"][0]["placement"]["ring"] = layer;
                },
                "map with a single key",
            ),
            (
                |value| {
                    if let Some(cell) = value["cells"][1].as_object_mut() {
                        cell.remove("placement");
                    }
                },
                "missing field `placement`",
            ),
            (
                |value| value["cells"][1]["id"] = "US".into(),
                "cell id has invalid byte 'U' at index 0",
            ),
            (
                |value| value["fingerprint"] = "abc".into(),
                "fingerprint is 3 bytes long; it must be 16 hex digits",
            ),
            (
                |value| {
                    value["directory"]["prefix"][0]["prefix"] = serde_json::json!({ "hex": "e" })
                },
                "hex has odd length 1; each byte takes two digits",
            ),
            (|value| value["format"] = (-1).into(), "expected u32"),
        ];
        for (edit, message) in cases {
            let error = parse_edited(edit).expect_err("the spec is malformed");
            assert!(error.to_string().contains(message), "{message}: {error}");
        }
    }

    #[test]
    fn an_unknown_format_deserializes_so_from_spec_can_reject_it() -> TestResult {
        let spec =
            parse_edited(|value| value["cells"][1]["placement"]["cells"]["format"] = 7.into())?;
        assert_eq!(
            Cells::<Cells>::from_spec(&spec, Xxh3).err(),
            Some(FromCellsSpecError::UnsupportedFormat {
                path: path(&["us"]),
                found: 7,
            })
        );
        Ok(())
    }
}
