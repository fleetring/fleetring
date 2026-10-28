//! The golden placement vectors: their catalog, generator, canonical form, and freeze guard
//! (FR-014, ADR-0008).
//!
//! `tests/vectors/placement-v1.json` pins the outputs of placement spec v1
//! (`docs/spec/placement-v1.md`), so that every platform, release, and port can check itself
//! against them: XXH3-64 digests, the logarithm table, `L(x)` values, and placement cases with
//! their fingerprints, replica sets, filtered replica sets, and bounded-load placements. The
//! file describes its own layout in its `layout` field.
//!
//! Three suites read it, each embedding it with `include_str!` so that it runs on targets
//! without a filesystem:
//!
//! - `tests/it/vectors.rs` checks every case through the public API;
//! - the tests in this module check the logarithm vectors, which need the crate-private math,
//!   and the freeze guard;
//! - `scripts/placement_ref.py`, an independent implementation written from the spec alone,
//!   checks the whole file in CI.
//!
//! # Generating
//!
//! [`catalog`] defines every case. To add cases, append them to the catalog and run:
//!
//! ```text
//! FLEETRING_WRITE_VECTORS=1 cargo test -p fleetring --all-features --lib vectors -- --ignored
//! ```
//!
//! The generator computes each case with the public API. It keeps the cases already in the file
//! byte for byte, appends the new ones, and appends a checkpoint that freezes them to the lock.
//! It writes nothing and fails if a case already in the file would change: the vectors are
//! append-only, so a changed expectation means placement changed, which needs a new spec
//! version. Without `FLEETRING_WRITE_VECTORS=1` it writes nothing either, and fails if the file
//! or the lock is out of date with the catalog.
//!
//! # Freeze guard
//!
//! Each line of `tests/vectors/placement-v1.lock` is a checkpoint, `<count> <digest>`:
//! `<digest>` is the XXH3-64, seed 0, of the [canonical form](canonical) of the file's reference
//! sections (`xxh3_64`, `log2_table`, and `neg_log2_q32`) together with its first `<count>`
//! cases. [`frozen_vectors_never_change`] verifies every checkpoint, so changing, reordering, or
//! removing a frozen case fails while appending a case doesn't. The lock is append-only too: the
//! generator only ever adds a line, so a diff that edits one stands out in review.

use std::env;
use std::fs;
use std::io;
use std::num::NonZeroU32;
use std::path::Path;

use serde_json::{Map, Value, json};

use crate::math::neg_log2_q32;
use crate::{
    BoundedLoads, CellId, CellPlacement, Cells, Directory, Headroom, Host, Node, Placement,
    ReplicationFactor, Ring, StableHasher, Strategy, VnodeParams, Weight, Xxh3, Zone,
};

/// The vector file.
const VECTORS: &str = include_str!("../tests/vectors/placement-v1.json");

/// The freeze guard's checkpoints.
const LOCK: &str = include_str!("../tests/vectors/placement-v1.lock");

/// Where the generator writes the vector file, relative to the crate root.
const VECTORS_PATH: &str = "tests/vectors/placement-v1.json";

/// Where the generator writes the lock, relative to the crate root.
const LOCK_PATH: &str = "tests/vectors/placement-v1.lock";

/// The command that appends new catalog cases to the vectors.
const GENERATE_COMMAND: &str =
    "FLEETRING_WRITE_VECTORS=1 cargo test -p fleetring --all-features --lib vectors -- --ignored";

/// The spec the vectors pin, recorded in the file's `spec` field.
const SPEC: &str = "docs/spec/placement-v1.md";

/// The file's `layout` field: how to read the rest of it.
const LAYOUT: &[&str] = &[
    "Golden vectors for FleetRing placement spec v1. The file is append-only: new cases go at the end, and existing cases never change (placement-v1.lock freezes them).",
    "64-bit values are strings, so that every JSON parser reads them exactly: seeds, hash digests, draws x, and fingerprints are 16 lowercase hex digits; loads, item loads, and capacities are decimal digits.",
    "Keys are their raw bytes in lowercase hex, so empty and non-UTF-8 keys can be written.",
    "xxh3_64: XXH3_64bits_withSeed over len bytes of input under seed gives digest; byte i of the input is (31 * i + 7) mod 256.",
    "log2_table: T[0] to T[1024] (spec section 4).",
    "neg_log2_q32: L(x) = l for 64-bit x (spec section 4).",
    "cases[].ring: the hasher, strategy (rendezvous, or vnodes with tokens_per_weight), seed, replication factor, and nodes. Nodes appear in the order supplied, which is not always canonical order (spec section 2). A node without a zone is in the default zone, and a node without a host is a host of its own.",
    "cases[].fingerprint: the ring's fingerprint (spec section 11).",
    "cases[].keys: each key's replica set at the ring's replication factor, preferred primary first (spec sections 5 to 7).",
    "cases[].filters: the down nodes are ineligible (spec section 8); each key's replica set among the other nodes, empty when every node is down.",
    "cases[].bounded: bounded-load placements (spec section 9). headroom is the percentage epsilon, item_load is the load of the key being placed, loads lists node loads (unlisted nodes have load 0), and capacity is every node's capacity for this item load. Each key's placement lists its replicas, whether it is saturated, and whether it is home.",
    "cases[].layer: a cells layer (spec section 13), in a case that has it in place of a ring. directory maps exact keys and prefixes, in lowercase hex, to cell IDs; selector, if present, is a ring whose node IDs are cell IDs; cells lists each cell's ID with its ring or a nested layer, in the order supplied. The fingerprint is the layer's (spec section 13).",
    "cases[].keys[].path: in a cells case, the cells on the key's path, top to leaf; replicas is the key's replica set on the leaf ring. In a cells case's filters, the down nodes are ineligible in whichever cell the key reaches, and the path is the key's unfiltered path, which a filter never changes.",
];

/// Field names in the order the file lists them. Each kind of object uses some of them, and this
/// one order suits every kind: a case lists `name`, `description`, `ring`, `fingerprint`, `keys`,
/// `filters`, then `bounded`; a filter lists `down`, then `keys`; and so on.
const FIELD_ORDER: &[&str] = &[
    "spec",
    "generated_by",
    "layout",
    "xxh3_64",
    "log2_table",
    "neg_log2_q32",
    "input",
    "name",
    "description",
    "cell",
    "ring",
    "layer",
    "directory",
    "exact",
    "prefix",
    "selector",
    "cells",
    "fingerprint",
    "down",
    "headroom",
    "item_load",
    "loads",
    "capacity",
    "keys",
    "filters",
    "bounded",
    "len",
    "hasher",
    "strategy",
    "kind",
    "tokens_per_weight",
    "seed",
    "digest",
    "replication_factor",
    "nodes",
    "id",
    "weight",
    "zone",
    "host",
    "key",
    "path",
    "replicas",
    "saturated",
    "home",
    "x",
    "l",
    "cases",
];

/// Fields whose values map node IDs to numbers, or hex keys and prefixes to cell IDs. They list
/// their keys in bytewise order.
const ID_MAPS: &[&str] = &["loads", "capacity", "exact", "prefix"];

/// The sections every checkpoint freezes in full, besides the cases.
const REFERENCE_SECTIONS: [&str; 3] = ["xxh3_64", "log2_table", "neg_log2_q32"];

/// Input lengths of the XXH3-64 vectors, covering every code path: 0, 1–3, 4–8, 9–16, 17–128,
/// 129–240, and above 240 bytes. The same vectors as `tests/it/hasher.rs`.
const XXH3_LENGTHS: [usize; 13] = [0, 1, 3, 4, 8, 9, 16, 17, 128, 129, 240, 241, 1024];

/// Seeds of the XXH3-64 vectors.
const XXH3_SEEDS: [u64; 3] = [0, 1, 0x9e37_79b9_7f4a_7c15];

/// A seed whose bytes all differ, so byte-order bugs show.
const GOLDEN_SEED: u64 = 0x9e37_79b9_7f4a_7c15;

/// One placement case: a ring, the keys placed on it, and optional filter and bounded-load
/// scenarios on the same ring.
///
/// A cells case (spec section 13) places its keys on a [`Layer`] instead, and ignores the ring
/// settings and bounded-load scenarios. Rings inside a layer are described by cases too, whose
/// names label their panics.
struct Case {
    name: &'static str,
    description: &'static str,
    strategy: Strategy,
    seed: u64,
    replication: u8,
    nodes: Vec<Node>,
    keys: Vec<Vec<u8>>,
    filters: Vec<Filter>,
    bounded: Vec<Scenario>,
    layer: Option<Box<Layer>>,
}

/// Nodes that are down, so that a filter makes them ineligible.
struct Filter {
    down: Vec<&'static str>,
}

/// The inputs of one bounded-load placement, besides the key.
struct Scenario {
    headroom: u16,
    item_load: u64,
    loads: Vec<(&'static str, u64)>,
}

impl Case {
    /// Starts a rendezvous case with seed 0, a replication factor of 3, no nodes, and the
    /// [standard keys](standard_keys).
    fn new(name: &'static str, description: &'static str) -> Self {
        Self {
            name,
            description,
            strategy: Strategy::Rendezvous,
            seed: 0,
            replication: 3,
            nodes: Vec::new(),
            keys: standard_keys(),
            filters: Vec::new(),
            bounded: Vec::new(),
            layer: None,
        }
    }

    /// Starts a cells case on `layer`, with the [standard keys](standard_keys).
    fn cells(name: &'static str, description: &'static str, layer: Layer) -> Self {
        Self {
            layer: Some(Box::new(layer)),
            ..Self::new(name, description)
        }
    }

    fn vnodes(mut self, tokens_per_weight: u32) -> Self {
        let tokens = NonZeroU32::new(tokens_per_weight).expect("catalog token counts are nonzero");
        self.strategy = Strategy::Vnodes(VnodeParams::new(tokens));
        self
    }

    fn seed(mut self, seed: u64) -> Self {
        self.seed = seed;
        self
    }

    fn replication(mut self, replication: u8) -> Self {
        self.replication = replication;
        self
    }

    fn nodes(mut self, nodes: Vec<Node>) -> Self {
        self.nodes = nodes;
        self
    }

    /// Replaces the keys.
    fn keys(mut self, keys: Vec<Vec<u8>>) -> Self {
        self.keys = keys;
        self
    }

    /// Adds the [long keys](long_keys).
    fn long_keys(mut self) -> Self {
        self.keys.extend(long_keys());
        self
    }

    /// Adds a filter scenario in which the nodes `down` are ineligible.
    fn filter(mut self, down: &[&'static str]) -> Self {
        self.filters.push(Filter {
            down: down.to_vec(),
        });
        self
    }

    /// Adds a bounded-load scenario.
    fn bounded(mut self, headroom: u16, item_load: u64, loads: &[(&'static str, u64)]) -> Self {
        self.bounded.push(Scenario {
            headroom,
            item_load,
            loads: loads.to_vec(),
        });
        self
    }

    fn ring(&self) -> Ring {
        let replication = ReplicationFactor::new(self.replication)
            .unwrap_or_else(|| panic!("case {}: the replication factor is zero", self.name));
        Ring::builder()
            .strategy(self.strategy)
            .seed(self.seed)
            .replication_factor(replication)
            .nodes(self.nodes.iter().cloned())
            .build()
            .unwrap_or_else(|error| panic!("case {}: invalid ring: {error}", self.name))
    }

    /// Computes the case's expected outputs with the public API.
    fn expected(&self) -> Value {
        if let Some(layer) = &self.layer {
            return self.cells_expected(layer);
        }
        let ring = self.ring();
        let keys: Vec<Value> = self
            .keys
            .iter()
            .map(|key| json!({"key": hex(key), "replicas": ids(&ring.replicas(key).to_ids())}))
            .collect();
        let mut case = json!({
            "name": self.name,
            "description": self.description,
            "ring": self.ring_json(),
            "fingerprint": ring.fingerprint().to_string(),
            "keys": keys,
        });
        if !self.filters.is_empty() {
            let filters: Vec<Value> = self
                .filters
                .iter()
                .map(|filter| filter.expected(&ring))
                .collect();
            case["filters"] = filters.into();
        }
        if !self.bounded.is_empty() {
            let bounded: Vec<Value> = self
                .bounded
                .iter()
                .map(|scenario| scenario.expected(&ring))
                .collect();
            case["bounded"] = bounded.into();
        }
        case
    }

    /// The case's ring, with its nodes in catalog order.
    fn ring_json(&self) -> Value {
        let strategy = match self.strategy {
            Strategy::Rendezvous => json!({"kind": "rendezvous"}),
            Strategy::Vnodes(params) => json!({
                "kind": "vnodes",
                "tokens_per_weight": params.tokens_per_weight().get(),
            }),
        };
        let nodes: Vec<Value> = self
            .nodes
            .iter()
            .map(|node| {
                let mut row = json!({"id": node.id().as_str(), "weight": node.weight().get()});
                if let Some(zone) = node.zone() {
                    row["zone"] = zone.as_str().into();
                }
                if let Some(host) = node.host() {
                    row["host"] = host.as_str().into();
                }
                row
            })
            .collect();
        json!({
            "hasher": Xxh3.algorithm(),
            "strategy": strategy,
            "seed": hex64(self.seed),
            "replication_factor": self.replication,
            "nodes": nodes,
        })
    }

    /// Computes a cells case's expected outputs with the public API. A layer's cells are all
    /// rings or all nested layers of rings, the shapes `Cells<Ring>` and `Cells<Cells<Ring>>`.
    fn cells_expected(&self, layer: &Layer) -> Value {
        if layer
            .cells
            .iter()
            .all(|(_, cell)| matches!(cell, Member::Ring(_)))
        {
            self.cells_case(layer, &layer.build(self.name, &Member::ring))
        } else {
            let nested = |cell: &Member| cell.layer(self.name).build(self.name, &Member::ring);
            self.cells_case(layer, &layer.build(self.name, &nested))
        }
    }

    /// The expected outputs of a cells case whose layer built as `cells`.
    fn cells_case<P: Placement<Hasher = Xxh3>>(&self, layer: &Layer, cells: &Cells<P>) -> Value {
        let keys: Vec<Value> = self
            .keys
            .iter()
            .map(|key| {
                let placed = cells.replicas(key);
                json!({
                    "key": hex(key),
                    "path": path(&placed),
                    "replicas": ids(&placed.replicas().to_ids()),
                })
            })
            .collect();
        let mut case = json!({
            "name": self.name,
            "description": self.description,
            "layer": layer.json(),
            "fingerprint": cells.fingerprint().to_string(),
            "keys": keys,
        });
        if !self.filters.is_empty() {
            let filters: Vec<Value> = self
                .filters
                .iter()
                .map(|filter| filter.expected_cells(cells))
                .collect();
            case["filters"] = filters.into();
        }
        case
    }
}

/// A cells layer of a cells case (spec section 13): directory rules, an optional selector ring,
/// and cells, each a ring or a nested layer, in the order supplied.
struct Layer {
    exact: Vec<(Vec<u8>, &'static str)>,
    prefixes: Vec<(Vec<u8>, &'static str)>,
    selector: Option<Case>,
    cells: Vec<(&'static str, Member)>,
}

/// One cell of a [`Layer`].
enum Member {
    /// A ring, described by a ring case's settings and nodes.
    Ring(Case),
    /// A nested layer.
    Layer(Layer),
}

impl Layer {
    /// Starts a layer with no rules, no selector ring, and no cells.
    fn new() -> Self {
        Self {
            exact: Vec::new(),
            prefixes: Vec::new(),
            selector: None,
            cells: Vec::new(),
        }
    }

    /// Adds an exact rule.
    fn exact(mut self, key: impl AsRef<[u8]>, cell: &'static str) -> Self {
        self.exact.push((key.as_ref().to_vec(), cell));
        self
    }

    /// Adds a prefix rule; the empty prefix is the catch-all.
    fn prefix(mut self, prefix: impl AsRef<[u8]>, cell: &'static str) -> Self {
        self.prefixes.push((prefix.as_ref().to_vec(), cell));
        self
    }

    /// Sets the selector ring, whose node IDs are cell IDs.
    fn selector(mut self, ring: Case) -> Self {
        self.selector = Some(ring);
        self
    }

    /// Adds a cell that is a ring.
    fn ring(mut self, id: &'static str, ring: Case) -> Self {
        self.cells.push((id, Member::Ring(ring)));
        self
    }

    /// Adds a cell that is a nested layer.
    fn layer(mut self, id: &'static str, layer: Layer) -> Self {
        self.cells.push((id, Member::Layer(layer)));
        self
    }

    /// Builds the layer, building each cell with `cell`.
    fn build<P: Placement<Hasher = Xxh3>>(
        &self,
        case: &str,
        cell: &dyn Fn(&Member) -> P,
    ) -> Cells<P> {
        let mut directory = Directory::new();
        for (key, id) in &self.exact {
            directory = directory.exact(key, label::<CellId>(id));
        }
        for (prefix, id) in &self.prefixes {
            directory = directory.prefix(prefix, label::<CellId>(id));
        }
        let mut builder = Cells::builder().directory(directory);
        if let Some(selector) = &self.selector {
            builder = builder.ring_selector(selector.ring());
        }
        builder
            .cells(
                self.cells
                    .iter()
                    .map(|(id, member)| (label::<CellId>(id), cell(member))),
            )
            .build()
            .unwrap_or_else(|error| panic!("case {case}: invalid layer: {error}"))
    }

    /// The layer as the vector file describes it.
    fn json(&self) -> Value {
        let rules = |rules: &[(Vec<u8>, &'static str)]| -> Map<String, Value> {
            rules
                .iter()
                .map(|(bytes, id)| (hex(bytes), Value::from(*id)))
                .collect()
        };
        let mut layer = Map::new();
        if !self.exact.is_empty() || !self.prefixes.is_empty() {
            layer.insert(
                "directory".to_owned(),
                json!({"exact": rules(&self.exact), "prefix": rules(&self.prefixes)}),
            );
        }
        if let Some(selector) = &self.selector {
            layer.insert("selector".to_owned(), selector.ring_json());
        }
        let cells: Vec<Value> = self
            .cells
            .iter()
            .map(|(id, member)| match member {
                Member::Ring(ring) => json!({"cell": id, "ring": ring.ring_json()}),
                Member::Layer(layer) => json!({"cell": id, "layer": layer.json()}),
            })
            .collect();
        layer.insert("cells".to_owned(), cells.into());
        Value::Object(layer)
    }
}

impl Member {
    /// Builds the cell's ring; the cell must be a ring.
    fn ring(&self) -> Ring {
        match self {
            Self::Ring(ring) => ring.ring(),
            Self::Layer(_) => panic!("a layer whose cells mix rings and layers can't be built"),
        }
    }

    /// Returns the cell's nested layer; the cell must be a layer.
    fn layer(&self, case: &str) -> &Layer {
        match self {
            Self::Layer(layer) => layer,
            Self::Ring(ring) => panic!(
                "case {case}: cell ring {} sits beside nested layers, which can't be built",
                ring.name
            ),
        }
    }
}

impl Filter {
    /// The filtered replica set of every scenario key.
    fn expected(&self, ring: &Ring) -> Value {
        let keys: Vec<Value> = scenario_keys()
            .iter()
            .map(|key| {
                let replicas = ring
                    .replicas_where(key, |node| !self.down.contains(&node.id().as_str()))
                    .map(|replicas| replicas.to_ids())
                    .unwrap_or_default();
                json!({"key": hex(key), "replicas": ids(&replicas)})
            })
            .collect();
        json!({"down": self.down, "keys": keys})
    }

    /// The filtered placement of every scenario key on a cells layer: the key's path, and its
    /// replica set among the nodes that are up, empty when every node of its cell is down.
    fn expected_cells<P: Placement<Hasher = Xxh3>>(&self, cells: &Cells<P>) -> Value {
        let keys: Vec<Value> = scenario_keys()
            .iter()
            .map(|key| {
                let placed = cells.replicas(key);
                let replicas = cells
                    .replicas_where(key, |node| !self.down.contains(&node.id().as_str()))
                    .map(|filtered| filtered.replicas().to_ids())
                    .unwrap_or_default();
                json!({"key": hex(key), "path": path(&placed), "replicas": ids(&replicas)})
            })
            .collect();
        json!({"down": self.down, "keys": keys})
    }
}

impl Scenario {
    /// The capacities, and the bounded-load placement of every scenario key.
    fn expected(&self, ring: &Ring) -> Value {
        let view = BoundedLoads::new(Headroom::from_percent(self.headroom))
            .load_view(ring, self.loads.iter().copied())
            .unwrap_or_else(|error| panic!("invalid catalog loads: {error}"));
        let loads: Map<String, Value> = self
            .loads
            .iter()
            .map(|&(id, load)| (id.to_owned(), load.to_string().into()))
            .collect();
        let capacity: Map<String, Value> = ring
            .nodes()
            .iter()
            .map(|node| {
                let capacity = view
                    .capacity(node.id(), self.item_load)
                    .expect("the view's ring has the node");
                (node.id().to_string(), capacity.to_string().into())
            })
            .collect();
        let keys: Vec<Value> = scenario_keys()
            .iter()
            .map(|key| {
                let placed = view.replicas(key, self.item_load);
                json!({
                    "key": hex(key),
                    "replicas": ids(&placed.replicas().to_ids()),
                    "saturated": placed.is_saturated(),
                    "home": placed.is_home(),
                })
            })
            .collect();
        json!({
            "headroom": self.headroom,
            "item_load": self.item_load.to_string(),
            "loads": loads,
            "capacity": capacity,
            "keys": keys,
        })
    }
}

/// Every case, in file order. Append new cases at the end; never edit or remove one, because
/// the generator refuses to change a case that the file already holds.
fn catalog() -> Vec<Case> {
    vec![
        // Rendezvous: sizes, weights, and seeds.
        Case::new(
            "rendezvous-single-node",
            "One node holds every key, alone, although R is 3.",
        )
        .nodes(nodes(&[("solo", 1, None, None)]))
        .long_keys()
        .filter(&["solo"])
        .bounded(25, 1, &[("solo", 10)])
        .bounded(0, u64::MAX, &[("solo", u64::MAX)]),
        Case::new(
            "rendezvous-two-nodes-r3",
            "Two equal nodes with R = 3: every replica set holds both nodes, in the key's order.",
        )
        .nodes(equal("node", 2))
        .filter(&["node-1"])
        .filter(&["node-1", "node-2"])
        .bounded(0, 1, &[])
        .bounded(25, 5, &[("node-1", 100)]),
        Case::new(
            "rendezvous-equal-weights",
            "Eight nodes of weight 1 under seed 0: equal weights rank by raw draw.",
        )
        .nodes(equal("node", 8))
        .long_keys(),
        Case::new(
            "rendezvous-equal-weights-seed-1",
            "The equal-weight ring under seed 1, which changes every node hash and draw.",
        )
        .nodes(equal("node", 8))
        .seed(1),
        Case::new(
            "rendezvous-equal-weights-seed-golden",
            "The equal-weight ring under a seed whose bytes all differ.",
        )
        .nodes(equal("node", 8))
        .seed(GOLDEN_SEED)
        .long_keys(),
        Case::new(
            "rendezvous-equal-weights-seed-max",
            "The equal-weight ring under seed 2^64 - 1.",
        )
        .nodes(equal("node", 8))
        .seed(u64::MAX),
        Case::new(
            "rendezvous-uniform-weight-7",
            "Six nodes that all weigh 7 rank exactly as if they all weighed 1.",
        )
        .nodes(nodes(&[
            ("w7-a", 7, None, None),
            ("w7-b", 7, None, None),
            ("w7-c", 7, None, None),
            ("w7-d", 7, None, None),
            ("w7-e", 7, None, None),
            ("w7-f", 7, None, None),
        ]))
        .replication(2),
        Case::new(
            "rendezvous-skewed-1-vs-1000",
            "Weights 1 and 1000 with R = 2: the heavy node is almost always the primary.",
        )
        .nodes(nodes(&[("light", 1, None, None), ("heavy", 1000, None, None)]))
        .replication(2),
        Case::new(
            "rendezvous-mixed-weights",
            "Weights 1, 10, 10, 100, and 1000: ranks compare logarithms scaled by weight, and the two nodes of weight 10 may compare raw draws instead (spec section 5).",
        )
        .nodes(nodes(&[
            ("w1000", 1000, None, None),
            ("w10-b", 10, None, None),
            ("w1", 1, None, None),
            ("w100", 100, None, None),
            ("w10-a", 10, None, None),
        ]))
        .seed(42),
        Case::new(
            "rendezvous-homelab",
            "A home lab: two laptops and a NAS weighing 1, 2, and 4, with R = 2 under seed 7.",
        )
        .nodes(nodes(&[
            ("laptop-1", 1, None, None),
            ("laptop-2", 2, None, None),
            ("nas", 4, None, None),
        ]))
        .seed(7)
        .replication(2)
        .filter(&["nas"])
        .filter(&["laptop-1", "laptop-2"])
        .bounded(25, 1, &[("laptop-1", 5), ("laptop-2", 10), ("nas", 40)])
        .bounded(10, 3, &[("laptop-1", 1), ("laptop-2", 30), ("nas", 2)]),
        Case::new(
            "rendezvous-extreme-weights",
            "Weights 1, 2, 2^16, 2^32 - 2, and 2^32 - 1, whose cross products need 128 bits.",
        )
        .nodes(nodes(&[
            ("max", u32::MAX, None, None),
            ("tiny", 1, None, None),
            ("almost-max", u32::MAX - 1, None, None),
            ("small", 2, None, None),
            ("mid", 1 << 16, None, None),
        ]))
        .seed(3),
        Case::new(
            "rendezvous-exact-ties",
            "Keys found by search whose ranks tie exactly at step 1: two equal-weight pairs with equal logarithms, and two pairs of weights 1 and 2 with equal rates. The larger raw draw goes first (spec section 5, step 2).",
        )
        .nodes(nodes(&[
            ("tie-01", 2, None, None),
            ("tie-35", 2, None, None),
            ("tie-07", 2, None, None),
            ("tie-53", 2, None, None),
            ("tie-31", 2, None, None),
            ("tie-36", 1, None, None),
            ("tie-51", 2, None, None),
            ("tie-34", 1, None, None),
        ]))
        .replication(8)
        .keys(
            [
                "tie-key-40508752",
                "tie-key-161397526",
                "tie-key-83378223",
                "tie-key-243690445",
            ]
            .map(|key| key.as_bytes().to_vec())
            .to_vec(),
        ),
        Case::new(
            "rendezvous-r1",
            "R = 1: each replica set is the preferred primary alone.",
        )
        .nodes(zones(&[3, 2]))
        .replication(1),
        Case::new(
            "rendezvous-r255",
            "R = 255 exceeds the 12 nodes, so every replica set is the key's whole preference list: zones first, then hosts, then the rest.",
        )
        .nodes(topology_12())
        .replication(255),
        // Rendezvous: zones and hosts.
        Case::new(
            "rendezvous-zones-fewer-than-r",
            "Two zones of three nodes with R = 3: the first two replicas cover both zones, and the third comes from the host pass.",
        )
        .nodes(zones(&[3, 3])),
        Case::new(
            "rendezvous-zones-equal-r",
            "Three zones of two nodes with R = 3: each replica set covers every zone.",
        )
        .nodes(zones(&[2, 2, 2])),
        Case::new(
            "rendezvous-zones-more-than-r",
            "Five zones of two nodes with R = 3: every replica is in a different zone.",
        )
        .nodes(zones(&[2, 2, 2, 2, 2])),
        Case::new(
            "rendezvous-default-and-named-zones",
            "Nodes without a zone share the default zone, which counts as one zone beside z1 and z2.",
        )
        .nodes(nodes(&[
            ("plain-1", 1, None, None),
            ("plain-2", 1, None, None),
            ("z1-a", 1, Some("z1"), None),
            ("z1-b", 1, Some("z1"), None),
            ("z2-a", 1, Some("z2"), None),
        ]))
        .replication(4),
        Case::new(
            "rendezvous-shared-hosts",
            "One zone; hosts h1, h2, and h3 hold 3, 2, and 1 nodes, and one node is its own host. With R = 4 the host pass picks three replicas.",
        )
        .nodes(nodes(&[
            ("disk-1", 1, None, Some("h1")),
            ("disk-2", 1, None, Some("h1")),
            ("disk-3", 1, None, Some("h1")),
            ("disk-4", 1, None, Some("h2")),
            ("disk-5", 1, None, Some("h2")),
            ("disk-6", 1, None, Some("h3")),
            ("loner", 1, None, None),
        ]))
        .replication(4),
        Case::new(
            "rendezvous-zones-and-hosts",
            "Three zones, each with two shared hosts and one unlabeled node, weights 1 to 4, and R = 5.",
        )
        .nodes(zones_and_hosts())
        .seed(0xdead_beef)
        .replication(5),
        Case::new(
            "rendezvous-skewed-zones",
            "Zones of 10, 10, and 2 nodes with R = 3: one replica of every key lands in the small zone.",
        )
        .nodes(zones(&[10, 10, 2])),
        Case::new(
            "rendezvous-byte-order-ids",
            "IDs supplied out of order, sorted bytewise (node-10 before node-2), using every ID byte and the 1- and 64-byte length limits.",
        )
        .nodes(byte_order_nodes()),
        Case::new(
            "rendezvous-fleet-64",
            "64 nodes in four zones with paired hosts and weights 1 to 4 under seed 42; filters take down zone z1 and three single nodes.",
        )
        .nodes(fleet(64, 4))
        .seed(42)
        .filter(&[
            "z1-n1", "z1-n2", "z1-n3", "z1-n4", "z1-n5", "z1-n6", "z1-n7", "z1-n8", "z1-n9",
            "z1-n10", "z1-n11", "z1-n12", "z1-n13", "z1-n14", "z1-n15", "z1-n16",
        ])
        .filter(&["z2-n3", "z3-n9", "z4-n16"]),
        // Rendezvous: keys.
        Case::new(
            "rendezvous-probe-keys",
            "The 64 fingerprint probe keys, so a port can compare each probe's replica set (spec section 11).",
        )
        .nodes(zones(&[2, 2, 2]))
        .keys(probe_keys()),
        Case::new(
            "rendezvous-key-lengths",
            "Keys of every XXH3 input-length class, 0 to 1024 bytes, mostly not UTF-8.",
        )
        .nodes(equal("node", 4))
        .seed(0x0123_4567_89ab_cdef)
        .keys(length_keys()),
        // Rendezvous: filters.
        Case::new(
            "rendezvous-failover",
            "Three zones of three nodes: filters take down one node, a whole zone, all nodes but one, and every node.",
        )
        .nodes(zones(&[3, 3, 3]))
        .filter(&["z1-n1"])
        .filter(&["z2-n1", "z2-n2", "z2-n3"])
        .filter(&[
            "z1-n1", "z1-n2", "z1-n3", "z2-n1", "z2-n2", "z2-n3", "z3-n1", "z3-n2",
        ])
        .filter(&[
            "z1-n1", "z1-n2", "z1-n3", "z2-n1", "z2-n2", "z2-n3", "z3-n1", "z3-n2", "z3-n3",
        ]),
        // Rendezvous: bounded loads.
        Case::new(
            "rendezvous-bounded-equal",
            "Bounded loads on six equal nodes: room everywhere, one overloaded node, an exact fit at zero headroom, no room anywhere, and room on a single node.",
        )
        .nodes(equal("node", 6))
        .bounded(25, 1, &[])
        .bounded(
            25,
            1,
            &[
                ("node-1", 30),
                ("node-2", 10),
                ("node-3", 10),
                ("node-4", 10),
                ("node-5", 10),
                ("node-6", 10),
            ],
        )
        .bounded(0, 1, &uniform_loads(10))
        .bounded(0, 2, &uniform_loads(10))
        .bounded(
            25,
            20,
            &[
                ("node-2", 50),
                ("node-3", 50),
                ("node-4", 50),
                ("node-5", 50),
                ("node-6", 50),
            ],
        ),
        Case::new(
            "rendezvous-bounded-zones",
            "Bounded loads across three weighted zones: replicas keep zone diversity among the nodes with room, and saturated placements fill from the zone-diverse preference list.",
        )
        .nodes(nodes(&[
            ("z1-big", 2, Some("z1"), None),
            ("z1-small", 1, Some("z1"), None),
            ("z2-big", 2, Some("z2"), None),
            ("z2-small", 1, Some("z2"), None),
            ("z3-only", 3, Some("z3"), None),
        ]))
        .bounded(
            10,
            4,
            &[
                ("z1-big", 40),
                ("z1-small", 5),
                ("z2-big", 20),
                ("z2-small", 30),
                ("z3-only", 35),
            ],
        )
        .bounded(
            50,
            10,
            &[
                ("z1-big", 10),
                ("z1-small", 60),
                ("z2-big", 10),
                ("z2-small", 10),
                ("z3-only", 90),
            ],
        )
        .bounded(
            0,
            10,
            &[
                ("z1-big", 100),
                ("z1-small", 50),
                ("z2-big", 100),
                ("z2-small", 50),
            ],
        ),
        Case::new(
            "rendezvous-bounded-extremes",
            "Bounded loads at the limits: weights up to 2^32 - 1, loads and items near 2^64 so that the total exceeds 2^64, headroom 0, 1, and 65535, and an item load of 0.",
        )
        .nodes(nodes(&[
            ("a", 1 << 31, None, None),
            ("b", 3 << 30, None, None),
            ("c", u32::MAX, None, None),
            ("d", (1 << 30) + 7, None, None),
        ]))
        .bounded(
            65_535,
            u64::MAX,
            &[("a", u64::MAX), ("b", u64::MAX - 1), ("c", 1 << 63), ("d", 0)],
        )
        .bounded(
            0,
            1 << 62,
            &[("a", u64::MAX), ("b", u64::MAX - 1), ("c", 1 << 63), ("d", 0)],
        )
        .bounded(
            1,
            1 << 63,
            &[
                ("a", u64::MAX),
                ("b", u64::MAX),
                ("c", u64::MAX),
                ("d", u64::MAX),
            ],
        )
        .bounded(0, 0, &[]),
        Case::new(
            "rendezvous-bounded-fewer-nodes-than-r",
            "Bounded loads with three nodes and R = 5: placements hold all three nodes, and are saturated only when some replica lacks room.",
        )
        .nodes(equal("node", 3))
        .replication(5)
        .bounded(25, 1, &[])
        .bounded(10, 3, &[("node-1", 9)])
        .bounded(0, 100, &[("node-1", 1), ("node-2", 1), ("node-3", 1)]),
        // Vnodes.
        Case::new(
            "vnodes-single-node",
            "A vnode ring of one node with one token: every key maps to it.",
        )
        .vnodes(1)
        .nodes(nodes(&[("solo", 1, None, None)]))
        .long_keys()
        .filter(&["solo"])
        .bounded(25, 1, &[("solo", 10)]),
        Case::new(
            "vnodes-one-token-each",
            "Six nodes with one token each: plain consistent hashing.",
        )
        .vnodes(1)
        .nodes(equal("node", 6)),
        Case::new(
            "vnodes-weighted",
            "Weights 1, 2, 3, and 5 at two tokens per weight unit: 22 tokens.",
        )
        .vnodes(2)
        .nodes(nodes(&[
            ("w1", 1, None, None),
            ("w2", 2, None, None),
            ("w3", 3, None, None),
            ("w5", 5, None, None),
        ])),
        Case::new(
            "vnodes-zones-fewer-than-r",
            "A vnode ring with two zones of three nodes and R = 3: the third replica comes from the host pass.",
        )
        .vnodes(4)
        .nodes(zones(&[3, 3])),
        Case::new(
            "vnodes-zones-equal-r",
            "A vnode ring with three zones of two nodes and R = 3.",
        )
        .vnodes(4)
        .nodes(zones(&[2, 2, 2])),
        Case::new(
            "vnodes-zones-more-than-r",
            "A vnode ring with five zones and R = 3.",
        )
        .vnodes(3)
        .nodes(zones(&[2, 1, 2, 1, 2])),
        Case::new(
            "vnodes-shared-hosts",
            "A vnode ring in one zone where hosts hold 3, 2, and 1 nodes and one node is its own host, with R = 4.",
        )
        .vnodes(3)
        .nodes(nodes(&[
            ("disk-1", 1, None, Some("h1")),
            ("disk-2", 1, None, Some("h1")),
            ("disk-3", 1, None, Some("h1")),
            ("disk-4", 1, None, Some("h2")),
            ("disk-5", 1, None, Some("h2")),
            ("disk-6", 1, None, Some("h3")),
            ("loner", 1, None, None),
        ]))
        .replication(4),
        Case::new(
            "vnodes-default-tokens",
            "The default 256 tokens per weight unit, with weights 1, 1, 2, and 3: 1792 tokens.",
        )
        .vnodes(256)
        .nodes(nodes(&[
            ("node-a", 1, None, None),
            ("node-b", 1, None, None),
            ("node-c", 2, None, None),
            ("node-d", 3, None, None),
        ]))
        .long_keys(),
        Case::new(
            "vnodes-seed-golden",
            "A vnode ring under a seed whose bytes all differ.",
        )
        .vnodes(8)
        .nodes(equal("node", 5))
        .seed(GOLDEN_SEED)
        .long_keys(),
        Case::new("vnodes-seed-max", "A vnode ring under seed 2^64 - 1.")
            .vnodes(5)
            .nodes(equal("node", 5))
            .seed(u64::MAX),
        Case::new(
            "vnodes-skewed-1-vs-1000",
            "Weights 1 and 1000 at one token per weight unit, with R = 2.",
        )
        .vnodes(1)
        .nodes(nodes(&[("light", 1, None, None), ("heavy", 1000, None, None)]))
        .replication(2),
        Case::new(
            "vnodes-r255",
            "A vnode ring with R = 255 over 12 nodes: every replica set is the whole preference list.",
        )
        .vnodes(2)
        .nodes(topology_12())
        .replication(255),
        Case::new(
            "vnodes-byte-order-ids",
            "A vnode ring whose IDs are supplied out of order and sort bytewise.",
        )
        .vnodes(3)
        .nodes(byte_order_nodes()),
        Case::new(
            "vnodes-failover",
            "A vnode ring of three zones of three nodes: filters take down one node, a whole zone, all nodes but one, and every node.",
        )
        .vnodes(4)
        .nodes(zones(&[3, 3, 3]))
        .filter(&["z1-n1"])
        .filter(&["z2-n1", "z2-n2", "z2-n3"])
        .filter(&[
            "z1-n1", "z1-n2", "z1-n3", "z2-n1", "z2-n2", "z2-n3", "z3-n1", "z3-n2",
        ])
        .filter(&[
            "z1-n1", "z1-n2", "z1-n3", "z2-n1", "z2-n2", "z2-n3", "z3-n1", "z3-n2", "z3-n3",
        ]),
        Case::new(
            "vnodes-bounded",
            "Bounded loads on a vnode ring of six equal nodes: one overloaded node, no room anywhere, and room on a single node.",
        )
        .vnodes(4)
        .nodes(equal("node", 6))
        .bounded(
            25,
            1,
            &[
                ("node-1", 10),
                ("node-2", 10),
                ("node-3", 30),
                ("node-4", 10),
                ("node-5", 10),
                ("node-6", 10),
            ],
        )
        .bounded(0, 2, &uniform_loads(10))
        .bounded(
            25,
            20,
            &[
                ("node-1", 50),
                ("node-2", 50),
                ("node-3", 50),
                ("node-4", 50),
                ("node-6", 50),
            ],
        ),
        Case::new(
            "vnodes-key-lengths",
            "A vnode ring under seed 1 with keys of every XXH3 input-length class.",
        )
        .vnodes(2)
        .nodes(equal("node", 4))
        .seed(1)
        .keys(length_keys()),
        Case::new(
            "vnodes-probe-keys",
            "A vnode ring with the 64 fingerprint probe keys.",
        )
        .vnodes(4)
        .nodes(zones(&[2, 2, 2]))
        .keys(probe_keys()),
        Case::new("vnodes-r1", "A vnode ring with R = 1 and 16 tokens per weight unit.")
            .vnodes(16)
            .nodes(equal("node", 8))
            .replication(1),
        Case::new(
            "vnodes-zones-and-hosts",
            "A vnode ring with three zones, each with two shared hosts and one unlabeled node, weights 1 to 4, and R = 5.",
        )
        .vnodes(2)
        .nodes(zones_and_hosts())
        .seed(0xdead_beef)
        .replication(5),
        // Cells (spec section 13).
        Case::cells(
            "cells-directory-catch-all",
            "A directory alone: an exact rule beats every prefix rule, the longest matching prefix wins, matching is bytewise and case-sensitive, and the catch-all (the empty prefix) takes every other key. The empty key has an exact rule of its own.",
            Layer::new()
                .exact(b"", "eu-vip")
                .exact(b"eu:vip:globex", "eu")
                .exact(b"tenant:acme", "eu")
                .prefix(b"eu:", "eu")
                .prefix(b"eu:vip:", "eu-vip")
                .prefix([0xff], "eu-vip")
                .prefix(b"", "us")
                .ring(
                    "us",
                    ring("us").seed(11).nodes(cell_nodes("us", &[2, 2, 2])),
                )
                .ring(
                    "eu",
                    ring("eu").vnodes(8).seed(12).nodes(cell_nodes("eu", &[2, 2])),
                )
                .ring(
                    "eu-vip",
                    ring("eu-vip").seed(13).replication(2).nodes(nodes(&[
                        ("vip-a", 2, None, Some("vip-h1")),
                        ("vip-b", 1, None, Some("vip-h1")),
                        ("vip-c", 1, None, None),
                    ])),
                ),
        )
        .keys(
            [
                &b""[..],
                b"eu:tenant:1",
                b"eu:vip:initech",
                b"eu:vip:globex",
                b"eu:vip:",
                b"eu:vip",
                b"eu:",
                b"eu",
                b"EU:tenant:1",
                b"tenant:acme",
                b"tenant:acme:2",
                &[0xff],
                &[0xff, 0xfe],
                &[0xfe, 0xff],
                &[0x00],
                "café".as_bytes(),
                b"us:tenant:9",
            ]
            .map(<[u8]>::to_vec)
            .to_vec(),
        ),
        Case::cells(
            "cells-ring-only",
            "A selector ring alone, whose nodes are cells weighing 1, 2, and 3: the key's preferred primary on the selector ring is its cell, and the cell's own ring, under a seed of its own, places the replicas.",
            Layer::new()
                .selector(ring("selector").seed(GOLDEN_SEED).nodes(nodes(&[
                    ("cell-c", 3, None, None),
                    ("cell-a", 1, None, None),
                    ("cell-b", 2, None, None),
                ])))
                .ring(
                    "cell-a",
                    ring("cell-a").seed(1).nodes(cell_nodes("a", &[2, 2, 2])),
                )
                .ring(
                    "cell-b",
                    ring("cell-b").vnodes(4).seed(2).nodes(cell_nodes("b", &[3, 3])),
                )
                .ring(
                    "cell-c",
                    ring("cell-c").seed(3).replication(2).nodes(nodes(&[
                        ("c-n1", 1, None, Some("c-h1")),
                        ("c-n2", 2, None, Some("c-h1")),
                        ("c-n3", 1, None, Some("c-h2")),
                        ("c-n4", 3, None, Some("c-h2")),
                    ])),
                ),
        )
        .keys(
            standard_keys()
                .into_iter()
                .chain((0..16).map(|i| format!("tenant:{i}").into_bytes()))
                .collect(),
        ),
        Case::cells(
            "cells-directory-and-ring",
            "Directory rules first, then a vnode selector ring for every key no rule matches. Filters take down nodes inside cells but never change a key's cell: a key whose cell is entirely down has no replicas.",
            Layer::new()
                .prefix(b"eu:", "eu")
                .prefix(b"us:", "us")
                .exact(b"room:general", "ap")
                .exact(b"user:42", "ap")
                .selector(ring("selector").vnodes(16).seed(7).nodes(nodes(&[
                    ("eu", 1, None, None),
                    ("us", 2, None, None),
                    ("ap", 1, None, None),
                ])))
                .ring(
                    "eu",
                    ring("eu").seed(101).nodes(cell_nodes("eu", &[2, 2, 2])),
                )
                .ring(
                    "us",
                    ring("us").seed(102).nodes(cell_nodes("us", &[3, 3])),
                )
                .ring(
                    "ap",
                    ring("ap").vnodes(8).seed(103).nodes(cell_nodes("ap", &[1, 1])),
                ),
        )
        .keys(
            standard_keys()
                .into_iter()
                .chain(
                    ["eu:tenant:1", "us:tenant:2", "eu:", "us:", "ap:tenant:3"]
                        .map(|key| key.as_bytes().to_vec()),
                )
                .collect(),
        )
        .filter(&["ap-n1", "ap-n2"])
        .filter(&["eu-n1", "us-n1", "us-n4"]),
        Case::cells(
            "cells-nested-mixed-strategies",
            "Two layers of cells, region then cell: a residency directory with a rendezvous ring fallback picks the region; eu picks its cell with a vnode selector ring alone, and us with a directory and a rendezvous ring; the leaf rings mix rendezvous and vnodes. Each path names the key's region, then its cell.",
            Layer::new()
                .prefix(b"eu:", "eu")
                .prefix(b"us:", "us")
                .selector(ring("regions").seed(0x0123).nodes(nodes(&[
                    ("us", 1, None, None),
                    ("eu", 1, None, None),
                ])))
                .layer(
                    "eu",
                    Layer::new()
                        .selector(ring("eu").vnodes(4).seed(0x0e00).nodes(nodes(&[
                            ("eu-1", 1, None, None),
                            ("eu-2", 2, None, None),
                        ])))
                        .ring(
                            "eu-1",
                            ring("eu-1").seed(0x0e01).nodes(cell_nodes("eu1", &[2, 2])),
                        )
                        .ring(
                            "eu-2",
                            ring("eu-2")
                                .vnodes(8)
                                .seed(0x0e02)
                                .nodes(cell_nodes("eu2", &[2, 2, 2])),
                        ),
                )
                .layer(
                    "us",
                    Layer::new()
                        .exact(b"us:tenant:big", "us-2")
                        .prefix(b"us:priority:", "us-2")
                        .selector(ring("us").seed(0x0f00).nodes(nodes(&[
                            ("us-1", 3, None, None),
                            ("us-2", 1, None, None),
                        ])))
                        .ring(
                            "us-1",
                            ring("us-1").vnodes(2).seed(0x0f01).nodes(cell_nodes("us1", &[3])),
                        )
                        .ring(
                            "us-2",
                            ring("us-2")
                                .seed(0x0f02)
                                .replication(2)
                                .nodes(cell_nodes("us2", &[1, 1, 1])),
                        ),
                ),
        )
        .keys(
            standard_keys()
                .into_iter()
                .chain(
                    [
                        "eu:tenant:1",
                        "eu:tenant:2",
                        "eu:tenant:3",
                        "eu:tenant:4",
                        "us:tenant:big",
                        "us:priority:7",
                        "us:tenant:5",
                        "us:tenant:6",
                    ]
                    .map(|key| key.as_bytes().to_vec()),
                )
                .collect(),
        ),
    ]
}

/// Builds nodes from `(id, weight, zone, host)` rows, where `None` is the default zone or the
/// node's own host.
fn nodes(rows: &[(&str, u32, Option<&str>, Option<&str>)]) -> Vec<Node> {
    rows.iter()
        .map(|&(id, weight, zone, host)| {
            let mut node = Node::new(label(id)).with_weight(
                Weight::new(weight).unwrap_or_else(|| panic!("node {id} has weight 0")),
            );
            if let Some(zone) = zone {
                node = node.with_zone(label::<Zone>(zone));
            }
            if let Some(host) = host {
                node = node.with_host(label::<Host>(host));
            }
            node
        })
        .collect()
}

/// `count` nodes of weight 1, named `{prefix}-1` to `{prefix}-{count}`.
fn equal(prefix: &str, count: usize) -> Vec<Node> {
    (1..=count)
        .map(|i| Node::new(label(&format!("{prefix}-{i}"))))
        .collect()
}

/// Weight-1 nodes in zones `z1`, `z2`, …, with `sizes[k]` nodes, named `z{k+1}-n1` onward, in
/// zone `k + 1`.
fn zones(sizes: &[usize]) -> Vec<Node> {
    sizes
        .iter()
        .zip(1..)
        .flat_map(|(&size, zone)| {
            (1..=size).map(move |i| {
                Node::new(label(&format!("z{zone}-n{i}"))).with_zone(label(&format!("z{zone}")))
            })
        })
        .collect()
}

/// `count` nodes dealt round-robin into zones `z1` to `z{zones}`. Within a zone, consecutive
/// pairs share a host, and weights cycle through 1, 2, 3, and 4.
fn fleet(count: usize, zones: usize) -> Vec<Node> {
    (0..count)
        .map(|i| {
            let zone = i % zones + 1;
            let rank = i / zones;
            let weight = u32::try_from(rank % 4 + 1).expect("weights are at most 4");
            Node::new(label(&format!("z{zone}-n{}", rank + 1)))
                .with_weight(Weight::new(weight).expect("weights are at least 1"))
                .with_zone(label(&format!("z{zone}")))
                .with_host(label(&format!("z{zone}-h{}", rank / 2 + 1)))
        })
        .collect()
}

/// Twelve nodes in zones z1, z2, and the default zone, with shared hosts, unlabeled nodes, and
/// weights from 1 to 5.
fn topology_12() -> Vec<Node> {
    nodes(&[
        ("a1", 1, Some("z1"), Some("rack-a")),
        ("a2", 2, Some("z1"), Some("rack-a")),
        ("a3", 1, Some("z1"), Some("rack-b")),
        ("a4", 3, Some("z1"), None),
        ("b1", 1, Some("z2"), Some("rack-c")),
        ("b2", 1, Some("z2"), Some("rack-c")),
        ("b3", 5, Some("z2"), Some("rack-c")),
        ("b4", 1, Some("z2"), None),
        ("c1", 2, None, Some("rack-d")),
        ("c2", 2, None, Some("rack-d")),
        ("c3", 1, None, None),
        ("c4", 4, None, None),
    ])
}

/// Three zones, each with two hosts of two nodes and one unlabeled node, weights 1 to 4.
fn zones_and_hosts() -> Vec<Node> {
    nodes(&[
        ("east-1", 1, Some("east"), Some("east-h1")),
        ("east-2", 2, Some("east"), Some("east-h1")),
        ("east-3", 3, Some("east"), Some("east-h2")),
        ("east-4", 4, Some("east"), Some("east-h2")),
        ("east-5", 1, Some("east"), None),
        ("west-1", 2, Some("west"), Some("west-h1")),
        ("west-2", 3, Some("west"), Some("west-h1")),
        ("west-3", 4, Some("west"), Some("west-h2")),
        ("west-4", 1, Some("west"), Some("west-h2")),
        ("west-5", 2, Some("west"), None),
        ("north-1", 3, Some("north"), Some("north-h1")),
        ("north-2", 4, Some("north"), Some("north-h1")),
        ("north-3", 1, Some("north"), Some("north-h2")),
        ("north-4", 2, Some("north"), Some("north-h2")),
        ("north-5", 3, Some("north"), None),
    ])
}

/// Nodes supplied out of canonical order: IDs that sort differently bytewise and numerically,
/// every allowed ID byte, and IDs of 1 and 64 bytes.
fn byte_order_nodes() -> Vec<Node> {
    nodes(&[
        ("node-2", 1, None, None),
        ("node-10", 1, None, None),
        ("z", 1, None, None),
        ("node-1", 1, None, None),
        (
            "abcdefghijklmnopqrstuvwxyz0123456789._:-0123456789abcdefghijklmn",
            1,
            None,
            None,
        ),
        ("node-9", 1, None, None),
        ("a.b_c:d-e", 1, None, None),
        ("0", 1, None, None),
        ("node-20", 1, None, None),
    ])
}

/// A ring for a cells layer, as a ring case named `name` with no keys of its own. Selector rings
/// name their cells with nodes.
fn ring(name: &'static str) -> Case {
    Case::new(name, "").keys(Vec::new())
}

/// Weight-1 nodes of one cell in zones `{prefix}-z1`, `{prefix}-z2`, …, with `sizes[k]` nodes in
/// zone `k + 1`, named `{prefix}-n1` onward across the zones. The prefix keeps node IDs unique
/// across cells, so a filter's down nodes name one node each.
fn cell_nodes(prefix: &str, sizes: &[usize]) -> Vec<Node> {
    sizes
        .iter()
        .zip(1..)
        .flat_map(|(&size, zone)| (0..size).map(move |_| zone))
        .zip(1..)
        .map(|(zone, i)| {
            Node::new(label(&format!("{prefix}-n{i}")))
                .with_zone(label(&format!("{prefix}-z{zone}")))
        })
        .collect()
}

/// A placement's cell path, as IDs.
fn path<'a>(placed: &CellPlacement<'a>) -> Vec<&'a str> {
    placed.path().iter().map(|cell| cell.as_str()).collect()
}

/// Loads of `load` on each of the six nodes `node-1` to `node-6`.
fn uniform_loads(load: u64) -> [(&'static str, u64); 6] {
    [
        ("node-1", load),
        ("node-2", load),
        ("node-3", load),
        ("node-4", load),
        ("node-5", load),
        ("node-6", load),
    ]
}

/// Parses a catalog label.
fn label<T: std::str::FromStr<Err = crate::ParseLabelError>>(text: &str) -> T {
    text.parse()
        .unwrap_or_else(|error| panic!("catalog label {text:?} is invalid: {error}"))
}

/// The keys most cases place: empty, a NUL byte, invalid UTF-8, non-ASCII UTF-8, keys that
/// differ only in case, and typical keys.
fn standard_keys() -> Vec<Vec<u8>> {
    let mut keys: Vec<Vec<u8>> = vec![
        Vec::new(),
        vec![0x00],
        vec![0xff, 0xfe],
        vec![0xc3, 0x28],
        "café".as_bytes().to_vec(),
        b"agent:sam-researcher".to_vec(),
        b"room:general".to_vec(),
        b"tenant:acme".to_vec(),
        b"TENANT:ACME".to_vec(),
        b"user:42".to_vec(),
    ];
    keys.extend((0..6).map(|i| format!("key-{i}").into_bytes()));
    keys
}

/// Keys of 100 and 200 bytes, then a 1 KiB key that isn't UTF-8.
fn long_keys() -> Vec<Vec<u8>> {
    vec![
        format!("tenant:{}", "a".repeat(93)).into_bytes(),
        format!("tenant:{}", "b".repeat(193)).into_bytes(),
        pattern(1024),
    ]
}

/// The keys of filter and bounded-load scenarios.
fn scenario_keys() -> Vec<Vec<u8>> {
    let mut keys: Vec<Vec<u8>> = vec![
        Vec::new(),
        vec![0xff, 0xfe],
        "café".as_bytes().to_vec(),
        b"agent:sam-researcher".to_vec(),
        b"room:general".to_vec(),
        b"tenant:acme".to_vec(),
        b"user:42".to_vec(),
    ];
    keys.extend((0..5).map(|i| format!("key-{i}").into_bytes()));
    keys
}

/// The fingerprint's probe keys, `fleetring-probe-00` to `fleetring-probe-63`.
fn probe_keys() -> Vec<Vec<u8>> {
    (0..64)
        .map(|i| format!("fleetring-probe-{i:02}").into_bytes())
        .collect()
}

/// Pattern keys whose lengths cover every XXH3 code path.
fn length_keys() -> Vec<Vec<u8>> {
    [
        0, 1, 2, 3, 4, 7, 8, 9, 15, 16, 17, 64, 128, 129, 200, 240, 241, 512, 1024,
    ]
    .into_iter()
    .map(pattern)
    .collect()
}

/// `len` bytes where byte `i` is `(31 * i + 7) mod 256`, the input of the XXH3 vectors.
fn pattern(len: usize) -> Vec<u8> {
    std::iter::successors(Some(7u8), |byte| Some(byte.wrapping_add(31)))
        .take(len)
        .collect()
}

/// Lowercase hex of `bytes`.
fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

/// A 64-bit value as 16 lowercase hex digits.
fn hex64(value: u64) -> String {
    format!("{value:016x}")
}

/// Parses 16 lowercase hex digits.
fn parse_hex64(text: &str) -> Option<u64> {
    let valid = text.len() == 16
        && text
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte));
    if !valid {
        return None;
    }
    u64::from_str_radix(text, 16).ok()
}

/// Node IDs as strings, in order.
fn ids(list: &[crate::NodeId]) -> Vec<&str> {
    list.iter().map(crate::NodeId::as_str).collect()
}

/// The XXH3-64 vectors.
fn xxh3_section() -> Value {
    let cases: Vec<Value> = XXH3_LENGTHS
        .iter()
        .flat_map(|&len| {
            XXH3_SEEDS.iter().map(move |&seed| {
                json!({
                    "len": len,
                    "seed": hex64(seed),
                    "digest": hex64(Xxh3.hash(&pattern(len), seed)),
                })
            })
        })
        .collect();
    json!({
        "input": "byte i of the input is (31 * i + 7) mod 256",
        "cases": cases,
    })
}

/// `T[j]`, read back through [`neg_log2_q32`] so that the vectors pin the table the algorithm
/// actually uses.
///
/// For `x = 2⁶³ + j · 2⁵³ − 1`, `x + 1` has exponent 63, table index `j`, and nothing left to
/// interpolate, so `L(x) = 2³² − T[j]` exactly (spec §4). For `j = 1024`, `x = 2⁶⁴ − 1` and
/// `L(x) = 0 = 2³² − T[1024]`.
fn table_entry(j: u64) -> u64 {
    assert!(j <= 1024, "the table has 1025 entries, not {}", j + 1);
    let x = (1u64 << 63) - 1 + (j << 53);
    (1u64 << 32) - neg_log2_q32(x)
}

fn log2_table() -> Vec<u64> {
    (0..=1024).map(table_entry).collect()
}

/// The `L(x)` vectors: the extremes, powers of two and their neighbors, table entries that need
/// no interpolation, and pseudo-random values of every magnitude.
fn neg_log2_inputs() -> Vec<u64> {
    let mut inputs = vec![0, 1, 2, 3, u64::MAX - 2, u64::MAX - 1, u64::MAX];
    for k in [8, 16, 31, 32, 33, 52, 53, 54, 62, 63] {
        let power = 1u64 << k;
        inputs.extend([power - 2, power - 1, power]);
    }
    // T[684] sits closest to a rounding boundary; j = 1023 interpolates up to T[1024] = 2³².
    let top = (1u64 << 63) - 1;
    inputs.extend([
        top + (684 << 53),
        top + (1023 << 53),
        top + (1023 << 53) + (1 << 52),
    ]);
    let mut state = GOLDEN_SEED;
    for shift in [
        0, 1, 2, 3, 5, 7, 9, 11, 13, 17, 19, 23, 27, 29, 31, 35, 37, 41, 43, 47, 51, 53, 57, 59, 61,
    ] {
        inputs.push(splitmix64(&mut state) >> shift);
    }
    inputs.sort_unstable();
    inputs.dedup();
    inputs
}

/// SplitMix64, for the pseudo-random `L(x)` inputs.
fn splitmix64(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9e37_79b9_7f4a_7c15);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

fn neg_log2_section() -> Value {
    neg_log2_inputs()
        .into_iter()
        .map(|x| json!({"x": hex64(x), "l": neg_log2_q32(x)}))
        .collect()
}

/// The canonical form of `value`: compact JSON, with every object's keys sorted bytewise.
///
/// The freeze guard hashes this form, so the file's whitespace and field order can change
/// without unfreezing anything, while any change to a value, a key, or an array's order is
/// caught.
fn canonical(value: &Value, out: &mut String) {
    match value {
        Value::Array(items) => {
            out.push('[');
            for (position, item) in items.iter().enumerate() {
                if position > 0 {
                    out.push(',');
                }
                canonical(item, out);
            }
            out.push(']');
        }
        Value::Object(fields) => {
            let mut entries: Vec<(&String, &Value)> = fields.iter().collect();
            entries.sort_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()));
            out.push('{');
            for (position, (key, item)) in entries.into_iter().enumerate() {
                if position > 0 {
                    out.push(',');
                }
                out.push_str(&quote(key));
                out.push(':');
                canonical(item, out);
            }
            out.push('}');
        }
        scalar => out.push_str(&scalar.to_string()),
    }
}

/// The digest a checkpoint records: XXH3-64, seed 0, of the canonical form of the reference
/// sections and the first `count` cases.
fn frozen_digest(vectors: &Value, count: usize) -> u64 {
    let mut frozen = Map::new();
    for section in REFERENCE_SECTIONS {
        frozen.insert(section.to_owned(), vectors[section].clone());
    }
    let cases = vectors["cases"].as_array().map_or(&[][..], Vec::as_slice);
    frozen.insert("cases".to_owned(), cases[..count].to_vec().into());
    let mut text = String::new();
    canonical(&Value::Object(frozen), &mut text);
    Xxh3.hash(text.as_bytes(), 0)
}

/// One line of the lock: the first `count` cases hash to `digest`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Checkpoint {
    count: usize,
    digest: u64,
}

/// Parses the lock's checkpoint lines, skipping blank lines and `#` comments.
fn checkpoints(lock: &str) -> Result<Vec<Checkpoint>, String> {
    lock.lines()
        .enumerate()
        .filter(|(_, line)| {
            let line = line.trim();
            !line.is_empty() && !line.starts_with('#')
        })
        .map(|(number, line)| {
            let mut fields = line.split_whitespace();
            let (Some(count), Some(digest), None) = (fields.next(), fields.next(), fields.next())
            else {
                return Err(format!(
                    "{LOCK_PATH}:{}: expected `<count> <digest>`, found {line:?}",
                    number + 1
                ));
            };
            let count = count.parse().ok().filter(|&count| count > 0);
            match (count, parse_hex64(digest)) {
                (Some(count), Some(digest)) => Ok(Checkpoint { count, digest }),
                _ => Err(format!(
                    "{LOCK_PATH}:{}: expected a positive case count and 16 lowercase hex \
                     digits, found {line:?}",
                    number + 1
                )),
            }
        })
        .collect()
}

/// Checks every checkpoint of `lock` against `vectors`, and returns how many cases the last one
/// freezes (0 if the lock has none).
fn verify_checkpoints(vectors: &Value, lock: &str) -> Result<usize, String> {
    let cases = vectors["cases"].as_array().map_or(0, Vec::len);
    let mut verified = 0;
    for Checkpoint { count, digest } in checkpoints(lock)? {
        if count <= verified {
            return Err(format!(
                "{LOCK_PATH}: checkpoint `{count} {digest:016x}` follows one for {verified} \
                 cases; checkpoints only ever grow"
            ));
        }
        if count > cases {
            return Err(format!(
                "{LOCK_PATH} freezes {count} cases, but {VECTORS_PATH} holds only {cases}: \
                 frozen cases are never removed (ADR-0008)"
            ));
        }
        let actual = frozen_digest(vectors, count);
        if actual != digest {
            let changed = if verified == 0 {
                format!("the reference sections or cases 0 to {}", count - 1)
            } else {
                format!("cases {verified} to {}", count - 1)
            };
            return Err(format!(
                "{changed} of {VECTORS_PATH} changed: they hash to {actual:016x}, but \
                 {LOCK_PATH} froze them as {digest:016x}. Golden vectors are append-only \
                 (ADR-0008): restore the frozen cases, and add new cases at the end instead. \
                 A change to frozen expectations means placement changed, which needs a new \
                 placement spec version."
            ));
        }
        verified = count;
    }
    Ok(verified)
}

/// The vector file and lock after merging the catalog into them.
#[derive(Debug)]
struct Merged {
    json: String,
    lock: String,
    kept: usize,
    appended: usize,
}

/// The lock's header, written when the generator creates the lock.
const LOCK_HEADER: &str = "\
# Freeze guard for placement-v1.json: the golden vectors are append-only (ADR-0008, FR-014).
#
# Each line `<count> <digest>` is a checkpoint that freezes the file's xxh3_64, log2_table, and
# neg_log2_q32 sections and its first <count> cases. <digest> is the XXH3-64, seed 0, of their
# canonical form (compact JSON with object keys sorted bytewise; see
# crates/fleetring/src/vectors.rs) as 16 lowercase hex digits. The unit tests check every line,
# so editing, reordering, or removing a frozen case fails, while appending a case doesn't.
#
# This file is append-only too: never edit or remove a line. To add cases, append them to the
# catalog in crates/fleetring/src/vectors.rs and run
#
#     FLEETRING_WRITE_VECTORS=1 cargo test -p fleetring --all-features --lib vectors -- --ignored
#
# which appends the new cases to placement-v1.json and one checkpoint covering them here.
";

/// Merges the catalog into the vector file and lock: keeps the file's cases byte for byte,
/// appends the catalog's new cases, and appends a checkpoint that freezes them.
///
/// Fails without merging anything if the file breaks its lock, if the file's layout is not the
/// one this module writes, or if a case already in the file would change.
fn merge(json: &str, lock: &str) -> Result<Merged, String> {
    let json = json.replace("\r\n", "\n");
    let lock = lock.replace("\r\n", "\n");
    let existing: Value = serde_json::from_str(&json)
        .map_err(|error| format!("{VECTORS_PATH} is not valid JSON: {error}"))?;
    let reference = reference_sections();
    // A file without cases is a placeholder for the first run.
    let (existing_cases, blocks) = match existing.get("cases") {
        None => (Vec::new(), Vec::new()),
        Some(cases) => {
            let cases = cases
                .as_array()
                .ok_or_else(|| format!("{VECTORS_PATH}: `cases` is not an array"))?
                .clone();
            for section in REFERENCE_SECTIONS {
                if existing[section] != reference[section] {
                    return Err(format!(
                        "{VECTORS_PATH}: the `{section}` section differs from what the \
                         generator computes; reference vectors never change (ADR-0008)"
                    ));
                }
            }
            let blocks = case_blocks(&json)?;
            if blocks.len() != cases.len() {
                return Err(format!(
                    "{VECTORS_PATH}: found {} case blocks for {} cases; the generator keeps \
                     cases byte for byte only in the layout it writes",
                    blocks.len(),
                    cases.len()
                ));
            }
            for (index, (block, case)) in blocks.iter().zip(&cases).enumerate() {
                let parsed: Value = serde_json::from_str(block)
                    .map_err(|error| format!("{VECTORS_PATH}: case {index}: {error}"))?;
                if &parsed != case {
                    return Err(format!(
                        "{VECTORS_PATH}: case block {index} does not match case {index}"
                    ));
                }
            }
            (cases, blocks)
        }
    };
    let frozen = verify_checkpoints(&existing, &lock)?;

    let catalog = catalog();
    let mut names: Vec<&str> = catalog.iter().map(|case| case.name).collect();
    names.sort_unstable();
    if let Some(pair) = names.windows(2).find(|pair| pair[0] == pair[1]) {
        return Err(format!("the catalog names two cases {:?}", pair[0]));
    }
    if existing_cases.len() > catalog.len() {
        return Err(format!(
            "{VECTORS_PATH} holds {} cases but the catalog only {}: cases are never removed",
            existing_cases.len(),
            catalog.len()
        ));
    }
    let generated: Vec<Value> = catalog.iter().map(Case::expected).collect();
    for (index, (existing, generated)) in existing_cases.iter().zip(&generated).enumerate() {
        if existing != generated {
            return Err(format!(
                "case {index} ({}) would change. Golden vectors are append-only (ADR-0008): \
                 if the catalog entry changed, restore it and append a new case instead; if \
                 placement changed, that needs a new placement spec version.",
                existing["name"]
            ));
        }
    }

    let mut texts: Vec<String> = blocks.iter().map(|&block| block.to_owned()).collect();
    texts.extend(generated[existing_cases.len()..].iter().map(render_case));
    let rendered = render_file(&texts);
    let parsed: Value = serde_json::from_str(&rendered).expect("the generator writes valid JSON");
    assert_eq!(
        parsed["cases"],
        Value::Array(generated.clone()),
        "the rendered cases parse back to the generated ones"
    );

    let mut new_lock = if lock.trim().is_empty() {
        LOCK_HEADER.to_owned()
    } else {
        lock.clone()
    };
    if !new_lock.ends_with('\n') {
        new_lock.push('\n');
    }
    if generated.len() > frozen {
        let digest = frozen_digest(&parsed, generated.len());
        new_lock.push_str(&format!("{} {digest:016x}\n", generated.len()));
    }
    Ok(Merged {
        json: rendered,
        lock: new_lock,
        kept: existing_cases.len(),
        appended: generated.len() - existing_cases.len(),
    })
}

/// The reference sections, keyed by name.
fn reference_sections() -> Value {
    json!({
        "xxh3_64": xxh3_section(),
        "log2_table": log2_table(),
        "neg_log2_q32": neg_log2_section(),
    })
}

/// Returns the exact text of each case in a file this module wrote: the lines from `    {` to
/// `    }`, without the comma that separates cases.
fn case_blocks(json: &str) -> Result<Vec<&str>, String> {
    const START: &str = "\n  \"cases\": [\n";
    let start = json
        .find(START)
        .ok_or_else(|| format!("{VECTORS_PATH}: no `cases` array at the top level"))?;
    let body = &json[start + START.len()..];
    let mut blocks = Vec::new();
    let mut open = None;
    let mut offset = 0;
    for line in body.split_inclusive('\n') {
        match line.trim_end_matches('\n') {
            "    {" if open.is_none() => open = Some(offset),
            "    }" | "    }," => {
                let begin = open
                    .take()
                    .ok_or_else(|| format!("{VECTORS_PATH}: unbalanced case blocks"))?;
                blocks.push(&body[begin..offset + "    }".len()]);
            }
            "  ]" if open.is_none() => return Ok(blocks),
            _ => {}
        }
        offset += line.len();
    }
    Err(format!("{VECTORS_PATH}: the `cases` array never ends"))
}

/// Renders the vector file around case texts that [`render_case`] wrote.
fn render_file(cases: &[String]) -> String {
    let mut out = String::from("{\n");
    out.push_str(&format!("  \"spec\": {},\n", quote(SPEC)));
    out.push_str(&format!(
        "  \"generated_by\": {},\n",
        quote(GENERATE_COMMAND)
    ));
    let layout: Vec<String> = LAYOUT
        .iter()
        .map(|line| format!("    {}", quote(line)))
        .collect();
    out.push_str(&format!("  \"layout\": [\n{}\n  ],\n", layout.join(",\n")));
    out.push_str("  \"xxh3_64\": ");
    write_block(&mut out, &xxh3_section(), 2, Some("xxh3_64"));
    let rows: Vec<String> = log2_table()
        .chunks(8)
        .map(|row| {
            let row: Vec<String> = row.iter().map(u64::to_string).collect();
            format!("    {}", row.join(", "))
        })
        .collect();
    out.push_str(&format!(
        ",\n  \"log2_table\": [\n{}\n  ],\n",
        rows.join(",\n")
    ));
    out.push_str("  \"neg_log2_q32\": ");
    write_block(&mut out, &neg_log2_section(), 2, Some("neg_log2_q32"));
    out.push_str(&format!(
        ",\n  \"cases\": [\n{}\n  ]\n}}\n",
        cases.join(",\n")
    ));
    out
}

/// Renders one case at the indentation of the `cases` array.
fn render_case(case: &Value) -> String {
    let mut out = String::from("    ");
    write_block(&mut out, case, 4, None);
    out
}

/// Writes `value`, whose first line starts at the cursor and whose later lines are indented by
/// `indent`. Objects and arrays put one field or element per line, except that a value goes on
/// one line when it holds only scalars and arrays of scalars. `name` is the field that holds
/// `value`, if any.
fn write_block(out: &mut String, value: &Value, indent: usize, name: Option<&str>) {
    if is_inline(value) {
        write_inline(out, value, name);
        return;
    }
    let inner = " ".repeat(indent + 2);
    match value {
        Value::Array(items) => {
            out.push('[');
            for (position, item) in items.iter().enumerate() {
                out.push_str(if position == 0 { "\n" } else { ",\n" });
                out.push_str(&inner);
                write_block(out, item, indent + 2, None);
            }
            out.push_str(&format!("\n{}]", " ".repeat(indent)));
        }
        Value::Object(fields) => {
            out.push('{');
            for (position, (key, item)) in ordered(fields, name).into_iter().enumerate() {
                out.push_str(if position == 0 { "\n" } else { ",\n" });
                out.push_str(&format!("{inner}{}: ", quote(key)));
                write_block(out, item, indent + 2, Some(key));
            }
            out.push_str(&format!("\n{}}}", " ".repeat(indent)));
        }
        scalar => out.push_str(&scalar.to_string()),
    }
}

/// Writes `value` on one line, with a space after each `,` and `:`.
fn write_inline(out: &mut String, value: &Value, name: Option<&str>) {
    match value {
        Value::Array(items) => {
            out.push('[');
            for (position, item) in items.iter().enumerate() {
                if position > 0 {
                    out.push_str(", ");
                }
                write_inline(out, item, None);
            }
            out.push(']');
        }
        Value::Object(fields) => {
            out.push('{');
            for (position, (key, item)) in ordered(fields, name).into_iter().enumerate() {
                if position > 0 {
                    out.push_str(", ");
                }
                out.push_str(&format!("{}: ", quote(key)));
                write_inline(out, item, Some(key));
            }
            out.push('}');
        }
        scalar => out.push_str(&scalar.to_string()),
    }
}

fn is_scalar(value: &Value) -> bool {
    !matches!(value, Value::Array(_) | Value::Object(_))
}

/// Whether `value` goes on one line: it holds only scalars and arrays of scalars.
fn is_inline(value: &Value) -> bool {
    let flat = |value: &Value| match value {
        Value::Array(items) => items.iter().all(is_scalar),
        other => is_scalar(other),
    };
    match value {
        Value::Array(items) => items.iter().all(is_scalar),
        Value::Object(fields) => fields.values().all(flat),
        _ => true,
    }
}

/// An object's fields in file order: node IDs bytewise in [`ID_MAPS`], [`FIELD_ORDER`] elsewhere.
fn ordered<'a>(fields: &'a Map<String, Value>, name: Option<&str>) -> Vec<(&'a String, &'a Value)> {
    let mut entries: Vec<(&String, &Value)> = fields.iter().collect();
    if name.is_some_and(|name| ID_MAPS.contains(&name)) {
        entries.sort_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()));
    } else {
        entries.sort_by_key(|(key, _)| {
            FIELD_ORDER
                .iter()
                .position(|field| field == key)
                .unwrap_or_else(|| panic!("field {key:?} is missing from FIELD_ORDER"))
        });
    }
    entries
}

/// `text` as a JSON string.
fn quote(text: &str) -> String {
    serde_json::to_string(text).expect("strings always serialize")
}

fn parse(text: &str) -> Value {
    serde_json::from_str(text).expect("placement-v1.json is valid JSON")
}

/// Reads a file, or returns an empty string if it doesn't exist yet.
fn read_or_empty(path: &Path) -> String {
    match fs::read_to_string(path) {
        Ok(text) => text,
        Err(error) if error.kind() == io::ErrorKind::NotFound => String::new(),
        Err(error) => panic!("cannot read {}: {error}", path.display()),
    }
}

#[test]
#[ignore = "checks tests/vectors against the catalog; FLEETRING_WRITE_VECTORS=1 appends new cases"]
fn generate_vectors() {
    let write = env::var("FLEETRING_WRITE_VECTORS").is_ok_and(|value| value == "1");
    if write {
        let root = Path::new(env!("CARGO_MANIFEST_DIR"));
        let (json_path, lock_path) = (root.join(VECTORS_PATH), root.join(LOCK_PATH));
        let mut json = read_or_empty(&json_path);
        if json.trim().is_empty() {
            json = "{}".to_owned();
        }
        let merged =
            merge(&json, &read_or_empty(&lock_path)).unwrap_or_else(|problem| panic!("{problem}"));
        for (path, text) in [(&json_path, &merged.json), (&lock_path, &merged.lock)] {
            fs::write(path, text)
                .unwrap_or_else(|error| panic!("cannot write {}: {error}", path.display()));
        }
        println!(
            "{VECTORS_PATH}: kept {} cases byte for byte, appended {}",
            merged.kept, merged.appended
        );
    } else {
        let merged = merge(VECTORS, LOCK).unwrap_or_else(|problem| panic!("{problem}"));
        assert!(
            merged.appended == 0
                && merged.json == VECTORS.replace("\r\n", "\n")
                && merged.lock == LOCK.replace("\r\n", "\n"),
            "{VECTORS_PATH} or {LOCK_PATH} is out of date with the catalog ({} new cases); run \
             {GENERATE_COMMAND}",
            merged.appended
        );
    }
}

#[test]
fn frozen_vectors_never_change() {
    let frozen =
        verify_checkpoints(&parse(VECTORS), LOCK).unwrap_or_else(|problem| panic!("{problem}"));
    assert!(frozen > 0, "{LOCK_PATH} must freeze at least one case");
}

#[test]
fn every_log2_table_entry_matches_the_math() {
    let vectors = parse(VECTORS);
    let table = vectors["log2_table"]
        .as_array()
        .expect("log2_table is an array");
    assert_eq!(table.len(), 1025, "T has entries 0 to 1024");
    for (j, entry) in (0u64..).zip(table) {
        assert_eq!(entry.as_u64(), Some(table_entry(j)), "T[{j}]");
    }
    assert_eq!(table[0], 0, "T[0] = 0");
    assert_eq!(table[1024], 1u64 << 32, "T[1024] = 2³²");
}

#[test]
fn every_neg_log2_vector_matches_the_math() {
    let vectors = parse(VECTORS);
    let entries = vectors["neg_log2_q32"]
        .as_array()
        .expect("neg_log2_q32 is an array");
    assert!(entries.len() >= 60, "only {} L(x) vectors", entries.len());
    for entry in entries {
        let x = entry["x"]
            .as_str()
            .and_then(parse_hex64)
            .unwrap_or_else(|| panic!("bad x in {entry}"));
        assert_eq!(Some(neg_log2_q32(x)), entry["l"].as_u64(), "L({x:#018x})");
    }
}

#[test]
fn the_canonical_form_is_compact_json_with_sorted_keys() {
    let value = json!({"b": [1, "x", {"d": null, "c": true}], "a": "é\""});
    let mut text = String::new();
    canonical(&value, &mut text);
    assert_eq!(text, r#"{"a":"é\"","b":[1,"x",{"c":true,"d":null}]}"#);
}

#[test]
fn checkpoints_parse_and_reject_malformed_lines() {
    let lock = "# comment\n\n12 00000000000000ab\r\n  40 0123456789abcdef  \n";
    assert_eq!(
        checkpoints(lock),
        Ok(vec![
            Checkpoint {
                count: 12,
                digest: 0xab
            },
            Checkpoint {
                count: 40,
                digest: 0x0123_4567_89ab_cdef
            },
        ])
    );
    for bad in [
        "12",
        "12 ab",
        "0 00000000000000ab",
        "12 00000000000000AB",
        "1 2 3",
    ] {
        assert!(checkpoints(bad).is_err(), "{bad:?} parsed");
    }
}

#[test]
fn a_changed_frozen_case_fails_the_freeze_guard_but_an_appended_case_does_not() {
    let vectors = parse(VECTORS);
    let count = vectors["cases"].as_array().map_or(0, Vec::len);
    let lock = format!("{count} {:016x}\n", frozen_digest(&vectors, count));
    assert_eq!(verify_checkpoints(&vectors, &lock), Ok(count));

    let mut appended = vectors.clone();
    let extra = appended["cases"][0].clone();
    appended["cases"]
        .as_array_mut()
        .expect("cases is an array")
        .push(extra);
    assert_eq!(verify_checkpoints(&appended, &lock), Ok(count));

    let mut edited = vectors.clone();
    edited["cases"][count - 1]["keys"][0]["replicas"] = json!([]);
    assert!(verify_checkpoints(&edited, &lock).is_err());

    let mut truncated = vectors;
    truncated["cases"]
        .as_array_mut()
        .expect("cases is an array")
        .pop();
    assert!(verify_checkpoints(&truncated, &lock).is_err());
}
