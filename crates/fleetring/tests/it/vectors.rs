//! The golden placement vectors through the public API (FR-014, FR-020, ADR-0008).
//!
//! `tests/vectors/placement-v1.json` pins placement spec v1, and describes its own layout in
//! its `layout` field. Most cases place keys on a ring; cells cases place them on a cells layer
//! (spec section 13) instead. `include_str!` embeds it, so these tests also run on targets without a
//! filesystem, such as wasm32-wasip1. The unit tests in `src/vectors.rs` check the logarithm
//! vectors and the freeze guard, and `scripts/placement_ref.py` checks the whole file against an
//! independent implementation of the spec.

use std::collections::BTreeSet;
use std::num::NonZeroU32;
use std::sync::OnceLock;

use fleetring::{
    BoundedLoads, CellId, CellPlacement, Cells, Directory, Fingerprint, Headroom, Node, Placement,
    ReplicaSet, ReplicationFactor, Ring, StableHasher, Strategy, VnodeParams, Weight, Xxh3,
};
use serde_json::Value;

/// The vector file.
const VECTORS: &str = include_str!("../vectors/placement-v1.json");

/// The parsed vector file, shared by every test.
fn vectors() -> &'static Value {
    static PARSED: OnceLock<Value> = OnceLock::new();
    PARSED.get_or_init(|| serde_json::from_str(VECTORS).expect("placement-v1.json is valid JSON"))
}

/// One placement case of the file: a ring case, or a cells case.
struct Case {
    name: &'static str,
    value: &'static Value,
}

impl Case {
    /// Builds the ring the case describes.
    fn ring(&self) -> Ring {
        ring(&self.value["ring"], self.name)
    }

    /// The case's filter or bounded-load scenarios under `field`, if it has any.
    fn scenarios(&self, field: &str) -> &'static [Value] {
        self.value
            .get(field)
            .map_or(&[], |scenarios| array(scenarios))
    }
}

/// Every ring case, in file order.
fn cases() -> Vec<Case> {
    all_cases()
        .into_iter()
        .filter(|case| case.value.get("ring").is_some())
        .collect()
}

/// Every cells case, in file order.
fn cells_cases() -> Vec<Case> {
    all_cases()
        .into_iter()
        .filter(|case| case.value.get("layer").is_some())
        .collect()
}

/// Every case of both kinds, in file order.
fn all_cases() -> Vec<Case> {
    array(&vectors()["cases"])
        .iter()
        .map(|value| Case {
            name: text(&value["name"]),
            value,
        })
        .collect()
}

/// Builds a ring the file describes, as a case's ring, a selector ring, or a cell's ring.
fn ring(ring: &Value, name: &str) -> Ring {
    assert_eq!(ring["hasher"], Xxh3.algorithm(), "{name}: hasher");
    let strategy = match text(&ring["strategy"]["kind"]) {
        "rendezvous" => Strategy::Rendezvous,
        "vnodes" => {
            let tokens = u32::try_from(number(&ring["strategy"]["tokens_per_weight"]))
                .ok()
                .and_then(NonZeroU32::new)
                .unwrap_or_else(|| panic!("{name}: bad tokens_per_weight"));
            Strategy::Vnodes(VnodeParams::new(tokens))
        }
        other => panic!("{name}: unknown strategy {other:?}"),
    };
    let replication = u8::try_from(number(&ring["replication_factor"]))
        .ok()
        .and_then(ReplicationFactor::new)
        .unwrap_or_else(|| panic!("{name}: bad replication factor"));
    let nodes: Vec<Node> = array(&ring["nodes"]).iter().map(node).collect();
    Ring::builder()
        .strategy(strategy)
        .seed(hex64(&ring["seed"]))
        .replication_factor(replication)
        .nodes(nodes)
        .build()
        .unwrap_or_else(|error| panic!("{name}: the ring does not build: {error}"))
}

/// What the cells checks need from a built layer, whatever its depth.
trait Layer {
    /// The key's path and replica IDs.
    fn place(&self, key: &[u8]) -> (Vec<String>, Vec<String>);
    /// The key's replica IDs among the nodes that aren't `down`, empty when none is up.
    fn place_where(&self, key: &[u8], down: &BTreeSet<&str>) -> Vec<String>;
    fn cell_of(&self, key: &[u8]) -> String;
    fn primary(&self, key: &[u8]) -> String;
    fn layer_fingerprint(&self) -> Fingerprint;
}

impl<P: Placement<Hasher = Xxh3>> Layer for Cells<P> {
    fn place(&self, key: &[u8]) -> (Vec<String>, Vec<String>) {
        let placed = self.replicas(key);
        (path_ids(&placed), owned_ids(placed.replicas()))
    }

    fn place_where(&self, key: &[u8], down: &BTreeSet<&str>) -> Vec<String> {
        let filtered = self.replicas_where(key, |node| !down.contains(node.id().as_str()));
        filtered
            .map(|placed| owned_ids(placed.replicas()))
            .unwrap_or_default()
    }

    fn cell_of(&self, key: &[u8]) -> String {
        Cells::cell_of(self, key).to_string()
    }

    fn primary(&self, key: &[u8]) -> String {
        Cells::primary(self, key).id().to_string()
    }

    fn layer_fingerprint(&self) -> Fingerprint {
        self.fingerprint()
    }
}

/// Builds the layer a cells case describes. A layer's cells are all rings, or all layers whose
/// cells are rings: `Cells<Ring>` or `Cells<Cells<Ring>>`.
fn cells_layer(case: &Case) -> Box<dyn Layer> {
    let value = &case.value["layer"];
    let entries = array(&value["cells"]);
    if entries.iter().all(|entry| entry.get("ring").is_some()) {
        Box::new(layer(value, case.name, &|entry| {
            ring(&entry["ring"], case.name)
        }))
    } else {
        Box::new(layer(value, case.name, &|entry| {
            layer(&entry["layer"], case.name, &|inner| {
                ring(&inner["ring"], case.name)
            })
        }))
    }
}

/// Builds a layer the file describes, building each cell entry with `cell`.
fn layer<P: Placement<Hasher = Xxh3>>(
    value: &Value,
    name: &str,
    cell: &dyn Fn(&Value) -> P,
) -> Cells<P> {
    let mut directory = Directory::new();
    if let Some(rules) = value.get("directory") {
        for (key, id) in rules["exact"].as_object().expect("exact is an object") {
            directory = directory.exact(hex_bytes(key), cell_id(id));
        }
        for (prefix, id) in rules["prefix"].as_object().expect("prefix is an object") {
            directory = directory.prefix(hex_bytes(prefix), cell_id(id));
        }
    }
    let mut builder = Cells::builder().directory(directory);
    if let Some(selector) = value.get("selector") {
        builder = builder.ring_selector(ring(selector, name));
    }
    for entry in array(&value["cells"]) {
        builder = builder.cell(cell_id(&entry["cell"]), cell(entry));
    }
    builder
        .build()
        .unwrap_or_else(|error| panic!("{name}: the layer does not build: {error}"))
}

fn cell_id(value: &Value) -> CellId {
    text(value).parse().expect("vector cell IDs are valid")
}

/// A placement's path, as IDs.
fn path_ids(placed: &CellPlacement<'_>) -> Vec<String> {
    placed.path().iter().map(|cell| cell.to_string()).collect()
}

/// A replica set's IDs, in order, as owned strings.
fn owned_ids(replicas: &ReplicaSet<'_>) -> Vec<String> {
    replicas.iter().map(|node| node.id().to_string()).collect()
}

/// The strings of a JSON array, such as a key entry's expected path.
fn strings(value: &Value) -> Vec<String> {
    array(value)
        .iter()
        .map(|item| text(item).to_owned())
        .collect()
}

fn node(row: &Value) -> Node {
    let id = text(&row["id"]);
    let weight = u32::try_from(number(&row["weight"]))
        .ok()
        .and_then(Weight::new)
        .unwrap_or_else(|| panic!("node {id}: bad weight"));
    let mut node = Node::new(id.parse().expect("vector node IDs are valid")).with_weight(weight);
    if let Some(zone) = row.get("zone") {
        node = node.with_zone(text(zone).parse().expect("vector zones are valid"));
    }
    if let Some(host) = row.get("host") {
        node = node.with_host(text(host).parse().expect("vector hosts are valid"));
    }
    node
}

fn text(value: &Value) -> &str {
    value
        .as_str()
        .unwrap_or_else(|| panic!("expected a string, found {value}"))
}

fn number(value: &Value) -> u64 {
    value
        .as_u64()
        .unwrap_or_else(|| panic!("expected an integer, found {value}"))
}

fn array(value: &Value) -> &[Value] {
    value
        .as_array()
        .unwrap_or_else(|| panic!("expected an array, found {value}"))
}

fn boolean(value: &Value) -> bool {
    value
        .as_bool()
        .unwrap_or_else(|| panic!("expected a boolean, found {value}"))
}

/// Parses a 64-bit value written as 16 lowercase hex digits.
fn hex64(value: &Value) -> u64 {
    let digits = text(value);
    assert!(
        digits.len() == 16
            && digits
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)),
        "expected 16 lowercase hex digits, found {digits:?}"
    );
    u64::from_str_radix(digits, 16).expect("16 hex digits fit in a u64")
}

/// Parses a decimal string, the form of loads and capacities.
fn decimal(value: &Value) -> u128 {
    text(value)
        .parse()
        .unwrap_or_else(|error| panic!("bad decimal {value}: {error}"))
}

/// Parses a key written as lowercase hex.
fn key_bytes(value: &Value) -> Vec<u8> {
    hex_bytes(text(value))
}

/// Parses bytes written as lowercase hex, such as a key or a directory rule's prefix.
fn hex_bytes(text: &str) -> Vec<u8> {
    let digits = text.as_bytes();
    assert!(digits.len().is_multiple_of(2), "odd-length hex {text:?}");
    digits
        .chunks(2)
        .map(|pair| {
            let pair = std::str::from_utf8(pair).expect("hex digits are ASCII");
            u8::from_str_radix(pair, 16).unwrap_or_else(|_| panic!("bad hex {text:?}"))
        })
        .collect()
}

/// The replica IDs a key entry expects.
fn expected_ids(entry: &Value) -> Vec<&str> {
    array(&entry["replicas"]).iter().map(text).collect()
}

/// A replica set's IDs, in order.
fn replica_ids<'a>(replicas: &'a ReplicaSet<'_>) -> Vec<&'a str> {
    replicas.iter().map(|node| node.id().as_str()).collect()
}

/// The reference input of the XXH3 vectors: byte `i` is `(31 * i + 7) mod 256`.
fn pattern(len: usize) -> Vec<u8> {
    std::iter::successors(Some(7u8), |byte| Some(byte.wrapping_add(31)))
        .take(len)
        .collect()
}

#[test]
fn every_xxh3_vector_matches() {
    let section = &vectors()["xxh3_64"];
    let entries = array(&section["cases"]);
    assert_eq!(entries.len(), 39, "the 39 vectors of tests/it/hasher.rs");
    for entry in entries {
        let len = usize::try_from(number(&entry["len"])).expect("small lengths");
        let seed = hex64(&entry["seed"]);
        assert_eq!(
            Xxh3.hash(&pattern(len), seed),
            hex64(&entry["digest"]),
            "XXH3-64 of {len} bytes under seed {seed:#018x}"
        );
    }
}

#[test]
fn every_ring_builds_with_its_fingerprint() {
    let all = all_cases();
    let names: BTreeSet<&str> = all.iter().map(|case| case.name).collect();
    assert_eq!(names.len(), all.len(), "case names are unique");
    assert_eq!(
        cases().len() + cells_cases().len(),
        all.len(),
        "every case has a ring or a layer"
    );
    let cases = cases();
    for case in &cases {
        let ring = case.ring();
        assert_eq!(
            ring.fingerprint().to_string(),
            text(&case.value["fingerprint"]),
            "{}: fingerprint",
            case.name
        );
    }
}

#[test]
fn every_key_maps_to_its_replicas() {
    for case in cases() {
        let ring = case.ring();
        for entry in array(&case.value["keys"]) {
            let key = key_bytes(&entry["key"]);
            let replicas = ring.replicas(&key);
            assert_eq!(
                replica_ids(&replicas),
                expected_ids(entry),
                "{}: key {}",
                case.name,
                entry["key"]
            );
            assert_eq!(
                ring.primary(&key).id(),
                replicas.primary().id(),
                "{}: key {}: the primary heads the replica set",
                case.name,
                entry["key"]
            );
        }
    }
}

#[test]
fn every_filtered_replica_set_matches() {
    for case in cases() {
        let ring = case.ring();
        for filter in case.scenarios("filters") {
            let down: BTreeSet<&str> = array(&filter["down"]).iter().map(text).collect();
            for entry in array(&filter["keys"]) {
                let key = key_bytes(&entry["key"]);
                let filtered = ring.replicas_where(&key, |node| !down.contains(node.id().as_str()));
                let got = filtered.as_ref().map(replica_ids).unwrap_or_default();
                assert_eq!(
                    got,
                    expected_ids(entry),
                    "{}: key {} with {down:?} down",
                    case.name,
                    entry["key"]
                );
            }
        }
    }
}

#[test]
fn every_bounded_placement_matches() {
    for case in cases() {
        let ring = case.ring();
        for scenario in case.scenarios("bounded") {
            let headroom = u16::try_from(number(&scenario["headroom"])).expect("headroom is a u16");
            let item_load = u64::try_from(decimal(&scenario["item_load"])).expect("a u64 item");
            let loads: Vec<(&str, u64)> = scenario["loads"]
                .as_object()
                .expect("loads is an object")
                .iter()
                .map(|(id, load)| {
                    (
                        id.as_str(),
                        u64::try_from(decimal(load)).expect("a u64 load"),
                    )
                })
                .collect();
            let context = format!("{}: headroom {headroom}%, item {item_load}", case.name);
            let view = BoundedLoads::new(Headroom::from_percent(headroom))
                .load_view(&ring, loads)
                .unwrap_or_else(|error| panic!("{context}: {error}"));

            let capacities = scenario["capacity"]
                .as_object()
                .expect("capacity is an object");
            assert_eq!(capacities.len(), ring.node_count(), "{context}: every node");
            for (id, capacity) in capacities {
                assert_eq!(
                    view.capacity(id, item_load),
                    Some(decimal(capacity)),
                    "{context}: capacity of {id}"
                );
            }

            for entry in array(&scenario["keys"]) {
                let key = key_bytes(&entry["key"]);
                let placed = view.replicas(&key, item_load);
                let at = format!("{context}: key {}", entry["key"]);
                assert_eq!(
                    replica_ids(placed.replicas()),
                    expected_ids(entry),
                    "{at}: replicas"
                );
                assert_eq!(
                    placed.is_saturated(),
                    boolean(&entry["saturated"]),
                    "{at}: saturated"
                );
                assert_eq!(placed.is_home(), boolean(&entry["home"]), "{at}: home");
            }
        }
    }
}

/// The file keeps covering what FR-014 requires. It is append-only, so this only fails if the
/// file is replaced.
#[test]
fn the_cases_cover_every_required_shape() {
    let cases = cases();
    assert!(cases.len() >= 40, "only {} cases", cases.len());
    let mut shapes = BTreeSet::new();
    for case in &cases {
        let ring = case.ring();
        let spec = &case.value["ring"];
        match ring.strategy() {
            Strategy::Rendezvous => {
                shapes.insert("rendezvous");
            }
            Strategy::Vnodes(params) => {
                shapes.insert("vnodes");
                if params.tokens_per_weight().get() <= 4 {
                    shapes.insert("small vnodes");
                }
            }
            other => panic!("{}: no coverage rule for {other:?}", case.name),
        }
        if ring.node_count() == 1 {
            shapes.insert("single node");
        }
        shapes.insert(if ring.seed() == 0 {
            "seed 0"
        } else {
            "nonzero seed"
        });
        let weights: Vec<u32> = ring
            .nodes()
            .iter()
            .map(|node| node.weight().get())
            .collect();
        let (lightest, heaviest) = (weights.iter().min(), weights.iter().max());
        if let (Some(&lightest), Some(&heaviest)) = (lightest, heaviest) {
            if lightest == heaviest && ring.node_count() > 1 {
                shapes.insert("equal weights");
            }
            if heaviest >= lightest.saturating_mul(1000) {
                shapes.insert("weights 1000 apart");
            }
        }
        let zones = ring.zone_count();
        let replication = usize::from(ring.replication_factor().get());
        if zones > 1 {
            shapes.insert(match zones.cmp(&replication) {
                std::cmp::Ordering::Less => "zones < R",
                std::cmp::Ordering::Equal => "zones = R",
                std::cmp::Ordering::Greater => "zones > R",
            });
        }
        let hosts: Vec<&str> = array(&spec["nodes"])
            .iter()
            .filter_map(|row| row.get("host").map(text))
            .collect();
        if hosts.iter().collect::<BTreeSet<_>>().len() < hosts.len() {
            shapes.insert("shared hosts");
        }
        for entry in array(&case.value["keys"]) {
            let key = key_bytes(&entry["key"]);
            if key.is_empty() {
                shapes.insert("empty key");
            }
            if std::str::from_utf8(&key).is_err() {
                shapes.insert("non-UTF-8 key");
            }
            if key.len() >= 1024 {
                shapes.insert("1 KiB key");
            }
        }
        for filter in case.scenarios("filters") {
            shapes.insert("filter");
            if array(&filter["down"]).len() == ring.node_count() {
                shapes.insert("every node down");
            }
        }
        for scenario in case.scenarios("bounded") {
            for entry in array(&scenario["keys"]) {
                shapes.insert(if boolean(&entry["saturated"]) {
                    "saturated"
                } else {
                    "unsaturated"
                });
                if !boolean(&entry["home"]) {
                    shapes.insert("away from home");
                }
            }
        }
    }
    let required = [
        "rendezvous",
        "vnodes",
        "small vnodes",
        "single node",
        "seed 0",
        "nonzero seed",
        "equal weights",
        "weights 1000 apart",
        "zones < R",
        "zones = R",
        "zones > R",
        "shared hosts",
        "empty key",
        "non-UTF-8 key",
        "1 KiB key",
        "filter",
        "every node down",
        "saturated",
        "unsaturated",
        "away from home",
    ];
    let missing: Vec<&str> = required
        .into_iter()
        .filter(|shape| !shapes.contains(shape))
        .collect();
    assert!(missing.is_empty(), "no case covers {missing:?}");
}

#[test]
fn every_cells_layer_builds_with_its_fingerprint() {
    for case in cells_cases() {
        let layer = cells_layer(&case);
        assert_eq!(
            layer.layer_fingerprint().to_string(),
            text(&case.value["fingerprint"]),
            "{}: fingerprint",
            case.name
        );
    }
}

#[test]
fn every_cells_key_maps_to_its_path_and_replicas() {
    for case in cells_cases() {
        let layer = cells_layer(&case);
        for entry in array(&case.value["keys"]) {
            let key = key_bytes(&entry["key"]);
            let at = format!("{}: key {}", case.name, entry["key"]);
            let (path, replicas) = layer.place(&key);
            assert_eq!(path, strings(&entry["path"]), "{at}: path");
            assert_eq!(replicas, strings(&entry["replicas"]), "{at}: replicas");
            assert_eq!(
                Some(&layer.cell_of(&key)),
                path.last(),
                "{at}: cell_of is the leaf"
            );
            assert_eq!(
                Some(&layer.primary(&key)),
                replicas.first(),
                "{at}: the primary heads the replica set"
            );
        }
    }
}

#[test]
fn every_cells_filter_keeps_the_path_and_matches_its_replicas() {
    for case in cells_cases() {
        let layer = cells_layer(&case);
        for filter in case.scenarios("filters") {
            let down: BTreeSet<&str> = array(&filter["down"]).iter().map(text).collect();
            for entry in array(&filter["keys"]) {
                let key = key_bytes(&entry["key"]);
                let at = format!("{}: key {} with {down:?} down", case.name, entry["key"]);
                let (path, _) = layer.place(&key);
                assert_eq!(path, strings(&entry["path"]), "{at}: path");
                assert_eq!(
                    layer.place_where(&key, &down),
                    strings(&entry["replicas"]),
                    "{at}: replicas"
                );
            }
        }
    }
}

/// The file keeps covering what FR-020 requires of cells. It is append-only, so this only fails
/// if the file is replaced.
#[test]
fn the_cells_cases_cover_every_required_shape() {
    let mut shapes = BTreeSet::new();
    for case in cells_cases() {
        let value = &case.value["layer"];
        let directory = value.get("directory");
        let has_catch_all = directory
            .and_then(|rules| rules["prefix"].as_object())
            .is_some_and(|prefixes| prefixes.contains_key(""));
        match (directory.is_some(), value.get("selector").is_some()) {
            (true, false) if has_catch_all => shapes.insert("directory with a catch-all"),
            (false, true) => shapes.insert("ring alone"),
            (true, true) => shapes.insert("directory and ring"),
            _ => false,
        };
        let entries = array(&value["cells"]);
        if entries.iter().any(|entry| entry.get("layer").is_some()) {
            shapes.insert("nested layers");
        }
        let mut kinds = BTreeSet::new();
        let mut rings: Vec<&Value> = value.get("selector").into_iter().collect();
        for entry in entries {
            match entry.get("layer") {
                Some(inner) => {
                    rings.extend(inner.get("selector"));
                    rings.extend(array(&inner["cells"]).iter().map(|cell| &cell["ring"]));
                }
                None => rings.push(&entry["ring"]),
            }
        }
        for ring in rings {
            kinds.insert(text(&ring["strategy"]["kind"]));
        }
        if kinds.len() > 1 {
            shapes.insert("mixed strategies");
        }
        for filter in case.scenarios("filters") {
            shapes.insert("filter");
            if array(&filter["keys"])
                .iter()
                .any(|entry| array(&entry["replicas"]).is_empty())
            {
                shapes.insert("every node of a cell down");
            }
        }
    }
    let required = [
        "directory with a catch-all",
        "ring alone",
        "directory and ring",
        "nested layers",
        "mixed strategies",
        "filter",
        "every node of a cell down",
    ];
    let missing: Vec<&str> = required
        .into_iter()
        .filter(|shape| !shapes.contains(shape))
        .collect();
    assert!(missing.is_empty(), "no cells case covers {missing:?}");
}
