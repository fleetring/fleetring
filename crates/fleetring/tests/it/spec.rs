//! Ring specs, fingerprints, and their serde form through the public API (FR-013).

use std::error::Error;
use std::str::FromStr;

use fleetring::{
    BuildRingError, Epoch, Fingerprint, FromSpecError, Node, NodeSpec, ParseLabelError,
    ReplicationFactor, Ring, RingSpec, StableHasher, Weight, Xxh3,
};
use proptest::prelude::*;
use proptest::sample::Index;

use crate::common::corpus::Corpus;
use crate::common::fixtures;

type TestResult = Result<(), Box<dyn Error>>;

/// Keys per corpus that round-trip tests compare placements on.
const KEYS: usize = 2_000;

/// Every strategy a spec can describe. FR-010 adds `Strategy::Vnodes` here.
const STRATEGIES: [fleetring::Strategy; 2] = [
    fleetring::Strategy::Rendezvous,
    // One token per weight unit: generated memberships weigh up to 1000 per node, and the spec
    // tests rebuild rings many times per case. The vnode algorithm has its own suite.
    fleetring::Strategy::Vnodes(fleetring::VnodeParams::new(
        std::num::NonZeroU32::new(1).expect("1 is nonzero"),
    )),
];

/// The fingerprint of [`fixture_ring`], which `tests/fixtures/ring-spec-v1.json` also stores.
const FIXTURE_FINGERPRINT: &str = "71699bba8f0655ae";

fn label<T: FromStr<Err = ParseLabelError>>(text: &str) -> T {
    text.parse().expect("test labels are valid")
}

fn weight(value: u32) -> Weight {
    Weight::new(value).expect("test weights are nonzero")
}

fn replication(value: u8) -> ReplicationFactor {
    ReplicationFactor::new(value).expect("test replication factors are nonzero")
}

/// The ring in `tests/fixtures/ring-spec-v1.json`: four nodes in two named zones and the default
/// zone, two of them on one host.
fn fixture_ring() -> Ring {
    let node = |id: &str, value: u32| Node::new(label(id)).with_weight(weight(value));
    Ring::builder()
        .seed(42)
        .replication_factor(replication(2))
        .epoch(Epoch::new(7))
        .node(
            node("laptop-1", 1)
                .with_zone(label("home"))
                .with_host(label("desk")),
        )
        .node(
            node("laptop-2", 1)
                .with_zone(label("home"))
                .with_host(label("desk")),
        )
        .node(
            node("nas", 4)
                .with_zone(label("garage"))
                .with_host(label("nas.local")),
        )
        .node(node("vps", 2))
        .build()
        .expect("the fixture ring is valid")
}

/// Rings over every named fixture membership, for every strategy, with non-default settings.
fn named_rings() -> Vec<(String, Ring)> {
    let memberships = [
        ("single", fixtures::single()),
        ("homelab", fixtures::homelab()),
        ("three_zones(3)", fixtures::three_zones(3)),
        ("skewed_zones", fixtures::skewed_zones()),
        ("fleet(12, 3)", fixtures::fleet(12, 3)),
    ];
    let mut rings = Vec::new();
    for strategy in STRATEGIES {
        for (name, nodes) in &memberships {
            let ring = Ring::builder()
                .strategy(strategy)
                .seed(0x9e37_79b9_7f4a_7c15)
                .replication_factor(replication(4))
                .epoch(Epoch::new(12))
                .nodes(nodes.iter().cloned())
                .build()
                .expect("fixture memberships are valid");
            rings.push((format!("{name}, {strategy:?}"), ring));
        }
    }
    rings
}

/// Specs of rings over random memberships, with random settings.
fn any_spec() -> impl Strategy<Value = RingSpec> {
    let settings = (
        proptest::sample::select(STRATEGIES.to_vec()),
        any::<u64>(),
        1..=8u8,
        any::<u64>(),
    );
    (fixtures::memberships_up_to(12), settings).prop_map(
        |(nodes, (strategy, seed, factor, epoch))| {
            Ring::builder()
                .strategy(strategy)
                .seed(seed)
                .replication_factor(replication(factor))
                .epoch(Epoch::new(epoch))
                .nodes(nodes)
                .build()
                .expect("generated memberships are valid")
                .to_spec()
        },
    )
}

/// Builds the ring `spec` describes with XXH3, skipping the fingerprint check that
/// [`Ring::from_spec`] makes, so tests can see the fingerprint a changed spec would need.
fn build_unchecked(spec: &RingSpec) -> Result<Ring, BuildRingError> {
    Ring::builder()
        .seed(spec.seed)
        .strategy(fleetring::Strategy::from(spec.strategy))
        .replication_factor(spec.replication_factor)
        .epoch(spec.epoch)
        .nodes(spec.nodes.iter().cloned().map(Node::from))
        .build()
}

/// Returns the fingerprint of the ring `spec` describes, ignoring the one it stores.
fn fingerprint_of(spec: &RingSpec) -> Fingerprint {
    build_unchecked(spec)
        .expect("the changed spec still describes a valid ring")
        .fingerprint()
}

/// Moves node `index`, and every node on the same host, into a zone that no node is in yet.
fn move_to_new_zone(nodes: &mut [NodeSpec], index: usize) {
    let id = nodes[index].id.clone();
    let host = nodes[index].host.clone();
    for node in nodes {
        if node.id == id || (host.is_some() && node.host == host) {
            node.zone = Some(label("fresh-zone"));
        }
    }
}

/// Asserts that two rings give every key of every corpus the same replica set.
fn assert_same_placement<H1, H2>(left: &Ring<H1>, right: &Ring<H2>, keys: usize, context: &str)
where
    H1: StableHasher,
    H2: StableHasher,
{
    for corpus in Corpus::ALL {
        for key in corpus.keys(keys) {
            assert_eq!(
                left.replicas(&key),
                right.replicas(&key),
                "{context}: {corpus} key {key:?}"
            );
        }
    }
}

#[test]
fn rings_rebuilt_from_their_specs_place_every_key_identically() -> TestResult {
    for (name, ring) in named_rings() {
        let spec = ring.to_spec();
        assert_eq!(spec.fingerprint, ring.fingerprint(), "{name}");
        let rebuilt = Ring::from_spec(&spec, Xxh3)?;
        assert_eq!(rebuilt.fingerprint(), ring.fingerprint(), "{name}");
        assert_eq!(rebuilt.nodes(), ring.nodes(), "{name}");
        assert_eq!(rebuilt.epoch(), ring.epoch(), "{name}");
        assert_eq!(rebuilt.to_spec(), spec, "{name}");
        assert_same_placement(&ring, &rebuilt, KEYS, &name);
    }
    Ok(())
}

#[test]
fn a_thousand_node_ring_round_trips() -> TestResult {
    let ring = Ring::builder().nodes(fixtures::fleet_1000()).build()?;
    let rebuilt = Ring::from_spec(&ring.to_spec(), Xxh3)?;
    assert_eq!(rebuilt.fingerprint(), ring.fingerprint());
    assert_same_placement(&ring, &rebuilt, 50, "fleet_1000");
    Ok(())
}

#[test]
fn specs_list_nodes_in_canonical_order_with_their_labels() {
    let spec = fixture_ring().to_spec();
    let ids: Vec<&str> = spec.nodes.iter().map(|node| node.id.as_str()).collect();
    assert_eq!(ids, ["laptop-1", "laptop-2", "nas", "vps"]);
    assert_eq!(
        spec.nodes[2],
        NodeSpec::from(
            &Node::new(label("nas"))
                .with_weight(weight(4))
                .with_zone(label("garage"))
                .with_host(label("nas.local"))
        )
    );
    assert_eq!(
        (spec.nodes[3].zone.as_ref(), spec.nodes[3].host.as_ref()),
        (None, None)
    );
    assert_eq!(spec.format, RingSpec::FORMAT);
    assert_eq!(spec.hasher, "xxh3-64");
    assert_eq!((spec.seed, spec.epoch), (42, Epoch::new(7)));
}

#[test]
fn the_fixture_ring_keeps_its_fingerprint() {
    // Pinned here as well as in the JSON fixture, so builds without serde check it too.
    assert_eq!(
        fixture_ring().fingerprint().to_string(),
        FIXTURE_FINGERPRINT
    );
}

proptest! {
    #[test]
    fn a_ring_rebuilt_from_its_spec_writes_the_same_spec(spec in any_spec()) {
        let ring = Ring::from_spec(&spec, Xxh3).expect("a ring's own spec is valid");
        prop_assert_eq!(ring.to_spec(), spec);
    }

    #[test]
    fn the_order_of_nodes_in_a_spec_does_not_matter(spec in any_spec()) {
        let mut reversed = spec.clone();
        reversed.nodes.reverse();
        let ring = Ring::from_spec(&reversed, Xxh3).expect("node order is irrelevant");
        prop_assert_eq!(ring.to_spec(), spec);
    }

    #[test]
    fn the_fingerprint_covers_the_seed(spec in any_spec(), flip in 1..=u64::MAX) {
        let mut changed = spec.clone();
        changed.seed ^= flip;
        prop_assert_ne!(fingerprint_of(&changed), spec.fingerprint);
    }

    #[test]
    fn the_fingerprint_covers_each_weight(spec in any_spec(), pick in any::<Index>()) {
        let mut changed = spec.clone();
        let node = pick.get_mut(&mut changed.nodes);
        node.weight = weight(node.weight.get() % u32::MAX + 1);
        prop_assert_ne!(fingerprint_of(&changed), spec.fingerprint);
    }

    #[test]
    fn the_fingerprint_covers_each_zone(spec in any_spec(), pick in any::<Index>()) {
        let mut changed = spec.clone();
        let index = pick.index(changed.nodes.len());
        move_to_new_zone(&mut changed.nodes, index);
        prop_assert_ne!(fingerprint_of(&changed), spec.fingerprint);
    }

    #[test]
    fn the_fingerprint_covers_each_host(spec in any_spec(), pick in any::<Index>()) {
        let mut changed = spec.clone();
        pick.get_mut(&mut changed.nodes).host = Some(label("fresh-host"));
        prop_assert_ne!(fingerprint_of(&changed), spec.fingerprint);
    }

    #[test]
    fn the_fingerprint_covers_the_replication_factor(spec in any_spec()) {
        let mut changed = spec.clone();
        changed.replication_factor = replication(spec.replication_factor.get() % u8::MAX + 1);
        prop_assert_ne!(fingerprint_of(&changed), spec.fingerprint);
    }

    #[test]
    fn the_fingerprint_covers_adding_a_node(spec in any_spec()) {
        let mut changed = spec.clone();
        changed.nodes.push(NodeSpec::from(&Node::new(label("fresh-node"))));
        prop_assert_ne!(fingerprint_of(&changed), spec.fingerprint);
    }

    #[test]
    fn the_fingerprint_covers_removing_a_node(spec in any_spec(), pick in any::<Index>()) {
        prop_assume!(spec.nodes.len() > 1, "a ring keeps at least one node");
        let mut changed = spec.clone();
        changed.nodes.remove(pick.index(changed.nodes.len()));
        prop_assert_ne!(fingerprint_of(&changed), spec.fingerprint);
    }

    #[test]
    fn the_fingerprint_ignores_the_epoch(spec in any_spec(), epoch in any::<u64>()) {
        let mut changed = spec.clone();
        changed.epoch = Epoch::new(epoch);
        prop_assert_eq!(fingerprint_of(&changed), spec.fingerprint);
        let ring = Ring::from_spec(&changed, Xxh3).expect("the stored fingerprint still holds");
        prop_assert_eq!(ring.epoch(), Epoch::new(epoch));
    }
}

#[test]
fn a_tampered_fingerprint_is_rejected() -> TestResult {
    let ring = Ring::builder().nodes(fixtures::homelab()).build()?;
    let honest = ring.fingerprint();
    let mut spec = ring.to_spec();
    spec.fingerprint = Fingerprint::new(honest.get() ^ 1);
    let error = Ring::from_spec(&spec, Xxh3).expect_err("the fingerprint was tampered with");
    assert_eq!(
        error,
        FromSpecError::FingerprintMismatch {
            stored: spec.fingerprint,
            computed: honest,
        }
    );
    assert_eq!(
        error.to_string(),
        format!(
            "ring spec fingerprint is {}, but the ring it describes has fingerprint {honest}",
            spec.fingerprint
        )
    );
    Ok(())
}

#[test]
fn a_spec_loaded_with_a_different_seed_is_rejected() -> TestResult {
    let ring = Ring::builder().seed(7).nodes(fixtures::homelab()).build()?;
    let mut spec = ring.to_spec();
    spec.seed = 8;
    let reseeded = ring.to_builder().seed(8).build()?;
    let error = Ring::from_spec(&spec, Xxh3).expect_err("the seed changed");
    assert_eq!(
        error,
        FromSpecError::FingerprintMismatch {
            stored: ring.fingerprint(),
            computed: reseeded.fingerprint(),
        }
    );
    Ok(())
}

#[test]
fn an_unknown_format_is_rejected_before_anything_else() -> TestResult {
    let mut spec = Ring::builder()
        .nodes(fixtures::homelab())
        .build()?
        .to_spec();
    spec.format = 2;
    // The hasher is wrong too, but the format is checked first.
    spec.hasher = "murmur3-64".to_owned();
    let error = Ring::from_spec(&spec, Xxh3).expect_err("format 2 is unknown");
    assert_eq!(error, FromSpecError::UnsupportedFormat { found: 2 });
    assert_eq!(
        error.to_string(),
        "ring spec format 2 is not supported; the supported format is 1"
    );
    spec.format = 0;
    assert_eq!(
        Ring::from_spec(&spec, Xxh3).err(),
        Some(FromSpecError::UnsupportedFormat { found: 0 })
    );
    Ok(())
}

#[test]
fn a_spec_for_another_hasher_is_rejected_before_its_nodes_are_checked() -> TestResult {
    let mut spec = Ring::builder()
        .nodes(fixtures::homelab())
        .build()?
        .to_spec();
    spec.hasher = "murmur3-64".to_owned();
    spec.nodes.clear();
    let error = Ring::from_spec(&spec, Xxh3).expect_err("the spec names another hasher");
    assert_eq!(
        error,
        FromSpecError::HasherMismatch {
            spec: "murmur3-64".to_owned(),
            hasher: "xxh3-64".to_owned(),
        }
    );
    assert_eq!(
        error.to_string(),
        "ring spec uses hasher 'murmur3-64', but the given hasher is 'xxh3-64'"
    );
    Ok(())
}

#[test]
fn an_invalid_ring_is_rejected_with_the_build_error_as_its_source() -> TestResult {
    let mut spec = Ring::builder()
        .nodes(fixtures::homelab())
        .build()?
        .to_spec();
    let duplicate = spec.nodes[1].clone();
    spec.nodes.push(duplicate);
    let error = Ring::from_spec(&spec, Xxh3).expect_err("a node appears twice");
    let cause = BuildRingError::DuplicateNode {
        id: spec.nodes[1].id.clone(),
    };
    assert_eq!(error, FromSpecError::InvalidRing(cause.clone()));
    assert_eq!(error.to_string(), "ring spec describes an invalid ring");
    let source = error
        .source()
        .and_then(|source| source.downcast_ref::<BuildRingError>());
    assert_eq!(source, Some(&cause));

    spec.nodes.clear();
    assert_eq!(
        Ring::from_spec(&spec, Xxh3).err(),
        Some(FromSpecError::InvalidRing(BuildRingError::NoNodes))
    );
    Ok(())
}

proptest! {
    #[test]
    fn fingerprints_print_16_lowercase_hex_digits_and_parse_back(value in any::<u64>()) {
        let fingerprint = Fingerprint::new(value);
        let text = fingerprint.to_string();
        prop_assert_eq!(text.len(), 16);
        prop_assert!(text.bytes().all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f')), "{text}");
        prop_assert_eq!(u64::from_str_radix(&text, 16), Ok(value));
        prop_assert_eq!(text.parse::<Fingerprint>(), Ok(fingerprint));
    }
}

#[test]
fn fingerprints_keep_leading_zeros_and_convert_to_integers() {
    assert_eq!(Fingerprint::new(0).to_string(), "0000000000000000");
    assert_eq!(Fingerprint::new(0xab).to_string(), "00000000000000ab");
    assert_eq!(Fingerprint::new(u64::MAX).to_string(), "ffffffffffffffff");
    assert_eq!(Fingerprint::from(7).get(), 7);
    assert_eq!(u64::from(Fingerprint::new(7)), 7);
}

#[test]
fn malformed_fingerprints_are_rejected() {
    let cases = [
        ("", "fingerprint is 0 bytes long; it must be 16 hex digits"),
        (
            "0123456789abcde",
            "fingerprint is 15 bytes long; it must be 16 hex digits",
        ),
        (
            "0123456789abcdef0",
            "fingerprint is 17 bytes long; it must be 16 hex digits",
        ),
        (
            "0123456789ABCDEF",
            "fingerprint has invalid byte 'A' at index 10; allowed are 0-9 and a-f",
        ),
        (
            "0x0123456789abcd",
            "fingerprint has invalid byte 'x' at index 1; allowed are 0-9 and a-f",
        ),
        (
            "+123456789abcdef",
            "fingerprint has invalid byte '+' at index 0; allowed are 0-9 and a-f",
        ),
        (
            "0123456789abcde ",
            "fingerprint has invalid byte ' ' at index 15; allowed are 0-9 and a-f",
        ),
        (
            "0123456789abcd\u{e9}",
            r"fingerprint has invalid byte '\xc3' at index 14; allowed are 0-9 and a-f",
        ),
    ];
    for (text, message) in cases {
        let error = text
            .parse::<Fingerprint>()
            .expect_err("the text is not a fingerprint");
        assert_eq!(error.to_string(), message, "{text:?}");
    }
}

/// Tests that swap the hasher, using the `test-util` hasher double.
#[cfg(feature = "test-util")]
mod hashers {
    use fleetring::test_util::TableHasher;

    use super::*;

    proptest! {
        #[test]
        fn the_fingerprint_covers_a_same_named_hasher_that_moves_a_probe(
            nodes in fixtures::memberships_up_to(12),
            probe in 0..64u32,
            hash in any::<u64>(),
        ) {
            let key = format!("fleetring-probe-{probe:02}");
            let build = |hasher: TableHasher| {
                Ring::builder()
                    .hasher(hasher)
                    .nodes(nodes.clone())
                    .build()
                    .expect("generated memberships are valid")
            };
            let honest = build(TableHasher::new());
            let skewed = build(TableHasher::new().with(&key, hash));
            prop_assume!(honest.replicas(&key) != skewed.replicas(&key), "the entry moves the probe");
            prop_assert_eq!(honest.hasher().algorithm(), skewed.hasher().algorithm());
            prop_assert_ne!(honest.fingerprint(), skewed.fingerprint());
        }
    }

    #[test]
    fn the_fingerprint_covers_the_hasher_name() -> TestResult {
        let xxh3 = Ring::builder().nodes(fixtures::homelab()).build()?;
        let table = Ring::builder()
            .hasher(TableHasher::new())
            .nodes(fixtures::homelab())
            .build()?;
        // An empty table hashes exactly like XXH3, so only the algorithm name differs.
        assert_same_placement(&xxh3, &table, KEYS / 10, "an empty table and XXH3");
        assert_ne!(xxh3.fingerprint(), table.fingerprint());
        Ok(())
    }

    #[test]
    fn a_spec_loads_only_with_a_hasher_of_the_name_it_records() -> TestResult {
        let spec = Ring::builder()
            .nodes(fixtures::homelab())
            .build()?
            .to_spec();
        let error = Ring::from_spec(&spec, TableHasher::new()).expect_err("the spec names xxh3-64");
        assert_eq!(
            error,
            FromSpecError::HasherMismatch {
                spec: "xxh3-64".to_owned(),
                hasher: "table-test".to_owned(),
            }
        );
        Ok(())
    }

    #[test]
    fn a_same_named_hasher_with_different_output_fails_the_fingerprint_check() -> TestResult {
        let nodes = fixtures::three_zones(2);
        let honest = Ring::builder()
            .hasher(TableHasher::new())
            .nodes(nodes.clone())
            .build()?;
        let spec = honest.to_spec();
        // A port whose hasher disagrees on the hash of one probe key.
        let key = "fleetring-probe-17";
        let buggy = (0..64)
            .map(|hash| TableHasher::new().with(key, hash))
            .find(|hasher| {
                let ring = Ring::builder()
                    .hasher(hasher.clone())
                    .nodes(nodes.clone())
                    .build()
                    .expect("valid membership");
                ring.replicas(key) != honest.replicas(key)
            })
            .ok_or("some hash moves the probe key")?;
        let error = Ring::from_spec(&spec, buggy).expect_err("the buggy hasher moves a probe");
        assert!(
            matches!(
                error,
                FromSpecError::FingerprintMismatch { stored, .. } if stored == spec.fingerprint
            ),
            "{error}"
        );
        Ok(())
    }
}

/// Tests of the serde form, which `tests/fixtures/ring-spec-v1.json` locks.
#[cfg(feature = "serde")]
mod json {
    use fleetring::{CellId, NodeId};

    use super::*;

    /// The v1 fixture. `include_str!` embeds it, so this runs on targets without a filesystem,
    /// such as wasm32-wasip1.
    const FIXTURE: &str = include_str!("../fixtures/ring-spec-v1.json");

    /// Returns the fixture as written: LF line endings and one trailing newline. Converting CRLF
    /// back keeps the comparison byte for byte on checkouts that rewrite line endings.
    fn fixture() -> String {
        FIXTURE.replace("\r\n", "\n")
    }

    /// Deserializes the fixture with the first occurrence of `from`, which it must contain,
    /// replaced by `to`.
    fn parse_edited(from: &str, to: &str) -> Result<RingSpec, serde_json::Error> {
        let fixture = fixture();
        assert!(fixture.contains(from), "the fixture contains {from:?}");
        serde_json::from_str(&fixture.replacen(from, to, 1))
    }

    #[test]
    fn the_v1_fixture_deserializes_to_the_ring_it_describes() -> TestResult {
        let ring = fixture_ring();
        let spec: RingSpec = serde_json::from_str(&fixture())?;
        assert_eq!(spec, ring.to_spec());
        let rebuilt = Ring::from_spec(&spec, Xxh3)?;
        assert_same_placement(&ring, &rebuilt, KEYS, "the v1 fixture");
        Ok(())
    }

    #[test]
    fn the_fixture_ring_serializes_to_the_v1_fixture_byte_for_byte() -> TestResult {
        // `to_string_pretty` writes no final newline; the file ends with exactly one.
        let written = serde_json::to_string_pretty(&fixture_ring().to_spec())? + "\n";
        assert!(
            written == fixture(),
            "ring-spec-v1.json no longer matches; the fixture ring now serializes as:\n{written}"
        );
        Ok(())
    }

    #[test]
    fn unknown_fields_are_ignored() -> TestResult {
        let mut value: serde_json::Value = serde_json::from_str(&fixture())?;
        value["comment"] = "written by a newer daemon".into();
        value["strategy"]["comment"] = "ignored inside the strategy too".into();
        value["nodes"][0]["rack"] = "r1".into();
        let spec: RingSpec = serde_json::from_value(value)?;
        assert_eq!(spec, fixture_ring().to_spec());
        Ok(())
    }

    #[test]
    fn building_blocks_serialize_as_strings_and_integers() -> TestResult {
        assert_eq!(
            serde_json::to_string(&Fingerprint::new(0xab))?,
            r#""00000000000000ab""#
        );
        assert_eq!(serde_json::to_string(&weight(4))?, "4");
        assert_eq!(serde_json::to_string(&replication(3))?, "3");
        assert_eq!(
            serde_json::to_string(&Epoch::new(u64::MAX))?,
            "18446744073709551615"
        );
        assert_eq!(serde_json::to_string(&label::<NodeId>("nas"))?, r#""nas""#);
        assert_eq!(serde_json::to_string(&label::<CellId>("eu"))?, r#""eu""#);
        // Escaped strings deserialize too: labels and fingerprints never borrow from the input.
        let id: NodeId = serde_json::from_str(r#""laptop-1""#)?;
        assert_eq!(id.as_str(), "laptop-1");
        let cell: CellId = serde_json::from_str(r#""eu-west""#)?;
        assert_eq!(cell.as_str(), "eu-west");
        let error = serde_json::from_str::<CellId>(r#""EU""#)
            .err()
            .ok_or("an uppercase cell id deserialized")?;
        assert!(
            error
                .to_string()
                .contains("cell id has invalid byte 'E' at index 0"),
            "{error}"
        );
        let fingerprint: Fingerprint = serde_json::from_str(r#""00000000000000ab""#)?;
        assert_eq!(fingerprint, Fingerprint::new(0xab));
        Ok(())
    }

    #[test]
    fn invalid_labels_fail_with_the_label_error() {
        let long_host = format!(r#""host": "{}""#, "h".repeat(65));
        let cases = [
            (
                r#""id": "laptop-1""#,
                r#""id": "Laptop-1""#,
                "node id has invalid byte 'L' at index 0",
            ),
            (r#""zone": "home""#, r#""zone": """#, "zone is empty"),
            (
                r#""host": "desk""#,
                r#""host": "desk top""#,
                "host has invalid byte ' ' at index 4",
            ),
            (
                r#""host": "desk""#,
                &long_host,
                "host is 65 bytes long; the maximum is 64",
            ),
            (
                r#""id": "laptop-1""#,
                r#""id": 1"#,
                "invalid type: integer `1`, expected a node id",
            ),
        ];
        for (from, to, message) in cases {
            let error = parse_edited(from, to).expect_err("the label is invalid");
            assert!(error.to_string().contains(message), "{to}: {error}");
        }
    }

    #[test]
    fn zero_weights_and_replication_factors_fail_with_a_clear_error() {
        let error = parse_edited(r#""weight": 4"#, r#""weight": 0"#).expect_err("zero weight");
        assert!(
            error
                .to_string()
                .contains("invalid value: integer `0`, expected a nonzero weight"),
            "{error}"
        );
        let error = parse_edited(r#""replication_factor": 2"#, r#""replication_factor": 0"#)
            .expect_err("zero replication factor");
        assert!(
            error
                .to_string()
                .contains("invalid value: integer `0`, expected a nonzero replication factor"),
            "{error}"
        );
    }

    #[test]
    fn malformed_specs_fail_with_an_error() {
        let fingerprint = format!(r#""fingerprint": "{FIXTURE_FINGERPRINT}""#);
        let cases = [
            (r#""weight": 4"#, r#""weight": 4294967296"#, "expected u32"),
            (r#""weight": 4"#, r#""weight": -4"#, "expected u32"),
            (
                r#""replication_factor": 2"#,
                r#""replication_factor": 256"#,
                "expected u8",
            ),
            (r#""epoch": 7"#, r#""epoch": -1"#, "expected u64"),
            (
                r#""kind": "rendezvous""#,
                r#""kind": "maglev""#,
                "unknown variant `maglev`",
            ),
            (
                &fingerprint,
                r#""fingerprint": "abc""#,
                "fingerprint is 3 bytes long; it must be 16 hex digits",
            ),
            (r#""hasher": "xxh3-64","#, "", "missing field `hasher`"),
        ];
        for (from, to, message) in cases {
            let error = parse_edited(from, to).expect_err("the spec is malformed");
            assert!(error.to_string().contains(message), "{to}: {error}");
        }
    }

    #[test]
    fn an_unknown_format_deserializes_so_from_spec_can_reject_it() -> TestResult {
        let spec = parse_edited(r#""format": 1"#, r#""format": 2"#)?;
        assert_eq!(
            Ring::from_spec(&spec, Xxh3).err(),
            Some(FromSpecError::UnsupportedFormat { found: 2 })
        );
        Ok(())
    }
}
