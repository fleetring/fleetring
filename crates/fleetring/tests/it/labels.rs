use fleetring::{Epoch, Host, LabelKind, Node, NodeId, ReplicationFactor, Weight, Zone};
use proptest::prelude::*;

/// Characters that are never valid in a label, ASCII and not.
const INVALID: &[char] = &['A', 'Z', ' ', '/', '@', '\n', '\0', 'é', '名', '\u{7f}'];

proptest! {
    #[test]
    fn grammar_strings_parse_and_round_trip(label in "[a-z0-9._:-]{1,64}") {
        let id: NodeId = label.parse().expect("label matches the grammar");
        prop_assert_eq!(id.as_str(), label.as_str());
        prop_assert_eq!(id.to_string(), label.clone());
        let zone = Zone::try_from(label.clone()).expect("label matches the grammar");
        let host = Host::try_from(label.as_str()).expect("label matches the grammar");
        prop_assert_eq!(zone.as_str(), label.as_str());
        prop_assert_eq!(host.as_str(), label.as_str());
    }

    #[test]
    fn one_invalid_character_is_rejected(
        prefix in "[a-z0-9._:-]{0,20}",
        bad in proptest::sample::select(INVALID),
        suffix in "[a-z0-9._:-]{0,20}",
    ) {
        let label = format!("{prefix}{bad}{suffix}");
        let error = label.parse::<Zone>().expect_err("label has an invalid character");
        prop_assert_eq!(error.kind(), LabelKind::Zone);
        let message = error.to_string();
        prop_assert!(message.starts_with("zone has invalid byte"), "{message}");
        let index = prefix.len();
        prop_assert!(message.contains(&format!("at index {index};")), "{message}");
    }

    #[test]
    fn labels_longer_than_64_bytes_are_rejected(label in "[a-z]{65,200}") {
        let error = label.parse::<Host>().expect_err("label is too long");
        prop_assert_eq!(error.kind(), LabelKind::Host);
        prop_assert_eq!(
            error.to_string(),
            format!("host is {} bytes long; the maximum is 64", label.len())
        );
    }
}

#[test]
fn empty_labels_are_rejected() {
    let error = "".parse::<NodeId>().expect_err("empty label");
    assert_eq!(error.kind(), LabelKind::NodeId);
    assert_eq!(error.to_string(), "node id is empty");
}

#[test]
fn invalid_byte_messages_escape_the_byte() {
    let error = "Laptop"
        .parse::<NodeId>()
        .expect_err("uppercase is invalid");
    assert_eq!(
        error.to_string(),
        "node id has invalid byte 'L' at index 0; allowed are a-z, 0-9, '-', '_', '.', and ':'"
    );
    let error = "a\u{e9}"
        .parse::<NodeId>()
        .expect_err("non-ASCII is invalid");
    assert!(error.to_string().contains(r"'\xc3' at index 1"), "{error}");
}

#[test]
fn zero_is_not_a_weight_or_replication_factor() {
    assert_eq!(Weight::new(0), None);
    assert_eq!(ReplicationFactor::new(0), None);
    assert_eq!(Weight::default(), Weight::ONE);
    assert_eq!(ReplicationFactor::default().get(), 3);
    assert_eq!(Weight::new(u32::MAX).map(Weight::get), Some(u32::MAX));
}

#[test]
fn nodes_default_to_weight_one_in_the_default_zone_on_their_own_host()
-> Result<(), Box<dyn std::error::Error>> {
    let node = Node::new("node-a".parse()?);
    assert_eq!(node.weight(), Weight::ONE);
    assert_eq!(node.zone(), None);
    assert_eq!(node.host(), None);

    let node = node
        .with_weight(Weight::new(3).ok_or("zero weight")?)
        .with_zone("z1".parse()?)
        .with_host("h1".parse()?);
    assert_eq!(node.weight().get(), 3);
    assert_eq!(node.zone().map(Zone::as_str), Some("z1"));
    assert_eq!(node.host().map(Host::as_str), Some("h1"));
    Ok(())
}

#[test]
fn epochs_order_numerically() {
    assert!(Epoch::new(2) > Epoch::new(1));
    assert_eq!(Epoch::default().get(), 0);
    assert_eq!(Epoch::from(9).to_string(), "9");
}
