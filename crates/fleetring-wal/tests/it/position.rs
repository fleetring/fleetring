//! Positions and their order-preserving byte encoding.

use fleetring_wal::{DecodePositionError, Generation, Position, Salts};
use proptest::prelude::*;

fn position(checkpoint_seq: u32, first: u32, second: u32, offset: u64) -> Position {
    Position::new(
        Generation::new(checkpoint_seq, Salts::new(first, second)),
        offset,
    )
}

#[test]
fn encoding_is_fixed_width_big_endian() {
    let p = position(0x0102_0304, 0x0506_0708, 0x090a_0b0c, 0x0d0e_0f10_1112_1314);
    assert_eq!(
        p.to_bytes(),
        core::array::from_fn::<u8, 20, _>(|i| u8::try_from(i + 1).expect("20 bytes"))
    );
    assert_eq!(Position::ENCODED_LEN, 20);
}

#[test]
fn from_bytes_needs_exactly_20_bytes() {
    for len in [0, 1, 19, 21, 40] {
        assert_eq!(
            Position::from_bytes(&vec![0; len]),
            Err(DecodePositionError::WrongLength { len })
        );
    }
}

#[test]
fn display_names_generation_and_offset() {
    let p = position(3, 0xab, 0xcd, 1080);
    assert_eq!(p.to_string(), "3:000000ab000000cd@1080");
}

#[test]
fn later_generations_and_offsets_sort_later() {
    let a = position(1, u32::MAX, u32::MAX, u64::MAX);
    let b = position(2, 0, 0, 32);
    let c = position(2, 0, 0, 568);
    assert!(a < b && b < c);
    assert!(a.to_bytes() < b.to_bytes() && b.to_bytes() < c.to_bytes());
    assert_eq!(Generation::new(2, Salts::new(0, 0)).start(), b);
}

fn any_position() -> impl Strategy<Value = Position> {
    // Small ranges make equal fields common, so ties at every level are exercised.
    let field = prop_oneof![0u32..3, any::<u32>()];
    let offset = prop_oneof![0u64..3, any::<u64>()];
    (field.clone(), field.clone(), field, offset)
        .prop_map(|(seq, first, second, offset)| position(seq, first, second, offset))
}

proptest! {
    #[test]
    fn bytes_round_trip(p in any_position()) {
        prop_assert_eq!(Position::from_bytes(&p.to_bytes()), Ok(p));
    }

    #[test]
    fn byte_order_equals_position_order(a in any_position(), b in any_position()) {
        prop_assert_eq!(a.to_bytes().cmp(&b.to_bytes()), a.cmp(&b));
    }
}
