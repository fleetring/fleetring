//! Parsing WAL and frame headers.

use fleetring_wal::{ChecksumOrder, FrameHeader, HeaderError, Salts, WAL_HEADER_LEN, WalHeader};

use crate::common::{ORDERS, PAGE, WalBuilder, two_txns};

#[test]
fn header_parses_in_both_orders() {
    for order in ORDERS {
        let wal = WalBuilder::new(order, 4096, 42, [0xdead_beef, 0x0102_0304]).bytes();
        let header = WalHeader::parse(&wal).expect("hand-built header is valid");
        assert_eq!(header.checksum_order(), order);
        assert_eq!(header.page_size(), 4096);
        assert_eq!(header.checkpoint_seq(), 42);
        assert_eq!(header.salts(), Salts::new(0xdead_beef, 0x0102_0304));
        assert_eq!(header.generation().checkpoint_seq(), 42);
        assert_eq!(header.start().offset(), 32);
        assert_eq!(header.as_bytes().as_slice(), &wal[..WAL_HEADER_LEN]);
    }
}

#[test]
fn magic_low_bit_selects_the_checksum_order() {
    let little = WalBuilder::small(ChecksumOrder::LittleEndian).bytes();
    let big = WalBuilder::small(ChecksumOrder::BigEndian).bytes();
    assert_eq!(little[..4], [0x37, 0x7f, 0x06, 0x82]);
    assert_eq!(big[..4], [0x37, 0x7f, 0x06, 0x83]);
    assert_ne!(
        little[24..32],
        big[24..32],
        "the orders give different checksums"
    );
}

#[test]
fn header_parse_ignores_bytes_after_the_header() {
    let wal = two_txns(ChecksumOrder::BigEndian).bytes();
    let whole = WalHeader::parse(&wal).expect("valid");
    let prefix = WalHeader::parse(&wal[..WAL_HEADER_LEN]).expect("valid");
    assert_eq!(whole, prefix);
}

#[test]
fn short_header_is_rejected() {
    let wal = WalBuilder::small(ChecksumOrder::LittleEndian).bytes();
    for len in 0..WAL_HEADER_LEN {
        assert_eq!(
            WalHeader::parse(&wal[..len]),
            Err(HeaderError::TooShort { len })
        );
    }
}

/// Rewrites one big-endian field of a header and fixes up its checksum.
fn with_field(order: ChecksumOrder, offset: usize, value: u32) -> Vec<u8> {
    let mut wal = WalBuilder::small(order).bytes();
    wal[offset..offset + 4].copy_from_slice(&value.to_be_bytes());
    let fields: Vec<u32> = wal[..24]
        .chunks(4)
        .map(|w| u32::from_be_bytes([w[0], w[1], w[2], w[3]]))
        .collect();
    // The header checksum reads the big-endian fields as words in the checksum order.
    let read = |v: u32| match order {
        ChecksumOrder::LittleEndian => u32::from_le_bytes(v.to_be_bytes()),
        ChecksumOrder::BigEndian => v,
    };
    let (mut s0, mut s1) = (0u32, 0u32);
    for pair in fields.chunks(2) {
        s0 = s0.wrapping_add(read(pair[0])).wrapping_add(s1);
        s1 = s1.wrapping_add(read(pair[1])).wrapping_add(s0);
    }
    wal[24..28].copy_from_slice(&s0.to_be_bytes());
    wal[28..32].copy_from_slice(&s1.to_be_bytes());
    wal
}

#[test]
fn bad_magic_is_rejected() {
    for magic in [0, 0x377f_0681, 0x377f_0684, 0x8206_7f37, 0x377f_0782] {
        let wal = with_field(ChecksumOrder::LittleEndian, 0, magic);
        assert_eq!(WalHeader::parse(&wal), Err(HeaderError::BadMagic { magic }));
    }
}

#[test]
fn other_format_versions_are_rejected() {
    for order in ORDERS {
        for version in [0, 3_006_999, 3_007_001, 3_008_000] {
            let wal = with_field(order, 4, version);
            assert_eq!(
                WalHeader::parse(&wal),
                Err(HeaderError::UnsupportedVersion { version })
            );
        }
    }
}

#[test]
fn page_size_must_be_a_power_of_two_from_512_to_65536() {
    for order in ORDERS {
        for page_size in [0, 1, 256, 511, 513, 1000, 4095, 131_072, u32::MAX] {
            let wal = with_field(order, 8, page_size);
            assert_eq!(
                WalHeader::parse(&wal),
                Err(HeaderError::InvalidPageSize { page_size })
            );
        }
        for shift in 9..=16 {
            let wal = with_field(order, 8, 1 << shift);
            let header = WalHeader::parse(&wal).expect("power-of-two page sizes are valid");
            assert_eq!(header.page_size(), 1 << shift);
        }
    }
}

#[test]
fn every_header_byte_is_covered() {
    for order in ORDERS {
        let wal = WalBuilder::small(order).bytes();
        for at in 0..WAL_HEADER_LEN {
            for bit in [0x01, 0x80] {
                let mut bad = wal.clone();
                bad[at] ^= bit;
                assert!(
                    WalHeader::parse(&bad).is_err(),
                    "flipping bit {bit:#x} of header byte {at} ({order:?}) went unnoticed"
                );
            }
        }
    }
}

#[test]
fn frame_header_decodes_its_fields() {
    let wal = two_txns(ChecksumOrder::LittleEndian).bytes();
    let first: [u8; 24] = wal[32..56].try_into().expect("24 bytes");
    let frame = FrameHeader::from_bytes(&first);
    assert_eq!(frame.page_number(), 1);
    assert_eq!(frame.commit_size(), None);
    assert!(!frame.is_commit());
    assert_eq!(
        frame.salts(),
        WalHeader::parse(&wal).expect("valid").salts()
    );

    let commit_at = 32 + 2 * (24 + PAGE as usize);
    let third: [u8; 24] = wal[commit_at..commit_at + 24].try_into().expect("24 bytes");
    let commit = FrameHeader::from_bytes(&third);
    assert_eq!(commit.page_number(), 3);
    assert_eq!(commit.commit_size(), Some(3));
    assert!(commit.is_commit());
}
