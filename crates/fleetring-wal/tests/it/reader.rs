//! Reading committed transactions from hand-built WAL bytes.

use fleetring_wal::{
    Generation, HeaderError, Position, ReadError, Salts, WalHeader, read_committed,
};

use crate::common::{FRAME, ORDERS, TWO_TXN_ENDS, WalBuilder, idx, two_txns};

/// The committed end that a read of `wal` from its start reaches, or 32 if it returns nothing.
fn end_from_start(wal: &[u8]) -> u64 {
    read_committed(wal, None)
        .expect("valid header")
        .map_or(32, |batch| batch.end().offset())
}

#[test]
fn reads_every_committed_frame_from_the_start() {
    for order in ORDERS {
        let wal = two_txns(order).bytes();
        let header = WalHeader::parse(&wal).expect("valid");
        let batch = read_committed(&wal, None)
            .expect("valid")
            .expect("committed frames");
        assert_eq!(batch.header(), Some(&header));
        assert_eq!(batch.start(), header.start());
        assert_eq!(
            batch.end(),
            Position::new(header.generation(), TWO_TXN_ENDS[1])
        );
        assert_eq!(batch.frame_count(), 5);
        assert_eq!(batch.frame_bytes(), &wal[32..idx(TWO_TXN_ENDS[1])]);
        let pages: Vec<u32> = batch.frame_headers().map(|f| f.page_number()).collect();
        assert_eq!(pages, [1, 2, 3, 2, 4]);
        let commits: Vec<Option<u32>> = batch.frame_headers().map(|f| f.commit_size()).collect();
        assert_eq!(commits, [None, None, Some(3), None, Some(4)]);
    }
}

#[test]
fn reads_only_what_follows_the_position() {
    for order in ORDERS {
        let wal = two_txns(order).bytes();
        let generation = WalHeader::parse(&wal).expect("valid").generation();
        let after = Position::new(generation, TWO_TXN_ENDS[0]);
        let batch = read_committed(&wal, Some(after))
            .expect("valid")
            .expect("second txn");
        assert_eq!(batch.header(), None);
        assert_eq!(batch.start(), after);
        assert_eq!(batch.end().offset(), TWO_TXN_ENDS[1]);
        assert_eq!(batch.frame_count(), 2);

        let at_end = Position::new(generation, TWO_TXN_ENDS[1]);
        assert_eq!(read_committed(&wal, Some(at_end)), Ok(None));

        let from_header = read_committed(&wal, Some(generation.start())).expect("valid");
        let from_header = from_header.expect("both txns");
        assert_eq!(
            from_header.header(),
            None,
            "the reader already has the header"
        );
        assert_eq!(from_header.frame_count(), 5);
    }
}

#[test]
fn nothing_committed_reads_as_none() {
    for order in ORDERS {
        let header_only = WalBuilder::small(order).bytes();
        assert_eq!(read_committed(&header_only, None), Ok(None));
        for len in 0..32 {
            assert_eq!(read_committed(&header_only[..len], None), Ok(None));
        }
        let mut uncommitted = WalBuilder::small(order);
        uncommitted.frame(1, 0, 1).frame(2, 0, 1);
        assert_eq!(read_committed(&uncommitted.bytes(), None), Ok(None));
    }
}

#[test]
fn a_torn_transaction_is_never_returned() {
    for order in ORDERS {
        let wal = two_txns(order).bytes();
        for len in 0..=wal.len() {
            let expected = TWO_TXN_ENDS
                .iter()
                .copied()
                .filter(|&end| end <= len as u64)
                .max();
            let got = read_committed(&wal[..len], None).expect("header is intact or absent");
            assert_eq!(
                got.map(|batch| batch.end().offset()),
                expected,
                "WAL cut at {len} bytes ({order:?})"
            );
        }
    }
}

#[test]
fn a_corrupted_frame_byte_ends_the_log_before_its_transaction() {
    for order in ORDERS {
        let wal = two_txns(order).bytes();
        let txn_starts = [32, TWO_TXN_ENDS[0], TWO_TXN_ENDS[1]];
        for at in 32..wal.len() {
            let frame_start = 32 + (at - 32) / FRAME * FRAME;
            let expected = txn_starts
                .iter()
                .copied()
                .filter(|&s| s <= frame_start as u64)
                .max();
            let expected = expected.expect("frames start at 32");
            let mut bad = wal.clone();
            bad[at] ^= 0x10;
            assert_eq!(
                end_from_start(&bad),
                expected,
                "corrupted byte {at} ({order:?})"
            );
        }
    }
}

#[test]
fn page_zero_ends_the_log() {
    for order in ORDERS {
        let mut wal = WalBuilder::small(order);
        wal.txn(&[1], 1, 0).txn(&[0], 1, 0);
        assert_eq!(end_from_start(&wal.bytes()), 32 + FRAME as u64);
    }
}

#[test]
fn frames_left_over_from_an_older_generation_are_ignored() {
    for order in ORDERS {
        let old = two_txns(order).bytes();
        let mut restarted = WalBuilder::new(order, 512, 8, [0x1122_3345, 0x0bad_f00d]);
        restarted.txn(&[1], 4, 0xd0);
        // SQLite overwrites the start of the file and leaves the old generation's frames after.
        let mut wal = old.clone();
        let new = restarted.bytes();
        wal[..new.len()].copy_from_slice(&new);

        let batch = read_committed(&wal, None).expect("valid").expect("new txn");
        assert_eq!(batch.generation().checkpoint_seq(), 8);
        assert_eq!(batch.frame_count(), 1);
        assert_eq!(batch.end().offset(), 32 + FRAME as u64);
    }
}

#[test]
fn an_older_generation_reads_the_new_one_from_its_header() {
    for order in ORDERS {
        let wal = two_txns(order).bytes();
        let header = WalHeader::parse(&wal).expect("valid");
        let older = Position::new(Generation::new(6, Salts::new(1, 2)), 9_999);
        let batch = read_committed(&wal, Some(older))
            .expect("valid")
            .expect("frames");
        assert_eq!(batch.header(), Some(&header));
        assert_eq!(batch.start(), header.start());
        assert_eq!(batch.frame_count(), 5);
    }
}

#[test]
fn positions_the_wal_cannot_continue_are_errors() {
    for order in ORDERS {
        let wal = two_txns(order).bytes();
        let generation = WalHeader::parse(&wal).expect("valid").generation();
        let end = Position::new(generation, TWO_TXN_ENDS[1]);

        let past = Position::new(generation, TWO_TXN_ENDS[1] + FRAME as u64);
        assert_eq!(
            read_committed(&wal, Some(past)),
            Err(ReadError::PastEnd {
                position: past,
                end
            })
        );

        for offset in [0, 31, 33, 32 + FRAME as u64, TWO_TXN_ENDS[0] + 1] {
            let position = Position::new(generation, offset);
            assert_eq!(
                read_committed(&wal, Some(position)),
                Err(ReadError::NotCommitBoundary { position }),
                "offset {offset}"
            );
        }

        let newer = Position::new(Generation::new(8, generation.salts()), 32);
        let sibling = Position::new(Generation::new(7, Salts::new(9, 9)), 32);
        for position in [newer, sibling] {
            assert_eq!(
                read_committed(&wal, Some(position)),
                Err(ReadError::UnknownGeneration {
                    position,
                    current: generation
                })
            );
        }
    }
}

#[test]
fn a_bad_header_is_an_error() {
    let mut wal = two_txns(fleetring_wal::ChecksumOrder::LittleEndian).bytes();
    wal[0] ^= 1;
    assert_eq!(
        read_committed(&wal, None),
        Err(ReadError::Header(HeaderError::BadMagic {
            magic: 0x367f_0682
        }))
    );
    // Flipping the order bit alone leaves a valid magic number but breaks the checksum.
    let mut wal = two_txns(fleetring_wal::ChecksumOrder::LittleEndian).bytes();
    wal[3] ^= 1;
    assert_eq!(
        read_committed(&wal, None),
        Err(ReadError::Header(HeaderError::ChecksumMismatch))
    );
}
