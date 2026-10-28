//! A follower's log end, mirrored in memory.

use fleetring_wal::{
    AppendError, Batch, FrameError, Generation, HeaderError, LogEnd, Position, ReadError, Salts,
    WalHeader, read_committed,
};

use crate::common::{FRAME, ORDERS, TWO_TXN_ENDS, WalBuilder, idx, two_txns};

/// A follower WAL kept in memory, extended the way [`fleetring_wal::Mirror`] extends a file.
struct MemoryMirror {
    end: Option<LogEnd>,
    bytes: Vec<u8>,
}

impl MemoryMirror {
    fn new() -> Self {
        Self {
            end: None,
            bytes: Vec::new(),
        }
    }

    fn ship_from(&mut self, primary: &[u8]) -> Option<Position> {
        let batch = read_committed(primary, self.end.map(|end| end.position())).expect("valid")?;
        let batch = Batch::from_bytes(&batch.to_bytes()).expect("payload round-trips");
        if let Some(header) = batch.header() {
            self.end = Some(LogEnd::new(*header));
            self.bytes = header.as_bytes().to_vec();
        }
        let end = self.end.expect("a generation has started");
        let next = end.after_batch(&batch).expect("batch continues the log");
        self.bytes.truncate(idx(end.position().offset()));
        self.bytes.extend_from_slice(batch.frame_bytes());
        self.end = Some(next);
        Some(next.position())
    }
}

#[test]
fn recover_finds_the_last_commit() {
    for order in ORDERS {
        let wal = two_txns(order).bytes();
        let end = LogEnd::recover(&wal).expect("valid").expect("has a header");
        assert_eq!(end.position().offset(), TWO_TXN_ENDS[1]);
        assert_eq!(end.header(), &WalHeader::parse(&wal).expect("valid"));
        assert_eq!(end.generation().checkpoint_seq(), 7);
        assert_eq!(LogEnd::recover(&wal[..31]), Ok(None));
        let header_only = LogEnd::recover(&wal[..32])
            .expect("valid")
            .expect("has a header");
        assert_eq!(header_only, LogEnd::new(*header_only.header()));
        assert_eq!(header_only.position().offset(), 32);
        let mut bad = wal.clone();
        bad[20] ^= 1;
        assert_eq!(LogEnd::recover(&bad), Err(HeaderError::ChecksumMismatch));
    }
}

#[test]
fn mirroring_batch_by_batch_copies_the_committed_prefix() {
    for order in ORDERS {
        let mut primary = WalBuilder::small(order);
        let mut follower = MemoryMirror::new();
        assert_eq!(follower.ship_from(&primary.bytes()), None);
        for round in 0..6u8 {
            let pages: Vec<u32> = (1..=u32::from(round % 3) + 1).collect();
            primary.txn(&pages, 3, round);
            if round % 2 == 1 {
                primary.txn(&[2], 3, round ^ 0xff);
            }
            let end = follower.ship_from(&primary.bytes()).expect("new frames");
            assert_eq!(end.offset(), primary.offset());
            assert_eq!(follower.bytes, primary.bytes(), "{order:?} round {round}");
        }
        // A torn transaction on the primary isn't shipped.
        primary.frame(1, 0, 0x55);
        assert_eq!(follower.ship_from(&primary.bytes()), None);
    }
}

#[test]
fn a_batch_must_continue_exactly_from_the_end() {
    for order in ORDERS {
        let wal = two_txns(order).bytes();
        let header = WalHeader::parse(&wal).expect("valid");
        let whole = read_committed(&wal, None).expect("valid").expect("frames");
        let second = read_committed(
            &wal,
            Some(Position::new(header.generation(), TWO_TXN_ENDS[0])),
        )
        .expect("valid")
        .expect("frames");
        let empty = LogEnd::new(header);

        // A gap.
        assert_eq!(
            empty.after_batch(&second),
            Err(AppendError::Discontinuous {
                expected: header.start(),
                found: second.start()
            })
        );
        // An overlap.
        let full = empty.after_batch(&whole).expect("continues");
        assert_eq!(full.position(), whole.end());
        assert_eq!(
            full.after_batch(&whole),
            Err(AppendError::Discontinuous {
                expected: whole.end(),
                found: whole.start()
            })
        );
        // Another generation.
        let other = WalBuilder::new(order, 512, 9, [1, 2]).bytes();
        let other = LogEnd::new(WalHeader::parse(&other).expect("valid"));
        assert!(matches!(
            other.after_batch(&whole),
            Err(AppendError::Discontinuous { .. })
        ));
    }
}

#[test]
fn a_batch_with_another_header_or_page_size_is_rejected() {
    for order in ORDERS {
        let salts = [0x1122_3344, 0x5566_7788];
        let small = two_txns(order).bytes();
        let batch = read_committed(&small, None)
            .expect("valid")
            .expect("frames");
        // Same generation, different page size: the headers differ.
        let mut big = WalBuilder::new(order, 1024, 7, salts);
        big.txn(&[1], 1, 0);
        let big_header = WalHeader::parse(&big.bytes()).expect("valid");
        assert_eq!(big_header.generation(), batch.generation());
        assert_eq!(
            LogEnd::new(big_header).after_batch(&batch),
            Err(AppendError::HeaderMismatch)
        );

        let big_batch = read_committed(&big.bytes(), Some(big_header.start()))
            .expect("valid")
            .expect("frames");
        let small_end = LogEnd::new(*batch.header().expect("starts a generation"));
        assert_eq!(
            small_end.after_batch(&big_batch),
            Err(AppendError::PageSizeMismatch {
                expected: 512,
                found: 1024
            })
        );
    }
}

#[test]
fn any_corrupted_frame_byte_is_rejected_on_append() {
    for order in ORDERS {
        let wal = two_txns(order).bytes();
        let generation = WalHeader::parse(&wal).expect("valid").generation();
        let after_first =
            LogEnd::at(&wal, Position::new(generation, TWO_TXN_ENDS[0])).expect("boundary");
        let second = read_committed(&wal, Some(after_first.position()))
            .expect("valid")
            .expect("frames");
        let bytes = second.to_bytes();
        for at in 30..bytes.len() {
            let mut bad = bytes.clone();
            bad[at] ^= 0x04;
            let applied = Batch::from_bytes(&bad).map(|batch| after_first.after_batch(&batch));
            assert!(
                !matches!(applied, Ok(Ok(_))),
                "corrupted payload byte {at} ({order:?}) was accepted"
            );
        }
        let mut bad = bytes.clone();
        bad[30 + FRAME + 200] ^= 1;
        let batch = Batch::from_bytes(&bad).expect("structure is intact");
        assert_eq!(
            after_first.after_batch(&batch),
            Err(AppendError::Frame(FrameError::ChecksumMismatch {
                frame: 1
            }))
        );
    }
}

#[test]
fn at_cuts_back_to_a_commit_boundary() {
    for order in ORDERS {
        let wal = two_txns(order).bytes();
        let generation = WalHeader::parse(&wal).expect("valid").generation();
        let first = Position::new(generation, TWO_TXN_ENDS[0]);
        let end = LogEnd::at(&wal, first).expect("boundary");
        assert_eq!(end.position(), first);
        assert_eq!(
            LogEnd::at(&wal, generation.start())
                .expect("start")
                .position(),
            generation.start()
        );

        // Re-appending what was cut gives back the same end.
        let second = read_committed(&wal, Some(first))
            .expect("valid")
            .expect("frames");
        let full = end.after_batch(&second).expect("continues");
        assert_eq!(full, LogEnd::recover(&wal).expect("valid").expect("header"));

        let inside = Position::new(generation, 32 + FRAME as u64);
        assert_eq!(
            LogEnd::at(&wal, inside),
            Err(ReadError::NotCommitBoundary { position: inside })
        );
        let past = Position::new(generation, TWO_TXN_ENDS[1] + FRAME as u64);
        assert!(matches!(
            LogEnd::at(&wal, past),
            Err(ReadError::PastEnd { .. })
        ));
        let older = Position::new(Generation::new(6, Salts::new(1, 2)), 32);
        assert_eq!(
            LogEnd::at(&wal, older),
            Err(ReadError::UnknownGeneration {
                position: older,
                current: generation
            })
        );
    }
}
