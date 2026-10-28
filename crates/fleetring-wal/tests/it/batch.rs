//! The batch payload encoding.

use fleetring_wal::{
    Batch, DecodeBatchError, FrameError, HeaderError, Position, WalHeader, read_committed,
};
use proptest::prelude::*;

use crate::common::{FRAME, ORDERS, PAGE, TWO_TXN_ENDS, WalBuilder, idx, two_txns};

/// Builds a payload by hand: version 1, then the given fields.
fn payload(kind: u8, start: Position, page_size: u32, frame_count: u32, rest: &[u8]) -> Vec<u8> {
    let mut out = vec![1, kind];
    out.extend_from_slice(&start.to_bytes());
    out.extend_from_slice(&page_size.to_be_bytes());
    out.extend_from_slice(&frame_count.to_be_bytes());
    out.extend_from_slice(rest);
    out
}

/// The batch of both transactions, starting the generation, and the second one alone.
fn batches(wal: &[u8]) -> (Batch, Batch) {
    let whole = read_committed(wal, None).expect("valid").expect("frames");
    let after = Position::new(whole.generation(), TWO_TXN_ENDS[0]);
    let second = read_committed(wal, Some(after))
        .expect("valid")
        .expect("frames");
    (whole, second)
}

#[test]
fn payloads_round_trip() {
    for order in ORDERS {
        let wal = two_txns(order).bytes();
        let (whole, second) = batches(&wal);
        for batch in [whole, second] {
            let bytes = batch.to_bytes();
            assert_eq!(Batch::from_bytes(&bytes), Ok(batch.clone()));
            let header_len = if batch.header().is_some() { 32 } else { 0 };
            assert_eq!(bytes.len(), 30 + header_len + batch.frame_bytes().len());
        }
    }
}

#[test]
fn payload_layout_is_stable() {
    let wal = two_txns(fleetring_wal::ChecksumOrder::BigEndian).bytes();
    let (whole, second) = batches(&wal);
    let mut rest = wal[..32].to_vec();
    rest.extend_from_slice(&wal[32..idx(TWO_TXN_ENDS[1])]);
    assert_eq!(whole.to_bytes(), payload(1, whole.start(), PAGE, 5, &rest));
    let frames = &wal[idx(TWO_TXN_ENDS[0])..idx(TWO_TXN_ENDS[1])];
    assert_eq!(
        second.to_bytes(),
        payload(0, second.start(), PAGE, 2, frames)
    );
}

#[test]
fn every_truncated_payload_is_rejected() {
    for order in ORDERS {
        let wal = two_txns(order).bytes();
        let (whole, second) = batches(&wal);
        for bytes in [whole.to_bytes(), second.to_bytes()] {
            for len in 0..bytes.len() {
                assert!(
                    Batch::from_bytes(&bytes[..len]).is_err(),
                    "prefix of {len} bytes decoded"
                );
            }
            let mut longer = bytes.clone();
            longer.push(0);
            assert!(Batch::from_bytes(&longer).is_err());
        }
    }
}

#[test]
fn malformed_fixed_fields_are_rejected() {
    let wal = two_txns(fleetring_wal::ChecksumOrder::LittleEndian).bytes();
    let (whole, second) = batches(&wal);
    let frames = second.frame_bytes();
    let start = second.start();

    let mut bytes = second.to_bytes();
    bytes[0] = 2;
    assert_eq!(
        Batch::from_bytes(&bytes),
        Err(DecodeBatchError::UnsupportedVersion { version: 2 })
    );
    bytes[0] = 1;
    bytes[1] = 7;
    assert_eq!(
        Batch::from_bytes(&bytes),
        Err(DecodeBatchError::UnknownKind { kind: 7 })
    );

    assert_eq!(
        Batch::from_bytes(&payload(0, start, 1000, 2, frames)),
        Err(DecodeBatchError::InvalidPageSize { page_size: 1000 })
    );
    assert_eq!(
        Batch::from_bytes(&payload(0, start, PAGE, 3, frames)),
        Err(DecodeBatchError::LengthMismatch {
            frame_count: 3,
            len: frames.len()
        })
    );

    let misaligned = Position::new(start.generation(), start.offset() + 8);
    assert_eq!(
        Batch::from_bytes(&payload(0, misaligned, PAGE, 2, frames)),
        Err(DecodeBatchError::MisalignedStart { start: misaligned })
    );
    let before_header = Position::new(start.generation(), 0);
    assert_eq!(
        Batch::from_bytes(&payload(0, before_header, PAGE, 2, frames)),
        Err(DecodeBatchError::MisalignedStart {
            start: before_header
        })
    );

    // A generation-starting batch must start right after its header and share its page size.
    let mut rest = whole
        .header()
        .expect("starts a generation")
        .as_bytes()
        .to_vec();
    rest.extend_from_slice(whole.frame_bytes());
    assert_eq!(
        Batch::from_bytes(&payload(1, start, PAGE, 5, &rest)),
        Err(DecodeBatchError::HeaderMismatch { start })
    );
    let mut big_header = WalBuilder::new(
        fleetring_wal::ChecksumOrder::LittleEndian,
        1024,
        7,
        [0x1122_3344, 0x5566_7788],
    )
    .bytes();
    big_header.extend_from_slice(whole.frame_bytes());
    assert_eq!(
        Batch::from_bytes(&payload(1, whole.start(), PAGE, 5, &big_header)),
        Err(DecodeBatchError::PageSizeMismatch {
            batch: PAGE,
            header: 1024
        })
    );
    let mut bad_header = rest.clone();
    bad_header[0] = 0;
    assert_eq!(
        Batch::from_bytes(&payload(1, whole.start(), PAGE, 5, &bad_header)),
        Err(DecodeBatchError::Header(HeaderError::BadMagic {
            magic: 0x007f_0682
        }))
    );
}

#[test]
fn frames_that_are_not_whole_committed_transactions_are_rejected() {
    let mut builder = WalBuilder::small(fleetring_wal::ChecksumOrder::LittleEndian);
    builder.txn(&[1, 2], 2, 0).frame(3, 0, 0);
    let wal = builder.bytes();
    let generation = WalHeader::parse(&wal).expect("valid").generation();
    let start = generation.start();

    assert_eq!(
        Batch::from_bytes(&payload(0, start, PAGE, 0, &[])),
        Err(DecodeBatchError::Frame(FrameError::Empty))
    );
    assert_eq!(
        Batch::from_bytes(&payload(0, start, PAGE, 3, &wal[32..])),
        Err(DecodeBatchError::Frame(FrameError::MissingCommit))
    );

    let other = Position::new(
        fleetring_wal::Generation::new(7, fleetring_wal::Salts::new(1, 2)),
        32,
    );
    assert_eq!(
        Batch::from_bytes(&payload(0, other, PAGE, 2, &wal[32..32 + 2 * FRAME])),
        Err(DecodeBatchError::Frame(FrameError::SaltMismatch {
            frame: 0
        }))
    );

    let mut zero_page = wal[32..32 + 2 * FRAME].to_vec();
    zero_page[FRAME..FRAME + 4].fill(0);
    assert_eq!(
        Batch::from_bytes(&payload(0, start, PAGE, 2, &zero_page)),
        Err(DecodeBatchError::Frame(FrameError::ZeroPageNumber {
            frame: 1
        }))
    );
}

#[test]
fn a_generation_starting_payload_has_its_checksums_checked() {
    for order in ORDERS {
        let wal = two_txns(order).bytes();
        let (whole, _) = batches(&wal);
        let bytes = whole.to_bytes();
        let frames_at = 30 + 32;
        // Corrupt the page content of each frame in turn; the frame headers stay well formed.
        for frame in 0..5 {
            let mut bad = bytes.clone();
            bad[frames_at + frame * FRAME + 24 + 100] ^= 1;
            assert_eq!(
                Batch::from_bytes(&bad),
                Err(DecodeBatchError::Frame(FrameError::ChecksumMismatch {
                    frame
                })),
                "{order:?}"
            );
        }
    }
}

proptest! {
    #[test]
    fn arbitrary_payloads_never_panic(bytes in proptest::collection::vec(any::<u8>(), 0..200)) {
        let _ = Batch::from_bytes(&bytes);
    }

    #[test]
    fn mutated_payloads_never_panic(at in 0usize..4096, value in any::<u8>()) {
        let wal = two_txns(fleetring_wal::ChecksumOrder::BigEndian).bytes();
        let mut bytes = read_committed(&wal, None).unwrap().unwrap().to_bytes();
        let at = at % bytes.len();
        bytes[at] = value;
        let _ = Batch::from_bytes(&bytes);
    }
}
