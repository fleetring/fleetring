//! Hand-built WAL bytes, written independently of the crate's own encoder.

#[cfg(not(target_family = "wasm"))]
pub(crate) mod sqlite;

use fleetring_wal::ChecksumOrder;

/// Both checksum byte orders, for tests that must pass in each.
pub(crate) const ORDERS: [ChecksumOrder; 2] =
    [ChecksumOrder::LittleEndian, ChecksumOrder::BigEndian];

/// The smallest page size, which keeps byte-by-byte tests fast.
pub(crate) const PAGE: u32 = 512;

/// The length of one frame at [`PAGE`].
pub(crate) const FRAME: usize = 24 + PAGE as usize;

/// Builds a WAL file the way SQLite writes one.
#[derive(Debug, Clone)]
pub(crate) struct WalBuilder {
    order: ChecksumOrder,
    page_size: u32,
    salts: [u32; 2],
    sum: [u32; 2],
    bytes: Vec<u8>,
}

impl WalBuilder {
    /// Starts a WAL with the given checksum order, page size, checkpoint sequence, and salts.
    pub(crate) fn new(
        order: ChecksumOrder,
        page_size: u32,
        checkpoint_seq: u32,
        salts: [u32; 2],
    ) -> Self {
        let magic: u32 = match order {
            ChecksumOrder::LittleEndian => 0x377f_0682,
            ChecksumOrder::BigEndian => 0x377f_0683,
        };
        let mut bytes = Vec::new();
        for field in [
            magic,
            3_007_000,
            page_size,
            checkpoint_seq,
            salts[0],
            salts[1],
        ] {
            bytes.extend_from_slice(&field.to_be_bytes());
        }
        let sum = checksum(order, [0, 0], &bytes);
        bytes.extend_from_slice(&sum[0].to_be_bytes());
        bytes.extend_from_slice(&sum[1].to_be_bytes());
        Self {
            order,
            page_size,
            salts,
            sum,
            bytes,
        }
    }

    /// A little- or big-endian WAL at [`PAGE`] with fixed salts.
    pub(crate) fn small(order: ChecksumOrder) -> Self {
        Self::new(order, PAGE, 7, [0x1122_3344, 0x5566_7788])
    }

    /// Appends one frame for `page`, a commit frame when `commit_size` isn't 0.
    pub(crate) fn frame(&mut self, page: u32, commit_size: u32, fill: u8) -> &mut Self {
        let mut frame = Vec::with_capacity(24 + self.page_size as usize);
        frame.extend_from_slice(&page.to_be_bytes());
        frame.extend_from_slice(&commit_size.to_be_bytes());
        let content = page_content(page, fill, self.page_size);
        let sum = checksum(self.order, self.sum, &frame);
        let sum = checksum(self.order, sum, &content);
        frame.extend_from_slice(&self.salts[0].to_be_bytes());
        frame.extend_from_slice(&self.salts[1].to_be_bytes());
        frame.extend_from_slice(&sum[0].to_be_bytes());
        frame.extend_from_slice(&sum[1].to_be_bytes());
        frame.extend_from_slice(&content);
        self.sum = sum;
        self.bytes.extend_from_slice(&frame);
        self
    }

    /// Appends a transaction writing `pages`, committing with a database of `db_size` pages.
    pub(crate) fn txn(&mut self, pages: &[u32], db_size: u32, fill: u8) -> &mut Self {
        let (last, rest) = pages
            .split_last()
            .expect("a transaction writes at least one page");
        for &page in rest {
            self.frame(page, 0, fill);
        }
        self.frame(*last, db_size, fill)
    }

    /// The WAL so far.
    pub(crate) fn bytes(&self) -> Vec<u8> {
        self.bytes.clone()
    }

    /// The WAL's length so far, as a position offset.
    pub(crate) fn offset(&self) -> u64 {
        self.bytes.len() as u64
    }
}

/// The deterministic content of `page` for a given fill byte.
pub(crate) fn page_content(page: u32, fill: u8, page_size: u32) -> Vec<u8> {
    (0..page_size)
        .map(|i| i.to_le_bytes()[0] ^ fill ^ page.to_le_bytes()[0].rotate_left(3))
        .collect()
}

/// SQLite's checksum, from the description in the WAL format documentation.
fn checksum(order: ChecksumOrder, [mut s0, mut s1]: [u32; 2], data: &[u8]) -> [u32; 2] {
    assert_eq!(data.len() % 8, 0, "checksummed length {}", data.len());
    for pair in data.chunks(8) {
        let words: Vec<u32> = pair
            .chunks(4)
            .map(|w| {
                let w = [w[0], w[1], w[2], w[3]];
                match order {
                    ChecksumOrder::LittleEndian => u32::from_le_bytes(w),
                    ChecksumOrder::BigEndian => u32::from_be_bytes(w),
                }
            })
            .collect();
        s0 = s0.wrapping_add(words[0]).wrapping_add(s1);
        s1 = s1.wrapping_add(words[1]).wrapping_add(s0);
    }
    [s0, s1]
}

/// Two committed transactions (3 frames, then 2) and an uncommitted frame after them.
pub(crate) fn two_txns(order: ChecksumOrder) -> WalBuilder {
    let mut wal = WalBuilder::small(order);
    wal.txn(&[1, 2, 3], 3, 0xa0)
        .txn(&[2, 4], 4, 0xb0)
        .frame(5, 0, 0xc0);
    wal
}

/// Converts a position offset into an index into WAL bytes.
pub(crate) fn idx(offset: u64) -> usize {
    usize::try_from(offset).expect("test WALs fit in memory")
}

/// The offsets just past each commit frame in [`two_txns`].
pub(crate) const TWO_TXN_ENDS: [u64; 2] = [32 + 3 * FRAME as u64, 32 + 5 * FRAME as u64];
