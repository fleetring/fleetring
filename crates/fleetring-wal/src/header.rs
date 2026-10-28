//! The 32-byte WAL header.

use std::fmt;

use crate::checksum::{Checksum, ChecksumOrder};
use crate::position::{Generation, Position};

/// The length of the WAL header in bytes. The first frame starts at this offset.
pub const WAL_HEADER_LEN: usize = 32;

/// The magic number of a WAL whose checksums read little-endian words. Setting the low bit
/// selects big-endian words.
const MAGIC: u32 = 0x377f_0682;

/// The only WAL format version SQLite has defined, written as 3007000 (SQLite 3.7.0).
pub(crate) const FORMAT_VERSION: u32 = 3_007_000;

/// The smallest page size SQLite supports.
const MIN_PAGE_SIZE: u32 = 512;

/// The largest page size SQLite supports.
const MAX_PAGE_SIZE: u32 = 65_536;

/// The two salts that SQLite writes into the WAL header and copies into every frame.
///
/// A frame belongs to the current generation of the WAL only if its salts match the header's.
/// SQLite increments the first salt and draws a new random second salt each time a checkpoint
/// restarts the WAL.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Salts {
    first: u32,
    second: u32,
}

impl Salts {
    /// Creates salts from the two values in header order.
    #[must_use]
    pub const fn new(first: u32, second: u32) -> Self {
        Self { first, second }
    }

    /// The first salt, at header offset 16 and frame offset 8.
    #[must_use]
    pub const fn first(self) -> u32 {
        self.first
    }

    /// The second salt, at header offset 20 and frame offset 12.
    #[must_use]
    pub const fn second(self) -> u32 {
        self.second
    }
}

/// A validated 32-byte WAL header.
///
/// Its fields are big-endian whatever the checksum byte order:
///
/// | Offset | Field |
/// |---|---|
/// | 0 | magic number, `0x377f0682` or `0x377f0683` |
/// | 4 | format version, 3007000 |
/// | 8 | page size |
/// | 12 | checkpoint sequence number |
/// | 16 | first salt |
/// | 20 | second salt |
/// | 24 | first checksum word, over bytes 0–23 |
/// | 28 | second checksum word |
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct WalHeader {
    bytes: [u8; WAL_HEADER_LEN],
}

impl WalHeader {
    /// Parses and validates the header at the start of `bytes`.
    ///
    /// Bytes after the first [`WAL_HEADER_LEN`] are ignored, so a whole WAL file may be passed.
    ///
    /// # Errors
    ///
    /// Returns a [`HeaderError`] if `bytes` is shorter than a header, or if the magic number,
    /// format version, page size, or header checksum is invalid.
    pub fn parse(bytes: &[u8]) -> Result<Self, HeaderError> {
        let Some(prefix) = bytes.first_chunk::<WAL_HEADER_LEN>() else {
            return Err(HeaderError::TooShort { len: bytes.len() });
        };
        let header = Self { bytes: *prefix };
        let magic = header.field(0);
        if magic & !1 != MAGIC {
            return Err(HeaderError::BadMagic { magic });
        }
        let version = header.field(4);
        if version != FORMAT_VERSION {
            return Err(HeaderError::UnsupportedVersion { version });
        }
        let page_size = header.field(8);
        if !is_valid_page_size(page_size) {
            return Err(HeaderError::InvalidPageSize { page_size });
        }
        let computed = Checksum::default().update(header.checksum_order(), &header.bytes[..24]);
        if computed != header.checksum() {
            return Err(HeaderError::ChecksumMismatch);
        }
        Ok(header)
    }

    /// The header's 32 bytes, exactly as they appear in the WAL file.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; WAL_HEADER_LEN] {
        &self.bytes
    }

    /// The byte order that this WAL's checksums use.
    #[must_use]
    pub fn checksum_order(&self) -> ChecksumOrder {
        if self.field(0) & 1 == 0 {
            ChecksumOrder::LittleEndian
        } else {
            ChecksumOrder::BigEndian
        }
    }

    /// The database page size in bytes: a power of two from 512 to 65536.
    #[must_use]
    pub fn page_size(&self) -> u32 {
        self.field(8)
    }

    /// The checkpoint sequence number, which SQLite increments each time it restarts the WAL.
    #[must_use]
    pub fn checkpoint_seq(&self) -> u32 {
        self.field(12)
    }

    /// The salts that every frame of this generation carries.
    #[must_use]
    pub fn salts(&self) -> Salts {
        Salts::new(self.field(16), self.field(20))
    }

    /// The generation of the WAL that this header starts.
    #[must_use]
    pub fn generation(&self) -> Generation {
        Generation::new(self.checkpoint_seq(), self.salts())
    }

    /// The position just after this header, before the generation's first frame.
    #[must_use]
    pub fn start(&self) -> Position {
        self.generation().start()
    }

    /// The length of one frame, header included, in bytes.
    pub(crate) fn frame_len(&self) -> usize {
        frame_len(self.page_size())
    }

    /// The header's checksum, which seeds the first frame's checksum.
    pub(crate) fn checksum(&self) -> Checksum {
        Checksum([self.field(24), self.field(28)])
    }

    /// Reads the big-endian field at `offset`.
    fn field(&self, offset: usize) -> u32 {
        be_u32(&self.bytes, offset)
    }
}

impl fmt::Debug for WalHeader {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WalHeader")
            .field("checksum_order", &self.checksum_order())
            .field("page_size", &self.page_size())
            .field("checkpoint_seq", &self.checkpoint_seq())
            .field("salts", &self.salts())
            .finish()
    }
}

/// An error from parsing a WAL header.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum HeaderError {
    /// The input is shorter than a WAL header.
    #[error("wal header needs {WAL_HEADER_LEN} bytes, got {len}")]
    TooShort {
        /// The input's length in bytes.
        len: usize,
    },

    /// The magic number is neither `0x377f0682` nor `0x377f0683`.
    #[error("bad wal magic number {magic:#010x}")]
    BadMagic {
        /// The magic number found.
        magic: u32,
    },

    /// The format version isn't 3007000.
    #[error("unsupported wal format version {version}")]
    UnsupportedVersion {
        /// The version found.
        version: u32,
    },

    /// The page size isn't a power of two from 512 to 65536.
    #[error("invalid wal page size {page_size}")]
    InvalidPageSize {
        /// The page size found.
        page_size: u32,
    },

    /// The header's checksum doesn't match its contents.
    #[error("wal header checksum mismatch")]
    ChecksumMismatch,
}

/// Whether `page_size` is a power of two from 512 to 65536.
pub(crate) fn is_valid_page_size(page_size: u32) -> bool {
    page_size.is_power_of_two() && (MIN_PAGE_SIZE..=MAX_PAGE_SIZE).contains(&page_size)
}

/// The length of one frame, header included, for a valid page size.
pub(crate) fn frame_len(page_size: u32) -> usize {
    let page_size = usize::try_from(page_size).unwrap_or(usize::MAX);
    crate::frame::FRAME_HEADER_LEN.saturating_add(page_size)
}

/// Reads a big-endian `u32` at `offset`. The caller guarantees `bytes` holds it.
pub(crate) fn be_u32(bytes: &[u8], offset: usize) -> u32 {
    let mut word = [0; 4];
    word.copy_from_slice(&bytes[offset..offset + 4]);
    u32::from_be_bytes(word)
}
