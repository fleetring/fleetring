//! WAL frames: the 24-byte frame header and the rules that make a frame valid.

use crate::checksum::{Checksum, ChecksumOrder};
use crate::header::{Salts, WalHeader, be_u32};

/// The length of a frame header in bytes. The page follows it.
pub const FRAME_HEADER_LEN: usize = 24;

/// A 24-byte WAL frame header.
///
/// Its fields are big-endian:
///
/// | Offset | Field |
/// |---|---|
/// | 0 | page number |
/// | 4 | for a commit frame, the database size in pages after the commit; otherwise 0 |
/// | 8 | first salt |
/// | 12 | second salt |
/// | 16 | first cumulative checksum word |
/// | 20 | second cumulative checksum word |
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct FrameHeader {
    page_number: u32,
    commit_size: u32,
    salts: Salts,
    checksum: [u32; 2],
}

impl FrameHeader {
    /// Decodes a frame header. Any 24 bytes decode; validity depends on the WAL around it.
    #[must_use]
    pub fn from_bytes(bytes: &[u8; FRAME_HEADER_LEN]) -> Self {
        Self {
            page_number: be_u32(bytes, 0),
            commit_size: be_u32(bytes, 4),
            salts: Salts::new(be_u32(bytes, 8), be_u32(bytes, 12)),
            checksum: [be_u32(bytes, 16), be_u32(bytes, 20)],
        }
    }

    /// The number of the database page this frame holds, counting from 1.
    #[must_use]
    pub const fn page_number(&self) -> u32 {
        self.page_number
    }

    /// For a commit frame, the database size in pages after the transaction; otherwise `None`.
    #[must_use]
    pub const fn commit_size(&self) -> Option<u32> {
        if self.commit_size == 0 {
            None
        } else {
            Some(self.commit_size)
        }
    }

    /// Whether this frame ends a transaction.
    #[must_use]
    pub const fn is_commit(&self) -> bool {
        self.commit_size != 0
    }

    /// The salts this frame carries. They must match the WAL header's.
    #[must_use]
    pub const fn salts(&self) -> Salts {
        self.salts
    }

    /// The cumulative checksum stored in this frame.
    #[must_use]
    pub const fn checksum(&self) -> [u32; 2] {
        self.checksum
    }
}

/// An error from validating the frames of a batch.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum FrameError {
    /// The batch holds no frames.
    #[error("batch has no frames")]
    Empty,

    /// The frame bytes aren't a whole number of frames.
    #[error("{len} bytes are not a whole number of {frame_len}-byte frames")]
    PartialFrame {
        /// The length of the frame bytes.
        len: usize,
        /// The length of one frame, header included.
        frame_len: usize,
    },

    /// A frame's salts don't match its generation's.
    #[error("frame {frame} has salts from another generation")]
    SaltMismatch {
        /// The frame's index in the batch, counting from 0.
        frame: usize,
    },

    /// A frame names page 0, which doesn't exist.
    #[error("frame {frame} has page number 0")]
    ZeroPageNumber {
        /// The frame's index in the batch, counting from 0.
        frame: usize,
    },

    /// A frame's cumulative checksum doesn't match the log before it and its own contents.
    #[error("frame {frame} checksum mismatch")]
    ChecksumMismatch {
        /// The frame's index in the batch, counting from 0.
        frame: usize,
    },

    /// The last frame isn't a commit frame, so the batch would end inside a transaction.
    #[error("batch does not end with a commit frame")]
    MissingCommit,
}

/// Checks everything about `frames` that doesn't need the checksum chain: they are whole,
/// non-empty, carry `salts`, name real pages, and end with a commit frame.
pub(crate) fn check_structure(
    frames: &[u8],
    frame_len: usize,
    salts: Salts,
) -> Result<(), FrameError> {
    if frames.is_empty() {
        return Err(FrameError::Empty);
    }
    if !frames.len().is_multiple_of(frame_len) {
        return Err(FrameError::PartialFrame {
            len: frames.len(),
            frame_len,
        });
    }
    let mut last = None;
    for (frame, bytes) in frames.chunks_exact(frame_len).enumerate() {
        let header = frame_header(bytes);
        if header.salts() != salts {
            return Err(FrameError::SaltMismatch { frame });
        }
        if header.page_number() == 0 {
            return Err(FrameError::ZeroPageNumber { frame });
        }
        last = Some(header);
    }
    match last {
        Some(header) if header.is_commit() => Ok(()),
        _ => Err(FrameError::MissingCommit),
    }
}

/// Verifies the cumulative checksum of each whole frame in `frames`, starting from `checksum`,
/// and returns the checksum after the last frame.
pub(crate) fn check_chain(
    frames: &[u8],
    frame_len: usize,
    order: ChecksumOrder,
    mut checksum: Checksum,
) -> Result<Checksum, FrameError> {
    for (frame, bytes) in frames.chunks_exact(frame_len).enumerate() {
        checksum = frame_checksum(bytes, order, checksum);
        if checksum.0 != frame_header(bytes).checksum() {
            return Err(FrameError::ChecksumMismatch { frame });
        }
    }
    Ok(checksum)
}

/// One valid frame found by [`Frames`].
#[derive(Debug, Clone, Copy)]
pub(crate) struct ValidFrame {
    /// The WAL offset just past the frame.
    pub(crate) end: usize,
    /// Whether the frame commits a transaction.
    pub(crate) is_commit: bool,
    /// The cumulative checksum after the frame.
    pub(crate) checksum: Checksum,
}

/// Walks the valid frames of a WAL in order, stopping, as SQLite's recovery does, at the first
/// frame that is cut short, carries other salts, names page 0, or breaks the checksum chain.
#[derive(Debug)]
pub(crate) struct Frames<'wal> {
    wal: &'wal [u8],
    frame_len: usize,
    order: ChecksumOrder,
    salts: Salts,
    offset: usize,
    checksum: Checksum,
}

impl<'wal> Frames<'wal> {
    /// Starts at the first frame after `header`, which must be the header of `wal`.
    pub(crate) fn new(wal: &'wal [u8], header: &WalHeader) -> Self {
        Self {
            wal,
            frame_len: header.frame_len(),
            order: header.checksum_order(),
            salts: header.salts(),
            offset: crate::header::WAL_HEADER_LEN,
            checksum: header.checksum(),
        }
    }

    /// Starts at the first byte of `frames`, a tail of `header`'s WAL that follows a frame whose
    /// cumulative checksum is `checksum`. Offsets in the frames found are relative to `frames`.
    pub(crate) fn resume(frames: &'wal [u8], header: &WalHeader, checksum: Checksum) -> Self {
        Self {
            wal: frames,
            frame_len: header.frame_len(),
            order: header.checksum_order(),
            salts: header.salts(),
            offset: 0,
            checksum,
        }
    }
}

impl Iterator for Frames<'_> {
    type Item = ValidFrame;

    fn next(&mut self) -> Option<ValidFrame> {
        let end = self.offset.checked_add(self.frame_len)?;
        let bytes = self.wal.get(self.offset..end)?;
        let header = frame_header(bytes);
        if header.salts() != self.salts || header.page_number() == 0 {
            return None;
        }
        let checksum = frame_checksum(bytes, self.order, self.checksum);
        if checksum.0 != header.checksum() {
            return None;
        }
        self.offset = end;
        self.checksum = checksum;
        Some(ValidFrame {
            end,
            is_commit: header.is_commit(),
            checksum,
        })
    }
}

/// Decodes the header of a whole frame.
pub(crate) fn frame_header(frame: &[u8]) -> FrameHeader {
    let mut bytes = [0; FRAME_HEADER_LEN];
    bytes.copy_from_slice(&frame[..FRAME_HEADER_LEN]);
    FrameHeader::from_bytes(&bytes)
}

/// Continues `checksum` over a whole frame: the first 8 bytes of its header, then its page.
fn frame_checksum(frame: &[u8], order: ChecksumOrder, checksum: Checksum) -> Checksum {
    checksum
        .update(order, &frame[..8])
        .update(order, &frame[FRAME_HEADER_LEN..])
}
