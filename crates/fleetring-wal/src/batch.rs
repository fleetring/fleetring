//! Batches of committed transactions, and their payload encoding.

use std::fmt;

use crate::checksum::Checksum;
use crate::frame::{self, FrameError, FrameHeader};
use crate::header::{self, HeaderError, WAL_HEADER_LEN, WalHeader};
use crate::position::{Generation, HEADER_END, Position};

/// The payload format version that [`Batch::to_bytes`] writes.
const PAYLOAD_VERSION: u8 = 1;

/// Payload kind: the batch continues a generation the receiver already has.
const KIND_CONTINUES: u8 = 0;

/// Payload kind: the batch starts a generation, and the WAL header follows the fixed fields.
const KIND_STARTS: u8 = 1;

/// The length of the payload's fixed fields: version, kind, start position, page size, and frame
/// count.
const PAYLOAD_FIXED_LEN: usize = 2 + Position::ENCODED_LEN + 4 + 4;

/// One or more complete committed transactions, as consecutive WAL frames.
///
/// A batch runs from [`start`](Self::start) to [`end`](Self::end) in one generation, and its last
/// frame is a commit frame. A batch that starts a generation also carries that generation's WAL
/// header, which a follower writes before the frames.
///
/// Batches come from [`read_committed`](crate::read_committed) or [`from_bytes`](Self::from_bytes).
/// Both check the frames' structure; the reader also checks their checksums, and a follower checks
/// them again when it appends the batch to its WAL.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct Batch {
    header: Option<WalHeader>,
    start: Position,
    page_size: u32,
    frames: Vec<u8>,
}

impl Batch {
    /// Builds a batch from frames already known to be valid.
    pub(crate) fn from_valid_frames(
        header: Option<WalHeader>,
        start: Position,
        page_size: u32,
        frames: Vec<u8>,
    ) -> Self {
        Self {
            header,
            start,
            page_size,
            frames,
        }
    }

    /// The header of the generation this batch starts, if it starts one.
    #[must_use]
    pub const fn header(&self) -> Option<&WalHeader> {
        self.header.as_ref()
    }

    /// The position the batch continues from. A follower must be exactly here to append it.
    #[must_use]
    pub const fn start(&self) -> Position {
        self.start
    }

    /// The position just past the batch's last commit frame.
    #[must_use]
    pub fn end(&self) -> Position {
        let len = u64::try_from(self.frames.len()).unwrap_or(u64::MAX);
        Position::new(
            self.start.generation(),
            self.start.offset().saturating_add(len),
        )
    }

    /// The generation the batch belongs to.
    #[must_use]
    pub const fn generation(&self) -> Generation {
        self.start.generation()
    }

    /// The database page size, which fixes the length of every frame.
    #[must_use]
    pub const fn page_size(&self) -> u32 {
        self.page_size
    }

    /// The frames, exactly as they appear in the WAL file.
    #[must_use]
    pub fn frame_bytes(&self) -> &[u8] {
        &self.frames
    }

    /// The number of frames in the batch. It is at least 1.
    #[must_use]
    pub fn frame_count(&self) -> usize {
        self.frames.len() / header::frame_len(self.page_size)
    }

    /// The header of each frame, in log order.
    pub fn frame_headers(&self) -> impl ExactSizeIterator<Item = FrameHeader> + '_ {
        self.frames
            .chunks_exact(header::frame_len(self.page_size))
            .map(frame::frame_header)
    }

    /// Encodes the batch as an opaque payload for shipping to a follower.
    ///
    /// The layout, version 1, is: a version byte; a kind byte, 1 if a WAL header follows and 0
    /// if not; the 20-byte start position; the page size and the frame count, each a big-endian
    /// `u32`; the 32-byte WAL header if the kind is 1; then the frames.
    ///
    /// # Panics
    ///
    /// Panics if the batch holds more than `u32::MAX` frames, which would take at least 2 TiB.
    #[must_use]
    pub fn to_bytes(&self) -> Vec<u8> {
        let frame_count = u32::try_from(self.frame_count())
            .unwrap_or_else(|_| panic!("batch of {} frames is too large", self.frame_count()));
        let header_len = if self.header.is_some() {
            WAL_HEADER_LEN
        } else {
            0
        };
        let mut out = Vec::with_capacity(PAYLOAD_FIXED_LEN + header_len + self.frames.len());
        out.push(PAYLOAD_VERSION);
        out.push(if self.header.is_some() {
            KIND_STARTS
        } else {
            KIND_CONTINUES
        });
        out.extend_from_slice(&self.start.to_bytes());
        out.extend_from_slice(&self.page_size.to_be_bytes());
        out.extend_from_slice(&frame_count.to_be_bytes());
        if let Some(header) = &self.header {
            out.extend_from_slice(header.as_bytes());
        }
        out.extend_from_slice(&self.frames);
        out
    }

    /// Decodes a payload written by [`to_bytes`](Self::to_bytes).
    ///
    /// Decoding checks the frames' structure: they are whole, carry the generation's salts, name
    /// real pages, and end with a commit frame. If the batch starts a generation, it also checks
    /// the header and the whole checksum chain. Otherwise the checksums can only be checked
    /// against the log the batch continues, which [`LogEnd::after_batch`](crate::LogEnd::after_batch)
    /// does.
    ///
    /// # Errors
    ///
    /// Returns a [`DecodeBatchError`] if the payload is malformed or its frames are invalid.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, DecodeBatchError> {
        let Some((fixed, rest)) = bytes.split_first_chunk::<PAYLOAD_FIXED_LEN>() else {
            return Err(DecodeBatchError::TooShort { len: bytes.len() });
        };
        let [version, kind, fixed @ ..] = fixed;
        if *version != PAYLOAD_VERSION {
            return Err(DecodeBatchError::UnsupportedVersion { version: *version });
        }
        let (position, sizes) = fixed.split_at(Position::ENCODED_LEN);
        let start = Position::from_bytes(position)
            .map_err(|_| DecodeBatchError::TooShort { len: bytes.len() })?;
        let page_size = header::be_u32(sizes, 0);
        let frame_count = header::be_u32(sizes, 4);
        if !header::is_valid_page_size(page_size) {
            return Err(DecodeBatchError::InvalidPageSize { page_size });
        }
        let (header, frames) = match *kind {
            KIND_CONTINUES => (None, rest),
            KIND_STARTS => {
                let header = WalHeader::parse(rest)?;
                (Some(header), &rest[WAL_HEADER_LEN..])
            }
            kind => return Err(DecodeBatchError::UnknownKind { kind }),
        };
        let frame_len = header::frame_len(page_size);
        let expected_len = usize::try_from(frame_count)
            .ok()
            .and_then(|n| n.checked_mul(frame_len));
        if expected_len != Some(frames.len()) {
            return Err(DecodeBatchError::LengthMismatch {
                frame_count,
                len: frames.len(),
            });
        }
        match &header {
            Some(header) => {
                if header.page_size() != page_size {
                    return Err(DecodeBatchError::PageSizeMismatch {
                        batch: page_size,
                        header: header.page_size(),
                    });
                }
                if start != header.start() {
                    return Err(DecodeBatchError::HeaderMismatch { start });
                }
            }
            None => {
                let aligned = start
                    .offset()
                    .checked_sub(HEADER_END)
                    .zip(u64::try_from(frame_len).ok())
                    .is_some_and(|(into, frame_len)| into.is_multiple_of(frame_len));
                if !aligned {
                    return Err(DecodeBatchError::MisalignedStart { start });
                }
            }
        }
        frame::check_structure(frames, frame_len, start.generation().salts())?;
        if let Some(header) = &header {
            frame::check_chain(
                frames,
                frame_len,
                header.checksum_order(),
                header.checksum(),
            )?;
        }
        let fits = u64::try_from(frames.len())
            .ok()
            .and_then(|len| start.offset().checked_add(len));
        if fits.is_none() {
            return Err(DecodeBatchError::MisalignedStart { start });
        }
        Ok(Self::from_valid_frames(
            header,
            start,
            page_size,
            frames.to_vec(),
        ))
    }

    /// Verifies the frames' checksum chain, continuing from `checksum`.
    pub(crate) fn check_chain(
        &self,
        order: crate::ChecksumOrder,
        checksum: Checksum,
    ) -> Result<Checksum, FrameError> {
        frame::check_chain(
            &self.frames,
            header::frame_len(self.page_size),
            order,
            checksum,
        )
    }
}

impl fmt::Debug for Batch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Batch")
            .field("header", &self.header)
            .field("start", &self.start)
            .field("end", &self.end())
            .field("page_size", &self.page_size)
            .field("frame_count", &self.frame_count())
            .finish()
    }
}

/// An error from decoding a [`Batch`] payload.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum DecodeBatchError {
    /// The payload is shorter than its fixed fields.
    #[error("batch payload is truncated at {len} bytes")]
    TooShort {
        /// The payload's length in bytes.
        len: usize,
    },

    /// The payload's format version is unknown.
    #[error("unsupported batch payload version {version}")]
    UnsupportedVersion {
        /// The version found.
        version: u8,
    },

    /// The payload's kind byte is unknown.
    #[error("unknown batch payload kind {kind}")]
    UnknownKind {
        /// The kind found.
        kind: u8,
    },

    /// The page size isn't a power of two from 512 to 65536.
    #[error("invalid batch page size {page_size}")]
    InvalidPageSize {
        /// The page size found.
        page_size: u32,
    },

    /// The frame bytes aren't the length the frame count says.
    #[error("batch says {frame_count} frames but holds {len} frame bytes")]
    LengthMismatch {
        /// The frame count in the payload's fixed fields.
        frame_count: u32,
        /// The length of the frame bytes.
        len: usize,
    },

    /// The WAL header that starts the generation is invalid.
    #[error("invalid wal header in batch")]
    Header(#[from] HeaderError),

    /// The batch's page size differs from its WAL header's.
    #[error("batch page size {batch} differs from its wal header's {header}")]
    PageSizeMismatch {
        /// The page size in the payload's fixed fields.
        batch: u32,
        /// The page size in the WAL header.
        header: u32,
    },

    /// A batch that starts a generation doesn't start just after that generation's header.
    #[error("batch start {start} does not match its wal header")]
    HeaderMismatch {
        /// The batch's start position.
        start: Position,
    },

    /// The batch's start isn't on a frame boundary, or its end overflows.
    #[error("batch start {start} is not on a frame boundary")]
    MisalignedStart {
        /// The batch's start position.
        start: Position,
    },

    /// The frames are invalid.
    #[error("invalid frames in batch")]
    Frame(#[from] FrameError),
}
