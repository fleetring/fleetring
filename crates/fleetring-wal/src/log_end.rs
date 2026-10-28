//! The committed end of a follower's WAL, and the rules for extending it.

use crate::batch::Batch;
use crate::checksum::Checksum;
use crate::frame::FrameError;
use crate::header::{HeaderError, WAL_HEADER_LEN, WalHeader};
use crate::position::{Generation, Position};
use crate::reader::{self, ReadError};

/// The committed end of a WAL: its header, the position after its last commit frame, and the
/// checksum chain up to there.
///
/// This is the state a follower needs to accept the next batch. It does no I/O; [`Mirror`]
/// pairs it with a file.
///
/// [`Mirror`]: crate::Mirror
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LogEnd {
    header: WalHeader,
    offset: u64,
    checksum: Checksum,
}

impl LogEnd {
    /// The end of a generation that holds only its header.
    #[must_use]
    pub fn new(header: WalHeader) -> Self {
        Self {
            offset: header.start().offset(),
            checksum: header.checksum(),
            header,
        }
    }

    /// Finds the committed end of `wal`, the whole content of a WAL file.
    ///
    /// Frames after the last valid commit frame are ignored, as SQLite's recovery ignores them.
    /// Returns `Ok(None)` if `wal` is shorter than a header.
    ///
    /// # Errors
    ///
    /// Returns a [`HeaderError`] if the WAL header is invalid.
    pub fn recover(wal: &[u8]) -> Result<Option<Self>, HeaderError> {
        if wal.len() < WAL_HEADER_LEN {
            return Ok(None);
        }
        let header = WalHeader::parse(wal)?;
        let span = reader::locate(wal, &header, header.start())
            .expect("a generation's start is always a commit boundary");
        Ok(Some(Self::at_offset(header, span.end, span.end_checksum)))
    }

    /// Finds the end of `wal` cut back to `position`, which must be a commit boundary in its
    /// committed log.
    ///
    /// # Errors
    ///
    /// Returns a [`ReadError`] if the WAL header is invalid, or if `position` is in another
    /// generation, past the last commit, or not on a commit boundary.
    pub fn at(wal: &[u8], position: Position) -> Result<Self, ReadError> {
        let header = WalHeader::parse(wal)?;
        if position.generation() != header.generation() {
            return Err(ReadError::UnknownGeneration {
                position,
                current: header.generation(),
            });
        }
        let span = reader::locate(wal, &header, position)?;
        Ok(Self::at_offset(header, span.from, span.from_checksum))
    }

    /// The WAL header of this generation.
    #[must_use]
    pub const fn header(&self) -> &WalHeader {
        &self.header
    }

    /// The generation the log is in.
    #[must_use]
    pub fn generation(&self) -> Generation {
        self.header.generation()
    }

    /// The position just past the last commit frame.
    #[must_use]
    pub fn position(&self) -> Position {
        Position::new(self.header.generation(), self.offset)
    }

    /// Validates `batch` as the next part of this log and returns the end after it.
    ///
    /// The batch must start exactly at [`position`](Self::position), have this log's page size,
    /// carry this generation's header if it carries one, and continue the checksum chain.
    ///
    /// # Errors
    ///
    /// Returns an [`AppendError`] if any of those checks fail.
    pub fn after_batch(&self, batch: &Batch) -> Result<Self, AppendError> {
        let expected = self.position();
        if batch.start() != expected {
            return Err(AppendError::Discontinuous {
                expected,
                found: batch.start(),
            });
        }
        if batch.header().is_some_and(|header| *header != self.header) {
            return Err(AppendError::HeaderMismatch);
        }
        if batch.page_size() != self.header.page_size() {
            return Err(AppendError::PageSizeMismatch {
                expected: self.header.page_size(),
                found: batch.page_size(),
            });
        }
        let checksum = batch.check_chain(self.header.checksum_order(), self.checksum)?;
        Ok(Self {
            header: self.header,
            offset: batch.end().offset(),
            checksum,
        })
    }

    fn at_offset(header: WalHeader, offset: usize, checksum: Checksum) -> Self {
        let offset = u64::try_from(offset).expect("a wal held in memory fits in u64");
        Self {
            header,
            offset,
            checksum,
        }
    }
}

/// An error from appending a batch to a log.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum AppendError {
    /// The batch doesn't start where the log ends.
    #[error("batch starts at {found} but the log ends at {expected}")]
    Discontinuous {
        /// The log's end.
        expected: Position,
        /// The batch's start.
        found: Position,
    },

    /// The batch carries a WAL header that differs from the log's.
    #[error("batch wal header differs from the log's")]
    HeaderMismatch,

    /// The batch's page size differs from the log's.
    #[error("batch page size {found} differs from the log's {expected}")]
    PageSizeMismatch {
        /// The log's page size.
        expected: u32,
        /// The batch's page size.
        found: u32,
    },

    /// The batch's frames don't continue the log's checksum chain.
    #[error("invalid frames in batch")]
    Frame(#[from] FrameError),
}
