//! Reading committed transactions out of a WAL.

use std::path::Path;
use std::{fs, io};

use crate::batch::Batch;
use crate::checksum::Checksum;
use crate::frame::Frames;
use crate::header::{HeaderError, WAL_HEADER_LEN, WalHeader};
use crate::position::{Generation, Position};

/// Returns every complete transaction in `wal` after `after`, as one batch.
///
/// `wal` is the whole content of a WAL file. `after` is the position the reader already has, or
/// `None` to read from the start of whatever generation `wal` holds. The batch runs through the
/// last commit frame that SQLite's recovery would accept: reading stops at the first frame that is
/// cut short, carries salts from another generation, or breaks the checksum chain, and frames
/// after the last commit frame before that point are left out. A batch therefore never holds
/// part of a transaction.
///
/// When `after` is in an older generation than `wal`'s, the WAL has restarted since, and the
/// batch starts the new generation: it carries the header and every committed frame. The caller
/// must already hold all of the older generation, as ADR-0016 guarantees by checkpointing only
/// after every follower has acknowledged it.
///
/// Returns `Ok(None)` when there is nothing new: `wal` is shorter than a header, or holds no
/// committed frame after `after`.
///
/// # Errors
///
/// Returns a [`ReadError`] if the WAL header is invalid, or if `after` is in a newer or unrelated
/// generation, past the last commit, or not on a commit boundary.
pub fn read_committed(wal: &[u8], after: Option<Position>) -> Result<Option<Batch>, ReadError> {
    if wal.len() < WAL_HEADER_LEN {
        return Ok(None);
    }
    let header = WalHeader::parse(wal)?;
    let generation = header.generation();
    let (from, carries_header) = match after {
        None => (header.start(), true),
        Some(position) if position.generation() == generation => (position, false),
        Some(position) if position.generation().checkpoint_seq() < generation.checkpoint_seq() => {
            (header.start(), true)
        }
        Some(position) => {
            return Err(ReadError::UnknownGeneration {
                position,
                current: generation,
            });
        }
    };
    let span = locate(wal, &header, from)?;
    if span.end == span.from {
        return Ok(None);
    }
    let frames = wal[span.from..span.end].to_vec();
    Ok(Some(Batch::from_valid_frames(
        carries_header.then_some(header),
        from,
        header.page_size(),
        frames,
    )))
}

/// Reads the WAL file at `path` and returns every complete transaction after `after`.
///
/// This is [`read_committed`] over the file's content. A missing file holds nothing yet, so it
/// gives `Ok(None)`.
///
/// # Errors
///
/// Returns [`ReadFileError::Io`] if the file can't be read, and [`ReadFileError::Read`] for the
/// errors [`read_committed`] returns.
pub fn read_committed_file(
    path: impl AsRef<Path>,
    after: Option<Position>,
) -> Result<Option<Batch>, ReadFileError> {
    let wal = match fs::read(path) {
        Ok(wal) => wal,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(ReadFileError::Io(err)),
    };
    Ok(read_committed(&wal, after)?)
}

/// The committed span of a WAL from a commit boundary to the last commit frame.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Span {
    /// The offset the span starts at, a commit boundary.
    pub(crate) from: usize,
    /// The offset just past the last valid commit frame.
    pub(crate) end: usize,
    /// The cumulative checksum at `from`.
    pub(crate) from_checksum: Checksum,
    /// The cumulative checksum at `end`.
    pub(crate) end_checksum: Checksum,
}

/// Finds the committed span of `wal` from `from`, which must be in `header`'s generation.
pub(crate) fn locate(wal: &[u8], header: &WalHeader, from: Position) -> Result<Span, ReadError> {
    debug_assert_eq!(
        from.generation(),
        header.generation(),
        "position {from} is in another generation"
    );
    let start = WAL_HEADER_LEN;
    let mut end = (start, header.checksum());
    let target = usize::try_from(from.offset()).ok();
    let mut found = (target == Some(start)).then_some(header.checksum());
    for frame in Frames::new(wal, header).filter(|frame| frame.is_commit) {
        end = (frame.end, frame.checksum);
        if Some(frame.end) == target {
            found = Some(frame.checksum);
        }
    }
    let (end, end_checksum) = end;
    match (found, target) {
        (Some(from_checksum), Some(from)) => Ok(Span {
            from,
            end,
            from_checksum,
            end_checksum,
        }),
        (_, Some(target)) if target <= end => Err(ReadError::NotCommitBoundary { position: from }),
        _ => Err(ReadError::PastEnd {
            position: from,
            end: Position::new(header.generation(), u64::try_from(end).unwrap_or(u64::MAX)),
        }),
    }
}

/// An error from reading committed transactions out of a WAL.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum ReadError {
    /// The WAL header is invalid.
    #[error("invalid wal header")]
    Header(#[from] HeaderError),

    /// The position is in a generation that the WAL doesn't hold and that isn't older than it.
    #[error("position {position} is not in wal generation {current} or an older one")]
    UnknownGeneration {
        /// The position asked for.
        position: Position,
        /// The WAL's generation.
        current: Generation,
    },

    /// The position is past the WAL's last commit.
    #[error("position {position} is past the last commit at {end}")]
    PastEnd {
        /// The position asked for.
        position: Position,
        /// The position just past the WAL's last commit frame.
        end: Position,
    },

    /// The position is inside the committed log but not just after a commit frame.
    #[error("position {position} is not on a commit boundary")]
    NotCommitBoundary {
        /// The position asked for.
        position: Position,
    },
}

/// An error from reading committed transactions out of a WAL file.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ReadFileError {
    /// The file couldn't be read.
    #[error("failed to read the wal file")]
    Io(#[from] io::Error),

    /// The file's content is invalid, or the position doesn't fit it.
    #[error("failed to read committed frames")]
    Read(#[from] ReadError),
}
