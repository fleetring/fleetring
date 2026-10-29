//! Reading committed transactions out of a WAL.

use std::fs::{self, File};
use std::io::{self, Read, Seek, SeekFrom};
use std::path::Path;

use crate::batch::Batch;
use crate::checksum::Checksum;
use crate::frame::{FRAME_HEADER_LEN, FrameHeader, Frames};
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
/// This gives what [`read_committed`] gives over the file's content, but reads only the file's
/// tail when `after` is a commit boundary of the file's current generation: the header, the
/// previous frame's header, and the frames from `after` on. The frames before `after` were
/// validated when they were read; the previous frame's salts and cumulative checksum link the
/// tail to them. Any position that doesn't fit that way is settled by reading the whole file.
/// A missing file holds nothing yet, so it gives `Ok(None)`.
///
/// # Errors
///
/// Returns [`ReadFileError::Io`] if the file can't be read, and [`ReadFileError::Read`] for the
/// errors [`read_committed`] returns.
pub fn read_committed_file(
    path: impl AsRef<Path>,
    after: Option<Position>,
) -> Result<Option<Batch>, ReadFileError> {
    let path = path.as_ref();
    if let Some(position) = after
        && let Some(batch) = read_tail(path, position)?
    {
        return Ok(batch);
    }
    let wal = match fs::read(path) {
        Ok(wal) => wal,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(ReadFileError::Io(err)),
    };
    Ok(read_committed(&wal, after)?)
}

/// Reads the frames after `after` alone, if `after` is frame-aligned in the file's current
/// generation and follows a commit frame of it. `None` means the file has to be read whole to
/// decide; `Some(None)` that there is nothing new.
fn read_tail(path: &Path, after: Position) -> Result<Option<Option<Batch>>, ReadFileError> {
    let mut file = match File::open(path) {
        Ok(file) => file,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(Some(None)),
        Err(err) => return Err(ReadFileError::Io(err)),
    };
    let mut head = [0; WAL_HEADER_LEN];
    match file.read_exact(&mut head) {
        Ok(()) => {}
        Err(err) if err.kind() == io::ErrorKind::UnexpectedEof => return Ok(Some(None)),
        Err(err) => return Err(ReadFileError::Io(err)),
    }
    let Ok(header) = WalHeader::parse(&head) else {
        return Ok(None);
    };
    if after.generation() != header.generation() {
        return Ok(None);
    }
    let frame_len = header.frame_len();
    let Some(frames_before) = usize::try_from(after.offset())
        .ok()
        .and_then(|offset| offset.checked_sub(WAL_HEADER_LEN))
    else {
        return Ok(None);
    };
    if !frames_before.is_multiple_of(frame_len) {
        return Ok(None);
    }
    let checksum = if frames_before == 0 {
        header.checksum()
    } else {
        let previous = after.offset() - frame_len as u64;
        file.seek(SeekFrom::Start(previous))
            .map_err(ReadFileError::Io)?;
        let mut bytes = [0; FRAME_HEADER_LEN];
        match file.read_exact(&mut bytes) {
            Ok(()) => {}
            Err(err) if err.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(err) => return Err(ReadFileError::Io(err)),
        }
        let previous = FrameHeader::from_bytes(&bytes);
        if previous.salts() != header.salts() || !previous.is_commit() {
            return Ok(None);
        }
        Checksum(previous.checksum())
    };
    file.seek(SeekFrom::Start(after.offset()))
        .map_err(ReadFileError::Io)?;
    let mut tail = Vec::new();
    file.read_to_end(&mut tail).map_err(ReadFileError::Io)?;
    let end = Frames::resume(&tail, &header, checksum)
        .filter(|frame| frame.is_commit)
        .last()
        .map_or(0, |frame| frame.end);
    if end == 0 {
        return Ok(Some(None));
    }
    tail.truncate(end);
    Ok(Some(Some(Batch::from_valid_frames(
        None,
        after,
        header.page_size(),
        tail,
    ))))
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
