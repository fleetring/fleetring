//! A follower's byte-for-byte copy of the primary's WAL.

use std::fs::{self, OpenOptions};
use std::io::{self, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use crate::batch::Batch;
use crate::header::{HeaderError, WalHeader};
use crate::log_end::{AppendError, LogEnd};
use crate::position::{Generation, Position};
use crate::reader::ReadError;

/// A follower's WAL file, kept as an exact copy of a prefix of the primary's (ADR-0016).
///
/// The mirror appends validated batches and fsyncs each one before returning, so a follower may
/// acknowledge a batch as soon as [`append`](Self::append) succeeds. It never opens the database:
/// on promotion SQLite's own recovery replays the mirrored frames up to the last commit.
///
/// Only one mirror may write a given file at a time.
#[derive(Debug)]
pub struct Mirror {
    path: PathBuf,
    end: Option<LogEnd>,
}

impl Mirror {
    /// Opens the mirror at `path`, a follower's `-wal` file, recovering its committed end.
    ///
    /// A missing or header-less file is an empty mirror with no generation. Bytes after the last
    /// commit frame are left in place; the next append or truncate cuts them off.
    ///
    /// # Errors
    ///
    /// Returns [`MirrorError::Io`] if the file exists but can't be read, and
    /// [`MirrorError::Header`] if its WAL header is invalid.
    pub fn open(path: impl Into<PathBuf>) -> Result<Self, MirrorError> {
        let path = path.into();
        let end = match fs::read(&path) {
            Ok(wal) => LogEnd::recover(&wal)?,
            Err(err) if err.kind() == io::ErrorKind::NotFound => None,
            Err(err) => return Err(MirrorError::Io(err)),
        };
        Ok(Self { path, end })
    }

    /// The path of the mirrored WAL file.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// The position just past the last mirrored commit, or `None` before the first generation.
    #[must_use]
    pub fn position(&self) -> Option<Position> {
        self.end.map(|end| end.position())
    }

    /// The WAL header of the current generation, or `None` before the first generation.
    #[must_use]
    pub fn header(&self) -> Option<&WalHeader> {
        self.end.as_ref().map(LogEnd::header)
    }

    /// Starts a new generation from the primary's WAL header, discarding the file's content.
    ///
    /// The follower must have checkpointed its mirror into its database first, because the
    /// frames of the old generation are gone afterwards. The new generation must be newer than
    /// the current one.
    ///
    /// # Errors
    ///
    /// Returns [`MirrorError::StaleGeneration`] if `header` doesn't start a newer generation, and
    /// [`MirrorError::Io`] if writing or syncing the file fails.
    pub fn start_generation(&mut self, header: &WalHeader) -> Result<Position, MirrorError> {
        if let Some(end) = &self.end
            && header.checkpoint_seq() <= end.header().checkpoint_seq()
        {
            return Err(MirrorError::StaleGeneration {
                current: end.generation(),
                offered: header.generation(),
            });
        }
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&self.path)?;
        file.write_all(header.as_bytes())?;
        file.sync_all()?;
        sync_parent(&self.path)?;
        let end = LogEnd::new(*header);
        self.end = Some(end);
        Ok(end.position())
    }

    /// Appends `batch` to the mirror and fsyncs it.
    ///
    /// The batch must continue exactly from [`position`](Self::position) and pass
    /// [`LogEnd::after_batch`]. Anything in the file past the old end is overwritten or cut off.
    /// Returns the new end.
    ///
    /// # Errors
    ///
    /// Returns [`MirrorError::NoGeneration`] before the first generation,
    /// [`MirrorError::Append`] if the batch doesn't continue the log, and [`MirrorError::Io`] if
    /// writing or syncing fails. On error the mirror's position is unchanged.
    pub fn append(&mut self, batch: &Batch) -> Result<Position, MirrorError> {
        let current = self.end.ok_or(MirrorError::NoGeneration)?;
        let next = current.after_batch(batch)?;
        let mut file = OpenOptions::new().write(true).open(&self.path)?;
        file.seek(SeekFrom::Start(current.position().offset()))?;
        file.write_all(batch.frame_bytes())?;
        file.set_len(next.position().offset())?;
        file.sync_data()?;
        self.end = Some(next);
        Ok(next.position())
    }

    /// Cuts the mirror back to `position`, a commit boundary in the current generation, and
    /// fsyncs it.
    ///
    /// A member discards frames past the position a new primary adopted this way before it
    /// accepts frames from the new epoch (ADR-0018, point 5).
    ///
    /// # Errors
    ///
    /// Returns [`MirrorError::NoGeneration`] before the first generation, [`MirrorError::Truncate`]
    /// if `position` isn't a commit boundary of the current generation's committed log, and
    /// [`MirrorError::Io`] if reading, truncating, or syncing fails.
    pub fn truncate(&mut self, position: Position) -> Result<(), MirrorError> {
        if self.end.is_none() {
            return Err(MirrorError::NoGeneration);
        }
        let wal = fs::read(&self.path)?;
        let end = LogEnd::at(&wal, position).map_err(MirrorError::Truncate)?;
        let file = OpenOptions::new().write(true).open(&self.path)?;
        file.set_len(position.offset())?;
        file.sync_data()?;
        self.end = Some(end);
        Ok(())
    }
}

/// Makes a newly created file's directory entry durable.
#[cfg(unix)]
fn sync_parent(path: &Path) -> io::Result<()> {
    use std::fs::File;

    match path.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => File::open(parent)?.sync_all(),
        _ => File::open(".")?.sync_all(),
    }
}

/// Makes a newly created file's directory entry durable. Only Unix can sync a directory.
#[cfg(not(unix))]
fn sync_parent(_path: &Path) -> io::Result<()> {
    Ok(())
}

/// An error from a [`Mirror`] operation.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum MirrorError {
    /// Reading, writing, or syncing the file failed.
    #[error("wal mirror i/o failed")]
    Io(#[from] io::Error),

    /// The existing file's WAL header is invalid.
    #[error("invalid wal header in mirror")]
    Header(#[from] HeaderError),

    /// The mirror has no generation yet, so it can't take frames.
    #[error("mirror has no generation yet")]
    NoGeneration,

    /// The offered header doesn't start a newer generation than the mirror's.
    #[error("generation {offered} is not newer than the mirror's {current}")]
    StaleGeneration {
        /// The mirror's generation.
        current: Generation,
        /// The generation offered.
        offered: Generation,
    },

    /// The batch doesn't continue the mirrored log.
    #[error("batch does not continue the mirror")]
    Append(#[from] AppendError),

    /// The truncation target isn't a commit boundary of the mirrored log.
    #[error("cannot truncate the mirror")]
    Truncate(#[source] ReadError),
}
