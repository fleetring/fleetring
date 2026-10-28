//! The data directory, locked so that only one daemon uses it (ADR-0016).

use std::fs::{self, File, TryLockError};
use std::io;
use std::path::{Path, PathBuf};

const LOCK_FILE: &str = "fleetringd.lock";

/// A data directory that this process holds an exclusive lock on.
///
/// The lock is released when the value is dropped, or when the process exits for any reason, so a
/// crashed daemon never leaves a stale lock behind.
#[derive(Debug)]
pub struct DataDir {
    path: PathBuf,
    _lock: File,
}

/// The error returned when a data directory can't be locked.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum DataDirError {
    /// Another process, usually another daemon, holds the directory.
    #[error("data directory {path} is in use by another fleetringd")]
    InUse {
        /// The directory.
        path: PathBuf,
    },
    /// The directory or its lock file couldn't be created or opened.
    #[error("can't use data directory {path}")]
    Io {
        /// The directory.
        path: PathBuf,
        /// The underlying error.
        source: io::Error,
    },
}

impl DataDir {
    /// Creates the directory if needed and takes its exclusive lock, without waiting.
    ///
    /// # Errors
    ///
    /// [`DataDirError::InUse`] if another process holds the lock, or [`DataDirError::Io`] if the
    /// directory or lock file can't be created.
    pub fn lock(path: impl Into<PathBuf>) -> Result<Self, DataDirError> {
        let path = path.into();
        let io_error = |source| DataDirError::Io {
            path: path.clone(),
            source,
        };
        fs::create_dir_all(&path).map_err(io_error)?;
        let lock = File::options()
            .create(true)
            .truncate(false)
            .write(true)
            .open(path.join(LOCK_FILE))
            .map_err(io_error)?;
        match lock.try_lock() {
            Ok(()) => Ok(Self { path, _lock: lock }),
            Err(TryLockError::WouldBlock) => Err(DataDirError::InUse { path }),
            Err(TryLockError::Error(source)) => Err(io_error(source)),
        }
    }

    /// The directory's path.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }
}
