//! Pinned snapshots: a consistent copy of a frame-source database that a follower reads while the
//! primary keeps writing (ADR-0016, point 8).

use std::fmt;
use std::fs::File;

use crate::engine::{EngineError, Position};

/// A consistent copy of a frame-source database, held in place so that it can be read without
/// blocking writes.
///
/// The engine pins the copy in one quick call on the database's own thread. From then on, the
/// [`files`](Self::files) hold exactly the copy for as long as this value lives, however many
/// writes commit meanwhile; dropping it lets the engine change them again. A reader reads every
/// file's first [`len`](SnapshotFile::len) bytes and then calls [`confirm`](Self::confirm), which
/// says whether what it read is still the copy.
///
/// Together, the files form a database location that
/// [`FrameSourceEngine::open_follower`](crate::engine::FrameSourceEngine::open_follower) opens at
/// [`position`](Self::position).
pub struct PinnedSnapshot {
    position: Option<Position>,
    files: Vec<SnapshotFile>,
    guard: Box<dyn SnapshotGuard>,
}

impl PinnedSnapshot {
    /// A pinned copy at `position` made of `files`, held in place by `guard` until it is dropped.
    #[must_use]
    pub fn new(
        position: Option<Position>,
        files: Vec<SnapshotFile>,
        guard: impl SnapshotGuard + 'static,
    ) -> Self {
        Self {
            position,
            files,
            guard: Box::new(guard),
        }
    }

    /// The log position the copy holds everything up to, or `None` for an empty log.
    #[must_use]
    pub fn position(&self) -> Option<&Position> {
        self.position.as_ref()
    }

    /// The copy's files, in the order a reader reads them.
    #[must_use]
    pub fn files(&self) -> &[SnapshotFile] {
        &self.files
    }

    /// Checks, after every file has been read, that nothing changed them meanwhile.
    ///
    /// # Errors
    ///
    /// [`EngineError::Storage`] if the files changed, so what was read isn't a consistent copy.
    pub fn confirm(&self) -> Result<(), EngineError> {
        self.guard.confirm()
    }
}

impl fmt::Debug for PinnedSnapshot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PinnedSnapshot")
            .field("position", &self.position)
            .field("files", &self.files)
            .finish_non_exhaustive()
    }
}

/// One file of a [`PinnedSnapshot`]: its name inside the database's location, and the open file
/// whose first `len` bytes belong to the copy.
#[derive(Debug)]
pub struct SnapshotFile {
    name: String,
    file: File,
    len: u64,
}

impl SnapshotFile {
    /// The file called `name` in the location, of which the first `len` bytes of `file` belong to
    /// the copy. Bytes past `len` may be written meanwhile and are not part of it.
    #[must_use]
    pub fn new(name: impl Into<String>, file: File, len: u64) -> Self {
        Self {
            name: name.into(),
            file,
            len,
        }
    }

    /// The file's name inside the database's location: a plain name, never a path.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// How many bytes of the file belong to the copy.
    #[must_use]
    pub fn len(&self) -> u64 {
        self.len
    }

    /// Whether the copy holds none of the file's bytes.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// The open file to read from its start.
    #[must_use]
    pub fn file(&self) -> &File {
        &self.file
    }
}

/// What keeps a [`PinnedSnapshot`]'s files in place: the engine releases them when it is dropped.
pub trait SnapshotGuard: Send + Sync {
    /// Checks, after every file has been read, that nothing changed them meanwhile.
    ///
    /// # Errors
    ///
    /// [`EngineError::Storage`] if the files changed.
    fn confirm(&self) -> Result<(), EngineError>;
}
