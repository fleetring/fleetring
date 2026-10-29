//! Where a sink's delivered position for one database is kept, durably.

use std::ffi::OsString;
use std::fs::{self, File};
use std::io::{self, Write as _};
use std::path::{Path, PathBuf};

use fleetring_control::DatabaseName;

use crate::engine::Position;
use crate::layout;

/// The file `<root>/<sink>/<database>` that holds the last position a sink delivered.
///
/// The file holds the position's bytes, and is written whole and renamed into place, so a crash
/// leaves either the old position or the new one. No file means nothing delivered yet.
#[derive(Debug, Clone)]
pub(super) struct PositionStore {
    path: PathBuf,
}

impl PositionStore {
    pub(super) fn new(root: &Path, sink: &str, database: &DatabaseName) -> Self {
        Self {
            path: root.join(sink).join(layout::database_dir(database)),
        }
    }

    /// The file's path.
    pub(super) fn path(&self) -> &Path {
        &self.path
    }

    /// Reads the stored position, or `None` if nothing was delivered yet.
    pub(super) fn load(&self) -> io::Result<Option<Position>> {
        match fs::read(&self.path) {
            Ok(bytes) if bytes.is_empty() => Ok(None),
            Ok(bytes) => Ok(Some(Position::from(bytes))),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error),
        }
    }

    /// Records `position` durably.
    pub(super) fn save(&self, position: &Position) -> io::Result<()> {
        let dir = self
            .path
            .parent()
            .ok_or_else(|| io::Error::other("a position file has a parent directory"))?;
        fs::create_dir_all(dir)?;
        let mut staged = OsString::from(self.path.as_os_str());
        staged.push(".tmp");
        let staged = PathBuf::from(staged);
        let mut file = File::create(&staged)?;
        file.write_all(position.as_bytes())?;
        file.sync_all()?;
        drop(file);
        fs::rename(&staged, &self.path)?;
        if cfg!(unix) {
            File::open(dir)?.sync_all()?;
        }
        Ok(())
    }
}
