//! A SQLite follower: a byte-for-byte mirror of the primary's WAL (ADR-0016, point 6).

use std::path::{Path, PathBuf};

use fleetring_wal::{Batch, Mirror, MirrorError};
use rusqlite::{Connection, OpenFlags};

use super::{
    Checkpoint, DATABASE_FILE, checkpoint, encode_position, keep_wal_on_close, remove_if_present,
    shm_path, storage, wal_path,
};
use crate::engine::{CheckpointMarker, EngineError, FrameBatch, FrameFollower, Position};

/// A follower's copy of a SQLite database, which mirrors the primary's WAL with
/// [`fleetring_wal::Mirror`].
///
/// The follower never keeps the database open. It starts from a primary's
/// [`snapshot`](crate::engine::Database::snapshot), appends each batch to its own `-wal` file and
/// fsyncs it, and opens the database only briefly to checkpoint. On promotion, SQLite's WAL
/// recovery replays the mirrored frames up to the last commit.
#[derive(Debug)]
pub struct SqliteFollower {
    database: PathBuf,
    mirror: Mirror,
}

impl SqliteFollower {
    /// Opens the follower copy at `location`.
    ///
    /// # Errors
    ///
    /// [`EngineError::NotFound`] if `location` holds no database file, and
    /// [`EngineError::Storage`] if the mirrored WAL can't be read.
    pub fn open(location: &Path) -> Result<Self, EngineError> {
        let database = location.join(DATABASE_FILE);
        if !database.try_exists().map_err(storage)? {
            return Err(EngineError::NotFound);
        }
        let mirror = Mirror::open(wal_path(&database)).map_err(mirror_error)?;
        Ok(Self { database, mirror })
    }

    /// Copies the mirrored frames into the database file.
    fn checkpoint_database(&self, mode: Checkpoint) -> Result<(), EngineError> {
        // A shared-memory index left from an earlier open may describe an older WAL.
        remove_if_present(&shm_path(&self.database)).map_err(storage)?;
        let conn = Connection::open_with_flags(
            &self.database,
            OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )
        .map_err(storage)?;
        if mode == Checkpoint::Passive {
            keep_wal_on_close(&conn)?;
        }
        checkpoint(&conn, mode)?;
        conn.close().map_err(|(_, error)| storage(error))
    }
}

impl FrameFollower for SqliteFollower {
    fn position(&self) -> Option<Position> {
        self.mirror.position().map(encode_position)
    }

    fn apply(&mut self, batch: &FrameBatch) -> Result<Position, EngineError> {
        let decoded = Batch::from_bytes(batch.payload.as_bytes())
            .map_err(|error| EngineError::InvalidPayload(error.into()))?;
        if let Some(header) = decoded.header()
            && self.mirror.header() != Some(header)
        {
            if self.mirror.header().is_some() {
                // The old generation's frames are gone once the new one starts.
                self.checkpoint_database(Checkpoint::Truncate)?;
            }
            self.mirror.start_generation(header).map_err(mirror_error)?;
        }
        let end = self.mirror.append(&decoded).map_err(mirror_error)?;
        Ok(encode_position(end))
    }

    fn checkpoint(&mut self, marker: &CheckpointMarker) -> Result<(), EngineError> {
        if self.position().as_ref() != Some(&marker.position) {
            return Err(EngineError::InvalidPosition {
                position: marker.position.clone(),
            });
        }
        // PASSIVE leaves the mirrored WAL in place, so the mirror stays valid until the
        // primary's next generation replaces it.
        self.checkpoint_database(Checkpoint::Passive)
    }
}

fn mirror_error(error: MirrorError) -> EngineError {
    match error {
        MirrorError::Io(_) | MirrorError::Header(_) => storage(error),
        other => EngineError::InvalidPayload(other.into()),
    }
}
