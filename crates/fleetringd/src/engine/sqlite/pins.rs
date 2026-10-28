//! Pinned snapshots of SQLite databases, which hold off checkpoints while a follower reads them.

use std::collections::BTreeMap;
use std::fs::File;
use std::io::Read as _;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard};

use fleetring_wal::{WAL_HEADER_LEN, WalHeader};

use super::{storage, wal_path};
use crate::engine::{EngineError, SnapshotGuard};

/// How many snapshots are pinned on each database file, shared by every clone of an engine.
///
/// A checkpoint is the only thing that rewrites a database file, and the only thing after which
/// a write may restart the WAL from its start, so a database refuses to checkpoint while any
/// snapshot of it is pinned.
#[derive(Debug, Default)]
pub(super) struct Pins(Mutex<BTreeMap<PathBuf, usize>>);

impl Pins {
    fn lock(&self) -> MutexGuard<'_, BTreeMap<PathBuf, usize>> {
        self.0.lock().expect("snapshot pins lock poisoned")
    }

    /// Pins the database file at `database`, whose WAL holds the generation that starts with
    /// `header`, if any, until the returned pin is dropped.
    pub(super) fn pin(self: &Arc<Self>, database: &Path, header: Option<WalHeader>) -> Pin {
        *self.lock().entry(database.to_owned()).or_default() += 1;
        Pin {
            pins: Arc::clone(self),
            database: database.to_owned(),
            header,
        }
    }

    /// Whether a snapshot of the database file at `database` is pinned.
    pub(super) fn is_pinned(&self, database: &Path) -> bool {
        self.lock().contains_key(database)
    }
}

/// One pinned snapshot.
#[derive(Debug)]
pub(super) struct Pin {
    pins: Arc<Pins>,
    database: PathBuf,
    /// The header of the WAL generation the snapshot holds a prefix of.
    header: Option<WalHeader>,
}

impl SnapshotGuard for Pin {
    /// A WAL fully checkpointed just before the pin restarts at the next commit, overwriting its
    /// prefix from the header on. If the header is unchanged once the prefix has been read, the
    /// restart hadn't started, so the prefix read is intact.
    fn confirm(&self) -> Result<(), EngineError> {
        let Some(header) = &self.header else {
            return Ok(());
        };
        let mut current = [0; WAL_HEADER_LEN];
        File::open(wal_path(&self.database))
            .and_then(|mut wal| wal.read_exact(&mut current))
            .map_err(storage)?;
        if current != *header.as_bytes() {
            return Err(storage(std::io::Error::other(
                "the log restarted while the snapshot was read",
            )));
        }
        Ok(())
    }
}

impl Drop for Pin {
    fn drop(&mut self) {
        // A poisoned lock means a panic elsewhere; a destructor mustn't add another.
        let Ok(mut pins) = self.pins.0.lock() else {
            return;
        };
        if let Some(count) = pins.get_mut(&self.database) {
            *count -= 1;
            if *count == 0 {
                pins.remove(&self.database);
            }
        }
    }
}
