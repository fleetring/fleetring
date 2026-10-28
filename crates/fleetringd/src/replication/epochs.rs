//! The highest epoch a follower has seen for each database, kept durably (ADR-0018, point 2).

use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::{self, Write as _};
use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard};

use fleetring_control::DatabaseName;

use crate::layout;

/// The file that holds a database's highest epoch, inside its directory under the store's root.
const EPOCH_FILE: &str = "epoch";

/// Every database's highest epoch, one small file each, cached in memory.
#[derive(Debug)]
pub(super) struct Epochs {
    root: PathBuf,
    cache: Mutex<BTreeMap<DatabaseName, u64>>,
}

/// The error returned when an epoch is refused or can't be recorded.
#[derive(Debug, thiserror::Error)]
pub(super) enum EpochError {
    /// The epoch is lower than one already seen.
    #[error("epoch {epoch} is older than epoch {highest}, which this replica has already seen")]
    Stale {
        /// The epoch offered.
        epoch: u64,
        /// The highest epoch recorded.
        highest: u64,
    },
    /// The epoch couldn't be read or written.
    #[error("can't record the epoch")]
    Io(#[from] io::Error),
}

impl Epochs {
    pub(super) fn new(root: PathBuf) -> Self {
        Self {
            root,
            cache: Mutex::default(),
        }
    }

    fn lock(&self) -> MutexGuard<'_, BTreeMap<DatabaseName, u64>> {
        self.cache.lock().expect("epoch cache lock poisoned")
    }

    fn file(&self, database: &DatabaseName) -> PathBuf {
        self.root
            .join(layout::database_dir(database))
            .join(EPOCH_FILE)
    }

    /// The highest epoch recorded for `database`, if any.
    pub(super) fn highest(&self, database: &DatabaseName) -> Option<u64> {
        if let Some(epoch) = self.lock().get(database) {
            return Some(*epoch);
        }
        let epoch = read(&self.file(database)).ok().flatten()?;
        self.lock().entry(database.clone()).or_insert(epoch);
        Some(epoch)
    }

    /// Admits `epoch` for `database`: refuses it if a higher one was seen, and records it durably
    /// before returning if it is the highest yet.
    ///
    /// It blocks on the file system when the epoch is new.
    pub(super) fn admit(&self, database: &DatabaseName, epoch: u64) -> Result<(), EpochError> {
        let mut cache = self.lock();
        let highest = match cache.get(database) {
            Some(highest) => Some(*highest),
            None => read(&self.file(database))?,
        };
        match highest {
            Some(highest) if epoch < highest => Err(EpochError::Stale { epoch, highest }),
            Some(highest) if epoch == highest => {
                cache.insert(database.clone(), highest);
                Ok(())
            }
            _ => {
                write(&self.file(database), epoch)?;
                cache.insert(database.clone(), epoch);
                Ok(())
            }
        }
    }
}

fn read(path: &Path) -> io::Result<Option<u64>> {
    match fs::read_to_string(path) {
        Ok(text) => text
            .trim()
            .parse()
            .map(Some)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error)),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error),
    }
}

/// Replaces the file at `path` with `epoch`, durably: write a temporary file, sync it, rename it
/// over the old one, and sync the directory.
fn write(path: &Path, epoch: u64) -> io::Result<()> {
    let dir = path
        .parent()
        .ok_or_else(|| io::Error::other("an epoch file has a parent directory"))?;
    fs::create_dir_all(dir)?;
    let temporary = path.with_extension("tmp");
    let mut file = File::create(&temporary)?;
    writeln!(file, "{epoch}")?;
    file.sync_all()?;
    fs::rename(&temporary, path)?;
    sync_dir(dir)
}

/// Makes new directory entries in `dir` durable. Only Unix can sync a directory.
pub(super) fn sync_dir(dir: &Path) -> io::Result<()> {
    if cfg!(unix) {
        File::open(dir)?.sync_all()?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn epochs_only_grow_and_survive_a_restart() {
        let dir = tempfile::tempdir().expect("temp dir");
        let db: DatabaseName = "db".parse().expect("name");
        let epochs = Epochs::new(dir.path().to_owned());
        assert_eq!(epochs.highest(&db), None);
        epochs.admit(&db, 3).expect("first epoch");
        epochs.admit(&db, 3).expect("same epoch");
        assert!(matches!(
            epochs.admit(&db, 2),
            Err(EpochError::Stale {
                epoch: 2,
                highest: 3
            })
        ));
        epochs.admit(&db, 5).expect("newer epoch");

        let reopened = Epochs::new(dir.path().to_owned());
        assert_eq!(reopened.highest(&db), Some(5));
        assert!(reopened.admit(&db, 4).is_err());
    }
}
