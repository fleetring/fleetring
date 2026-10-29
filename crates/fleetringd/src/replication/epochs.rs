//! What a replica knows about each database's epochs, kept durably (ADR-0018, points 2 and 5):
//! the highest epoch it has seen and who holds it, the epoch its log came from, and where the
//! log of an epoch it leads began.

use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::{self, Write as _};
use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard};

use fleetring::NodeId;
use fleetring_control::DatabaseName;
use serde::{Deserialize, Serialize};

use crate::engine::Position;
use crate::layout;

/// The file that holds a database's record, inside its directory under the store's root.
const EPOCH_FILE: &str = "epoch";

/// One database's epochs, as a replica keeps them.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct Record {
    /// The database, so that a scan of the store can name it. Absent from records that hold only
    /// an epoch.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    database: Option<String>,
    /// The highest epoch seen: offered by a stream, a fence, or this node's own lease.
    highest: u64,
    /// The node that holds the lease at `highest`, once a fence or a stream named it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    holder: Option<String>,
    /// The epoch of the primary that wrote the last frames of this replica's log, or 0 if no
    /// primary has since this record began.
    #[serde(default)]
    log_epoch: u64,
    /// The epoch this node was promoted to lead, and the position its log had then.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    led: Option<Led>,
}

/// An epoch this node leads after a promotion, and where its log began.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct Led {
    epoch: u64,
    start: Vec<u8>,
}

impl Record {
    /// The epoch of the primary that wrote the last frames of the log, 0 if unknown.
    pub(super) fn log_epoch(&self) -> u64 {
        self.log_epoch
    }

    /// Where the log of `epoch` began, if this node was promoted to lead it.
    pub(super) fn start_of(&self, epoch: u64) -> Option<Position> {
        self.led
            .as_ref()
            .filter(|led| led.epoch == epoch)
            .map(|led| Position::from(led.start.clone()))
    }
}

/// Every database's record, one small file each, cached in memory.
#[derive(Debug)]
pub(super) struct Epochs {
    root: PathBuf,
    cache: Mutex<BTreeMap<DatabaseName, Record>>,
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
    /// Another node holds the lease at this epoch.
    #[error("epoch {epoch} belongs to {holder}, not {offered}")]
    OtherHolder {
        /// The epoch.
        epoch: u64,
        /// The holder recorded for it.
        holder: String,
        /// The node that claimed it.
        offered: String,
    },
    /// The record couldn't be read or written.
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

    fn lock(&self) -> MutexGuard<'_, BTreeMap<DatabaseName, Record>> {
        self.cache.lock().expect("epoch cache lock poisoned")
    }

    fn file(&self, database: &DatabaseName) -> PathBuf {
        self.root
            .join(layout::database_dir(database))
            .join(EPOCH_FILE)
    }

    /// The record of `database` from the cache, reading it into the cache first if needed.
    fn cached<'c>(
        &self,
        cache: &'c mut BTreeMap<DatabaseName, Record>,
        database: &DatabaseName,
    ) -> io::Result<&'c mut Record> {
        if !cache.contains_key(database) {
            let record = read(&self.file(database))?.unwrap_or_default();
            cache.insert(database.clone(), record);
        }
        Ok(cache.entry(database.clone()).or_default())
    }

    /// Changes `database`'s record with `change` and writes it durably, if `change` changed it.
    fn update<T>(
        &self,
        database: &DatabaseName,
        change: impl FnOnce(&mut Record) -> Result<T, EpochError>,
    ) -> Result<T, EpochError> {
        let mut cache = self.lock();
        let record = self.cached(&mut cache, database)?;
        let mut next = record.clone();
        let result = change(&mut next)?;
        if next != *record {
            next.database = Some(database.to_string());
            write(&self.file(database), &next)?;
            *record = next;
        }
        Ok(result)
    }

    /// The highest epoch recorded for `database`, if any.
    pub(super) fn highest(&self, database: &DatabaseName) -> Option<u64> {
        let record = self.record(database);
        (record.highest > 0).then_some(record.highest)
    }

    /// The record of `database`; an empty one if there is none or it can't be read.
    pub(super) fn record(&self, database: &DatabaseName) -> Record {
        let mut cache = self.lock();
        self.cached(&mut cache, database)
            .map(|record| record.clone())
            .unwrap_or_default()
    }

    /// Admits `epoch` for `database`: refuses it if a higher one was seen, and records it durably
    /// before returning if it is the highest yet.
    ///
    /// It blocks on the file system when the epoch is new.
    pub(super) fn admit(&self, database: &DatabaseName, epoch: u64) -> Result<(), EpochError> {
        self.update(database, |record| raise(record, epoch, None))
    }

    /// Admits `epoch` for `database` from `holder`, the node that leads it: as
    /// [`admit`](Self::admit), and also refuses it if another node was recorded as its holder.
    pub(super) fn admit_from(
        &self,
        database: &DatabaseName,
        epoch: u64,
        holder: &NodeId,
    ) -> Result<(), EpochError> {
        self.update(database, |record| raise(record, epoch, Some(holder)))
    }

    /// Records durably that the last frames of `database`'s log came from the primary of
    /// `log_epoch`.
    pub(super) fn set_log_epoch(
        &self,
        database: &DatabaseName,
        log_epoch: u64,
    ) -> Result<(), EpochError> {
        self.update(database, |record| {
            record.log_epoch = log_epoch;
            Ok(())
        })
    }

    /// Records durably that this node, `node`, leads `database` at `epoch`, with its log from
    /// that epoch on, and, after a promotion, that the log began at `start`.
    pub(super) fn lead(
        &self,
        database: &DatabaseName,
        node: &NodeId,
        epoch: u64,
        start: Option<&Position>,
    ) -> Result<(), EpochError> {
        self.update(database, |record| {
            raise(record, epoch, Some(node))?;
            record.log_epoch = epoch;
            if let Some(start) = start {
                record.led = Some(Led {
                    epoch,
                    start: start.as_bytes().to_vec(),
                });
            }
            Ok(())
        })
    }

    /// Deletes `database`'s record, as when the database is dropped, so that a database created
    /// again under the name starts from no epoch here.
    ///
    /// It blocks on the file system.
    pub(super) fn forget(&self, database: &DatabaseName) -> io::Result<()> {
        let mut cache = self.lock();
        cache.remove(database);
        let dir = self.root.join(layout::database_dir(database));
        match fs::remove_dir_all(&dir) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
        if let Some(parent) = dir.parent() {
            // The parent may not exist if the record was never written.
            let _ = sync_dir(parent);
        }
        Ok(())
    }

    /// Every database with a record that names it, in name order.
    ///
    /// It walks the store's directory, so it blocks on the file system.
    pub(super) fn databases(&self) -> Vec<DatabaseName> {
        let mut found = Vec::new();
        let mut pending = vec![self.root.clone()];
        while let Some(dir) = pending.pop() {
            let Ok(entries) = fs::read_dir(&dir) else {
                continue;
            };
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    pending.push(path);
                } else if path.file_name().is_some_and(|name| name == EPOCH_FILE)
                    && let Ok(Some(record)) = read(&path)
                    && let Some(name) = record.database.and_then(|name| name.parse().ok())
                {
                    found.push(name);
                }
            }
        }
        found.sort();
        found.dedup();
        found
    }
}

/// Raises `record` to `epoch` from `holder`, or refuses a lower epoch or another holder.
fn raise(record: &mut Record, epoch: u64, holder: Option<&NodeId>) -> Result<(), EpochError> {
    if epoch < record.highest {
        return Err(EpochError::Stale {
            epoch,
            highest: record.highest,
        });
    }
    if epoch > record.highest {
        record.highest = epoch;
        record.holder = None;
    }
    if let Some(holder) = holder {
        match &record.holder {
            Some(recorded) if recorded != holder.as_str() => {
                return Err(EpochError::OtherHolder {
                    epoch,
                    holder: recorded.clone(),
                    offered: holder.to_string(),
                });
            }
            Some(_) => {}
            None => record.holder = Some(holder.to_string()),
        }
    }
    Ok(())
}

/// Reads the record at `path`: JSON, or a bare epoch as older nodes wrote it.
fn read(path: &Path) -> io::Result<Option<Record>> {
    match fs::read_to_string(path) {
        Ok(text) => {
            let text = text.trim();
            if let Ok(highest) = text.parse() {
                return Ok(Some(Record {
                    highest,
                    ..Record::default()
                }));
            }
            serde_json::from_str(text)
                .map(Some)
                .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error),
    }
}

/// Replaces the file at `path` with `record`, durably: write a temporary file, sync it, rename
/// it over the old one, and sync the directory.
fn write(path: &Path, record: &Record) -> io::Result<()> {
    let dir = path
        .parent()
        .ok_or_else(|| io::Error::other("an epoch file has a parent directory"))?;
    fs::create_dir_all(dir)?;
    let temporary = path.with_extension("tmp");
    let mut file = File::create(&temporary)?;
    let text = serde_json::to_string(record).map_err(io::Error::other)?;
    writeln!(file, "{text}")?;
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

    fn node(text: &str) -> NodeId {
        text.parse().expect("node id")
    }

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

        reopened.forget(&db).expect("forget");
        assert_eq!(reopened.highest(&db), None);
        reopened.forget(&db).expect("forgetting twice is harmless");
        let again = Epochs::new(dir.path().to_owned());
        assert_eq!(again.highest(&db), None);
        again.admit(&db, 1).expect("the name starts over");
    }

    #[test]
    fn an_epoch_has_one_holder() {
        let dir = tempfile::tempdir().expect("temp dir");
        let db: DatabaseName = "db".parse().expect("name");
        let epochs = Epochs::new(dir.path().to_owned());
        epochs
            .admit_from(&db, 2, &node("a"))
            .expect("a fences at 2");
        epochs.admit_from(&db, 2, &node("a")).expect("a again");
        epochs.admit(&db, 2).expect("a batch at 2");
        assert!(matches!(
            epochs.admit_from(&db, 2, &node("b")),
            Err(EpochError::OtherHolder { .. })
        ));
        epochs
            .admit_from(&db, 3, &node("b"))
            .expect("b at a newer epoch");
        let reopened = Epochs::new(dir.path().to_owned());
        assert!(reopened.admit_from(&db, 3, &node("a")).is_err());
        assert!(reopened.admit_from(&db, 3, &node("b")).is_ok());
    }

    #[test]
    fn leading_records_the_log_and_its_start_and_old_records_load() {
        let dir = tempfile::tempdir().expect("temp dir");
        let db: DatabaseName = "tenant/a".parse().expect("name");
        let epochs = Epochs::new(dir.path().to_owned());
        let start = Position::new([1, 2, 3]);
        epochs.lead(&db, &node("a"), 4, Some(&start)).expect("lead");
        epochs.set_log_epoch(&db, 4).expect("log epoch");
        let reopened = Epochs::new(dir.path().to_owned());
        let record = reopened.record(&db);
        assert_eq!(record.log_epoch(), 4);
        assert_eq!(record.start_of(4), Some(start));
        assert_eq!(record.start_of(3), None);
        assert_eq!(reopened.databases(), vec![db.clone()]);

        let old: DatabaseName = "old".parse().expect("name");
        let file = dir.path().join("old").join(EPOCH_FILE);
        fs::create_dir_all(file.parent().expect("parent")).expect("dir");
        fs::write(&file, "7\n").expect("old record");
        assert_eq!(reopened.highest(&old), Some(7));
        assert_eq!(reopened.record(&old).log_epoch(), 0);
    }
}
