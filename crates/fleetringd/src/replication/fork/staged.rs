//! Staged copies on a data node's disk (ADR-0046, point 4.3): each one a directory named by its
//! fork identity and attempt, holding the copy's files and a sidecar that describes them.
//!
//! ```text
//! <data_dir>/staged/<fork, as the databases root encodes it>/<incarnation>-<operation>/<attempt>/
//!     copy/            the copy's files, as the source's primary pinned them
//!     sidecar.json     the identity, the attempt, the position, the chain hash, the stamped
//!                      source incarnation, the stream's size and checksum, the files in stream
//!                      order, and log epoch 0
//!     installed        present once the copy was moved into the database's place
//! ```
//!
//! A copy is staged in three durable steps: its files go into `copy.tmp/` and the sidecar into
//! `sidecar.json.tmp`, each synced; `copy.tmp/` is renamed to `copy/`; then `sidecar.json.tmp`
//! to `sidecar.json`, and the directory is synced. The sidecar's rename is the commit point: a
//! directory without `sidecar.json` holds no staged copy and was never reported, so the scan at
//! start removes it, with every `.tmp` entry. A sidecar whose `copy/` is gone, without
//! `installed`, describes a lost copy, which the node reports as such and never opens.
//!
//! The store deletes a staged directory only when told to, on a positive signal for its own
//! identity (ADR-0046, point 4.6, and ADR-0042), or at an operator's request for an orphan.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File};
use std::io::{self, Read as _};
use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard};

use fleetring_control::fork::{CopyLossReason, ForkIdentity, OperationId};
use fleetring_control::{DatabaseName, Incarnation};
use serde::{Deserialize, Serialize};
use twox_hash::XxHash3_128;

use super::super::archive::{CHECKSUM_LEN, remove_dir_if_present};
use super::super::epochs::sync_dir;
use crate::engine::Position;
use crate::layout;
use crate::replication::ChainHash;

/// The directory of a staged copy's files.
const COPY: &str = "copy";
/// The sidecar that describes a staged copy; its rename commits the copy.
const SIDECAR: &str = "sidecar.json";
/// Present once the copy was moved into the database's place.
const INSTALLED: &str = "installed";
/// The suffix of everything not committed yet.
const TEMPORARY: &str = ".tmp";

/// A staged copy's name: the fork identity and the attempt that fetched it.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct StagedKey {
    /// The fork identity.
    pub(crate) fork: ForkIdentity,
    /// The attempt.
    pub(crate) attempt: u32,
}

impl StagedKey {
    /// The directory, under the store's root, of the staged copy this key names.
    fn dir(&self, root: &Path) -> PathBuf {
        identity_dir(root, &self.fork).join(self.attempt.to_string())
    }
}

/// The directory, under the store's root, of every staged copy of `fork`.
fn identity_dir(root: &Path, fork: &ForkIdentity) -> PathBuf {
    root.join(layout::database_dir(&fork.name))
        .join(format!("{}-{}", fork.incarnation, fork.operation))
}

/// One file of a staged copy, in stream order.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct StagedFile {
    pub(crate) name: String,
    pub(crate) len: u64,
}

/// What a sidecar records about a staged copy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Sidecar {
    /// The copy's name.
    pub(crate) key: StagedKey,
    /// The source position the copy holds.
    pub(crate) position: Position,
    /// The source's chain hash there.
    pub(crate) chain_hash: ChainHash,
    /// The source incarnation the source's primary stamped on the stream.
    pub(crate) source_incarnation: Incarnation,
    /// The stream's size in bytes.
    pub(crate) size: u64,
    /// The stream's XXH3-128 checksum.
    pub(crate) checksum: [u8; CHECKSUM_LEN],
    /// The copy's files, in stream order.
    pub(crate) files: Vec<StagedFile>,
}

/// The sidecar's file format: JSON, with bytes as lowercase hex.
#[derive(Debug, Serialize, Deserialize)]
struct SidecarFile {
    version: u32,
    database: String,
    fork_incarnation: String,
    operation: String,
    attempt: u32,
    position: String,
    chain_hash: String,
    source_incarnation: String,
    size: u64,
    checksum: String,
    files: Vec<StagedFile>,
    /// Always 0: promotion ranks a staged copy at `(0, position)` (ADR-0046, point 4.3).
    log_epoch: u64,
}

impl Sidecar {
    fn to_file(&self) -> SidecarFile {
        SidecarFile {
            version: 1,
            database: self.key.fork.name.to_string(),
            fork_incarnation: self.key.fork.incarnation.to_string(),
            operation: self.key.fork.operation.to_string(),
            attempt: self.key.attempt,
            position: super::hex(self.position.as_bytes()),
            chain_hash: self.chain_hash.to_string(),
            source_incarnation: self.source_incarnation.to_string(),
            size: self.size,
            checksum: super::hex(&self.checksum),
            files: self.files.clone(),
            log_epoch: 0,
        }
    }

    fn from_file(file: SidecarFile) -> Option<Self> {
        let incarnation = |text: &str| u64::from_str_radix(text, 16).ok().map(Incarnation::new);
        if file.version != 1 || file.log_epoch != 0 {
            return None;
        }
        let name: DatabaseName = file.database.parse().ok()?;
        let operation: OperationId = file.operation.parse().ok()?;
        Some(Self {
            key: StagedKey {
                fork: ForkIdentity::new(name, incarnation(&file.fork_incarnation)?, operation),
                attempt: file.attempt,
            },
            position: Position::from(super::unhex(&file.position)?),
            chain_hash: file.chain_hash.parse().ok()?,
            source_incarnation: incarnation(&file.source_incarnation)?,
            size: file.size,
            checksum: super::unhex(&file.checksum)?.try_into().ok()?,
            files: file.files,
        })
    }
}

/// Where a staged copy stands on disk.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Held {
    /// Its files are in place, as staged.
    Present,
    /// Its files were moved into the database's place.
    Installed,
    /// Its files are gone, and were never installed.
    Lost,
}

/// A staged copy the store knows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Staged {
    pub(crate) sidecar: Sidecar,
    pub(crate) held: Held,
}

/// The staged copies on one node's disk, indexed by their sidecars.
#[derive(Debug)]
pub(crate) struct StagedStore {
    root: PathBuf,
    entries: Mutex<BTreeMap<StagedKey, Staged>>,
    /// The staged copies whose identity this node can't confirm (ADR-0046, point 4.6).
    orphans: Mutex<BTreeSet<StagedKey>>,
}

impl StagedStore {
    /// The store under `root`, empty until [`scan`](Self::scan) reads the disk.
    pub(crate) fn new(root: PathBuf) -> Self {
        Self {
            root,
            entries: Mutex::default(),
            orphans: Mutex::default(),
        }
    }

    /// The directory staged copies live under.
    pub(crate) fn root(&self) -> &Path {
        &self.root
    }

    fn lock(&self) -> MutexGuard<'_, BTreeMap<StagedKey, Staged>> {
        self.entries.lock().expect("staged copies lock poisoned")
    }

    fn lock_orphans(&self) -> MutexGuard<'_, BTreeSet<StagedKey>> {
        self.orphans.lock().expect("staged orphans lock poisoned")
    }

    /// Reads the disk at start: removes what a crash left uncommitted, and indexes every staged
    /// copy by its sidecar. It blocks on the file system.
    pub(crate) fn scan(&self) {
        let mut found = BTreeMap::new();
        let mut pending = vec![self.root.clone()];
        while let Some(dir) = pending.pop() {
            let Ok(entries) = fs::read_dir(&dir) else {
                continue;
            };
            let mut sidecar = false;
            let mut subdirs = Vec::new();
            for entry in entries.flatten() {
                let path = entry.path();
                let name = entry.file_name();
                let name = name.to_string_lossy();
                if name.ends_with(TEMPORARY) {
                    let removed = if path.is_dir() {
                        remove_dir_if_present(&path)
                    } else {
                        fs::remove_file(&path)
                    };
                    if let Err(error) = removed {
                        tracing::warn!(path = %path.display(), %error, "can't remove a staging leftover");
                    }
                } else if name == SIDECAR {
                    sidecar = true;
                } else if path.is_dir() && name != COPY {
                    subdirs.push(path);
                }
            }
            if sidecar {
                match read_sidecar(&dir) {
                    Some(staged) => {
                        found.insert(staged.sidecar.key.clone(), staged);
                    }
                    None => {
                        tracing::warn!(dir = %dir.display(), "a staged copy's sidecar can't be read; kept it untouched as an orphan")
                    }
                }
            } else if dir.join(COPY).is_dir() {
                // Renamed into place, but the sidecar never was: never reported, so nothing
                // anyone counted. Removed as a staging leftover.
                if let Err(error) = remove_dir_if_present(&dir.join(COPY)) {
                    tracing::warn!(dir = %dir.display(), %error, "can't remove an uncommitted staged copy");
                }
            }
            pending.extend(subdirs);
        }
        if !found.is_empty() {
            tracing::info!(copies = found.len(), "found staged copies of forks on disk");
        }
        *self.lock() = found;
    }

    /// The staged copies of the fork named `name`, of any identity.
    pub(crate) fn of_name(&self, name: &DatabaseName) -> Vec<Staged> {
        self.lock()
            .values()
            .filter(|staged| staged.sidecar.key.fork.name == *name)
            .cloned()
            .collect()
    }

    /// The staged copy `key` names, if the store knows it.
    pub(crate) fn get(&self, key: &StagedKey) -> Option<Staged> {
        self.lock().get(key).cloned()
    }

    /// The names with a staged copy here.
    pub(crate) fn names(&self) -> BTreeSet<DatabaseName> {
        self.lock()
            .keys()
            .map(|key| key.fork.name.clone())
            .collect()
    }

    /// A fresh directory to unpack `key`'s copy into before [`commit`](Self::commit).
    pub(crate) fn incoming(&self, key: &StagedKey) -> PathBuf {
        key.dir(&self.root).join(format!("{COPY}{TEMPORARY}"))
    }

    /// Commits the copy unpacked into [`incoming`](Self::incoming) as `sidecar` describes it:
    /// writes and syncs the sidecar, renames the files into place, then the sidecar, and syncs
    /// the directory. It blocks on the file system. The store doesn't know the copy until
    /// [`publish`](Self::publish).
    ///
    /// # Errors
    ///
    /// The I/O error if any step fails; nothing is committed then, and the scan at the next
    /// start removes what is left.
    pub(crate) fn commit(&self, sidecar: &Sidecar) -> io::Result<()> {
        let dir = sidecar.key.dir(&self.root);
        let incoming = self.incoming(&sidecar.key);
        let text = serde_json::to_string_pretty(&sidecar.to_file()).map_err(io::Error::other)?;
        let temporary = dir.join(format!("{SIDECAR}{TEMPORARY}"));
        {
            let mut file = File::create(&temporary)?;
            io::Write::write_all(&mut file, text.as_bytes())?;
            file.sync_all()?;
        }
        let copy = dir.join(COPY);
        remove_dir_if_present(&copy)?;
        fs::rename(&incoming, &copy)?;
        sync_dir(&dir)?;
        fs::rename(&temporary, dir.join(SIDECAR))?;
        sync_dir(&dir)?;
        if let Some(parent) = dir.parent() {
            sync_dir(parent)?;
        }
        Ok(())
    }

    /// Makes a committed copy known, so that heartbeats report it and peers may fetch it.
    /// Returns `false`, and publishes nothing, if its directory is gone meanwhile, as when a
    /// drop deleted it.
    pub(crate) fn publish(&self, sidecar: Sidecar) -> bool {
        let dir = sidecar.key.dir(&self.root);
        if !dir.join(SIDECAR).is_file() {
            return false;
        }
        self.lock().insert(
            sidecar.key.clone(),
            Staged {
                sidecar,
                held: Held::Present,
            },
        );
        true
    }

    /// The directory of `key`'s files, for packing them to a peer or moving them into place.
    pub(crate) fn copy_dir(&self, key: &StagedKey) -> PathBuf {
        key.dir(&self.root).join(COPY)
    }

    /// Checks `key`'s staged copy against its sidecar and against `expected`, the position and
    /// chain hash control state recorded: the sidecar is the one on disk, every file is there
    /// at its length, and the files hash, in stream order, to the sidecar's checksum. It reads
    /// every byte, and blocks on the file system.
    ///
    /// # Errors
    ///
    /// [`CopyLossReason::Missing`] if the sidecar or a file is gone, and
    /// [`CopyLossReason::Damaged`] if anything differs.
    pub(crate) fn verify(
        &self,
        key: &StagedKey,
        expected: (&Position, &ChainHash),
    ) -> Result<Sidecar, CopyLossReason> {
        let dir = key.dir(&self.root);
        let Some(on_disk) = read_sidecar(&dir) else {
            return Err(if dir.join(SIDECAR).exists() {
                CopyLossReason::Damaged
            } else {
                CopyLossReason::Missing
            });
        };
        let sidecar = on_disk.sidecar;
        if on_disk.held != Held::Present {
            return Err(CopyLossReason::Missing);
        }
        if sidecar.key != *key
            || &sidecar.position != expected.0
            || &sidecar.chain_hash != expected.1
            || self.get(key).is_some_and(|known| known.sidecar != sidecar)
        {
            return Err(CopyLossReason::Damaged);
        }
        let copy = dir.join(COPY);
        let (size, checksum) = stream_checksum(&copy, &sidecar.files).map_err(|error| {
            if error.kind() == io::ErrorKind::NotFound {
                CopyLossReason::Missing
            } else {
                CopyLossReason::Damaged
            }
        })?;
        if size != sidecar.size || checksum != sidecar.checksum {
            return Err(CopyLossReason::Damaged);
        }
        Ok(sidecar)
    }

    /// Records that `key`'s files were moved into the database's place: they are no longer
    /// here, but the sidecar stays until a signal deletes it. It blocks on the file system.
    ///
    /// # Errors
    ///
    /// The I/O error if the record can't be written durably.
    pub(crate) fn installed(&self, key: &StagedKey) -> io::Result<()> {
        let dir = key.dir(&self.root);
        File::create(dir.join(INSTALLED))?.sync_all()?;
        sync_dir(&dir)?;
        if let Some(staged) = self.lock().get_mut(key) {
            staged.held = Held::Installed;
        }
        Ok(())
    }

    /// Deletes `key`'s staged directory, on a positive signal for its identity. It blocks on
    /// the file system.
    ///
    /// # Errors
    ///
    /// The I/O error if the directory can't be deleted; the store forgets it either way.
    pub(crate) fn delete(&self, key: &StagedKey) -> io::Result<()> {
        self.lock().remove(key);
        self.lock_orphans().remove(key);
        let dir = key.dir(&self.root);
        let removed = remove_dir_if_present(&dir);
        // The identity's and the name's directories go with their last copy.
        let mut parent = dir.parent();
        while let Some(current) = parent {
            if current == self.root || fs::remove_dir(current).is_err() {
                break;
            }
            parent = current.parent();
        }
        if let Some(parent) = dir.parent() {
            let _ = sync_dir(parent);
        }
        removed
    }

    /// Deletes every staged directory of `fork`, of every attempt, committed or not, on a
    /// positive signal for its identity: a fetch under way then finds its directory gone, and
    /// publishes nothing. It blocks on the file system.
    ///
    /// # Errors
    ///
    /// The I/O error if the directory can't be deleted; the store forgets its copies either way.
    pub(crate) fn delete_identity(&self, fork: &ForkIdentity) -> io::Result<()> {
        self.lock().retain(|key, _| key.fork != *fork);
        self.lock_orphans().retain(|key| key.fork != *fork);
        let dir = identity_dir(&self.root, fork);
        let removed = remove_dir_if_present(&dir);
        if let Some(parent) = dir.parent() {
            if parent != self.root {
                let _ = fs::remove_dir(parent);
            }
            let _ = sync_dir(parent.parent().unwrap_or(&self.root));
        }
        removed
    }

    /// Marks `key` an orphan: kept, untouched, until an operator removes it.
    pub(crate) fn mark_orphan(&self, key: &StagedKey) -> bool {
        self.lock_orphans().insert(key.clone())
    }

    /// Whether `key` is an orphan.
    pub(crate) fn is_orphan(&self, key: &StagedKey) -> bool {
        self.lock_orphans().contains(key)
    }

    /// Clears `key`'s orphan mark, once its identity is confirmed again.
    pub(crate) fn confirm(&self, key: &StagedKey) {
        self.lock_orphans().remove(key);
    }

    /// The staged orphans, each with its size on disk.
    pub(crate) fn orphans(&self) -> Vec<(StagedKey, u64)> {
        let orphans = self.lock_orphans().clone();
        let entries = self.lock();
        orphans
            .into_iter()
            .map(|key| {
                let bytes = entries.get(&key).map_or(0, |staged| {
                    staged.sidecar.files.iter().map(|file| file.len).sum()
                });
                (key, bytes)
            })
            .collect()
    }
}

/// Reads the sidecar in `dir`, with where its files stand.
fn read_sidecar(dir: &Path) -> Option<Staged> {
    let text = fs::read_to_string(dir.join(SIDECAR)).ok()?;
    let file: SidecarFile = serde_json::from_str(&text).ok()?;
    let sidecar = Sidecar::from_file(file)?;
    let held = if dir.join(INSTALLED).exists() {
        Held::Installed
    } else if dir.join(COPY).is_dir() {
        Held::Present
    } else {
        Held::Lost
    };
    Some(Staged { sidecar, held })
}

/// The size and XXH3-128 checksum of the stream that packs `files`, in order, from `dir`: what
/// [`Packer`](super::super::archive::Packer) would send for them.
///
/// # Errors
///
/// The I/O error if a file can't be read, and [`io::ErrorKind::InvalidData`] if one isn't its
/// recorded length.
pub(crate) fn stream_checksum(
    dir: &Path,
    files: &[StagedFile],
) -> io::Result<(u64, [u8; CHECKSUM_LEN])> {
    let mut hasher = XxHash3_128::new();
    let mut size: u64 = 0;
    let mut buffer = vec![0_u8; 1 << 20];
    for staged in files {
        let header = super::super::archive::file_header(&staged.name, staged.len);
        hasher.write(&header);
        size = size.saturating_add(u64::try_from(header.len()).unwrap_or(u64::MAX));
        let mut file = File::open(dir.join(&staged.name))?;
        let mut read_total: u64 = 0;
        loop {
            let read = file.read(&mut buffer)?;
            if read == 0 {
                break;
            }
            hasher.write(&buffer[..read]);
            read_total = read_total.saturating_add(u64::try_from(read).unwrap_or(u64::MAX));
        }
        if read_total != staged.len {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "{} holds {read_total} bytes, not {}",
                    staged.name, staged.len
                ),
            ));
        }
        size = size.saturating_add(read_total);
    }
    Ok((size, hasher.finish_128().to_be_bytes()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(attempt: u32) -> StagedKey {
        StagedKey {
            fork: ForkIdentity::new(
                "tenant/t".parse().expect("a name"),
                Incarnation::new(0x3F9A_0C6E_21D4_B870),
                OperationId::from_bytes([7; 16]),
            ),
            attempt,
        }
    }

    fn stage(store: &StagedStore, key: &StagedKey, content: &[u8]) -> Sidecar {
        let incoming = store.incoming(key);
        fs::create_dir_all(&incoming).expect("incoming");
        fs::write(incoming.join("data.db"), content).expect("file");
        let files = vec![StagedFile {
            name: "data.db".to_owned(),
            len: u64::try_from(content.len()).expect("small"),
        }];
        let (size, checksum) = stream_checksum(&incoming, &files).expect("checksum");
        let sidecar = Sidecar {
            key: key.clone(),
            position: Position::new([1, 2, 3]),
            chain_hash: ChainHash::GENESIS,
            source_incarnation: Incarnation::new(u64::MAX),
            size,
            checksum,
            files,
        };
        store.commit(&sidecar).expect("commit");
        assert!(store.publish(sidecar.clone()));
        sidecar
    }

    #[test]
    fn a_committed_copy_survives_a_restart_and_verifies() {
        let dir = tempfile::tempdir().expect("dir");
        let store = StagedStore::new(dir.path().join("staged"));
        let first = key(1);
        let sidecar = stage(&store, &first, b"hello");
        // A crash mid-staging leaves temporary files, which the next start removes.
        let leftover = store.incoming(&key(2));
        fs::create_dir_all(&leftover).expect("leftover");
        let again = StagedStore::new(dir.path().join("staged"));
        again.scan();
        assert!(!leftover.exists(), "uncommitted files go");
        let staged = again.get(&first).expect("indexed from its sidecar");
        assert_eq!(staged.sidecar, sidecar);
        assert_eq!(staged.held, Held::Present);
        let position = Position::new([1, 2, 3]);
        assert_eq!(
            again.verify(&first, (&position, &ChainHash::GENESIS)),
            Ok(sidecar)
        );
        // Another position, or a changed byte, fails the check.
        assert_eq!(
            again.verify(&first, (&Position::new([9]), &ChainHash::GENESIS)),
            Err(CopyLossReason::Damaged)
        );
        fs::write(again.copy_dir(&first).join("data.db"), b"hellp").expect("damage");
        assert_eq!(
            again.verify(&first, (&position, &ChainHash::GENESIS)),
            Err(CopyLossReason::Damaged)
        );
        fs::remove_dir_all(again.copy_dir(&first)).expect("lose it");
        assert_eq!(
            again.verify(&first, (&position, &ChainHash::GENESIS)),
            Err(CopyLossReason::Missing)
        );
    }

    #[test]
    fn deleting_one_attempt_keeps_the_others() {
        let dir = tempfile::tempdir().expect("dir");
        let store = StagedStore::new(dir.path().join("staged"));
        stage(&store, &key(1), b"one");
        stage(&store, &key(2), b"two");
        store.delete(&key(1)).expect("delete");
        assert_eq!(store.get(&key(1)), None);
        assert!(store.get(&key(2)).is_some());
        assert!(store.copy_dir(&key(2)).is_dir());
        store.delete(&key(2)).expect("delete");
        assert!(
            fs::read_dir(store.root()).expect("root").next().is_none(),
            "empty directories go with the last copy"
        );
    }
}
