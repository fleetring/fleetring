//! The object-store seam (ADR-0029, point 2): whole immutable objects by key, and the stores
//! that keep them in memory, in a local directory, or behind a schedule of failures.

use std::collections::BTreeMap;
use std::fmt;
use std::fs::{self, File};
use std::future::Future;
use std::io::{self, Write as _};
use std::num::NonZeroU32;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

use crate::engine::Execution;

/// The future an [`ObjectStore`] call returns.
pub type StoreFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, StoreError>> + Send + 'a>>;

/// Whole immutable objects by key: the only operations an archive needs (ADR-0029, point 2).
///
/// A key is one or more components joined by `/`; no component is empty or begins with `.`.
/// Every store refuses other keys with [`StoreError::InvalidKey`], so an archive's keys are
/// valid on every store. An archive never overwrites an object, and never needs a lock or a
/// conditional write: names say what objects hold, and `list` over a prefix is the manifest.
///
/// A store must give read-after-write and list-after-write consistency, as AWS S3, MinIO, and
/// Cloudflare R2 do. One that doesn't can make a restore fail, never succeed wrongly, since a
/// restore verifies every byte it applies.
pub trait ObjectStore: fmt::Debug + Send + Sync + 'static {
    /// Stores `bytes` under `key`, replacing any object there and nothing else. Completes once
    /// the object is durable.
    fn put(&self, key: &str, bytes: Vec<u8>) -> StoreFuture<'_, ()>;

    /// The object under `key`, or `None` if there is none.
    fn get(&self, key: &str) -> StoreFuture<'_, Option<Vec<u8>>>;

    /// Every object whose key starts with `prefix`, in key order. A prefix is a key, an empty
    /// string, or either followed by `/`.
    fn list(&self, prefix: &str) -> StoreFuture<'_, Vec<ObjectEntry>>;

    /// Removes the object under `key`; removing a missing key succeeds.
    fn delete(&self, key: &str) -> StoreFuture<'_, ()>;
}

/// One object a [`list`](ObjectStore::list) found.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ObjectEntry {
    /// The object's key.
    pub key: String,
    /// Its length in bytes.
    pub size: u64,
}

/// The error an [`ObjectStore`] call returns.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum StoreError {
    /// The store can't be reached or failed for a reason that may pass; callers retry.
    #[error("object store unavailable: {reason}")]
    Unavailable {
        /// Why.
        reason: String,
    },
    /// The store refused the request, as for missing permissions or a missing bucket.
    #[error("object store rejected the request: {reason}")]
    Rejected {
        /// Why.
        reason: String,
    },
    /// The key isn't one every store accepts.
    #[error("invalid object key '{key}'")]
    InvalidKey {
        /// The key.
        key: String,
    },
}

impl StoreError {
    /// Whether a retry may succeed.
    #[must_use]
    pub fn is_transient(&self) -> bool {
        matches!(self, Self::Unavailable { .. })
    }
}

/// How many times an archive retries a store call that failed for a reason that may pass,
/// before it gives up and lets its caller retry the whole operation.
const CALL_RETRIES: u32 = 3;

/// The wait before the first retry of a store call; each later wait doubles.
const CALL_BACKOFF: std::time::Duration = std::time::Duration::from_millis(20);

/// Runs `call` until it succeeds, fails for good, or has failed transiently
/// [`CALL_RETRIES`] times more, waiting a doubling backoff between attempts. An operation of
/// several calls, such as uploading a snapshot, retries each call, so that a store that fails
/// one call in a few never fails the whole operation every time.
pub(crate) async fn retrying<T, F>(mut call: impl FnMut() -> F) -> Result<T, StoreError>
where
    F: Future<Output = Result<T, StoreError>>,
{
    let mut backoff = CALL_BACKOFF;
    let mut retries = 0;
    loop {
        match call().await {
            Err(error) if error.is_transient() && retries < CALL_RETRIES => {
                retries += 1;
                tracing::debug!(%error, retry_in = ?backoff, "an object store call failed");
                tokio::time::sleep(backoff).await;
                backoff *= 2;
            }
            result => return result,
        }
    }
}

/// Checks that `key` is a valid object key.
pub(crate) fn check_key(key: &str) -> Result<(), StoreError> {
    if key.split('/').all(valid_component) {
        Ok(())
    } else {
        Err(StoreError::InvalidKey {
            key: key.to_owned(),
        })
    }
}

/// Checks that `prefix` is a valid key prefix: empty, or a key, optionally followed by `/`.
pub(crate) fn check_prefix(prefix: &str) -> Result<(), StoreError> {
    let trimmed = prefix.strip_suffix('/').unwrap_or(prefix);
    if trimmed.is_empty() && prefix.is_empty() {
        return Ok(());
    }
    check_key(trimmed).map_err(|_| StoreError::InvalidKey {
        key: prefix.to_owned(),
    })
}

fn valid_component(component: &str) -> bool {
    !component.is_empty() && !component.starts_with('.') && !component.contains(['\\', '\0'])
}

fn ready<T: Send + 'static>(result: Result<T, StoreError>) -> StoreFuture<'static, T> {
    Box::pin(std::future::ready(result))
}

/// A store in memory, for unit tests and simulation. Clones share the objects.
#[derive(Debug, Clone, Default)]
pub struct MemoryStore {
    objects: Arc<Mutex<BTreeMap<String, Vec<u8>>>>,
}

impl MemoryStore {
    /// An empty store.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    fn lock(&self) -> MutexGuard<'_, BTreeMap<String, Vec<u8>>> {
        self.objects.lock().expect("memory store lock poisoned")
    }
}

impl ObjectStore for MemoryStore {
    fn put(&self, key: &str, bytes: Vec<u8>) -> StoreFuture<'_, ()> {
        ready(check_key(key).map(|()| {
            self.lock().insert(key.to_owned(), bytes);
        }))
    }

    fn get(&self, key: &str) -> StoreFuture<'_, Option<Vec<u8>>> {
        ready(check_key(key).map(|()| self.lock().get(key).cloned()))
    }

    fn list(&self, prefix: &str) -> StoreFuture<'_, Vec<ObjectEntry>> {
        ready(check_prefix(prefix).map(|()| {
            self.lock()
                .range(prefix.to_owned()..)
                .take_while(|(key, _)| key.starts_with(prefix))
                .map(|(key, bytes)| ObjectEntry {
                    key: key.clone(),
                    size: u64::try_from(bytes.len()).unwrap_or(u64::MAX),
                })
                .collect()
        }))
    }

    fn delete(&self, key: &str) -> StoreFuture<'_, ()> {
        ready(check_key(key).map(|()| {
            self.lock().remove(key);
        }))
    }
}

/// A store in a local directory: for tests, and for a mounted volume.
///
/// A key's object is the file at the key's components under the root. A put writes a hidden
/// temporary file beside it, syncs it, renames it into place, and syncs the directory, so an
/// object is either whole or absent; `list` skips the hidden files a crash left behind.
#[derive(Debug, Clone)]
pub struct DirectoryStore {
    root: PathBuf,
    execution: Execution,
    staged: Arc<AtomicU64>,
}

impl DirectoryStore {
    /// A store whose objects live under `root`, which is created on the first put.
    #[must_use]
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            root: root.into(),
            execution: Execution::Threads,
            staged: Arc::default(),
        }
    }

    /// The same store, running its file-system calls as `execution` says: inline under
    /// simulation, so that it stays on one deterministic thread (ADR-0020).
    #[must_use]
    pub fn with_execution(mut self, execution: Execution) -> Self {
        self.execution = execution;
        self
    }

    /// The directory the objects live under.
    #[must_use]
    pub fn root(&self) -> &Path {
        &self.root
    }

    fn path(&self, key: &str) -> PathBuf {
        key.split('/')
            .fold(self.root.clone(), |path, component| path.join(component))
    }

    fn run<T: Send + 'static>(
        &self,
        call: impl FnOnce() -> io::Result<T> + Send + 'static,
    ) -> StoreFuture<'static, T> {
        let unavailable = |error: io::Error| StoreError::Unavailable {
            reason: error.to_string(),
        };
        match self.execution {
            Execution::Inline => ready(call().map_err(unavailable)),
            Execution::Threads => Box::pin(async move {
                tokio::task::spawn_blocking(call)
                    .await
                    .map_err(|error| StoreError::Unavailable {
                        reason: error.to_string(),
                    })?
                    .map_err(unavailable)
            }),
        }
    }
}

impl ObjectStore for DirectoryStore {
    fn put(&self, key: &str, bytes: Vec<u8>) -> StoreFuture<'_, ()> {
        if let Err(error) = check_key(key) {
            return ready(Err(error));
        }
        let path = self.path(key);
        let n = self.staged.fetch_add(1, Ordering::Relaxed);
        self.run(move || write_whole(&path, &bytes, n))
    }

    fn get(&self, key: &str) -> StoreFuture<'_, Option<Vec<u8>>> {
        if let Err(error) = check_key(key) {
            return ready(Err(error));
        }
        let path = self.path(key);
        self.run(move || match fs::read(&path) {
            Ok(bytes) => Ok(Some(bytes)),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
            // A key whose path is a directory holds no object.
            Err(_) if path.is_dir() => Ok(None),
            Err(error) => Err(error),
        })
    }

    fn list(&self, prefix: &str) -> StoreFuture<'_, Vec<ObjectEntry>> {
        if let Err(error) = check_prefix(prefix) {
            return ready(Err(error));
        }
        // Walk from the deepest directory the prefix names whole.
        let (dir_key, _) = prefix.rsplit_once('/').unwrap_or(("", prefix));
        let dir = if dir_key.is_empty() {
            self.root.clone()
        } else {
            self.path(dir_key)
        };
        let base = dir_key.to_owned();
        let prefix = prefix.to_owned();
        self.run(move || {
            let mut found = Vec::new();
            walk(&dir, &base, &mut found)?;
            found.retain(|entry| entry.key.starts_with(&prefix));
            found.sort();
            Ok(found)
        })
    }

    fn delete(&self, key: &str) -> StoreFuture<'_, ()> {
        if let Err(error) = check_key(key) {
            return ready(Err(error));
        }
        let path = self.path(key);
        self.run(move || match fs::remove_file(&path) {
            Err(error) if error.kind() != io::ErrorKind::NotFound => Err(error),
            _ => Ok(()),
        })
    }
}

/// Writes `bytes` to `path` whole: a hidden temporary file numbered `n`, synced, renamed into
/// place, and the directory synced.
fn write_whole(path: &Path, bytes: &[u8], n: u64) -> io::Result<()> {
    let dir = path
        .parent()
        .ok_or_else(|| io::Error::other("an object's path has a parent directory"))?;
    fs::create_dir_all(dir)?;
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| io::Error::other("an object's path ends in a name"))?;
    let staged = dir.join(format!(".{name}.{n}.tmp"));
    let mut file = File::create(&staged)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    drop(file);
    fs::rename(&staged, path)?;
    if cfg!(unix) {
        File::open(dir)?.sync_all()?;
    }
    Ok(())
}

/// Adds every object file under `dir`, whose key is `key`, to `found`.
fn walk(dir: &Path, key: &str, found: &mut Vec<ObjectEntry>) -> io::Result<()> {
    let entries = match fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(error)
            if matches!(
                error.kind(),
                io::ErrorKind::NotFound | io::ErrorKind::NotADirectory
            ) =>
        {
            return Ok(());
        }
        Err(error) => return Err(error),
    };
    for entry in entries {
        let entry = entry?;
        let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
            continue;
        };
        if !valid_component(&name) {
            continue;
        }
        let child = if key.is_empty() {
            name
        } else {
            format!("{key}/{name}")
        };
        let kind = entry.file_type()?;
        if kind.is_dir() {
            walk(&entry.path(), &child, found)?;
        } else if kind.is_file() {
            found.push(ObjectEntry {
                key: child,
                size: entry.metadata()?.len(),
            });
        }
    }
    Ok(())
}

/// A store that fails every `fail_every`-th call with [`StoreError::Unavailable`], without
/// passing it on, and passes every other call to the store it wraps. The schedule is a count of
/// calls, so it is deterministic: for simulation and tests.
#[derive(Debug)]
pub struct FaultyStore {
    inner: Arc<dyn ObjectStore>,
    fail_every: NonZeroU32,
    calls: AtomicU32,
}

impl FaultyStore {
    /// `inner`, failing every `fail_every`-th call.
    #[must_use]
    pub fn new(inner: Arc<dyn ObjectStore>, fail_every: NonZeroU32) -> Self {
        Self {
            inner,
            fail_every,
            calls: AtomicU32::new(0),
        }
    }

    /// Whether this call is one that fails.
    fn fails(&self, operation: &str, key: &str) -> Option<StoreError> {
        let call = self.calls.fetch_add(1, Ordering::Relaxed).wrapping_add(1);
        call.is_multiple_of(self.fail_every.get())
            .then(|| StoreError::Unavailable {
                reason: format!("injected failure of call {call} ({operation} {key})"),
            })
    }
}

impl ObjectStore for FaultyStore {
    fn put(&self, key: &str, bytes: Vec<u8>) -> StoreFuture<'_, ()> {
        match self.fails("put", key) {
            Some(error) => ready(Err(error)),
            None => self.inner.put(key, bytes),
        }
    }

    fn get(&self, key: &str) -> StoreFuture<'_, Option<Vec<u8>>> {
        match self.fails("get", key) {
            Some(error) => ready(Err(error)),
            None => self.inner.get(key),
        }
    }

    fn list(&self, prefix: &str) -> StoreFuture<'_, Vec<ObjectEntry>> {
        match self.fails("list", prefix) {
            Some(error) => ready(Err(error)),
            None => self.inner.list(prefix),
        }
    }

    fn delete(&self, key: &str) -> StoreFuture<'_, ()> {
        match self.fails("delete", key) {
            Some(error) => ready(Err(error)),
            None => self.inner.delete(key),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keys_are_components_none_empty_or_hidden() {
        for key in ["a", "a/b", "fleetring/tenant%2fa/segments/x_y.seg"] {
            assert_eq!(check_key(key), Ok(()), "{key}");
        }
        for key in ["", "/a", "a/", "a//b", "a/./b", "a/../b", ".hidden", "a\\b"] {
            assert!(check_key(key).is_err(), "{key}");
        }
        for prefix in ["", "a/", "a/b", "a/b/"] {
            assert_eq!(check_prefix(prefix), Ok(()), "{prefix}");
        }
        for prefix in ["/", "a//", "../"] {
            assert!(check_prefix(prefix).is_err(), "{prefix}");
        }
    }
}
