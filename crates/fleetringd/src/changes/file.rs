//! The file sink: one append-only NDJSON file per database.

use std::fs::{self, OpenOptions};
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;

use fleetring_control::DatabaseName;

use crate::changes::record::ChangeRecord;
use crate::changes::{Change, ChangeSink, Published, SinkError};
use crate::engine::Execution;
use crate::layout;

/// The extension of each database's file.
const EXTENSION: &str = "ndjson";

/// A sink that appends each change to `<dir>/<database>.ndjson` as one JSON line, for tests and
/// archives.
///
/// Each line is a [`ChangeRecord`]. The file is synced after every change, so a change the
/// sink took is on disk. The database's name is encoded as its directory under the data
/// directory is, so every database gets its own file on every file system.
#[derive(Debug, Clone)]
pub struct FileSink {
    inner: Arc<Inner>,
}

#[derive(Debug)]
struct Inner {
    name: String,
    dir: PathBuf,
    execution: Execution,
}

impl FileSink {
    /// A sink named `name` that writes under `dir`, which it creates when needed.
    #[must_use]
    pub fn new(name: impl Into<String>, dir: impl Into<PathBuf>) -> Self {
        Self {
            inner: Arc::new(Inner {
                name: name.into(),
                dir: dir.into(),
                execution: Execution::Threads,
            }),
        }
    }

    /// The same sink, appending inline on its task instead of on a blocking thread, as a
    /// simulation needs (ADR-0020).
    #[must_use]
    pub fn with_execution(self, execution: Execution) -> Self {
        Self {
            inner: Arc::new(Inner {
                name: self.inner.name.clone(),
                dir: self.inner.dir.clone(),
                execution,
            }),
        }
    }

    /// The directory the files live in.
    #[must_use]
    pub fn dir(&self) -> &Path {
        &self.inner.dir
    }

    /// The file `database`'s changes are appended to.
    #[must_use]
    pub fn database_file(&self, database: &DatabaseName) -> PathBuf {
        database_file(&self.inner.dir, database)
    }
}

/// The file under `dir` that holds `database`'s changes.
#[must_use]
pub fn database_file(dir: &Path, database: &DatabaseName) -> PathBuf {
    let mut path = dir.join(layout::database_dir(database));
    let mut name = path.file_name().map(ToOwned::to_owned).unwrap_or_default();
    name.push(".");
    name.push(EXTENSION);
    path.set_file_name(name);
    path
}

impl ChangeSink for FileSink {
    fn name(&self) -> &str {
        &self.inner.name
    }

    fn publish(&self, change: Change) -> Published {
        let inner = Arc::clone(&self.inner);
        Box::pin(async move {
            let path = inner.database_file(&change.database);
            let record = ChangeRecord::from(&change);
            let line = serde_json::to_string(&record).map_err(|error| SinkError::Rejected {
                reason: format!("can't encode the change: {error}"),
            })?;
            match inner.execution {
                Execution::Inline => append(&path, &line),
                Execution::Threads => tokio::task::spawn_blocking(move || append(&path, &line))
                    .await
                    .map_err(|error| SinkError::Failed {
                        reason: format!("append task failed: {error}"),
                    })?,
            }
        }) as Pin<Box<_>>
    }
}

impl Inner {
    fn database_file(&self, database: &DatabaseName) -> PathBuf {
        database_file(&self.dir, database)
    }
}

fn append(path: &Path, line: &str) -> Result<(), SinkError> {
    let failed = |error: std::io::Error| SinkError::Failed {
        reason: format!("can't append to {}: {error}", path.display()),
    };
    if let Some(dir) = path.parent() {
        fs::create_dir_all(dir).map_err(failed)?;
    }
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .map_err(failed)?;
    file.write_all(line.as_bytes()).map_err(failed)?;
    file.write_all(b"\n").map_err(failed)?;
    file.sync_data().map_err(failed)?;
    Ok(())
}
