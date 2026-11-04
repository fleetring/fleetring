//! The archive: a physical change sink that writes one segment per delivery, and base
//! snapshots at the archive's own position (ADR-0029, points 1, 6, 8, and 9).

use std::collections::BTreeMap;
use std::fmt;
use std::sync::{Arc, Mutex, MutexGuard};

use fleetring_control::{DatabaseName, LeaseEpoch};

use super::config::{ArchiveConfig, ArchiveSettings, StoreKind};
use super::format::{
    FORMAT, SegmentHeader, SnapshotDescriptor, checksum_hex, decode_segment, encode_descriptor,
    encode_segment,
};
use super::keys::{ArchiveKeys, SegmentName};
use super::listing::Listing;
use super::retention::{apply_retention, retention};
use super::s3::{S3ConfigError, S3Store};
use super::store::{DirectoryStore, ObjectStore, retrying};
use crate::changes::{
    Change, ChangeKind, ChangeSink, ConfiguredSink, DatabaseMatchers, Format, Published, SinkError,
    hex,
};
use crate::clock::Clock;
use crate::engine::{
    DatabaseEngine, EngineCapability, EngineKind, Execution, FrameBatch, FrameSourceEngine,
    Position,
};
use crate::replication::{ChainHash, Packer, Replication};

/// What an [`Archive`] runs on: the node's replication, whose registry it pins snapshots
/// through, its engine, its clock, and where blocking reads run.
#[derive(Debug, Clone)]
pub struct ArchiveParts {
    /// The node's replication.
    pub replication: Replication,
    /// The engine the node's databases run on, which must be a frame source.
    pub engine: Arc<dyn DatabaseEngine>,
    /// The clock segments and snapshots are stamped with.
    pub clock: Arc<dyn Clock>,
    /// Where blocking reads of a snapshot's files run: inline under simulation.
    pub execution: Execution,
}

/// The error returned when an archive can't be built.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum ArchiveError {
    /// The engine replicates itself and ships no batches to archive.
    #[error(
        "archive `{archive}` can't run on {engine} databases, which replicate themselves and ship no batches"
    )]
    SelfReplicating {
        /// The archive.
        archive: String,
        /// The engine.
        engine: EngineKind,
    },
    /// The archive's S3 store can't be built.
    #[error("archive `{archive}`: {source}")]
    S3 {
        /// The archive.
        archive: String,
        /// Why.
        source: S3ConfigError,
    },
}

/// Builds the store an `[[archive]]` entry names, a directory or an S3 bucket.
///
/// # Errors
///
/// [`ArchiveError::S3`] if the S3 store can't be built.
pub fn build_store(
    config: &ArchiveConfig,
    clock: Arc<dyn Clock>,
    execution: Execution,
) -> Result<Arc<dyn ObjectStore>, ArchiveError> {
    let s3_error = |source| ArchiveError::S3 {
        archive: config.name.clone(),
        source,
    };
    match (config.store, &config.path, config.s3_settings()) {
        (StoreKind::Directory, Some(path), _) => Ok(Arc::new(
            DirectoryStore::new(path).with_execution(execution),
        )),
        (StoreKind::S3, _, Some(settings)) => {
            Ok(Arc::new(S3Store::new(settings, clock).map_err(s3_error)?))
        }
        _ => Err(s3_error(S3ConfigError::InvalidEndpoint {
            endpoint: String::new(),
            reason: "the [[archive]] entry wasn't validated".to_owned(),
        })),
    }
}

/// An archive: the sink that feeds it, its store, and what it knows of each database.
///
/// Cloning is cheap and shares everything.
#[derive(Debug, Clone)]
pub struct Archive {
    inner: Arc<Inner>,
}

struct Inner {
    name: String,
    prefix: String,
    databases: DatabaseMatchers,
    settings: ArchiveSettings,
    store: Arc<dyn ObjectStore>,
    replication: Replication,
    engine: Arc<dyn DatabaseEngine>,
    clock: Arc<dyn Clock>,
    execution: Execution,
    /// Each database's state, behind a lock its sink and its snapshots share.
    databases_state: Mutex<BTreeMap<DatabaseName, Arc<tokio::sync::Mutex<DbArchive>>>>,
}

impl fmt::Debug for Inner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Archive")
            .field("name", &self.name)
            .field("prefix", &self.prefix)
            .field("databases", &self.databases)
            .field("settings", &self.settings)
            .field("store", &self.store)
            .finish_non_exhaustive()
    }
}

/// What the archive knows of one database.
#[derive(Debug, Default)]
struct DbArchive {
    /// Where the archive's history of the database ends, as far as this node knows.
    continuity: Continuity,
    /// The bytes of segments uploaded since the newest base snapshot.
    bytes_since_snapshot: u64,
    /// When the newest base snapshot was taken, if the archive knows of one.
    last_snapshot_ms: Option<u64>,
    /// The newest `archived_at_ms` of a segment, which the next one is never stamped before.
    last_archived_ms: u64,
}

/// Where the archive's history of a database ends.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
enum Continuity {
    /// Not known: this node hasn't archived the database since it started.
    #[default]
    Unknown,
    /// At a position, with the hash chain there: a segment or a snapshot ends there.
    At {
        position: Option<Position>,
        chain: ChainHash,
    },
}

impl Archive {
    /// The archive an `[[archive]]` entry describes, writing to `store`.
    ///
    /// # Errors
    ///
    /// [`ArchiveError::SelfReplicating`] unless the engine is a frame source.
    pub fn new(
        config: &ArchiveConfig,
        store: Arc<dyn ObjectStore>,
        parts: ArchiveParts,
    ) -> Result<Self, ArchiveError> {
        Self::with_settings(
            &config.name,
            &config.prefix,
            config.matchers(),
            config.settings(),
            store,
            parts,
        )
    }

    /// An archive named `name` of the databases `databases` names, under `prefix` in `store`,
    /// run by `settings`.
    ///
    /// # Errors
    ///
    /// [`ArchiveError::SelfReplicating`] unless the engine is a frame source.
    pub fn with_settings(
        name: &str,
        prefix: &str,
        databases: DatabaseMatchers,
        settings: ArchiveSettings,
        store: Arc<dyn ObjectStore>,
        parts: ArchiveParts,
    ) -> Result<Self, ArchiveError> {
        if let EngineCapability::SelfReplicating = parts.engine.capability() {
            return Err(ArchiveError::SelfReplicating {
                archive: name.to_owned(),
                engine: parts.engine.kind(),
            });
        }
        Ok(Self {
            inner: Arc::new(Inner {
                name: name.to_owned(),
                prefix: prefix.to_owned(),
                databases,
                settings,
                store,
                replication: parts.replication,
                engine: parts.engine,
                clock: parts.clock,
                execution: parts.execution,
                databases_state: Mutex::default(),
            }),
        })
    }

    /// The archive's name, which its sink's marks are kept by.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.inner.name
    }

    /// The store the archive writes to.
    #[must_use]
    pub fn store(&self) -> Arc<dyn ObjectStore> {
        Arc::clone(&self.inner.store)
    }

    /// The keys of `database`'s objects in the archive.
    #[must_use]
    pub fn keys(&self, database: &DatabaseName) -> ArchiveKeys {
        ArchiveKeys::new(&self.inner.prefix, database)
    }

    /// Whether the archive covers `database`.
    #[must_use]
    pub fn covers(&self, database: &DatabaseName) -> bool {
        self.inner.databases.matches(database)
    }

    /// The change sink that writes the archive's segments: physical, paced by the segment
    /// interval, for the databases the archive covers.
    #[must_use]
    pub fn sink(&self) -> ConfiguredSink {
        ConfiguredSink {
            sink: Arc::new(ArchiveSink {
                inner: Arc::clone(&self.inner),
            }),
            format: Format::Physical,
            databases: self.inner.databases.clone(),
        }
    }
}

/// The change sink an [`Archive`] is fed by.
#[derive(Debug)]
struct ArchiveSink {
    inner: Arc<Inner>,
}

impl ChangeSink for ArchiveSink {
    fn name(&self) -> &str {
        &self.inner.name
    }

    fn publish(&self, change: Change) -> Published {
        let inner = Arc::clone(&self.inner);
        Box::pin(async move { inner.publish(change).await })
    }

    fn pace(&self) -> std::time::Duration {
        self.inner.settings.segment_interval
    }
}

fn failed(reason: impl Into<String>) -> SinkError {
    SinkError::Failed {
        reason: reason.into(),
    }
}

/// Runs `call` where `execution` says: inline, or on a blocking thread.
pub(crate) async fn blocking<T: Send + 'static>(
    execution: Execution,
    call: impl FnOnce() -> T + Send + 'static,
) -> Result<T, String> {
    match execution {
        Execution::Inline => Ok(call()),
        Execution::Threads => tokio::task::spawn_blocking(call)
            .await
            .map_err(|error| format!("a blocking call failed: {error}")),
    }
}

impl Inner {
    fn lock_states(
        &self,
    ) -> MutexGuard<'_, BTreeMap<DatabaseName, Arc<tokio::sync::Mutex<DbArchive>>>> {
        self.databases_state.lock().expect("archive lock poisoned")
    }

    fn state(&self, database: &DatabaseName) -> Arc<tokio::sync::Mutex<DbArchive>> {
        Arc::clone(self.lock_states().entry(database.clone()).or_default())
    }

    fn frame_source(&self) -> Result<&dyn FrameSourceEngine, SinkError> {
        match self.engine.capability() {
            EngineCapability::FrameSource(engine) => Ok(engine),
            _ => Err(SinkError::Rejected {
                reason: format!("{} databases ship no batches", self.engine.kind()),
            }),
        }
    }

    /// Takes one delivered batch: uploads it as a segment if the archive holds a path to its
    /// start, and otherwise takes a base snapshot at its end, which holds it.
    async fn publish(self: Arc<Self>, change: Change) -> Result<(), SinkError> {
        let Change {
            database,
            epoch,
            start,
            position,
            kind,
            chain_start,
            chain_end,
        } = change;
        let ChangeKind::Physical(payload) = kind else {
            return Err(SinkError::Rejected {
                reason: "an archive takes physical changes only".to_owned(),
            });
        };
        let Some(chain_end) = chain_end else {
            return Err(failed(format!(
                "the primary's hash chain doesn't reach {position} yet"
            )));
        };
        let keys = ArchiveKeys::new(&self.prefix, &database);
        let state = self.state(&database);
        let mut state = state.lock().await;
        let held = match &state.continuity {
            Continuity::At {
                position: at,
                chain,
            } if *at == start => Some(*chain),
            _ => match &start {
                Some(start) => self.find_path(&mut state, &keys, &database, start).await?,
                None => None,
            },
        };
        let held = match (held, chain_start) {
            (Some(held), Some(primary)) if held != primary => {
                tracing::warn!(archive = %self.name, %database, ?start, archived = %held, %primary, "the archive's chain differs from the primary's here; taking a new base snapshot");
                None
            }
            (held, _) => held,
        };
        let Some(held) = held else {
            // No path to the batch's start: a new archive, a reset stream, or an emptied store. A
            // snapshot at the batch's end holds it, and holds nothing unacknowledged.
            return self
                .snapshot(&mut state, &keys, &database, epoch, &position, chain_end)
                .await
                .map_err(failed);
        };
        let batch = FrameBatch {
            start: start.clone().unwrap_or_default(),
            end: position.clone(),
            payload,
        };
        let computed = ChainHash::chain(self.frame_source()?, held, start.as_ref(), &batch)
            .map_err(|error| failed(format!("can't hash the batch: {error}")))?;
        if computed != chain_end {
            tracing::error!(archive = %self.name, %database, ?start, end = %position, %computed, primary = %chain_end, "a delivered batch doesn't hash to the primary's chain; not archiving it");
            return Err(failed(format!(
                "the batch to {position} hashes to {computed}, not to the primary's {chain_end}"
            )));
        }
        let now = self.clock.now().as_millis();
        let archived_at_ms = now.max(state.last_archived_ms);
        let name = SegmentName {
            start: start.clone(),
            end: position.clone(),
            epoch,
            archived_at_ms,
        };
        let header = SegmentHeader {
            format: FORMAT,
            database: database.to_string(),
            engine: self.engine.kind().to_string(),
            epoch: epoch.get(),
            start: start.as_ref().map(|start| hex(start.as_bytes())),
            end: hex(position.as_bytes()),
            chain_start: held.to_string(),
            chain_end: chain_end.to_string(),
            archived_at_ms,
            payload_len: u64::try_from(batch.payload.len()).unwrap_or(u64::MAX),
        };
        let bytes = encode_segment(&header, batch.payload.as_bytes());
        let size = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
        let key = keys.segment(&name);
        retrying(|| self.store.put(&key, bytes.clone()))
            .await
            .map_err(|error| failed(format!("can't upload {key}: {error}")))?;
        state.continuity = Continuity::At {
            position: Some(position.clone()),
            chain: chain_end,
        };
        state.bytes_since_snapshot = state.bytes_since_snapshot.saturating_add(size);
        state.last_archived_ms = archived_at_ms;
        let due = state
            .last_snapshot_ms
            .is_none_or(|taken| now.saturating_sub(taken) >= self.settings.snapshot_interval_ms)
            || state.bytes_since_snapshot >= self.settings.snapshot_after_bytes;
        if due {
            // The segment is in the store either way: a failed snapshot is taken again later.
            if let Err(error) = self
                .snapshot(&mut state, &keys, &database, epoch, &position, chain_end)
                .await
            {
                tracing::warn!(archive = %self.name, %database, %error, "a due base snapshot failed; it is retried after the next segment");
            }
        }
        Ok(())
    }

    /// Looks in the store for a path to `start`: a complete snapshot there, or a segment that
    /// ends there. Learns, on the way, what the store holds of the database. Returns the hash
    /// chain at `start` if there is one.
    async fn find_path(
        &self,
        state: &mut DbArchive,
        keys: &ArchiveKeys,
        database: &DatabaseName,
        start: &Position,
    ) -> Result<Option<ChainHash>, SinkError> {
        let listing = Listing::read(self.store.as_ref(), keys, database)
            .await
            .map_err(|error| failed(format!("can't list the archive: {error}")))?;
        let (_, latest_ms) = listing.latest();
        state.last_archived_ms = state.last_archived_ms.max(latest_ms.unwrap_or(0));
        let newest = listing
            .snapshots
            .iter()
            .max_by(|a, b| (&a.position, a.epoch).cmp(&(&b.position, b.epoch)));
        if let Some(newest) = newest {
            state.last_snapshot_ms = Some(newest.descriptor.taken_at_ms);
            state.bytes_since_snapshot = listing
                .segments
                .iter()
                .filter(|segment| segment.name.start >= newest.position)
                .map(|segment| segment.size)
                .sum();
        }
        if let Some(snapshot) = listing
            .snapshots
            .iter()
            .find(|snapshot| snapshot.position.as_ref() == Some(start))
        {
            return Ok(Some(snapshot.chain));
        }
        let ending = listing
            .segments
            .iter()
            .filter(|segment| segment.name.end == *start)
            .max_by(|a, b| {
                (a.name.epoch, a.name.archived_at_ms).cmp(&(b.name.epoch, b.name.archived_at_ms))
            });
        let Some(segment) = ending else {
            return Ok(None);
        };
        let bytes = retrying(|| self.store.get(&segment.key))
            .await
            .map_err(|error| failed(format!("can't read {}: {error}", segment.key)))?;
        let chain = bytes.and_then(|bytes| {
            let (header, _) = decode_segment(&bytes).ok()?;
            header.chain_end.parse::<ChainHash>().ok()
        });
        if chain.is_none() {
            tracing::warn!(archive = %self.name, %database, key = %segment.key, "the segment the archive continues from can't be read; taking a new base snapshot");
        }
        Ok(chain)
    }

    /// Takes a base snapshot of `database` at `at`, where the hash chain is `chain`, and
    /// uploads it: its chunks, then its descriptor, which marks it complete. Then prunes.
    async fn snapshot(
        &self,
        state: &mut DbArchive,
        keys: &ArchiveKeys,
        database: &DatabaseName,
        epoch: LeaseEpoch,
        at: &Position,
        chain: ChainHash,
    ) -> Result<(), String> {
        let handle = self
            .replication
            .registry()
            .open(database)
            .await
            .map_err(|error| error.to_string())?;
        let pinned = handle
            .pin_snapshot_at(Some(at.clone()))
            .await
            .map_err(|error| format!("can't pin a snapshot at {at}: {error}"))?;
        let taken_at_ms = self.clock.now().as_millis();
        let id = ArchiveKeys::snapshot_id(Some(at), epoch);
        let mut packer = Packer::new(pinned).map_err(|error| error.to_string())?;
        let chunk_bytes = self.settings.chunk_bytes;
        let mut chunk_checksums = Vec::new();
        let mut bytes = 0_u64;
        loop {
            let (returned, next) = blocking(self.execution, move || {
                let next = packer.next_chunk(chunk_bytes);
                (packer, next)
            })
            .await?;
            packer = returned;
            let Some(data) = next.map_err(|error| format!("can't read the snapshot: {error}"))?
            else {
                break;
            };
            let index = u32::try_from(chunk_checksums.len()).map_err(|error| error.to_string())?;
            chunk_checksums.push(checksum_hex(&data));
            bytes = bytes.saturating_add(u64::try_from(data.len()).unwrap_or(u64::MAX));
            let key = keys.chunk(&id, index);
            retrying(|| self.store.put(&key, data.clone()))
                .await
                .map_err(|error| format!("can't upload {key}: {error}"))?;
        }
        let checksum = packer
            .finish()
            .map_err(|error| format!("the snapshot changed while it was read: {error}"))?;
        drop(packer);
        let descriptor = SnapshotDescriptor {
            format: FORMAT,
            database: database.to_string(),
            engine: self.engine.kind().to_string(),
            epoch: epoch.get(),
            position: Some(hex(at.as_bytes())),
            chain: chain.to_string(),
            taken_at_ms,
            chunks: u32::try_from(chunk_checksums.len()).map_err(|error| error.to_string())?,
            bytes,
            checksum: hex(&checksum),
            chunk_checksums,
        };
        let key = keys.descriptor(&id);
        let encoded = encode_descriptor(&descriptor);
        retrying(|| self.store.put(&key, encoded.clone()))
            .await
            .map_err(|error| format!("can't upload {key}: {error}"))?;
        tracing::info!(archive = %self.name, %database, %at, bytes, "took a base snapshot");
        state.continuity = Continuity::At {
            position: Some(at.clone()),
            chain,
        };
        state.bytes_since_snapshot = 0;
        state.last_snapshot_ms = Some(taken_at_ms);
        self.prune(keys, database).await;
        Ok(())
    }

    /// Deletes what retention says no kept snapshot's path can use. A failure leaves the rest
    /// for the next snapshot.
    async fn prune(&self, keys: &ArchiveKeys, database: &DatabaseName) {
        let pruned = match Listing::read(self.store.as_ref(), keys, database).await {
            Ok(listing) => {
                let doomed = retention(&listing, self.settings.keep_snapshots);
                apply_retention(self.store.as_ref(), &doomed)
                    .await
                    .map(|()| doomed.len())
            }
            Err(error) => Err(error),
        };
        match pruned {
            Ok(0) => {}
            Ok(deleted) => {
                tracing::debug!(archive = %self.name, %database, deleted, "pruned the archive");
            }
            Err(error) => {
                tracing::warn!(archive = %self.name, %database, %error, "can't prune the archive; retrying after the next snapshot");
            }
        }
    }
}
