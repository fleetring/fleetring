//! Restore execution (ADR-0029, point 10): builds a copy from a plan, verifying every byte.
//!
//! The snapshot's chunks are checked against their checksums and the stream against its own,
//! then the copy is opened as a follower. Every segment is checked against its checksum and
//! against the name it was listed under; its `chain_start` must be the hash the copy is at, and
//! the chain recomputed over its batch must give its `chain_end`, before the engine applies it.
//! Anything that doesn't hold fails the restore as data loss, naming the object, and the staging
//! directory is removed: nothing partial is left behind, and nothing is skipped.

use std::io;
use std::path::Path;
use std::sync::Arc;

use fleetring_control::DatabaseName;

use super::archive::blocking;
use super::format::{SegmentHeader, checksum_hex, decode_segment};
use super::keys::ArchiveKeys;
use super::listing::{Listing, SegmentEntry};
use super::plan::{PlanError, RestorePlan, RestoreTarget, plan};
use super::store::{ObjectStore, StoreError, retrying};
use crate::changes::{hex, unhex};
use crate::engine::{
    DatabaseCapability, DatabaseEngine, EngineCapability, EngineError, Execution, FrameBatch,
    FrameFollower, OpenMode, Position,
};
use crate::replication::{ChainHash, Unpacker, remove_dir_if_present};

/// What a restore built.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct Restored {
    /// The position the copy is at, or `None` for the log's beginning.
    pub position: Option<Position>,
    /// The hash chain there, verified over every batch applied.
    pub chain: ChainHash,
    /// The snapshot it started from.
    pub snapshot: String,
    /// How many segments it applied.
    pub segments: u32,
    /// When the last segment applied was archived, or `None` if none was.
    pub archived_at_ms: Option<u64>,
}

/// The error returned when a restore fails. Nothing is left in the staging directory.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum RestoreError {
    /// The archive can't reach the target.
    #[error(transparent)]
    Plan(#[from] PlanError),
    /// The target position isn't a transaction boundary of the segment it falls in.
    #[error("{target} isn't a transaction boundary; the nearest are {} and {}", position_text(before.as_ref()), position_text(after.as_ref()))]
    NotBoundary {
        /// The target.
        target: Position,
        /// The boundary before it.
        before: Option<Position>,
        /// The boundary after it.
        after: Option<Position>,
    },
    /// The store can't be read.
    #[error(transparent)]
    Store(#[from] StoreError),
    /// An object is missing, altered, or doesn't continue the chain.
    #[error("data loss in {object}: {reason}")]
    DataLoss {
        /// The object.
        object: String,
        /// What is wrong with it.
        reason: String,
    },
    /// The engine can't open or apply the copy.
    #[error("the engine failed the restore")]
    Engine(#[source] EngineError),
    /// The staging directory can't be written.
    #[error("can't write the restored copy")]
    Io(#[source] io::Error),
    /// The engine replicates itself, and its databases can't be restored from an archive.
    #[error("{0} databases can't be restored from an archive")]
    SelfReplicating(String),
}

fn position_text(position: Option<&Position>) -> String {
    position.map_or_else(|| "the log's beginning".to_owned(), ToString::to_string)
}

fn data_loss(object: &str, reason: impl Into<String>) -> RestoreError {
    RestoreError::DataLoss {
        object: object.to_owned(),
        reason: reason.into(),
    }
}

/// Builds the copy of `database` that `target` names, from the archive under `keys` in `store`,
/// in `staging`, a directory that must not exist yet. The copy is the engine's follower copy,
/// byte for byte what a follower holding that prefix of the log would hold.
///
/// # Errors
///
/// [`RestoreError`] if the archive can't reach the target, an object is missing or altered,
/// the store or the engine fails, or `staging` exists. On any error `staging` is removed.
pub async fn restore_into(
    store: &dyn ObjectStore,
    keys: &ArchiveKeys,
    database: &DatabaseName,
    engine: Arc<dyn DatabaseEngine>,
    target: &RestoreTarget,
    staging: &Path,
    execution: Execution,
) -> Result<Restored, RestoreError> {
    if let EngineCapability::SelfReplicating = engine.capability() {
        return Err(RestoreError::SelfReplicating(engine.kind().to_string()));
    }
    if staging.try_exists().map_err(RestoreError::Io)? {
        return Err(RestoreError::Io(io::Error::new(
            io::ErrorKind::AlreadyExists,
            format!("{} exists", staging.display()),
        )));
    }
    let listing = Listing::read(store, keys, database, engine.kind()).await?;
    let plan = plan(&listing, target)?;
    let built = build(store, keys, database, engine, &plan, staging, execution).await;
    if built.is_err() {
        let staging = staging.to_owned();
        let removed = blocking(execution, move || remove_dir_if_present(&staging)).await;
        if let Ok(Err(error)) = removed {
            tracing::warn!(%error, "can't remove a failed restore's staging directory");
        }
    }
    built
}

/// Builds the copy `plan` describes in `staging`.
async fn build(
    store: &dyn ObjectStore,
    keys: &ArchiveKeys,
    database: &DatabaseName,
    engine: Arc<dyn DatabaseEngine>,
    plan: &RestorePlan,
    staging: &Path,
    execution: Execution,
) -> Result<Restored, RestoreError> {
    unpack_snapshot(store, keys, plan, staging, execution).await?;
    let follower = {
        let engine = Arc::clone(&engine);
        let staging = staging.to_owned();
        blocking(execution, move || match engine.capability() {
            EngineCapability::FrameSource(frames) => frames.open_follower(&staging),
            _ => Err(EngineError::NotFound),
        })
        .await
        .map_err(|reason| RestoreError::Io(io::Error::other(reason)))?
        .map_err(RestoreError::Engine)?
    };
    let snapshot = &plan.snapshot;
    // A copy pinned where the log continues from holds no log of its own yet.
    let opened_at = follower.position();
    if opened_at != snapshot.position
        && !(opened_at.is_none() && follower.log_start() == snapshot.position)
    {
        return Err(data_loss(
            &snapshot.key,
            format!(
                "the snapshot's copy is at {}, not at {}",
                position_text(opened_at.as_ref()),
                position_text(snapshot.position.as_ref())
            ),
        ));
    }
    let mut follower = follower;
    let mut position = snapshot.position.clone();
    let mut chain = snapshot.chain;
    let mut archived_at_ms = None;
    let last = plan.segments.len().saturating_sub(1);
    for (index, segment) in plan.segments.iter().enumerate() {
        let cut = plan.cut.as_ref().filter(|_| index == last);
        let (returned, end_chain) = apply_segment(
            store,
            database,
            &engine,
            follower,
            segment,
            (position.as_ref(), chain),
            cut,
            execution,
        )
        .await?;
        follower = returned;
        position = Some(cut.cloned().unwrap_or_else(|| segment.name.end.clone()));
        chain = end_chain;
        archived_at_ms = Some(segment.name.archived_at_ms);
    }
    drop(follower);
    Ok(Restored {
        position,
        chain,
        snapshot: snapshot.id.clone(),
        segments: u32::try_from(plan.segments.len()).unwrap_or(u32::MAX),
        archived_at_ms,
    })
}

/// Downloads the snapshot's chunks in order, each checked against its checksum, into an
/// unpacker in `staging`, and checks the stream's own checksum at the end.
async fn unpack_snapshot(
    store: &dyn ObjectStore,
    keys: &ArchiveKeys,
    plan: &RestorePlan,
    staging: &Path,
    execution: Execution,
) -> Result<(), RestoreError> {
    let snapshot = &plan.snapshot;
    let descriptor = &snapshot.descriptor;
    let wanted: Vec<String> = (0..descriptor.chunks)
        .map(|index| keys.chunk(&snapshot.id, index))
        .collect();
    if let Some(extra) = snapshot.chunks.iter().find(|key| !wanted.contains(key)) {
        return Err(data_loss(
            extra,
            format!(
                "the snapshot's descriptor lists {} chunks, not this one",
                descriptor.chunks
            ),
        ));
    }
    let checksum = unhex(&descriptor.checksum)
        .map_err(|_| data_loss(&snapshot.key, "its checksum isn't hex"))?;
    let mut unpacker = {
        let staging = staging.to_owned();
        blocking(execution, move || Unpacker::create(&staging))
            .await
            .map_err(|reason| RestoreError::Io(io::Error::other(reason)))?
            .map_err(RestoreError::Io)?
    };
    for (key, expected) in wanted.iter().zip(&descriptor.chunk_checksums) {
        let data = retrying(|| store.get(key))
            .await?
            .ok_or_else(|| data_loss(key, "the chunk is missing"))?;
        if checksum_hex(&data) != *expected {
            return Err(data_loss(key, "the chunk doesn't match its checksum"));
        }
        let (returned, written) = blocking(execution, move || {
            let written = unpacker.write(&data);
            (unpacker, written)
        })
        .await
        .map_err(|reason| RestoreError::Io(io::Error::other(reason)))?;
        unpacker = returned;
        written.map_err(|error| stream_error(&snapshot.key, error))?;
    }
    if unpacker.offset() != descriptor.bytes {
        return Err(data_loss(
            &snapshot.key,
            format!(
                "its chunks hold {} bytes, not {}",
                unpacker.offset(),
                descriptor.bytes
            ),
        ));
    }
    blocking(execution, move || unpacker.finish(&checksum))
        .await
        .map_err(|reason| RestoreError::Io(io::Error::other(reason)))?
        .map_err(|error| stream_error(&snapshot.key, error))
}

fn stream_error(object: &str, error: io::Error) -> RestoreError {
    if error.kind() == io::ErrorKind::InvalidData {
        data_loss(object, error.to_string())
    } else {
        RestoreError::Io(error)
    }
}

/// Downloads, checks, and applies one segment to `follower`, which is at `from` with the hash
/// chain at it, and cuts the copy back to `cut` if given. Returns the follower and the chain
/// where the copy now is.
#[expect(
    clippy::too_many_arguments,
    reason = "each is one piece of the walk's state; a struct would only rename them"
)]
async fn apply_segment(
    store: &dyn ObjectStore,
    database: &DatabaseName,
    engine: &Arc<dyn DatabaseEngine>,
    follower: Box<dyn FrameFollower>,
    segment: &SegmentEntry,
    (from, chain): (Option<&Position>, ChainHash),
    cut: Option<&Position>,
    execution: Execution,
) -> Result<(Box<dyn FrameFollower>, ChainHash), RestoreError> {
    let key = &segment.key;
    let bytes = retrying(|| store.get(key))
        .await?
        .ok_or_else(|| data_loss(key, "the segment is missing"))?;
    let (header, payload) =
        decode_segment(&bytes).map_err(|error| data_loss(key, error.to_string()))?;
    check_header(&header, segment, database, engine.kind().to_string())
        .map_err(|reason| data_loss(key, reason))?;
    if header.chain_start.parse::<ChainHash>().ok() != Some(chain) {
        return Err(data_loss(
            key,
            format!(
                "it continues the chain from {}, but the copy is at {chain}",
                header.chain_start
            ),
        ));
    }
    let batch = FrameBatch {
        start: from.cloned().unwrap_or_default(),
        end: segment.name.end.clone(),
        payload: payload.to_vec().into(),
    };
    let EngineCapability::FrameSource(frames) = engine.capability() else {
        return Err(RestoreError::SelfReplicating(engine.kind().to_string()));
    };
    let hashes = ChainHash::chain_each(frames, chain, from, &batch)
        .map_err(|error| data_loss(key, format!("its batch can't be hashed: {error}")))?;
    let end_chain = hashes.last().map_or(chain, |(_, hash)| *hash);
    if header.chain_end.parse::<ChainHash>().ok() != Some(end_chain) {
        return Err(data_loss(
            key,
            format!(
                "its batch hashes to {end_chain}, not to the {} it recorded",
                header.chain_end
            ),
        ));
    }
    let cut_chain = match cut {
        Some(cut) => {
            let Some((_, hash)) = hashes.iter().find(|(end, _)| end == cut) else {
                let before = hashes
                    .iter()
                    .map(|(end, _)| end)
                    .filter(|end| *end < cut)
                    .max()
                    .cloned()
                    .or_else(|| from.cloned());
                let after = hashes
                    .iter()
                    .map(|(end, _)| end)
                    .find(|end| *end > cut)
                    .cloned();
                return Err(RestoreError::NotBoundary {
                    target: cut.clone(),
                    before,
                    after,
                });
            };
            Some(*hash)
        }
        None => None,
    };
    let end = segment.name.end.clone();
    let cut_at = cut.cloned();
    let (follower, applied) = blocking(execution, move || {
        let mut follower = follower;
        let applied = follower.apply(&batch).and_then(|reached| {
            if reached != end {
                return Ok(Err(reached));
            }
            match &cut_at {
                Some(cut) => follower.truncate_to(cut).map(Ok),
                None => Ok(Ok(())),
            }
        });
        (follower, applied)
    })
    .await
    .map_err(|reason| RestoreError::Io(io::Error::other(reason)))?;
    match applied {
        Ok(Ok(())) => Ok((follower, cut_chain.unwrap_or(end_chain))),
        Ok(Err(reached)) => Err(data_loss(
            key,
            format!("applying it brought the copy to {reached}, not to its end"),
        )),
        Err(EngineError::InvalidPayload(error)) => Err(data_loss(
            key,
            format!("the engine refused its batch: {error}"),
        )),
        Err(error) => Err(RestoreError::Engine(error)),
    }
}

/// Checks that a segment's header says what its name and the restore expect.
fn check_header(
    header: &SegmentHeader,
    segment: &SegmentEntry,
    database: &DatabaseName,
    engine: String,
) -> Result<(), String> {
    let name = &segment.name;
    let start = name.start.as_ref().map(|start| hex(start.as_bytes()));
    if header.database != database.as_str() {
        return Err(format!("it holds database `{}`", header.database));
    }
    if header.engine != engine {
        return Err(format!(
            "it holds a {} batch, not a {engine} one",
            header.engine
        ));
    }
    if header.start != start
        || header.end != hex(name.end.as_bytes())
        || header.epoch != name.epoch.get()
        || header.archived_at_ms != name.archived_at_ms
    {
        return Err("its header doesn't match its name".to_owned());
    }
    Ok(())
}

/// Opens a restored copy at `location` once as its primary would, and checkpoints it, so that
/// the directory holds a database its engine's own tools open directly, with no fleet: what an
/// offline restore leaves for a disaster recovery.
///
/// # Errors
///
/// [`EngineError`] if the copy can't be opened, checkpointed, or closed.
pub fn finish_offline(engine: &dyn DatabaseEngine, location: &Path) -> Result<(), EngineError> {
    let mut database = engine.open(location, OpenMode::Existing)?;
    let checkpointed = match database.capability() {
        DatabaseCapability::FrameSource(frames) => frames.checkpoint().map(drop),
        _ => Ok(()),
    };
    let closed = database.close();
    checkpointed.and(closed)
}
