//! Snapshot catch-up (ADR-0016, point 8): the primary's `Snapshot` RPC, and the follower's
//! transfer that seeds or re-seeds its copy from it.
//!
//! The primary pins a consistent copy through the engine
//! ([`FrameSource::pin_snapshot`](crate::engine::FrameSource::pin_snapshot)), which takes one
//! quick call on the database's thread, and streams the copy's files from a task of its own, so
//! writes go on while it does. The follower writes the stream into a staging directory as it
//! arrives, verifies the checksum at the end, and only then swaps the directory into the
//! database's place. A failed or interrupted transfer leaves the old copy, or none, in place, and
//! its staging directory is removed; the next attempt restarts from offset 0.

use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;

use fleetring::NodeId;
use fleetring_control::DatabaseName;
use fleetring_proto::v1::{SnapshotChunk, SnapshotRequest};
use tokio::sync::mpsc;
use tokio::time;
use tokio_stream::Stream;
use tokio_stream::wrappers::ReceiverStream;
use tonic::Status;

use super::archive::{Packer, Unpacker, remove_dir_if_present};
use super::epochs::sync_dir;
use super::failover;
use super::follower::{admit, blocking};
use super::primary::held;
use super::{Inner, SeedError};
use crate::engine::{EngineCapability, Execution, Position};
use crate::transport::replication_client;

/// The largest snapshot chunk the `Snapshot` RPC sends.
const CHUNK_BYTES: usize = 1024 * 1024;

/// Staging directory kinds under the registry's root. Database directories never contain `.`,
/// so these never collide with one.
const INCOMING: &str = "incoming";
const RETIRED: &str = "retired";

pub(super) type ChunkStream = Pin<Box<dyn Stream<Item = Result<SnapshotChunk, Status>> + Send>>;

/// Serves a `Snapshot` request: pins a copy of the database this node is primary for, and
/// streams it from a task of its own.
pub(super) async fn serve(
    inner: &Arc<Inner>,
    request: SnapshotRequest,
) -> Result<ChunkStream, Status> {
    let name: DatabaseName = request
        .database
        .parse()
        .map_err(|error| Status::invalid_argument(format!("invalid database name: {error}")))?;
    if let EngineCapability::SelfReplicating = inner.engine.capability() {
        return Err(Status::failed_precondition(format!(
            "{} databases replicate themselves and serve no snapshots",
            inner.engine.kind()
        )));
    }
    let primary = inner.primary(&name).ok_or_else(|| {
        Status::failed_precondition(format!("{name} has no primary on this node"))
    })?;
    let epoch = primary.role().epoch.get();
    if request.epoch != epoch {
        return Err(Status::failed_precondition(format!(
            "{name} is at epoch {epoch} here, not {}",
            request.epoch
        )));
    }
    let handle = inner
        .registry()
        .open(&name)
        .await
        .map_err(|error| Status::unavailable(error.to_string()))?;
    let pinned = handle
        .pin_snapshot()
        .await
        .map_err(|error| Status::unavailable(format!("can't pin a snapshot: {error}")))?;
    let packer = Packer::new(pinned).map_err(|error| Status::internal(error.to_string()))?;
    let (chunks, stream) = mpsc::channel(2);
    tokio::spawn(send_chunks(
        packer,
        inner.execution,
        inner.settings.open_timeout,
        chunks,
    ));
    Ok(Box::pin(ReceiverStream::new(stream)))
}

/// Streams `packer`'s chunks, then a last chunk with the position and checksum. Gives up, and so
/// releases the pinned copy, once the receiver goes away or takes longer than `limit` to accept
/// a chunk.
async fn send_chunks(
    mut packer: Packer,
    execution: Execution,
    limit: std::time::Duration,
    chunks: mpsc::Sender<Result<SnapshotChunk, Status>>,
) {
    loop {
        let offset = packer.offset();
        let read = blocking(execution, move || {
            let next = packer.next_chunk(CHUNK_BYTES);
            (packer, next)
        })
        .await;
        let next = match read {
            Ok((returned, next)) => {
                packer = returned;
                next
            }
            Err(status) => {
                let _ = time::timeout(limit, chunks.send(Err(status))).await;
                return;
            }
        };
        let chunk = match next {
            Ok(Some(data)) => SnapshotChunk {
                offset,
                data,
                position: None,
                checksum: None,
            },
            Ok(None) => match packer.finish() {
                Ok(checksum) => SnapshotChunk {
                    offset,
                    data: Vec::new(),
                    position: Some(
                        packer
                            .position()
                            .map(|position| position.as_bytes().to_vec())
                            .unwrap_or_default(),
                    ),
                    checksum: Some(checksum.to_vec()),
                },
                Err(error) => {
                    let status = Status::aborted(format!("the snapshot changed: {error}"));
                    let _ = time::timeout(limit, chunks.send(Err(status))).await;
                    return;
                }
            },
            Err(error) => {
                let status = Status::internal(format!("can't read the snapshot: {error}"));
                let _ = time::timeout(limit, chunks.send(Err(status))).await;
                return;
            }
        };
        let last = chunk.position.is_some();
        if !matches!(
            time::timeout(limit, chunks.send(Ok(chunk))).await,
            Ok(Ok(()))
        ) || last
        {
            return;
        }
    }
}

/// Fetches a snapshot of `database` at `epoch` from `from`, its primary, and swaps it into
/// `location`, replacing any copy there. Returns the position the new copy is at.
///
/// Nothing partial is ever at `location`: the snapshot is verified in a staging directory
/// first, and removed if anything fails.
pub(super) async fn fetch(
    inner: &Arc<Inner>,
    database: &DatabaseName,
    epoch: u64,
    from: &NodeId,
    location: &Path,
) -> Result<Option<Position>, Status> {
    let staging = inner.staging(INCOMING);
    let received = receive(inner, database, epoch, from, &staging).await;
    let installed = match received {
        Ok(position) => {
            let aside = inner.staging(RETIRED);
            let (staging, location) = (staging.clone(), location.to_owned());
            blocking(inner.execution, move || {
                install(&staging, &location, &aside)
            })
            .await?
            .map(|()| position)
            .map_err(|error| Status::internal(format!("can't install the snapshot: {error}")))
        }
        Err(status) => Err(status),
    };
    // The copy is now the log of the primary at `epoch`, which it ranks by in promotions and
    // which keeps a later stream at that epoch from cutting it back.
    if installed.is_ok() {
        failover::set_log_epoch(inner, database, epoch).await?;
    }
    if installed.is_err() {
        let removed = {
            let staging = staging.clone();
            blocking(inner.execution, move || remove_dir_if_present(&staging)).await
        };
        if let Ok(Err(error)) = removed {
            tracing::warn!(dir = %staging.display(), %error, "can't remove a failed snapshot's staging directory");
        }
    }
    installed
}

/// Streams the snapshot into `staging` and verifies it.
async fn receive(
    inner: &Arc<Inner>,
    database: &DatabaseName,
    epoch: u64,
    from: &NodeId,
    staging: &Path,
) -> Result<Option<Position>, Status> {
    let address = inner
        .addresses
        .address(from)
        .ok_or_else(|| Status::unavailable(format!("no address for {from}")))?;
    let channel = inner
        .peers
        .channel_to(&address)
        .map_err(|error| Status::unavailable(error.to_string()))?;
    let mut chunks = replication_client(channel)
        .snapshot(SnapshotRequest {
            database: database.to_string(),
            epoch,
        })
        .await?
        .into_inner();
    let mut unpacker = {
        let staging = staging.to_owned();
        blocking(inner.execution, move || Unpacker::create(&staging))
            .await?
            .map_err(|error| Status::internal(format!("can't stage the snapshot: {error}")))?
    };
    while let Some(chunk) = chunks.message().await? {
        if chunk.offset != unpacker.offset() {
            return Err(Status::data_loss(format!(
                "a snapshot chunk starts at offset {}, after {} bytes",
                chunk.offset,
                unpacker.offset()
            )));
        }
        if let Some(position) = chunk.position {
            if !chunk.data.is_empty() {
                return Err(Status::data_loss("the snapshot's last chunk carries data"));
            }
            let checksum = chunk
                .checksum
                .ok_or_else(|| Status::data_loss("the snapshot's last chunk has no checksum"))?;
            blocking(inner.execution, move || unpacker.finish(&checksum))
                .await?
                .map_err(transfer_status)?;
            return Ok(held(position));
        }
        let data = chunk.data;
        let (returned, written) = blocking(inner.execution, move || {
            let written = unpacker.write(&data);
            (unpacker, written)
        })
        .await?;
        unpacker = returned;
        written.map_err(transfer_status)?;
    }
    Err(Status::data_loss("the snapshot ended early"))
}

fn transfer_status(error: io::Error) -> Status {
    if error.kind() == io::ErrorKind::InvalidData {
        Status::data_loss(format!("the snapshot is corrupt: {error}"))
    } else {
        Status::internal(format!("can't write the snapshot: {error}"))
    }
}

/// Moves the verified `staging` directory to `location`, durably. An existing copy is renamed to
/// `aside` first and removed afterwards, so `location` holds the old copy, nothing, or the new
/// copy, and never a mix.
fn install(staging: &Path, location: &Path, aside: &Path) -> io::Result<()> {
    let parent: PathBuf = location
        .parent()
        .map_or_else(PathBuf::new, Path::to_path_buf);
    fs::create_dir_all(&parent)?;
    let replacing = location.try_exists()?;
    if replacing {
        fs::rename(location, aside)?;
    }
    fs::rename(staging, location)?;
    sync_dir(&parent)?;
    if let Some(root) = staging.parent()
        && root != parent
    {
        sync_dir(root)?;
    }
    if replacing && let Err(error) = remove_dir_if_present(aside) {
        tracing::warn!(dir = %aside.display(), %error, "can't remove a replaced copy");
    }
    Ok(())
}

/// Removes the staging directories that transfers interrupted by a crash left under `root`.
pub(super) fn sweep(root: &Path) {
    let entries = match fs::read_dir(root) {
        Ok(entries) => entries,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return,
        Err(error) => {
            tracing::warn!(dir = %root.display(), %error, "can't list staging directories");
            return;
        }
    };
    for entry in entries.flatten() {
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        let staged = [INCOMING, RETIRED, "outgoing"]
            .iter()
            .any(|kind| name.starts_with(&format!(".{kind}-")));
        if staged && let Err(error) = remove_dir_if_present(&entry.path()) {
            tracing::warn!(dir = %entry.path().display(), %error, "can't remove a staging directory");
        }
    }
}

/// Seeds this node's copy of `database` from `from`, as [`Replication::seed`] describes.
///
/// [`Replication::seed`]: super::Replication::seed
pub(super) async fn seed(
    inner: &Arc<Inner>,
    database: &DatabaseName,
    from: &NodeId,
    epoch: u64,
) -> Result<Option<Position>, SeedError> {
    if let EngineCapability::SelfReplicating = inner.engine.capability() {
        return Err(SeedError::SelfReplicating {
            engine: inner.engine.kind(),
        });
    }
    if inner.primary(database).is_some() || inner.registry().is_open(database) {
        return Err(SeedError::Primary {
            database: database.clone(),
        });
    }
    let failed = |status: Status| SeedError::Failed {
        database: database.clone(),
        reason: status.message().to_owned(),
    };
    admit(inner, database, epoch).await.map_err(failed)?;
    let slot = inner.slot(database);
    let _applying = slot.take_over().await;
    // A fence that went first may have recorded a newer epoch, and the copy it reported on must
    // not change under an older one.
    admit(inner, database, epoch).await.map_err(failed)?;
    let location = inner.registry().location(database);
    let position = time::timeout(
        inner.settings.open_timeout,
        fetch(inner, database, epoch, from, &location),
    )
    .await
    .map_err(|_| failed(Status::deadline_exceeded("the snapshot transfer timed out")))?
    .map_err(failed)?;
    slot.seeded(position.clone());
    tracing::info!(database = %database, %from, ?position, "seeded a copy from a snapshot");
    Ok(position)
}
