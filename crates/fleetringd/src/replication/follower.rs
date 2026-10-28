//! The follower side: the `Replication` service that applies a primary's batches, and the
//! `Snapshot` RPC a new follower fetches its first copy through.

use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use fleetring::NodeId;
use fleetring_control::DatabaseName;
use fleetring_proto::v1::replicate_message::Message;
use fleetring_proto::v1::replication_server::Replication;
use fleetring_proto::v1::{
    FenceRequest, FenceResponse, ReplicateAck, ReplicateMessage, SnapshotChunk, SnapshotRequest,
};
use tokio::sync::{mpsc, watch};
use tokio_stream::Stream;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};

use super::epochs::{EpochError, sync_dir};
use super::primary::held;
use super::{Inner, PRIMARY_METADATA_KEY, archive};
use crate::engine::{
    CheckpointMarker, EngineCapability, EngineError, Execution, FrameBatch, Payload, Position,
};
use crate::transport::replication_client;

/// The largest snapshot chunk the `Snapshot` RPC sends.
const CHUNK_BYTES: usize = 1024 * 1024;

type BoxStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send>>;

/// The `Replication` gRPC service of one data node: it follows the databases primaries stream to
/// it, and serves snapshots of the databases it is primary for.
#[derive(Clone)]
pub struct ReplicationService {
    inner: Arc<Inner>,
}

impl fmt::Debug for ReplicationService {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReplicationService")
            .field("node", &self.inner.node)
            .finish_non_exhaustive()
    }
}

impl ReplicationService {
    pub(super) fn new(inner: Arc<Inner>) -> Self {
        Self { inner }
    }
}

/// One database this node follows: which stream owns it, and where its copy is.
#[derive(Debug, Default)]
pub(super) struct FollowerSlot {
    /// Counts the streams that have opened; only the latest may apply.
    sessions: watch::Sender<u64>,
    /// Held by the stream applying batches, so that a newer stream waits for an older one.
    applying: tokio::sync::Mutex<()>,
    positions: Mutex<Positions>,
}

#[derive(Debug, Default, Clone)]
struct Positions {
    position: Option<Position>,
    checkpointed: Option<Position>,
}

impl FollowerSlot {
    /// Starts a new session, which ends every older one.
    fn begin(&self) -> u64 {
        let mut session = 0;
        self.sessions.send_modify(|current| {
            *current += 1;
            session = *current;
        });
        session
    }

    fn is_current(&self, session: u64) -> bool {
        *self.sessions.borrow() == session
    }

    /// The copy's position and last checkpoint, once a stream has opened it.
    pub(super) fn positions(&self) -> (Option<Position>, Option<Position>) {
        let positions = self
            .positions
            .lock()
            .expect("follower lock poisoned")
            .clone();
        (positions.position, positions.checkpointed)
    }

    fn set_position(&self, position: Option<Position>) {
        self.positions
            .lock()
            .expect("follower lock poisoned")
            .position = position;
    }

    fn set_checkpointed(&self, position: Position) {
        self.positions
            .lock()
            .expect("follower lock poisoned")
            .checkpointed = Some(position);
    }
}

#[tonic::async_trait]
impl Replication for ReplicationService {
    type ReplicateStream = BoxStream<ReplicateAck>;
    type SnapshotStream = BoxStream<SnapshotChunk>;

    async fn replicate(
        &self,
        request: Request<Streaming<ReplicateMessage>>,
    ) -> Result<Response<Self::ReplicateStream>, Status> {
        let primary: Option<NodeId> = request
            .metadata()
            .get(PRIMARY_METADATA_KEY)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse().ok());
        let mut inbound = request.into_inner();
        let open = match inbound.message().await? {
            Some(ReplicateMessage {
                message: Some(Message::Open(open)),
            }) => open,
            _ => {
                return Err(Status::invalid_argument(
                    "a replication stream starts with an open message",
                ));
            }
        };
        let name: DatabaseName = open
            .database
            .parse()
            .map_err(|error| Status::invalid_argument(format!("invalid database name: {error}")))?;
        let inner = &self.inner;
        if let EngineCapability::SelfReplicating = inner.engine.capability() {
            return Err(Status::failed_precondition(format!(
                "{} databases replicate themselves",
                inner.engine.kind()
            )));
        }
        admit(inner, &name, open.epoch).await?;
        if inner.primary(&name).is_some() || inner.registry().is_open(&name) {
            return Err(Status::failed_precondition(format!(
                "{name} is open as a primary on this node"
            )));
        }
        let slot = inner.slot(&name);
        let session = slot.begin();
        let (acks, replies) = mpsc::channel(inner.settings.max_in_flight.get() + 1);
        let stream = Follow {
            inner: Arc::clone(inner),
            slot,
            session,
            name,
            epoch: open.epoch,
            primary,
        };
        tokio::spawn(async move {
            if let Err(status) = stream.run(inbound, &acks).await {
                tracing::debug!(database = %stream.name, %status, "replication stream ended");
                let _ = acks.send(Err(status)).await;
            }
        });
        Ok(Response::new(Box::pin(ReceiverStream::new(replies))))
    }

    async fn fence(
        &self,
        _request: Request<FenceRequest>,
    ) -> Result<Response<FenceResponse>, Status> {
        Err(Status::unimplemented("fencing arrives with failover"))
    }

    async fn snapshot(
        &self,
        request: Request<SnapshotRequest>,
    ) -> Result<Response<Self::SnapshotStream>, Status> {
        let request = request.into_inner();
        let name: DatabaseName = request
            .database
            .parse()
            .map_err(|error| Status::invalid_argument(format!("invalid database name: {error}")))?;
        let inner = &self.inner;
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
        let staging = inner.staging("outgoing");
        let handle = inner
            .registry()
            .open(&name)
            .await
            .map_err(|error| Status::unavailable(error.to_string()))?;
        let snapshot = handle.snapshot(&staging).await;
        let packed = {
            let staging = staging.clone();
            blocking(inner.execution, move || archive::pack(&staging)).await
        };
        let removal = {
            let staging = staging.clone();
            blocking(inner.execution, move || fs::remove_dir_all(&staging)).await
        };
        if let Ok(Err(error)) = removal {
            tracing::warn!(dir = %staging.display(), %error, "can't remove a snapshot's staging directory");
        }
        let snapshot = snapshot.map_err(|error| Status::internal(error.to_string()))?;
        let archive = packed?.map_err(|error| Status::internal(error.to_string()))?;
        let position = snapshot
            .position
            .map(Position::into_bytes)
            .unwrap_or_default();
        Ok(Response::new(Box::pin(tokio_stream::iter(
            chunks(archive, position).into_iter().map(Ok),
        ))))
    }
}

/// Splits `archive` into chunks; the last carries the snapshot's position.
fn chunks(archive: Vec<u8>, position: Vec<u8>) -> Vec<SnapshotChunk> {
    let mut chunks: Vec<SnapshotChunk> = archive
        .chunks(CHUNK_BYTES)
        .scan(0_u64, |offset, data| {
            let chunk = SnapshotChunk {
                offset: *offset,
                data: data.to_vec(),
                position: None,
                checksum: None,
            };
            *offset += u64::try_from(data.len()).unwrap_or(u64::MAX);
            Some(chunk)
        })
        .collect();
    match chunks.last_mut() {
        Some(last) => last.position = Some(position),
        None => chunks.push(SnapshotChunk {
            offset: 0,
            data: Vec::new(),
            position: Some(position),
            checksum: None,
        }),
    }
    chunks
}

/// One open `Replicate` stream on the follower.
struct Follow {
    inner: Arc<Inner>,
    slot: Arc<FollowerSlot>,
    session: u64,
    name: DatabaseName,
    epoch: u64,
    primary: Option<NodeId>,
}

type Acks = mpsc::Sender<Result<ReplicateAck, Status>>;

impl Follow {
    async fn run(
        &self,
        mut inbound: Streaming<ReplicateMessage>,
        acks: &Acks,
    ) -> Result<(), Status> {
        let inner = &self.inner;
        let _applying = self.slot.applying.lock().await;
        if !self.slot.is_current(self.session) {
            return Err(Status::aborted("a newer stream replaced this one"));
        }
        let location = inner.registry().location(&self.name);
        let exists = {
            let location = location.clone();
            blocking(inner.execution, move || location.try_exists()).await?
        }
        .map_err(|error| Status::internal(error.to_string()))?;
        if !exists {
            tokio::time::timeout(inner.settings.open_timeout, self.bootstrap(&location))
                .await
                .map_err(|_| Status::deadline_exceeded("fetching the initial copy timed out"))??;
        }
        let mut follower = {
            let engine = Arc::clone(&inner.engine);
            let location = location.clone();
            blocking(inner.execution, move || match engine.capability() {
                EngineCapability::FrameSource(frames) => frames.open_follower(&location),
                _ => Err(EngineError::NotFound),
            })
            .await?
            .map_err(engine_status)?
        };
        let mut position = follower.position();
        self.slot.set_position(position.clone());
        if !send_ack(acks, position.as_ref()).await {
            return Ok(());
        }
        let mut sessions = self.slot.sessions.subscribe();
        loop {
            let message = tokio::select! {
                _ = sessions.changed() => return Err(Status::aborted("a newer stream replaced this one")),
                message = inbound.message() => message?,
            };
            let Some(ReplicateMessage {
                message: Some(message),
            }) = message
            else {
                return match message {
                    None => Ok(()),
                    Some(_) => Err(Status::invalid_argument("empty replication message")),
                };
            };
            match message {
                Message::Batch(batch) => {
                    admit(inner, &self.name, batch.epoch).await?;
                    if held(batch.start.clone()) != position {
                        return Err(Status::failed_precondition(format!(
                            "batch starts at {}, but the copy is at {}",
                            Position::from(batch.start),
                            position.unwrap_or_default()
                        )));
                    }
                    let frames = FrameBatch {
                        start: Position::from(batch.start),
                        end: Position::from(batch.end),
                        payload: Payload::from(batch.payload),
                    };
                    let (returned, applied) = blocking(inner.execution, move || {
                        let applied = follower.apply(&frames);
                        (follower, applied)
                    })
                    .await?;
                    follower = returned;
                    position = Some(applied.map_err(engine_status)?);
                    self.slot.set_position(position.clone());
                    if !send_ack(acks, position.as_ref()).await {
                        return Ok(());
                    }
                }
                Message::Checkpoint(marker) => {
                    admit(inner, &self.name, marker.epoch).await?;
                    let marker = CheckpointMarker {
                        position: Position::from(marker.position),
                    };
                    if position.as_ref() != Some(&marker.position) {
                        tracing::debug!(database = %self.name, at = ?position, marker = %marker.position, "skipping a checkpoint marker the copy isn't at");
                        continue;
                    }
                    let (returned, checkpointed) = blocking(inner.execution, move || {
                        let checkpointed = follower.checkpoint(&marker).map(|()| marker.position);
                        (follower, checkpointed)
                    })
                    .await?;
                    follower = returned;
                    self.slot
                        .set_checkpointed(checkpointed.map_err(engine_status)?);
                }
                Message::Open(_) => {
                    return Err(Status::invalid_argument("a replication stream opens once"));
                }
            }
        }
    }

    /// Fetches the database's first copy from the primary and moves it to `location`.
    async fn bootstrap(&self, location: &Path) -> Result<(), Status> {
        let inner = &self.inner;
        let primary = self.primary.as_ref().ok_or_else(|| {
            Status::failed_precondition(format!(
                "no copy of {} here, and the stream names no primary to copy it from",
                self.name
            ))
        })?;
        let address = inner
            .addresses
            .address(primary)
            .ok_or_else(|| Status::unavailable(format!("no address for {primary}")))?;
        let channel = inner
            .peers
            .channel_to(&address)
            .map_err(|error| Status::unavailable(error.to_string()))?;
        let mut chunks = replication_client(channel)
            .snapshot(SnapshotRequest {
                database: self.name.to_string(),
                epoch: self.epoch,
            })
            .await?
            .into_inner();
        let mut archive = Vec::new();
        let mut complete = false;
        while let Some(chunk) = chunks.message().await? {
            if chunk.offset != u64::try_from(archive.len()).unwrap_or(u64::MAX) {
                return Err(Status::data_loss("snapshot chunks arrived out of order"));
            }
            archive.extend_from_slice(&chunk.data);
            if chunk.position.is_some() {
                complete = true;
                break;
            }
        }
        if !complete {
            return Err(Status::data_loss("the snapshot ended early"));
        }
        let staging = inner.staging("incoming");
        let location = location.to_owned();
        blocking(inner.execution, move || {
            install(&archive, &staging, &location)
        })
        .await?
        .map_err(|error| Status::internal(format!("can't install the initial copy: {error}")))?;
        tracing::info!(database = %self.name, from = %primary, "installed the initial copy");
        Ok(())
    }
}

/// Unpacks `archive` into `staging`, then renames it to `location`, durably.
fn install(archive: &[u8], staging: &Path, location: &Path) -> std::io::Result<()> {
    if let Some(root) = staging.parent() {
        fs::create_dir_all(root)?;
    }
    archive::unpack(archive, staging)?;
    let parent: PathBuf = location
        .parent()
        .map_or_else(PathBuf::new, Path::to_path_buf);
    fs::create_dir_all(&parent)?;
    fs::rename(staging, location)?;
    sync_dir(&parent)
}

/// Admits `epoch` for `database`, recording it durably if it is new.
async fn admit(inner: &Arc<Inner>, database: &DatabaseName, epoch: u64) -> Result<(), Status> {
    if inner.epochs.highest(database) == Some(epoch) {
        return Ok(());
    }
    let admitted = {
        let inner = Arc::clone(inner);
        let database = database.clone();
        blocking(inner.execution, move || {
            inner.epochs.admit(&database, epoch)
        })
        .await?
    };
    admitted.map_err(|error| match error {
        EpochError::Stale { .. } => Status::failed_precondition(error.to_string()),
        EpochError::Io(_) => Status::internal(error.to_string()),
    })
}

async fn send_ack(acks: &Acks, position: Option<&Position>) -> bool {
    let ack = ReplicateAck {
        position: position.map(|p| p.as_bytes().to_vec()).unwrap_or_default(),
    };
    acks.send(Ok(ack)).await.is_ok()
}

/// Runs `call` where blocking calls run: on a blocking thread, or inline under simulation.
async fn blocking<T: Send + 'static>(
    execution: Execution,
    call: impl FnOnce() -> T + Send + 'static,
) -> Result<T, Status> {
    match execution {
        Execution::Inline => Ok(call()),
        Execution::Threads => tokio::task::spawn_blocking(call)
            .await
            .map_err(|error| Status::internal(format!("blocking call failed: {error}"))),
    }
}

fn engine_status(error: EngineError) -> Status {
    match error {
        EngineError::InvalidPayload(_) | EngineError::InvalidPosition { .. } => {
            Status::failed_precondition(error.to_string())
        }
        EngineError::NotFound => Status::not_found(error.to_string()),
        _ => Status::internal(error.to_string()),
    }
}
