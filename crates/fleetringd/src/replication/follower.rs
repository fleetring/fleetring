//! The follower side: the `Replication` service that applies a primary's batches, and the
//! `Snapshot` RPC a new follower fetches its first copy through.

use std::fmt;
use std::path::Path;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
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

use super::epochs::EpochError;
use super::primary::held;
use super::{Inner, PRIMARY_METADATA_KEY, snapshot};
use crate::engine::{
    CheckpointMarker, EngineCapability, EngineError, Execution, FrameBatch, FrameFollower, Payload,
    Position,
};

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
    pub(super) applying: tokio::sync::Mutex<()>,
    positions: Mutex<Positions>,
    /// How many snapshots have replaced the copy since this node started.
    snapshots: AtomicU64,
}

#[derive(Debug, Default, Clone)]
struct Positions {
    position: Option<Position>,
    checkpointed: Option<Position>,
}

impl FollowerSlot {
    /// Starts a new session, which ends every older one.
    pub(super) fn begin(&self) -> u64 {
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

    /// Records that a snapshot at `position` replaced the copy.
    pub(super) fn seeded(&self, position: Option<Position>) {
        *self.positions.lock().expect("follower lock poisoned") = Positions {
            position,
            checkpointed: None,
        };
        self.snapshots.fetch_add(1, Ordering::Relaxed);
    }

    /// How many snapshots have replaced the copy since this node started.
    pub(super) fn snapshots(&self) -> u64 {
        self.snapshots.load(Ordering::Relaxed)
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
        snapshot::serve(&self.inner, request.into_inner())
            .await
            .map(Response::new)
    }
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
                Message::Reseed(reseed) => {
                    admit(inner, &self.name, reseed.epoch).await?;
                    // The copy is replaced underneath the open follower, so close it first.
                    drop(follower);
                    let (reopened, at) = self.reseed(&location).await?;
                    follower = reopened;
                    position = at;
                    if !send_ack(acks, position.as_ref()).await {
                        return Ok(());
                    }
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
        let position = snapshot::fetch(inner, &self.name, self.epoch, primary, location).await?;
        self.slot.seeded(position);
        tracing::info!(database = %self.name, from = %primary, "installed the initial copy");
        Ok(())
    }

    /// Replaces the copy at `location` with a snapshot from the stream's primary, because the
    /// primary's log no longer holds what it needs, and reopens it.
    async fn reseed(
        &self,
        location: &Path,
    ) -> Result<(Box<dyn FrameFollower>, Option<Position>), Status> {
        let inner = &self.inner;
        let primary = self.primary.as_ref().ok_or_else(|| {
            Status::failed_precondition(format!(
                "the stream names no primary to re-seed {} from",
                self.name
            ))
        })?;
        let fetched = tokio::time::timeout(
            inner.settings.open_timeout,
            snapshot::fetch(inner, &self.name, self.epoch, primary, location),
        )
        .await
        .map_err(|_| Status::deadline_exceeded("re-seeding from a snapshot timed out"))??;
        let follower = {
            let engine = Arc::clone(&inner.engine);
            let location = location.to_owned();
            blocking(inner.execution, move || match engine.capability() {
                EngineCapability::FrameSource(frames) => frames.open_follower(&location),
                _ => Err(EngineError::NotFound),
            })
            .await?
            .map_err(engine_status)?
        };
        let position = follower.position();
        if position != fetched {
            return Err(Status::data_loss(format!(
                "the snapshot was at {}, but the copy opened at {}",
                fetched.unwrap_or_default(),
                position.clone().unwrap_or_default()
            )));
        }
        self.slot.seeded(position.clone());
        tracing::info!(database = %self.name, from = %primary, ?position, "re-seeded the copy from a snapshot");
        Ok((follower, position))
    }
}

/// Admits `epoch` for `database`, recording it durably if it is new.
pub(super) async fn admit(
    inner: &Arc<Inner>,
    database: &DatabaseName,
    epoch: u64,
) -> Result<(), Status> {
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
pub(super) async fn blocking<T: Send + 'static>(
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
