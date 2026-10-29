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
    FenceRequest, FenceResponse, FetchRequest, FetchResponse, PromoteRequest, PromoteResponse,
    ReplicateAck, ReplicateMessage, SnapshotChunk, SnapshotRequest,
};
use tokio::sync::{mpsc, watch};
use tokio_stream::Stream;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};

use super::failover::{self, Reconciled};
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
    /// Held by whatever changes the copy: the stream applying batches or re-seeding it, a fence,
    /// a fetch, or a seed. A newer stream waits for an older one.
    applying: Arc<tokio::sync::Mutex<()>>,
    /// Held by a caller taking the copy over, so that two never keep superseding each other.
    taking_over: tokio::sync::Mutex<()>,
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
    /// The lock held by whatever changes the copy.
    pub(super) fn applying(&self) -> &Arc<tokio::sync::Mutex<()>> {
        &self.applying
    }

    /// Starts a new session, which ends every older one.
    pub(super) fn begin(&self) -> u64 {
        let mut session = 0;
        self.sessions.send_modify(|current| {
            *current += 1;
            session = *current;
        });
        session
    }

    /// Ends every stream applying to the copy and returns the lock they held, for a caller that
    /// replaces the copy itself.
    ///
    /// Whatever begins a session while this waits goes ahead of it: a stream then finds itself
    /// superseded and ends, and a fence runs to completion first. A stream that began while this
    /// waits would otherwise take the lock and keep it, so this begins a session again whenever
    /// another one begins. Callers taking the copy over queue for their turn, so that two never
    /// supersede each other in turn forever.
    pub(super) async fn take_over(&self) -> tokio::sync::MutexGuard<'_, ()> {
        let _contending = self.taking_over.lock().await;
        let mut sessions = self.sessions.subscribe();
        loop {
            let session = self.begin();
            tokio::select! {
                biased;
                applying = self.applying.lock() => return applying,
                _ = sessions.wait_for(|current| *current != session) => {}
            }
        }
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

    pub(super) fn set_position(&self, position: Option<Position>) {
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
        failover::admit_stream(inner, &name, open.epoch, primary.as_ref()).await?;
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
            epoch_start: open.epoch_start.and_then(held),
            primary,
        };
        tokio::spawn(async move {
            match stream.run(inbound, &acks).await {
                Ok(()) => stream.inner.stream_ended(&stream.name),
                Err(status) => {
                    tracing::debug!(database = %stream.name, %status, "replication stream ended");
                    // A stream a newer one replaced says nothing about the database.
                    if status.code() != tonic::Code::Aborted {
                        stream.inner.stream_ended(&stream.name);
                    }
                    let _ = acks.send(Err(status)).await;
                }
            }
        });
        Ok(Response::new(Box::pin(ReceiverStream::new(replies))))
    }

    async fn fence(
        &self,
        request: Request<FenceRequest>,
    ) -> Result<Response<FenceResponse>, Status> {
        failover::fence_rpc(&self.inner, request.into_inner())
            .await
            .map(Response::new)
    }

    async fn fetch(
        &self,
        request: Request<FetchRequest>,
    ) -> Result<Response<FetchResponse>, Status> {
        failover::fetch_rpc(&self.inner, request.into_inner())
            .await
            .map(Response::new)
    }

    async fn promote(
        &self,
        request: Request<PromoteRequest>,
    ) -> Result<Response<PromoteResponse>, Status> {
        failover::promote_rpc(&self.inner, request.into_inner())?;
        Ok(Response::new(PromoteResponse {}))
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
    /// Where the epoch's log began, if its primary was promoted to it.
    epoch_start: Option<Position>,
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
        // Subscribed before the session check, so that a session begun at any point after the
        // check ends this stream once it starts reading batches.
        let mut sessions = self.slot.sessions.subscribe();
        let _applying = self.slot.applying().lock().await;
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
        let follower = {
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
        let reconciled = failover::reconcile(
            inner,
            &self.name,
            self.epoch,
            self.epoch_start.as_ref(),
            follower,
        )
        .await?;
        // `None` while the copy waits for the primary to re-seed it: it takes no frames until then.
        let (mut follower, first) = match reconciled {
            Reconciled::Follows(follower) => {
                position = follower.position();
                self.slot.set_position(position.clone());
                (Some(follower), ack(position.as_ref()))
            }
            // The copy stays closed until the primary answers with `Reseed`.
            Reconciled::NeedsSnapshot => (None, needs_snapshot(position.as_ref())),
        };
        if acks.send(Ok(first)).await.is_err() {
            return Ok(());
        }
        let mut noted = false;
        loop {
            let message = tokio::select! {
                biased;
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
                    let Some(open) = follower.take() else {
                        // Sent before the primary learned that the copy needs a snapshot.
                        continue;
                    };
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
                        let mut open = open;
                        let applied = open.apply(&frames);
                        (open, applied)
                    })
                    .await?;
                    let reply = match applied {
                        Ok(end) => {
                            follower = Some(returned);
                            position = Some(end);
                            failover::note_applied(inner, &self.name, self.epoch, &mut noted)
                                .await?;
                            self.slot.set_position(position.clone());
                            ack(position.as_ref())
                        }
                        // The frames start at the copy's position but don't continue its log:
                        // the copy diverged from the primary's at or before that position, as a
                        // replica that missed an epoch can (ADR-0022). Only a snapshot brings it
                        // in line; it takes no frames until then.
                        Err(EngineError::InvalidPayload(error)) => {
                            tracing::warn!(database = %self.name, at = ?position, %error, "the primary's frames don't continue the copy; it needs a snapshot");
                            drop(returned);
                            needs_snapshot(position.as_ref())
                        }
                        Err(error) => return Err(engine_status(error)),
                    };
                    if acks.send(Ok(reply)).await.is_err() {
                        return Ok(());
                    }
                }
                Message::Checkpoint(marker) => {
                    admit(inner, &self.name, marker.epoch).await?;
                    let Some(open) = follower.take() else {
                        continue;
                    };
                    let marker = CheckpointMarker {
                        position: Position::from(marker.position),
                    };
                    if position.as_ref() != Some(&marker.position) {
                        tracing::debug!(database = %self.name, at = ?position, marker = %marker.position, "skipping a checkpoint marker the copy isn't at");
                        follower = Some(open);
                        continue;
                    }
                    let (returned, checkpointed) = blocking(inner.execution, move || {
                        let mut open = open;
                        let checkpointed = open.checkpoint(&marker).map(|()| marker.position);
                        (open, checkpointed)
                    })
                    .await?;
                    follower = Some(returned);
                    self.slot
                        .set_checkpointed(checkpointed.map_err(engine_status)?);
                }
                Message::Reseed(reseed) => {
                    admit(inner, &self.name, reseed.epoch).await?;
                    // The copy is replaced underneath the open follower, so close it first. The
                    // apply lock stays held throughout, so no fence or fetch sees a half-swapped
                    // copy.
                    drop(follower.take());
                    let (reopened, at) = self.reseed(&location).await?;
                    follower = Some(reopened);
                    position = at;
                    noted = true;
                    if acks.send(Ok(ack(position.as_ref()))).await.is_err() {
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
    admitted.map_err(failover::epoch_status)
}

/// Acknowledges that the copy holds the log up to `position`.
fn ack(position: Option<&Position>) -> ReplicateAck {
    ReplicateAck {
        position: position.map(|p| p.as_bytes().to_vec()).unwrap_or_default(),
        needs_snapshot: false,
    }
}

/// Tells the primary that the copy, at `position`, can't follow its log and needs a snapshot.
fn needs_snapshot(position: Option<&Position>) -> ReplicateAck {
    ReplicateAck {
        needs_snapshot: true,
        ..ack(position)
    }
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

pub(super) fn engine_status(error: EngineError) -> Status {
    match error {
        EngineError::InvalidPayload(_) | EngineError::InvalidPosition { .. } => {
            Status::failed_precondition(error.to_string())
        }
        EngineError::NotFound => Status::not_found(error.to_string()),
        _ => Status::internal(error.to_string()),
    }
}
