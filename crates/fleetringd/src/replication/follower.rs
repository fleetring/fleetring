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

use super::chain::{self, Anchor, Chain, ChainHash, Links, LogRead, Provenance};
use super::failover::{self, Reconciled};
use super::primary::{held, read_back_status, unreadable};
use super::snapshot::Seeded;
use super::{Inner, PRIMARY_METADATA_KEY, Verified, snapshot};
use crate::engine::{
    CheckpointMarker, EngineCapability, EngineError, Execution, FrameBatch, FrameFollower,
    FrameSourceEngine, Payload, Position,
};
use crate::tls::Peer;

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
    /// The hash chain over the copy, once a stream, a fetch, or a verification built it.
    chain: Mutex<Option<Chain>>,
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

    /// Reads the copy's chain, if it is built.
    pub(super) fn with_chain<T>(&self, read: impl FnOnce(Option<&Chain>) -> T) -> T {
        read(self.chain.lock().expect("follower lock poisoned").as_ref())
    }

    /// Changes the copy's chain, if it is built.
    fn update_chain<T>(&self, change: impl FnOnce(&mut Chain) -> T) -> Option<T> {
        self.chain
            .lock()
            .expect("follower lock poisoned")
            .as_mut()
            .map(change)
    }

    pub(super) fn set_chain(&self, chain: Option<Chain>) {
        *self.chain.lock().expect("follower lock poisoned") = chain;
    }
}

/// The engine as a frame source, which a followed database's engine is.
pub(super) fn frame_source(inner: &Inner) -> Result<&dyn FrameSourceEngine, Status> {
    match inner.engine.capability() {
        EngineCapability::FrameSource(engine) => Ok(engine),
        EngineCapability::SelfReplicating => Err(Status::failed_precondition(format!(
            "{} databases replicate themselves",
            inner.engine.kind()
        ))),
    }
}

/// Builds the chain over the copy `follower` opened: from its record and the log, as
/// [`chain::recover`] does, recording the anchor of a log that began empty here. Blocking.
fn recover_chain(
    engine: &dyn FrameSourceEngine,
    chains: &chain::Chains,
    database: &DatabaseName,
    follower: &dyn FrameFollower,
) -> Result<Chain, Status> {
    let recorded = match chains.load(database) {
        Ok(recorded) => recorded,
        Err(error) => {
            tracing::warn!(database = %database, %error, "can't read the chain's record; deriving one");
            None
        }
    };
    let position = follower.position();
    let anchored_at = recorded.as_ref().map(Anchor::position);
    let read = match anchored_at {
        Some(at) if at == position => LogRead::Continues(None),
        Some(at) => match follower.committed_since(at.as_ref()) {
            Ok(batch) => LogRead::Continues(batch),
            Err(EngineError::InvalidPosition { .. }) => {
                LogRead::Restarted(follower.committed_since(None).map_err(engine_status)?)
            }
            Err(error) => return Err(engine_status(error)),
        },
        None => LogRead::Restarted(follower.committed_since(None).map_err(engine_status)?),
    };
    let recovered = chain::recover(engine, recorded, position.as_ref(), read)
        .map_err(|error| Status::data_loss(format!("can't hash the log of {database}: {error}")))?;
    if let Some(anchor) = recovered.record
        && let Err(error) = chains.store(database, &anchor)
    {
        tracing::warn!(database = %database, %error, "can't record the chain's anchor");
    }
    let chain = recovered.chain;
    if chain.is_derived() {
        tracing::info!(database = %database, anchor = ?chain.anchor().position(), "no chain record fits the copy; the chain is derived from where its log begins");
    }
    Ok(chain)
}

/// The chain over the copy `follower` opened, from the slot if a stream built it, else rebuilt
/// and kept in the slot.
pub(super) async fn follower_chain(
    inner: &Arc<Inner>,
    database: &DatabaseName,
    slot: &FollowerSlot,
    follower: Box<dyn FrameFollower>,
) -> Result<(Box<dyn FrameFollower>, Chain), Status> {
    if let Some(chain) = slot.with_chain(|chain| chain.cloned()) {
        return Ok((follower, chain));
    }
    let (follower, chain) = {
        let inner = Arc::clone(inner);
        let database = database.clone();
        blocking(inner.execution, move || {
            let chain = frame_source(&inner)
                .and_then(|engine| recover_chain(engine, &inner.chains, &database, &*follower));
            (follower, chain)
        })
        .await?
    };
    let chain = chain?;
    slot.set_chain(Some(chain.clone()));
    Ok((follower, chain))
}

/// Records `anchor` as `database`'s chain anchor, durably. Failure costs a recomputation from
/// the log after a restart, so it is logged, not returned.
pub(super) async fn record_anchor(inner: &Arc<Inner>, database: &DatabaseName, anchor: Anchor) {
    let stored = {
        let chains = inner.chains.clone();
        let database = database.clone();
        blocking(inner.execution, move || chains.store(&database, &anchor)).await
    };
    match stored {
        Ok(Ok(())) => {}
        Ok(Err(error)) => {
            tracing::warn!(database = %database, %error, "can't record the chain's anchor");
        }
        Err(status) => {
            tracing::warn!(database = %database, %status, "can't record the chain's anchor");
        }
    }
}

/// Starts the copy's chain from the snapshot it was seeded with: at the snapshot's position,
/// from the primary's hash there, or derived if the primary sent none.
pub(super) async fn seeded_chain(
    inner: &Arc<Inner>,
    database: &DatabaseName,
    slot: &FollowerSlot,
    seeded: &Seeded,
) {
    let anchor = match seeded.chain {
        Some(hash) => Anchor::new(seeded.position.as_ref(), hash, Provenance::Recorded),
        None => Anchor::new(
            seeded.position.as_ref(),
            ChainHash::derived(seeded.position.as_ref()),
            Provenance::Derived,
        ),
    };
    slot.set_chain(Some(Chain::new(anchor.clone())));
    record_anchor(inner, database, anchor).await;
}

/// Reports the chain over this node's copy of `database` at `at`, or at the copy's end,
/// recomputed from the log on disk.
pub(super) async fn verify(
    inner: &Arc<Inner>,
    database: &DatabaseName,
    at: Option<Position>,
) -> Result<Verified, Status> {
    let engine = frame_source(inner)?;
    let slot = inner
        .slot_if_any(database)
        .ok_or_else(|| Status::not_found(format!("{database} has no copy on this node")))?;
    let location = inner.registry().location(database);
    let follower = {
        let engine = Arc::clone(&inner.engine);
        blocking(inner.execution, move || match engine.capability() {
            EngineCapability::FrameSource(frames) => frames.open_follower(&location),
            _ => Err(EngineError::NotFound),
        })
        .await?
        .map_err(engine_status)?
    };
    let (follower, chain) = follower_chain(inner, database, &slot, follower).await?;
    // The position the copy acknowledged, rather than what its log reads back to now: a log
    // that lost its tail since must say so.
    let at = at
        .or_else(|| slot.positions().0)
        .or_else(|| follower.position());
    let anchor = chain.anchor().clone();
    let held = chain.hash_at(at.as_ref());
    let hash = match (&at, anchor.position()) {
        (Some(at), anchored) if Some(at) > anchored.as_ref() => {
            let at = at.clone();
            let (batch, at) = blocking(inner.execution, move || {
                let batch = follower.committed_between(anchored.as_ref(), &at);
                (batch, at)
            })
            .await?;
            let batch = batch.map_err(|error| {
                read_back_status(database, anchor.position().as_ref(), &at, Some(&error))
            })?;
            chain::recompute(engine, &anchor, batch.as_ref(), &at)
                .map_err(|error| unreadable(database, &at, &error.to_string()))?
        }
        _ => held.ok_or_else(|| {
            Status::not_found(format!(
                "{database} holds no chain hash at {}",
                at.as_ref()
                    .map_or_else(|| "the empty log".to_owned(), ToString::to_string)
            ))
        })?,
    };
    if let Some(held) = held
        && held != hash
    {
        tracing::warn!(database = %database, at = ?at, %held, recomputed = %hash, "the copy on disk no longer hashes to what the chain recorded");
    }
    Ok(Verified {
        epoch: fleetring_control::LeaseEpoch::new(inner.epochs.record(database).log_epoch()),
        position: at,
        hash,
    })
}

/// What applying a batch came to, chain check included.
enum Applied {
    /// The batch was applied, or the engine refused it, with the hashes of its transactions
    /// when the chain could compute them.
    Done(Result<Position, EngineError>, Option<Links>),
    /// The batch hashes to something other than the primary says, so it wasn't applied.
    Refused {
        computed: ChainHash,
        expected: ChainHash,
    },
}

#[tonic::async_trait]
impl Replication for ReplicationService {
    type ReplicateStream = BoxStream<ReplicateAck>;
    type SnapshotStream = BoxStream<SnapshotChunk>;

    async fn replicate(
        &self,
        request: Request<Streaming<ReplicateMessage>>,
    ) -> Result<Response<Self::ReplicateStream>, Status> {
        let peer = Peer::of(&request);
        peer.require_node("Replicate")?;
        let primary: Option<NodeId> = request
            .metadata()
            .get(PRIMARY_METADATA_KEY)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse().ok());
        if let Some(primary) = &primary {
            peer.check_node(primary, "Replicate")?;
        }
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
        Peer::of(&request).require_node("Fence")?;
        failover::fence_rpc(&self.inner, request.into_inner())
            .await
            .map(Response::new)
    }

    async fn fetch(
        &self,
        request: Request<FetchRequest>,
    ) -> Result<Response<FetchResponse>, Status> {
        Peer::of(&request).require_node("Fetch")?;
        failover::fetch_rpc(&self.inner, request.into_inner())
            .await
            .map(Response::new)
    }

    async fn promote(
        &self,
        request: Request<PromoteRequest>,
    ) -> Result<Response<PromoteResponse>, Status> {
        Peer::of(&request).require_node("Promote")?;
        failover::promote_rpc(&self.inner, request.into_inner())?;
        Ok(Response::new(PromoteResponse {}))
    }

    async fn snapshot(
        &self,
        request: Request<SnapshotRequest>,
    ) -> Result<Response<Self::SnapshotStream>, Status> {
        Peer::of(&request).require_node("Snapshot")?;
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
                let (follower, chain) = {
                    let inner = Arc::clone(inner);
                    let database = self.name.clone();
                    blocking(inner.execution, move || {
                        let chain = frame_source(&inner).and_then(|engine| {
                            recover_chain(engine, &inner.chains, &database, &*follower)
                        });
                        (follower, chain)
                    })
                    .await?
                };
                let chain = chain?;
                if chain.tip().0 != position.as_ref() {
                    return Err(Status::internal(format!(
                        "the chain of {} ends at {:?}, but the copy is at {position:?}",
                        self.name,
                        chain.tip().0
                    )));
                }
                self.slot.set_chain(Some(chain));
                (Some(follower), ack(position.as_ref()))
            }
            // The copy stays closed until the primary answers with `Reseed`.
            Reconciled::NeedsSnapshot => {
                self.slot.set_chain(None);
                (None, needs_snapshot(position.as_ref()))
            }
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
                    let expected = batch.chain.as_deref().and_then(ChainHash::from_bytes);
                    let (from, provenance) = self.slot.with_chain(|chain| {
                        chain.map_or((None, Provenance::Derived), |chain| {
                            (chain.hash_at(Some(&frames.start)), chain.provenance())
                        })
                    });
                    let (returned, applied) = {
                        let inner = Arc::clone(inner);
                        blocking(inner.execution, move || {
                            let mut open = open;
                            let applied =
                                apply(&inner, &mut *open, &frames, from, expected, provenance);
                            (open, applied)
                        })
                        .await?
                    };
                    let reply = match applied {
                        Applied::Done(Ok(end), links) => {
                            let log_start = returned.log_start();
                            follower = Some(returned);
                            position = Some(end);
                            failover::note_applied(inner, &self.name, self.epoch, &mut noted)
                                .await?;
                            self.slot.set_position(position.clone());
                            self.chain_applied(links, expected, position.as_ref(), provenance)
                                .await;
                            self.chain_restarted(log_start.as_ref()).await;
                            ack(position.as_ref())
                        }
                        // The batch's bytes hash to something else than the primary's chain
                        // says: this copy's log, or the batch, differs from the primary's, and
                        // only a snapshot brings it in line.
                        Applied::Refused { computed, expected } => {
                            tracing::warn!(database = %self.name, at = ?position, %computed, %expected, "the batch doesn't continue the copy's hash chain; the copy needs a snapshot");
                            drop(returned);
                            needs_snapshot(position.as_ref())
                        }
                        // The frames start at the copy's position but don't continue its log:
                        // the copy diverged from the primary's at or before that position, as a
                        // replica that missed an epoch can (ADR-0022). Only a snapshot brings it
                        // in line; it takes no frames until then.
                        Applied::Done(Err(EngineError::InvalidPayload(error)), _) => {
                            tracing::warn!(database = %self.name, at = ?position, %error, "the primary's frames don't continue the copy; it needs a snapshot");
                            drop(returned);
                            needs_snapshot(position.as_ref())
                        }
                        Applied::Done(Err(error), _) => return Err(engine_status(error)),
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
                    let checkpointed = checkpointed.map_err(engine_status)?;
                    if let Some(Some(anchor)) =
                        self.slot.update_chain(|chain| chain.rebase(&checkpointed))
                    {
                        record_anchor(inner, &self.name, anchor).await;
                    }
                    self.slot.set_checkpointed(checkpointed);
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
        let seeded = snapshot::fetch(inner, &self.name, self.epoch, primary, location).await?;
        self.slot.seeded(seeded.position.clone());
        seeded_chain(inner, &self.name, &self.slot, &seeded).await;
        tracing::info!(database = %self.name, from = %primary, "installed the initial copy");
        Ok(())
    }

    /// Moves the chain's anchor up to `log_start`, the position the copy's log now continues
    /// from, once it is past the anchor: the log before it can't be read back any more, so a
    /// restart must recompute from here.
    async fn chain_restarted(&self, log_start: Option<&Position>) {
        let Some(log_start) = log_start else {
            return;
        };
        let rebased = self.slot.update_chain(|chain| {
            if Some(log_start) > chain.anchor_position() {
                chain.rebase(log_start)
            } else {
                None
            }
        });
        if let Some(Some(anchor)) = rebased {
            record_anchor(&self.inner, &self.name, anchor).await;
        }
    }

    /// Carries the chain on past a batch that was applied: with `links`, the hashes the chain
    /// computed over it, or else with `expected`, the primary's hash at its end, which a derived
    /// chain adopts.
    async fn chain_applied(
        &self,
        links: Option<Links>,
        expected: Option<ChainHash>,
        end: Option<&Position>,
        provenance: Provenance,
    ) {
        let adopt = match (&links, expected) {
            (Some(links), Some(expected)) => (links.end() != expected).then_some(expected),
            (None, Some(expected)) => Some(expected),
            (_, None) => None,
        };
        match (adopt, links) {
            (Some(expected), _) => {
                if provenance == Provenance::Derived {
                    tracing::info!(database = %self.name, at = ?end, "taking the primary's chain hash for a derived chain");
                }
                let anchor = self.slot.update_chain(|chain| chain.adopt(end, expected));
                let anchor = match anchor {
                    Some(anchor) => anchor,
                    None => {
                        let anchor = Anchor::new(end, expected, Provenance::Recorded);
                        self.slot.set_chain(Some(Chain::new(anchor.clone())));
                        anchor
                    }
                };
                record_anchor(&self.inner, &self.name, anchor).await;
            }
            (None, Some(links)) => {
                self.slot.update_chain(|chain| chain.commit(links));
            }
            (None, None) => {}
        }
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
        let seeded = tokio::time::timeout(
            inner.settings.open_timeout,
            snapshot::fetch(inner, &self.name, self.epoch, primary, location),
        )
        .await
        .map_err(|_| Status::deadline_exceeded("re-seeding from a snapshot timed out"))??;
        let fetched = seeded.position.clone();
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
        seeded_chain(inner, &self.name, &self.slot, &seeded).await;
        tracing::info!(database = %self.name, from = %primary, ?position, "re-seeded the copy from a snapshot");
        Ok((follower, position))
    }
}

/// Hashes `frames` from `from`, the chain's hash at its start, checks the result against
/// `expected`, the primary's hash at its end, and applies the batch unless they differ on a
/// recorded chain. Blocking.
fn apply(
    inner: &Inner,
    open: &mut dyn FrameFollower,
    frames: &FrameBatch,
    from: Option<ChainHash>,
    expected: Option<ChainHash>,
    provenance: Provenance,
) -> Applied {
    let engine = match inner.engine.capability() {
        EngineCapability::FrameSource(engine) => engine,
        EngineCapability::SelfReplicating => return Applied::Done(open.apply(frames), None),
    };
    let links = match from.map(|from| Links::compute(engine, from, Some(&frames.start), frames)) {
        Some(Ok(links)) => Some(links),
        // A batch the engine can't delimit is one it can't apply either; let it say so.
        Some(Err(error)) => {
            tracing::debug!(%error, "can't hash the batch");
            None
        }
        None => None,
    };
    if let (Some(links), Some(expected)) = (&links, expected)
        && links.end() != expected
        && provenance == Provenance::Recorded
    {
        return Applied::Refused {
            computed: links.end(),
            expected,
        };
    }
    Applied::Done(open.apply(frames), links)
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
