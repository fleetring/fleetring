//! Epochs, fencing, and the replica's side of failover (ADR-0018).
//!
//! - **Fencing** ([`fence`]): a replica records a promotion's epoch and its candidate durably,
//!   in one step with reading its log's position, so that no batch from an older epoch lands
//!   after the position it reports. A primary at an older epoch steps down first.
//! - **Streams** ([`admit_stream`]): a follower accepts a stream only at the highest epoch it
//!   has seen, and only from the node recorded as that epoch's holder.
//! - **Divergent tails** ([`reconcile`]): a follower whose log came from an earlier epoch and runs
//!   past the position the current epoch began at truncates it back there before it takes the
//!   epoch's frames. One that can't be cut back there asks the primary to re-seed it from a
//!   snapshot instead.
//! - **Log epochs**: each replica records the epoch of the primary that wrote its last frames
//!   ([`note_applied`]), and promotions rank logs by it before their positions, since positions
//!   on logs written by different primaries don't compare.
//! - **Catching up** ([`fetch`], [`Replication::catch_up`]): a promotion's candidate fetches the
//!   frames it lacks from the most advanced fenced member.

use std::collections::BTreeMap;
use std::io;
use std::sync::Arc;
use std::time::Duration;

use fleetring::{NodeId, ReplicationFactor};
use fleetring_control::{DatabaseName, LeaseEpoch, Quorums};
use fleetring_proto::v1::{
    self as proto, FenceRequest, FenceResponse, FetchRequest, FetchResponse, PromoteRequest,
};
use tokio::sync::{OwnedMutexGuard, watch};
use tonic::{Code, Status};

use super::epochs::EpochError;
use super::follower::{FollowerSlot, blocking, engine_status};
use super::primary::held;
use super::{Inner, PrimaryRole, Replication, ServePrimaryError};
use crate::engine::{EngineCapability, EngineError, FrameBatch, FrameFollower, Payload, Position};
use crate::transport::replication_client;

/// A promotion the control node asked this node to carry out as its candidate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct PromotionRequest {
    /// The epoch the candidate will lead at.
    pub epoch: LeaseEpoch,
    /// The database's replication factor.
    pub replication_factor: ReplicationFactor,
}

/// What a fenced replica reported.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct Fenced {
    /// The position its log is at, or `None` if it holds no copy or an empty log.
    pub position: Option<Position>,
    /// The epoch of the primary that wrote its log's last frames; 0 if unknown.
    pub log_epoch: LeaseEpoch,
}

impl Fenced {
    /// How far the log is, in the order promotions adopt logs by: log epoch, then position.
    #[must_use]
    pub fn rank(&self) -> (LeaseEpoch, Option<&Position>) {
        (self.log_epoch, self.position.as_ref())
    }
}

/// Holds a follower slot's apply lock: no stream applies while it is held.
struct Exclusive {
    _applying: OwnedMutexGuard<()>,
}

/// Ends the slot's current stream and waits until nothing applies to its copy.
async fn exclusive(inner: &Inner, slot: &Arc<FollowerSlot>) -> Result<Exclusive, Status> {
    slot.begin();
    let applying = tokio::time::timeout(
        inner.settings.open_timeout,
        Arc::clone(slot.applying()).lock_owned(),
    )
    .await
    .map_err(|_| Status::unavailable("the replica's copy stayed busy"))?;
    Ok(Exclusive {
        _applying: applying,
    })
}

/// Stops serving `database` as its primary if this node serves it at an epoch below `epoch`, and
/// closes it so that its copy can follow. Returns whether it stepped down.
pub(super) async fn step_down(inner: &Inner, database: &DatabaseName, epoch: u64) -> bool {
    let serving = inner
        .primary(database)
        .is_some_and(|primary| primary.role().epoch.get() < epoch);
    if serving {
        tracing::info!(database = %database, epoch, "a newer epoch exists; stepping down as primary");
        let removed = inner.lock_primaries().remove(database);
        drop(removed);
        inner.registry().close(database).await;
    }
    serving
}

/// Fences this replica of `database` at `epoch` for `candidate`, and reports where its log is.
pub(super) async fn fence(
    inner: &Arc<Inner>,
    database: &DatabaseName,
    epoch: u64,
    candidate: Option<&NodeId>,
) -> Result<Fenced, Status> {
    if let EngineCapability::SelfReplicating = inner.engine.capability() {
        return Err(Status::failed_precondition(format!(
            "{} databases replicate themselves",
            inner.engine.kind()
        )));
    }
    step_down(inner, database, epoch).await;
    let slot = inner.slot(database);
    let _exclusive = exclusive(inner, &slot).await?;
    record_epoch(inner, database, epoch, candidate).await?;
    if inner.primary(database).is_some() || inner.registry().is_open(database) {
        return Err(Status::failed_precondition(format!(
            "{database} is open as a primary at epoch {epoch} or later on this node"
        )));
    }
    let position = match open_follower(inner, database).await {
        Ok(follower) => follower.position(),
        Err(status) if status.code() == Code::NotFound => None,
        Err(status) => return Err(status),
    };
    slot.set_position(position.clone());
    let log_epoch = LeaseEpoch::new(inner.epochs.record(database).log_epoch());
    tracing::info!(database = %database, epoch, ?candidate, ?position, log_epoch = log_epoch.get(), "fenced");
    Ok(Fenced {
        position,
        log_epoch,
    })
}

/// The `Fence` RPC.
pub(super) async fn fence_rpc(
    inner: &Arc<Inner>,
    request: FenceRequest,
) -> Result<FenceResponse, Status> {
    let name = parse_name(&request.database)?;
    let candidate: Option<NodeId> = if request.candidate.is_empty() {
        None
    } else {
        Some(
            request
                .candidate
                .parse()
                .map_err(|error| Status::invalid_argument(format!("invalid candidate: {error}")))?,
        )
    };
    let fenced = fence(inner, &name, request.epoch, candidate.as_ref()).await?;
    Ok(FenceResponse {
        epoch: request.epoch,
        position: fenced
            .position
            .map(Position::into_bytes)
            .unwrap_or_default(),
        log_epoch: fenced.log_epoch.get(),
    })
}

/// The `Fetch` RPC: everything this fenced replica holds after the caller's position.
pub(super) async fn fetch_rpc(
    inner: &Arc<Inner>,
    request: FetchRequest,
) -> Result<FetchResponse, Status> {
    let name = parse_name(&request.database)?;
    let record = inner.epochs.record(&name);
    if inner.epochs.highest(&name) != Some(request.epoch) {
        return Err(Status::failed_precondition(format!(
            "{name} is not fenced at epoch {} here",
            request.epoch
        )));
    }
    if inner.primary(&name).is_some() || inner.registry().is_open(&name) {
        return Err(Status::failed_precondition(format!(
            "{name} is open as a primary on this node"
        )));
    }
    let slot = inner.slot(&name);
    let _exclusive = exclusive(inner, &slot).await?;
    let follower = open_follower(inner, &name).await?;
    let after = held(request.after.clone());
    let (_, batch) = blocking(inner.execution, move || {
        let batch = follower.committed_since(after.as_ref());
        (follower, batch)
    })
    .await?;
    let batch = batch.map_err(engine_status)?;
    Ok(FetchResponse {
        // The batch continues the caller's copy from `after`, as a stream's batches do, even when
        // it starts the log's next generation.
        batch: batch.map(|batch| proto::Batch {
            epoch: record.log_epoch(),
            start: request.after,
            end: batch.end.into_bytes(),
            payload: batch.payload.into_bytes(),
        }),
        log_epoch: record.log_epoch(),
    })
}

/// The `Promote` RPC: queues the promotion for the node's lease keeper.
pub(super) fn promote_rpc(inner: &Inner, request: PromoteRequest) -> Result<(), Status> {
    let name = parse_name(&request.database)?;
    let replication_factor = u8::try_from(request.replication_factor)
        .ok()
        .and_then(ReplicationFactor::new)
        .ok_or_else(|| Status::invalid_argument("invalid replication factor"))?;
    let epoch = LeaseEpoch::new(request.epoch);
    if inner
        .epochs
        .highest(&name)
        .is_some_and(|highest| highest > epoch.get())
    {
        return Err(Status::failed_precondition(format!(
            "{name} has an epoch newer than {epoch} here"
        )));
    }
    inner.promotions.send_modify(|pending| {
        pending.insert(
            name,
            PromotionRequest {
                epoch,
                replication_factor,
            },
        );
    });
    Ok(())
}

/// Admits a stream that opens at `epoch` from `primary`: refuses an older epoch, or another node
/// than the epoch's recorded holder, and steps down as primary for a newer one.
pub(super) async fn admit_stream(
    inner: &Arc<Inner>,
    database: &DatabaseName,
    epoch: u64,
    primary: Option<&NodeId>,
) -> Result<(), Status> {
    if inner
        .epochs
        .highest(database)
        .is_none_or(|highest| highest < epoch)
    {
        step_down(inner, database, epoch).await;
    }
    record_epoch(inner, database, epoch, primary).await
}

/// Records `epoch` for `database`, from `holder` if known, durably.
async fn record_epoch(
    inner: &Arc<Inner>,
    database: &DatabaseName,
    epoch: u64,
    holder: Option<&NodeId>,
) -> Result<(), Status> {
    let admitted = {
        let inner = Arc::clone(inner);
        let database = database.clone();
        let holder = holder.cloned();
        blocking(inner.execution, move || match &holder {
            Some(holder) => inner.epochs.admit_from(&database, epoch, holder),
            None => inner.epochs.admit(&database, epoch),
        })
        .await?
    };
    admitted.map_err(epoch_status)
}

pub(super) fn epoch_status(error: EpochError) -> Status {
    match error {
        EpochError::Stale { .. } | EpochError::OtherHolder { .. } => {
            Status::failed_precondition(error.to_string())
        }
        EpochError::Io(_) => Status::internal(error.to_string()),
    }
}

/// What a follower's copy needs before it takes an epoch's frames.
pub(super) enum Reconciled {
    /// It follows the epoch's log from where it is.
    Follows(Box<dyn FrameFollower>),
    /// It diverges from the epoch's log at a point it can't be cut back to, so only a snapshot
    /// from the epoch's primary brings it in line.
    NeedsSnapshot,
}

/// Brings a follower's copy in line with the epoch its stream opened at, before the first
/// acknowledgement: a copy whose log came from an earlier epoch and runs past where this epoch's
/// log began is cut back there (ADR-0018, point 5). A copy that can't be cut back there, because
/// that point is at or before its last checkpoint or in an older generation, needs a snapshot.
///
/// A copy installed from a snapshot already records the snapshot's epoch as its log epoch, so it
/// is never cut back.
pub(super) async fn reconcile(
    inner: &Arc<Inner>,
    database: &DatabaseName,
    epoch: u64,
    epoch_start: Option<&Position>,
    follower: Box<dyn FrameFollower>,
) -> Result<Reconciled, Status> {
    let Some(start) = epoch_start else {
        return Ok(Reconciled::Follows(follower));
    };
    let log_epoch = inner.epochs.record(database).log_epoch();
    let position = follower.position();
    if log_epoch >= epoch || position.as_ref() <= Some(start) {
        return Ok(Reconciled::Follows(follower));
    }
    tracing::info!(
        database = %database,
        epoch,
        log_epoch,
        from = ?position,
        to = %start,
        "truncating a divergent tail",
    );
    let target = start.clone();
    let (follower, truncated) = blocking(inner.execution, move || {
        let mut follower = follower;
        let truncated = follower.truncate_to(&target);
        (follower, truncated)
    })
    .await?;
    match truncated {
        Ok(()) => Ok(Reconciled::Follows(follower)),
        Err(EngineError::InvalidPosition { .. }) => {
            tracing::info!(
                database = %database,
                epoch,
                to = %start,
                "the copy diverges before a point it can truncate to; it needs a snapshot",
            );
            Ok(Reconciled::NeedsSnapshot)
        }
        Err(error) => Err(engine_status(error)),
    }
}

/// Records, once per stream, that the copy's last frames now come from the primary of `epoch`.
/// Called after a batch applies and before it is acknowledged, so every replica that
/// acknowledged a frame of an epoch says so.
pub(super) async fn note_applied(
    inner: &Arc<Inner>,
    database: &DatabaseName,
    epoch: u64,
    noted: &mut bool,
) -> Result<(), Status> {
    if !*noted {
        if inner.epochs.record(database).log_epoch() != epoch {
            set_log_epoch(inner, database, epoch).await?;
        }
        *noted = true;
    }
    Ok(())
}

/// Records durably that the copy's last frames come from the primary of `epoch`.
pub(super) async fn set_log_epoch(
    inner: &Arc<Inner>,
    database: &DatabaseName,
    epoch: u64,
) -> Result<(), Status> {
    let recorded = {
        let inner = Arc::clone(inner);
        let database = database.clone();
        blocking(inner.execution, move || {
            inner.epochs.set_log_epoch(&database, epoch)
        })
        .await?
    };
    recorded.map_err(epoch_status)
}

/// Opens this node's copy of `database` as a follower.
async fn open_follower(
    inner: &Inner,
    database: &DatabaseName,
) -> Result<Box<dyn FrameFollower>, Status> {
    let engine = Arc::clone(&inner.engine);
    let location = inner.registry().location(database);
    blocking(inner.execution, move || match engine.capability() {
        EngineCapability::FrameSource(frames) => frames.open_follower(&location),
        _ => Err(EngineError::NotFound),
    })
    .await?
    .map_err(engine_status)
}

fn parse_name(text: &str) -> Result<DatabaseName, Status> {
    text.parse()
        .map_err(|error| Status::invalid_argument(format!("invalid database name: {error}")))
}

/// A promotion's candidate side, and leading a database at an epoch.
impl Replication {
    /// The promotions the control node asked this node to carry out, by database.
    ///
    /// The `Promote` RPC adds to it; the node's lease keeper watches it and drives each one.
    #[must_use]
    pub fn promotions(&self) -> watch::Receiver<BTreeMap<DatabaseName, PromotionRequest>> {
        self.inner.promotions.subscribe()
    }

    /// The role this node serves `database` at as its primary, if it does.
    #[must_use]
    pub fn serving(&self, database: &DatabaseName) -> Option<PrimaryRole> {
        self.inner.primary(database).map(|primary| primary.role())
    }

    /// The highest epoch this node has seen for `database`: offered by a stream, a fence, or its
    /// own lease.
    #[must_use]
    pub fn highest_epoch(&self, database: &DatabaseName) -> Option<LeaseEpoch> {
        self.inner.epochs.highest(database).map(LeaseEpoch::new)
    }

    /// The members of `database`'s replica set in this node's membership view, in preference
    /// order.
    #[must_use]
    pub fn replica_set(
        &self,
        database: &DatabaseName,
        replication_factor: ReplicationFactor,
    ) -> Vec<NodeId> {
        self.inner
            .membership
            .view()
            .ring()
            .map_or_else(Vec::new, |ring| {
                ring.replicas_n(database.as_str(), replication_factor)
                    .to_ids()
            })
    }

    /// The promotion quorum Q for a replica set of `members` members.
    #[must_use]
    pub fn promotion_quorum(members: usize) -> usize {
        let size = u8::try_from(members)
            .ok()
            .and_then(ReplicationFactor::new)
            .unwrap_or(ReplicationFactor::ONE);
        usize::from(Quorums::new(size).promotion())
    }

    /// Leads `database` at `role`'s epoch: records durably that this node holds it and that its
    /// log continues from that epoch on, then serves it as its primary. After a promotion,
    /// `start` is the position the adopted log had, which followers from earlier epochs truncate
    /// back to.
    ///
    /// Serving at the same epoch again keeps the streams that are open.
    ///
    /// # Errors
    ///
    /// [`ServePrimaryError::Superseded`] if this node has seen a newer epoch,
    /// [`ServePrimaryError::Failed`] if the record can't be written, and
    /// [`ServePrimaryError::SelfReplicating`] for an engine that replicates itself.
    pub async fn lead(
        &self,
        database: &DatabaseName,
        role: PrimaryRole,
        start: Option<Position>,
    ) -> Result<(), ServePrimaryError> {
        let inner = &self.inner;
        let recorded = {
            let inner = Arc::clone(inner);
            let database = database.clone();
            let start = start.clone();
            blocking(inner.execution, move || {
                inner
                    .epochs
                    .lead(&database, &inner.node, role.epoch.get(), start.as_ref())
            })
            .await
        };
        match recorded {
            Ok(Ok(())) => {}
            Ok(Err(EpochError::Stale { highest, .. })) => {
                return Err(ServePrimaryError::Superseded {
                    epoch: role.epoch,
                    highest: LeaseEpoch::new(highest),
                });
            }
            Ok(Err(error)) => {
                return Err(ServePrimaryError::Failed {
                    reason: error.to_string(),
                });
            }
            Err(status) => {
                return Err(ServePrimaryError::Failed {
                    reason: status.message().to_owned(),
                });
            }
        }
        let same = inner
            .primary(database)
            .is_some_and(|primary| primary.role() == role && start.is_none());
        if same {
            return Ok(());
        }
        self.serve_primary(database.clone(), role)
    }

    /// Stops serving `database` as its primary, if it does, and closes it, so that its copy can
    /// follow the new primary. Its writes are no longer acknowledged.
    pub async fn demote(&self, database: &DatabaseName) {
        let removed = self.inner.lock_primaries().remove(database);
        if removed.is_some() {
            tracing::info!(database = %database, "no longer the primary");
        }
        drop(removed);
        self.inner.registry().close(database).await;
    }

    /// Fences this node's own replica of `database` at `epoch`, as a promotion's candidate does
    /// first.
    ///
    /// # Errors
    ///
    /// `FAILED_PRECONDITION` if this node has seen a newer epoch or another holder, and the
    /// status of a copy that can't be read.
    pub async fn fence_local(
        &self,
        database: &DatabaseName,
        epoch: LeaseEpoch,
    ) -> Result<Fenced, Status> {
        let node = self.inner.node.clone();
        fence(&self.inner, database, epoch.get(), Some(&node)).await
    }

    /// Fences `node`'s replica of `database` at `epoch` for this node, waiting at most `limit`.
    ///
    /// # Errors
    ///
    /// `UNAVAILABLE` if `node` has no known address or doesn't answer in time, and the status it
    /// answered with otherwise.
    pub async fn fence_peer(
        &self,
        node: &NodeId,
        database: &DatabaseName,
        epoch: LeaseEpoch,
        limit: Duration,
    ) -> Result<Fenced, Status> {
        let inner = &self.inner;
        let address = inner
            .addresses
            .address(node)
            .ok_or_else(|| Status::unavailable(format!("no address for {node}")))?;
        let channel = inner
            .peers
            .channel_to(&address)
            .map_err(|error| Status::unavailable(error.to_string()))?;
        let request = FenceRequest {
            database: database.to_string(),
            epoch: epoch.get(),
            candidate: inner.node.to_string(),
        };
        let response = tokio::time::timeout(limit, replication_client(channel).fence(request))
            .await
            .map_err(|_| {
                inner.peers.forget(&address);
                Status::unavailable(format!("{node} didn't answer the fence in time"))
            })??
            .into_inner();
        Ok(Fenced {
            position: held(response.position),
            log_epoch: LeaseEpoch::new(response.log_epoch),
        })
    }

    /// Fetches what this node's copy of `database` lacks from `node`, fenced at `epoch`, and
    /// applies it durably, waiting at most `limit` for the transfer. Returns the copy's new
    /// position.
    ///
    /// # Errors
    ///
    /// `UNAVAILABLE` if `node` can't be reached in time, `FAILED_PRECONDITION` if its log doesn't
    /// continue this copy's, and the status of a copy that can't be read or written.
    pub async fn catch_up(
        &self,
        database: &DatabaseName,
        epoch: LeaseEpoch,
        node: &NodeId,
        limit: Duration,
    ) -> Result<Option<Position>, Status> {
        let inner = &self.inner;
        let address = inner
            .addresses
            .address(node)
            .ok_or_else(|| Status::unavailable(format!("no address for {node}")))?;
        let channel = inner
            .peers
            .channel_to(&address)
            .map_err(|error| Status::unavailable(error.to_string()))?;
        let slot = inner.slot(database);
        let _exclusive = exclusive(inner, &slot).await?;
        let follower = open_follower(inner, database).await?;
        let after = follower.position();
        let request = FetchRequest {
            database: database.to_string(),
            epoch: epoch.get(),
            after: after.clone().map(Position::into_bytes).unwrap_or_default(),
        };
        let response = tokio::time::timeout(limit, replication_client(channel).fetch(request))
            .await
            .map_err(|_| {
                inner.peers.forget(&address);
                Status::unavailable(format!("{node} didn't send the frames in time"))
            })??
            .into_inner();
        let Some(batch) = response.batch else {
            return Ok(after);
        };
        if held(batch.start.clone()) != after {
            return Err(Status::failed_precondition(format!(
                "{node}'s frames don't continue this copy of {database}"
            )));
        }
        let frames = FrameBatch {
            start: Position::from(batch.start),
            end: Position::from(batch.end),
            payload: Payload::from(batch.payload),
        };
        let (follower, applied) = blocking(inner.execution, move || {
            let mut follower = follower;
            let applied = follower.apply(&frames);
            (follower, applied)
        })
        .await?;
        let position = applied.map_err(engine_status)?;
        drop(follower);
        set_log_epoch(inner, database, response.log_epoch).await?;
        slot.set_position(Some(position.clone()));
        tracing::info!(database = %database, from = %node, %position, "caught up from the most advanced fenced member");
        Ok(Some(position))
    }

    /// Removes every trace of `database` from this node, because the control plane dropped it:
    /// stops serving it as primary, ends any stream following it, closes it, deletes its files,
    /// and forgets its epochs, so that a database created again under the name starts over.
    ///
    /// # Errors
    ///
    /// The I/O error if the files can't be deleted. The database is closed and no longer served
    /// either way; the next check deletes again.
    pub async fn discard(&self, database: &DatabaseName) -> io::Result<()> {
        let inner = &self.inner;
        let removed = inner.lock_primaries().remove(database);
        drop(removed);
        let slot = inner.slot(database);
        // The stream, fence, or fetch holding the copy finishes first; a stream that begins later
        // finds the copy gone and ends with `NOT_FOUND` until the primary gives up.
        let exclusive = exclusive(inner, &slot).await;
        let deleted = inner.registry().remove(database).await;
        let forgotten = {
            let inner = Arc::clone(inner);
            let database = database.clone();
            blocking(inner.execution, move || inner.epochs.forget(&database))
                .await
                .map_err(|status| io::Error::other(status.message().to_owned()))
                .and_then(|forgotten| forgotten)
        };
        drop(exclusive);
        inner.lock_followers().remove(database);
        tracing::info!(database = %database, "discarded the dropped database");
        deleted.and(forgotten)
    }

    /// Every database this node has an epoch record for, in name order: those it leads or led,
    /// and those it follows or was fenced for.
    pub async fn recorded_databases(&self) -> Vec<DatabaseName> {
        let inner = Arc::clone(&self.inner);
        blocking(inner.execution, move || inner.epochs.databases())
            .await
            .unwrap_or_default()
    }

    /// Reads where this node's copies of `databases` are, for those it doesn't serve as primary,
    /// so that its heartbeats report them before any stream reaches them, as after a restart.
    pub async fn recover_positions(&self, databases: &[DatabaseName]) {
        let inner = &self.inner;
        for database in databases {
            if inner.primary(database).is_some() || inner.registry().is_open(database) {
                continue;
            }
            let slot = inner.slot(database);
            if slot.positions().0.is_some() {
                continue;
            }
            if let Ok(follower) = open_follower(inner, database).await {
                let position = follower.position();
                // A stream may have opened meanwhile; it knows better.
                if slot.positions().0.is_none() {
                    slot.set_position(position);
                }
            }
        }
    }
}
