//! A data node's leases over time: renewal, demotion, serving again after a restart, and the
//! promotions it carries out as a candidate (ADR-0018).
//!
//! - **Renewal.** Every renewal interval, the node renews each lease it holds with `RenewLease`.
//!   A refusal that names a newer epoch or another holder means the lease is gone: the node stops
//!   serving the database and closes it, so that its copy can follow the new primary. A control
//!   plane that can't be reached changes nothing: the primary keeps serving, because no other
//!   node can be granted a newer epoch meanwhile (point 6). Safety never depends on this timer:
//!   followers fenced at a newer epoch refuse the old primary's batches, so it can't acknowledge
//!   a write however long it takes to notice.
//! - **Dropped databases.** A renewal refused as an unknown database means the database was
//!   dropped: the primary stops serving it and deletes its copy, within one renewal interval of
//!   the drop. Its streams end with it, and a follower whose stream ends asks the control plane
//!   whether the database still exists; if not, it deletes its copy too. A node that was away
//!   makes the same check for every database it has a record of when it recovers.
//! - **After a restart**, the node asks the control plane about every database it has an epoch
//!   record for, and leads each one whose lease it holds at once, rather than when the first
//!   request arrives. The same pass reads its copies' positions, so its heartbeats report them.
//! - **Promotions.** The control node tells a promotion's candidate through
//!   `Replication.Promote`. The candidate:
//!   1. fences its own replica at the new epoch, then the other members of the replica set, in
//!      parallel, each with `Replication.Fence`, and records each fenced member with
//!      `RecordFenced`, until Q members are fenced;
//!   2. adopts the most advanced log among them, ranked by log epoch and then position, and
//!      fetches what its own copy lacks from that member (`Replication.Fetch`);
//!   3. calls `CompletePromotion`, and then leads the database at the new epoch. The position
//!      it adopted is where the epoch's log begins: followers whose logs came from earlier epochs
//!      and run past it truncate back to it.

use std::collections::BTreeMap;
use std::time::Duration;

use fleetring::NodeId;
use fleetring_control::{DatabaseName, LeaseEpoch};
use fleetring_proto::v1::{
    CompletePromotionRequest, Placement, RecordFencedRequest, RenewLeaseRequest,
};
use tokio::task::{JoinHandle, JoinSet};
use tokio::time::MissedTickBehavior;
use tonic::{Code, Status};

use crate::control::missing_database;
use crate::data::node::{DataNode, HeldLease};
use crate::engine::Position;
use crate::replication::{Fenced, PromotionRequest, Replication};

/// How a data node paces its leases and promotions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct LeaseSettings {
    /// How often each lease is renewed. Well under the lease duration, so that a few lost
    /// renewals don't let it expire.
    pub(crate) renew_interval: Duration,
    /// How long a candidate waits for one replica to answer a fence.
    pub(crate) fence_timeout: Duration,
    /// How long a candidate waits for the frames it lacks.
    pub(crate) transfer_timeout: Duration,
}

/// Aborts a task when dropped.
#[derive(Debug)]
struct AbortOnDrop(JoinHandle<()>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// The task that keeps a data node's leases. Dropping it stops it.
#[derive(Debug)]
pub(crate) struct LeaseKeeper {
    _task: AbortOnDrop,
}

impl LeaseKeeper {
    /// Starts keeping `node`'s leases.
    pub(crate) fn start(node: DataNode, settings: LeaseSettings) -> Self {
        Self {
            _task: AbortOnDrop(tokio::spawn(run(node, settings))),
        }
    }
}

async fn run(node: DataNode, settings: LeaseSettings) {
    let mut promotions = node.replication().promotions();
    let mut ended = node.replication().ended_streams();
    let mut seen_ended: BTreeMap<DatabaseName, u64> = BTreeMap::new();
    let mut checking = JoinSet::new();
    let mut running: BTreeMap<DatabaseName, (LeaseEpoch, AbortOnDrop)> = BTreeMap::new();
    let mut renewing: Option<AbortOnDrop> = None;
    // Databases this node serves again after a restart, each waiting for its followers.
    let mut restarting = JoinSet::new();
    let mut recovered = false;
    let mut ticks = tokio::time::interval(settings.renew_interval);
    ticks.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = ticks.tick() => {
                if !recovered {
                    recovered = recover(&node, &mut restarting).await;
                }
                // Renewals run beside promotions, so that a slow control plane never holds up a
                // failover.
                if renewing.as_ref().is_none_or(|task| task.0.is_finished()) {
                    let node = node.clone();
                    renewing = Some(AbortOnDrop(tokio::spawn(async move {
                        renew_all(&node, settings.renew_interval).await;
                    })));
                }
            }
            Some(_) = restarting.join_next() => {}
            Some(_) = checking.join_next() => {}
            changed = ended.changed() => {
                if changed.is_err() {
                    return;
                }
                let counts = ended.borrow_and_update().clone();
                for (database, count) in counts {
                    if seen_ended.insert(database.clone(), count) == Some(count) {
                        continue;
                    }
                    let node = node.clone();
                    checking.spawn(async move {
                        discard_if_dropped(&node, &database).await;
                    });
                }
            }
            changed = promotions.changed() => {
                if changed.is_err() {
                    return;
                }
                let requests = promotions.borrow_and_update().clone();
                for (database, request) in requests {
                    let busy = running.get(&database).is_some_and(|(epoch, task)| {
                        *epoch == request.epoch && !task.0.is_finished()
                    });
                    if busy || is_settled(&node, &database, request.epoch) {
                        continue;
                    }
                    let task = {
                        let node = node.clone();
                        let database = database.clone();
                        tokio::spawn(async move {
                            if let Err(status) = promote(&node, &database, request, settings).await {
                                tracing::warn!(database = %database, epoch = request.epoch.get(), %status, "promotion attempt failed; the control node will ask again");
                            }
                        })
                    };
                    running.insert(database, (request.epoch, AbortOnDrop(task)));
                }
            }
        }
    }
}

/// Whether the promotion of `database` to `epoch` needs nothing more from this node: it serves the
/// database at that epoch, or has seen a newer one.
fn is_settled(node: &DataNode, database: &DatabaseName, epoch: LeaseEpoch) -> bool {
    let replication = node.replication();
    replication
        .serving(database)
        .is_some_and(|role| role.epoch == epoch)
        || replication
            .highest_epoch(database)
            .is_some_and(|highest| highest > epoch)
}

/// Learns which of the databases this node has records of it holds the lease on, and leads
/// them; reads its copies' positions for its heartbeats. Returns `false` if the control plane
/// couldn't be reached, so the pass runs again.
///
/// Each database starts serving in a task of its own on `restarting`, since it waits for its
/// followers first, and the lease keeper goes on renewing and promoting meanwhile.
async fn recover(node: &DataNode, restarting: &mut JoinSet<()>) -> bool {
    let replication = node.replication();
    let databases = replication.recorded_databases().await;
    replication.recover_positions(&databases).await;
    for name in &databases {
        if node.held(name).is_some() {
            continue;
        }
        let placement = match node.locate(name).await {
            Ok(placement) => placement,
            Err(status) if is_unreachable(&status) => return false,
            Err(status) if missing_database(&status).is_some() => {
                discard(node, name).await;
                continue;
            }
            Err(_) => continue,
        };
        if is_past_incarnation(node, name, &placement) {
            discard(node, name).await;
            continue;
        }
        if placement.primary.as_deref() != Some(node.id().as_str()) {
            continue;
        }
        if let Err(error) = node.registry().open(name).await {
            tracing::warn!(database = %name, %error, "holds the lease but can't open the database");
            continue;
        }
        tracing::info!(database = %name, epoch = placement.epoch, "holds the lease after a restart");
        let node = node.clone();
        let name = name.clone();
        restarting.spawn(async move {
            if let Err(status) = node
                .start_serving(&name, HeldLease::of(&placement), None)
                .await
            {
                tracing::warn!(database = %name, %status, "can't serve after a restart");
            }
        });
    }
    true
}

/// Renews every lease the node holds, each within `limit`, and stops serving each one the control
/// plane says is gone.
async fn renew_all(node: &DataNode, limit: Duration) {
    let mut renewals = JoinSet::new();
    for (database, lease) in node.held_leases() {
        let control = node.inner.control.clone();
        let request = RenewLeaseRequest {
            database: database.to_string(),
            node_id: node.id().to_string(),
            epoch: lease.epoch.get(),
        };
        renewals.spawn(async move {
            let renewed = tokio::time::timeout(limit, control.renew_lease(request)).await;
            (database, lease.epoch, renewed)
        });
    }
    while let Some(joined) = renewals.join_next().await {
        let Ok((database, epoch, renewed)) = joined else {
            continue;
        };
        match renewed {
            Ok(Ok(_)) => {}
            Ok(Err(status)) if missing_database(&status).is_some() => {
                tracing::info!(database = %database, epoch = epoch.get(), "the database was dropped");
                if node.forget(&database, epoch) {
                    discard(node, &database).await;
                }
            }
            Ok(Err(status)) if is_lost(&status) => {
                tracing::warn!(database = %database, epoch = epoch.get(), %status, "lost the lease");
                demote(node, &database, epoch).await;
            }
            Ok(Err(status)) => {
                tracing::debug!(database = %database, %status, "can't renew the lease; still serving");
            }
            Err(_) => {
                tracing::debug!(database = %database, "renewing the lease timed out; still serving");
            }
        }
    }
}

/// Stops serving `database`, whose lease at `epoch` is gone.
async fn demote(node: &DataNode, database: &DatabaseName, epoch: LeaseEpoch) {
    if node.forget(database, epoch) {
        node.replication().demote(database).await;
    }
}

/// Asks the control plane whether `database` still exists, and discards this node's copy if it
/// doesn't, or if the copy belongs to a database that was dropped and created again under the
/// name. A control plane that can't be reached, or any other failure, leaves the copy alone: the
/// next ended stream or recovery pass asks again.
async fn discard_if_dropped(node: &DataNode, database: &DatabaseName) {
    match node.locate(database).await {
        Ok(placement) if is_past_incarnation(node, database, &placement) => {
            discard(node, database).await;
        }
        Ok(_) => {}
        Err(status) if missing_database(&status).is_some() => discard(node, database).await,
        Err(status) => {
            tracing::debug!(database = %database, %status, "can't ask whether the database still exists");
        }
    }
}

/// Whether this node's copy of `database` comes from a database that was dropped and created
/// again under the name: the control plane never issues an epoch below one a replica has seen, so
/// a placement below the recorded epoch belongs to a new database.
fn is_past_incarnation(node: &DataNode, database: &DatabaseName, placement: &Placement) -> bool {
    node.replication()
        .highest_epoch(database)
        .is_some_and(|highest| placement.epoch < highest.get())
}

/// Removes every trace of `database`, which the control plane no longer knows: the lease this
/// node holds on it, its streams, its files, and its epochs.
async fn discard(node: &DataNode, database: &DatabaseName) {
    node.forget_any(database);
    if let Err(error) = node.replication().discard(database).await {
        tracing::warn!(database = %database, %error, "can't delete the dropped database's files");
    }
}

/// Whether a refused renewal means the lease is gone: a newer epoch, another holder, or no
/// database. An unknown member isn't one: the node is rejoining, and its lease may still stand.
fn is_lost(status: &Status) -> bool {
    status.code() == Code::FailedPrecondition || missing_database(status).is_some()
}

fn is_unreachable(status: &Status) -> bool {
    matches!(status.code(), Code::Unavailable | Code::DeadlineExceeded)
}

/// Carries out the promotion of `database` to `request.epoch` as its candidate.
async fn promote(
    node: &DataNode,
    database: &DatabaseName,
    request: PromotionRequest,
    settings: LeaseSettings,
) -> Result<(), Status> {
    let replication = node.replication();
    let epoch = request.epoch;
    if replication
        .serving(database)
        .is_some_and(|role| role.epoch == epoch)
    {
        return Ok(());
    }
    if replication
        .highest_epoch(database)
        .is_some_and(|highest| highest > epoch)
    {
        return Err(Status::failed_precondition(format!(
            "{database} has moved past epoch {epoch}"
        )));
    }
    let me = node.id().clone();
    let replicas = replication.replica_set(database, request.replication_factor);
    if !replicas.contains(&me) {
        return Err(Status::failed_precondition(format!(
            "{me} isn't in the replica set of {database}"
        )));
    }
    let needed = Replication::promotion_quorum(replicas.len());
    tracing::info!(database = %database, epoch = epoch.get(), needed, "promoting: fencing replicas");

    // 1. Fence this replica, then the others in parallel, until Q are fenced and recorded.
    let mine = replication.fence_local(database, epoch).await?;
    record_fenced(node, database, &me, epoch, &mine).await?;
    let mut fenced: Vec<(NodeId, Fenced)> = vec![(me.clone(), mine.clone())];
    let mut fences = JoinSet::new();
    for peer in replicas.iter().filter(|peer| **peer != me).cloned() {
        let replication = replication.clone();
        let database = database.clone();
        fences.spawn(async move {
            let result = replication
                .fence_peer(&peer, &database, epoch, settings.fence_timeout)
                .await;
            (peer, result)
        });
    }
    while fenced.len() < needed {
        let Some(joined) = fences.join_next().await else {
            break;
        };
        let Ok((peer, result)) = joined else {
            continue;
        };
        match result {
            Ok(reported) => {
                if record_fenced(node, database, &peer, epoch, &reported)
                    .await
                    .is_ok()
                {
                    fenced.push((peer, reported));
                }
            }
            Err(status) => {
                tracing::debug!(database = %database, %peer, %status, "can't fence a replica");
            }
        }
    }
    drop(fences);
    if fenced.len() < needed {
        return Err(Status::unavailable(format!(
            "fenced {} of the {needed} replicas a promotion needs",
            fenced.len()
        )));
    }

    // 2. Adopt the most advanced log among the fenced replicas.
    let mut best = (&me, &mine);
    for (peer, reported) in &fenced {
        if reported.rank() > best.1.rank() {
            best = (peer, reported);
        }
    }
    let mut position = mine.position.clone();
    if best.1.rank() > mine.rank() {
        let (from, wanted) = (best.0.clone(), best.1.position.clone());
        position = replication
            .catch_up(database, epoch, &from, settings.transfer_timeout)
            .await?;
        if position != wanted {
            return Err(Status::failed_precondition(format!(
                "caught up to {position:?} from {from}, not to {wanted:?}"
            )));
        }
    }

    // 3. Complete the promotion, then lead at the new epoch from the adopted position.
    complete(node, database, epoch).await?;
    let lease = HeldLease::new(epoch, request.replication_factor);
    node.start_serving(database, lease, position).await?;
    tracing::info!(database = %database, epoch = epoch.get(), "promoted to primary");
    Ok(())
}

/// Records with the control plane that `peer` is fenced at `epoch`, at the position it reported.
async fn record_fenced(
    node: &DataNode,
    database: &DatabaseName,
    peer: &NodeId,
    epoch: LeaseEpoch,
    reported: &Fenced,
) -> Result<(), Status> {
    let request = RecordFencedRequest {
        database: database.to_string(),
        node_id: peer.to_string(),
        epoch: epoch.get(),
        position: reported
            .position
            .clone()
            .map(Position::into_bytes)
            .unwrap_or_default(),
    };
    match node.inner.control.record_fenced(request).await {
        Ok(_) => Ok(()),
        // Recorded already, by an earlier attempt.
        Err(status) if status.code() == Code::AlreadyExists => Ok(()),
        Err(status) => Err(status),
    }
}

/// Completes the promotion, or confirms that an earlier attempt whose reply was lost did.
async fn complete(
    node: &DataNode,
    database: &DatabaseName,
    epoch: LeaseEpoch,
) -> Result<(), Status> {
    let request = CompletePromotionRequest {
        database: database.to_string(),
        epoch: epoch.get(),
    };
    match node.inner.control.complete_promotion(request).await {
        Ok(_) => Ok(()),
        Err(status) => {
            let placement = node.locate(database).await?;
            if placement.epoch == epoch.get()
                && placement.primary.as_deref() == Some(node.id().as_str())
            {
                Ok(())
            } else {
                Err(status)
            }
        }
    }
}
