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
//! - **Dropped databases.** A node deletes a copy only when it saw the database's drop itself
//!   (ADR-0042): the control node that dropped or expired the database tells each replica
//!   through `Replication.DropCopy`, and the replica deletes its copy once control state agrees
//!   that it no longer holds the database at that epoch. Absence alone is never a drop: control
//!   state that was lost, replaced, or rolled back, or a node pointed at another fleet, says
//!   the same. So a renewal refused as an unknown database, a renewal refused because the name
//!   now belongs to a database that has never been leased, a follower whose stream ends and
//!   finds the database gone or at a lower epoch, and a recovery pass that finds the same, each
//!   keep the copy as an **orphan**: closed, neither served nor replicated, logged at warn once,
//!   listed in `status --node`, and counted in `fleetring_databases_orphaned`. A creation or
//!   restore of the name on the node refuses it, naming it, and `fleetringd orphan remove`
//!   deletes it.
//! - **Retiring.** A primary that stops serving because the control plane no longer knows its
//!   database, by a notice or a refused renewal, admits no write after that. It flushes the
//!   database's change streams, its archive included, for up to [`DROP_FLUSH_LIMIT`] (ADR-0041,
//!   point 4): every stream delivers what the copy acknowledged, and an archive puts its open
//!   segment at once, so the archive holds every acknowledged write. A flush that runs out of
//!   time, or a primary that is down when the drop happens, leaves the archive as of its last
//!   archived segment. The flush runs beside the renewals, so it never holds up another
//!   database's lease. Then the copy is deleted if a drop notice arrived, before or during the
//!   flush, and kept as an orphan otherwise. Until then, the node refuses to create or restore
//!   a database of the same name with `UNAVAILABLE`, which clients retry, and the deletion
//!   leaves the copy alone if the node serves the name at another epoch by then. A node that
//!   was down when the drop happened gets no notice, and keeps its copy as an orphan.
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
//! - **Draining.** When the membership says this node is draining, as after
//!   `fleetringd node drain`, the keeper hands its leases over (the `drain`
//!   module). A shutdown that leaves does the same before it leaves.

use std::collections::BTreeMap;
use std::time::Duration;

use fleetring::NodeId;
use fleetring_control::{DatabaseName, LeaseEpoch};
use fleetring_proto::v1::{
    CompletePromotionRequest, Placement, RecordFencedRequest, RenewLeaseRequest,
};
use tokio::sync::mpsc;
use tokio::task::{JoinHandle, JoinSet};
use tokio::time::MissedTickBehavior;
use tonic::{Code, Status};

use crate::changes::{Flushed, HeldStreams};
use crate::control::missing_database;
use crate::data::drain::DrainSettings;
use crate::data::node::{DataNode, Fate, HeldLease, Retiring};
use crate::engine::Position;
use crate::replication::{Fenced, PromotionRequest, Replication};

/// The longest a primary waits for a dropped database's change streams to deliver what its copy
/// acknowledged before it deletes the copy (ADR-0041, point 4).
pub(crate) const DROP_FLUSH_LIMIT: Duration = Duration::from_secs(30);

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
    /// How a drain hands the node's leases over.
    pub(crate) drain: DrainSettings,
    /// How long a dropped database's change streams have to flush before its copy is deleted:
    /// [`DROP_FLUSH_LIMIT`], unless a test shortens it.
    pub(crate) drop_flush_limit: Duration,
}

/// A database this node stopped serving at `epoch` because it was dropped, with its change
/// streams held for the flush before its copy is deleted, and its name kept from a new database
/// until then.
#[derive(Debug)]
struct Dropped {
    database: DatabaseName,
    epoch: LeaseEpoch,
    streams: HeldStreams,
    _retiring: Retiring,
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
    let mut membership = node.replication().membership();
    let mut membership_ended = false;
    // The drain this node runs because the membership says it is draining, until the membership
    // says otherwise, as after it joins again.
    let mut draining: Option<AbortOnDrop> = None;
    let mut drops = node.replication().drops();
    // A notice that arrived before the keeper started is acted on too.
    drops.mark_changed();
    let mut seen_drops: BTreeMap<DatabaseName, u64> = BTreeMap::new();
    // Databases this node stopped serving because the control plane no longer knows them, each
    // flushed, then deleted or kept, in a task of its own, so that a slow flush holds up no
    // renewal. The tasks are aborted when the keeper stops, as at shutdown: a copy whose flush or
    // deletion was cut short stays on disk, and the next start's recovery pass keeps it as an
    // orphan.
    let (dropped_found, mut dropped) = mpsc::unbounded_channel::<Dropped>();
    let mut retiring = JoinSet::new();
    let mut ticks = tokio::time::interval(settings.renew_interval);
    ticks.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            view = membership.changed(), if !membership_ended => {
                let Some(view) = view else {
                    membership_ended = true;
                    continue;
                };
                if !view.is_draining(node.id()) {
                    draining = None;
                } else if draining.is_none() {
                    tracing::info!(node = %node.id(), "the control plane says this node is draining");
                    let node = node.clone();
                    draining = Some(AbortOnDrop(tokio::spawn(async move {
                        let report = node.drain(settings.drain).await;
                        tracing::info!(moved = report.moved.len(), stuck = report.stuck.len(), "drained");
                    })));
                }
            }
            _ = ticks.tick() => {
                if !recovered {
                    recovered = recover(&node, &mut restarting).await;
                }
                // Renewals run beside promotions, so that a slow control plane never holds up a
                // failover.
                if renewing.as_ref().is_none_or(|task| task.0.is_finished()) {
                    let node = node.clone();
                    let dropped_found = dropped_found.clone();
                    renewing = Some(AbortOnDrop(tokio::spawn(async move {
                        renew_all(&node, settings.renew_interval, &dropped_found).await;
                    })));
                }
            }
            Some(found) = dropped.recv() => {
                let node = node.clone();
                retiring.spawn(async move {
                    retire(&node, found, settings.drop_flush_limit).await;
                });
            }
            Some(_) = retiring.join_next() => {}
            Some(_) = restarting.join_next() => {}
            Some(_) = checking.join_next() => {}
            changed = drops.changed() => {
                if changed.is_err() {
                    return;
                }
                let notices = drops.borrow_and_update().clone();
                for (database, notice) in notices {
                    if seen_drops.insert(database.clone(), notice.count) == Some(notice.count) {
                        continue;
                    }
                    let node = node.clone();
                    let dropped_found = dropped_found.clone();
                    checking.spawn(async move {
                        drop_seen(&node, &database, notice.epoch, &dropped_found).await;
                    });
                }
            }
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
                        keep_if_unknown(&node, &database).await;
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
/// them; reads its copies' positions for its heartbeats; and keeps as an orphan each copy whose
/// database the control plane doesn't know, or holds only at a lower epoch (ADR-0042). Returns
/// `false` if the control plane couldn't be reached, so the pass runs again.
///
/// Each database starts serving in a task of its own on `restarting`, since it waits for its
/// followers first, and the lease keeper goes on renewing and promoting meanwhile.
async fn recover(node: &DataNode, restarting: &mut JoinSet<()>) -> bool {
    let replication = node.replication();
    let databases = replication.recorded_databases().await;
    replication.recover_positions(&databases).await;
    for name in &databases {
        if node.held(name).is_some() || replication.is_orphan(name) {
            continue;
        }
        let placement = match node.locate(name).await {
            Ok(placement) => placement,
            Err(status) if is_unreachable(&status) => return false,
            Err(status) if missing_database(&status).is_some() => {
                keep_orphan(node, name, UNKNOWN).await;
                continue;
            }
            Err(_) => continue,
        };
        if is_past_incarnation(node, name, &placement) {
            keep_orphan(node, name, OLDER).await;
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
/// plane says is gone. One the control plane doesn't know goes to `dropped`, to be flushed, and
/// then kept as an orphan unless a drop notice arrives (ADR-0042).
async fn renew_all(node: &DataNode, limit: Duration, dropped: &mpsc::UnboundedSender<Dropped>) {
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
        let outcome = match &renewed {
            Ok(Ok(_)) => "renewed",
            Ok(Err(status)) if missing_database(status).is_some() || is_lost(status) => "refused",
            Ok(Err(_)) | Err(_) => "failed",
        };
        node.metrics().renewal(outcome);
        match renewed {
            Ok(Ok(_)) => {}
            Ok(Err(status)) if missing_database(&status).is_some() => {
                // Not proof of a drop: an empty, older, or foreign control state answers the
                // same. Only a drop notice deletes the copy (ADR-0042).
                tracing::info!(database = %database, epoch = epoch.get(), "the control plane doesn't know the database; stopped serving it");
                stop_serving(node, &database, epoch, Fate::Kept, dropped).await;
            }
            Ok(Err(status)) if is_lost(&status) => {
                if created_again(node, &database).await {
                    // Not proof of a drop either: an empty or recreated control state looks the
                    // same. The copy stays, an orphan a creation here refuses (ADR-0042).
                    demote(node, &database, epoch).await;
                    keep_orphan(node, &database, NEVER_LEASED).await;
                } else {
                    tracing::warn!(database = %database, epoch = epoch.get(), %status, "lost the lease");
                    demote(node, &database, epoch).await;
                }
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

/// Stops serving `database`, whose lease at `epoch` belongs to a database the control plane no
/// longer knows, and hands it to `dropped`, to be flushed and then settled by `fate`, or by a
/// drop notice that arrives meanwhile.
async fn stop_serving(
    node: &DataNode,
    database: &DatabaseName,
    epoch: LeaseEpoch,
    fate: Fate,
    dropped: &mpsc::UnboundedSender<Dropped>,
) {
    // Both taken before serving stops: the streams of a database the node no longer serves stop
    // as soon as they notice, and the name must never be free for a new database while the old
    // copy is still here.
    let retiring = node.begin_retiring(database, fate);
    let streams = node.change_streams().hold(database);
    if node.forget(database, epoch) {
        let found = Dropped {
            database: database.clone(),
            epoch,
            streams,
            _retiring: retiring,
        };
        if let Err(unsent) = dropped.send(found) {
            // The keeper has stopped; the copy is settled without a flush.
            let Dropped {
                epoch,
                streams,
                _retiring,
                ..
            } = unsent.0;
            drop(streams);
            settle(node, database, epoch).await;
            drop(_retiring);
        }
    }
}

/// Whether the control plane's `database` has never been leased, while this node leads one of
/// the name: a database created under the name after a drop, or after the control state was
/// lost or replaced. Its control plane refuses the old lease as stale rather than unknown.
async fn created_again(node: &DataNode, database: &DatabaseName) -> bool {
    matches!(
        node.locate(database).await,
        Ok(placement) if placement.epoch == 0 && placement.primary.is_none()
    )
}

/// Settles the copy of a database this node stopped serving because the control plane no longer
/// knows it, once its change streams have delivered what the copy acknowledged, or once `limit`
/// passes: deletes it if its drop was seen, and keeps it as an orphan otherwise.
async fn retire(node: &DataNode, dropped: Dropped, limit: Duration) {
    let Dropped {
        database,
        epoch,
        streams,
        _retiring,
    } = dropped;
    match node.change_streams().flush_database(&database, limit).await {
        Flushed::Delivered => {
            tracing::info!(database = %database, "flushed the database's change streams");
        }
        Flushed::TimedOut => {
            tracing::warn!(
                database = %database,
                limit_ms = u64::try_from(limit.as_millis()).unwrap_or(u64::MAX),
                "the database's change streams didn't catch up in time; its archive holds it \
                 as of its last archived segment"
            );
        }
    }
    drop(streams);
    settle(node, &database, epoch).await;
    // Only now may a new database take the name here.
    drop(_retiring);
}

/// Deletes the copy of `database` this node led at `epoch` if its drop was seen, and keeps it as
/// an orphan otherwise. A notice that arrives while the orphan is being kept still deletes it.
///
/// The drop's epoch may be past the lease's, as when a promotion to a newer epoch had begun
/// when the database was dropped, and fenced this copy at it; the copy goes all the same.
async fn settle(node: &DataNode, database: &DatabaseName, epoch: LeaseEpoch) {
    if node.fate(database) == Fate::Kept {
        keep_orphan(node, database, UNKNOWN).await;
    }
    if let Fate::Deleted { epoch: dropped } = node.fate(database) {
        discard_retired(node, database, epoch, epoch.max(dropped)).await;
    }
}

/// Deletes the copy of `database` this node led at `epoch` until it was dropped at `dropped`,
/// unless the node has since come to serve the name at another epoch, or seen one past
/// `dropped`: that copy is a newer database's, and stays.
async fn discard_retired(
    node: &DataNode,
    database: &DatabaseName,
    epoch: LeaseEpoch,
    dropped: LeaseEpoch,
) {
    let replication = node.replication();
    let newer = replication
        .serving(database)
        .is_some_and(|role| role.epoch != epoch)
        || replication
            .highest_epoch(database)
            .is_some_and(|highest| highest > dropped);
    if newer {
        tracing::warn!(database = %database, epoch = epoch.get(), "the name serves a newer database here; the dropped copy's deletion leaves it alone");
        return;
    }
    discard(node, database).await;
}

/// Stops serving `database`, whose lease at `epoch` is gone.
async fn demote(node: &DataNode, database: &DatabaseName, epoch: LeaseEpoch) {
    if node.forget(database, epoch) {
        node.replication().demote(database).await;
    }
}

/// Asks the control plane whether `database` still exists, after a stream following it ended,
/// and keeps this node's copy as an orphan if it doesn't, or if the control plane holds the name
/// only at a lower epoch (ADR-0042). A control plane that can't be reached, or any other
/// failure, leaves the copy as it is: the next ended stream or recovery pass asks again.
async fn keep_if_unknown(node: &DataNode, database: &DatabaseName) {
    if node.replication().is_orphan(database) {
        return;
    }
    match node.locate(database).await {
        Ok(placement) if is_past_incarnation(node, database, &placement) => {
            keep_orphan(node, database, OLDER).await;
        }
        Ok(_) => {}
        Err(status) if missing_database(&status).is_some() => {
            keep_orphan(node, database, UNKNOWN).await;
        }
        Err(status) => {
            tracing::debug!(database = %database, %status, "can't ask whether the database still exists");
        }
    }
}

/// Why a copy was kept as an orphan: the control plane doesn't know its database.
const UNKNOWN: &str =
    "the control plane doesn't know the database, and no control node said it dropped it";

/// Why a copy was kept as an orphan: the control plane holds the name at a lower epoch.
const OLDER: &str = "the control plane holds the name only at a lower epoch than this copy's";

/// Why a copy was kept as an orphan: the control plane's database of the name was never leased.
const NEVER_LEASED: &str =
    "the control plane's database of this name has never been leased, so it isn't this copy's";

/// Keeps this node's copy of `database` as an orphan, closed, and logs it at warn the first time,
/// saying `why` (ADR-0042).
async fn keep_orphan(node: &DataNode, database: &DatabaseName, why: &'static str) {
    node.forget_any(database);
    if let Some(epoch) = node.replication().orphan(database).await {
        tracing::warn!(
            database = %database,
            epoch = epoch.get(),
            node = %node.id(),
            reason = why,
            "kept this node's copy as an orphan, closed; `fleetringd orphan remove` deletes it"
        );
    }
}

/// Acts on a drop a control node told this node of (ADR-0042): deletes this node's copy of
/// `database`, after flushing its change streams if this node serves it.
///
/// The copy is deleted only if its highest epoch is at most `epoch`, the one the database was
/// dropped at, and only while control state still agrees: it holds no database of the name, or
/// one at a lower epoch. It is asked again here, since a database of the name may have been
/// created and leased since the notice arrived. A control plane that can't be reached leaves the
/// copy, which becomes an orphan the usual way.
async fn drop_seen(
    node: &DataNode,
    database: &DatabaseName,
    epoch: LeaseEpoch,
    dropped: &mpsc::UnboundedSender<Dropped>,
) {
    let Some(highest) = node.replication().highest_epoch(database) else {
        return;
    };
    if highest > epoch {
        tracing::info!(database = %database, epoch = epoch.get(), highest = highest.get(), "a drop notice names an older database than this node's copy; kept the copy");
        return;
    }
    match node.locate(database).await {
        Ok(placement) if placement.epoch < epoch.get() => {}
        Err(status) if missing_database(&status).is_some() => {}
        Ok(_) => {
            tracing::info!(database = %database, epoch = epoch.get(), "the control plane holds the name at the dropped epoch again; kept the copy");
            return;
        }
        Err(status) => {
            tracing::info!(database = %database, epoch = epoch.get(), %status, "can't confirm a drop notice with the control plane; kept the copy");
            return;
        }
    }
    if node.confirm_drop(database, epoch) {
        tracing::info!(database = %database, epoch = epoch.get(), "the database was dropped; its copy goes once its change streams are flushed");
        return;
    }
    if let Some(lease) = node.held_leases().get(database).copied() {
        tracing::info!(database = %database, epoch = epoch.get(), "the database was dropped; stopped serving it");
        stop_serving(
            node,
            database,
            lease.epoch,
            Fate::Deleted { epoch },
            dropped,
        )
        .await;
        return;
    }
    tracing::info!(database = %database, epoch = epoch.get(), "the database was dropped; deleting this node's copy");
    discard(node, database).await;
}

/// Whether this node's copy of `database` comes from a database that was dropped and created
/// again under the name: the control plane never issues an epoch below one a replica has seen, so
/// a placement below the recorded epoch belongs to a new database.
fn is_past_incarnation(node: &DataNode, database: &DatabaseName, placement: &Placement) -> bool {
    node.replication()
        .highest_epoch(database)
        .is_some_and(|highest| placement.epoch < highest.get())
}

/// Removes every trace of `database`, whose drop this node saw: the lease this node holds on it,
/// its streams, its files, and its epochs.
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
    if node
        .replication()
        .serving(database)
        .is_some_and(|role| role.epoch == request.epoch)
    {
        return Ok(());
    }
    let started = tokio::time::Instant::now();
    let result = carry_out(node, database, request, settings).await;
    match &result {
        Ok(()) => node
            .metrics()
            .promotion("completed", Some(started.elapsed())),
        Err(status) if status.code() == Code::FailedPrecondition => {
            node.metrics().promotion("superseded", None);
        }
        Err(_) => node.metrics().promotion("failed", None),
    }
    result
}

/// Fences a quorum, adopts the most advanced log among it, completes the promotion, and serves
/// `database` at `request.epoch`. `FAILED_PRECONDITION` means control state has moved past the
/// epoch, or this node is no longer a candidate.
async fn carry_out(
    node: &DataNode,
    database: &DatabaseName,
    request: PromotionRequest,
    settings: LeaseSettings,
) -> Result<(), Status> {
    let replication = node.replication();
    let epoch = request.epoch;
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
