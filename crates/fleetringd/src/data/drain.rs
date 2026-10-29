//! Draining: handing a node's leases to other replicas before it leaves (ADR-0018, ADR-0022).
//!
//! A planned departure needn't cost clients a failover. Once the control plane has marked the
//! node as draining (`Control.Drain`), [`drain`] hands over every database the node is primary
//! for, in parallel up to a concurrency limit, each within a bound:
//!
//! 1. **Stop taking writes.** Requests for the database answer `UNAVAILABLE` until the lease is
//!    gone; from then on they are redirected to the new primary as any request to a non-primary
//!    is. Writes already accepted finish.
//! 2. **Pick the target:** under `quorum` and `all`, the connected, live follower that has
//!    acknowledged the most, preferring the earliest in the ring's preference order on ties,
//!    since every follower holds the current epoch's log. Under `local` (ADR-0023) a follower may
//!    be whole epochs behind, so the control plane picks instead, by log epoch and then position.
//!    Then **ship the tail**: wait until the target has acknowledged everything committed.
//! 3. **Ask the control plane to transfer** (`Control.TransferLease`), naming the target, or
//!    naming none and letting the control plane choose. The control plane issues the next epoch
//!    and starts a promotion with the target as its candidate, exactly as a failover would after
//!    an expiry: a new epoch, Q fenced members, the most advanced fenced log adopted. The node
//!    tells the candidate at once (`Replication.Promote`), and the control node keeps telling it
//!    until the promotion completes.
//! 4. **Step down.** The lease is forgotten and the database closed, so that the node's copy can
//!    follow the new primary, and the fence or the first stream at the new epoch finds nothing
//!    serving at the old one.
//! 5. **Wait for the hand-over** to complete, within the bound, so that the node leaves only once
//!    its databases have a primary again.
//!
//! A database whose transfer can't start, because no follower is live or the control plane
//! refuses it, is reported as stuck and keeps serving: its lease expires and fails over the slow
//! way once the node stops. Safety never depends on any of this: the transfer is the promotion
//! protocol with a chosen candidate, and the quorum rules of ADR-0018 hold throughout.

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use fleetring::NodeId;
use fleetring_control::{Acks, DatabaseName, LeaseEpoch};
use fleetring_proto::v1::{DrainRequest, TransferLeaseRequest};
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tonic::{Code, Status};

use crate::data::node::{DataNode, HeldLease};
use crate::engine::Position;

/// How a node paces a drain.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct DrainSettings {
    /// How many databases are handed over at once.
    pub(crate) concurrency: NonZeroUsize,
    /// How long to wait for the target to acknowledge the tail before the transfer is asked for
    /// anyway; the candidate fetches what it lacks during the promotion.
    pub(crate) catch_up: Duration,
    /// How long one hand-over may take from the transfer to the new primary serving, and how
    /// long the control plane gets to answer a call.
    pub(crate) handover: Duration,
}

impl DrainSettings {
    /// Sixteen databases at a time; `catch_up` and `handover` as given.
    pub(crate) fn new(catch_up: Duration, handover: Duration) -> Self {
        Self {
            concurrency: NonZeroUsize::new(16).unwrap_or(NonZeroUsize::MIN),
            catch_up,
            handover,
        }
    }
}

/// What a drain did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct DrainReport {
    /// The databases handed over, each with its new primary once the control plane named one.
    pub(crate) moved: Vec<(DatabaseName, Option<NodeId>)>,
    /// The databases that couldn't be handed over and why; they keep serving until the node
    /// stops, and fail over the slow way.
    pub(crate) stuck: Vec<(DatabaseName, String)>,
}

impl DrainReport {
    /// Whether every lease was handed over and its hand-over completed.
    pub(crate) fn is_complete(&self) -> bool {
        self.stuck.is_empty() && self.moved.iter().all(|(_, to)| to.is_some())
    }
}

/// Why a database couldn't be handed over.
#[derive(Debug, thiserror::Error)]
enum Stuck {
    #[error("not serving the database")]
    NotServing,
    #[error("no live follower is connected")]
    NoFollower,
    #[error("the control plane refused the transfer: {0}")]
    Refused(Status),
}

/// Asks the control plane to mark this node as draining, so that it gets no new lease and may
/// hand its leases over, waiting at most `limit`.
///
/// # Errors
///
/// `UNAVAILABLE` if the control plane doesn't answer in time, and its status otherwise.
pub(crate) async fn mark_draining(node: &DataNode, limit: Duration) -> Result<(), Status> {
    let request = DrainRequest {
        node_id: node.id().to_string(),
    };
    let response = tokio::time::timeout(limit, node.inner.control.drain(request))
        .await
        .map_err(|_| Status::unavailable("the control plane didn't answer the drain in time"))??;
    tracing::info!(
        node = %node.id(),
        primaries = response.primaries.len(),
        "draining: handing leases over",
    );
    Ok(())
}

/// Hands over every lease `node` holds, as the module's documentation describes, and reports
/// what moved and what is stuck. The control plane must have marked the node as draining first.
pub(crate) async fn drain(node: &DataNode, settings: DrainSettings) -> DrainReport {
    let limit = Arc::new(Semaphore::new(settings.concurrency.get()));
    let mut transfers = JoinSet::new();
    for (database, lease) in node.held_leases() {
        let node = node.clone();
        let limit = Arc::clone(&limit);
        transfers.spawn(async move {
            // The semaphore outlives every task, so a permit is always granted.
            let _permit = limit.acquire().await;
            let outcome = transfer(&node, &database, lease, settings).await;
            (database, outcome)
        });
    }
    let mut report = DrainReport::default();
    while let Some(joined) = transfers.join_next().await {
        let Ok((database, outcome)) = joined else {
            continue;
        };
        match outcome {
            Ok(to) => {
                match &to {
                    Some(to) => {
                        tracing::info!(database = %database, %to, "lease handed over");
                    }
                    None => tracing::warn!(
                        database = %database,
                        "lease handed over, but the new primary isn't serving yet",
                    ),
                }
                report.moved.push((database, to));
            }
            Err(stuck) => {
                tracing::warn!(database = %database, %stuck, "can't hand the lease over; it will fail over once this node stops");
                report.stuck.push((database, stuck.to_string()));
            }
        }
    }
    report.moved.sort();
    report.stuck.sort();
    report
}

/// Hands `database`'s lease over, and returns the new primary once the control plane names
/// one, or `None` if the hand-over started but didn't complete within the bound.
async fn transfer(
    node: &DataNode,
    database: &DatabaseName,
    lease: HeldLease,
    settings: DrainSettings,
) -> Result<Option<NodeId>, Stuck> {
    let replication = node.replication();
    // 1. Stop taking writes. The guard lifts the refusal when the hand-over is over, whether the
    //    lease is gone by then or the database is stuck and keeps serving.
    let _refusing = node.refuse_requests(database);

    // 2. Pick the target and ship it the tail. A `local` database's followers may hold logs from
    //    different epochs, which only the control plane can rank, so it chooses; the tail is
    //    shipped to its choice once it is known.
    let target = if lease
        .durability
        .is_some_and(|durability| durability.acks() == Acks::Local)
    {
        None
    } else {
        Some(pick_target(node, database, lease)?)
    };
    if let Some(target) = &target {
        ship_tail(node, database, target, settings).await;
    }

    // 3. Ask the control plane to start the promotion with the target as its candidate.
    let (epoch, candidate) =
        ask_transfer(node, database, lease.epoch, target.as_ref(), settings).await?;
    tracing::info!(database = %database, epoch = epoch.get(), candidate = ?candidate, "lease transfer started");
    if let Some(candidate) = &candidate {
        if target.is_none() {
            ship_tail(node, database, candidate, settings).await;
        }
        if let Err(status) = replication
            .promote_peer(
                candidate,
                database,
                epoch,
                lease.replication_factor,
                settings.catch_up,
            )
            .await
        {
            tracing::debug!(database = %database, %candidate, %status, "can't tell the candidate; the control node will");
        }
    }

    // 4. Step down: the lease is superseded, so nothing this node does with it counts.
    if node.forget(database, lease.epoch) {
        replication.demote(database).await;
    }

    // 5. Wait for the new primary.
    let served = tokio::time::timeout(settings.handover, async {
        loop {
            if let Ok(placement) = node.locate(database).await
                && placement.epoch >= epoch.get()
                && let Some(primary) = placement.primary
            {
                return primary.parse().ok();
            }
            tokio::time::sleep(replication.settings().retry_interval).await;
        }
    })
    .await;
    Ok(served.ok().flatten())
}

/// Waits, within the catch-up bound, until `target` has acknowledged everything `database`'s
/// primary committed. If it hasn't by then, the candidate fetches the rest during the promotion.
async fn ship_tail(
    node: &DataNode,
    database: &DatabaseName,
    target: &NodeId,
    settings: DrainSettings,
) {
    if !node
        .replication()
        .await_caught_up(database, target, settings.catch_up)
        .await
    {
        tracing::info!(database = %database, %target, "the target hasn't acknowledged the tail; it will fetch it during the promotion");
    }
}

/// The follower to hand `database` to: among the connected, live followers in its replica set,
/// the one that has acknowledged the most, and the earliest in preference order on ties.
fn pick_target(
    node: &DataNode,
    database: &DatabaseName,
    lease: HeldLease,
) -> Result<NodeId, Stuck> {
    let replication = node.replication();
    let status = replication
        .primary_status(database)
        .ok_or(Stuck::NotServing)?;
    let view = replication.membership_view();
    let replicas = replication.replica_set(database, lease.replication_factor);
    let mut best: Option<(&NodeId, Option<&Position>)> = None;
    for replica in &replicas {
        if replica == node.id() || view.liveness(replica) != Some(fleetring_control::Liveness::Live)
        {
            continue;
        }
        let Some(progress) = status.followers.iter().find(|f| &f.node == replica) else {
            continue;
        };
        if !progress.connected || !progress.reported {
            continue;
        }
        let acknowledged = progress.acknowledged.as_ref();
        // Strictly greater, so the earliest in preference order wins ties.
        if best.is_none_or(|(_, held)| acknowledged > held) {
            best = Some((replica, acknowledged));
        }
    }
    best.map(|(replica, _)| replica.clone())
        .ok_or(Stuck::NoFollower)
}

/// Asks the control plane to transfer `database`'s lease to `target`, or to whoever it ranks
/// highest if none is named, and returns the promotion's epoch and candidate. If the control
/// plane refuses the target, it may pick one itself; if it refuses that too, a promotion already
/// past `held` means a transfer is under way, which is what was asked for, though the candidate
/// is then known only if this node named it.
async fn ask_transfer(
    node: &DataNode,
    database: &DatabaseName,
    held: LeaseEpoch,
    target: Option<&NodeId>,
    settings: DrainSettings,
) -> Result<(LeaseEpoch, Option<NodeId>), Stuck> {
    let control = &node.inner.control;
    let ask = |target: String| {
        let request = TransferLeaseRequest {
            database: database.to_string(),
            target,
        };
        async move {
            tokio::time::timeout(settings.handover, control.transfer_lease(request))
                .await
                .map_err(|_| Status::unavailable("the control plane didn't answer in time"))?
        }
    };
    let mut asked = ask(target.map_or_else(String::new, ToString::to_string)).await;
    if let Some(target) = target
        && asked
            .as_ref()
            .is_err_and(|status| status.code() == Code::FailedPrecondition)
    {
        tracing::debug!(database = %database, %target, "the control plane refused the target; letting it pick");
        asked = ask(String::new()).await;
    }
    match asked {
        Ok(response) => {
            let candidate = response
                .candidate
                .parse()
                .map_err(|_| Stuck::Refused(Status::internal("invalid candidate")))?;
            Ok((LeaseEpoch::new(response.epoch), Some(candidate)))
        }
        Err(status) if status.code() == Code::FailedPrecondition => {
            // A lost reply, or a promotion that started meanwhile: either way the lease is on
            // its way out.
            let placement = node.locate(database).await.map_err(Stuck::Refused)?;
            if placement.epoch > held.get() && placement.primary.is_none() {
                // The control node tells the real candidate; the nudge below may miss.
                Ok((LeaseEpoch::new(placement.epoch), target.cloned()))
            } else {
                Err(Stuck::Refused(status))
            }
        }
        Err(status) => Err(Stuck::Refused(status)),
    }
}

impl DataNode {
    /// Hands over every lease this node holds, one drain at a time.
    ///
    /// The control plane must have marked the node as draining ([`mark_draining`]), or every
    /// transfer is refused. A drain that starts while another runs waits for it, then hands
    /// over whatever is left.
    pub(crate) async fn drain(&self, settings: DrainSettings) -> DrainReport {
        let _one_at_a_time = self.inner.draining.lock().await;
        drain(self, settings).await
    }

    /// Marks this node as draining and hands its leases over, as a graceful shutdown does
    /// before it leaves. A control plane that can't be reached leaves the leases to expire.
    pub(crate) async fn drain_before_leaving(&self, settings: DrainSettings) -> DrainReport {
        if let Err(status) = mark_draining(self, settings.handover).await {
            tracing::warn!(%status, "can't mark this node as draining; its leases will expire instead");
            return DrainReport::default();
        }
        let report = self.drain(settings).await;
        tracing::info!(
            moved = report.moved.len(),
            stuck = report.stuck.len(),
            complete = report.is_complete(),
            "drained",
        );
        report
    }
}
