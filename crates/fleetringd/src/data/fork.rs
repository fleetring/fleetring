//! A data node's fork work over time (ADR-0046, points 4 and 6): it seeds the forks control
//! state names it a replica of, deletes staged copies only on positive signals, takes a seeded
//! fork's first lease when it is the first live holder, and opens a fork it leads only once the
//! fork-reset write is committed at the bootstrap quorum.
//!
//! - **Which forks.** The heartbeat reply names the forks that aren't ready whose replica set
//!   names this node; the node also looks at every name it holds a staged copy of, and every
//!   fork it leads whose gate is closed. For each it asks control state for the placement, and
//!   acts on what it shows.
//! - **Seeding.** While a fork seeds attempt *n* with this node in its replica set and not
//!   refused for want of disk, the node fetches attempt *n*'s copy: from the source's primary as
//!   the seeder, else from the seeder. A fetch that reported a failure isn't started again for
//!   the same attempt; the control plane decides what comes next.
//! - **Signals.** A staged copy is deleted only when control state names its own identity
//!   failed or ready, seeded without this node, or at a later attempt, or a drop notice names it
//!   (see the lease keeper). A staged copy of another identity than the placement's, or of a
//!   name control state doesn't know, is an orphan: kept, untouched, for an operator.
//! - **The first lease.** Once a fork is seeded and no lease was issued, the first live holder
//!   in the placement's order checks its staged copy against what control state recorded,
//!   reports it lost and never asks if it fails, else takes the lease, moves the copy into the
//!   database's place, and serves it, gated.
//! - **Opening.** The primary of a fork that isn't ready serves no client until its copy holds
//!   the fork's marker committed at the bootstrap quorum, max(⌊R/2⌋ + 1, W): it runs the
//!   conditional fork-reset write if the marker is absent, waits for the quorum, reports the
//!   reset committed, and opens its gate. One whose control view says ready but whose copy has
//!   no marker refuses to serve and logs an error.
//! - **Followers** report the reset once their durable log reaches the position the fork's
//!   primary reported it committed at, at that primary's epoch.

use std::collections::BTreeSet;
use std::time::Duration;

use fleetring::NodeId;
use fleetring_control::fork::{CopyLost, ForkIdentity, ResetReport};
use fleetring_control::{Acks, DatabaseName, LeaseEpoch, Quorums};
use fleetring_proto::v1::{AcquireLeaseRequest, Placement};
use tokio::task::JoinHandle;
use tokio::time::MissedTickBehavior;
use tonic::Status;

use crate::control::missing_database;
use crate::data::node::{DataNode, HeldLease};
use crate::engine::{ForkMarker, ForkReset, Position};
use crate::replication::FaultPoint;
use crate::replication::fork::seed::{SeedFrom, SeedPlan};
use crate::replication::fork::staged::{Held, StagedKey};
use crate::replication::fork::view::view_from_placement;
use crate::replication::fork::{ForkPhase, ForkView, InstallAs};

/// The longest the opening of a fork's primary waits between two tries.
const OPEN_BACKOFF_CAP: Duration = Duration::from_secs(5);

/// The task that does a data node's fork work. Dropping it stops it.
#[derive(Debug)]
pub(crate) struct ForkKeeper {
    task: JoinHandle<()>,
}

impl Drop for ForkKeeper {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl ForkKeeper {
    /// Starts `node`'s fork work, looking again every `poll`, and whenever a heartbeat reply
    /// names other forks.
    pub(crate) fn start(node: DataNode, poll: Duration) -> Self {
        Self {
            task: tokio::spawn(run(node, poll)),
        }
    }
}

async fn run(node: DataNode, poll: Duration) {
    let membership = node.replication().membership();
    let mut named = membership.forks_changed();
    let mut ticks = tokio::time::interval(poll);
    ticks.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = ticks.tick() => {}
            changed = named.changed() => {
                if changed.is_err() {
                    return;
                }
            }
        }
        let mut names: BTreeSet<DatabaseName> = membership.pending_forks();
        let forks = node.replication().forks();
        names.extend(forks.store().names());
        names.extend(forks.gated());
        names.extend(forks.frozen_names());
        for name in names {
            if let Err(status) = tend(&node, &name).await {
                tracing::debug!(database = %name, %status, "fork work will try again");
            }
        }
    }
}

/// Acts on what control state shows of the fork `name`.
async fn tend(node: &DataNode, name: &DatabaseName) -> Result<(), Status> {
    let replication = node.replication();
    let forks = replication.forks();
    let placement = match node.locate(name).await {
        Ok(placement) => placement,
        Err(status) if missing_database(&status).is_some() => {
            // A name control state doesn't know: its staged copies are orphans (ADR-0042).
            forks.observe(name, None);
            forks.follow_view(name, None);
            forks.abandon_seeds(name, None);
            mark_orphans(node, name, None);
            return Ok(());
        }
        Err(status) => return Err(status),
    };
    let view = view_from_placement(&placement)?;
    // A seeded fork's replica set is its recorded holders until this node sees it ready, its
    // gate open or not (ADR-0046, point 3.10).
    forks.follow_view(name, view.fork.as_ref());
    let Some(fork) = view.fork else {
        forks.observe(name, None);
        forks.abandon_seeds(name, None);
        mark_orphans(node, name, None);
        return Ok(());
    };
    // A fork that is ready or failed needs no more reports from this node.
    forks.observe(name, fork.is_pending().then_some(&fork.identity));
    signals(node, &fork).await;
    match &fork.phase {
        ForkPhase::Seeding {
            attempt,
            seeder,
            disk_refused,
        } => {
            let key = StagedKey {
                fork: fork.identity.clone(),
                attempt: *attempt,
            };
            forks.abandon_seeds(name, Some(&key));
            let me = node.id();
            let member = view.holders.replicas.contains(me) && !disk_refused.contains(me);
            if member && let Some(seeder) = seeder {
                seed(node, &fork, key, seeder);
            }
        }
        ForkPhase::Seeded {
            attempt,
            at,
            chain_hash,
            holders,
            reset,
        } => {
            forks.abandon_seeds(name, None);
            let me = node.id();
            if !holders.contains(me) {
                return Ok(());
            }
            let key = StagedKey {
                fork: fork.identity.clone(),
                attempt: *attempt,
            };
            match placement.primary.as_deref() {
                None if placement.candidate.is_none()
                    && first_live_holder(&placement).as_ref() == Some(me) =>
                {
                    open_first(node, &placement, &key, at, chain_hash).await?;
                }
                Some(primary) if primary != me.as_str() => {
                    report_follower_reset(node, &fork.identity, reset.as_ref()).await;
                }
                _ => {}
            }
        }
        _ => forks.abandon_seeds(name, None),
    }
    Ok(())
}

/// The first holder the placement shows live, in its order: the one that takes a seeded fork's
/// first lease (ADR-0046, point 6.2).
fn first_live_holder(placement: &Placement) -> Option<NodeId> {
    placement
        .replicas
        .iter()
        .find(|replica| replica.live)
        .and_then(|replica| replica.node_id.parse().ok())
}

/// Deletes the staged copies of `fork`'s name that control state gives a positive signal for
/// (ADR-0046, point 4.6), and marks those of another identity orphans.
async fn signals(node: &DataNode, fork: &ForkView) {
    let replication = node.replication();
    let store = replication.forks().store();
    for staged in store.of_name(&fork.identity.name) {
        let key = staged.sidecar.key.clone();
        if key.fork != fork.identity {
            // Another incarnation or operation of the name: a view, not a signal.
            if store.mark_orphan(&key) {
                tracing::warn!(fork = %key.fork, attempt = key.attempt, current = %fork.identity, "kept a staged copy of another fork identity as an orphan; `fleetringd orphan remove` deletes it");
            }
            continue;
        }
        store.confirm(&key);
        let signal = match &fork.phase {
            ForkPhase::Failed => Some("the fork failed"),
            ForkPhase::Ready => Some("the fork is ready"),
            ForkPhase::Seeding { attempt, .. } if key.attempt < *attempt => Some("a later attempt"),
            ForkPhase::Seeded { attempt, .. } if key.attempt < *attempt => Some("a later attempt"),
            ForkPhase::Seeded {
                attempt, holders, ..
            } if key.attempt == *attempt && !holders.contains(node.id()) => {
                Some("seeded without this node")
            }
            _ => None,
        };
        if let Some(signal) = signal {
            let earlier_opened = signal == "a later attempt" && staged.held == Held::Installed;
            if earlier_opened {
                // The copy an earlier attempt opened is in the database's place: it goes with
                // its staged copy, on the same signal, unless the node serves the name.
                let _name = node.lock_name(&fork.identity.name).await;
                if replication.serving(&fork.identity.name).is_none()
                    && node.held(&fork.identity.name).is_none()
                {
                    if let Err(error) = replication.discard(&fork.identity.name).await {
                        tracing::warn!(fork = %key.fork, attempt = key.attempt, %error, "can't delete the copy an earlier attempt opened");
                    } else {
                        tracing::info!(target: "fleetringd::audit", fork = %key.fork, attempt = key.attempt, signal, "deleted the copy an earlier attempt opened");
                    }
                }
            }
            replication.delete_staged(&key, signal).await;
        }
    }
}

/// Marks every staged copy of `name` an orphan, but one of `keep`'s identity.
fn mark_orphans(node: &DataNode, name: &DatabaseName, keep: Option<&ForkIdentity>) {
    let store = node.replication().forks().store();
    for staged in store.of_name(name) {
        let key = staged.sidecar.key;
        if Some(&key.fork) != keep && store.mark_orphan(&key) {
            tracing::warn!(fork = %key.fork, attempt = key.attempt, "kept a staged copy whose fork control state doesn't show as an orphan; `fleetringd orphan remove` deletes it");
        }
    }
}

/// Starts fetching `key`'s copy of `fork`, from the source's primary if this node is `seeder`,
/// else from `seeder`, unless the node holds it, fetches it, or reported it failed.
fn seed(node: &DataNode, fork: &ForkView, key: StagedKey, seeder: &NodeId) {
    let replication = node.replication();
    let forks = replication.forks();
    if forks.store().get(&key).is_some() || forks.is_seeding(&key) || forks.failed(&key) {
        return;
    }
    let from = if seeder == node.id() {
        SeedFrom::Source
    } else {
        SeedFrom::Seeder(seeder.clone())
    };
    let plan = SeedPlan {
        key,
        source: fork.source.clone(),
        source_incarnation: fork.source_incarnation,
        from,
    };
    if replication.seed_fork(plan.clone()) {
        tracing::info!(target: "fleetringd::audit", fork = %plan.key.fork, attempt = plan.key.attempt, source = %plan.source, source_incarnation = %plan.source_incarnation, from = ?plan.from, "fetching a copy of the fork");
    }
}

/// Reports the fork's reset committed on this follower once its durable log reaches the
/// position the fork's primary reported it committed at, at that primary's epoch.
async fn report_follower_reset(
    node: &DataNode,
    fork: &ForkIdentity,
    reset: Option<&(LeaseEpoch, Position)>,
) {
    let Some((epoch, position)) = reset else {
        return;
    };
    let replication = node.replication();
    replication
        .forks()
        .faults()
        .pass(FaultPoint::FollowerReset, fork.operation)
        .await;
    let reached = replication.highest_epoch(&fork.name) == Some(*epoch)
        && replication
            .follower_status(&fork.name)
            .and_then(|status| status.position)
            .is_some_and(|at| at >= *position);
    if !reached {
        return;
    }
    let report = ResetReport::committed(
        fork.clone(),
        *epoch,
        fleetring_control::Position::new(position.as_bytes().to_vec()),
    );
    let forks = replication.forks();
    if forks.reset(&fork.name).as_ref() != Some(&report) {
        tracing::info!(fork = %fork, epoch = epoch.get(), "the fork's reset is in this follower's durable log");
        forks.report_reset(report);
    }
}

/// Takes a seeded fork's first lease as its first live holder, and opens it (ADR-0046, point
/// 6.2): checks the staged copy against what control state recorded, and reports it lost
/// without asking if it fails; takes the lease; moves the copy into place; and serves it,
/// gated. A copy that fails after the lease is reported lost and nothing is served: the lease
/// isn't renewed, so it expires and another holder takes over.
async fn open_first(
    node: &DataNode,
    placement: &Placement,
    key: &StagedKey,
    at: &Position,
    chain_hash: &crate::replication::ChainHash,
) -> Result<(), Status> {
    let name = &key.fork.name;
    let replication = node.replication();
    let forks = replication.forks();
    let _name = node.lock_name(name).await;
    if replication.serving(name).is_some() || node.held(name).is_some() {
        return Ok(());
    }
    if forks
        .store()
        .get(key)
        .is_some_and(|staged| staged.held == Held::Installed)
    {
        // Moved into place by an earlier opening, whose lease has gone since.
        return Ok(());
    }
    node.check_not_dropping(name)?;
    node.refuse_orphan(name)?;
    let sidecar = match replication.verify_staged(key, at, chain_hash).await {
        Ok(sidecar) => sidecar,
        Err(reason) => {
            tracing::warn!(target: "fleetringd::audit", fork = %key.fork, attempt = key.attempt, ?reason, "the staged copy fails its check; not asking for the first lease");
            forks.report_lost(CopyLost::new(key.fork.clone(), key.attempt, reason));
            return Ok(());
        }
    };
    forks
        .faults()
        .pass(FaultPoint::FirstLease, key.fork.operation)
        .await;
    let lease = node
        .inner
        .control
        .acquire_lease(AcquireLeaseRequest {
            database: name.to_string(),
            node_id: node.id().to_string(),
        })
        .await?;
    tracing::info!(target: "fleetringd::audit", fork = %key.fork, attempt = key.attempt, epoch = lease.epoch, "took the fork's first lease");
    if let Err(status) = replication
        .install_staged(key, &sidecar, InstallAs::Primary)
        .await
    {
        tracing::error!(fork = %key.fork, %status, "can't open the staged copy after taking the lease; serving nothing, so the lease expires");
        forks.report_lost(CopyLost::new(
            key.fork.clone(),
            key.attempt,
            fleetring_control::fork::CopyLossReason::Damaged,
        ));
        return Ok(());
    }
    let placement = node
        .locate(name)
        .await
        .unwrap_or_else(|_| placement.clone());
    node.serve_at(name, &placement, Some(at.clone())).await?;
    Ok(())
}

/// Freezes the replica set of `name`, a seeded fork that isn't ready, at its recorded holders,
/// as control state shows them now, before its primary here starts streaming (ADR-0046, point
/// 3.10). A placement it can't learn leaves the replica set as this node last saw it.
pub(crate) async fn freeze_holders(node: &DataNode, name: &DatabaseName) {
    let Ok(placement) = node.locate(name).await else {
        return;
    };
    let Ok(view) = view_from_placement(&placement) else {
        return;
    };
    node.replication()
        .forks()
        .follow_view(name, view.fork.as_ref());
}

/// Closes `name`'s readiness gate before its primary here serves at `epoch`, unless the gate
/// was opened at that epoch already.
pub(crate) fn close_unless_open(node: &DataNode, name: &DatabaseName, epoch: LeaseEpoch) {
    let replication = node.replication();
    if !replication.forks().opened_at(name, epoch) {
        replication.close_gate(name);
    }
}

/// Opens `name`'s gate for its primary here under `lease`, in a task of its own, once the fork
/// is ready to serve, unless that runs, or ran, at the lease's epoch already.
pub(crate) fn open_when_ready(node: &DataNode, name: &DatabaseName, lease: HeldLease) {
    let task_node = node.clone();
    let task_name = name.clone();
    node.replication()
        .forks()
        .start_opening(name, lease.epoch, move || {
            tokio::spawn(async move { open(task_node, task_name, lease).await })
        });
}

/// Whether the opening of a fork's primary may serve.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Opened {
    /// It may: the copy holds the fork's marker, committed at the bootstrap quorum.
    Open,
    /// It may not, for good: control state says the fork is ready, but the copy holds no
    /// marker.
    Refused,
}

/// Opens `name`'s gate once its primary here may serve under `lease`, trying again while it
/// can't yet and it still leads the database at the lease's epoch.
async fn open(node: DataNode, name: DatabaseName, lease: HeldLease) {
    let replication = node.replication();
    let mut backoff = replication.settings().retry_interval;
    loop {
        if replication.serving(&name).map(|role| role.epoch) != Some(lease.epoch) {
            return;
        }
        match try_open(&node, &name, lease).await {
            Ok(Opened::Open) => {
                // The replica set stays frozen at the holders until the fork is seen ready: a
                // write served from here on counts only them, as a promotion fences only them.
                replication.forks().mark_opened(&name, lease.epoch);
                replication.open_gate(&name);
                tracing::info!(target: "fleetringd::audit", database = %name, epoch = lease.epoch.get(), "the fork serves clients");
                return;
            }
            Ok(Opened::Refused) => return,
            Err(status) => {
                tracing::debug!(database = %name, %status, "the fork can't open yet");
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(OPEN_BACKOFF_CAP);
            }
        }
    }
}

/// One try at opening `name`'s primary under `lease`.
async fn try_open(
    node: &DataNode,
    name: &DatabaseName,
    lease: HeldLease,
) -> Result<Opened, Status> {
    let placement = node.locate(name).await?;
    let view = view_from_placement(&placement)?;
    let Some(fork) = view.fork else {
        return Ok(Opened::Open);
    };
    let handle = node
        .registry()
        .open(name)
        .await
        .map_err(|error| Status::unavailable(format!("can't open {name}: {error}")))?;
    let marker_status = |error: crate::DatabaseError| {
        Status::unavailable(format!("can't read the fork marker of {name}: {error}"))
    };
    match &fork.phase {
        ForkPhase::Ready => {
            let held = handle.fork_marker().await.map_err(marker_status)?;
            let ours = held.is_some_and(|marker| {
                marker.fork_incarnation == fork.identity.incarnation.get()
                    && marker.operation == fork.identity.operation.to_bytes()
            });
            if ours {
                Ok(Opened::Open)
            } else {
                tracing::error!(fork = %fork.identity, "control state says the fork is ready, but this copy holds no marker for it; refusing to serve it");
                Ok(Opened::Refused)
            }
        }
        ForkPhase::Seeded { at, .. } => {
            node.replication().forks().follow_view(name, Some(&fork));
            reset(node, name, lease, &fork, at, &handle).await
        }
        _ => Err(Status::failed_precondition(format!(
            "{name} isn't seeded; its primary serves nothing"
        ))),
    }
}

/// The bootstrap quorum of a database with `lease`'s replication factor and durability:
/// max(⌊R/2⌋ + 1, W), whatever its `acks` (ADR-0046, point 3).
fn bootstrap_quorum(lease: HeldLease) -> usize {
    let factor = lease.replication_factor;
    let majority = Quorums::with_acks(factor, Acks::Quorum).write();
    let write = lease
        .durability
        .map_or(majority, |durability| durability.quorums(factor).write());
    usize::from(majority.max(write))
}

/// Runs the fork-reset write unless the copy holds the fork's marker, waits until the bootstrap
/// quorum holds it, and reports it committed (ADR-0046, points 6.1 and 6.2).
async fn reset(
    node: &DataNode,
    name: &DatabaseName,
    lease: HeldLease,
    fork: &ForkView,
    at: &Position,
    handle: &crate::DatabaseHandle,
) -> Result<Opened, Status> {
    let replication = node.replication();
    let forks = replication.forks();
    let faults = forks.faults();
    let identity = fork.identity.clone();
    let failed = |error: crate::DatabaseError| {
        Status::unavailable(format!("the fork-reset write of {name} failed: {error}"))
    };
    let marker = ForkMarker {
        fork_incarnation: identity.incarnation.get(),
        operation: identity.operation.to_bytes(),
        source_incarnation: fork.source_incarnation.get(),
        at: at.clone(),
    };
    let report = |position: &Position, committed: bool| {
        let position = fleetring_control::Position::new(position.as_bytes().to_vec());
        forks.report_reset(if committed {
            ResetReport::committed(identity.clone(), lease.epoch, position)
        } else {
            ResetReport::running(identity.clone(), lease.epoch, position)
        });
    };
    faults
        .pass(FaultPoint::BeforeReset, identity.operation)
        .await;
    let held = handle.fork_marker().await.map_err(failed)?;
    let position = if held.as_ref().is_some_and(|held| held.same_fork(&marker)) {
        tracing::info!(target: "fleetringd::audit", fork = %identity, epoch = lease.epoch.get(), "the copy holds the fork's marker; the reset doesn't run again");
        handle.position().await.map_err(failed)?
    } else {
        let before = handle.position().await.map_err(failed)?;
        if let Some(before) = &before {
            report(before, false);
        }
        faults
            .pass(FaultPoint::DuringReset, identity.operation)
            .await;
        match handle.fork_reset(marker).await.map_err(failed)? {
            ForkReset::Written { position } => {
                tracing::info!(target: "fleetringd::audit", fork = %identity, epoch = lease.epoch.get(), %position, "ran the fork-reset write");
                faults
                    .pass(FaultPoint::AfterLocalCommit, identity.operation)
                    .await;
                Some(position)
            }
            _ => handle.position().await.map_err(failed)?,
        }
    };
    let position = position.ok_or_else(|| {
        Status::internal(format!(
            "{name} has no log position after its fork-reset write"
        ))
    })?;
    report(&position, false);
    let need = bootstrap_quorum(lease);
    let poll = replication.settings().retry_interval;
    loop {
        if replication.serving(name).map(|role| role.epoch) != Some(lease.epoch) {
            return Err(Status::unavailable(format!(
                "this node no longer leads {name} at epoch {}",
                lease.epoch.get()
            )));
        }
        if replication.held_by(name, &position).unwrap_or(0) >= need {
            break;
        }
        tokio::time::sleep(poll).await;
    }
    faults
        .pass(FaultPoint::AfterQuorumCommit, identity.operation)
        .await;
    report(&position, true);
    tracing::info!(target: "fleetringd::audit", fork = %identity, epoch = lease.epoch.get(), %position, quorum = need, "the fork-reset write is committed at the bootstrap quorum");
    Ok(Opened::Open)
}

#[cfg(test)]
mod tests {
    use super::*;
    use fleetring::ReplicationFactor;
    use fleetring_control::Durability;

    fn lease(replicas: u8, acks: Acks) -> HeldLease {
        let factor = ReplicationFactor::new(replicas).expect("nonzero");
        let mut lease = HeldLease::new(LeaseEpoch::new(1), factor);
        lease.durability = Some(Durability::new(acks, factor));
        lease
    }

    #[test]
    fn the_bootstrap_quorum_is_at_least_a_majority() {
        assert_eq!(bootstrap_quorum(lease(3, Acks::Local)), 2);
        assert_eq!(bootstrap_quorum(lease(3, Acks::Quorum)), 2);
        assert_eq!(bootstrap_quorum(lease(3, Acks::All)), 3);
        assert_eq!(bootstrap_quorum(lease(1, Acks::Local)), 1);
        assert_eq!(bootstrap_quorum(lease(5, Acks::Local)), 3);
    }
}
