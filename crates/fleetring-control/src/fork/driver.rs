//! The fork driver: what the control node proposes to move each fork on (ADR-0046, point
//! 3.11).

use crate::mutation::Mutation;
use std::collections::{BTreeMap, BTreeSet};

use fleetring::NodeId;

use crate::fork::apply::tick_due;
use crate::fork::rules::{bootstrap_quorum, completion_quorum_of, identity_of, next_seeder};
use crate::fork::{
    ChainHash, Fork, ForkFailure, ForkIdentity, ForkState, RetryCause, SeedFailureReason,
};
use crate::{Action, Database, LeaseEpoch, Position, Report, State, Timestamp};

/// Returns the commands that move every fork on at `now`, in database order, with a
/// [`Action::Tick`] last if a fork's deadline or an operation's retention has passed.
///
/// The driver is a pure function of the state and the time, so a control node that restarts,
/// or a new leader, proposes the same commands from the same state and loses nothing. None of
/// its commands needs a random value. Each proposal is one `apply` would accept at `now`, as far
/// as the state shows, and `apply` checks it again: a proposal made stale by a command logged in
/// between is refused and changes nothing. A report counts only if it carries the fork's current
/// identity. For each fork, the driver proposes at most one of:
///
/// - [`Action::FailFork`] when the source was dropped or recreated, or a replica reports a
///   permanent failure about the source;
/// - [`Action::CompleteFork`] as soon as a completion quorum of the replicas that didn't refuse
///   on disk reports the same staged copy, at log epoch 0, for the current attempt;
/// - [`Action::RecordForkDiskRefusal`] for a replica that reports it can't take the copy for
///   want of disk;
/// - [`Action::RetryForkSeed`] when the seeder reports a failure worth retrying, has left the
///   replica set, or has been down for the fork's seeder timeout, or when the attempt has run
///   for its attempt timeout, with that cause;
/// - [`Action::DropForkHolder`] for a holder of a seeded fork that is no longer a member, or
///   that reports its staged copy lost, only before the first lease since completion;
/// - [`Action::MarkForkReady`] when the bootstrap quorum of a seeded fork's replica set reports
///   its reset committed;
/// - [`Action::ReleaseForkHolders`] when a ready fork that keeps its holders has a lease holder
///   that reports, at the lease's epoch, that the write quorum of placement at the current
///   membership epoch holds its log;
///
/// and a [`Action::Tick`] when a seeding fork is past its deadline, which the tick fails, or an
/// operation's record is past its retention, which the tick forgets.
///
/// # Examples
///
/// ```
/// use fleetring_control::{fork, ControlConfig, State, Timestamp};
///
/// let state = State::new(ControlConfig::new());
/// assert!(fork::actions(&state, Timestamp::from_millis(0)).is_empty());
/// ```
#[must_use]
pub fn actions(state: &State, now: Timestamp) -> Vec<Action> {
    let now = now.max(state.now());
    let mut actions: Vec<Action> = state
        .databases()
        .filter_map(|db| {
            let fork = db.fork()?;
            match fork.state() {
                ForkState::Seeding { .. } => seeding(state, now, db, fork),
                ForkState::Seeded { .. } => seeded(state, db, fork),
                ForkState::Ready {
                    holders: Some(_), ..
                } => release(state, db, fork),
                _ => None,
            }
        })
        .collect();
    if tick_due(state, now) {
        actions.push(Action::Tick);
    }
    actions
}

/// What moves a seeding fork on, if anything.
fn seeding(state: &State, now: Timestamp, db: &Database, fork: &Fork) -> Option<Action> {
    let ForkState::Seeding {
        attempt,
        seeder,
        seeder_excluded,
        disk_refused,
        attempt_started_at,
        deadline,
        ..
    } = fork.state()
    else {
        return None;
    };
    let attempt = *attempt;
    // A `Tick` fails it.
    if now >= *deadline {
        return None;
    }
    let identity = identity_of(db, fork);
    let fail = |reason| Action::FailFork {
        fork: identity.clone(),
        attempt,
        reason,
    };
    match state.database(fork.source()) {
        None => return Some(fail(ForkFailure::SourceGone)),
        Some(source)
            if source.incarnation() != fork.source_incarnation()
                && !Mutation::SkipSourceIncarnation.is_active() =>
        {
            return Some(fail(ForkFailure::SourceRecreated));
        }
        Some(_) => {}
    }
    let replicas = state.replica_set(db.name()).ok()?;
    let reports: Vec<(&NodeId, &Report)> = replicas
        .iter()
        .filter(|id| !disk_refused.contains(*id))
        .filter_map(|id| Some((id, state.report(id, db.name())?)))
        .collect();
    let need = completion_quorum_of(db);

    if let Some(complete) = completion(&reports, &identity, attempt, fork, seeder, need) {
        return Some(complete);
    }

    // Failures about this fork: a refusal for want of disk counts whatever the attempt, since
    // it is about the replica; anything else only for the current attempt.
    let mut seeder_failure = None;
    for (id, report) in &reports {
        let Some(failure) = report
            .seed_failure()
            .filter(|failure| *failure.fork() == identity)
        else {
            continue;
        };
        let reason = failure.reason();
        if reason == SeedFailureReason::InsufficientDisk {
            return Some(Action::RecordForkDiskRefusal {
                fork: identity,
                attempt,
                node: (*id).clone(),
            });
        }
        if failure.attempt() != attempt {
            continue;
        }
        if let Some(permanent) = reason.as_failure() {
            return Some(fail(permanent));
        }
        if *id == seeder {
            seeder_failure = reason.as_retry();
        }
    }

    let config = state.config();
    // A seeder that left the fleet or the replica set, or that has been down for the seeder
    // timeout, is passed over.
    let seeder_gone = state.member(seeder).is_none() || !replicas.contains(seeder);
    let seeder_timed_out = state.last_heartbeat(seeder).is_some_and(|heartbeat| {
        let down_since = heartbeat.saturating_add(config.heartbeat_timeout());
        now > down_since
            && now
                >= down_since
                    .max(*attempt_started_at)
                    .saturating_add(fork.limits().seeder_timeout)
    });
    let retry = if disk_refused.contains(seeder) {
        Some((RetryCause::SeederDiskRefused, None))
    } else if let Some(cause) = seeder_failure {
        // A seeder whose transfer keeps failing is passed over, so that one node's persistent
        // trouble doesn't spend every attempt.
        Some((cause, Some(seeder)))
    } else if seeder_gone || seeder_timed_out {
        Some((RetryCause::SeederDown, Some(seeder)))
    } else if now >= attempt_started_at.saturating_add(fork.limits().attempt_timeout) {
        Some((RetryCause::AttemptTimedOut, None))
    } else {
        None
    };
    let (cause, exclude_seeder) = retry?;
    // Out of attempts, `apply` fails the fork whatever is live; otherwise the next attempt
    // needs a live seeder, and waits for one.
    let mut excluded = seeder_excluded.clone();
    excluded.extend(exclude_seeder.cloned());
    if attempt < fork.limits().max_attempts
        && next_seeder(state, &replicas, &excluded, disk_refused, now).is_none()
    {
        return None;
    }
    Some(Action::RetryForkSeed {
        fork: identity,
        attempt,
        cause,
        exclude_seeder: exclude_seeder.cloned(),
    })
}

/// A `CompleteFork` if a completion quorum of the replicas reports the same staged copy of the
/// current attempt at log epoch 0: the seeder's copy if enough replicas hold it, or else the
/// copy most hold, the lowest position first on ties.
fn completion(
    reports: &[(&NodeId, &Report)],
    identity: &ForkIdentity,
    attempt: u32,
    fork: &Fork,
    seeder: &NodeId,
    need: usize,
) -> Option<Action> {
    let mut copies: BTreeMap<(&Position, &ChainHash), BTreeSet<NodeId>> = BTreeMap::new();
    for (id, report) in reports {
        if report.log_epoch() != LeaseEpoch::ZERO {
            continue;
        }
        let Some(staged) = report.staged().filter(|staged| {
            *staged.fork() == *identity
                && staged.attempt() == attempt
                && (Mutation::SkipSourceIncarnation.is_active()
                    || staged.source_incarnation() == fork.source_incarnation())
        }) else {
            continue;
        };
        copies
            .entry((staged.position(), staged.chain_hash()))
            .or_default()
            .insert((*id).clone());
    }
    let seeders = copies
        .iter()
        .find(|(_, holders)| holders.contains(seeder) && holders.len() >= need);
    let best = seeders.or_else(|| {
        copies
            .iter()
            .filter(|(_, holders)| holders.len() >= need)
            .reduce(|best, copy| {
                if copy.1.len() > best.1.len() {
                    copy
                } else {
                    best
                }
            })
    });
    let ((at, chain_hash), holders) = best?;
    Some(Action::CompleteFork {
        fork: identity.clone(),
        attempt,
        at: (*at).clone(),
        chain_hash: (*chain_hash).clone(),
        holders: holders.clone(),
    })
}

/// What moves a seeded fork on: before the first lease since completion, `DropForkHolder` for a
/// holder that left the fleet or lost its copy; otherwise `MarkForkReady` once the bootstrap quorum of its
/// replica set reports the same reset committed, at an epoch the database has issued.
fn seeded(state: &State, db: &Database, fork: &Fork) -> Option<Action> {
    let identity = identity_of(db, fork);
    let ForkState::Seeded {
        attempt,
        holders,
        lease_from,
        ..
    } = fork.state()
    else {
        return None;
    };
    // Holders are dropped only before the first lease since completion: after it, the primary
    // counts its write quorum over every holder, and the fork may hold acknowledged writes.
    // Seeding again needs a fleet to place the fork on; with no members, nothing is proposed.
    let droppable = db.epoch() == *lease_from && state.ring().is_ok();
    for holder in holders.iter().filter(|_| droppable) {
        let cause = if state.member(holder).is_none() {
            RetryCause::HoldersLeft
        } else if state
            .report(holder, db.name())
            .and_then(Report::copy_lost)
            .is_some_and(|lost| *lost.fork() == identity && lost.attempt() == *attempt)
        {
            RetryCause::HolderLost
        } else {
            continue;
        };
        return Some(Action::DropForkHolder {
            fork: identity,
            attempt: *attempt,
            holder: holder.clone(),
            cause,
        });
    }

    let replicas = state.replica_set(db.name()).ok()?;
    let mut resets: BTreeMap<(LeaseEpoch, &Position), usize> = BTreeMap::new();
    for replica in &replicas {
        let Some(reset) = state
            .report(replica, db.name())
            .and_then(Report::reset)
            .filter(|reset| {
                reset.is_committed()
                    && *reset.fork() == identity
                    && reset.epoch() > *lease_from
                    && reset.epoch() <= db.epoch()
            })
        else {
            continue;
        };
        *resets.entry((reset.epoch(), reset.position())).or_default() += 1;
    }
    // A ready fork always has a lease: its reset was written under one.
    db.lease()?;
    let need = usize::from(bootstrap_quorum(db.replication_factor(), db.durability()));
    let ((epoch, position), _) = resets.into_iter().find(|(_, count)| *count >= need)?;
    Some(Action::MarkForkReady {
        fork: identity,
        epoch,
        reset_position: position.clone(),
    })
}

/// A `ReleaseForkHolders` for a ready fork that keeps its holders, once the primary serving it
/// now reports that placement's write quorum, at the current membership epoch, holds its log
/// (ADR-0046, point 3.10, as built).
fn release(state: &State, db: &Database, fork: &Fork) -> Option<Action> {
    let identity = identity_of(db, fork);
    let lease = db
        .lease()
        .filter(|lease| lease.epoch() == db.epoch() && db.promotion().is_none())?;
    let epoch = lease.epoch();
    state
        .report(lease.holder(), db.name())
        .and_then(Report::placement_caught_up)
        .filter(|caught_up| {
            *caught_up.fork() == identity
                && caught_up.epoch() == epoch
                && caught_up.membership_epoch() == state.membership_epoch()
        })?;
    Some(Action::ReleaseForkHolders {
        fork: identity,
        epoch,
    })
}
