//! How `apply` creates and drives forks (ADR-0046, point 3).
//!
//! Every function here checks everything first and changes the state only once nothing can be
//! refused, so a rejection leaves the state exactly as it was.

use std::collections::BTreeSet;

use fleetring::NodeId;

use crate::fork::rules::{
    Claim, admitting, bootstrap_quorum, check_holders, check_source, completion_quorum,
    completion_quorum_of, current_fork, identity_of, next_seeder, reported_size, resets_reported,
    shows_seeded_copy,
};
use crate::fork::{
    CancelCause, ChainHash, Fork, ForkFailure, ForkIdentity, ForkIntent, ForkLimits, ForkState,
    MAX_FORK_OPERATIONS, OperationId, OperationOutcome, OperationRecord, RetryCause,
};
use crate::{
    Applied, Database, Incarnation, LeaseEpoch, Metadata, Outcome, Position, Rejection, State,
    Timestamp,
};

/// Checks that a new database may take `incarnation`: it isn't the legacy 0, and no database or
/// fork operation records it (ADR-0046, point 1).
///
/// # Errors
///
/// [`Rejection::IncarnationUnavailable`] otherwise.
pub(crate) fn check_incarnation(state: &State, incarnation: Incarnation) -> Result<(), Rejection> {
    let used = incarnation.is_legacy()
        || state
            .databases()
            .any(|database| database.incarnation() == incarnation)
        || state
            .fork_operations
            .values()
            .any(|record| record.target_incarnation() == incarnation);
    if used {
        return Err(Rejection::IncarnationUnavailable { incarnation });
    }
    Ok(())
}

/// Returns the operation record for `operation` unless it is past its retention at `now`: a
/// forgotten ID is unknown, whether or not a `Tick` has pruned it yet.
pub(crate) fn live_operation<'s>(
    state: &'s State,
    operation: &OperationId,
    now: Timestamp,
) -> Option<&'s OperationRecord> {
    state
        .fork_operations
        .get(operation)
        .filter(|record| !record.is_forgotten_at(now))
}

/// Answers a resubmission of a known operation: its record, if it comes from the same
/// requester with the same intent. Nothing is resolved again (ADR-0046, point 5.2).
///
/// # Errors
///
/// [`Rejection::OperationNotYours`] for another requester, and
/// [`Rejection::OperationConflict`] naming the first field of the intent that differs.
pub(crate) fn resend<'r>(
    record: &'r OperationRecord,
    operation: OperationId,
    intent: &ForkIntent,
) -> Result<&'r OperationRecord, Rejection> {
    if record.intent().requested_by != intent.requested_by {
        return Err(Rejection::OperationNotYours { operation });
    }
    if let Some(field) = record.intent().first_difference(intent) {
        return Err(Rejection::OperationConflict { operation, field });
    }
    Ok(record)
}

/// What a `ForkDatabase` carries besides the caller's intent: values the proposer drew or
/// resolved, and the limits from its configuration.
#[derive(Debug)]
pub(crate) struct Proposed {
    pub(crate) incarnation: Incarnation,
    pub(crate) resolved_expiry: Option<Timestamp>,
    pub(crate) limits: ForkLimits,
}

/// Applies `ForkDatabase` (ADR-0046, points 3.1, 5.2, and 7.3).
pub(crate) fn fork_database(
    state: &mut State,
    now: Timestamp,
    operation: OperationId,
    intent: ForkIntent,
    proposed: Proposed,
) -> Outcome {
    if let Some(record) = live_operation(state, &operation, now) {
        let record = resend(record, operation, &intent)?;
        return Ok(Applied::ForkResent {
            operation,
            database: record.target().clone(),
            incarnation: record.target_incarnation(),
            outcome: record.outcome().clone(),
        });
    }
    let Proposed {
        incarnation,
        resolved_expiry,
        limits,
    } = proposed;
    let live = state
        .fork_operations
        .values()
        .filter(|record| !record.is_forgotten_at(now))
        .count();
    if live >= MAX_FORK_OPERATIONS {
        return Err(Rejection::TooManyOperations {
            max: MAX_FORK_OPERATIONS,
        });
    }
    let name = intent.name.clone();
    if state.databases.contains_key(&name) {
        return Err(Rejection::DatabaseExists { database: name });
    }
    let source = state.database_or_reject(&intent.source)?;
    if !source.is_ready() {
        return Err(Rejection::SourceNotReady {
            source_database: intent.source.clone(),
        });
    }
    let source_incarnation = source.incarnation();
    if let Some(expected) = intent
        .expect_source_incarnation
        .filter(|expected| *expected != source_incarnation)
    {
        return Err(Rejection::SourceIncarnationMismatch {
            source_database: intent.source.clone(),
            expected,
            current: Some(source_incarnation),
        });
    }
    let engine = source.engine();
    if !engine.can_fork() {
        return Err(Rejection::EngineCannotFork { engine });
    }
    let durability = intent.resolved_durability();
    durability
        .check(intent.replication_factor)
        .map_err(|source| Rejection::InvalidDurability {
            database: name.clone(),
            source,
        })?;
    let expires_at = intent.expiry.resolve(now, resolved_expiry);
    if let Some(expires_at) = expires_at {
        crate::apply::check_expiry(&name, now, expires_at)?;
    }
    let ring = state.ring().map_err(|_| Rejection::NoMembers)?;
    let replicas = ring
        .replicas_n(name.as_str(), intent.replication_factor)
        .to_ids();
    let need = completion_quorum(intent.replication_factor, durability);
    if replicas.len() < usize::from(need) {
        return Err(Rejection::TooFewReplicas {
            database: name,
            needed: need,
            available: replicas.len(),
        });
    }
    let none = BTreeSet::new();
    let Some(seeder) = next_seeder(state, &replicas, &none, &none, now) else {
        return Err(Rejection::NoCandidate { database: name });
    };
    // Best effort: sizes and free disk are recorded only once forks are enabled, so the first
    // fork of a fleet passes unconditionally; the data node's own check decides.
    let source_bytes = reported_size(state, &intent.source);
    let admitted = admitting(state, &replicas, source_bytes, limits.admission_floor());
    if admitted < usize::from(need) {
        return Err(Rejection::InsufficientDisk {
            database: name,
            admitting: admitted,
            need,
        });
    }
    check_incarnation(state, incarnation)?;

    let fork = Fork::new(
        operation,
        intent.source.clone(),
        source_incarnation,
        intent.requested_by.clone(),
        now,
        limits,
        ForkState::Seeding {
            attempt: 1,
            seeder: seeder.clone(),
            seeder_excluded: BTreeSet::new(),
            disk_refused: BTreeSet::new(),
            attempt_started_at: now,
            deadline: now.saturating_add(limits.deadline),
            last_cause: None,
        },
    )
    .with_source_bytes(source_bytes);
    let metadata = Metadata::created(intent.metadata(expires_at), now);
    let database = Database::new(
        name.clone(),
        intent.replication_factor,
        engine,
        durability,
        Some(metadata),
    )
    .with_incarnation(incarnation)
    .with_fork(fork);
    let record = OperationRecord::new(
        intent,
        source_incarnation,
        expires_at,
        incarnation,
        now,
        limits.operation_retention,
    );
    state.databases.insert(name.clone(), database);
    state.fork_operations.insert(operation, record);
    Ok(Applied::ForkCreated {
        database: name,
        incarnation,
        operation,
        seeder,
    })
}

/// What a seeding fork's current attempt is.
struct Attempt {
    seeder: NodeId,
    seeder_excluded: BTreeSet<NodeId>,
    disk_refused: BTreeSet<NodeId>,
    attempt_started_at: Timestamp,
    deadline: Timestamp,
    last_cause: Option<RetryCause>,
}

/// Returns the fork's current attempt, if it is seeding at `attempt`.
///
/// # Errors
///
/// [`Rejection::ForkNotSeeding`] if the fork isn't seeding, or is at another attempt.
fn seeding_at(fork: &Fork, identity: &ForkIdentity, attempt: u32) -> Result<Attempt, Rejection> {
    match fork.state() {
        ForkState::Seeding {
            attempt: current,
            seeder,
            seeder_excluded,
            disk_refused,
            attempt_started_at,
            deadline,
            last_cause,
        } if *current == attempt => Ok(Attempt {
            seeder: seeder.clone(),
            seeder_excluded: seeder_excluded.clone(),
            disk_refused: disk_refused.clone(),
            attempt_started_at: *attempt_started_at,
            deadline: *deadline,
            last_cause: *last_cause,
        }),
        _ => Err(Rejection::ForkNotSeeding {
            database: identity.name.clone(),
            attempt,
        }),
    }
}

/// Applies `CompleteFork` (ADR-0046, point 3.2). The holders become the fork's replica set
/// until it is ready.
pub(crate) fn complete_fork(
    state: &mut State,
    now: Timestamp,
    identity: &ForkIdentity,
    attempt: u32,
    at: Position,
    chain_hash: ChainHash,
    holders: BTreeSet<NodeId>,
) -> Outcome {
    let (db, fork) = current_fork(state, identity)?;
    let current = seeding_at(fork, identity, attempt)?;
    check_source(state, fork)?;
    let replicas = state.replica_set(&identity.name)?;
    let claim = Claim {
        identity,
        attempt,
        at: &at,
        chain_hash: &chain_hash,
        source_incarnation: fork.source_incarnation(),
    };
    check_holders(
        state,
        &claim,
        &holders,
        &replicas,
        &current.disk_refused,
        completion_quorum_of(db),
    )?;
    let lease_from = db.epoch();

    let operation = fork.operation();
    set_fork_state(
        state,
        identity,
        ForkState::Seeded {
            attempt,
            at,
            chain_hash,
            holders: holders.clone(),
            lease_from,
            completed_at: now,
        },
    )?;
    set_outcome(state, operation, OperationOutcome::Seeded, now);
    Ok(Applied::ForkSeeded {
        database: identity.name.clone(),
        holders,
    })
}

/// How a fork's next attempt starts.
struct NextAttempt {
    /// Replicas passed over as seeders.
    seeder_excluded: BTreeSet<NodeId>,
    /// Replicas that refused on disk.
    disk_refused: BTreeSet<NodeId>,
    /// When the fork fails if it is still seeding.
    deadline: Timestamp,
    /// Whether the fork was seeded and seeds again: its lease, if any, is cleared, and no live
    /// seeder is needed.
    reseed: bool,
}

/// Moves the current fork `identity`, after its attempt `attempt` ended for `cause`, to its next
/// attempt, or fails it if its budget or its deadline has run out (ADR-0046, points 3.3, 3.4,
/// and 3.6).
fn next_attempt(
    state: &mut State,
    now: Timestamp,
    identity: &ForkIdentity,
    attempt: u32,
    cause: RetryCause,
    next: NextAttempt,
) -> Outcome {
    let (db, fork) = current_fork(state, identity)?;
    let out_of_budget = attempt >= fork.limits().max_attempts;
    let past_deadline = now >= next.deadline;
    let operation = fork.operation();
    // Seeding follows placement, not a seeded fork's frozen holders.
    let replicas = state.placed_replicas(db)?;
    if next.reseed && (out_of_budget || past_deadline) {
        clear_lease(state, identity);
    }
    if out_of_budget {
        return fail(
            state,
            now,
            identity,
            attempt,
            ForkFailure::RetriesExhausted { last: cause },
        );
    }
    if past_deadline {
        return fail(
            state,
            now,
            identity,
            attempt,
            ForkFailure::DeadlineExceeded { last: cause },
        );
    }
    let live = next_seeder(
        state,
        &replicas,
        &next.seeder_excluded,
        &next.disk_refused,
        now,
    );
    // A fork seeding again goes on even if no replica is live now: the seeder timeout moves it
    // on, within its budget.
    let fallback = || {
        if next.reseed {
            replicas
                .iter()
                .find(|id| !next.disk_refused.contains(*id))
                .cloned()
        } else {
            None
        }
    };
    let Some(seeder) = live.or_else(fallback) else {
        return Err(Rejection::NoCandidate {
            database: identity.name.clone(),
        });
    };
    let following = attempt.saturating_add(1);

    if next.reseed {
        clear_lease(state, identity);
    }
    set_fork_state(
        state,
        identity,
        ForkState::Seeding {
            attempt: following,
            seeder: seeder.clone(),
            seeder_excluded: next.seeder_excluded,
            disk_refused: next.disk_refused,
            attempt_started_at: now,
            deadline: next.deadline,
            last_cause: Some(cause),
        },
    )?;
    set_outcome(
        state,
        operation,
        OperationOutcome::Pending { attempt: following },
        now,
    );
    Ok(Applied::ForkRetried {
        database: identity.name.clone(),
        attempt: following,
        seeder,
        cause,
    })
}

/// Applies `RetryForkSeed` (ADR-0046, point 3.3).
pub(crate) fn retry_fork_seed(
    state: &mut State,
    now: Timestamp,
    identity: &ForkIdentity,
    attempt: u32,
    cause: RetryCause,
    exclude_seeder: Option<NodeId>,
) -> Outcome {
    let (_, fork) = current_fork(state, identity)?;
    let current = seeding_at(fork, identity, attempt)?;
    let mut seeder_excluded = current.seeder_excluded;
    seeder_excluded.extend(exclude_seeder);
    next_attempt(
        state,
        now,
        identity,
        attempt,
        cause,
        NextAttempt {
            seeder_excluded,
            disk_refused: current.disk_refused,
            deadline: current.deadline,
            reseed: false,
        },
    )
}

/// Applies `RecordForkDiskRefusal` (ADR-0046, point 3.4).
pub(crate) fn record_disk_refusal(
    state: &mut State,
    now: Timestamp,
    identity: &ForkIdentity,
    attempt: u32,
    node: NodeId,
) -> Outcome {
    let (db, fork) = current_fork(state, identity)?;
    let current = seeding_at(fork, identity, attempt)?;
    let replicas = state.replica_set(&identity.name)?;
    if !replicas.contains(&node) {
        return Err(Rejection::NotInReplicaSet {
            id: node,
            database: identity.name.clone(),
        });
    }
    let mut disk_refused = current.disk_refused;
    disk_refused.insert(node.clone());
    let with_room = replicas
        .iter()
        .filter(|replica| !disk_refused.contains(*replica))
        .count();
    if with_room < completion_quorum_of(db) {
        return fail(state, now, identity, attempt, ForkFailure::InsufficientDisk);
    }
    // A seeder that refused moves on to the next attempt when another replica can seed;
    // until one can, the refusal is recorded and the attempt runs on, bounded by its timeout
    // and the deadline, and the driver starts the next attempt once a seeder is live.
    let replacement = next_seeder(
        state,
        &replicas,
        &current.seeder_excluded,
        &disk_refused,
        now,
    );
    if node == current.seeder && replacement.is_some() {
        return next_attempt(
            state,
            now,
            identity,
            attempt,
            RetryCause::SeederDiskRefused,
            NextAttempt {
                seeder_excluded: current.seeder_excluded,
                disk_refused,
                deadline: current.deadline,
                reseed: false,
            },
        );
    }
    set_fork_state(
        state,
        identity,
        ForkState::Seeding {
            attempt,
            seeder: current.seeder,
            seeder_excluded: current.seeder_excluded,
            disk_refused,
            attempt_started_at: current.attempt_started_at,
            deadline: current.deadline,
            last_cause: current.last_cause,
        },
    )?;
    Ok(Applied::ForkDiskRefusalRecorded {
        database: identity.name.clone(),
        node,
    })
}

/// Applies `FailFork` (ADR-0046, point 3.5).
pub(crate) fn fail_fork(
    state: &mut State,
    now: Timestamp,
    identity: &ForkIdentity,
    attempt: u32,
    reason: ForkFailure,
) -> Outcome {
    let (_, fork) = current_fork(state, identity)?;
    seeding_at(fork, identity, attempt)?;
    fail(state, now, identity, attempt, reason)
}

/// Fails the current fork `identity`, already checked, after `attempts`.
fn fail(
    state: &mut State,
    now: Timestamp,
    identity: &ForkIdentity,
    attempts: u32,
    reason: ForkFailure,
) -> Outcome {
    let (_, fork) = current_fork(state, identity)?;
    let operation = fork.operation();
    set_fork_state(
        state,
        identity,
        ForkState::Failed {
            reason,
            attempts,
            failed_at: now,
        },
    )?;
    set_outcome(state, operation, OperationOutcome::Failed { reason }, now);
    Ok(Applied::ForkFailed {
        database: identity.name.clone(),
        reason,
    })
}

/// Applies `DropForkHolder` (ADR-0046, point 3.6).
pub(crate) fn drop_fork_holder(
    state: &mut State,
    now: Timestamp,
    identity: &ForkIdentity,
    attempt: u32,
    holder: NodeId,
    cause: RetryCause,
) -> Outcome {
    let (db, fork) = current_fork(state, identity)?;
    let ForkState::Seeded {
        attempt: seeded_at,
        at,
        chain_hash,
        holders,
        lease_from,
        completed_at,
    } = fork.state()
    else {
        return Err(Rejection::ForkNotSeeded {
            database: identity.name.clone(),
        });
    };
    if *seeded_at != attempt {
        return Err(Rejection::ForkNotSeeded {
            database: identity.name.clone(),
        });
    }
    let gone = state.member(&holder).is_none();
    let lost = db.epoch() == *lease_from
        && state
            .report(&holder, &identity.name)
            .and_then(crate::Report::copy_lost)
            .is_some_and(|lost| *lost.fork() == *identity && lost.attempt() == attempt);
    if !holders.contains(&holder) || !(gone || lost) {
        return Err(Rejection::HolderNotDroppable {
            id: holder,
            database: identity.name.clone(),
        });
    }
    let mut remaining = holders.clone();
    remaining.remove(&holder);
    if remaining.len() < completion_quorum_of(db) {
        // Time spent seeded isn't spent seeding: the next attempt has a fresh deadline.
        let deadline = now.saturating_add(fork.limits().deadline);
        return next_attempt(
            state,
            now,
            identity,
            attempt,
            cause,
            NextAttempt {
                seeder_excluded: BTreeSet::new(),
                disk_refused: BTreeSet::new(),
                deadline,
                reseed: true,
            },
        );
    }
    let seeded = ForkState::Seeded {
        attempt,
        at: at.clone(),
        chain_hash: chain_hash.clone(),
        holders: remaining,
        lease_from: *lease_from,
        completed_at: *completed_at,
    };

    set_fork_state(state, identity, seeded)?;
    Ok(Applied::ForkHolderDropped {
        database: identity.name.clone(),
        holder,
        cause,
    })
}

/// Applies `MarkForkReady` (ADR-0046, point 3.7).
pub(crate) fn mark_fork_ready(
    state: &mut State,
    now: Timestamp,
    identity: &ForkIdentity,
    epoch: LeaseEpoch,
    reset_position: Position,
) -> Outcome {
    let (db, fork) = current_fork(state, identity)?;
    let ForkState::Seeded {
        at,
        chain_hash,
        completed_at,
        lease_from,
        ..
    } = fork.state()
    else {
        return Err(Rejection::ForkNotSeeded {
            database: identity.name.clone(),
        });
    };
    // Only a reset written under a lease issued since this completion counts: an earlier
    // attempt's reset marks another copy. A ready fork always has a lease.
    if epoch <= *lease_from || epoch > db.epoch() || db.lease().is_none() {
        return Err(Rejection::StaleEpoch {
            database: identity.name.clone(),
            given: epoch,
            current: db.epoch(),
        });
    }
    let need = usize::from(bootstrap_quorum(db.replication_factor(), db.durability()));
    if resets_reported(state, identity, epoch, &reset_position) < need {
        return Err(Rejection::ResetNotReported {
            database: identity.name.clone(),
        });
    }
    let ready = ForkState::Ready {
        at: at.clone(),
        chain_hash: chain_hash.clone(),
        completed_at: *completed_at,
        reset_epoch: epoch,
        reset_position: reset_position.clone(),
        ready_at: now,
    };
    let outcome = OperationOutcome::Ready {
        at: at.clone(),
        chain_hash: chain_hash.clone(),
    };
    let operation = fork.operation();

    set_fork_state(state, identity, ready)?;
    set_outcome(state, operation, outcome, now);
    Ok(Applied::ForkReady {
        database: identity.name.clone(),
        epoch,
        reset_position,
    })
}

/// Checks a lease grant on `db` to `node` against the fork rules (ADR-0046, point 3.9): no
/// lease while seeding or failed, and a seeded fork's lease, always its first since it was
/// seeded, only to a holder whose latest report still shows the staged copy it completed with,
/// at log epoch 0. The caller has checked that `node` is in the replica set, which for a seeded
/// fork is its holders.
///
/// # Errors
///
/// [`Rejection::LeaseOnUnseededFork`] or [`Rejection::StagedCopyNotReported`].
pub(crate) fn check_lease(state: &State, db: &Database, node: &NodeId) -> Result<(), Rejection> {
    let Some(fork) = db.fork() else {
        return Ok(());
    };
    match fork.state() {
        ForkState::Seeding { .. } | ForkState::Failed { .. } => {
            Err(Rejection::LeaseOnUnseededFork {
                database: db.name().clone(),
            })
        }
        // `AcquireLease` needs no lease, and a seeded fork has none only before its first:
        // completion records the epoch, and seeding again clears the lease.
        ForkState::Seeded { holders, .. } => {
            let shown = holders.contains(node)
                && state
                    .report(node, db.name())
                    .is_some_and(|report| shows_seeded_copy(db, fork, report));
            if shown {
                Ok(())
            } else {
                Err(Rejection::StagedCopyNotReported {
                    id: node.clone(),
                    database: db.name().clone(),
                })
            }
        }
        _ => Ok(()),
    }
}

/// Records what removing `db` means for its fork's operation: a fork removed before it was
/// ready, seeding or seeded, is cancelled for `cause` (ADR-0046, points 3.8 and 5.5); a ready
/// or failed one keeps its terminal outcome.
pub(crate) fn dropped(state: &mut State, db: &Database, now: Timestamp, cause: CancelCause) {
    let Some(fork) = db.fork() else {
        return;
    };
    let operation = fork.operation();
    let ours = state
        .fork_operations
        .get(&operation)
        .is_some_and(|record| record.target_incarnation() == db.incarnation());
    if ours {
        // A terminal outcome, ready or failed, never changes.
        set_outcome(state, operation, OperationOutcome::Cancelled { cause }, now);
    }
}

/// What a `Tick` does for forks: fails every seeding fork whose deadline has passed, then
/// forgets every operation record past its retention (ADR-0046, points 3.11 and 5.3).
pub(crate) fn tick(state: &mut State, now: Timestamp) {
    let overdue: Vec<(ForkIdentity, u32, Option<RetryCause>)> = state
        .databases()
        .filter_map(|db| {
            let fork = db.fork()?;
            match fork.state() {
                ForkState::Seeding {
                    attempt,
                    deadline,
                    last_cause,
                    ..
                } if now >= *deadline => Some((identity_of(db, fork), *attempt, *last_cause)),
                _ => None,
            }
        })
        .collect();
    for (identity, attempt, last_cause) in overdue {
        let reason = ForkFailure::DeadlineExceeded {
            last: last_cause.unwrap_or(RetryCause::AttemptTimedOut),
        };
        // Found just above, so it can't be refused.
        let _failed = fail(state, now, &identity, attempt, reason);
    }
    state
        .fork_operations
        .retain(|_, record| !record.is_forgotten_at(now));
}

/// Returns whether a `Tick` at `now` would change anything for forks.
pub(crate) fn tick_due(state: &State, now: Timestamp) -> bool {
    let overdue = state.databases().any(|db| {
        db.fork().is_some_and(
            |fork| matches!(fork.state(), ForkState::Seeding { deadline, .. } if now >= *deadline),
        )
    });
    overdue
        || state
            .fork_operations
            .values()
            .any(|record| record.is_forgotten_at(now))
}

/// Withdraws a seeded fork's lease and any promotion as it seeds again or fails, keeping its
/// epoch. No client was ever served, and the next first lease takes a higher epoch, which
/// fences any primary of the earlier attempt.
fn clear_lease(state: &mut State, identity: &ForkIdentity) {
    if let Some(db) = state.databases.get_mut(&identity.name) {
        db.clear_lease();
    }
}

fn set_fork_state(
    state: &mut State,
    identity: &ForkIdentity,
    fork_state: ForkState,
) -> Result<(), Rejection> {
    let fork = state
        .databases
        .get_mut(&identity.name)
        .and_then(Database::fork_mut)
        .ok_or_else(|| Rejection::StaleForkIdentity {
            fork: identity.clone(),
        })?;
    fork.set_state(fork_state);
    Ok(())
}

fn set_outcome(
    state: &mut State,
    operation: OperationId,
    outcome: OperationOutcome,
    now: Timestamp,
) {
    if let Some(record) = state.fork_operations.get_mut(&operation) {
        record.set_outcome(outcome, now);
    }
}
