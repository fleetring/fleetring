//! A fork's life between its request and readiness: cancellation, resubmission, the frozen
//! replica set, holders that lose their copy, how staged copies rank in a promotion, majorities
//! whatever the durability, and why a fork fails (ADR-0046).

use std::collections::BTreeSet;

use fleetring_control::fork::{
    CancelCause, ChainHash, ForkExpiry, ForkFailure, ForkState, OperationOutcome, RetryCause,
    StagedCopy,
};
use fleetring_control::{
    Acks, Action, Applied, Durability, HostedReplica, Incarnation, LeaseEpoch, Member, Millis,
    Position, Rejection, ReplicaRole, State, fork, membership_ring,
};

use crate::common::fork::{
    SOURCE, attempt, complete, create_at, drive, first_lease, fleet, fork_request, fork_state,
    heartbeat_of, identity, limits, op, replica, report_copy_lost, report_reset, report_staged,
    seeded, staged, with_intent,
};
use crate::common::{at, current_software, db, factor, id, run};

fn refused(state: &mut State, millis: u64, action: Action, expected: &Rejection) {
    let before = state.clone();
    let outcome = run(state, millis, action.clone());
    assert_eq!(outcome.as_ref().err(), Some(expected), "{action:?}");
    assert_eq!(*state, before, "a refused {action:?} changed the state");
}

fn replicas(state: &State, name: &str) -> Vec<String> {
    state
        .replica_set(&db(name))
        .expect("a replica set")
        .iter()
        .map(|node| node.as_str().to_owned())
        .collect()
}

/// Where placement puts `name` with `replication_factor` replicas over the current members.
fn placed(state: &State, name: &str, replication_factor: u8) -> Vec<String> {
    membership_ring(state.members(), state.membership_epoch())
        .expect("a ring")
        .replicas_n(name, factor(replication_factor))
        .to_ids()
        .iter()
        .map(|node| node.as_str().to_owned())
        .collect()
}

/// [`seeded`], then leased on `n0` at 30 ms and made ready at 40 ms on reset reports from `n0`
/// and `n1`.
fn ready() -> State {
    let mut state = seeded();
    let fork = identity(&state, "t");
    first_lease(&mut state, 30, "n0").expect("lease");
    for node in ["n0", "n1"] {
        run(
            &mut state,
            35,
            report_reset(node, &fork, LeaseEpoch::new(1), &[3]),
        )
        .expect("hb");
    }
    let proposed = drive(&mut state, 40);
    assert!(
        matches!(proposed.as_slice(), [(Action::MarkForkReady { .. }, Ok(_))]),
        "{proposed:?}"
    );
    state
}

fn outcome(state: &State, millis: u64) -> Option<OperationOutcome> {
    state
        .fork_operation(&op(1), at(millis))
        .map(|record| record.outcome().clone())
}

// ---- Cancellation ----

/// Dropping a fork that isn't ready, seeding or seeded, cancels its operation, and a resume
/// afterwards answers `Cancelled`; a ready or failed fork keeps its terminal outcome. Retention
/// starts at the terminal outcome.
#[test]
fn a_drop_in_each_state_ends_the_operation_as_the_rules_say() {
    let dropped = OperationOutcome::Cancelled {
        cause: CancelCause::Dropped,
    };

    let mut seeding = fleet(&["n0", "n1", "n2"]);
    run(&mut seeding, 10, fork_request(1, "t")).expect("fork");
    assert_eq!(
        outcome(&seeding, 10),
        Some(OperationOutcome::Pending { attempt: 1 })
    );
    run(&mut seeding, 50, Action::DropDatabase { name: db("t") }).expect("drop");
    assert_eq!(outcome(&seeding, 50), Some(dropped.clone()));

    let mut seeded = seeded();
    assert_eq!(
        seeded
            .fork_operation(&op(1), at(20))
            .and_then(|r| r.forgotten_at()),
        None,
        "a seeded operation isn't terminal, so its retention hasn't started"
    );
    run(&mut seeded, 50, Action::DropDatabase { name: db("t") }).expect("drop");
    assert_eq!(outcome(&seeded, 50), Some(dropped.clone()));
    assert_eq!(
        seeded
            .fork_operation(&op(1), at(50))
            .and_then(|r| r.forgotten_at()),
        Some(at(50 + limits().operation_retention.get()))
    );
    // A resume after the drop answers with the cancelled operation and makes nothing.
    let resumed = run(&mut seeded, 60, fork_request(1, "t")).expect("resumed");
    assert!(
        matches!(&resumed, Applied::ForkResent { outcome, .. } if *outcome == dropped),
        "{resumed:?}"
    );
    assert!(seeded.database(&db("t")).is_none());

    let mut ready = ready();
    run(&mut ready, 50, Action::DropDatabase { name: db("t") }).expect("drop");
    assert!(matches!(
        outcome(&ready, 50),
        Some(OperationOutcome::Ready { .. })
    ));

    let mut failed = fleet(&["n0", "n1", "n2"]);
    run(&mut failed, 10, fork_request(1, "t")).expect("fork");
    let fail = Action::FailFork {
        fork: identity(&failed, "t"),
        attempt: 1,
        reason: ForkFailure::SourceTooLarge,
    };
    run(&mut failed, 20, fail).expect("fail");
    run(&mut failed, 50, Action::DropDatabase { name: db("t") }).expect("drop");
    assert_eq!(
        outcome(&failed, 50),
        Some(OperationOutcome::Failed {
            reason: ForkFailure::SourceTooLarge
        })
    );
    assert_eq!(
        failed
            .fork_operation(&op(1), at(50))
            .and_then(|r| r.forgotten_at()),
        Some(at(20 + limits().operation_retention.get()))
    );
}

/// A fork removed by its expiry before it is ready is cancelled too, with the cause recorded.
#[test]
fn an_expired_fork_that_is_not_ready_is_cancelled() {
    let mut state = fleet(&["n0", "n1", "n2"]);
    let expiring = with_intent(fork_request(1, "t"), |intent| {
        intent.expiry = ForkExpiry::After(Millis::new(5_000));
    });
    run(&mut state, 10, expiring).expect("fork");
    assert_eq!(state.expired_databases(at(5_010)), [db("t")]);
    run(&mut state, 5_010, Action::ExpireDatabase { name: db("t") }).expect("expired");
    assert_eq!(
        outcome(&state, 5_010),
        Some(OperationOutcome::Cancelled {
            cause: CancelCause::Expired
        })
    );
}

// ---- Resume and resubmission ----

/// A resubmission resolves nothing again: after the source is dropped and recreated it answers
/// with the original operation, first pending and then failed, and never forks the new source.
#[test]
fn a_resume_after_the_source_is_recreated_returns_the_original_outcome() {
    let mut state = fleet(&["n0", "n1", "n2"]);
    run(&mut state, 10, fork_request(1, "t")).expect("fork");
    let incarnation = identity(&state, "t").incarnation;
    run(&mut state, 20, Action::DropDatabase { name: db(SOURCE) }).expect("drop");
    run(&mut state, 30, create_at(SOURCE, 3, 0x5A)).expect("recreate");

    let resumed = run(&mut state, 40, fork_request(1, "t")).expect("resumed");
    assert_eq!(
        resumed,
        Applied::ForkResent {
            operation: op(1),
            database: db("t"),
            incarnation,
            outcome: OperationOutcome::Pending { attempt: 1 },
        }
    );
    let record = state.fork_operation(&op(1), at(40)).expect("the record");
    assert_eq!(record.source_incarnation(), Incarnation::LEGACY);

    let proposed = drive(&mut state, 50);
    assert!(
        matches!(
            proposed.as_slice(),
            [(
                Action::FailFork {
                    reason: ForkFailure::SourceRecreated,
                    ..
                },
                Ok(_)
            )]
        ),
        "{proposed:?}"
    );
    let resumed = run(&mut state, 60, fork_request(1, "t")).expect("resumed");
    assert_eq!(
        resumed,
        Applied::ForkResent {
            operation: op(1),
            database: db("t"),
            incarnation,
            outcome: OperationOutcome::Failed {
                reason: ForkFailure::SourceRecreated
            },
        }
    );
    assert_eq!(identity(&state, "t").incarnation, incarnation);
}

/// A relative expiry is part of the intent and resolved once: the same span resubmitted later
/// matches, and the expiry doesn't move.
#[test]
fn a_relative_expiry_resubmitted_later_matches() {
    let mut state = fleet(&["n0", "n1", "n2"]);
    let in_an_hour = |action| {
        with_intent(action, |intent| {
            intent.expiry = ForkExpiry::After(Millis::new(3_600_000));
        })
    };
    run(&mut state, 10, in_an_hour(fork_request(1, "t"))).expect("fork");
    let expires_at = at(3_600_010);
    let resumed = run(&mut state, 50_000, in_an_hour(fork_request(1, "t"))).expect("resumed");
    assert!(matches!(resumed, Applied::ForkResent { .. }), "{resumed:?}");
    let record = state
        .fork_operation(&op(1), at(50_000))
        .expect("the record");
    assert_eq!(record.expires_at(), Some(expires_at));
    assert_eq!(
        record.intent().expiry,
        ForkExpiry::After(Millis::new(3_600_000))
    );
    let metadata = state.database(&db("t")).and_then(|d| d.metadata().cloned());
    assert_eq!(metadata.and_then(|m| m.expires_at()), Some(expires_at));
}

/// `admin` on the target reads an operation only while the target is the database it made: a
/// drop and recreate under the same name hides the earlier record from it.
#[test]
fn a_targets_admin_sees_only_the_operation_that_made_that_incarnation() {
    let mut state = fleet(&["n0", "n1", "n2"]);
    run(&mut state, 10, fork_request(1, "t")).expect("fork");
    assert!(state.fork_operation_of_target(&op(1), at(10)).is_some());
    run(&mut state, 20, Action::DropDatabase { name: db("t") }).expect("drop");
    assert!(state.fork_operation_of_target(&op(1), at(20)).is_none());
    run(&mut state, 30, create_at("t", 3, 0x7E)).expect("recreate");
    assert!(state.fork_operation_of_target(&op(1), at(30)).is_none());
    // The requester still reads it.
    assert!(state.fork_operation(&op(1), at(30)).is_some());
}

// ---- The frozen replica set ----

/// Between completion and readiness a fork's replica set is its holders, whatever placement
/// says; once ready it follows placement like any database.
#[test]
fn a_seeded_forks_replica_set_is_frozen_until_ready() {
    let mut state = fleet(&["n0", "n1", "n2"]);
    run(&mut state, 10, fork_request(1, "t")).expect("fork");
    let fork = identity(&state, "t");
    let order = replicas(&state, "t");
    let holders = [order[0].as_str(), order[1].as_str()];
    for node in holders {
        run(&mut state, 20, report_staged(node, &fork, 1)).expect("hb");
    }
    run(&mut state, 20, complete(&fork, 1, &holders)).expect("complete");
    // The holders, in ID order.
    let mut frozen = holders.map(str::to_owned).to_vec();
    frozen.sort();
    assert_eq!(replicas(&state, "t"), frozen);

    // Placement changes: new members join.
    for node in ["n3", "n4", "n5"] {
        let member = Member::new(id(node)).with_software(current_software());
        run(&mut state, 25, Action::Join { member }).expect("join");
    }
    assert_ne!(placed(&state, "t", 3), frozen);
    assert_eq!(replicas(&state, "t"), frozen);
    assert_eq!(state.quorums(&db("t")).map(|q| q.replicas()), Ok(2));

    first_lease(&mut state, 30, holders[0]).expect("a holder takes the first lease");
    for node in holders {
        run(
            &mut state,
            35,
            report_reset(node, &fork, LeaseEpoch::new(1), &[3]),
        )
        .expect("hb");
    }
    let proposed = drive(&mut state, 40);
    assert!(
        matches!(proposed.as_slice(), [(Action::MarkForkReady { .. }, Ok(_))]),
        "{proposed:?}"
    );
    assert_eq!(replicas(&state, "t"), placed(&state, "t", 3));
}

// ---- Holders that lose their copy ----

/// A holder whose latest report no longer shows the staged copy the fork completed with, at log
/// epoch 0, can't take the first lease; the others still can.
#[test]
fn a_holder_that_lost_its_copy_is_refused_the_first_lease() {
    let base = seeded();
    let fork = identity(&base, "t");
    let not_reported = |node: &str| Rejection::StagedCopyNotReported {
        id: id(node),
        database: db("t"),
    };

    // No copy reported any more.
    let mut state = base.clone();
    run(&mut state, 25, heartbeat_of("n0", vec![replica("t")], None)).expect("hb");
    refused(
        &mut state,
        30,
        Action::AcquireLease {
            database: db("t"),
            node: id("n0"),
        },
        &not_reported("n0"),
    );
    // A report of a lost copy: the driver drops the holder, two still hold it.
    run(&mut state, 26, report_copy_lost("n1", &fork, 1)).expect("hb");
    assert_eq!(first_lease(&mut state, 30, "n1"), Err(not_reported("n1")));
    let proposed = drive(&mut state, 30);
    assert!(
        matches!(
            proposed.as_slice(),
            [(
                Action::DropForkHolder {
                    cause: RetryCause::HolderLost,
                    ..
                },
                Ok(Applied::ForkHolderDropped { .. })
            )]
        ),
        "{proposed:?}"
    );
    assert_eq!(replicas(&state, "t").len(), 2);
    first_lease(&mut state, 30, "n2").expect("n2 still holds the copy");

    // Another copy, or the right copy at a log epoch other than 0, doesn't count either.
    let other = StagedCopy::new(
        fork.clone(),
        1,
        Position::new([1, 0]),
        ChainHash::new([0]),
        Incarnation::LEGACY,
    );
    let mutant = HostedReplica::new(db("t"), ReplicaRole::Follower, Position::new([1, 0]))
        .with_log_epoch(LeaseEpoch::new(1))
        .with_staged(staged(&fork, 1));
    for report in [replica("t").with_staged(other), mutant] {
        let mut state = base.clone();
        run(&mut state, 26, heartbeat_of("n1", vec![report], None)).expect("hb");
        assert_eq!(first_lease(&mut state, 30, "n1"), Err(not_reported("n1")));
    }
}

/// When holders that lost their copy leave fewer than the completion quorum, before any lease,
/// the fork seeds again at the next attempt, following placement, with a fresh deadline.
#[test]
fn holders_below_the_quorum_return_the_fork_to_seeding() {
    let mut state = seeded();
    let fork = identity(&state, "t");
    for node in ["n0", "n1"] {
        run(&mut state, 25, report_copy_lost(node, &fork, 1)).expect("hb");
    }
    let first = drive(&mut state, 30);
    assert!(
        matches!(
            first.as_slice(),
            [(
                Action::DropForkHolder { .. },
                Ok(Applied::ForkHolderDropped { .. })
            )]
        ),
        "{first:?}"
    );
    let second = drive(&mut state, 31);
    assert!(
        matches!(
            second.as_slice(),
            [(
                Action::DropForkHolder {
                    cause: RetryCause::HolderLost,
                    ..
                },
                Ok(Applied::ForkRetried {
                    attempt: 2,
                    cause: RetryCause::HolderLost,
                    ..
                })
            )]
        ),
        "{second:?}"
    );
    assert_eq!(attempt(&state, "t"), 2);
    assert_eq!(
        outcome(&state, 31),
        Some(OperationOutcome::Pending { attempt: 2 })
    );
    assert_eq!(replicas(&state, "t"), placed(&state, "t", 3));
    let ForkState::Seeding { deadline, .. } = fork_state(&state, "t") else {
        panic!("seeding");
    };
    assert_eq!(deadline, at(31 + limits().deadline.get()));
    assert!(
        state
            .database(&db("t"))
            .is_some_and(|d| d.lease().is_none())
    );
}

/// A holder that lost its copy after the first lease isn't dropped: it is re-seeded from the
/// primary as any follower is.
#[test]
fn a_lost_copy_after_the_first_lease_keeps_the_holder() {
    let mut state = seeded();
    let fork = identity(&state, "t");
    first_lease(&mut state, 30, "n0").expect("lease");
    run(&mut state, 35, report_copy_lost("n1", &fork, 1)).expect("hb");
    assert!(fork::actions(&state, at(40)).is_empty());
    refused(
        &mut state,
        40,
        Action::DropForkHolder {
            fork,
            attempt: 1,
            holder: id("n1"),
            cause: RetryCause::HolderLost,
        },
        &Rejection::HolderNotDroppable {
            id: id("n1"),
            database: db("t"),
        },
    );
}

/// A seeded fork whose holders leave the fleet drops them one by one, and once fewer than the
/// completion quorum remain it seeds again, even after a lease: the lease is cleared, the epoch
/// kept, and the next first lease takes a higher epoch.
#[test]
fn a_seeded_fork_whose_holders_all_left_seeds_again() {
    let mut state = fleet(&["n0", "n1", "n2", "n3", "n4"]);
    run(&mut state, 10, fork_request(1, "t")).expect("fork");
    let fork = identity(&state, "t");
    let holders = replicas(&state, "t");
    for node in &holders {
        run(&mut state, 20, report_staged(node, &fork, 1)).expect("hb");
    }
    let named: Vec<&str> = holders.iter().map(String::as_str).collect();
    run(&mut state, 20, complete(&fork, 1, &named)).expect("complete");
    first_lease(&mut state, 30, &holders[0]).expect("lease");
    for node in &holders {
        run(&mut state, 40, Action::Leave { id: id(node) }).expect("leave");
    }
    let first = drive(&mut state, 50);
    assert!(
        matches!(
            first.as_slice(),
            [(
                Action::DropForkHolder {
                    cause: RetryCause::HoldersLeft,
                    ..
                },
                Ok(Applied::ForkHolderDropped { .. })
            )]
        ),
        "{first:?}"
    );
    let second = drive(&mut state, 51);
    assert!(
        matches!(
            second.as_slice(),
            [(
                Action::DropForkHolder { .. },
                Ok(Applied::ForkRetried {
                    cause: RetryCause::HoldersLeft,
                    ..
                })
            )]
        ),
        "{second:?}"
    );
    assert_eq!(attempt(&state, "t"), 2);
    let database = state.database(&db("t")).expect("t");
    assert!(database.lease().is_none());
    assert_eq!(database.epoch(), LeaseEpoch::new(1));

    // The members left seed it and take the next first lease at a higher epoch.
    let rest = replicas(&state, "t");
    for node in &rest {
        run(&mut state, 60, report_staged(node, &fork, 2)).expect("hb");
    }
    let proposed = drive(&mut state, 60);
    assert!(
        matches!(proposed.as_slice(), [(Action::CompleteFork { .. }, Ok(_))]),
        "{proposed:?}"
    );
    let granted = first_lease(&mut state, 70, &rest[0]).expect("lease");
    assert!(
        matches!(&granted, Applied::LeaseGranted { lease, .. } if lease.epoch() == LeaseEpoch::new(2)),
        "{granted:?}"
    );
}

// ---- Staged copies in promotions ----

/// A staged copy ranks at log epoch 0 and its copy's position, whatever its report claims, and
/// counts only if it is exactly the copy the fork completed with.
#[test]
fn a_staged_copy_ranks_at_log_epoch_zero() {
    let base = {
        let mut state = seeded();
        let order = replicas(&state, "t");
        first_lease(&mut state, 30, &order[0]).expect("lease");
        (state, order)
    };
    let (base, order) = base;
    let fork = identity(&base, "t");
    let (a, b, c) = (order[0].as_str(), order[1].as_str(), order[2].as_str());
    // `b` claims log epoch 5 at a far position for its staged copy.
    let mutant = HostedReplica::new(db("t"), ReplicaRole::Follower, Position::new([9, 9]))
        .with_log_epoch(LeaseEpoch::new(5))
        .with_staged(staged(&fork, 1));
    let follower = HostedReplica::new(db("t"), ReplicaRole::Follower, Position::new([1, 1]))
        .with_log_epoch(LeaseEpoch::new(1));

    let start = |state: &mut State| {
        let started = run(state, 2_000, Action::StartPromotion { database: db("t") });
        match started {
            Ok(Applied::PromotionStarted { candidate, .. }) => Ok(candidate.as_str().to_owned()),
            Ok(other) => panic!("{other:?}"),
            Err(rejection) => Err(rejection),
        }
    };

    // A replica holding the reset at (1, at + 1) outranks the staged copy at (0, at).
    let mut state = base.clone();
    run(
        &mut state,
        1_900,
        heartbeat_of(b, vec![mutant.clone()], None),
    )
    .expect("hb");
    run(&mut state, 1_900, heartbeat_of(c, vec![follower], None)).expect("hb");
    assert_eq!(start(&mut state), Ok(c.to_owned()));

    // Two staged copies tie at (0, at), whatever they claim; the earlier in preference wins.
    let mut state = base.clone();
    run(&mut state, 1_900, heartbeat_of(b, vec![mutant], None)).expect("hb");
    run(&mut state, 1_900, heartbeat_of(c, Vec::new(), None)).expect("hb");
    assert_eq!(start(&mut state), Ok(a.to_owned()));

    // A staged copy other than the completed one is no candidate.
    let mut state = base;
    let other = StagedCopy::new(
        fork,
        1,
        Position::new([9, 9]),
        ChainHash::new([0]),
        Incarnation::LEGACY,
    );
    run(
        &mut state,
        3_500,
        heartbeat_of(b, vec![replica("t").with_staged(other)], None),
    )
    .expect("hb");
    run(&mut state, 3_500, heartbeat_of(c, Vec::new(), None)).expect("hb");
    assert_eq!(
        run(
            &mut state,
            3_500,
            Action::StartPromotion { database: db("t") }
        ),
        Err(Rejection::NoCandidate { database: db("t") })
    );
}

// ---- Majorities whatever the durability ----

/// Under `acks = local`, W is 1, but completion still needs a majority of holders and readiness
/// a majority of reset reports.
#[test]
fn completion_and_readiness_need_a_majority_under_local_acks() {
    let mut state = fleet(&["n0", "n1", "n2"]);
    let local = with_intent(fork_request(1, "t"), |intent| {
        intent.durability = Some(Durability::new(Acks::Local, factor(3)));
    });
    run(&mut state, 10, local).expect("fork");
    let fork = identity(&state, "t");
    for node in ["n0", "n1", "n2"] {
        run(&mut state, 20, report_staged(node, &fork, 1)).expect("hb");
    }
    refused(
        &mut state,
        20,
        complete(&fork, 1, &["n0"]),
        &Rejection::NotEnoughHolders { have: 1, need: 2 },
    );
    run(&mut state, 20, complete(&fork, 1, &["n0", "n1"])).expect("complete");
    first_lease(&mut state, 30, "n0").expect("lease");
    let ready = Action::MarkForkReady {
        fork: fork.clone(),
        epoch: LeaseEpoch::new(1),
        reset_position: Position::new([3]),
    };
    run(
        &mut state,
        35,
        report_reset("n0", &fork, LeaseEpoch::new(1), &[3]),
    )
    .expect("hb");
    assert!(fork::actions(&state, at(36)).is_empty());
    refused(
        &mut state,
        36,
        ready.clone(),
        &Rejection::ResetNotReported { database: db("t") },
    );
    run(
        &mut state,
        37,
        report_reset("n1", &fork, LeaseEpoch::new(1), &[3]),
    )
    .expect("hb");
    assert_eq!(fork::actions(&state, at(38)), vec![ready.clone()]);
    run(&mut state, 38, ready).expect("ready");
}

/// A fork's durability can't change before it is ready.
#[test]
fn a_forks_durability_changes_only_once_it_is_ready() {
    let change = Action::ChangeDurability {
        database: db("t"),
        durability: Durability::new(Acks::All, factor(3)),
    };
    let not_ready = Rejection::ForkNotReady { database: db("t") };
    let mut seeding = fleet(&["n0", "n1", "n2"]);
    run(&mut seeding, 10, fork_request(1, "t")).expect("fork");
    refused(&mut seeding, 20, change.clone(), &not_ready);
    let mut seeded = seeded();
    refused(&mut seeded, 25, change.clone(), &not_ready);
    let mut ready = ready();
    run(&mut ready, 50, change).expect("a ready fork is an ordinary database");
}

// ---- Why a fork fails ----

/// Runs a fork whose every seeder stops sending heartbeats as soon as it is chosen, while every
/// disk is fine, until `until`, and returns the final state and every outcome the driver's
/// commands had.
fn seeders_keep_timing_out(max_attempts: u32, until: u64) -> (State, Vec<Applied>) {
    let mut state = fleet(&["n0", "n1", "n2"]);
    let mut request = fork_request(1, "t");
    if let Action::ForkDatabase { limits, .. } = &mut request {
        limits.max_attempts = max_attempts;
    }
    run(&mut state, 10, request).expect("fork");
    let mut were_seeders: BTreeSet<String> = BTreeSet::new();
    let mut applied = Vec::new();
    for millis in (1_000..=until).step_by(1_000) {
        if let ForkState::Seeding { seeder, .. } = fork_state(&state, "t") {
            were_seeders.insert(seeder.as_str().to_owned());
        }
        for node in ["n0", "n1", "n2"] {
            if !were_seeders.contains(node) {
                let disk = Some(1 << 40);
                run(&mut state, millis, heartbeat_of(node, Vec::new(), disk)).expect("hb");
            }
        }
        for (action, outcome) in drive(&mut state, millis) {
            applied.push(outcome.unwrap_or_else(|rejection| panic!("{action:?}: {rejection}")));
        }
        if let ForkState::Seeding { disk_refused, .. } = fork_state(&state, "t") {
            assert!(
                disk_refused.is_empty(),
                "a timeout counted as a disk refusal"
            );
        }
    }
    (state, applied)
}

/// A seeder that times out is only passed over as a seeder, never counted as disk-refused: the
/// fork fails because its budget or its deadline ran out, keeping the real cause, and never
/// with `InsufficientDisk` (ADR-0046, points 3.3 and 7.2).
#[test]
fn a_seeder_that_keeps_timing_out_never_counts_as_out_of_disk() {
    let (state, applied) = seeders_keep_timing_out(3, 30_000);
    let retried = applied
        .iter()
        .filter(|applied| {
            matches!(
                applied,
                Applied::ForkRetried {
                    cause: RetryCause::SeederDown,
                    ..
                }
            )
        })
        .count();
    assert_eq!(retried, 2, "{applied:?}");
    let ForkState::Failed {
        reason, attempts, ..
    } = fork_state(&state, "t")
    else {
        panic!("{:?}", fork_state(&state, "t"));
    };
    assert_eq!(
        (reason, attempts),
        (
            ForkFailure::RetriesExhausted {
                last: RetryCause::SeederDown
            },
            3
        )
    );

    // With attempts to spare and every replica passed over and down, the fork waits for a
    // seeder until its deadline, and fails keeping the last retry's cause.
    let deadline = 10 + limits().deadline.get();
    let (state, _) = seeders_keep_timing_out(5, deadline + 1_000);
    let failed = ForkFailure::DeadlineExceeded {
        last: RetryCause::SeederDown,
    };
    assert!(
        matches!(fork_state(&state, "t"), ForkState::Failed { reason, .. } if reason == failed),
        "{:?}",
        fork_state(&state, "t")
    );
    assert_eq!(
        outcome(&state, deadline + 1_000),
        Some(OperationOutcome::Failed { reason: failed })
    );
}

/// Passing over a seeder is a preference, not a ban: once every live replica was passed over,
/// the first of them in preference order seeds again.
#[test]
fn a_passed_over_seeder_is_chosen_again_when_no_other_is_live() {
    let mut state = fleet(&["n0", "n1", "n2"]);
    let mut request = fork_request(1, "t");
    if let Action::ForkDatabase { limits, .. } = &mut request {
        limits.max_attempts = 5;
    }
    run(&mut state, 10, request).expect("fork");
    let fork = identity(&state, "t");
    let order = replicas(&state, "t");
    let retry = |attempt, seeder: &str| Action::RetryForkSeed {
        fork: fork.clone(),
        attempt,
        cause: RetryCause::TransferFailed,
        exclude_seeder: Some(id(seeder)),
    };
    run(&mut state, 20, retry(1, &order[0])).expect("attempt 2");
    run(&mut state, 30, retry(2, &order[1])).expect("attempt 3");
    run(&mut state, 40, retry(3, &order[2])).expect("attempt 4");
    assert_eq!(attempt(&state, "t"), 4);
    let ForkState::Seeding {
        seeder,
        seeder_excluded,
        ..
    } = fork_state(&state, "t")
    else {
        panic!("seeding");
    };
    assert_eq!(seeder_excluded.len(), 3);
    assert_eq!(seeder.as_str(), order[0]);
}
