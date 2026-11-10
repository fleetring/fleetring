//! Forks after an independent review of control state: resets from an earlier attempt, refused
//! proposals the driver mustn't repeat, stale copies in promotions, passing over a failing
//! seeder, and fleets too small for a fork (ADR-0046).

use fleetring_control::fork::{ForkIdentity, ForkState, RetryCause, SeedFailureReason, StagedCopy};
use fleetring_control::{
    Acks, Action, Applied, Durability, HostedReplica, Incarnation, LeaseEpoch, Member, Position,
    Rejection, ReplicaRole, State, fork,
};

use crate::common::fork::{
    copy_hash, drive, first_lease, fleet, fork_request, fork_state, heartbeat_of, identity,
    replica, report_failure, report_reset, seeded, seeder, with_intent,
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

/// A heartbeat from `node` reporting a staged copy of `fork` from `attempt` at `at`.
fn staged_at(node: &str, fork: &ForkIdentity, attempt: u32, at: &[u8]) -> Action {
    let copy = StagedCopy::new(
        fork.clone(),
        attempt,
        Position::new(at),
        copy_hash(),
        Incarnation::LEGACY,
    );
    heartbeat_of(
        node,
        vec![replica(fork.name.as_str()).with_staged(copy)],
        None,
    )
}

/// A `CompleteFork` of `fork` at `attempt` and `at` by `holders`.
fn complete_at(fork: &ForkIdentity, attempt: u32, at: &[u8], holders: &[String]) -> Action {
    Action::CompleteFork {
        fork: fork.clone(),
        attempt,
        at: Position::new(at),
        chain_hash: copy_hash(),
        holders: holders.iter().map(|node| id(node)).collect(),
    }
}

/// A heartbeat from `node` reporting a copy of `name` that isn't staged, whose log came from
/// the primary of `log_epoch`, at `position`.
fn opened(node: &str, name: &str, log_epoch: u64, position: &[u8]) -> Action {
    let report = HostedReplica::new(db(name), ReplicaRole::Follower, Position::new(position))
        .with_log_epoch(LeaseEpoch::new(log_epoch));
    heartbeat_of(node, vec![report], None)
}

fn join(state: &mut State, millis: u64, node: &str) {
    let member = Member::new(id(node)).with_software(current_software());
    run(state, millis, Action::Join { member }).expect("join");
}

// ---- Resets from an earlier attempt ----

/// A leased fork whose holders leave below the completion quorum keeps every holder and its
/// lease, rather than seeding again: its primary may have acknowledged writes once the reset
/// committed, and the resets its holders committed under the lease still mark it ready
/// (ADR-0046, point 3.6, as built). A fork seeds again only before any lease since completion,
/// so its epoch is still the completion's, and a reset from an earlier attempt, written under an
/// epoch at or below that, never counts.
#[test]
fn a_leased_fork_whose_holders_leave_becomes_ready_on_its_resets() {
    let mut state = fleet(&["n0", "n1", "n2", "n3", "n4", "n5", "n6"]);
    // R = 5 with min_replicas = 4: the completion quorum is 4, the bootstrap quorum 3.
    let request = with_intent(fork_request(1, "t"), |intent| {
        intent.replication_factor = factor(5);
        intent.durability = Some(
            Durability::with_min_replicas(Acks::Quorum, 4, factor(5)).expect("valid durability"),
        );
    });
    run(&mut state, 10, request).expect("fork");
    let fork = identity(&state, "t");
    let first = replicas(&state, "t");
    for node in &first {
        run(&mut state, 20, staged_at(node, &fork, 1, &[1, 0])).expect("hb");
    }
    run(&mut state, 20, complete_at(&fork, 1, &[1, 0], &first)).expect("complete");
    first_lease(&mut state, 30, &first[0]).expect("lease at epoch 1");
    // The primary commits the reset at the bootstrap quorum, but before readiness is recorded
    // two holders leave, below the completion quorum.
    for node in &first[..3] {
        run(
            &mut state,
            40,
            report_reset(node, &fork, LeaseEpoch::new(1), &[3]),
        )
        .expect("hb");
    }
    for node in &first[3..] {
        run(&mut state, 50, Action::Leave { id: id(node) }).expect("leave");
    }
    let ready = drive(&mut state, 60);
    assert!(
        matches!(ready.as_slice(), [(Action::MarkForkReady { epoch, .. }, Ok(_))] if *epoch == LeaseEpoch::new(1)),
        "{ready:?}"
    );
    assert!(
        matches!(fork_state(&state, "t"), ForkState::Ready { holders: Some(kept), .. } if kept.len() == first.len()),
        "{:?}",
        fork_state(&state, "t")
    );
    let database = state.database(&db("t")).expect("t");
    assert!(database.lease().is_some());
}

// ---- Proposals the driver mustn't repeat ----

/// A seeder that refuses for want of disk while no other replica can seed is recorded as
/// refused, and the attempt runs on: the driver proposes nothing more until a replica is live,
/// then starts the next attempt.
#[test]
fn a_seeder_out_of_disk_with_no_replacement_is_recorded_once() {
    let mut state = fleet(&["n0", "n1", "n2"]);
    run(&mut state, 10, fork_request(1, "t")).expect("fork");
    let fork = identity(&state, "t");
    let order = replicas(&state, "t");
    let refusal = report_failure(&order[0], &fork, 1, SeedFailureReason::InsufficientDisk);
    // The other two went quiet at 0 and are down by 4 s.
    run(&mut state, 4_000, refusal).expect("hb");
    let proposed = drive(&mut state, 4_000);
    assert_eq!(
        proposed,
        vec![(
            Action::RecordForkDiskRefusal {
                fork: fork.clone(),
                attempt: 1,
                node: id(&order[0]),
            },
            Ok(Applied::ForkDiskRefusalRecorded {
                database: db("t"),
                node: id(&order[0]),
            })
        )]
    );
    assert!(
        matches!(fork_state(&state, "t"), ForkState::Seeding { attempt: 1, disk_refused, .. } if disk_refused.contains(&id(&order[0])))
    );
    assert!(fork::actions(&state, at(4_001)).is_empty());
    assert!(fork::actions(&state, at(4_500)).is_empty());

    run(&mut state, 5_000, heartbeat_of(&order[1], Vec::new(), None)).expect("hb");
    let proposed = drive(&mut state, 5_000);
    assert!(
        matches!(
            proposed.as_slice(),
            [(
                Action::RetryForkSeed {
                    cause: RetryCause::SeederDiskRefused,
                    ..
                },
                Ok(Applied::ForkRetried { .. })
            )]
        ),
        "{proposed:?}"
    );
    assert_eq!(seeder(&state, "t"), order[1]);
}

/// With every member gone, a seeded fork can't seed again anywhere: the driver proposes no
/// `DropForkHolder` that `apply` would refuse on every tick.
#[test]
fn the_driver_proposes_nothing_for_a_fork_in_an_empty_fleet() {
    let mut state = seeded();
    for node in ["n0", "n1", "n2"] {
        run(&mut state, 30, Action::Leave { id: id(node) }).expect("leave");
    }
    assert!(fork::actions(&state, at(40)).is_empty());
    assert!(fork::actions(&state, at(10_000)).is_empty());
}

// ---- Stale copies in promotions ----

/// While a fork is seeded, a copy that isn't staged is a promotion candidate only if its log
/// comes from a lease issued since the completion: an undeleted copy of an earlier database of
/// the same name never outranks the staged copies.
#[test]
fn a_stale_copy_never_outranks_the_staged_copies() {
    let start = |state: &mut State, millis| match run(
        state,
        millis,
        Action::StartPromotion { database: db("t") },
    ) {
        Ok(Applied::PromotionStarted { candidate, .. }) => candidate.as_str().to_owned(),
        other => panic!("{other:?}"),
    };

    // An undeleted copy of an earlier database of the same name, at a higher epoch.
    let mut state = seeded();
    first_lease(&mut state, 30, "n0").expect("lease at epoch 1");
    run(&mut state, 1_500, opened("n1", "t", 5, &[9, 9])).expect("hb");
    let candidate = start(&mut state, 1_500);
    assert_ne!(candidate, "n1");

    // A follower of the fork's own lease is a candidate, and outranks the staged copies.
    let mut state = seeded();
    first_lease(&mut state, 30, "n0").expect("lease at epoch 1");
    run(&mut state, 1_500, opened("n2", "t", 1, &[1, 1])).expect("hb");
    assert_eq!(start(&mut state, 1_500), "n2");
}

// ---- Fleets too small for a fork ----

/// A fork whose replica set can't reach its completion quorum is refused as such, not as a
/// shortage of disk.
#[test]
fn a_fleet_too_small_for_the_completion_quorum_is_its_own_refusal() {
    let mut state = fleet(&["n0", "n1"]);
    let five = with_intent(fork_request(1, "t"), |intent| {
        intent.replication_factor = factor(5);
    });
    refused(
        &mut state,
        10,
        five,
        &Rejection::TooFewReplicas {
            database: db("t"),
            needed: 3,
            available: 2,
        },
    );
    join(&mut state, 20, "n2");
    let five = with_intent(fork_request(1, "t"), |intent| {
        intent.replication_factor = factor(5);
    });
    run(&mut state, 20, five).expect("three replicas reach the quorum of five");
}
