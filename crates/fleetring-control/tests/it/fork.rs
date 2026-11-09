//! Forks in control state (ADR-0046): incarnations, the fork record and its identity, the rules
//! `apply` enforces, operations, admission, and the fork driver.

use std::collections::BTreeSet;

use fleetring_control::fork::{
    CancelCause, ForkExpiry, ForkFailure, ForkIdentity, ForkIntent, ForkState, IntentField,
    OperationOutcome, ResetReport, RetryCause, SeedFailureReason, StagedCopy,
};
use fleetring_control::{
    Acks, Action, Applied, Durability, Feature, Incarnation, Labels, LeaseEpoch, Member, Millis,
    Position, Rejection, Release, Software, State, fork, parse_label,
};

use crate::common::fork::{
    SOURCE, accepted, attempt, complete, copy_at, copy_hash, create_at, drive, first_lease, fleet,
    fork_incarnation, fork_of, fork_request, fork_state, heartbeat_of, identity, limits, op,
    principal, replica, report_failure, report_reset, report_staged, seeded, seeder, staged,
    with_incarnation, with_intent,
};
use crate::common::{at, config, create, db, factor, id, run};

/// Applies `action` and checks it is refused with `expected`, leaving the state exactly as it
/// was.
fn refused(state: &mut State, millis: u64, action: Action, expected: &Rejection) {
    let before = state.clone();
    let outcome = run(state, millis, action.clone());
    assert_eq!(outcome.as_ref().err(), Some(expected), "{action:?}");
    assert_eq!(*state, before, "a refused {action:?} changed the state");
}

/// The replica set of `name`, as strings, in preference order.
fn replicas(state: &State, name: &str) -> Vec<String> {
    state
        .replica_set(&db(name))
        .expect("a replica set")
        .iter()
        .map(|node| node.as_str().to_owned())
        .collect()
}

fn stale(fork: &ForkIdentity) -> Rejection {
    Rejection::StaleForkIdentity { fork: fork.clone() }
}

// ---- Incarnations and the feature gate ----

/// Software with every feature but `fork`.
fn without_fork() -> Software {
    [Feature::Undrain, Feature::Metadata, Feature::Expiry]
        .into_iter()
        .fold(
            Software::new(Release::new(0, 7, 0), 1),
            Software::with_feature,
        )
}

/// Removes every field forks added from a serialized command or state.
fn strip_fork_fields(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::Object(map) => {
            for key in [
                "disk_free_bytes",
                "size_bytes",
                "staged",
                "seed_failure",
                "reset",
                "incarnation",
                "fork",
                "next_incarnation",
                "fork_operations",
            ] {
                map.remove(key);
            }
            map.values_mut().for_each(strip_fork_fields);
        }
        serde_json::Value::Array(items) => items.iter_mut().for_each(strip_fork_fields),
        _ => {}
    }
}

/// Until `fork` is enabled, no command assigns an incarnation or records what replicas report
/// about forks: the state is byte for byte what the same log, without the fields forks added,
/// gives, which is what a release before forks reads and writes (ADR-0046, points 1.2 and 8).
#[test]
fn without_the_fork_feature_control_state_stays_as_it_was() {
    let mut log = Vec::new();
    for node in ["n0", "n1", "n2"] {
        let member = Member::new(id(node)).with_software(without_fork());
        log.push(Action::Join { member });
    }
    let fork = ForkIdentity::new(db("a"), Incarnation::new(1), op(1));
    log.extend([
        create("a", 3),
        heartbeat_of(
            "n0",
            vec![
                replica("a")
                    .with_size_bytes(1 << 20)
                    .with_staged(staged(&fork, 1))
                    .with_reset(ResetReport::committed(
                        fork.clone(),
                        LeaseEpoch::new(1),
                        Position::new([1]),
                    )),
            ],
            Some(1 << 40),
        ),
        Action::DropDatabase { name: db("a") },
        // An incarnation in the command is ignored until forks are enabled.
        create_at("a", 3, 0x77),
        Action::Tick,
    ]);
    let mut state = State::new(config());
    for (step, action) in log.iter().enumerate() {
        let millis = u64::try_from(step).expect("small") * 10;
        run(&mut state, millis, action.clone()).expect("accepted");
    }
    assert!(!state.is_enabled(Feature::Fork));
    assert!(
        state
            .databases()
            .all(|database| database.incarnation() == Incarnation::LEGACY
                && database.fork().is_none()
                && database.is_seeded()
                && database.is_ready())
    );
    assert_eq!(state.disk_free_bytes(&id("n0")), None);
    let report = state.report(&id("n0"), &db("a"));
    assert!(report.is_none(), "the drop took the report: {report:?}");
    assert_eq!(state.fork_operations().len(), 0);

    // The same log, as a release before forks decodes it, gives the same bytes.
    let mut previous = State::new(config());
    for (step, action) in log.iter().enumerate() {
        let mut value = serde_json::to_value(action).expect("serializes");
        strip_fork_fields(&mut value);
        let action: Action = serde_json::from_value(value).expect("decodes");
        let millis = u64::try_from(step).expect("small") * 10;
        run(&mut previous, millis, action).expect("accepted");
    }
    let snapshot = serde_json::to_string(&state).expect("serializes");
    assert_eq!(
        snapshot,
        serde_json::to_string(&previous).expect("serializes")
    );
    for key in [
        "incarnation",
        "next_incarnation",
        "fork_operations",
        "disk_free_bytes",
        "size_bytes",
        "staged",
        "reset",
    ] {
        assert!(!snapshot.contains(key), "{key} in {snapshot}");
    }

    // A fork is refused before it is logged, naming the first member that lacks the feature.
    let request = fork_of(1, "t", "a", 3);
    let unsupported = Rejection::FeatureUnsupported {
        feature: Feature::Fork,
        id: id("n0"),
        release: Release::new(0, 7, 0),
    };
    assert_eq!(state.check_admission(&request), Err(unsupported.clone()));
    refused(&mut state, 100, request, &unsupported);
    // So is every command that drives a fork; `apply` refuses it for its identity first.
    let drive = complete(&fork, 1, &["n0", "n1"]);
    assert_eq!(state.check_admission(&drive), Err(unsupported));
    refused(&mut state, 100, drive, &stale(&fork));
}

/// Once `fork` is enabled, every database created carries the incarnation the control node drew
/// in its command; `apply` refuses none, 0, or one a live record uses, so a name dropped and
/// created again under a fresh draw names a different database. A member without the feature
/// can no longer join (ADR-0038, point 3).
#[test]
fn once_forks_are_enabled_every_creation_carries_a_fresh_incarnation() {
    let mut state = fleet(&["n0", "n1", "n2"]);
    assert_eq!(
        state.database(&db(SOURCE)).map(|d| d.incarnation()),
        Some(Incarnation::LEGACY)
    );
    let created = run(&mut state, 10, fork_request(1, "t")).expect("fork");
    assert!(
        matches!(created, Applied::ForkCreated { incarnation, .. } if incarnation == fork_incarnation(1)),
        "{created:?}"
    );
    assert!(state.is_enabled(Feature::Fork));

    let incarnation = |state: &State, name: &str| {
        state
            .database(&db(name))
            .map(|database| database.incarnation())
            .expect("exists")
    };
    // A creation that carries no incarnation carries 0, which is refused once forks are on.
    for taken in [0, fork_incarnation(1).get()] {
        refused(
            &mut state,
            20,
            create_at("plain", 1, taken),
            &Rejection::IncarnationUnavailable {
                incarnation: Incarnation::new(taken),
            },
        );
    }
    run(&mut state, 20, create_at("plain", 1, 0xA1)).expect("create");
    let first = incarnation(&state, "plain");
    run(&mut state, 30, Action::DropDatabase { name: db("plain") }).expect("drop");
    run(&mut state, 40, create_at("plain", 1, 0x0B)).expect("recreate");
    let second = incarnation(&state, "plain");
    assert_ne!(first, second);
    assert!(second < first, "nothing rests on incarnations growing");

    // A fork dropped and made again under the same name is a different database too; a fork
    // whose drawn incarnation is in use is refused like a creation.
    run(&mut state, 50, Action::DropDatabase { name: db("t") }).expect("drop");
    let reused = with_incarnation(fork_request(2, "t"), second.get());
    refused(
        &mut state,
        60,
        reused,
        &Rejection::IncarnationUnavailable {
            incarnation: second,
        },
    );
    // The dropped fork's operation record still names its incarnation, so it isn't drawn again.
    let reused = with_incarnation(fork_request(2, "t"), fork_incarnation(1).get());
    assert!(matches!(
        run(&mut state, 60, reused),
        Err(Rejection::IncarnationUnavailable { .. })
    ));
    run(&mut state, 60, fork_request(2, "t")).expect("fork again");
    assert_ne!(incarnation(&state, "t"), fork_incarnation(1));

    let old = Member::new(id("old")).with_software(without_fork());
    let refused_join = run(&mut state, 70, Action::Join { member: old });
    assert_eq!(
        refused_join,
        Err(Rejection::MissingFeature {
            id: id("old"),
            release: Release::new(0, 7, 0),
            feature: Feature::Fork,
        })
    );
}

// ---- Fencing by fork identity ----

/// Builds the adversarial setup: the fork `t` (i1, o1) reaches attempt 2 and is dropped, and a
/// new fork `t` (i2, o2) reaches attempt 2. Returns the state with both identities.
fn replaced_fork() -> (State, ForkIdentity, ForkIdentity) {
    let mut state = fleet(&["n0", "n1", "n2"]);
    run(&mut state, 10, fork_request(1, "t")).expect("fork");
    let old = identity(&state, "t");
    let retry = |fork: &ForkIdentity| Action::RetryForkSeed {
        fork: fork.clone(),
        attempt: 1,
        cause: RetryCause::AttemptTimedOut,
        exclude_seeder: None,
    };
    run(&mut state, 20, retry(&old)).expect("attempt 2");
    run(&mut state, 30, Action::DropDatabase { name: db("t") }).expect("drop");
    run(&mut state, 40, fork_request(2, "t")).expect("fork again");
    let new = identity(&state, "t");
    run(&mut state, 50, retry(&new)).expect("attempt 2");
    assert_eq!(attempt(&state, "t"), 2);
    assert_ne!(old, new);
    assert_eq!(old.name, new.name);
    (state, old, new)
}

/// A dropped fork's commands and reports never touch the fork that replaced it, even at the
/// same name and attempt (ADR-0046, point 3, invariant (d)).
#[test]
fn a_dropped_forks_identity_never_touches_its_replacement() {
    let (base, old, new) = replaced_fork();
    let nodes = ["n0", "n1", "n2"];

    // Every replica reports a staged copy and a committed reset for the old identity.
    let mut attacked = base.clone();
    for node in nodes {
        let report = replica("t")
            .with_staged(staged(&old, 2))
            .with_reset(ResetReport::committed(
                old.clone(),
                LeaseEpoch::new(1),
                Position::new([5]),
            ));
        run(&mut attacked, 60, heartbeat_of(node, vec![report], None)).expect("heartbeat");
    }
    for action in [
        complete(&old, 2, &nodes),
        Action::RetryForkSeed {
            fork: old.clone(),
            attempt: 2,
            cause: RetryCause::SeederDown,
            exclude_seeder: None,
        },
        Action::FailFork {
            fork: old.clone(),
            attempt: 2,
            reason: ForkFailure::SourceGone,
        },
        Action::MarkForkReady {
            fork: old.clone(),
            epoch: LeaseEpoch::new(1),
            reset_position: Position::new([5]),
        },
    ] {
        refused(&mut attacked, 60, action, &stale(&old));
    }

    // The same run without any of it.
    let mut clean = base;
    for node in nodes {
        run(&mut clean, 60, heartbeat_of(node, vec![replica("t")], None)).expect("heartbeat");
    }
    // The driver counts none of the old identity's reports.
    assert_eq!(
        fork::actions(&attacked, at(70)),
        fork::actions(&clean, at(70))
    );
    assert!(fork::actions(&attacked, at(70)).is_empty());

    // The new fork then completes the same way in both runs.
    for state in [&mut attacked, &mut clean] {
        for node in nodes {
            run(state, 80, report_staged(node, &new, 2)).expect("heartbeat");
        }
        let proposed = drive(state, 90);
        assert!(
            matches!(proposed.as_slice(), [(Action::CompleteFork { .. }, Ok(_))]),
            "{proposed:?}"
        );
    }
    let record = |state: &State| {
        let database = serde_json::to_string(state.database(&db("t")).expect("t")).expect("json");
        let operations: Vec<String> = state
            .fork_operations()
            .map(|entry| serde_json::to_string(&entry).expect("json"))
            .collect();
        (database, operations)
    };
    assert_eq!(record(&attacked), record(&clean));
}

/// Every command that drives a fork is refused with `StaleForkIdentity` before any other
/// check, in every state, when its identity differs from the current fork's in any part.
#[test]
fn every_fork_command_with_another_identity_is_refused_first() {
    let mut state = seeded();
    let current = identity(&state, "t");
    let variants = [
        ForkIdentity::new(db("t"), Incarnation::new(9), current.operation),
        ForkIdentity::new(db("t"), current.incarnation, op(9)),
        ForkIdentity::new(db("u"), current.incarnation, current.operation),
        ForkIdentity::new(db(SOURCE), Incarnation::LEGACY, current.operation),
    ];
    for fork in &variants {
        for action in [
            complete(fork, 1, &["n0", "n1"]),
            Action::RetryForkSeed {
                fork: fork.clone(),
                attempt: 1,
                cause: RetryCause::TransferFailed,
                exclude_seeder: None,
            },
            Action::RecordForkDiskRefusal {
                fork: fork.clone(),
                attempt: 1,
                node: id("n0"),
            },
            Action::FailFork {
                fork: fork.clone(),
                attempt: 1,
                reason: ForkFailure::SourceGone,
            },
            Action::DropForkHolder {
                fork: fork.clone(),
                attempt: 1,
                holder: id("n0"),
                cause: RetryCause::HoldersLeft,
            },
            Action::MarkForkReady {
                fork: fork.clone(),
                epoch: LeaseEpoch::new(1),
                reset_position: Position::new([1]),
            },
        ] {
            refused(&mut state, 30, action, &stale(fork));
        }
    }
}

// ---- Leases ----

/// A fork takes no lease while seeding or failed, and its first lease only on a holder of the
/// staged copy (ADR-0046, point 3.7, invariant (a)).
#[test]
fn leases_wait_for_seeding_and_the_first_goes_to_a_holder() {
    let mut state = fleet(&["n0", "n1", "n2"]);
    run(&mut state, 10, fork_request(1, "t")).expect("fork");
    let fork = identity(&state, "t");
    let unseeded = Rejection::LeaseOnUnseededFork { database: db("t") };
    for node in ["n0", "n1", "n2"] {
        let outcome = first_lease(&mut state, 20, node);
        assert_eq!(outcome, Err(unseeded.clone()));
    }

    let order = replicas(&state, "t");
    let (outsider, holders) = (order[2].as_str(), [order[0].as_str(), order[1].as_str()]);
    for node in holders {
        run(&mut state, 30, report_staged(node, &fork, 1)).expect("heartbeat");
    }
    run(&mut state, 30, complete(&fork, 1, &holders)).expect("complete");
    // The seeded fork's replica set is frozen at its holders, so the outsider isn't in it.
    assert_eq!(
        first_lease(&mut state, 40, outsider),
        Err(Rejection::NotInReplicaSet {
            id: id(outsider),
            database: db("t"),
        })
    );
    let granted = first_lease(&mut state, 40, holders[1]).expect("a holder takes it");
    assert!(
        matches!(&granted, Applied::LeaseGranted { lease, .. } if lease.epoch() == LeaseEpoch::new(1)),
        "{granted:?}"
    );
    assert!(
        state
            .database(&db("t"))
            .is_some_and(|d| d.is_seeded() && !d.is_ready())
    );

    // A failed fork never takes one.
    let mut failed = fleet(&["n0", "n1", "n2"]);
    run(&mut failed, 10, fork_request(1, "t")).expect("fork");
    let fork = identity(&failed, "t");
    let fail = Action::FailFork {
        fork,
        attempt: 1,
        reason: ForkFailure::EngineUnsupported,
    };
    run(&mut failed, 20, fail).expect("fail");
    assert_eq!(first_lease(&mut failed, 30, "n0"), Err(unseeded));
}

// ---- Completion ----

/// Every reason `apply` refuses a `CompleteFork` (ADR-0046, point 3.2).
#[test]
fn completion_needs_a_write_quorum_of_matching_staged_copies() {
    let mut state = fleet(&["n0", "n1", "n2", "n3"]);
    run(&mut state, 10, fork_request(1, "t")).expect("fork");
    let fork = identity(&state, "t");
    let order = replicas(&state, "t");
    let [r0, r1, r2] = [order[0].as_str(), order[1].as_str(), order[2].as_str()];
    let outsider = ["n0", "n1", "n2", "n3"]
        .into_iter()
        .find(|node| !order.iter().any(|replica| replica == node))
        .expect("a member outside the replica set");
    for node in [r0, r1, r2, outsider] {
        run(&mut state, 20, report_staged(node, &fork, 1)).expect("heartbeat");
    }

    refused(
        &mut state,
        30,
        complete(&fork, 1, &[r0]),
        &Rejection::NotEnoughHolders { have: 1, need: 2 },
    );
    refused(
        &mut state,
        30,
        complete(&fork, 1, &[r0, "ghost"]),
        &Rejection::UnknownMember { id: id("ghost") },
    );
    refused(
        &mut state,
        30,
        complete(&fork, 1, &[r0, outsider]),
        &Rejection::NotInReplicaSet {
            id: id(outsider),
            database: db("t"),
        },
    );
    refused(
        &mut state,
        30,
        complete(&fork, 2, &[r0, r1]),
        &Rejection::ForkNotSeeding {
            database: db("t"),
            attempt: 2,
        },
    );

    // A holder whose report differs in any field doesn't count.
    let other_fork = ForkIdentity::new(db("t"), fork.incarnation, op(9));
    let differing = [
        StagedCopy::new(
            fork.clone(),
            1,
            Position::new([2]),
            copy_hash(),
            Incarnation::LEGACY,
        ),
        StagedCopy::new(
            fork.clone(),
            1,
            copy_at(),
            fleetring_control::fork::ChainHash::new([8]),
            Incarnation::LEGACY,
        ),
        StagedCopy::new(fork.clone(), 2, copy_at(), copy_hash(), Incarnation::LEGACY),
        StagedCopy::new(fork.clone(), 1, copy_at(), copy_hash(), Incarnation::new(5)),
        StagedCopy::new(other_fork, 1, copy_at(), copy_hash(), Incarnation::LEGACY),
    ];
    for copy in differing {
        let mut changed = state.clone();
        let report = heartbeat_of(r1, vec![replica("t").with_staged(copy.clone())], None);
        run(&mut changed, 25, report).expect("heartbeat");
        refused(
            &mut changed,
            30,
            complete(&fork, 1, &[r0, r1]),
            &Rejection::HolderReportMismatch {
                id: id(r1),
                database: db("t"),
            },
        );
        // A heartbeat without the copy withdraws it too.
        let mut withdrawn = state.clone();
        run(
            &mut withdrawn,
            25,
            heartbeat_of(r1, vec![replica("t")], None),
        )
        .expect("hb");
        assert!(matches!(
            run(&mut withdrawn, 30, complete(&fork, 1, &[r0, r1])),
            Err(Rejection::HolderReportMismatch { .. })
        ));
    }

    // The source dropped, or dropped and recreated, since the fork began.
    let mut gone = state.clone();
    run(&mut gone, 25, Action::DropDatabase { name: db(SOURCE) }).expect("drop");
    refused(
        &mut gone,
        30,
        complete(&fork, 1, &[r0, r1]),
        &Rejection::SourceIncarnationMismatch {
            source_database: db(SOURCE),
            expected: Incarnation::LEGACY,
            current: None,
        },
    );
    run(&mut gone, 26, create_at(SOURCE, 3, 0x5A)).expect("recreate");
    let recreated = gone.database(&db(SOURCE)).map(|d| d.incarnation());
    assert!(recreated > Some(Incarnation::LEGACY));
    refused(
        &mut gone,
        30,
        complete(&fork, 1, &[r0, r1]),
        &Rejection::SourceIncarnationMismatch {
            source_database: db(SOURCE),
            expected: Incarnation::LEGACY,
            current: recreated,
        },
    );

    // With every check passing, the fork is seeded, once.
    let done = run(&mut state, 30, complete(&fork, 1, &[r0, r1])).expect("complete");
    assert!(matches!(done, Applied::ForkSeeded { .. }), "{done:?}");
    assert!(matches!(fork_state(&state, "t"), ForkState::Seeded { .. }));
    refused(
        &mut state,
        40,
        complete(&fork, 1, &[r0, r1, r2]),
        &Rejection::ForkNotSeeding {
            database: db("t"),
            attempt: 1,
        },
    );
    let record = state.fork_operation(&op(1), at(40)).expect("the record");
    assert_eq!(record.outcome(), &OperationOutcome::Seeded);
}

/// A fork that must be acknowledged by every replica needs every replica as a holder; its
/// `min_replicas` counts too (ADR-0023).
#[test]
fn completion_counts_the_forks_own_durability() {
    let mut state = fleet(&["n0", "n1", "n2"]);
    let request = with_intent(fork_request(1, "t"), |intent| {
        intent.durability = Some(Durability::new(Acks::All, factor(3)));
    });
    run(&mut state, 10, request).expect("fork");
    let fork = identity(&state, "t");
    for node in ["n0", "n1", "n2"] {
        run(&mut state, 20, report_staged(node, &fork, 1)).expect("heartbeat");
    }
    refused(
        &mut state,
        30,
        complete(&fork, 1, &["n0", "n1"]),
        &Rejection::NotEnoughHolders { have: 2, need: 3 },
    );
    run(&mut state, 30, complete(&fork, 1, &["n0", "n1", "n2"])).expect("complete");
}

// ---- Readiness ----

/// `MarkForkReady` needs a majority of the replica set to report the reset committed for this
/// identity at an issued epoch and position, and is accepted once (ADR-0046, point 3.5).
#[test]
fn readiness_needs_a_committed_reset_and_happens_once() {
    let mut state = fleet(&["n0", "n1", "n2"]);
    run(&mut state, 10, fork_request(1, "t")).expect("fork");
    let fork = identity(&state, "t");
    let ready = |epoch: u64, position: &[u8]| Action::MarkForkReady {
        fork: fork.clone(),
        epoch: LeaseEpoch::new(epoch),
        reset_position: Position::new(position),
    };
    let not_seeded = Rejection::ForkNotSeeded { database: db("t") };
    refused(&mut state, 15, ready(1, &[5]), &not_seeded);

    for node in ["n0", "n1", "n2"] {
        run(&mut state, 20, report_staged(node, &fork, 1)).expect("heartbeat");
    }
    run(&mut state, 20, complete(&fork, 1, &["n0", "n1", "n2"])).expect("complete");
    let not_reported = Rejection::ResetNotReported { database: db("t") };
    refused(
        &mut state,
        25,
        ready(0, &[5]),
        &Rejection::StaleEpoch {
            database: db("t"),
            given: LeaseEpoch::ZERO,
            current: LeaseEpoch::ZERO,
        },
    );
    first_lease(&mut state, 30, "n0").expect("lease");
    refused(&mut state, 35, ready(1, &[5]), &not_reported);

    // A reset still running, or committed elsewhere, doesn't count.
    for node in ["n0", "n1"] {
        run(&mut state, 40, running_reset(node, &fork)).expect("heartbeat");
    }
    refused(&mut state, 45, ready(1, &[5]), &not_reported);
    for node in ["n0", "n1"] {
        run(
            &mut state,
            50,
            report_reset(node, &fork, LeaseEpoch::new(1), &[6]),
        )
        .expect("hb");
    }
    refused(&mut state, 55, ready(1, &[5]), &not_reported);
    refused(
        &mut state,
        55,
        ready(2, &[6]),
        &Rejection::StaleEpoch {
            database: db("t"),
            given: LeaseEpoch::new(2),
            current: LeaseEpoch::new(1),
        },
    );

    let done = run(&mut state, 60, ready(1, &[6])).expect("ready");
    assert!(matches!(done, Applied::ForkReady { .. }), "{done:?}");
    assert!(state.database(&db("t")).is_some_and(|d| d.is_ready()));
    refused(&mut state, 70, ready(1, &[6]), &not_seeded);
    assert!(matches!(
        state.fork_operation(&op(1), at(70)).map(|r| r.outcome()),
        Some(OperationOutcome::Ready { .. })
    ));
    // The provenance stays.
    let ForkState::Ready {
        at: copied,
        reset_epoch,
        ..
    } = fork_state(&state, "t")
    else {
        panic!("ready");
    };
    assert_eq!((copied, reset_epoch), (copy_at(), LeaseEpoch::new(1)));
}

/// A heartbeat from `node` reporting the reset of `fork` still running at epoch 1.
fn running_reset(node: &str, fork: &ForkIdentity) -> Action {
    let reset = ResetReport::running(fork.clone(), LeaseEpoch::new(1), Position::new([5]));
    heartbeat_of(node, vec![replica("t").with_reset(reset)], None)
}

// ---- Fork requests ----

/// A fork's source must exist, be ready, and be the incarnation the request expects, and its
/// name must be free (ADR-0046, point 3.1).
#[test]
fn a_fork_request_checks_its_source_and_its_name() {
    let mut state = fleet(&["n0", "n1", "n2"]);
    run(&mut state, 10, fork_request(1, "t")).expect("fork");
    refused(
        &mut state,
        20,
        fork_of(2, "u", "t", 3),
        &Rejection::SourceNotReady {
            source_database: db("t"),
        },
    );
    refused(
        &mut state,
        20,
        fork_of(2, "u", "missing", 3),
        &Rejection::UnknownDatabase {
            database: db("missing"),
        },
    );
    refused(
        &mut state,
        20,
        fork_of(2, "t", SOURCE, 3),
        &Rejection::DatabaseExists { database: db("t") },
    );
    let expecting = with_intent(fork_of(2, "u", SOURCE, 3), |intent| {
        intent.expect_source_incarnation = Some(Incarnation::new(4));
    });
    refused(
        &mut state,
        20,
        expecting,
        &Rejection::SourceIncarnationMismatch {
            source_database: db(SOURCE),
            expected: Incarnation::new(4),
            current: Some(Incarnation::LEGACY),
        },
    );

    // The fork has the source's engine, its own durability and metadata, and its record.
    let database = state.database(&db("t")).expect("t");
    let record = database.fork().expect("a fork");
    assert_eq!(record.source(), &db(SOURCE));
    assert_eq!(record.source_incarnation(), Incarnation::LEGACY);
    assert_eq!(record.requested_by(), Some(&principal("alice")));
    assert_eq!(record.limits(), limits());
    assert_eq!(
        database.metadata().and_then(|m| m.created_by()),
        Some(&principal("alice"))
    );
    assert!(matches!(
        record.state(),
        ForkState::Seeding { attempt: 1, seeder_excluded, .. } if seeder_excluded.is_empty()
    ));
    assert_eq!(seeder(&state, "t"), replicas(&state, "t")[0]);
}

/// The seeder is the first live member of the replica set, in preference order.
#[test]
fn the_seeder_is_the_first_live_replica() {
    let mut state = fleet(&["n0", "n1", "n2"]);
    // Only the last in preference order for `t` keeps sending heartbeats.
    let mut probe = state.clone();
    run(&mut probe, 10, fork_request(1, "t")).expect("fork");
    let order = replicas(&probe, "t");
    run(&mut state, 3_000, heartbeat_of(&order[2], Vec::new(), None)).expect("hb");
    run(&mut state, 3_500, fork_request(1, "t")).expect("fork");
    assert_eq!(seeder(&state, "t"), order[2]);

    // With no live replica there is no seeder, and no fork.
    let mut down = fleet(&["n0", "n1", "n2"]);
    refused(
        &mut down,
        10_000,
        fork_request(1, "t"),
        &Rejection::NoCandidate { database: db("t") },
    );
}

// ---- Operations ----

/// A resend with the same ID, requester, and request returns the operation and changes
/// nothing; the control node answers it before logging (ADR-0046, point 5.2).
#[test]
fn a_resend_returns_the_operation_and_changes_nothing() {
    let mut state = fleet(&["n0", "n1", "n2"]);
    run(&mut state, 10, fork_request(1, "t")).expect("fork");
    let request = fork_request(1, "t");
    let record = state
        .fork_resend(&request, at(20))
        .expect("a known operation")
        .expect("a resend")
        .clone();
    assert_eq!(record.outcome(), &OperationOutcome::Pending { attempt: 1 });
    assert_eq!(record.target(), &db("t"));

    let before = state.clone();
    let resent = run(&mut state, 20, request).expect("resent");
    assert_eq!(
        resent,
        Applied::ForkResent {
            operation: op(1),
            database: db("t"),
            incarnation: record.target_incarnation(),
            outcome: OperationOutcome::Pending { attempt: 1 },
        }
    );
    let unchanged = |state: &State| {
        let mut value = serde_json::to_value(state).expect("json");
        value["now"] = serde_json::Value::Null;
        value
    };
    assert_eq!(unchanged(&state), unchanged(&before));

    // The intent is compared as given: an explicit default durability is another intent.
    let explicit = with_intent(fork_request(1, "t"), |intent| {
        intent.durability = Some(Durability::quorum(factor(3)));
    });
    assert!(matches!(
        state.fork_resend(&explicit, at(20)),
        Some(Err(Rejection::OperationConflict {
            field: IntentField::Durability,
            ..
        }))
    ));
    // Other actions, and unknown IDs, aren't resends.
    assert!(state.fork_resend(&fork_request(2, "u"), at(20)).is_none());
    assert!(state.fork_resend(&Action::Tick, at(20)).is_none());
}

/// A change to a fork intent.
type Edit = fn(&mut ForkIntent);

/// The same ID from the same requester with any field of the intent different is a conflict
/// naming the field, and from another principal is refused without saying anything about the
/// operation (ADR-0046, point 5.2).
#[test]
fn a_reused_operation_id_with_another_intent_or_requester_is_refused() {
    let mut state = fleet(&["n0", "n1", "n2"]);
    run(&mut state, 10, fork_request(1, "t")).expect("fork");
    let edits: [(IntentField, Edit); 8] = [
        (IntentField::Name, |intent| intent.name = db("other")),
        (IntentField::Source, |intent| intent.source = db("t")),
        (IntentField::ExpectSourceIncarnation, |intent| {
            intent.expect_source_incarnation = Some(Incarnation::LEGACY)
        }),
        (IntentField::ReplicationFactor, |intent| {
            intent.replication_factor = factor(2)
        }),
        (IntentField::Durability, |intent| {
            intent.durability = Some(Durability::new(Acks::All, factor(3)))
        }),
        (IntentField::Description, |intent| {
            intent.description = Some("Another.".parse().expect("valid"))
        }),
        (IntentField::Labels, |intent| {
            intent.labels = Labels::try_from_pairs([parse_label("purpose=task").expect("valid")])
                .expect("one label");
        }),
        (IntentField::Expiry, |intent| {
            intent.expiry = ForkExpiry::After(Millis::new(60_000))
        }),
    ];
    for (field, edit) in edits {
        let action = with_intent(fork_request(1, "t"), edit);
        let conflict = Rejection::OperationConflict {
            operation: op(1),
            field,
        };
        assert_eq!(
            state.fork_resend(&action, at(20)).map(|r| r.err()),
            Some(Some(conflict.clone()))
        );
        refused(&mut state, 20, action, &conflict);
        assert!(
            conflict.to_string().contains(&field.to_string()),
            "{conflict}"
        );
    }

    for requester in [Some(principal("mallory")), None] {
        let theirs = with_intent(fork_request(1, "t"), |intent| {
            intent.requested_by = requester
        });
        let not_yours = Rejection::OperationNotYours { operation: op(1) };
        refused(&mut state, 20, theirs, &not_yours);
        let message = not_yours.to_string();
        for secret in ["'t'", SOURCE, "alice", "pending", "seeding"] {
            assert!(!message.contains(secret), "{message} reveals {secret}");
        }
    }
}

/// An operation's record, and so its deduplication, lasts exactly its retention after its final
/// outcome; a pending or seeded one is never forgotten (ADR-0046, point 5.3).
#[test]
fn an_operation_is_known_for_exactly_its_retention_after_its_outcome() {
    let mut state = fleet(&["n0", "n1", "n2"]);
    run(&mut state, 10, fork_request(1, "t")).expect("fork");
    // Dropping a fork that isn't ready cancels its operation.
    run(&mut state, 1_000, Action::DropDatabase { name: db("t") }).expect("drop");
    let record = state.fork_operation(&op(1), at(1_000)).expect("kept");
    assert_eq!(
        record.outcome(),
        &OperationOutcome::Cancelled {
            cause: CancelCause::Dropped
        }
    );
    let forgotten = 1_000 + limits().operation_retention.get();
    assert_eq!(record.forgotten_at(), Some(at(forgotten)));

    // Keep the members live without changing anything else.
    let live = |state: &mut State, millis: u64| {
        for node in ["n0", "n1", "n2"] {
            run(state, millis, heartbeat_of(node, Vec::new(), None)).expect("hb");
        }
    };
    live(&mut state, forgotten - 1);
    run(&mut state, forgotten - 1, Action::Tick).expect("tick");
    assert!(state.fork_operation(&op(1), at(forgotten - 1)).is_some());
    // A resend just inside the window returns the cancelled operation, not a new fork.
    let resent = run(&mut state, forgotten - 1, fork_request(1, "t")).expect("resent");
    assert!(
        matches!(
            &resent,
            Applied::ForkResent {
                outcome: OperationOutcome::Cancelled { .. },
                ..
            }
        ),
        "{resent:?}"
    );
    assert!(state.database(&db("t")).is_none());
    assert_eq!(
        fork::actions(&state, at(forgotten - 1)),
        Vec::<Action>::new()
    );

    // At the boundary the ID is unknown, before and after a tick forgets it.
    assert!(state.fork_operation(&op(1), at(forgotten)).is_none());
    assert!(
        state
            .fork_resend(&fork_request(1, "t"), at(forgotten))
            .is_none()
    );
    assert_eq!(fork::actions(&state, at(forgotten)), vec![Action::Tick]);
    live(&mut state, forgotten);
    run(&mut state, forgotten, Action::Tick).expect("tick");
    assert_eq!(state.fork_operations().len(), 0);
    // So a request with it is a new operation.
    let created = run(&mut state, forgotten, fork_request(1, "t")).expect("a new fork");
    assert!(
        matches!(created, Applied::ForkCreated { .. }),
        "{created:?}"
    );

    // A seeded fork's operation is never forgotten, however late.
    let mut seeded = seeded();
    run(&mut seeded, u64::MAX / 2, Action::Tick).expect("tick");
    assert_eq!(
        seeded
            .fork_operation(&op(1), at(u64::MAX / 2))
            .map(|r| r.outcome().clone()),
        Some(OperationOutcome::Seeded)
    );
}

/// Races between a drop and the driver's commands, and duplicated commands, are refused and
/// change nothing.
#[test]
fn races_and_duplicates_are_refused() {
    // Complete after the drop.
    let mut state = fleet(&["n0", "n1", "n2"]);
    run(&mut state, 10, fork_request(1, "t")).expect("fork");
    let fork = identity(&state, "t");
    for node in ["n0", "n1", "n2"] {
        run(&mut state, 20, report_staged(node, &fork, 1)).expect("heartbeat");
    }
    let proposed = fork::actions(&state, at(20));
    run(&mut state, 25, Action::DropDatabase { name: db("t") }).expect("drop");
    for action in proposed {
        refused(&mut state, 30, action, &stale(&fork));
    }

    // A duplicated retry, and a duplicated completion.
    let mut state = fleet(&["n0", "n1", "n2"]);
    run(&mut state, 10, fork_request(1, "t")).expect("fork");
    let fork = identity(&state, "t");
    let retry = Action::RetryForkSeed {
        fork: fork.clone(),
        attempt: 1,
        cause: RetryCause::AttemptTimedOut,
        exclude_seeder: None,
    };
    run(&mut state, 20, retry.clone()).expect("retry");
    let not_seeding = Rejection::ForkNotSeeding {
        database: db("t"),
        attempt: 1,
    };
    refused(&mut state, 21, retry, &not_seeding);
    for node in ["n0", "n1", "n2"] {
        run(&mut state, 30, report_staged(node, &fork, 2)).expect("heartbeat");
    }
    let done = complete(&fork, 2, &["n0", "n1"]);
    run(&mut state, 40, done.clone()).expect("complete");
    refused(
        &mut state,
        41,
        done,
        &Rejection::ForkNotSeeding {
            database: db("t"),
            attempt: 2,
        },
    );
    // A failure after completion, and a drop after completion, which cancels.
    refused(
        &mut state,
        42,
        Action::FailFork {
            fork,
            attempt: 2,
            reason: ForkFailure::SourceGone,
        },
        &Rejection::ForkNotSeeding {
            database: db("t"),
            attempt: 2,
        },
    );
}

// ---- Retries and budgets ----

/// An attempt past the fork's budget, or past its deadline, fails the fork instead of starting
/// another.
#[test]
fn retries_stop_at_the_budget_and_the_deadline() {
    let mut state = fleet(&["n0", "n1", "n2"]);
    run(&mut state, 10, fork_request(1, "t")).expect("fork");
    let fork = identity(&state, "t");
    let retry = |attempt| Action::RetryForkSeed {
        fork: fork.clone(),
        attempt,
        cause: RetryCause::TransferFailed,
        exclude_seeder: None,
    };
    run(&mut state, 20, retry(1)).expect("attempt 2");
    run(&mut state, 30, retry(2)).expect("attempt 3");
    let failed = run(&mut state, 40, retry(3)).expect("out of attempts");
    assert_eq!(
        failed,
        Applied::ForkFailed {
            database: db("t"),
            reason: ForkFailure::RetriesExhausted {
                last: RetryCause::TransferFailed
            },
        }
    );
    assert!(matches!(
        fork_state(&state, "t"),
        ForkState::Failed { attempts: 3, .. }
    ));

    let mut late = fleet(&["n0", "n1", "n2"]);
    run(&mut late, 10, fork_request(1, "t")).expect("fork");
    let fork = identity(&late, "t");
    let deadline = 10 + limits().deadline.get();
    let retry = Action::RetryForkSeed {
        fork,
        attempt: 1,
        cause: RetryCause::AttemptTimedOut,
        exclude_seeder: None,
    };
    let failed = run(&mut late, deadline, retry).expect("past the deadline");
    assert!(
        matches!(
            failed,
            Applied::ForkFailed {
                reason: ForkFailure::DeadlineExceeded {
                    last: RetryCause::AttemptTimedOut
                },
                ..
            }
        ),
        "{failed:?}"
    );
}

/// A tick fails every seeding fork past its deadline, exactly at it.
#[test]
fn a_tick_fails_a_fork_at_its_deadline() {
    let mut state = fleet(&["n0", "n1", "n2"]);
    run(&mut state, 10, fork_request(1, "t")).expect("fork");
    let deadline = 10 + limits().deadline.get();
    assert!(!fork::actions(&state, at(deadline - 1)).contains(&Action::Tick));
    run(&mut state, deadline - 1, Action::Tick).expect("tick");
    assert_eq!(attempt(&state, "t"), 1);
    assert_eq!(fork::actions(&state, at(deadline)), vec![Action::Tick]);
    run(&mut state, deadline, Action::Tick).expect("tick");
    assert!(matches!(
        fork_state(&state, "t"),
        ForkState::Failed {
            reason: ForkFailure::DeadlineExceeded { .. },
            ..
        }
    ));
    assert_eq!(
        state
            .fork_operation(&op(1), at(deadline))
            .map(|r| r.outcome().clone()),
        Some(OperationOutcome::Failed {
            reason: ForkFailure::DeadlineExceeded {
                last: RetryCause::AttemptTimedOut
            }
        })
    );
}

// ---- Disk ----

/// With R = 3 and C = 2, a seeder that refuses for want of disk is recorded as disk-refused,
/// not as a passed-over seeder, the next attempt starts without it, and the other two complete
/// the fork; a peer that refuses is recorded and left out of the holders (ADR-0046, points 3.4
/// and 7.2).
#[test]
fn one_replica_out_of_disk_is_excluded_and_the_others_complete() {
    let mut state = fleet(&["n0", "n1", "n2"]);
    run(&mut state, 10, fork_request(1, "t")).expect("fork");
    let fork = identity(&state, "t");
    let first = seeder(&state, "t");
    run(
        &mut state,
        20,
        report_failure(&first, &fork, 1, SeedFailureReason::InsufficientDisk),
    )
    .expect("heartbeat");
    let proposed = drive(&mut state, 20);
    assert_eq!(
        proposed,
        vec![(
            Action::RecordForkDiskRefusal {
                fork: fork.clone(),
                attempt: 1,
                node: id(&first),
            },
            Ok(Applied::ForkRetried {
                database: db("t"),
                attempt: 2,
                seeder: id(&seeder(&state, "t")),
                cause: RetryCause::SeederDiskRefused,
            })
        )]
    );
    let second = seeder(&state, "t");
    assert_ne!(second, first);
    let ForkState::Seeding {
        seeder_excluded,
        disk_refused,
        ..
    } = fork_state(&state, "t")
    else {
        panic!("seeding");
    };
    assert!(
        seeder_excluded.is_empty(),
        "a disk refusal isn't a seeder exclusion"
    );
    assert_eq!(disk_refused, BTreeSet::from([id(&first)]));

    let others: Vec<String> = replicas(&state, "t")
        .into_iter()
        .filter(|node| *node != first)
        .collect();
    for node in &others {
        run(&mut state, 30, report_staged(node, &fork, 2)).expect("heartbeat");
    }
    // A disk-refused replica is never a holder, even if it reports a copy.
    let mut refused_holder = state.clone();
    run(&mut refused_holder, 30, report_staged(&first, &fork, 2)).expect("heartbeat");
    refused(
        &mut refused_holder,
        30,
        complete(&fork, 2, &[first.as_str(), others[0].as_str()]),
        &Rejection::DiskRefusedHolder {
            id: id(&first),
            database: db("t"),
        },
    );
    let proposed = drive(&mut state, 30);
    let holders: BTreeSet<_> = others.iter().map(|node| id(node)).collect();
    assert!(
        matches!(proposed.as_slice(), [(Action::CompleteFork { holders: h, .. }, Ok(_))] if *h == holders),
        "{proposed:?}"
    );

    // A peer out of disk at attempt 1: it is recorded, and the seeder and the other peer
    // complete it.
    let mut state = fleet(&["n0", "n1", "n2"]);
    run(&mut state, 10, fork_request(1, "t")).expect("fork");
    let fork = identity(&state, "t");
    let order = replicas(&state, "t");
    run(
        &mut state,
        20,
        report_failure(&order[1], &fork, 1, SeedFailureReason::InsufficientDisk),
    )
    .expect("heartbeat");
    let proposed = drive(&mut state, 20);
    assert_eq!(
        proposed,
        vec![(
            Action::RecordForkDiskRefusal {
                fork: fork.clone(),
                attempt: 1,
                node: id(&order[1]),
            },
            Ok(Applied::ForkDiskRefusalRecorded {
                database: db("t"),
                node: id(&order[1]),
            })
        )]
    );
    assert_eq!(attempt(&state, "t"), 1);
    assert!(fork::actions(&state, at(20)).is_empty());
    for node in [&order[0], &order[2]] {
        run(&mut state, 30, report_staged(node, &fork, 1)).expect("heartbeat");
    }
    let proposed = drive(&mut state, 30);
    assert!(
        matches!(proposed.as_slice(), [(Action::CompleteFork { holders, .. }, Ok(_))]
            if !holders.contains(&id(&order[1]))),
        "{proposed:?}"
    );
}

/// With two of three replicas out of disk, fewer than C remain, and the fork fails with
/// `InsufficientDisk`.
#[test]
fn too_few_replicas_with_room_fail_the_fork() {
    let mut state = fleet(&["n0", "n1", "n2"]);
    run(&mut state, 10, fork_request(1, "t")).expect("fork");
    let fork = identity(&state, "t");
    let order = replicas(&state, "t");
    for node in [&order[1], &order[2]] {
        run(
            &mut state,
            20,
            report_failure(node, &fork, 1, SeedFailureReason::InsufficientDisk),
        )
        .expect("heartbeat");
    }
    let first = drive(&mut state, 20);
    assert!(
        matches!(
            first.as_slice(),
            [(
                Action::RecordForkDiskRefusal { .. },
                Ok(Applied::ForkDiskRefusalRecorded { .. })
            )]
        ),
        "{first:?}"
    );
    let second = drive(&mut state, 21);
    assert_eq!(
        second,
        vec![(
            Action::RecordForkDiskRefusal {
                fork,
                attempt: 1,
                node: id(&order[2]),
            },
            Ok(Applied::ForkFailed {
                database: db("t"),
                reason: ForkFailure::InsufficientDisk,
            })
        )]
    );
}

/// Control-plane admission counts each replica's free disk less the copies seeding forks have
/// reserved there: two forks of 60% of a node's free space on the same replicas can't both
/// pass (ADR-0046, point 7.3).
#[test]
fn admission_subtracts_the_seeds_already_pending() {
    const GIB: u64 = 1 << 30;
    let mut state = fleet(&["n0", "n1", "n2"]);
    // The first fork enables forks; until then heartbeats record no sizes.
    run(&mut state, 10, fork_of(1, "warm", SOURCE, 1)).expect("fork");
    for node in ["n0", "n1", "n2"] {
        let source = replica(SOURCE).with_size_bytes(60 * GIB);
        run(
            &mut state,
            20,
            heartbeat_of(node, vec![source], Some(100 * GIB)),
        )
        .expect("hb");
    }
    assert_eq!(state.disk_free_bytes(&id("n0")), Some(100 * GIB));
    let created = run(&mut state, 30, fork_request(2, "a")).expect("the first fits");
    assert!(matches!(created, Applied::ForkCreated { .. }));
    assert_eq!(
        state
            .database(&db("a"))
            .and_then(|d| d.fork())
            .map(|f| f.source_bytes()),
        Some(60 * GIB)
    );
    refused(
        &mut state,
        40,
        fork_request(3, "b"),
        &Rejection::InsufficientDisk {
            database: db("b"),
            admitting: 0,
            need: 2,
        },
    );

    // Once a replica has staged its copy, its bytes are on disk and its free space says so.
    let fork = identity(&state, "a");
    for node in ["n0", "n1"] {
        let report = replica("a").with_staged(staged(&fork, 1));
        run(
            &mut state,
            50,
            heartbeat_of(node, vec![report], Some(100 * GIB)),
        )
        .expect("hb");
    }
    let created = run(&mut state, 60, fork_request(3, "b")).expect("two replicas have room");
    assert!(matches!(created, Applied::ForkCreated { .. }));
}

// ---- The driver ----

/// The driver retries when the seeder is down for the seeder timeout, excluding it, and when
/// the attempt runs for the attempt timeout.
#[test]
fn the_driver_retries_after_the_seeder_or_the_attempt_times_out() {
    let mut state = fleet(&["n0", "n1", "n2"]);
    run(&mut state, 10, fork_request(1, "t")).expect("fork");
    let fork = identity(&state, "t");
    let first = seeder(&state, "t");
    let keep_live = |state: &mut State, millis: u64, except: &str| {
        for node in ["n0", "n1", "n2"]
            .into_iter()
            .filter(|node| *node != except)
        {
            run(state, millis, heartbeat_of(node, Vec::new(), None)).expect("hb");
        }
    };
    // The seeder's last heartbeat was at 0: down after 3 s, retried 5 s later.
    keep_live(&mut state, 7_000, &first);
    assert!(fork::actions(&state, at(7_999)).is_empty());
    let proposed = fork::actions(&state, at(8_000));
    assert_eq!(
        proposed,
        vec![Action::RetryForkSeed {
            fork: fork.clone(),
            attempt: 1,
            cause: RetryCause::SeederDown,
            exclude_seeder: Some(id(&first)),
        }]
    );
    drive(&mut state, 8_000);
    assert_eq!(attempt(&state, "t"), 2);
    let second = seeder(&state, "t");
    assert_ne!(second, first);

    // Every replica stays live; the attempt runs out after 20 s.
    for millis in (10_000..28_000).step_by(2_000) {
        keep_live(&mut state, millis, "");
    }
    assert_eq!(fork::actions(&state, at(27_999)), Vec::<Action>::new());
    let proposed = fork::actions(&state, at(28_000));
    assert_eq!(
        proposed,
        vec![Action::RetryForkSeed {
            fork,
            attempt: 2,
            cause: RetryCause::AttemptTimedOut,
            exclude_seeder: None,
        }]
    );
}

/// The driver retries on the seeder's retryable failure, and fails the fork on a permanent one
/// or when the source is dropped or recreated; it ignores failures of other attempts.
#[test]
fn the_driver_reacts_to_seed_failures_and_to_the_source() {
    let mut state = fleet(&["n0", "n1", "n2"]);
    run(&mut state, 10, fork_request(1, "t")).expect("fork");
    let fork = identity(&state, "t");
    let first = seeder(&state, "t");
    let mut stale_attempt = state.clone();
    run(
        &mut stale_attempt,
        20,
        report_failure(&first, &fork, 7, SeedFailureReason::TransferFailed),
    )
    .expect("hb");
    assert!(fork::actions(&stale_attempt, at(20)).is_empty());

    let mut retryable = state.clone();
    run(
        &mut retryable,
        20,
        report_failure(&first, &fork, 1, SeedFailureReason::TransferFailed),
    )
    .expect("hb");
    assert_eq!(
        fork::actions(&retryable, at(20)),
        vec![Action::RetryForkSeed {
            fork: fork.clone(),
            attempt: 1,
            cause: RetryCause::TransferFailed,
            exclude_seeder: None,
        }]
    );

    let mut permanent = state.clone();
    run(
        &mut permanent,
        20,
        report_failure(&first, &fork, 1, SeedFailureReason::SourceTooLarge),
    )
    .expect("hb");
    let fail = |reason| Action::FailFork {
        fork: fork.clone(),
        attempt: 1,
        reason,
    };
    assert_eq!(
        fork::actions(&permanent, at(20)),
        vec![fail(ForkFailure::SourceTooLarge)]
    );

    let mut gone = state.clone();
    run(&mut gone, 20, Action::DropDatabase { name: db(SOURCE) }).expect("drop");
    assert_eq!(
        fork::actions(&gone, at(20)),
        vec![fail(ForkFailure::SourceGone)]
    );
    run(&mut gone, 20, create_at(SOURCE, 3, 0x5A)).expect("recreate");
    assert_eq!(
        fork::actions(&gone, at(20)),
        vec![fail(ForkFailure::SourceRecreated)]
    );
    let proposed = drive(&mut gone, 20);
    assert!(proposed.iter().all(|(_, outcome)| accepted(outcome)));
    assert!(matches!(
        fork_state(&gone, "t"),
        ForkState::Failed {
            reason: ForkFailure::SourceRecreated,
            ..
        }
    ));
}

/// The driver takes a fork from request to ready on reports alone, and its proposals are a
/// function of the state: the same state gives the same proposals.
#[test]
fn the_driver_takes_a_fork_from_request_to_ready() {
    let mut state = fleet(&["n0", "n1", "n2"]);
    run(&mut state, 10, fork_request(1, "t")).expect("fork");
    let fork = identity(&state, "t");
    for node in ["n0", "n1", "n2"] {
        run(&mut state, 20, report_staged(node, &fork, 1)).expect("heartbeat");
    }
    assert_eq!(
        fork::actions(&state, at(20)),
        fork::actions(&state.clone(), at(20))
    );
    let proposed = drive(&mut state, 20);
    assert!(
        matches!(proposed.as_slice(), [(Action::CompleteFork { holders, .. }, Ok(_))] if holders.len() == 3),
        "{proposed:?}"
    );
    first_lease(&mut state, 30, "n1").expect("lease");
    // Nothing to do until the primary reports the reset.
    assert!(fork::actions(&state, at(40)).is_empty());
    run(
        &mut state,
        40,
        report_reset("n1", &fork, LeaseEpoch::new(1), &[3]),
    )
    .expect("hb");
    // One of three isn't a majority.
    assert!(fork::actions(&state, at(45)).is_empty());
    run(
        &mut state,
        45,
        report_reset("n2", &fork, LeaseEpoch::new(1), &[3]),
    )
    .expect("hb");
    let proposed = drive(&mut state, 50);
    assert_eq!(
        proposed,
        vec![(
            Action::MarkForkReady {
                fork,
                epoch: LeaseEpoch::new(1),
                reset_position: Position::new([3]),
            },
            Ok(Applied::ForkReady {
                database: db("t"),
                epoch: LeaseEpoch::new(1),
                reset_position: Position::new([3]),
            })
        )]
    );
    assert!(fork::actions(&state, at(60)).is_empty());
}
