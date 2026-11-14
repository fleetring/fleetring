//! FR-102: a membership change keeps a leased database's old replica set as the one promotions
//! fence until its primary reports that placement's write quorum holds its log (ADR-0031,
//! amendment 2).

use fleetring::{Epoch, NodeId};
use fleetring_control::{
    Action, Applied, Feature, HostedReplica, Incarnation, LeaseEpoch, Member, Position, Rejection,
    Release, ReplicaRole, ReplicasCaughtUp, Software, State, UnknownReports,
};

use crate::common::{config, create, current_software, db, id, run};

/// The database the tests move: its name, its replica set over `a`, `b`, `c`, and its replica
/// set once `d` joins.
struct Moved {
    name: &'static str,
    before: Vec<NodeId>,
    after: Vec<NodeId>,
}

/// A fleet of `a`, `b`, `c` on `software`, with a three-replica database that `d`'s join moves
/// and whose preferred primary stays, leased to that primary.
fn fleet(software: &Software) -> (State, Moved) {
    for n in 0..200 {
        let mut state = State::new(config());
        for name in ["a", "b", "c"] {
            let member = Member::new(id(name)).with_software(software.clone());
            run(&mut state, 0, Action::Join { member }).expect("join");
        }
        let name: &'static str = Box::leak(format!("t{n}").into_boxed_str());
        run(&mut state, 0, create(name, 3)).expect("create");
        let before = state.replica_set(&db(name)).expect("placed");
        let mut probe = state.clone();
        let joining = Member::new(id("d")).with_software(software.clone());
        run(&mut probe, 0, Action::Join { member: joining }).expect("join");
        let after = probe.placed_replica_set(&db(name)).expect("placed");
        if after.contains(&id("d")) && after.first() == before.first() {
            let primary = before[0].clone();
            run(
                &mut state,
                0,
                Action::AcquireLease {
                    database: db(name),
                    node: primary,
                },
            )
            .expect("lease");
            return (
                state,
                Moved {
                    name,
                    before,
                    after,
                },
            );
        }
    }
    panic!("no name that d's join moves while its primary stays");
}

/// Applies the membership change `action` as the control node's writer logs it: preceded by
/// `EnableHandover` when the change would keep the first replica set (`State::handover_due`).
fn change(state: &mut State, millis: u64, action: Action) -> fleetring_control::Outcome {
    if state.handover_due(&action) {
        assert_eq!(
            run(state, millis, Action::EnableHandover),
            Ok(Applied::HandoverEnabled)
        );
    }
    run(state, millis, action)
}

fn join_d(state: &mut State, software: &Software) {
    let member = Member::new(id("d")).with_software(software.clone());
    change(state, 10, Action::Join { member }).expect("join");
}

fn report(state: &mut State, node: &NodeId, name: &str, caught_up: Option<ReplicasCaughtUp>) {
    let mut replica = HostedReplica::new(db(name), ReplicaRole::Primary, Position::new([9]))
        .with_log_epoch(LeaseEpoch::new(1));
    if let Some(caught_up) = caught_up {
        replica = replica.with_replicas_caught_up(caught_up);
    }
    let heartbeat = Action::Heartbeat {
        id: node.clone(),
        replicas: vec![replica],
        unknown_reports: UnknownReports::Skip,
        software: None,
        disk_free_bytes: None,
    };
    run(state, 20, heartbeat).expect("heartbeat");
}

fn release(name: &str, epoch: u64) -> Action {
    Action::ReleaseKeptReplicas {
        database: db(name),
        incarnation: Incarnation::LEGACY,
        epoch: LeaseEpoch::new(epoch),
    }
}

fn same_members(a: &[NodeId], b: &[NodeId]) -> bool {
    let mut a = a.to_vec();
    let mut b = b.to_vec();
    a.sort();
    b.sort();
    a == b
}

#[test]
fn a_join_keeps_the_old_replica_set_until_the_primary_reports_placement_caught_up() {
    let software = current_software();
    let (mut state, moved) = fleet(&software);
    let name = moved.name;
    let primary = moved.before[0].clone();
    assert!(!state.is_enabled(Feature::Handover));
    join_d(&mut state, &software);
    // The first membership change that keeps a replica set comes after the command that enables
    // the feature for good.
    assert!(state.is_enabled(Feature::Handover));

    let database = state.database(&db(name)).expect("exists");
    assert_eq!(database.kept_replicas(), Some(moved.before.as_slice()));
    assert_eq!(state.replica_set(&db(name)), Ok(moved.before.clone()));
    assert_eq!(state.placed_replica_set(&db(name)), Ok(moved.after.clone()));
    assert!(state.releases_due().is_empty());

    // Nothing reported yet.
    assert_eq!(
        run(&mut state, 20, release(name, 1)),
        Err(Rejection::KeptReplicasNotReleasable { database: db(name) })
    );
    // A report about an older ring, or from a follower, doesn't release it.
    let stale = ReplicasCaughtUp::new(
        Incarnation::LEGACY,
        LeaseEpoch::new(1),
        Epoch::new(state.membership_epoch().get() - 1),
    );
    report(&mut state, &primary, name, Some(stale));
    assert!(state.releases_due().is_empty());
    let current = ReplicasCaughtUp::new(
        Incarnation::LEGACY,
        LeaseEpoch::new(1),
        state.membership_epoch(),
    );
    let follower = moved.before[1].clone();
    report(&mut state, &follower, name, Some(current));
    assert!(state.releases_due().is_empty());
    assert_eq!(
        run(&mut state, 20, release(name, 1)),
        Err(Rejection::KeptReplicasNotReleasable { database: db(name) })
    );
    // A report at another lease epoch is stale.
    assert!(matches!(
        run(&mut state, 20, release(name, 2)),
        Err(Rejection::StaleEpoch { .. })
    ));

    // The primary's report about the current ring releases it.
    report(&mut state, &primary, name, Some(current));
    assert_eq!(state.releases_due(), vec![release(name, 1)]);
    assert_eq!(
        run(&mut state, 30, release(name, 1)),
        Ok(Applied::KeptReplicasReleased {
            database: db(name),
            epoch: LeaseEpoch::new(1),
        })
    );
    assert_eq!(state.replica_set(&db(name)), Ok(moved.after.clone()));
    assert_eq!(
        state.database(&db(name)).expect("exists").kept_replicas(),
        None
    );
    assert!(state.releases_due().is_empty());
    assert_eq!(
        run(&mut state, 30, release(name, 1)),
        Err(Rejection::KeptReplicasNotReleasable { database: db(name) })
    );
}

#[test]
fn a_promotion_while_the_set_is_kept_fences_the_kept_set() {
    let software = current_software();
    let (mut state, moved) = fleet(&software);
    let name = moved.name;
    join_d(&mut state, &software);
    // Every member reports, but the primary goes quiet; its lease expires.
    for node in moved.before.iter().skip(1).chain([&id("d")]) {
        let replica = HostedReplica::new(db(name), ReplicaRole::Follower, Position::new([1]));
        let heartbeat = Action::Heartbeat {
            id: node.clone(),
            replicas: vec![replica],
            unknown_reports: UnknownReports::Skip,
            software: None,
            disk_free_bytes: None,
        };
        run(&mut state, 2_000, heartbeat).expect("heartbeat");
    }
    let started = run(
        &mut state,
        2_000,
        Action::StartPromotion { database: db(name) },
    )
    .expect("a promotion starts");
    let Applied::PromotionStarted { candidate, .. } = started else {
        panic!("expected a promotion, got {started:?}");
    };
    assert!(moved.before.contains(&candidate), "{candidate} isn't kept");
    // The joined node isn't in the set the promotion fences; a node only the kept set names is.
    let fence = |node: &NodeId| Action::RecordFenced {
        database: db(name),
        node: node.clone(),
        epoch: LeaseEpoch::new(2),
        position: Position::new([1]),
    };
    assert_eq!(
        run(&mut state, 2_000, fence(&id("d"))),
        Err(Rejection::NotInReplicaSet {
            id: id("d"),
            database: db(name),
        })
    );
    let displaced = moved
        .before
        .iter()
        .find(|node| !moved.after.contains(node))
        .expect("a node d displaced");
    assert!(run(&mut state, 2_000, fence(displaced)).is_ok());
    // No release while the promotion runs, even with a report at the old epoch.
    assert!(state.releases_due().is_empty());
}

#[test]
fn placement_coming_back_stops_keeping_the_set() {
    let software = current_software();
    let (mut state, moved) = fleet(&software);
    let name = moved.name;
    join_d(&mut state, &software);
    assert!(
        state
            .database(&db(name))
            .expect("exists")
            .kept_replicas()
            .is_some()
    );
    change(&mut state, 40, Action::Leave { id: id("d") }).expect("leave");
    assert_eq!(
        state.database(&db(name)).expect("exists").kept_replicas(),
        None
    );
    assert!(same_members(
        &state.replica_set(&db(name)).expect("placed"),
        &moved.before
    ));
}

#[test]
fn a_second_change_before_the_release_keeps_the_first_set() {
    let software = current_software();
    let (mut state, moved) = fleet(&software);
    let name = moved.name;
    join_d(&mut state, &software);
    let member = Member::new(id("e")).with_software(software.clone());
    change(&mut state, 50, Action::Join { member }).expect("join");
    assert_eq!(state.replica_set(&db(name)), Ok(moved.before.clone()));
}

#[test]
fn a_database_without_a_lease_follows_placement_at_once() {
    let software = current_software();
    let (mut state, moved) = fleet(&software);
    run(&mut state, 0, create("unleased", 3)).expect("create");
    join_d(&mut state, &software);
    assert_eq!(
        state
            .database(&db("unleased"))
            .expect("exists")
            .kept_replicas(),
        None
    );
    assert_eq!(
        state.replica_set(&db("unleased")),
        state.placed_replica_set(&db("unleased"))
    );
    assert!(
        state
            .database(&db(moved.name))
            .expect("exists")
            .kept_replicas()
            .is_some()
    );
}

#[test]
fn a_member_without_the_feature_leaves_membership_changes_as_they_were() {
    // A 0.7 member: every feature but the hand-over.
    let older = Feature::ALL
        .into_iter()
        .filter(|feature| *feature != Feature::Handover)
        .fold(
            Software::new(Release::new(0, 7, 0), 1),
            Software::with_feature,
        );
    let (mut state, moved) = fleet(&older);
    join_d(&mut state, &current_software());
    assert!(!state.is_enabled(Feature::Handover));
    assert_eq!(
        state
            .database(&db(moved.name))
            .expect("exists")
            .kept_replicas(),
        None
    );
    assert_eq!(state.replica_set(&db(moved.name)), Ok(moved.after.clone()));
    // Nor can a release be logged.
    assert!(matches!(
        run(&mut state, 20, release(moved.name, 1)),
        Err(Rejection::FeatureUnsupported {
            feature: Feature::Handover,
            ..
        })
    ));
    // A report isn't recorded either, so control state stays as it was.
    let primary = moved.before[0].clone();
    let caught_up = ReplicasCaughtUp::new(
        Incarnation::LEGACY,
        LeaseEpoch::new(1),
        state.membership_epoch(),
    );
    report(&mut state, &primary, moved.name, Some(caught_up));
    assert_eq!(
        state
            .report(&primary, &db(moved.name))
            .and_then(fleetring_control::Report::replicas_caught_up),
        None
    );
}

#[test]
fn only_a_change_that_keeps_a_replica_set_enables_the_feature() {
    let software = current_software();
    let mut state = State::new(config());
    for name in ["a", "b", "c"] {
        let member = Member::new(id(name)).with_software(software.clone());
        run(&mut state, 0, Action::Join { member }).expect("join");
    }
    run(&mut state, 0, create("unleased", 3)).expect("create");
    join_d(&mut state, &software);
    change(&mut state, 20, Action::Leave { id: id("d") }).expect("leave");
    // No leased database moved: a fleet can still roll back.
    assert!(!state.is_enabled(Feature::Handover));
}

#[test]
fn once_enabled_a_member_without_the_feature_cant_join() {
    let software = current_software();
    let (mut state, _) = fleet(&software);
    join_d(&mut state, &software);
    let older = Software::new(Release::new(0, 7, 0), 1).with_feature(Feature::Fork);
    let member = Member::new(id("old")).with_software(older);
    assert!(matches!(
        run(&mut state, 60, Action::Join { member }),
        Err(Rejection::MissingFeature {
            feature: Feature::Handover,
            ..
        })
    ));
}

#[test]
fn a_join_by_a_member_without_the_feature_doesnt_enable_it() {
    let software = current_software();
    let (mut state, moved) = fleet(&software);
    let older = Software::new(Release::new(0, 7, 0), 1).with_feature(Feature::Fork);
    let join = Action::Join {
        member: Member::new(id("d")).with_software(older),
    };
    // Enabling the feature here would refuse the very join it was logged for, and close rollback
    // for a change that never happens.
    assert!(!state.handover_due(&join));
    change(&mut state, 10, join).expect("a mixed fleet's join goes through");
    assert!(!state.is_enabled(Feature::Handover));
    // As in a mixed fleet, the change moves the replica set at once and keeps nothing.
    assert_eq!(state.replica_set(&db(moved.name)), Ok(moved.after));
}

#[test]
fn a_state_without_kept_sets_serializes_as_before() {
    let software = current_software();
    let (state, moved) = fleet(&software);
    let text = serde_json::to_string(&state).expect("serializes");
    assert!(!text.contains("kept_replicas"), "{text}");
    let mut moved_state = state.clone();
    join_d(&mut moved_state, &software);
    let text = serde_json::to_string(&moved_state).expect("serializes");
    assert!(text.contains("kept_replicas"), "{text}");
    let loaded: State = serde_json::from_str(&text).expect("loads");
    assert_eq!(loaded, moved_state);
    assert_eq!(
        loaded.replica_set(&db(moved.name)),
        Ok(moved.before.clone())
    );
}

/// FR-102's second review, finding 1: a log a control node wrote without keeping replica sets,
/// as 0.7 writes it, with every member reporting `handover` (0.7 keeps feature names it doesn't
/// know in recorded software), replays to the state its writer had: no kept set, the feature
/// not enabled, and the replica set placement. Whether a membership change keeps a set depends
/// on the logged `EnableHandover` alone, never on what the replaying binary knows.
#[test]
fn a_log_written_without_keeping_replays_to_the_writers_state() {
    let software = current_software();
    let (mut writer, moved) = fleet(&software);
    let mut log: Vec<fleetring_control::Command> = Vec::new();
    // The writer's log, rebuilt: the fleet's commands, then d's join, logged with no
    // `EnableHandover` before it, as a control node that doesn't know the feature logs it.
    let mut replayed = State::new(crate::common::config());
    for name in ["a", "b", "c"] {
        let member = Member::new(id(name)).with_software(software.clone());
        log.push(fleetring_control::Command::new(
            crate::common::at(0),
            Action::Join { member },
        ));
    }
    log.push(fleetring_control::Command::new(
        crate::common::at(0),
        create(moved.name, 3),
    ));
    log.push(fleetring_control::Command::new(
        crate::common::at(0),
        Action::AcquireLease {
            database: db(moved.name),
            node: moved.before[0].clone(),
        },
    ));
    let member = Member::new(id("d")).with_software(software.clone());
    let join = Action::Join { member };
    assert!(writer.handover_due(&join), "the join would keep a set");
    run(&mut writer, 10, join.clone()).expect("join");
    log.push(fleetring_control::Command::new(crate::common::at(10), join));
    // The writer, without the command, kept nothing.
    assert!(!writer.is_enabled(Feature::Handover));
    assert_eq!(writer.replica_set(&db(moved.name)), Ok(moved.after.clone()));
    // Replayed through the log's serialization, from scratch and from a snapshot taken before
    // the join, it reaches the same state.
    let text = serde_json::to_string(&log).expect("serializes");
    let commands: Vec<fleetring_control::Command> = serde_json::from_str(&text).expect("loads");
    let mut snapshot = None;
    for (index, command) in commands.iter().enumerate() {
        if index + 1 == commands.len() {
            snapshot = Some(serde_json::to_string(&replayed).expect("snapshot"));
        }
        let _ = fleetring_control::apply(&mut replayed, command.clone());
    }
    assert_eq!(replayed, writer);
    assert!(
        replayed
            .database(&db(moved.name))
            .is_some_and(|database| database.kept_replicas().is_none())
    );
    let mut from_snapshot: State =
        serde_json::from_str(&snapshot.expect("taken")).expect("the snapshot loads");
    let last = commands.last().expect("a command").clone();
    let _ = fleetring_control::apply(&mut from_snapshot, last);
    assert_eq!(from_snapshot, writer);
    assert!(replayed.releases_due().is_empty());
}
