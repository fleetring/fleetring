//! FR-035: durability settings per database (ADR-0023): quorums per mode, the floor, changes
//! that wait for the next lease, and old logs that load with the defaults.

use fleetring::NodeId;
use fleetring_control::{
    Acks, Action, Applied, Database, Durability, DurabilityError, HostedReplica, LeaseEpoch,
    Member, Position, Quorums, Rejection, Role, State, UnknownReports,
};

use crate::common::{config, create, create_with, db, factor, heartbeat, id, run, zone};

/// A state with members `ids` joined at time 0, each in its own zone.
fn cluster(ids: &[&str]) -> State {
    let mut state = State::new(config());
    for (i, name) in ids.iter().enumerate() {
        let member = Member::new(id(name)).with_zone(zone(&format!("z{i}")));
        run(&mut state, 0, Action::Join { member }).expect("join");
    }
    state
}

fn acquire(database: &str, node: &NodeId) -> Action {
    Action::AcquireLease {
        database: db(database),
        node: node.clone(),
    }
}

fn change(database: &str, durability: Durability) -> Action {
    Action::ChangeDurability {
        database: db(database),
        durability,
    }
}

fn fence(database: &str, node: &NodeId, epoch: u64, position: &[u8]) -> Action {
    Action::RecordFenced {
        database: db(database),
        node: node.clone(),
        epoch: LeaseEpoch::new(epoch),
        position: Position::new(position),
    }
}

fn complete(database: &str, epoch: u64) -> Action {
    Action::CompletePromotion {
        database: db(database),
        epoch: LeaseEpoch::new(epoch),
    }
}

#[test]
fn each_mode_fixes_the_write_and_promotion_quorums() {
    for (replicas, quorum, all, local) in [
        (1, (1, 1), (1, 1), (1, 1)),
        (2, (2, 1), (2, 1), (1, 2)),
        (3, (2, 2), (3, 1), (1, 2)),
        (4, (3, 2), (4, 1), (1, 3)),
        (5, (3, 3), (5, 1), (1, 3)),
    ] {
        let size = factor(replicas);
        let of = |acks| {
            let quorums = Quorums::with_acks(size, acks);
            (quorums.write(), quorums.promotion())
        };
        assert_eq!(of(Acks::Quorum), quorum, "quorum, R = {replicas}");
        assert_eq!(of(Acks::All), all, "all, R = {replicas}");
        assert_eq!(of(Acks::Local), local, "local, R = {replicas}");
        assert_eq!(Quorums::new(size), Quorums::with_acks(size, Acks::Quorum));
        // Safety holds for the modes that promise it: W + Q > R.
        for acks in [Acks::Quorum, Acks::All] {
            let (write, promotion) = of(acks);
            assert!(write + promotion > replicas, "{acks}, R = {replicas}");
        }
    }
    assert!(Acks::Quorum.acknowledged_writes_survive());
    assert!(Acks::All.acknowledged_writes_survive());
    assert!(!Acks::Local.acknowledged_writes_survive());
}

#[test]
fn the_floor_defaults_to_w_and_must_lie_between_w_and_r() {
    let three = factor(3);
    assert_eq!(Durability::new(Acks::Quorum, three).min_replicas(), 2);
    assert_eq!(Durability::new(Acks::All, three).min_replicas(), 3);
    assert_eq!(Durability::new(Acks::Local, three).min_replicas(), 1);
    assert_eq!(
        Durability::with_min_replicas(Acks::Quorum, 3, three).map(|d| d.min_replicas()),
        Ok(3)
    );
    assert_eq!(
        Durability::with_min_replicas(Acks::Quorum, 1, three),
        Err(DurabilityError::BelowWriteQuorum {
            min_replicas: 1,
            write: 2
        })
    );
    assert_eq!(
        Durability::with_min_replicas(Acks::Local, 4, three),
        Err(DurabilityError::AboveReplicationFactor {
            min_replicas: 4,
            replication_factor: 3
        })
    );
    let error = Durability::with_min_replicas(Acks::All, 2, three).expect_err("below W");
    assert_eq!(
        error.to_string(),
        "min_replicas 2 is below the write quorum 3"
    );
}

#[test]
fn a_database_records_its_settings_and_the_state_answers_quorums_from_them() {
    let mut state = cluster(&["a", "b", "c"]);
    run(&mut state, 0, create("majority", 3)).expect("create");
    run(&mut state, 0, create_with("everyone", 3, Acks::All)).expect("create");
    run(&mut state, 0, create_with("alone", 3, Acks::Local)).expect("create");
    let settings = |name: &str| state.database(&db(name)).expect("exists").durability();
    assert_eq!(settings("majority"), Durability::quorum(factor(3)));
    assert_eq!(settings("everyone"), Durability::new(Acks::All, factor(3)));
    assert_eq!(settings("alone"), Durability::new(Acks::Local, factor(3)));
    let quorums = |name: &str| {
        let quorums = state.quorums(&db(name)).expect("quorums");
        (quorums.write(), quorums.promotion())
    };
    assert_eq!(quorums("majority"), (2, 2));
    assert_eq!(quorums("everyone"), (3, 1));
    assert_eq!(quorums("alone"), (1, 2));

    // Settings that don't fit the replication factor are rejected at creation.
    let bad = Action::CreateDatabase {
        name: db("bad"),
        replication_factor: factor(3),
        engine: fleetring_control::Engine::Sqlite,
        durability: Some(Durability::new(Acks::All, factor(5))),
    };
    let before = state.clone();
    let rejection = run(&mut state, 0, bad).expect_err("floor above R");
    assert!(
        matches!(
            rejection,
            Rejection::InvalidDurability {
                source: DurabilityError::AboveReplicationFactor { .. },
                ..
            }
        ),
        "{rejection}"
    );
    assert_eq!(state, before);
}

#[test]
fn a_promotion_fences_under_each_modes_quorum() {
    // `all`: one fenced member completes the promotion.
    let mut state = cluster(&["a", "b", "c"]);
    run(&mut state, 0, create_with("everyone", 3, Acks::All)).expect("create");
    let replicas = state.replica_set(&db("everyone")).expect("replica set");
    run(&mut state, 0, acquire("everyone", &replicas[0])).expect("lease");
    for follower in &replicas[1..] {
        run(
            &mut state,
            2_000,
            heartbeat(follower.as_str(), &[("everyone", &[1])]),
        )
        .expect("heartbeat");
    }
    let started = run(
        &mut state,
        2_000,
        Action::StartPromotion {
            database: db("everyone"),
        },
    )
    .expect("promotion starts");
    let Applied::PromotionStarted { candidate, .. } = started else {
        panic!("expected a promotion, got {started:?}");
    };
    let recorded = run(&mut state, 2_000, fence("everyone", &candidate, 2, &[1])).expect("fence");
    assert!(
        matches!(
            recorded,
            Applied::FenceRecorded {
                fenced: 1,
                needed: 1,
                ..
            }
        ),
        "{recorded:?}"
    );
    run(&mut state, 2_000, complete("everyone", 2)).expect("one fenced member is Q for all");

    // `local`: a majority must be fenced, as for `quorum`.
    let mut state = cluster(&["a", "b", "c"]);
    run(&mut state, 0, create_with("alone", 3, Acks::Local)).expect("create");
    let replicas = state.replica_set(&db("alone")).expect("replica set");
    run(&mut state, 0, acquire("alone", &replicas[0])).expect("lease");
    for follower in &replicas[1..] {
        run(
            &mut state,
            2_000,
            heartbeat(follower.as_str(), &[("alone", &[1])]),
        )
        .expect("heartbeat");
    }
    let started = run(
        &mut state,
        2_000,
        Action::StartPromotion {
            database: db("alone"),
        },
    )
    .expect("promotion starts");
    let Applied::PromotionStarted { candidate, .. } = started else {
        panic!("expected a promotion, got {started:?}");
    };
    run(&mut state, 2_000, fence("alone", &candidate, 2, &[1])).expect("fence");
    let early = run(&mut state, 2_000, complete("alone", 2)).expect_err("one is too few");
    assert!(
        matches!(
            early,
            Rejection::QuorumNotReached {
                fenced: 1,
                needed: 2
            }
        ),
        "{early}"
    );
}

#[test]
fn a_durability_change_waits_for_the_next_lease() {
    let mut state = cluster(&["a", "b", "c"]);
    run(&mut state, 0, create("acme", 3)).expect("create");
    let replicas = state.replica_set(&db("acme")).expect("replica set");
    run(&mut state, 0, acquire("acme", &replicas[0])).expect("lease");
    let stricter = Durability::new(Acks::All, factor(3));
    let changed = run(&mut state, 100, change("acme", stricter)).expect("change");
    assert_eq!(
        changed,
        Applied::DurabilityChanged {
            database: db("acme"),
            durability: stricter,
            current: Durability::quorum(factor(3)),
        }
    );
    let database = state.database(&db("acme")).expect("exists");
    assert_eq!(database.durability(), Durability::quorum(factor(3)));
    assert_eq!(database.pending_durability(), Some(stricter));
    // The primary's quorum doesn't change under it.
    assert_eq!(state.quorums(&db("acme")).expect("quorums").write(), 2);

    // The promotion fences a majority, the old setting's Q, then the new setting takes effect.
    for follower in &replicas[1..] {
        run(
            &mut state,
            2_000,
            heartbeat(follower.as_str(), &[("acme", &[1])]),
        )
        .expect("heartbeat");
    }
    let started = run(
        &mut state,
        2_000,
        Action::StartPromotion {
            database: db("acme"),
        },
    )
    .expect("promotion starts");
    let Applied::PromotionStarted { candidate, .. } = started else {
        panic!("expected a promotion, got {started:?}");
    };
    run(&mut state, 2_000, fence("acme", &candidate, 2, &[1])).expect("fence");
    let early = run(&mut state, 2_000, complete("acme", 2)).expect_err("one is too few");
    assert!(
        matches!(early, Rejection::QuorumNotReached { needed: 2, .. }),
        "{early}"
    );
    let other = replicas
        .iter()
        .find(|node| **node != candidate)
        .expect("another replica");
    run(&mut state, 2_000, fence("acme", other, 2, &[1])).expect("fence");
    run(&mut state, 2_000, complete("acme", 2)).expect("complete");
    let database = state.database(&db("acme")).expect("exists");
    assert_eq!(database.durability(), stricter);
    assert_eq!(database.pending_durability(), None);
    assert_eq!(state.quorums(&db("acme")).expect("quorums").write(), 3);

    // A change that doesn't fit is rejected; one before the first lease applies at that lease.
    let rejection = run(
        &mut state,
        2_100,
        change(
            "acme",
            Durability::with_min_replicas(Acks::Local, 1, factor(1)).expect("fits R = 1"),
        ),
    )
    .expect("local with min_replicas 1 fits R = 3");
    assert!(matches!(rejection, Applied::DurabilityChanged { .. }));
    let bad = Durability::new(Acks::Quorum, factor(7));
    let rejection = run(&mut state, 2_100, change("acme", bad)).expect_err("floor above R");
    assert!(
        matches!(rejection, Rejection::InvalidDurability { .. }),
        "{rejection}"
    );
    run(&mut state, 2_100, create("fresh", 3)).expect("create");
    run(&mut state, 2_100, change("fresh", stricter)).expect("change");
    let replicas = state.replica_set(&db("fresh")).expect("replica set");
    run(&mut state, 2_100, acquire("fresh", &replicas[0])).expect("lease");
    assert_eq!(
        state.database(&db("fresh")).expect("exists").durability(),
        stricter
    );
}

#[test]
fn old_logs_and_snapshots_load_as_quorum_with_the_floor_at_w() {
    // A creation logged before the field existed carries no durability.
    let old = r#"{"create_database":{"name":"acme","replication_factor":3,"engine":"sqlite"}}"#;
    let action: Action = serde_json::from_str(old).expect("an old creation loads");
    assert!(matches!(
        action,
        Action::CreateDatabase {
            durability: None,
            ..
        }
    ));
    let mut state = cluster(&["a", "b", "c"]);
    run(&mut state, 0, action).expect("create");
    let database = state.database(&db("acme")).expect("exists");
    assert_eq!(database.durability(), Durability::quorum(factor(3)));
    assert_eq!(database.durability().min_replicas(), 2);
    assert_eq!(database.pending_durability(), None);

    // A database in a snapshot taken before the fields existed.
    let old = r#"{"name":"acme","replication_factor":4,"engine":"sqlite","epoch":0,"lease":null,"promotion":null}"#;
    let database: Database = serde_json::from_str(old).expect("an old record loads");
    assert_eq!(database.durability(), Durability::quorum(factor(4)));
    assert_eq!(database.durability().min_replicas(), 3);
    assert_eq!(database.pending_durability(), None);

    // New records round-trip with the fields, and a change round-trips too.
    let mut state = cluster(&["a"]);
    run(&mut state, 0, create_with("solo", 1, Acks::Local)).expect("create");
    run(
        &mut state,
        0,
        change("solo", Durability::new(Acks::All, factor(1))),
    )
    .expect("change");
    let encoded = serde_json::to_string(&state).expect("serializes");
    assert!(encoded.contains(r#""acks":"local""#), "{encoded}");
    assert!(encoded.contains(r#""min_replicas":1"#), "{encoded}");
    let back: State = serde_json::from_str(&encoded).expect("loads");
    assert_eq!(back, state);
    let logged = serde_json::to_string(&change("solo", Durability::new(Acks::All, factor(1))))
        .expect("serializes");
    assert_eq!(
        logged,
        r#"{"change_durability":{"database":"solo","durability":{"acks":"all","min_replicas":1}}}"#
    );
    let back: Action = serde_json::from_str(&logged).expect("loads");
    assert_eq!(back, change("solo", Durability::new(Acks::All, factor(1))));
}

fn report_at(node: &str, database: &str, log_epoch: u64, position: &[u8]) -> Action {
    Action::Heartbeat {
        id: id(node),
        replicas: vec![
            HostedReplica::new(db(database), Role::Follower, Position::new(position))
                .with_log_epoch(LeaseEpoch::new(log_epoch)),
        ],
        unknown_reports: UnknownReports::Skip,
    }
}

/// A `local` primary acknowledges on its own, so its followers may hold logs from different
/// epochs. A transfer that names no target goes to the follower with the newest log epoch, even
/// if another has a longer log from an older epoch; one that names a follower goes there.
#[test]
fn a_transfer_without_a_target_ranks_local_followers_by_log_epoch_first() {
    let mut state = cluster(&["a", "b", "c"]);
    run(&mut state, 0, create_with("acme", 3, Acks::Local)).expect("create");
    let rs = state.replica_set(&db("acme")).expect("replica set");
    run(&mut state, 0, acquire("acme", &rs[0])).expect("lease");
    // The second replica has the longer log, but from before the current epoch; the third
    // follows the current primary.
    run(
        &mut state,
        1_000,
        report_at(rs[1].as_str(), "acme", 0, &[20]),
    )
    .expect("report");
    run(
        &mut state,
        1_000,
        report_at(rs[2].as_str(), "acme", 1, &[3]),
    )
    .expect("report");
    run(&mut state, 1_010, Action::Drain { id: rs[0].clone() }).expect("drain");

    let started = run(
        &mut state,
        1_020,
        Action::TransferLease {
            database: db("acme"),
            target: None,
        },
    )
    .expect("transfer");
    assert_eq!(
        started,
        Applied::PromotionStarted {
            database: db("acme"),
            epoch: LeaseEpoch::new(2),
            candidate: rs[2].clone(),
            position: Position::new([3]),
        }
    );
    // The promotion fences under `local`'s Q: a majority, so the handed-over log is adopted
    // only once two members, the candidate among them, have fenced.
    assert_eq!(state.quorums(&db("acme")).expect("quorums").promotion(), 2);
}
