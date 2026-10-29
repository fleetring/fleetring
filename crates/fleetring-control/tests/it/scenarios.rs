//! Scenario tests: leases, failover, fencing, and membership, step by step.

use fleetring::{BuildRingError, Host, NodeId, Weight};
use fleetring_control::{
    Action, Applied, JoinKind, LeaseEpoch, Liveness, Member, Position, Rejection, State,
};

use crate::common::{at, config, create, db, heartbeat, id, run, zone};

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

fn renew(database: &str, node: &NodeId, epoch: u64) -> Action {
    Action::RenewLease {
        database: db(database),
        node: node.clone(),
        epoch: LeaseEpoch::new(epoch),
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

fn start(database: &str) -> Action {
    Action::StartPromotion {
        database: db(database),
    }
}

/// Applies `action` and asserts that it was rejected without changing the state.
fn reject(state: &mut State, millis: u64, action: Action) -> Rejection {
    let before = state.clone();
    let rejection = run(state, millis, action).expect_err("command is rejected");
    assert_eq!(*state, before, "rejected command changed the state");
    rejection
}

#[test]
fn an_empty_cluster_has_no_ring() {
    let state = State::new(config());
    assert_eq!(state.ring().err(), Some(BuildRingError::NoNodes));
}

#[test]
fn a_lease_is_acquired_renewed_and_expires() {
    let mut state = cluster(&["a", "b", "c"]);
    run(&mut state, 0, create("acme", 3)).expect("create");
    let replicas = state.replica_set(&db("acme")).expect("replica set");
    let (primary, other) = (&replicas[0], &replicas[1]);

    let Ok(Applied::LeaseGranted { lease, .. }) = run(&mut state, 100, acquire("acme", primary))
    else {
        panic!("lease granted");
    };
    assert_eq!(lease.holder(), primary);
    assert_eq!(lease.epoch(), LeaseEpoch::new(1));
    assert_eq!(lease.expires_at(), at(1_100));

    // Later holders come only from promotions.
    assert!(matches!(
        reject(&mut state, 200, acquire("acme", other)),
        Rejection::LeaseAlreadyIssued { .. }
    ));
    // Only the holder renews.
    assert!(matches!(
        reject(&mut state, 200, renew("acme", other, 1)),
        Rejection::NotLeaseHolder { .. }
    ));
    let Ok(Applied::LeaseRenewed { lease, .. }) = run(&mut state, 600, renew("acme", primary, 1))
    else {
        panic!("lease renewed");
    };
    assert_eq!(lease.expires_at(), at(1_600));

    // Failover may start only after expiry plus the 200 ms margin.
    for heartbeat_from in [primary, other] {
        let report = heartbeat(heartbeat_from.as_str(), &[("acme", &[1])]);
        run(&mut state, 1_700, report).expect("heartbeat");
    }
    assert_eq!(
        reject(&mut state, 1_800, start("acme")),
        Rejection::LeaseNotExpired {
            database: db("acme"),
            available_after: at(1_800),
        }
    );
    let Ok(Applied::PromotionStarted { epoch, .. }) = run(&mut state, 1_801, start("acme")) else {
        panic!("promotion started");
    };
    assert_eq!(epoch, LeaseEpoch::new(2));
    assert_eq!(
        state.database(&db("acme")).map(|d| d.epoch()),
        Some(LeaseEpoch::new(2))
    );
}

#[test]
fn a_lease_goes_only_to_a_live_replica_set_member() {
    let mut state = cluster(&["a", "b", "c"]);
    run(&mut state, 0, create("solo", 1)).expect("create");
    let replicas = state.replica_set(&db("solo")).expect("replica set");
    assert_eq!(replicas.len(), 1);
    let outsider = ["a", "b", "c"]
        .map(id)
        .into_iter()
        .find(|node| *node != replicas[0])
        .expect("an outsider");
    assert!(matches!(
        reject(&mut state, 10, acquire("solo", &outsider)),
        Rejection::NotInReplicaSet { .. }
    ));
    assert!(matches!(
        reject(&mut state, 10, acquire("solo", &id("ghost"))),
        Rejection::UnknownMember { .. }
    ));
    // Past the 3 s heartbeat timeout, the member is down.
    assert_eq!(
        state.liveness(&replicas[0], at(3_001)),
        Some(Liveness::Down)
    );
    assert_eq!(
        reject(&mut state, 3_001, acquire("solo", &replicas[0])),
        Rejection::MemberDown {
            id: replicas[0].clone()
        }
    );
    run(&mut state, 3_001, acquire("solo", &replicas[0])).expect_err("still down");
    run(&mut state, 3_002, heartbeat(replicas[0].as_str(), &[])).expect("heartbeat");
    run(&mut state, 3_003, acquire("solo", &replicas[0])).expect("live again");
}

#[test]
fn failover_picks_the_most_advanced_live_candidate_and_needs_a_quorum() {
    let mut state = cluster(&["a", "b", "c", "d"]);
    run(&mut state, 0, create("acme", 3)).expect("create");
    let rs = state.replica_set(&db("acme")).expect("replica set");
    assert_eq!(rs.len(), 3);
    run(&mut state, 0, acquire("acme", &rs[0])).expect("lease");

    // rs[2] holds the most advanced log but goes silent; rs[1] beats rs[0] by byte order, not by
    // length: [9, 255] < [10].
    run(
        &mut state,
        100,
        heartbeat(rs[2].as_str(), &[("acme", &[10])]),
    )
    .expect("heartbeat");
    run(
        &mut state,
        3_500,
        heartbeat(rs[0].as_str(), &[("acme", &[5])]),
    )
    .expect("heartbeat");
    run(
        &mut state,
        3_500,
        heartbeat(rs[1].as_str(), &[("acme", &[9, 255])]),
    )
    .expect("heartbeat");

    let started = run(&mut state, 3_600, start("acme")).expect("promotion starts");
    assert_eq!(
        started,
        Applied::PromotionStarted {
            database: db("acme"),
            epoch: LeaseEpoch::new(2),
            candidate: rs[1].clone(),
            position: Position::new([9, 255]),
        }
    );

    // The candidate must be fenced, and Q = 2 of the 3 members must be.
    assert_eq!(
        reject(&mut state, 3_610, complete("acme", 2)),
        Rejection::CandidateNotFenced { id: rs[1].clone() }
    );
    assert!(matches!(
        reject(&mut state, 3_610, fence("acme", &rs[2], 1, &[10])),
        Rejection::StaleEpoch { .. }
    ));
    run(&mut state, 3_610, fence("acme", &rs[1], 2, &[9, 255])).expect("fence");
    assert!(matches!(
        reject(&mut state, 3_610, fence("acme", &rs[1], 2, &[9, 255])),
        Rejection::AlreadyFenced { .. }
    ));
    assert_eq!(
        reject(&mut state, 3_620, complete("acme", 2)),
        Rejection::QuorumNotReached {
            fenced: 1,
            needed: 2
        }
    );
    // A down member can still be fenced: fencing is a fact it reports, not a sign of life.
    run(&mut state, 3_630, fence("acme", &rs[2], 2, &[10])).expect("fence");
    let completed = run(&mut state, 3_640, complete("acme", 2)).expect("promotion completes");
    let Applied::PromotionCompleted {
        lease,
        fenced,
        quorums,
        adopted,
        ..
    } = completed
    else {
        panic!("promotion completed");
    };
    assert_eq!(lease.holder(), &rs[1]);
    assert_eq!(lease.epoch(), LeaseEpoch::new(2));
    assert_eq!(lease.expires_at(), at(4_640));
    assert_eq!((fenced, quorums.promotion(), quorums.write()), (2, 2, 2));
    assert_eq!(adopted, Position::new([10]));
    let database = state.database(&db("acme")).expect("database");
    assert!(database.promotion().is_none());
    assert_eq!(database.lease(), Some(&lease));
}

#[test]
fn ties_go_to_the_earliest_member_in_preference_order() {
    let mut state = cluster(&["a", "b", "c"]);
    run(&mut state, 0, create("acme", 3)).expect("create");
    let rs = state.replica_set(&db("acme")).expect("replica set");
    run(&mut state, 0, acquire("acme", &rs[0])).expect("lease");
    for (node, position) in [(&rs[2], [7]), (&rs[1], [7]), (&rs[0], [3])] {
        run(
            &mut state,
            1_000,
            heartbeat(node.as_str(), &[("acme", &position)]),
        )
        .expect("hb");
    }
    let Ok(Applied::PromotionStarted { candidate, .. }) = run(&mut state, 1_201, start("acme"))
    else {
        panic!("promotion started");
    };
    assert_eq!(candidate, rs[1]);
}

#[test]
fn a_stale_holder_cannot_renew_once_a_newer_epoch_exists() {
    let mut state = cluster(&["a", "b"]);
    run(&mut state, 0, create("acme", 2)).expect("create");
    let rs = state.replica_set(&db("acme")).expect("replica set");
    let (old, new) = (&rs[0], &rs[1]);
    run(&mut state, 0, acquire("acme", old)).expect("lease");
    run(
        &mut state,
        1_500,
        heartbeat(new.as_str(), &[("acme", &[4])]),
    )
    .expect("heartbeat");
    run(&mut state, 1_500, start("acme")).expect("promotion starts");

    // Once epoch 2 is issued, the epoch-1 holder can't extend its lease, even mid-promotion.
    let stale = Rejection::StaleEpoch {
        database: db("acme"),
        given: LeaseEpoch::new(1),
        current: LeaseEpoch::new(2),
    };
    assert_eq!(reject(&mut state, 1_510, renew("acme", old, 1)), stale);

    // R = 2, so W = 2 and Q = 1: the candidate alone is a promotion quorum.
    run(&mut state, 1_520, fence("acme", new, 2, &[4])).expect("fence");
    run(&mut state, 1_530, complete("acme", 2)).expect("complete");
    assert_eq!(reject(&mut state, 1_540, renew("acme", old, 1)), stale);
    assert!(matches!(
        reject(&mut state, 1_540, renew("acme", old, 2)),
        Rejection::NotLeaseHolder { .. }
    ));
    run(&mut state, 1_550, renew("acme", new, 2)).expect("new holder renews");
}

#[test]
fn a_stalled_promotion_can_be_superseded_with_a_higher_epoch() {
    let mut state = cluster(&["a", "b", "c"]);
    run(&mut state, 0, create("acme", 3)).expect("create");
    let rs = state.replica_set(&db("acme")).expect("replica set");
    run(&mut state, 0, acquire("acme", &rs[0])).expect("lease");
    run(
        &mut state,
        1_000,
        heartbeat(rs[1].as_str(), &[("acme", &[9])]),
    )
    .expect("hb");
    run(
        &mut state,
        1_000,
        heartbeat(rs[2].as_str(), &[("acme", &[8])]),
    )
    .expect("hb");
    run(&mut state, 1_201, start("acme")).expect("start");
    assert!(matches!(
        reject(&mut state, 1_300, start("acme")),
        Rejection::PromotionInProgress { .. }
    ));

    // rs[1] goes silent; rs[2] keeps reporting.
    run(
        &mut state,
        3_900,
        heartbeat(rs[2].as_str(), &[("acme", &[8])]),
    )
    .expect("hb");
    let Ok(Applied::PromotionStarted {
        epoch, candidate, ..
    }) = run(&mut state, 4_100, start("acme"))
    else {
        panic!("superseding promotion");
    };
    assert_eq!((epoch, &candidate), (LeaseEpoch::new(3), &rs[2]));
    // Fences from the abandoned epoch no longer count.
    assert!(matches!(
        reject(&mut state, 4_110, fence("acme", &rs[2], 2, &[8])),
        Rejection::StaleEpoch { .. }
    ));
}

#[test]
fn stamps_that_go_backwards_count_as_the_last_accepted_time() {
    let mut state = cluster(&["a"]);
    run(&mut state, 5_000, heartbeat("a", &[])).expect("heartbeat");
    run(&mut state, 1_000, create("acme", 1)).expect("create");
    assert_eq!(state.now(), at(5_000));

    let Ok(Applied::LeaseGranted { lease, .. }) = run(&mut state, 2_000, acquire("acme", &id("a")))
    else {
        panic!("lease granted");
    };
    assert_eq!(lease.expires_at(), at(6_000));
    assert_eq!(state.now(), at(5_000));
    // A rejected command doesn't move time either.
    reject(&mut state, 9_000, acquire("acme", &id("a")));
    assert_eq!(state.now(), at(5_000));
}

#[test]
fn membership_changes_bump_the_membership_epoch() {
    let mut state = cluster(&["a", "b"]);
    assert_eq!(state.membership_epoch().get(), 2);

    let same = state.member(&id("a")).cloned().expect("member");
    let rejoined = run(
        &mut state,
        10,
        Action::Join {
            member: same.clone(),
        },
    );
    assert!(matches!(
        rejoined,
        Ok(Applied::MemberJoined {
            kind: JoinKind::Rejoined,
            ..
        })
    ));
    assert_eq!(state.membership_epoch().get(), 2);

    let heavier = same.with_weight(Weight::new(5).expect("nonzero"));
    let updated = run(&mut state, 20, Action::Join { member: heavier });
    assert!(matches!(
        updated,
        Ok(Applied::MemberJoined {
            kind: JoinKind::Updated,
            ..
        })
    ));
    assert_eq!(state.membership_epoch().get(), 3);
    assert_eq!(state.ring().expect("ring").epoch().get(), 3);

    run(&mut state, 30, Action::Leave { id: id("b") }).expect("leave");
    assert_eq!(state.membership_epoch().get(), 4);
    assert_eq!(state.ring().expect("ring").node_count(), 1);
    reject(&mut state, 40, Action::Leave { id: id("b") });
}

#[test]
fn a_new_address_bumps_the_membership_epoch_but_moves_no_key() {
    let mut state = cluster(&["a", "b", "c"]);
    run(&mut state, 0, create("acme", 2)).expect("create");
    let before = state.replica_set(&db("acme")).expect("replica set");
    let fingerprint = state.ring().expect("ring").fingerprint();

    let member = state.member(&id("a")).cloned().expect("member");
    let moved = member.with_address("10.0.0.1:7400");
    let joined = run(
        &mut state,
        10,
        Action::Join {
            member: moved.clone(),
        },
    );
    assert!(matches!(
        joined,
        Ok(Applied::MemberJoined {
            kind: JoinKind::Updated,
            ..
        })
    ));
    assert_eq!(state.membership_epoch().get(), 4);
    assert_eq!(
        state.member(&id("a")).and_then(Member::address),
        Some("10.0.0.1:7400")
    );
    assert_eq!(state.replica_set(&db("acme")).expect("replica set"), before);
    let ring = state.ring().expect("ring");
    assert_eq!(ring.epoch().get(), 4);
    assert_eq!(
        ring.fingerprint(),
        fingerprint,
        "addresses aren't placed by"
    );

    // The same address again is a plain rejoin.
    run(&mut state, 20, Action::Join { member: moved }).expect("rejoin");
    assert_eq!(state.membership_epoch().get(), 4);
}

#[test]
fn a_join_that_would_break_the_ring_is_rejected() {
    let mut state = State::new(config());
    let host: Host = "box".parse().expect("valid host");
    let first = Member::new(id("a"))
        .with_zone(zone("z0"))
        .with_host(host.clone());
    run(&mut state, 0, Action::Join { member: first }).expect("join");
    let second = Member::new(id("b")).with_zone(zone("z1")).with_host(host);
    assert!(matches!(
        reject(&mut state, 0, Action::Join { member: second }),
        Rejection::InvalidTopology { .. }
    ));
}

#[test]
fn heartbeats_skip_unknown_databases_and_name_them() {
    let mut state = cluster(&["a"]);
    run(&mut state, 0, create("acme", 1)).expect("create");
    // A database the control plane doesn't know costs the node nothing: the heartbeat counts,
    // the known report is recorded, and the unknown name comes back.
    let applied = run(
        &mut state,
        10,
        heartbeat("a", &[("nope", &[1]), ("acme", &[1])]),
    )
    .expect("heartbeat");
    assert!(matches!(
        &applied,
        Applied::HeartbeatRecorded { unknown, .. } if unknown == &[db("nope")]
    ));
    assert_eq!(state.last_heartbeat(&id("a")), Some(at(10)));
    let report = state.report(&id("a"), &db("acme")).expect("report");
    assert_eq!(report.position(), &Position::new([1]));
    assert_eq!(report.at(), at(10));
    assert!(state.report(&id("a"), &db("nope")).is_none());
    assert!(matches!(
        reject(
            &mut state,
            10,
            heartbeat("a", &[("acme", &[1]), ("acme", &[2])])
        ),
        Rejection::DuplicateReport { .. }
    ));
    assert!(matches!(
        reject(&mut state, 10, heartbeat("ghost", &[])),
        Rejection::UnknownMember { .. }
    ));
}

#[test]
fn heartbeats_from_old_logs_keep_rejecting_unknown_databases() {
    // A heartbeat logged before `unknown_reports` existed carries no such field.
    let old = r#"{"heartbeat":{"id":"a","replicas":[{"database":"nope","role":"follower","position":[1]}]}}"#;
    let action: Action = serde_json::from_str(old).expect("an old heartbeat loads");
    assert!(matches!(
        action,
        Action::Heartbeat {
            unknown_reports: fleetring_control::UnknownReports::Reject,
            ..
        }
    ));
    let mut state = cluster(&["a"]);
    assert!(matches!(
        reject(&mut state, 10, action),
        Rejection::UnknownDatabase { .. }
    ));
    assert_eq!(state.last_heartbeat(&id("a")), Some(at(0)));
}

#[test]
fn rejection_messages_are_lowercase() {
    let rejection = Rejection::QuorumNotReached {
        fenced: 1,
        needed: 2,
    };
    assert_eq!(
        rejection.to_string(),
        "promotion needs 2 fenced members; 1 are fenced"
    );
    let rejection = Rejection::StaleEpoch {
        database: db("acme"),
        given: LeaseEpoch::new(1),
        current: LeaseEpoch::new(2),
    };
    assert_eq!(
        rejection.to_string(),
        "epoch 1 of database 'acme' is stale; the current epoch is 2"
    );
}

fn heartbeat_at(node: &str, database: &str, log_epoch: u64, position: &[u8]) -> Action {
    Action::Heartbeat {
        id: id(node),
        replicas: vec![
            fleetring_control::HostedReplica::new(
                db(database),
                fleetring_control::Role::Follower,
                Position::new(position),
            )
            .with_log_epoch(LeaseEpoch::new(log_epoch)),
        ],
        unknown_reports: fleetring_control::UnknownReports::Skip,
    }
}

#[test]
fn a_tick_only_moves_time_and_lets_a_promotion_become_due() {
    let mut state = cluster(&["a", "b", "c"]);
    run(&mut state, 0, create("acme", 3)).expect("create");
    let replicas = state.replica_set(&db("acme")).expect("replica set");
    run(&mut state, 0, acquire("acme", &replicas[0])).expect("lease");
    for follower in &replicas[1..] {
        run(
            &mut state,
            900,
            heartbeat(follower.as_str(), &[("acme", &[1])]),
        )
        .expect("heartbeat");
    }

    let before = state.clone();
    let Ok(Applied::Ticked { at: ticked }) = run(&mut state, 1_100, Action::Tick) else {
        panic!("ticked");
    };
    assert_eq!(ticked, at(1_100));
    assert_eq!(state.now(), at(1_100));
    let mut moved = before.clone();
    run(&mut moved, 1_100, Action::Tick).expect("tick");
    assert_eq!(moved, state, "a tick is deterministic");
    assert_eq!(
        state.databases().collect::<Vec<_>>(),
        before.databases().collect::<Vec<_>>()
    );
    assert_eq!(state.membership_epoch(), before.membership_epoch());

    // Expiry plus the margin is 1 200 ms; the followers are live until 3 900 ms.
    assert!(state.promotions_due(at(1_200)).is_empty());
    assert_eq!(state.promotions_due(at(1_201)), vec![db("acme")]);
    // A tick past expiry makes the promotion due at the state's own time.
    run(&mut state, 1_300, Action::Tick).expect("tick");
    assert_eq!(state.promotions_due(at(0)), vec![db("acme")]);
    run(&mut state, 1_300, start("acme")).expect("promotion started");
    assert!(
        state.promotions_due(at(1_300)).is_empty(),
        "already in progress"
    );
    // Once nobody is live, nothing is due.
    assert!(state.promotions_due(at(10_000)).is_empty());
}

#[test]
fn the_candidate_has_the_newest_log_epoch_before_the_highest_position() {
    let mut state = cluster(&["a", "b", "c"]);
    run(&mut state, 0, create("acme", 3)).expect("create");
    let replicas = state.replica_set(&db("acme")).expect("replica set");
    run(&mut state, 0, acquire("acme", &replicas[0])).expect("lease");
    // The second replica holds a longer log, but from an older primary.
    run(
        &mut state,
        900,
        heartbeat_at(replicas[1].as_str(), "acme", 1, &[9]),
    )
    .expect("heartbeat");
    run(
        &mut state,
        900,
        heartbeat_at(replicas[2].as_str(), "acme", 2, &[5]),
    )
    .expect("heartbeat");
    let Ok(Applied::PromotionStarted {
        candidate,
        position,
        ..
    }) = run(&mut state, 1_300, start("acme"))
    else {
        panic!("promotion started");
    };
    assert_eq!(candidate, replicas[2]);
    assert_eq!(position, Position::new([5]));
}

#[test]
fn reports_without_a_log_epoch_load_from_old_logs() {
    let old = r#"{"database":"acme","role":"follower","position":[1,2]}"#;
    let replica: fleetring_control::HostedReplica =
        serde_json::from_str(old).expect("an old report loads");
    assert_eq!(replica.log_epoch(), LeaseEpoch::ZERO);
    assert_eq!(serde_json::to_string(&replica).expect("serializes"), old);
    let tick = serde_json::to_string(&Action::Tick).expect("serializes");
    let back: Action = serde_json::from_str(&tick).expect("a tick loads");
    assert_eq!(back, Action::Tick);
}
