//! Rows 12 to 22 of FR-087's matrix: the control plane, clients and operators, the network, a
//! replaced fork, the reset, disk, and change history after readiness.
//!
//! Every row runs on real data nodes. These parts are control-plane messages a scenario sends
//! itself, because they are about how the control plane handles messages rather than about a
//! data path: row 13's duplicated heartbeats, row 14's resubmissions, and row 18's delayed
//! proposals and reports of a dropped fork identity. Row 18's late seeding requests are real
//! `Snapshot` calls to the data nodes' `seeding` binding.
//!
//! Row 23 goes beyond the ticket's 22: ADR-0046 point 12's "placement changes between `Seeded`
//! and `Ready`", with the hand-over to placement that follows.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use fleetring::NodeId;
use fleetring_control::fork::{
    ChainHash, ForkFailure, ForkIdentity, ForkState, OperationId, ResetReport, RetryCause,
    SeedFailure, SeedFailureReason, StagedCopy,
};
use fleetring_control::{
    Action, DatabaseName, HostedReplica, Incarnation, LeaseEpoch, Position, ReplicaRole,
};
use fleetringd::replication::FaultPoint;
use fleetringd::sim::{
    Ack, DurabilityMode, Expected, Fleet, LossRate, OTHER_PRINCIPAL, Operation, Outcome, Script,
    SeedingProbe,
};

use super::matrix::{
    CONTROL, LIMIT, NODES, Point, REPLICAS, Result, Run, Setup, fork_request, row,
};

/// Row 12: the control node crashes at each step and restarts.
fn control_restart(seed: u64, point: Point) -> Result<()> {
    let mut run = Run::start(seed, &Setup::default())?;
    let target = "dst".parse()?;
    let (operation, _) = run.fork_to(&target, point)?;
    run.crash(CONTROL)?;
    run.release(point.hook(), operation)?;
    run.wait(Duration::from_secs(1))?;
    run.restart(CONTROL)?;
    run.finish(operation, Expected::Ready)
}

row!(
    row_12_control_restart,
    sweep_row_12_control_restart,
    Point::ALL,
    control_restart
);

/// The full report `node` last made, as control state holds it, to send again: a duplicated,
/// late heartbeat.
fn replay_report(run: &Run, node: &str) -> Result<Operation> {
    let state = run.state()?;
    let id: NodeId = node.parse()?;
    let mut replicas = Vec::new();
    for db in state.databases() {
        let Some(report) = state.report(&id, db.name()) else {
            continue;
        };
        let mut replica =
            HostedReplica::new(db.name().clone(), report.role(), report.position().clone())
                .with_log_epoch(report.log_epoch());
        if let Some(staged) = report.staged() {
            replica = replica.with_staged(staged.clone());
        }
        if let Some(reset) = report.reset() {
            replica = replica.with_reset(reset.clone());
        }
        if let Some(failure) = report.seed_failure() {
            replica = replica.with_seed_failure(failure.clone());
        }
        replicas.push(replica);
    }
    Ok(Operation::Report {
        node: id,
        replicas,
        disk_free_bytes: Some(1 << 40),
    })
}

/// Sends every replica's last heartbeat again, twice, late: once the fork's work held at `point`
/// goes on, half a second after.
fn duplicate_heartbeats(run: &mut Run, point: Point, operation: OperationId) -> Result<()> {
    let duplicates = NODES
        .iter()
        .map(|node| replay_report(run, node))
        .collect::<Result<Vec<_>>>()?;
    run.release(point.hook(), operation)?;
    run.wait(Duration::from_millis(500))?;
    for duplicate in duplicates {
        run.operator.push(duplicate.clone());
        run.operator.push(duplicate);
    }
    run.wait(Duration::from_secs(2))
}

/// Row 13: a lost `ForkDatabase` reply, and duplicated, late heartbeats, at each step. The
/// control service applies the fork request and answers `UNAVAILABLE` in place of its reply, as
/// a reply lost on the way looks to the client, whose client library resends it with its
/// operation ID and must get the fork the first request made (property f); then each replica's
/// last heartbeat is sent again, twice, late, and must be ignored as stale.
fn lost_reply_and_duplicates(seed: u64, point: Point) -> Result<()> {
    let mut run = Run::start(seed, &Setup::default())?;
    let target: DatabaseName = "dst".parse()?;
    let operation = run.operation_id();
    run.hold(point.hook(), operation)?;
    run.faults(CONTROL)?.drop_next_fork_reply();
    run.fork_as(&target, operation)?;
    run.reach(operation, point)?;
    let incarnation = run.identity(&target, operation)?.incarnation;
    run.fleet
        .run_until(LIMIT, "the resent fork request answered", |fleet| {
            fleet.history().records().iter().any(|record| {
                matches!(record.operation(), Operation::Fork { operation: id, .. } if *id == operation)
                    && matches!(record.outcome(), Outcome::Acknowledged(Ack::Forked { .. }))
            })
        })?;
    // The client resends a request whose reply was lost: the answer it got came after the
    // lost one, and names the fork the first request made.
    if run.faults(CONTROL)?.drops_next_fork_reply() {
        return Err("the control service never lost a fork request's reply".into());
    }
    let answers: Vec<Outcome> = run
        .fleet
        .history()
        .records()
        .iter()
        .filter(|record| {
            matches!(record.operation(), Operation::Fork { operation: id, .. } if *id == operation)
        })
        .map(|record| record.outcome().clone())
        .collect();
    let same = answers.iter().all(|outcome| {
        matches!(outcome, Outcome::Acknowledged(Ack::Forked { incarnation: got }) if *got == incarnation)
            || matches!(outcome, Outcome::Indeterminate { .. })
    });
    if !same {
        return Err(format!(
            "the resent fork request was to be answered with fork incarnation {incarnation}: {answers:?}"
        )
        .into());
    }
    duplicate_heartbeats(&mut run, point, operation)?;
    run.finish(operation, Expected::Ready)
}

row!(
    row_13_lost_reply_and_duplicates,
    sweep_row_13_lost_reply_and_duplicates,
    Point::ALL,
    lost_reply_and_duplicates
);

/// Row 13, under link loss: lost heartbeats and their replies, at each step. Every replica's link
/// to the control node loses a fifth of its messages for a while, and each replica's last
/// heartbeat is sent again, twice, late.
fn link_loss(seed: u64, point: Point) -> Result<()> {
    let mut run = Run::start(seed, &Setup::default())?;
    let target: DatabaseName = "dst".parse()?;
    let (operation, _) = run.fork_to(&target, point)?;
    for node in NODES {
        run.fleet
            .set_loss(node, CONTROL, LossRate::per_mille(200))?;
    }
    duplicate_heartbeats(&mut run, point, operation)?;
    for node in NODES {
        run.fleet.set_loss(node, CONTROL, LossRate::NONE)?;
    }
    run.finish(operation, Expected::Ready)
}

row!(
    row_13_link_loss,
    sweep_row_13_link_loss,
    Point::ALL,
    link_loss
);

/// Row 13 under link loss, on the seeds where the first lease's holder ran the fork reset after
/// a promotion had opened the fork and acknowledged writes (fork property (h)): its lease expired
/// while its `Locate` after the lease stalled, and it never saw the promotion's epoch.
#[test]
fn row_13_link_loss_a_superseded_first_lease_runs_no_reset() {
    super::matrix::pinned(&[904, 526], Point::BeforeFirstLease, link_loss);
}

/// Row 13 under link loss, on the seed where the source's primary, its control calls stalled,
/// refused the attempt's seeder for its binding once the source's lease had moved: a request made
/// at another moment, answered with a retry-after (ADR-0046, points 4.1 and 4.4, as amended).
#[test]
fn row_13_link_loss_a_moved_source_lease_is_answered_not_refused() {
    super::matrix::pinned(&[870], Point::BeforePin, link_loss);
}

/// Runs `operation` on a client named `name` as `principal` until it is answered, and returns
/// the answer.
fn ask(run: &mut Run, name: &str, principal: &str, operation: Operation) -> Result<Outcome> {
    run.fleet
        .client_as(name, principal, Script::new([operation]));
    let mut found = None;
    run.fleet
        .run_until(LIMIT, &format!("{name} answered"), |fleet| {
            found = fleet
                .history()
                .records()
                .iter()
                .filter(|record| record.client() == name)
                .map(|record| record.outcome().clone())
                .find(|outcome| !matches!(outcome, Outcome::Indeterminate { .. }));
            found.is_some()
        })?;
    Ok(found.ok_or("no answer")?)
}

/// Checks that `outcome`, the answer to `what`, is what `want` says.
fn expect_answer(outcome: &Outcome, what: &str, want: impl Fn(&Outcome) -> bool) -> Result<()> {
    if want(outcome) {
        Ok(())
    } else {
        Err(format!("{what} was answered {outcome}").into())
    }
}

/// A seven-day operation retention, as the fork's limits record it by default.
const RETENTION: Duration = Duration::from_secs(7 * 24 * 60 * 60);

/// Row 14: client retries with the same operation ID at each step: from the same principal with
/// the same request, which answers with the same fork; with a conflicting replication factor and
/// a conflicting expected source incarnation, and from another principal, each refused. Then,
/// once the fork is ready, just inside the 7-day retention a resubmission still answers with the
/// fork, and just outside it is a new operation, refused because the name exists: the clocks
/// move on in simulated time.
fn retries(seed: u64, point: Point) -> Result<()> {
    let mut run = Run::start(seed, &Setup::default())?;
    let target: DatabaseName = "dst".parse()?;
    let (operation, _) = run.fork_to(&target, point)?;
    let source = run.source.clone();
    let same = fork_request(&source, &target, operation, REPLICAS);
    let incarnation = run
        .state()?
        .database(&target)
        .map(fleetring_control::Database::incarnation)
        .ok_or("no fork")?;
    let forked_as = |want: Incarnation| move |outcome: &Outcome| matches!(outcome, Outcome::Acknowledged(Ack::Forked { incarnation }) if *incarnation == want);
    let refused_with = |code: tonic::Code| move |outcome: &Outcome| matches!(outcome, Outcome::Rejected { code: got, .. } if *got == code);
    let again = ask(&mut run, "again", "workload", same.clone())?;
    expect_answer(&again, "the same request", forked_as(incarnation))?;
    let wider = ask(
        &mut run,
        "wider",
        "workload",
        fork_request(&source, &target, operation, 2),
    )?;
    expect_answer(
        &wider,
        "another replication factor",
        refused_with(tonic::Code::AlreadyExists),
    )?;
    let expecting = Operation::Fork {
        source: source.clone(),
        target: target.clone(),
        operation,
        replication_factor: REPLICAS,
        expect_source_incarnation: Some(Incarnation::new(0x5EED)),
    };
    let expecting = ask(&mut run, "expecting", "workload", expecting)?;
    expect_answer(
        &expecting,
        "another expected source incarnation",
        refused_with(tonic::Code::AlreadyExists),
    )?;
    let other = ask(&mut run, "other", OTHER_PRINCIPAL, same.clone())?;
    expect_answer(
        &other,
        "another principal's request",
        refused_with(tonic::Code::PermissionDenied),
    )?;
    run.release(point.hook(), operation)?;
    run.await_outcome(operation, Expected::Ready)?;
    run.await_more_writes(&target, 5)?;
    // Retention counts from the outcome.
    run.fleet.shift_clocks(RETENTION - Duration::from_secs(10));
    run.wait(Duration::from_secs(3))?;
    let inside = ask(&mut run, "inside", "workload", same.clone())?;
    expect_answer(
        &inside,
        "a resubmission just inside the retention",
        forked_as(incarnation),
    )?;
    run.fleet.shift_clocks(Duration::from_secs(20));
    run.wait(Duration::from_secs(3))?;
    // Past it the ID is unknown: the request is a new operation, for a name that exists.
    let outside = ask(&mut run, "outside", "workload", same)?;
    expect_answer(
        &outside,
        "a resubmission just outside the retention",
        refused_with(tonic::Code::AlreadyExists),
    )?;
    run.end()
}

row!(row_14_retries, sweep_row_14_retries, Point::ALL, retries);

/// Whether a fork in control state ended an attempt with its pin expired, or failed.
fn pin_expired(fleet: &Fleet) -> bool {
    fleet.control_state().is_some_and(|state| {
        state.databases().any(|db| {
            db.fork().is_some_and(|fork| {
                matches!(
                    fork.state(),
                    ForkState::Seeding {
                        last_cause: Some(RetryCause::PinExpired),
                        ..
                    } | ForkState::Failed { .. }
                )
            })
        })
    })
}

/// Row 15: the pin expires from a slow transfer. The source's primary may pin it for 300 ms
/// only; its stream to the seeder starts late, held at the hook past the pin's time, over a slow
/// link (the simulated network has latency, not bandwidth, so the hold stands for a transfer that
/// takes long), and the attempt ends with the pin expired. The next attempt seeds.
fn pin_expiry(seed: u64, point: Point) -> Result<()> {
    let setup = Setup {
        max_pin_ms: Some(300),
        ..Setup::default()
    };
    let mut run = Run::start(seed, &setup)?;
    let source = run.source.clone();
    let primary = run.primary(&source)?;
    let target = run.pick("dst", REPLICAS, |placed| placed[0] != primary)?;
    let (operation, _) = run.fork_to(&target, point)?;
    let seeder = run.seeder(operation)?;
    run.fleet
        .set_latency(&seeder, &primary, Duration::from_millis(120))?;
    run.wait(Duration::from_millis(500))?;
    run.release(point.hook(), operation)?;
    run.fleet
        .run_until(LIMIT, "the pin expiring", pin_expired)?;
    run.fleet
        .set_latency(&seeder, &primary, Duration::from_millis(5))?;
    run.finish(operation, Expected::Ready)
}

row!(
    row_15_pin_expiry,
    sweep_row_15_pin_expiry,
    [Point::DuringStream],
    pin_expiry
);

/// Row 16: the fork is dropped at each step: its operation is cancelled, and its late work
/// changes nothing.
fn fork_dropped(seed: u64, point: Point) -> Result<()> {
    let mut run = Run::start(seed, &Setup::default())?;
    // The dropped fork's late seeding requests are refused by the binding.
    run.allow_binding_refusals();
    let target: DatabaseName = "dst".parse()?;
    let (operation, _) = run.fork_to(&target, point)?;
    run.traffic.remove(&target);
    run.script([Operation::Drop { database: target }])?;
    run.release(point.hook(), operation)?;
    run.wait(Duration::from_secs(2))?;
    run.finish(operation, Expected::Cancelled)
}

row!(
    row_16_fork_dropped,
    sweep_row_16_fork_dropped,
    Point::ALL,
    fork_dropped
);

/// Row 17: a partition between the seeder and the source's primary, and between the peers and
/// the seeder, at each step, repaired three seconds later.
fn partitions(seed: u64, point: Point) -> Result<()> {
    let mut run = Run::start(seed, &Setup::default())?;
    let target: DatabaseName = "dst".parse()?;
    let (operation, _) = run.fork_to(&target, point)?;
    let seeder = run.seeder(operation)?;
    let primary = run.primary(&run.source.clone())?;
    let mut cut = Vec::new();
    if seeder != primary {
        cut.push((seeder.clone(), primary));
    }
    for peer in run.placement(target.as_str(), REPLICAS)? {
        if peer != seeder {
            cut.push((peer, seeder.clone()));
        }
    }
    for (a, b) in &cut {
        run.fleet.partition(a, b)?;
    }
    run.release(point.hook(), operation)?;
    run.wait(Duration::from_secs(3))?;
    for (a, b) in &cut {
        run.fleet.repair(a, b)?;
    }
    run.finish(operation, Expected::Ready)
}

row!(
    row_17_partitions,
    sweep_row_17_partitions,
    Point::ALL,
    partitions
);

/// The probes a late peer and a late seeder would send for `operation`'s fork, each naming
/// `fork` and attempt 1: a peer's request to the seeder for its staged copy, and the seeder's
/// request to the source's primary. Each comes with the host it is sent to.
fn late_seeding_probes(
    run: &Run,
    operation: OperationId,
    fork: &ForkIdentity,
) -> Result<[(String, Operation); 2]> {
    let source = run.source.clone();
    let source_incarnation = run
        .state()?
        .database(&source)
        .map(fleetring_control::Database::incarnation)
        .ok_or("no source")?;
    let seeder = run.seeder(operation)?;
    let source_primary = run.primary(&source)?;
    let peer = run
        .placement(fork.name.as_str(), REPLICAS)?
        .into_iter()
        .find(|node| *node != seeder)
        .ok_or("no peer")?;
    let to_seeder = Operation::SeedingProbe {
        to: seeder.parse()?,
        as_node: peer.parse()?,
        request: SeedingProbe {
            fork: fork.clone(),
            attempt: 1,
            database: fork.name.clone(),
            source_incarnation,
            staged: true,
        },
    };
    let to_source = Operation::SeedingProbe {
        to: source_primary.parse()?,
        as_node: seeder.parse()?,
        request: SeedingProbe {
            fork: fork.clone(),
            attempt: 1,
            database: source,
            source_incarnation,
            staged: false,
        },
    };
    Ok([(seeder, to_seeder), (source_primary, to_source)])
}

/// How many binding refusals `host` has counted.
fn refusals(run: &Run, host: &str) -> Result<u64> {
    Ok(run.fleet.binding_refusals(host).ok_or("a host is down")?)
}

/// A seeding request with the fork's own identity that arrives after the fork completed, as a
/// late peer's or seeder's may, is answered `UNAVAILABLE` with no binding named, and neither the
/// seeder nor the source's primary counts it as a binding refusal (ADR-0046, point 4.4, as
/// amended). The run's own check that no legitimate call was refused for its binding holds too.
fn late_request_answered(seed: u64, point: Point) -> Result<()> {
    let mut run = Run::start(seed, &Setup::default())?;
    let target: DatabaseName = "dst".parse()?;
    let (operation, _) = run.fork_to(&target, point)?;
    let fork = run.identity(&target, operation)?;
    let probes = late_seeding_probes(&run, operation, &fork)?;
    for (index, (to, probe)) in probes.into_iter().enumerate() {
        let before = refusals(&run, &to)?;
        let answer = ask(&mut run, &format!("late-{index}"), "workload", probe)?;
        let retryable = matches!(
            &answer,
            Outcome::Acknowledged(Ack::Refused {
                code: tonic::Code::Unavailable,
                binding: None,
            })
        );
        if !retryable {
            return Err(format!("a late request to {to} was answered {answer}").into());
        }
        if refusals(&run, &to)? != before {
            return Err(format!("{to} counted a late request as a binding refusal").into());
        }
    }
    run.release(point.hook(), operation)?;
    run.finish(operation, Expected::Ready)
}

#[test]
fn a_late_seeding_request_is_answered_not_refused() {
    super::matrix::fixed(&[Point::BeforeFirstLease], late_request_answered);
}

/// The same requests, naming another operation under the fork's name and incarnation, are still
/// refused for their binding and counted (ADR-0046, point 4.4).
fn mismatched_request_refused(seed: u64, point: Point) -> Result<()> {
    let mut run = Run::start(seed, &Setup::default())?;
    // The scenario's own probes are refused, and counted below.
    run.allow_binding_refusals();
    let target: DatabaseName = "dst".parse()?;
    let (operation, _) = run.fork_to(&target, point)?;
    let fork = run.identity(&target, operation)?;
    let other = ForkIdentity::new(fork.name.clone(), fork.incarnation, run.operation_id());
    let probes = late_seeding_probes(&run, operation, &other)?;
    for (index, (to, probe)) in probes.into_iter().enumerate() {
        let before = refusals(&run, &to)?;
        let answer = ask(&mut run, &format!("mismatched-{index}"), "workload", probe)?;
        let refused = matches!(
            &answer,
            Outcome::Acknowledged(Ack::Refused {
                code: tonic::Code::PermissionDenied,
                binding: Some(binding),
            }) if binding == "seeding"
        );
        if !refused {
            return Err(format!("a mismatched request to {to} was answered {answer}").into());
        }
        if refusals(&run, &to)? != before + 1 {
            return Err(format!("{to} didn't count a mismatched request as a refusal").into());
        }
    }
    run.release(point.hook(), operation)?;
    run.finish(operation, Expected::Ready)
}

#[test]
fn a_mismatched_seeding_request_is_still_refused_and_counted() {
    super::matrix::fixed(&[Point::BeforeFirstLease], mismatched_request_refused);
}

/// Row 18: the fork is dropped and created again under its name, a new fork with the same
/// source, placement, seeder, and attempt number, while the old fork's work is held at each
/// step. Then the old identity's proposals arrive late (`CompleteFork`, `RetryForkSeed`,
/// `MarkForkReady`, `RecordForkDiskRefusal`, `DropForkHolder`, and `FailFork`), every replica
/// reports the old identity's staged copy, failure, and reset, the old seeder asks the source's
/// primary for its copy and a replica asks the new seeder for the old staged copy, and the old
/// work goes on, its staging directories left on disk. None of it may change the new fork
/// (property d).
fn replaced(seed: u64, point: Point) -> Result<()> {
    let mut run = Run::start(seed, &Setup::default())?;
    // The old identity's seeding requests, and its old work, are refused by the binding.
    run.allow_binding_refusals();
    let target: DatabaseName = "dst".parse()?;
    let (old_operation, _) = run.fork_to(&target, point)?;
    let old = run.identity(&target, old_operation)?;
    let source = run.source.clone();
    let source_incarnation = run
        .state()?
        .database(&source)
        .map(fleetring_control::Database::incarnation)
        .ok_or("no source")?;
    run.traffic.remove(&target);
    run.script([Operation::Drop {
        database: target.clone(),
    }])?;
    // The new fork waits before its seeder's first request, so it stays at attempt 1.
    let new_operation = run.operation_id();
    run.hold(FaultPoint::Fetch, new_operation)?;
    run.fork_as(&target, new_operation)?;
    let seeder = run.seeder(new_operation)?;
    let placed = run.placement(target.as_str(), REPLICAS)?;
    let peer = placed
        .iter()
        .find(|node| **node != seeder)
        .cloned()
        .ok_or("no peer")?;
    let at = Position::new([0, 0, 0, 1]);
    let hash = ChainHash::new([7; 32]);
    let late = [
        Action::CompleteFork {
            fork: old.clone(),
            attempt: 1,
            at: at.clone(),
            chain_hash: hash.clone(),
            holders: Run::ids(&placed)?.into_iter().collect(),
        },
        Action::RetryForkSeed {
            fork: old.clone(),
            attempt: 1,
            cause: RetryCause::AttemptTimedOut,
            exclude_seeder: None,
        },
        Action::MarkForkReady {
            fork: old.clone(),
            epoch: LeaseEpoch::new(1),
            reset_position: at.clone(),
        },
        Action::RecordForkDiskRefusal {
            fork: old.clone(),
            attempt: 1,
            node: peer.parse()?,
        },
        Action::DropForkHolder {
            fork: old.clone(),
            attempt: 1,
            holder: peer.parse()?,
            cause: RetryCause::HolderLost,
        },
        Action::FailFork {
            fork: old.clone(),
            attempt: 1,
            reason: ForkFailure::SourceGone,
        },
    ];
    for action in late {
        run.operator.push(Operation::Propose {
            action: Box::new(action),
        });
    }
    for node in &placed {
        let replica = HostedReplica::new(target.clone(), ReplicaRole::Follower, Position::new([]))
            .with_staged(StagedCopy::new(
                old.clone(),
                1,
                at.clone(),
                hash.clone(),
                source_incarnation,
            ))
            .with_seed_failure(SeedFailure::new(
                old.clone(),
                1,
                SeedFailureReason::InsufficientDisk,
            ))
            .with_reset(ResetReport::committed(
                old.clone(),
                LeaseEpoch::new(1),
                at.clone(),
            ));
        run.operator.push(Operation::Report {
            node: node.parse()?,
            replicas: vec![replica],
            disk_free_bytes: Some(1 << 40),
        });
    }
    let source_primary: NodeId = run.primary(&source)?.parse()?;
    run.script([
        Operation::SeedingProbe {
            to: source_primary,
            as_node: seeder.parse()?,
            request: SeedingProbe {
                fork: old.clone(),
                attempt: 1,
                database: source.clone(),
                source_incarnation,
                staged: false,
            },
        },
        Operation::SeedingProbe {
            to: seeder.parse()?,
            as_node: peer.parse()?,
            request: SeedingProbe {
                fork: old,
                attempt: 1,
                database: target.clone(),
                source_incarnation,
                staged: true,
            },
        },
    ])?;
    run.wait(Duration::from_millis(500))?;
    // The old work goes on, and must find itself gone.
    run.release(point.hook(), old_operation)?;
    run.wait(Duration::from_secs(1))?;
    run.release(FaultPoint::Fetch, new_operation)?;
    run.fleet
        .fork_expectations()
        .expect(old_operation, Expected::Cancelled);
    run.finish(new_operation, Expected::Ready)
}

row!(row_18_replaced, sweep_row_18_replaced, Point::ALL, replaced);

/// The step of the fork-reset write row 19 crashes the first primary at, for each point that
/// stands for it: before the write, during it, after its local commit, and after its quorum
/// commit.
fn reset_step(point: Point) -> FaultPoint {
    match point {
        Point::BeforePin => FaultPoint::BeforeReset,
        Point::DuringStream => FaultPoint::DuringReset,
        Point::AfterSeederStaged => FaultPoint::AfterLocalCommit,
        _ => FaultPoint::AfterQuorumCommit,
    }
}

/// Row 19: the fork's first primary crashes before, during, and after the fork-reset write:
/// after its local commit, and after its quorum commit. The surviving history holds the reset
/// exactly once, before any client write (property h); clients try the fork meanwhile.
fn reset_crash(seed: u64, point: Point) -> Result<()> {
    let hook = reset_step(point);
    if hook == FaultPoint::AfterLocalCommit {
        return reset_crash_unreplicated(seed);
    }
    let mut run = Run::start(seed, &Setup::default())?;
    let target = "dst".parse()?;
    let operation = run.operation_id();
    run.hold(hook, operation)?;
    run.fork_as(&target, operation)?;
    let primary = run.await_arrival(hook, &BTreeMap::new())?;
    run.wait(Duration::from_millis(500))?;
    run.crash(&primary)?;
    run.release(hook, operation)?;
    run.wait(Duration::from_secs(4))?;
    run.restart(&primary)?;
    run.finish(operation, Expected::Ready)
}

/// Holds `operation`'s work before its reset, asks for the fork, and cuts its first primary off
/// from the other holders there. Returns the primary and the other holders.
fn cut_off_before_reset(
    run: &mut Run,
    target: &DatabaseName,
    operation: OperationId,
) -> Result<(String, Vec<String>)> {
    run.hold(FaultPoint::BeforeReset, operation)?;
    run.fork_as(target, operation)?;
    let primary = run.await_arrival(FaultPoint::BeforeReset, &BTreeMap::new())?;
    let others: Vec<String> = run
        .holders(target)?
        .into_iter()
        .filter(|holder| *holder != primary)
        .collect();
    for other in &others {
        run.fleet.partition(&primary, other)?;
    }
    Ok((primary, others))
}

/// Row 19 after the local commit, before W_boot: the first primary is cut off from the other
/// holders before it writes the reset, so its local commit reaches none of them, and crashes
/// once it committed; then the partition heals. The holder promoted in its place finds no
/// marker and runs the reset itself, as its hooks show; the crashed primary's commit isn't in
/// the surviving history, and every copy ends holding exactly one marker (property h).
fn reset_crash_unreplicated(seed: u64) -> Result<()> {
    let mut run = Run::start(seed, &Setup::default())?;
    let target: DatabaseName = "dst".parse()?;
    let operation = run.operation_id();
    run.hold(FaultPoint::AfterLocalCommit, operation)?;
    let (primary, others) = cut_off_before_reset(&mut run, &target, operation)?;
    let committed = BTreeMap::from([(
        primary.clone(),
        run.faults(&primary)?.arrivals(FaultPoint::AfterLocalCommit),
    )]);
    run.release(FaultPoint::BeforeReset, operation)?;
    let held = run.await_arrival(FaultPoint::AfterLocalCommit, &committed)?;
    if held != primary {
        return Err(format!("{held}, not the first primary {primary}, committed the reset").into());
    }
    run.crash(&primary)?;
    run.release(FaultPoint::AfterLocalCommit, operation)?;
    for other in &others {
        run.fleet.repair(&primary, other)?;
    }
    run.wait(Duration::from_secs(4))?;
    run.restart(&primary)?;
    run.await_outcome(operation, Expected::Ready)?;
    let again: Vec<String> = run
        .fleet
        .history()
        .fork_passes()
        .iter()
        .filter(|pass| {
            pass.point == FaultPoint::AfterLocalCommit
                && pass.operation == Some(operation)
                && pass.host != primary
        })
        .map(|pass| pass.host.clone())
        .collect();
    if again.is_empty() {
        return Err(format!(
            "no holder but the crashed first primary {primary} committed the reset: the promoted one didn't run it again"
        )
        .into());
    }
    run.finish(operation, Expected::Ready)
}

/// Row 19's four steps, named by the points that stand for them ([`reset_step`]).
const RESET_STEPS: [Point; 4] = [
    Point::BeforePin,
    Point::DuringStream,
    Point::AfterSeederStaged,
    Point::DuringPeerCopies,
];

row!(
    row_19_reset_crash,
    sweep_row_19_reset_crash,
    RESET_STEPS,
    reset_crash
);

/// Row 19 with the write quorum unreachable (ADR-0046 point 12, "the write quorum is
/// unreachable"): the first primary is cut off from the other holders before its reset, which
/// commits on it alone and can't reach its bootstrap quorum. For three seconds clients keep
/// trying the fork and must get nothing (property a), and the fork mustn't become ready; then
/// the partition heals, and it does.
fn reset_quorum_unreachable(seed: u64, point: Point) -> Result<()> {
    let _ = point;
    let mut run = Run::start(seed, &Setup::default())?;
    let target: DatabaseName = "dst".parse()?;
    let operation = run.operation_id();
    let (primary, others) = cut_off_before_reset(&mut run, &target, operation)?;
    run.release(FaultPoint::BeforeReset, operation)?;
    run.wait(Duration::from_secs(3))?;
    for other in &others {
        run.fleet.repair(&primary, other)?;
    }
    run.finish(operation, Expected::Ready)
}

row!(
    row_19_quorum_unreachable,
    sweep_row_19_quorum_unreachable,
    [Point::DuringReset],
    reset_quorum_unreachable
);

/// Waits until `target`'s lease is held by another node than `old`.
fn await_new_primary(run: &mut Run, target: &DatabaseName, old: &str) -> Result<()> {
    run.fleet
        .run_until(LIMIT, "the fork's lease moving", |fleet| {
            fleet.control_state().is_some_and(|state| {
                state
                    .database(target)
                    .and_then(|db| db.lease())
                    .is_some_and(|lease| lease.holder().as_str() != old)
            })
        })?;
    Ok(())
}

/// Waits until every other replica of `target` follows its primary `primary`, connected and
/// reported.
fn await_followers(run: &mut Run, target: &DatabaseName, primary: &str) -> Result<()> {
    let others = usize::try_from(REPLICAS)? - 1;
    run.fleet
        .run_until(LIMIT, "the fork's holders following its primary", |fleet| {
            fleet
                .replication(primary)
                .and_then(|replication| replication.primary_status(target))
                .is_some_and(|status| {
                    status
                        .followers
                        .iter()
                        .filter(|follower| follower.connected && follower.reported)
                        .count()
                        == others
                })
        })?;
    Ok(())
}

/// Row 20: a failover between completion and the reset: the first lease expires, its primary
/// cut off from the control node, or its primary is drained, before the reset commits; and
/// again just after, once clients have written to the fork. The reset runs exactly once
/// (property h). The four cases are named by the points that stand for them: an expiry and a
/// drain before the reset, then an expiry and a drain after it.
fn failover(seed: u64, point: Point) -> Result<()> {
    let expire = matches!(point, Point::BeforePin | Point::AfterSeederStaged);
    let after = matches!(point, Point::AfterSeederStaged | Point::DuringPeerCopies);
    let mut run = Run::start(seed, &Setup::default())?;
    let target: DatabaseName = "dst".parse()?;
    let operation = run.operation_id();
    if !after {
        run.hold(FaultPoint::BeforeReset, operation)?;
    }
    run.fork_as(&target, operation)?;
    let primary = if after {
        run.await_more_writes(&target, 5)?;
        run.primary(&target)?
    } else {
        run.await_arrival(FaultPoint::BeforeReset, &BTreeMap::new())?
    };
    if expire {
        run.fleet.partition(&primary, CONTROL)?;
    } else {
        if !after {
            // A drain hands a lease to a connected follower that has reported, and leaves one
            // it can't hand over to fail over once the node stops: the drain comes once the
            // fork's other holders follow its first primary, as they do before its reset.
            await_followers(&mut run, &target, &primary)?;
        }
        run.operator.push(Operation::Drain {
            node: primary.parse()?,
        });
    }
    if !after {
        run.wait(Duration::from_millis(300))?;
        run.release(FaultPoint::BeforeReset, operation)?;
    }
    await_new_primary(&mut run, &target, &primary)?;
    if expire {
        run.fleet.repair(&primary, CONTROL)?;
    }
    run.finish(operation, Expected::Ready)
}

/// Row 20's cases ([`failover`]) but the drain before the reset.
const FAILOVERS: [Point; 3] = [
    Point::BeforePin,
    Point::AfterSeederStaged,
    Point::DuringPeerCopies,
];

row!(row_20_failover, sweep_row_20_failover, FAILOVERS, failover);

row!(
    row_20_drain_before_reset,
    sweep_row_20_drain_before_reset,
    [Point::DuringStream],
    failover
);

/// Row 21: one replica refuses on disk at each seeding step before it copies: the other two
/// seed the fork.
fn disk_refusal(seed: u64, point: Point) -> Result<()> {
    let mut run = Run::start(seed, &Setup::default())?;
    let target: DatabaseName = "dst".parse()?;
    let (operation, _) = run.fork_to(&target, point)?;
    let seeder = run.seeder(operation)?;
    let peer = run
        .placement(target.as_str(), REPLICAS)?
        .into_iter()
        .find(|node| *node != seeder)
        .ok_or("no peer")?;
    run.faults(&peer)?.set_disk(4096, 1 << 30);
    run.release(point.hook(), operation)?;
    run.finish(operation, Expected::Ready)
}

row!(
    row_21_disk_refusal,
    sweep_row_21_disk_refusal,
    [
        Point::BeforePin,
        Point::DuringStream,
        Point::AfterSeederStaged
    ],
    disk_refusal
);

/// The size of `database`'s copy on `host`, as `host` last reported it to the control plane.
fn reported_size(run: &mut Run, host: &str, database: &DatabaseName) -> Result<u64> {
    let id: NodeId = host.parse()?;
    let mut size = None;
    run.fleet.run_until(
        LIMIT,
        &format!("{host} reporting {database}'s size"),
        |fleet| {
            size = fleet.control_state().and_then(|state| {
                state
                    .report(&id, database)
                    .and_then(fleetring_control::Report::size_bytes)
            });
            size.is_some_and(|size| size > 0)
        },
    )?;
    Ok(size.ok_or("no size reported")?)
}

/// Whether some replica reports a staged copy of `target`.
fn staged(fleet: &Fleet, target: &DatabaseName) -> bool {
    fleet.control_state().is_some_and(|state| {
        state.members().any(|member| {
            state
                .report(member.id(), target)
                .is_some_and(|report| report.staged().is_some())
        })
    })
}

/// Row 21: two concurrent forks of two replicas each compete for one node's free space, room
/// for one copy and a half of the source's size as its primary reports it to the control
/// plane: the second asks while the first is admitted and its bytes haven't landed. The node
/// admits the first, which becomes ready, and refuses the second, which fails for want of disk,
/// since its completion needs both its replicas (disk admission, ADR-0046 point 7.1).
fn disk_competition(seed: u64, point: Point) -> Result<()> {
    let _ = point;
    let setup = Setup {
        pad: Some(256 * 1024),
        ..Setup::default()
    };
    let mut run = Run::start(seed, &setup)?;
    let source = run.source.clone();
    let primary = run.primary(&source)?;
    // A node both forks place second, so that it copies from each seeder.
    let mut chosen = None;
    for node in NODES {
        let second = |placed: &[String]| placed.get(1).map(String::as_str) == Some(node);
        let (Ok(a), Ok(b)) = (run.pick("dsta", 2, second), run.pick("dstb", 2, second)) else {
            continue;
        };
        chosen = Some((node.to_owned(), a, b));
        break;
    }
    let (node, a, b) = chosen.ok_or("no node both forks place second")?;
    let (op_a, op_b) = (run.operation_id(), run.operation_id());
    let hooks = run.faults(&node)?;
    hooks.hold_operation(FaultPoint::Fetch, op_a);
    hooks.hold_operation(FaultPoint::Fetch, op_b);
    run.fork_of(&source, &a, op_a, 2)?;
    run.fork_of(&source, &b, op_b, 2)?;
    // Both seeders stage; the shared node waits to copy from both.
    run.fleet
        .run_until(LIMIT, "both seeders staging", |fleet| {
            staged(fleet, &a) && staged(fleet, &b)
        })?;
    let estimate = reported_size(&mut run, &primary, &source)?;
    hooks.set_disk(estimate * 16 / 10, 1 << 30);
    run.fleet.fork_expectations().limit_copies(node.parse()?, 1);
    // The first copy is admitted and its bytes haven't landed when the second asks.
    hooks.hold_operation(FaultPoint::Admitted, op_a);
    let before = hooks.arrivals(FaultPoint::Admitted);
    hooks.release_operation(FaultPoint::Fetch, op_a);
    run.fleet.run_until(LIMIT, "the first copy admitted", |_| {
        hooks.arrivals(FaultPoint::Admitted) > before
    })?;
    hooks.release_operation(FaultPoint::Fetch, op_b);
    run.wait(Duration::from_secs(2))?;
    hooks.release_operation(FaultPoint::Admitted, op_a);
    run.fleet.fork_expectations().expect(op_a, Expected::Ready);
    run.await_outcome(op_a, Expected::Ready)?;
    run.finish(op_b, Expected::FailedWith(ForkFailure::InsufficientDisk))
}

row!(
    row_21_disk_competition,
    sweep_row_21_disk_competition,
    [Point::DuringPeerCopies],
    disk_competition
);

/// Row 22: just after the fork is ready, clients write to it, a sink delivers its changes, and a
/// watch follows them; then its primary fails over, crashed or drained (named by the points that
/// stand for them), and clients go on. The change history recorded after readiness survives: no
/// reset runs again (property h); no watch sees a change the fork inherited, and each watch, a
/// live tail, the one after the failover among them, sees every write made while it watched
/// (property h); and the fork's sinks, across the failover, deliver every write acknowledged to
/// it and nothing of its source.
fn after_readiness(seed: u64, point: Point) -> Result<()> {
    let crash = point == Point::BeforePin;
    let setup = Setup {
        sinks: true,
        ..Setup::default()
    };
    let mut run = Run::start(seed, &setup)?;
    let target: DatabaseName = "dst".parse()?;
    let operation = run.operation_id();
    run.fork_as(&target, operation)?;
    run.await_outcome(operation, Expected::Ready)?;
    run.await_more_writes(&target, 5)?;
    run.script([Operation::Watch {
        database: target.clone(),
    }])?;
    let primary = run.primary(&target)?;
    if crash {
        run.crash(&primary)?;
    } else {
        run.operator.push(Operation::Drain {
            node: primary.parse()?,
        });
    }
    await_new_primary(&mut run, &target, &primary)?;
    run.await_more_writes(&target, 5)?;
    run.script([Operation::Watch {
        database: target.clone(),
    }])?;
    if crash {
        run.restart(&primary)?;
    }
    sinks_deliver_the_fork(&mut run, &target)?;
    run.finish(operation, Expected::Ready)
}

/// Waits until `target`'s sinks, on every data host together, delivered every write to it
/// acknowledged so far, and checks that they delivered no value written to its source.
fn sinks_deliver_the_fork(run: &mut Run, target: &DatabaseName) -> Result<()> {
    let written_to = |database: &DatabaseName, acknowledged: bool| -> BTreeSet<i64> {
        run.fleet
            .history()
            .records()
            .iter()
            .filter_map(|record| match (record.operation(), record.outcome()) {
                (
                    Operation::Write {
                        database: to,
                        value,
                    },
                    outcome,
                ) if to == database
                    && (!acknowledged
                        || matches!(outcome, Outcome::Acknowledged(Ack::Written { .. }))) =>
                {
                    Some(*value)
                }
                _ => None,
            })
            .collect()
    };
    let acknowledged = written_to(target, true);
    let source = written_to(&run.source, false);
    if acknowledged.is_empty() {
        return Err(format!("no write to {target} was acknowledged").into());
    }
    let delivered = |fleet: &Fleet| -> BTreeSet<i64> {
        fleet
            .sink_values(target)
            .unwrap_or_default()
            .into_values()
            .flatten()
            .collect()
    };
    let wait = run
        .fleet
        .run_until(LIMIT, "the fork's sinks delivering its writes", |fleet| {
            delivered(fleet).is_superset(&acknowledged)
        });
    let delivered = delivered(&run.fleet);
    if let Some(value) = delivered.iter().find(|value| source.contains(value)) {
        return Err(format!("a sink of {target} delivered {value}, a write to its source").into());
    }
    if wait.is_err() {
        let missing: Vec<&i64> = acknowledged.difference(&delivered).collect();
        return Err(format!(
            "{target}'s sinks never delivered the acknowledged writes {missing:?}"
        )
        .into());
    }
    Ok(())
}

row!(
    row_22_after_readiness,
    sweep_row_22_after_readiness,
    [Point::BeforePin, Point::DuringStream],
    after_readiness
);

/// Row 16, after readiness, found by FR-087 (#165): a ready fork is dropped, and the name forked
/// again under a new operation: the new fork must become ready as the first did. Its first lease
/// has the epoch the dropped fork's had, often on the same first holder, which must still run the
/// new fork's reset before it serves: its gate opened for the dropped fork doesn't count.
fn refork_after_drop(seed: u64, point: Point) -> Result<()> {
    let mut run = Run::start(seed, &Setup::default())?;
    // The dropped fork's late calls are refused by the binding, as in row 16.
    run.allow_binding_refusals();
    let target: DatabaseName = "dst".parse()?;
    let (first, _) = run.fork_to(&target, point)?;
    run.release(point.hook(), first)?;
    run.await_outcome(first, Expected::Ready)?;
    run.await_more_writes(&target, 5)?;
    run.traffic.remove(&target);
    run.script([Operation::Drop {
        database: target.clone(),
    }])?;
    run.wait(Duration::from_secs(5))?;
    let second = run.operation_id();
    run.fork_as(&target, second)?;
    run.fleet.fork_expectations().expect(first, Expected::Ready);
    run.finish(second, Expected::Ready)
}

row!(
    row_16_refork_after_drop,
    sweep_row_16_refork_after_drop,
    [Point::BeforePin],
    refork_after_drop
);

/// Longer than a fork's control call may wait, five seconds, so a call cut off for it fails.
const PAST_A_CONTROL_CALL: Duration = Duration::from_secs(6);

/// Row 16 after an unconfirmed first lease, found by the FR-106 review: the fork's first holder
/// takes the first lease and moves its staged copy into place, and is cut off from the control
/// node before control state confirms the lease, so it serves nothing; its lease expires, and a
/// promotion may open the fork meanwhile, at an epoch the holder never sees. Then the fork is
/// dropped.
/// The copy in place must still be known to the holder, so the drop deletes it: a database of
/// the name made afterwards never starts from the dropped fork's data. Returns the dropped
/// fork's operation.
fn drop_after_unconfirmed_first_lease(run: &mut Run, target: &DatabaseName) -> Result<OperationId> {
    run.allow_binding_refusals();
    let operation = run.operation_id();
    run.hold(FaultPoint::ConfirmFirstLease, operation)?;
    run.fork_as(target, operation)?;
    let holder = run.await_arrival(FaultPoint::ConfirmFirstLease, &BTreeMap::new())?;
    run.fleet.partition(&holder, CONTROL)?;
    run.release(FaultPoint::ConfirmFirstLease, operation)?;
    run.wait(PAST_A_CONTROL_CALL)?;
    run.fleet.repair(&holder, CONTROL)?;
    run.traffic.remove(target);
    run.script([Operation::Drop {
        database: target.clone(),
    }])?;
    run.wait(Duration::from_secs(5))?;
    Ok(operation)
}

/// Fails if any data host's copy of `target` holds a value written to `source`.
fn no_copy_holds_writes_to(run: &Run, target: &DatabaseName, source: &DatabaseName) -> Result<()> {
    let written: BTreeSet<i64> = run
        .fleet
        .history()
        .records()
        .iter()
        .filter_map(|record| match record.operation() {
            Operation::Write { database, value } if database == source => Some(*value),
            _ => None,
        })
        .collect();
    for (host, rows) in run.fleet.replica_rows(target)? {
        if let Some(value) = rows.iter().find(|value| written.contains(value)) {
            return Err(format!(
                "{host}'s copy of {target} holds {value}, a write to {source}: the dropped fork's copy was opened as the new database"
            )
            .into());
        }
    }
    Ok(())
}

/// Row 16 after an unconfirmed first lease, then an ordinary database created under the name:
/// it starts empty on every replica.
fn create_after_unconfirmed_first_lease(seed: u64, _point: Point) -> Result<()> {
    let mut run = Run::start(seed, &Setup::default())?;
    let target: DatabaseName = "dst".parse()?;
    let dropped = drop_after_unconfirmed_first_lease(&mut run, &target)?;
    run.script([Operation::Create {
        database: target.clone(),
        replication_factor: REPLICAS,
        durability: DurabilityMode::Quorum,
    }])?;
    run.script([Operation::Write {
        database: target.clone(),
        value: 1_000_000,
    }])?;
    run.wait(Duration::from_secs(2))?;
    let source = run.source.clone();
    no_copy_holds_writes_to(&run, &target, &source)?;
    run.finish(dropped, Expected::Terminal)
}

row!(
    row_16_create_after_unconfirmed_first_lease,
    sweep_row_16_create_after_unconfirmed_first_lease,
    [Point::BeforeFirstLease],
    create_after_unconfirmed_first_lease
);

/// Row 16 after an unconfirmed first lease, then the name forked again: the new fork becomes
/// ready, its first holder's copy its own.
fn refork_after_unconfirmed_first_lease(seed: u64, _point: Point) -> Result<()> {
    let mut run = Run::start(seed, &Setup::default())?;
    let target: DatabaseName = "dst".parse()?;
    let dropped = drop_after_unconfirmed_first_lease(&mut run, &target)?;
    let second = run.operation_id();
    run.fork_as(&target, second)?;
    run.fleet
        .fork_expectations()
        .expect(dropped, Expected::Terminal);
    run.finish(second, Expected::Ready)
}

row!(
    row_16_refork_after_unconfirmed_first_lease,
    sweep_row_16_refork_after_unconfirmed_first_lease,
    [Point::BeforeFirstLease],
    refork_after_unconfirmed_first_lease
);

/// Row 16 before the reset, then an ordinary database created under the name, found by the
/// FR-106 review: the fork's first primary has closed its readiness gate and is about to run the
/// reset when the fork is dropped. The gate goes with the copy, so the new database, which no
/// fork made, serves its clients.
fn create_after_drop_before_reset(seed: u64, _point: Point) -> Result<()> {
    let mut run = Run::start(seed, &Setup::default())?;
    run.allow_binding_refusals();
    let target: DatabaseName = "dst".parse()?;
    let operation = run.operation_id();
    run.hold(FaultPoint::BeforeReset, operation)?;
    run.fork_as(&target, operation)?;
    run.await_arrival(FaultPoint::BeforeReset, &BTreeMap::new())?;
    run.traffic.remove(&target);
    run.script([Operation::Drop {
        database: target.clone(),
    }])?;
    run.release(FaultPoint::BeforeReset, operation)?;
    run.wait(Duration::from_secs(5))?;
    run.script([Operation::Create {
        database: target.clone(),
        replication_factor: REPLICAS,
        durability: DurabilityMode::Quorum,
    }])?;
    // A client retries a refused write a few times, as one would while a primary settles.
    let mut answer = None;
    for attempt in 0..5_i64 {
        let outcome = ask(
            &mut run,
            &format!("writer-{attempt}"),
            "workload",
            Operation::Write {
                database: target.clone(),
                value: 2_000_000 + attempt,
            },
        )?;
        if matches!(outcome, Outcome::Acknowledged(Ack::Written { .. })) {
            answer = None;
            break;
        }
        answer = Some(outcome);
        run.wait(Duration::from_secs(1))?;
    }
    if let Some(outcome) = answer {
        return Err(
            format!("the database created under {target} never took a write: {outcome}").into(),
        );
    }
    run.finish(operation, Expected::Terminal)
}

row!(
    row_16_create_after_drop_before_reset,
    sweep_row_16_create_after_drop_before_reset,
    [Point::DuringReset],
    create_after_drop_before_reset
);

/// Row 23, ADR-0046 point 12's "placement changes between `Seeded` and `Ready`", at the two
/// seeded steps: before the first lease, and during the reset. A data host outside the source's
/// replica set leaves the fleet, and the fork is placed without it; it comes back while the fork
/// is seeded, so that placement then names it, in place of a holder. The replica set must stay
/// the holders until the fork is ready (property i); then the primary hands it over, once
/// placement's write quorum holds its log (`PlacementCaughtUp`, `ReleaseForkHolders`), and every
/// write acknowledged before is on a write quorum of placement (property i).
fn placement_moves_while_seeded(seed: u64, point: Point) -> Result<()> {
    let (mut run, operation, target, returning) = ready_with_holders_kept(seed, point)?;
    let returning_id: NodeId = returning.parse()?;
    run.fleet
        .run_until(LIMIT, "the fork's holders released to placement", |fleet| {
            fleet.control_state().is_some_and(|state| {
                state
                    .database(&target)
                    .and_then(|db| db.fork())
                    .is_some_and(|fork| {
                        matches!(fork.state(), ForkState::Ready { holders: None, .. })
                    })
            })
        })?;
    let released = run.fleet.control_state().is_some_and(|state| {
        state
            .replica_set(&target)
            .is_ok_and(|replicas| replicas.contains(&returning_id))
    });
    if !released {
        return Err(format!("{target} was released to a placement without {returning}").into());
    }
    run.finish(operation, Expected::Ready)
}

/// Row 23's setup: a data host leaves, the fork is placed without it, and it comes back while
/// the fork is seeded, so that placement names it in place of a holder. Returns the run once
/// the fork is ready with its holders kept, its operation, its name, and the returning host.
fn ready_with_holders_kept(
    seed: u64,
    point: Point,
) -> Result<(Run, OperationId, DatabaseName, String)> {
    let mut run = Run::start(seed, &Setup::default())?;
    let source = run.source.clone();
    let source_set: Vec<String> = run
        .state()?
        .replica_set(&source)?
        .iter()
        .map(ToString::to_string)
        .collect();
    let mut chosen = None;
    for node in NODES
        .iter()
        .filter(|node| !source_set.iter().any(|held| held == *node))
    {
        let without: Vec<&str> = NODES
            .iter()
            .copied()
            .filter(|other| other != node)
            .collect();
        for n in 0..500 {
            let name = format!("dst-{n}");
            let all = run.placement_among(&name, REPLICAS, &NODES)?;
            if all.iter().any(|placed| placed == node)
                && run.placement_among(&name, REPLICAS, &without)? != all
            {
                chosen = Some(((*node).to_owned(), name));
                break;
            }
        }
        if chosen.is_some() {
            break;
        }
    }
    let (returning, name) = chosen.ok_or("no node and name for a placement that moves")?;
    let target: DatabaseName = name.parse()?;
    let returning_id: NodeId = returning.parse()?;
    run.crash(&returning)?;
    run.operator.push(Operation::Leave {
        node: returning_id.clone(),
    });
    run.fleet.run_until(LIMIT, "the node leaving", |fleet| {
        fleet
            .control_state()
            .is_some_and(|state| state.member(&returning_id).is_none())
    })?;
    let (operation, _) = run.fork_to(&target, point)?;
    let holders = run.holders(&target)?;
    if holders.contains(&returning) {
        return Err(format!("{returning} holds the fork though it had left").into());
    }
    run.restart(&returning)?;
    run.fleet
        .run_until(LIMIT, "placement naming the returning node", |fleet| {
            fleet.control_state().is_some_and(|state| {
                state.member(&returning_id).is_some()
                    && state.ring().is_ok_and(|ring| {
                        ring.replicas_n(
                            target.as_str(),
                            fleetring::ReplicationFactor::new(3).expect("nonzero"),
                        )
                        .to_ids()
                        .contains(&returning_id)
                    })
            })
        })?;
    // Seeded across the change: control state's replica set is checked against the holders
    // after every command (property i).
    run.wait(Duration::from_secs(1))?;
    run.release(point.hook(), operation)?;
    run.await_outcome(operation, Expected::Ready)?;
    Ok((run, operation, target, returning))
}

row!(
    row_23_placement_moves_while_seeded,
    sweep_row_23_placement_moves_while_seeded,
    [Point::BeforeFirstLease, Point::DuringReset],
    placement_moves_while_seeded
);

/// Row 24, from FR-102's review (finding 2): a membership change during and just after a fork's
/// hand-over to placement. Row 23's fork is ready with its holders kept; its primary reports
/// that placement's write quorum holds its log, and is then held before it follows the view in
/// which control state released the holders (`FollowReady`), as one that hasn't polled again
/// yet. A member of the placement the holders were released to leaves, which moves placement
/// again; control state keeps the released placement as the replica set promotions fence
/// (ADR-0031, amendment 2). The primary, cut off from the nodes that would make that set's
/// write quorum, is given a write, and crashes. Every write it acknowledged must be on a write
/// quorum of the kept set, which the promotion fences (`AcknowledgedWritesFenced`). Before the
/// fix the primary counted the holders and its own, newer ring, and acknowledged a write the
/// promotion couldn't find.
fn membership_change_during_handover(seed: u64, point: Point) -> Result<()> {
    let (mut run, operation, target, _) = ready_with_holders_kept(seed, point)?;
    let primary = run.primary(&target)?;
    let primary_id: NodeId = primary.parse()?;
    let holders = run.holders(&target)?;
    let reported = |fleet: &Fleet| {
        fleet.control_state().is_some_and(|state| {
            state
                .report(&primary_id, &target)
                .and_then(|report| report.placement_caught_up())
                .is_some()
                || state
                    .database(&target)
                    .and_then(|db| db.fork())
                    .is_some_and(|fork| {
                        matches!(fork.state(), ForkState::Ready { holders: None, .. })
                    })
        })
    };
    run.fleet
        .run_until(LIMIT, "the primary reporting placement caught up", reported)?;
    // From here on the primary doesn't follow the fork's view, so it may not see the release.
    run.faults(&primary)?
        .hold_operation(FaultPoint::FollowReady, operation);
    run.fleet
        .run_until(LIMIT, "the fork's holders released to placement", |fleet| {
            fleet.control_state().is_some_and(|state| {
                state
                    .database(&target)
                    .and_then(|db| db.fork())
                    .is_some_and(|fork| {
                        matches!(fork.state(), ForkState::Ready { holders: None, .. })
                    })
            })
        })?;
    let released = run.placement(target.as_str(), REPLICAS)?;
    // A member of the released placement that is neither the primary nor a holder leaves,
    // which moves placement.
    let members: Vec<&str> = NODES.to_vec();
    let mut leaving = None;
    for node in &released {
        if *node == primary || holders.contains(node) {
            continue;
        }
        let rest: Vec<&str> = members.iter().copied().filter(|m| m != node).collect();
        let moved = run.placement_among(target.as_str(), REPLICAS, &rest)?;
        leaving = Some((node.clone(), moved));
        break;
    }
    let Some((leaving, moved)) = leaving else {
        // Every member of the released placement is a holder: no change moves it off a set
        // the primary doesn't count anyway.
        run.faults(&primary)?
            .release_operation(FaultPoint::FollowReady, operation);
        return run.finish(operation, Expected::Ready);
    };
    let leaving_id: NodeId = leaving.parse()?;
    run.fleet.note(format!(
        "{target}: holders {holders:?}, primary {primary}, released to {released:?}; {leaving} leaves, moving placement to {moved:?}"
    ));
    // Stopped first: a removed node that runs joins again.
    run.crash(&leaving)?;
    run.operator.push(Operation::Leave {
        node: leaving_id.clone(),
    });
    let kept = target.clone();
    run.fleet
        .run_until(LIMIT, "the leave keeping the released placement", |fleet| {
            fleet.control_state().is_some_and(|state| {
                state.member(&leaving_id).is_none()
                    && state
                        .database(&kept)
                        .is_some_and(|db| db.kept_replicas().is_some())
            })
        })?;
    run.fleet.await_ring_on(&[primary.as_str()], LIMIT)?;
    // Cut the primary off from nodes so that the holders' and the new placement's write
    // quorums can still acknowledge, but the kept set's can't: what a primary that counts its
    // own ring would acknowledge, and a promotion of the kept set couldn't find.
    let Some(cut) = cut_for_loss(&primary, &leaving, &holders, &released, &moved) else {
        run.fleet
            .note("no partition separates the kept set from the counted ones".to_owned());
        run.faults(&primary)?
            .release_operation(FaultPoint::FollowReady, operation);
        return run.finish(operation, Expected::Ready);
    };
    for node in &cut {
        run.fleet.partition(&primary, node)?;
    }
    run.wait(Duration::from_secs(2))?;
    // Sent once: a script would send it again after an uncertain answer, and a later attempt,
    // acknowledged by the next primary, would read as the same write an earlier read saw.
    run.operator.push(Operation::Write {
        database: target.clone(),
        value: 24_102,
    });
    run.wait(Duration::from_secs(5))?;
    run.crash(&primary)?;
    for node in &cut {
        run.fleet.repair(&primary, node)?;
    }
    // The node that left comes back, so that a quorum of the kept set is live without the
    // primary: its join moves placement back onto that set.
    run.restart(&leaving)?;
    run.faults(&primary)?
        .release_operation(FaultPoint::FollowReady, operation);
    let promoted = target.clone();
    let old = primary.clone();
    run.fleet.run_until(LIMIT, "a promotion", |fleet| {
        fleet.control_state().is_some_and(|state| {
            state
                .database(&promoted)
                .and_then(|db| db.lease())
                .is_some_and(|lease| lease.holder().as_str() != old)
        })
    })?;
    run.fleet.check_history()?;
    run.restart(&primary)?;
    run.finish(operation, Expected::Ready)
}

row!(
    row_24_membership_change_during_handover,
    sweep_row_24_membership_change_during_handover,
    [Point::BeforeFirstLease, Point::DuringReset],
    membership_change_during_handover
);

/// The nodes to cut `primary` off from so that, counting the write quorums of `holders` and
/// `moved` alone, it could acknowledge a write that fewer than a write quorum of `kept` hold:
/// the smallest such set, or `None` if there is none. `leaving` is removed, and counts nowhere.
fn cut_for_loss(
    primary: &str,
    leaving: &str,
    holders: &[String],
    kept: &[String],
    moved: &[String],
) -> Option<Vec<String>> {
    let others: Vec<&str> = NODES
        .iter()
        .copied()
        .filter(|node| *node != primary && *node != leaving)
        .collect();
    let write_quorum = |set: &[String]| set.len() / 2 + 1;
    let mut best: Option<Vec<String>> = None;
    for mask in 0_u32..(1 << others.len()) {
        let cut: Vec<String> = others
            .iter()
            .enumerate()
            .filter(|(index, _)| mask & (1 << index) != 0)
            .map(|(_, node)| (*node).to_owned())
            .collect();
        let holds = |node: &String| {
            node == primary
                || (!cut.contains(node) && (holders.contains(node) || moved.contains(node)))
        };
        let count = |set: &[String]| set.iter().filter(|node| holds(node)).count();
        let acknowledged =
            count(holders) >= write_quorum(holders) && count(moved) >= write_quorum(moved);
        let lost = count(kept) < write_quorum(kept);
        if acknowledged && lost && best.as_ref().is_none_or(|best| cut.len() < best.len()) {
            best = Some(cut);
        }
    }
    best
}
