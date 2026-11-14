//! Forks replay: a log with every fork command and refusal replays to the identical state, a
//! state and log written before forks load and re-serialize byte for byte, and generated fork
//! logs keep ADR-0046's invariants.

use std::collections::{BTreeMap, BTreeSet};

use fleetring_control::fork::{
    ChainHash, CopyLossReason, CopyLost, ForkFailure, ForkIdentity, ForkState, OperationId,
    OperationOutcome, PlacementCaughtUp, ResetReport, RetryCause, SeedFailure, SeedFailureReason,
    StagedCopy,
};
use fleetring_control::{
    Action, Applied, Command, Durability, HostedReplica, Incarnation, LeaseEpoch, Member, Outcome,
    Position, Rejection, ReplicaRole, State, apply, fork,
};
use proptest::prelude::*;

use crate::common::fork::{
    SOURCE, complete, create_at, fork_of, fork_request, heartbeat_of, identity, limits, op,
    principal, replica, report_copy_lost, report_reset, report_staged, with_incarnation,
    with_intent,
};
use crate::common::{at, config, create, current_software, db, factor, id};
use crate::replay::{
    check_replay_from_scratch, check_replay_from_snapshots, check_replay_through_serialization,
};

/// A control log being written: every command applied and kept with its outcome.
struct Log {
    state: State,
    commands: Vec<Command>,
    outcomes: Vec<Outcome>,
}

impl Log {
    fn new() -> Self {
        Self {
            state: State::new(config()),
            commands: Vec::new(),
            outcomes: Vec::new(),
        }
    }

    fn run(&mut self, millis: u64, action: Action) -> Outcome {
        let command = Command::new(at(millis), action);
        let outcome = apply(&mut self.state, command.clone());
        self.commands.push(command);
        self.outcomes.push(outcome.clone());
        outcome
    }

    /// Applies and logs everything the fork driver proposes at `millis`.
    fn drive(&mut self, millis: u64) -> Vec<Outcome> {
        fork::actions(&self.state, at(millis))
            .into_iter()
            .map(|action| self.run(millis, action))
            .collect()
    }

    fn check(&self) {
        check_replay_from_scratch(&self.commands, &self.outcomes, &self.state);
        check_replay_from_snapshots(&self.commands, &self.outcomes, &self.state);
        check_replay_through_serialization(&self.commands, &self.outcomes, &self.state);
    }
}

fn applied_kind(outcome: &Outcome) -> &'static str {
    match outcome {
        Ok(Applied::ForkCreated { .. }) => "created",
        Ok(Applied::ForkResent { .. }) => "resent",
        Ok(Applied::ForkSeeded { .. }) => "seeded",
        Ok(Applied::ForkRetried { .. }) => "retried",
        Ok(Applied::ForkDiskRefusalRecorded { .. }) => "disk refused",
        Ok(Applied::ForkHolderDropped { .. }) => "holder dropped",
        Ok(Applied::ForkFailed { .. }) => "failed",
        Ok(Applied::ForkReady { .. }) => "ready",
        Ok(Applied::ForkHoldersReleased { .. }) => "holders released",
        Ok(Applied::LeaseGranted { .. }) => "leased",
        Ok(Applied::DatabaseDropped { .. }) => "dropped",
        Ok(Applied::Ticked { .. }) => "ticked",
        Ok(_) => "other",
        Err(Rejection::StaleForkIdentity { .. }) => "stale identity",
        Err(Rejection::OperationConflict { .. }) => "conflict",
        Err(Rejection::OperationNotYours { .. }) => "not yours",
        Err(Rejection::SourceNotReady { .. }) => "source not ready",
        Err(Rejection::SourceIncarnationMismatch { .. }) => "source mismatch",
        Err(Rejection::ForkNotSeeding { .. }) => "not seeding",
        Err(Rejection::ForkNotSeeded { .. }) => "not seeded",
        Err(Rejection::NotEnoughHolders { .. }) => "not enough holders",
        Err(Rejection::HolderReportMismatch { .. }) => "report mismatch",
        Err(Rejection::LeaseOnUnseededFork { .. }) => "lease on unseeded",
        Err(Rejection::StagedCopyNotReported { .. }) => "copy not reported",
        Err(Rejection::ResetNotReported { .. }) => "reset not reported",
        Err(Rejection::ForkNotReady { .. }) => "not ready",
        Err(Rejection::IncarnationUnavailable { .. }) => "incarnation unavailable",
        Err(Rejection::HolderNotDroppable { .. }) => "not droppable",
        Err(Rejection::HoldersNotReleasable { .. }) => "not releasable",
        Err(Rejection::DatabaseExists { .. }) => "exists",
        Err(_) => "rejected",
    }
}

/// A log with every fork command and every fork refusal, a drop and recreate of a fork target,
/// a first lease, readiness, and a tick that forgets operations, replays to the identical state
/// and outcomes, from scratch, from snapshots, and through serialization.
#[test]
fn a_log_with_every_fork_command_and_refusal_replays_identically() {
    let mut log = Log::new();
    for node in ["n0", "n1", "n2", "n3"] {
        let member = Member::new(id(node)).with_software(current_software());
        log.run(0, Action::Join { member }).expect("join");
    }
    log.run(0, create(SOURCE, 3)).expect("source");

    // A fork, and every refusal of a request.
    log.run(10, fork_request(1, "t")).expect("fork");
    let first = identity(&log.state, "t");
    log.run(11, fork_request(1, "t")).expect("resent");
    log.run(
        11,
        with_intent(fork_request(1, "t"), |intent| {
            intent.replication_factor = factor(2);
        }),
    )
    .expect_err("conflict");
    log.run(
        11,
        with_intent(fork_request(1, "t"), |intent| {
            intent.requested_by = Some(principal("mallory"));
        }),
    )
    .expect_err("not yours");
    log.run(11, fork_request(2, "t")).expect_err("exists");
    log.run(11, fork_of(2, "u", "t", 3))
        .expect_err("source not ready");
    log.run(
        11,
        with_intent(fork_of(2, "u", SOURCE, 3), |intent| {
            intent.expect_source_incarnation = Some(Incarnation::new(9));
        }),
    )
    .expect_err("source mismatch");
    log.run(
        11,
        Action::AcquireLease {
            database: db("t"),
            node: id("n0"),
        },
    )
    .expect_err("lease on unseeded");
    log.run(
        11,
        Action::ChangeDurability {
            database: db("t"),
            durability: Durability::quorum(factor(3)),
        },
    )
    .expect_err("not ready");
    log.run(11, create_at("x", 1, 0))
        .expect_err("incarnation 0");
    log.run(11, create_at("x", 1, first.incarnation.get()))
        .expect_err("incarnation in use");
    log.run(11, complete(&first, 1, &["n0"]))
        .expect_err("not enough");
    log.run(11, complete(&first, 2, &["n0", "n1"]))
        .expect_err("not seeding");

    // Retries and a disk refusal.
    let order: Vec<String> = log
        .state
        .replica_set(&db("t"))
        .expect("replicas")
        .iter()
        .map(|node| node.as_str().to_owned())
        .collect();
    log.run(
        20,
        Action::RetryForkSeed {
            fork: first.clone(),
            attempt: 1,
            cause: RetryCause::TransferFailed,
            exclude_seeder: None,
        },
    )
    .expect("attempt 2");
    let refusal = SeedFailure::new(first.clone(), 2, SeedFailureReason::InsufficientDisk);
    let report = replica("t").with_seed_failure(refusal);
    log.run(30, heartbeat_of(&order[2], vec![report], None))
        .expect("hb");
    log.drive(31);

    // The target is dropped and made again; the old identity's commands are refused.
    log.run(40, Action::DropDatabase { name: db("t") })
        .expect("drop");
    log.run(50, with_incarnation(fork_request(3, "t"), 0x0B))
        .expect("fork again");
    let second = identity(&log.state, "t");
    for action in [
        complete(&first, 2, &["n0", "n1"]),
        Action::FailFork {
            fork: first.clone(),
            attempt: 2,
            reason: ForkFailure::SourceGone,
        },
        Action::MarkForkReady {
            fork: first.clone(),
            epoch: LeaseEpoch::new(1),
            reset_position: Position::new([1]),
        },
    ] {
        log.run(51, action).expect_err("stale");
    }

    // Completion, a lost copy, the first lease, and readiness.
    let order: Vec<String> = log
        .state
        .replica_set(&db("t"))
        .expect("replicas")
        .iter()
        .map(|node| node.as_str().to_owned())
        .collect();
    for node in &order {
        log.run(60, report_staged(node, &second, 1)).expect("hb");
    }
    log.run(61, complete(&second, 1, &[order[0].as_str()]))
        .expect_err("not enough");
    log.drive(62);
    log.run(70, report_copy_lost(&order[2], &second, 1))
        .expect("hb");
    log.run(
        71,
        Action::AcquireLease {
            database: db("t"),
            node: id(&order[2]),
        },
    )
    .expect_err("copy not reported");
    log.drive(72);
    log.run(
        73,
        Action::MarkForkReady {
            fork: second.clone(),
            epoch: LeaseEpoch::new(1),
            reset_position: Position::new([3]),
        },
    )
    .expect_err("stale epoch");
    log.run(
        74,
        Action::AcquireLease {
            database: db("t"),
            node: id(&order[0]),
        },
    )
    .expect("first lease");
    log.run(
        75,
        report_reset(&order[0], &second, LeaseEpoch::new(1), &[3]),
    )
    .expect("hb");
    log.run(
        76,
        Action::MarkForkReady {
            fork: second.clone(),
            epoch: LeaseEpoch::new(1),
            reset_position: Position::new([3]),
        },
    )
    .expect_err("one of a bootstrap quorum of two");
    log.run(
        77,
        report_reset(&order[1], &second, LeaseEpoch::new(1), &[3]),
    )
    .expect("hb");
    log.drive(78);
    log.run(
        79,
        Action::MarkForkReady {
            fork: second.clone(),
            epoch: LeaseEpoch::new(1),
            reset_position: Position::new([3]),
        },
    )
    .expect_err("ready already");
    // Ready, it keeps its holders until its primary reports placement caught up.
    let release = Action::ReleaseForkHolders {
        fork: second.clone(),
        epoch: LeaseEpoch::new(1),
    };
    log.run(79, release.clone()).expect_err("not reported");
    let caught_up = HostedReplica::new(db("t"), ReplicaRole::Primary, Position::new([4]))
        .with_log_epoch(LeaseEpoch::new(1))
        .with_placement_caught_up(PlacementCaughtUp::new(
            second.clone(),
            LeaseEpoch::new(1),
            log.state.membership_epoch(),
        ));
    log.run(79, heartbeat_of(&order[0], vec![caught_up], None))
        .expect("hb");
    log.drive(79);
    log.run(79, release).expect_err("released already");

    // A fork that fails, and a tick past every retention.
    log.run(80, fork_of(4, "u", SOURCE, 3)).expect("fork");
    let failing = identity(&log.state, "u");
    log.run(
        81,
        Action::FailFork {
            fork: failing,
            attempt: 1,
            reason: ForkFailure::SourceTooLarge,
        },
    )
    .expect("fail");
    let forgotten = 81 + limits().operation_retention.get();
    assert!(fork::actions(&log.state, at(forgotten)).contains(&Action::Tick));
    log.run(forgotten, Action::Tick).expect("tick");
    let left: Vec<OperationId> = log
        .state
        .fork_operations()
        .map(|(operation, _)| *operation)
        .collect();
    assert_eq!(left, Vec::<OperationId>::new());

    let mut kinds: BTreeMap<&str, usize> = BTreeMap::new();
    for outcome in &log.outcomes {
        *kinds.entry(applied_kind(outcome)).or_default() += 1;
    }
    for kind in [
        "created",
        "resent",
        "seeded",
        "retried",
        "disk refused",
        "holder dropped",
        "failed",
        "ready",
        "holders released",
        "not releasable",
        "leased",
        "dropped",
        "ticked",
        "stale identity",
        "conflict",
        "not yours",
        "source not ready",
        "source mismatch",
        "not seeding",
        "not seeded",
        "not enough holders",
        "lease on unseeded",
        "copy not reported",
        "reset not reported",
        "not ready",
        "incarnation unavailable",
        "exists",
    ] {
        assert!(kinds.contains_key(kind), "no '{kind}' in {kinds:?}");
    }
    log.check();
}

/// A log in which a leased fork's holders leave the fleet replays identically: every drop is
/// refused, and the fork stays seeded on all its holders with its lease (ADR-0046, point 3.6,
/// as built).
#[test]
fn a_log_where_a_leased_forks_holders_leave_replays_identically() {
    let mut log = Log::new();
    for node in ["n0", "n1", "n2", "n3", "n4"] {
        let member = Member::new(id(node)).with_software(current_software());
        log.run(0, Action::Join { member }).expect("join");
    }
    log.run(0, create(SOURCE, 3)).expect("source");
    log.run(10, fork_request(1, "t")).expect("fork");
    let fork = identity(&log.state, "t");
    let holders: Vec<String> = log
        .state
        .replica_set(&db("t"))
        .expect("replicas")
        .iter()
        .map(|node| node.as_str().to_owned())
        .collect();
    for node in &holders {
        log.run(20, report_staged(node, &fork, 1)).expect("hb");
    }
    log.drive(21);
    log.run(
        30,
        Action::AcquireLease {
            database: db("t"),
            node: id(&holders[0]),
        },
    )
    .expect("first lease");
    for node in &holders[1..] {
        log.run(40, Action::Leave { id: id(node) }).expect("leave");
    }
    log.drive(50);
    for node in &holders[1..] {
        let _ = log.run(
            51,
            Action::DropForkHolder {
                fork: fork.clone(),
                attempt: 1,
                holder: id(node),
                cause: RetryCause::HoldersLeft,
            },
        );
    }
    let refusals = log
        .outcomes
        .iter()
        .filter(|outcome| matches!(outcome, Err(Rejection::HolderNeededAfterLease { .. })))
        .count();
    assert_eq!(refusals, holders.len() - 1, "{:?}", log.outcomes);
    assert!(
        !log.outcomes
            .iter()
            .any(|outcome| matches!(outcome, Ok(Applied::ForkHolderDropped { .. }))),
        "{:?}",
        log.outcomes
    );
    assert!(matches!(
        log.state
            .database(&db("t"))
            .and_then(|d| d.fork())
            .map(|f| f.state()),
        Some(ForkState::Seeded { attempt: 1, holders: kept, .. }) if kept.len() == holders.len()
    ));
    log.check();
}

// ---- Before forks ----

/// A state and a log written before forks, by the build before them, load unchanged: every
/// database reads as seeded and ready at the legacy incarnation, re-serializing gives the same
/// bytes, and replaying the log gives the same state, byte for byte.
///
/// The fixtures were written by unmodified `main` at commit a0d51f2, from the generated logs
/// `record_with(&fixed_ops(0x08F0, 400), SoftwareMix::Current)` and
/// `record_with(&fixed_ops(0x08F1, 400), SoftwareMix::Mixed)` in `common`, serialized with
/// `serde_json::to_string` as the control log and its snapshots are. That commit's
/// `fleetring-control` sources are the `fleetringd-v0.7.0` tag's, so the fixtures are what 0.7.0
/// writes, and this is the replay rule for a 0.7 log and snapshot under 0.8 (FR-096). They must
/// never be regenerated: they stand for what a release before forks wrote.
#[test]
fn a_state_and_log_from_before_forks_load_and_reserialize_byte_for_byte() {
    for (log, snapshot) in [
        (
            include_str!("fixtures/pre_0_8_log.json"),
            include_str!("fixtures/pre_0_8_state.json"),
        ),
        (
            include_str!("fixtures/pre_0_8_mixed_log.json"),
            include_str!("fixtures/pre_0_8_mixed_state.json"),
        ),
    ] {
        let state: State = serde_json::from_str(snapshot).expect("the snapshot loads");
        assert_eq!(serde_json::to_string(&state).expect("serializes"), snapshot);
        assert!(state.databases().len() > 0);
        for database in state.databases() {
            assert_eq!(database.incarnation(), Incarnation::LEGACY);
            assert!(database.fork().is_none());
            assert!(database.is_seeded() && database.is_ready());
        }
        assert_eq!(state.fork_operations().len(), 0);

        let commands: Vec<Command> = serde_json::from_str(log).expect("the log loads");
        assert_eq!(serde_json::to_string(&commands).expect("serializes"), log);
        let mut replayed = State::new(config());
        for command in commands {
            let _ = apply(&mut replayed, command);
        }
        assert_eq!(
            serde_json::to_string(&replayed).expect("serializes"),
            snapshot
        );
    }
}

/// A creation screened before `fork` was enabled, and applied after, stores the incarnation it
/// carries; before, the same creation stores 0.
#[test]
fn a_creation_screened_before_forks_and_applied_after_stores_its_incarnation() {
    let mut log = Log::new();
    for node in ["n0", "n1", "n2"] {
        let member = Member::new(id(node)).with_software(current_software());
        log.run(0, Action::Join { member }).expect("join");
    }
    log.run(0, create_at(SOURCE, 3, 0x51)).expect("source");
    assert_eq!(
        log.state.database(&db(SOURCE)).map(|d| d.incarnation()),
        Some(Incarnation::LEGACY)
    );
    let screened = create_at("later", 1, 0x77);
    assert_eq!(log.state.check_admission(&screened), Ok(()));
    log.run(10, fork_request(1, "t"))
        .expect("fork enables the feature");
    log.run(20, screened).expect("create");
    assert_eq!(
        log.state.database(&db("later")).map(|d| d.incarnation()),
        Some(Incarnation::new(0x77))
    );
    let json = serde_json::to_value(&log.state).expect("serializes");
    assert_eq!(
        json["databases"]["later"]["incarnation"],
        serde_json::json!("0000000000000077")
    );
    log.check();
}

// ---- Generated fork logs ----

/// One step of a generated fork log.
#[derive(Debug, Clone, Copy)]
struct Step {
    kind: u8,
    a: u8,
    b: u8,
    dt: u16,
}

fn arb_step() -> impl Strategy<Value = Step> {
    (any::<u8>(), any::<u8>(), any::<u8>(), 0_u16..3_000).prop_map(|(kind, a, b, dt)| Step {
        kind,
        a,
        b,
        dt,
    })
}

const FORK_NODES: [&str; 4] = ["n0", "n1", "n2", "n3"];
const FORKS: [&str; 2] = ["t", "u"];

/// The identity of `name` if it is a fork, or a made-up one, so that commands aim at both.
fn aim(state: &State, name: &str, stale: bool) -> ForkIdentity {
    let current = state
        .database(&db(name))
        .and_then(|database| {
            Some(ForkIdentity::new(
                db(name),
                database.incarnation(),
                database.fork()?.operation(),
            ))
        })
        .unwrap_or_else(|| ForkIdentity::new(db(name), Incarnation::new(1), op(1)));
    if stale {
        ForkIdentity::new(current.name, current.incarnation, op(9))
    } else {
        current
    }
}

fn current_attempt(state: &State, name: &str) -> u32 {
    match state
        .database(&db(name))
        .and_then(|d| d.fork())
        .map(|f| f.state())
    {
        Some(ForkState::Seeding { attempt, .. } | ForkState::Seeded { attempt, .. }) => *attempt,
        _ => 1,
    }
}

/// The action a step makes, aimed with the state.
fn step_action(state: &State, step: Step, clock: u64) -> Vec<Action> {
    let a = usize::from(step.a);
    let b = usize::from(step.b);
    let node = FORK_NODES[a % FORK_NODES.len()];
    let name = FORKS[b % FORKS.len()];
    let stale = b % 7 == 3;
    let fork = aim(state, name, stale);
    let attempt = current_attempt(state, name).saturating_sub(u32::from(b % 5 == 4));
    match step.kind % 12 {
        0 => {
            let source = if b % 6 == 5 { "t" } else { SOURCE };
            let mut action = fork_of(1 + step.a % 4, name, source, 3);
            if b % 4 == 1 {
                action = with_intent(action, |intent| {
                    intent.requested_by = Some(principal("bob"));
                });
            }
            vec![with_incarnation(action, 1 + u64::from(step.b % 6))]
        }
        1 if b % 3 == 0 => vec![Action::DropDatabase { name: db(name) }],
        1 => vec![create_at(name, 3, 1 + u64::from(step.b % 6))],
        2..=4 => {
            let mut report =
                HostedReplica::new(db(name), ReplicaRole::Follower, Position::new([1, 0]))
                    .with_log_epoch(LeaseEpoch::new(u64::from(b % 9 == 0)));
            report = match b % 5 {
                0 | 1 => report.with_staged(StagedCopy::new(
                    fork.clone(),
                    attempt,
                    Position::new([1, 0]),
                    ChainHash::new([7]),
                    Incarnation::LEGACY,
                )),
                2 => report.with_copy_lost(CopyLost::new(
                    fork.clone(),
                    attempt,
                    CopyLossReason::Damaged,
                )),
                3 => report.with_seed_failure(SeedFailure::new(
                    fork.clone(),
                    attempt,
                    if a % 2 == 0 {
                        SeedFailureReason::InsufficientDisk
                    } else {
                        SeedFailureReason::TransferFailed
                    },
                )),
                _ => report.with_reset(ResetReport::committed(
                    fork.clone(),
                    LeaseEpoch::new(1 + u64::from(step.a % 2)),
                    Position::new([3]),
                )),
            };
            vec![heartbeat_of(node, vec![report], Some(1 << 40))]
        }
        5 | 6 => fork::actions(state, at(clock)),
        7 => vec![Action::AcquireLease {
            database: db(name),
            node: id(node),
        }],
        8 => vec![Action::Tick],
        9 if b % 4 == 0 => vec![Action::Leave { id: id(node) }],
        9 => {
            let member = Member::new(id(node)).with_software(current_software());
            vec![Action::Join { member }]
        }
        10 => vec![complete(&fork, attempt, &FORK_NODES[..2 + a % 3])],
        _ => vec![Action::MarkForkReady {
            fork,
            epoch: LeaseEpoch::new(1),
            reset_position: Position::new([3]),
        }],
    }
}

/// Builds a fork log from `steps`, calling `check` with the state before each command, the
/// command, its outcome, and the state after it.
fn fork_log(steps: &[Step], mut check: impl FnMut(&State, &Command, &Outcome, &State)) -> Log {
    let mut log = Log::new();
    for node in FORK_NODES {
        let member = Member::new(id(node)).with_software(current_software());
        log.run(0, Action::Join { member }).expect("join");
    }
    log.run(0, create(SOURCE, 3)).expect("source");
    let mut clock = 0_u64;
    for &step in steps {
        clock += u64::from(step.dt);
        // What the driver proposes, `apply` accepts: a refusal would be logged on every tick.
        let driven = matches!(step.kind % 12, 5 | 6);
        for action in step_action(&log.state, step, clock) {
            let before = log.state.clone();
            let command = Command::new(at(clock), action.clone());
            let outcome = log.run(clock, action);
            assert!(
                !driven || outcome.is_ok(),
                "the driver proposed {command:?}, refused: {outcome:?}"
            );
            check(&before, &command, &outcome, &log.state);
        }
        // Members keep sending heartbeats, so that liveness doesn't decide everything.
        if step.b % 3 == 0 {
            let node = FORK_NODES[usize::from(step.a) % FORK_NODES.len()];
            let _ = log.run(clock, heartbeat_of(node, Vec::new(), None));
        }
    }
    log
}

/// What the invariants remember across a log.
#[derive(Default)]
struct Seen {
    /// Each operation's fork, `(name, incarnation)`, once recorded.
    operations: BTreeMap<OperationId, (String, Incarnation)>,
    /// Each operation's terminal outcome, once it has one.
    terminal: BTreeMap<OperationId, OperationOutcome>,
}

/// ADR-0046's invariants that control state alone can show.
fn check_invariants(
    seen: &mut Seen,
    before: &State,
    command: &Command,
    outcome: &Outcome,
    after: &State,
) {
    // A refused command changes nothing.
    if outcome.is_err() {
        assert_eq!(
            before, after,
            "{command:?} was refused and changed the state"
        );
    }
    // (a) No lease before seeded; the first lease of a seeded fork only on a holder that still
    // reports the matching staged copy.
    if let Ok(Applied::LeaseGranted { database, lease }) = outcome
        && let Some(fork) = before.database(database).and_then(|d| d.fork())
    {
        let ForkState::Seeded { holders, .. } = fork.state() else {
            panic!("a lease on {database} in {:?}", fork.state());
        };
        assert!(holders.contains(lease.holder()));
        let report = before.report(lease.holder(), database).expect("a report");
        assert_eq!(report.log_epoch(), LeaseEpoch::ZERO);
        assert!(report.staged().is_some());
    }
    for database in after.databases() {
        let Some(fork) = database.fork() else {
            continue;
        };
        if !fork.is_seeded() {
            assert!(
                database.lease().is_none(),
                "{} leased while {:?}",
                database.name(),
                fork.state()
            );
        }
        // (i) A seeded fork's replica set is its holders.
        if let ForkState::Seeded { holders, .. } = fork.state() {
            let replicas: BTreeSet<_> = after
                .replica_set(database.name())
                .expect("replicas")
                .into_iter()
                .collect();
            assert_eq!(&replicas, holders);
        }
        assert!(
            !database.incarnation().is_legacy(),
            "a fork at incarnation 0"
        );
    }
    // Incarnations of live databases are distinct.
    let mut incarnations = BTreeSet::new();
    for database in after.databases().filter(|d| !d.incarnation().is_legacy()) {
        assert!(
            incarnations.insert(database.incarnation()),
            "{} repeated",
            database.incarnation()
        );
    }
    // (f) An operation ID names one fork, and a terminal outcome never changes.
    for (operation, record) in after.fork_operations() {
        let fork = (record.target().to_string(), record.target_incarnation());
        let first = seen
            .operations
            .entry(*operation)
            .or_insert_with(|| fork.clone());
        if before
            .fork_operations()
            .any(|(known, _)| known == operation)
        {
            assert_eq!(*first, fork, "operation {operation} changed forks");
        } else {
            *first = fork;
            seen.terminal.remove(operation);
        }
        if record.outcome().is_terminal() {
            let terminal = seen
                .terminal
                .entry(*operation)
                .or_insert_with(|| record.outcome().clone());
            assert_eq!(terminal, record.outcome(), "a terminal outcome changed");
        }
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(48))]

    /// Generated fork logs keep the invariants after every command, and replay identically.
    #[test]
    fn generated_fork_logs_keep_the_invariants_and_replay(
        steps in prop::collection::vec(arb_step(), 1..80),
    ) {
        let mut seen = Seen::default();
        let log = fork_log(&steps, |before, command, outcome, after| {
            check_invariants(&mut seen, before, command, outcome, after);
        });
        check_replay_from_scratch(&log.commands, &log.outcomes, &log.state);
    }
}

/// Two long fixed fork logs replay identically from snapshots and through serialization, and
/// reach every kind of fork change.
#[test]
fn fixed_fork_logs_replay_from_snapshots_and_reach_every_change() {
    let mut kinds: BTreeMap<&str, usize> = BTreeMap::new();
    for seed in [0x0F0E_u64, 0xF0A7] {
        let steps: Vec<Step> = crate::common::fixed_ops(seed, 1_500)
            .into_iter()
            .map(|op| Step {
                kind: op.kind,
                a: op.a,
                b: op.b,
                dt: op.dt * 5,
            })
            .collect();
        let mut seen = Seen::default();
        let log = fork_log(&steps, |before, command, outcome, after| {
            check_invariants(&mut seen, before, command, outcome, after);
        });
        log.check();
        for outcome in &log.outcomes {
            *kinds.entry(applied_kind(outcome)).or_default() += 1;
        }
    }
    for kind in [
        "created",
        "seeded",
        "retried",
        "failed",
        "leased",
        "dropped",
        "stale identity",
    ] {
        assert!(kinds.contains_key(kind), "no '{kind}' in {kinds:?}");
    }
}
