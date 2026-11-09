//! Shorthands for fork tests: a fleet whose members support forks, fork requests, and the
//! heartbeats that report staged copies, seed failures, and resets.

use std::collections::BTreeSet;

use fleetring_control::fork::{
    ChainHash, CopyLossReason, CopyLost, ForkIdentity, ForkIntent, ForkLimits, ForkState,
    OperationId, ResetReport, SeedFailure, SeedFailureReason, StagedCopy,
};
use fleetring_control::{
    Action, Applied, Command, HostedReplica, Incarnation, LeaseEpoch, Member, Millis, Outcome,
    Position, PrincipalName, ReplicaRole, State, UnknownReports, apply, fork,
};

use crate::common::{at, config, create, current_software, db, id, run};

/// The source every fork test copies.
pub(crate) const SOURCE: &str = "src";

/// The operation with ID bytes all `n`.
pub(crate) fn op(n: u8) -> OperationId {
    OperationId::from_bytes([n; 16])
}

pub(crate) fn principal(name: &str) -> PrincipalName {
    name.parse().expect("a valid principal name")
}

/// Short limits, so that tests reach deadlines and retention quickly: 3 attempts, a 60 s
/// deadline, 20 s attempts, a 5 s seeder timeout, and 100 s retention.
pub(crate) fn limits() -> ForkLimits {
    ForkLimits {
        max_attempts: 3,
        deadline: Millis::new(60_000),
        attempt_timeout: Millis::new(20_000),
        seeder_timeout: Millis::new(5_000),
        operation_retention: Millis::new(100_000),
    }
}

/// `alice` forks `source` as `name` with `replication_factor` replicas, under operation `n`.
pub(crate) fn fork_of(n: u8, name: &str, source: &str, replication_factor: u8) -> Action {
    let mut intent = ForkIntent::new(
        db(name),
        db(source),
        crate::common::factor(replication_factor),
    );
    intent.requested_by = Some(principal("alice"));
    Action::ForkDatabase {
        operation: op(n),
        intent,
        incarnation: fork_incarnation(n),
        resolved_expiry: None,
        limits: limits(),
    }
}

/// The incarnation the control node drew for the fork of operation `n`: any value, as long as no
/// live record uses it.
pub(crate) fn fork_incarnation(n: u8) -> Incarnation {
    Incarnation::new(0xF0_0000 + u64::from(n))
}

/// Creates `name` with `replication_factor` replicas at `incarnation`, as the control node
/// does once forks are enabled.
pub(crate) fn create_at(name: &str, replication_factor: u8, incarnation: u64) -> Action {
    let mut action = create(name, replication_factor);
    if let Action::CreateDatabase {
        incarnation: drawn, ..
    } = &mut action
    {
        *drawn = Incarnation::new(incarnation);
    }
    action
}

/// A `ForkDatabase` with `incarnation` drawn instead.
pub(crate) fn with_incarnation(mut action: Action, value: u64) -> Action {
    if let Action::ForkDatabase { incarnation, .. } = &mut action {
        *incarnation = Incarnation::new(value);
    }
    action
}

/// `alice` forks [`SOURCE`] as `name` with three replicas, under operation `n`.
pub(crate) fn fork_request(n: u8, name: &str) -> Action {
    fork_of(n, name, SOURCE, 3)
}

/// A fleet of `nodes` that all support forks, joined at 0, with [`SOURCE`] created at three
/// replicas before any fork, so at the legacy incarnation.
pub(crate) fn fleet(nodes: &[&str]) -> State {
    let mut state = State::new(config());
    for node in nodes {
        let member = Member::new(id(node)).with_software(current_software());
        run(&mut state, 0, Action::Join { member }).expect("join");
    }
    run(&mut state, 0, create(SOURCE, 3)).expect("create the source");
    state
}

/// The identity of the fork `name` as the state has it now.
pub(crate) fn identity(state: &State, name: &str) -> ForkIdentity {
    let database = state.database(&db(name)).expect("the fork exists");
    let fork = database.fork().expect("a fork");
    ForkIdentity::new(db(name), database.incarnation(), fork.operation())
}

/// The fork's state as the state has it now.
pub(crate) fn fork_state(state: &State, name: &str) -> ForkState {
    state
        .database(&db(name))
        .and_then(|database| database.fork())
        .expect("a fork")
        .state()
        .clone()
}

/// The fork's current attempt.
pub(crate) fn attempt(state: &State, name: &str) -> u32 {
    match fork_state(state, name) {
        ForkState::Seeding { attempt, .. } => attempt,
        other => panic!("{name} isn't seeding: {other:?}"),
    }
}

/// The fork's current seeder.
pub(crate) fn seeder(state: &State, name: &str) -> String {
    match fork_state(state, name) {
        ForkState::Seeding { seeder, .. } => seeder.as_str().to_owned(),
        other => panic!("{name} isn't seeding: {other:?}"),
    }
}

/// The source position every staged copy in these tests holds.
pub(crate) fn copy_at() -> Position {
    Position::new([1, 0])
}

/// The chain hash every staged copy in these tests has.
pub(crate) fn copy_hash() -> ChainHash {
    ChainHash::new([7, 7])
}

/// The staged copy of `fork` at `attempt` from the legacy source.
pub(crate) fn staged(fork: &ForkIdentity, attempt: u32) -> StagedCopy {
    StagedCopy::new(
        fork.clone(),
        attempt,
        copy_at(),
        copy_hash(),
        Incarnation::LEGACY,
    )
}

/// A follower replica of `database` with nothing else to say.
pub(crate) fn replica(database: &str) -> HostedReplica {
    HostedReplica::new(db(database), ReplicaRole::Follower, Position::default())
}

/// A heartbeat from `node` reporting `replicas` and `disk_free_bytes`.
pub(crate) fn heartbeat_of(
    node: &str,
    replicas: Vec<HostedReplica>,
    disk_free_bytes: Option<u64>,
) -> Action {
    Action::Heartbeat {
        id: id(node),
        replicas,
        unknown_reports: UnknownReports::Skip,
        software: None,
        disk_free_bytes,
    }
}

/// A heartbeat from `node` reporting its staged copy of `fork` at `attempt`.
pub(crate) fn report_staged(node: &str, fork: &ForkIdentity, attempt: u32) -> Action {
    heartbeat_of(
        node,
        vec![replica(fork.name.as_str()).with_staged(staged(fork, attempt))],
        None,
    )
}

/// A heartbeat from `node` reporting that its staged copy of `fork` from `attempt` is lost.
pub(crate) fn report_copy_lost(node: &str, fork: &ForkIdentity, attempt: u32) -> Action {
    let lost = CopyLost::new(fork.clone(), attempt, CopyLossReason::Missing);
    heartbeat_of(
        node,
        vec![replica(fork.name.as_str()).with_copy_lost(lost)],
        None,
    )
}

/// A heartbeat from `node` reporting that it couldn't seed `fork` at `attempt`.
pub(crate) fn report_failure(
    node: &str,
    fork: &ForkIdentity,
    attempt: u32,
    reason: SeedFailureReason,
) -> Action {
    heartbeat_of(
        node,
        vec![
            replica(fork.name.as_str()).with_seed_failure(SeedFailure::new(
                fork.clone(),
                attempt,
                reason,
            )),
        ],
        None,
    )
}

/// A heartbeat from `node`, the primary of `fork` at `epoch`, reporting its reset committed at
/// `position`.
pub(crate) fn report_reset(
    node: &str,
    fork: &ForkIdentity,
    epoch: LeaseEpoch,
    position: &[u8],
) -> Action {
    let report =
        HostedReplica::new(fork.name.clone(), ReplicaRole::Primary, Position::new([9])).with_reset(
            ResetReport::committed(fork.clone(), epoch, Position::new(position)),
        );
    heartbeat_of(node, vec![report], None)
}

/// A `CompleteFork` of `fork` at `attempt` by `holders`, claiming the tests' staged copy.
pub(crate) fn complete(fork: &ForkIdentity, attempt: u32, holders: &[&str]) -> Action {
    Action::CompleteFork {
        fork: fork.clone(),
        attempt,
        at: copy_at(),
        chain_hash: copy_hash(),
        holders: holders.iter().map(|node| id(node)).collect::<BTreeSet<_>>(),
    }
}

/// Changes the intent of a `ForkDatabase` with `edit`.
pub(crate) fn with_intent(mut action: Action, edit: impl FnOnce(&mut ForkIntent)) -> Action {
    if let Action::ForkDatabase { intent, .. } = &mut action {
        edit(intent);
    }
    action
}

/// Applies every action the fork driver proposes at `millis`, and returns them with their
/// outcomes.
pub(crate) fn drive(state: &mut State, millis: u64) -> Vec<(Action, Outcome)> {
    fork::actions(state, at(millis))
        .into_iter()
        .map(|action| {
            let outcome = apply(state, Command::new(at(millis), action.clone()));
            (action, outcome)
        })
        .collect()
}

/// A fleet of `n0`, `n1`, and `n2` with the fork `t` of [`SOURCE`] made at 10 ms and seeded at
/// 20 ms by every replica.
pub(crate) fn seeded() -> State {
    let mut state = fleet(&["n0", "n1", "n2"]);
    run(&mut state, 10, fork_request(1, "t")).expect("fork");
    let fork = identity(&state, "t");
    for node in ["n0", "n1", "n2"] {
        run(&mut state, 20, report_staged(node, &fork, 1)).expect("heartbeat");
    }
    run(&mut state, 20, complete(&fork, 1, &["n0", "n1", "n2"])).expect("complete");
    state
}

/// The epoch-1 lease of `t` in `state`, granted to `node`.
pub(crate) fn first_lease(state: &mut State, millis: u64, node: &str) -> Outcome {
    run(
        state,
        millis,
        Action::AcquireLease {
            database: db("t"),
            node: id(node),
        },
    )
}

/// Whether `outcome` accepted the command.
pub(crate) fn accepted(outcome: &Outcome) -> bool {
    matches!(outcome, Ok(applied) if !matches!(applied, Applied::ForkResent { .. }))
}
