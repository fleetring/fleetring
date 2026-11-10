//! FR-085's simulation hooks: a fork driven to `ready` by reports fed without data nodes
//! (ADR-0046), as FR-087's scenarios feed them.
//!
//! A control-only fleet; three nodes that join reporting `fork`, as a release that seeds forks
//! would; a fork of a database on them; then staged copies, a lease, and reset writes reported by
//! hand, some delayed and some duplicated, and one for another fork identity that must count for
//! nothing.

use std::collections::BTreeSet;
use std::error::Error;
use std::time::Duration;

use fleetring::NodeId;
use fleetring_control::fork::{
    ChainHash, ForkIdentity, ForkState, OperationId, ResetReport, StagedCopy,
};
use fleetring_control::{
    DatabaseName, Feature, HostedReplica, Incarnation, LeaseEpoch, Position, ReplicaRole, State,
};
use fleetringd::sim::{Ack, Fleet, Operation, Operator, seeds};

use crate::{BINARY, TestResult};

const CONTROL: &str = "control";
const NODES: [&str; 3] = ["n1", "n2", "n3"];
const LIMIT: Duration = Duration::from_secs(10);

/// The fork's state in `state`, if the fork exists.
fn fork_state(state: &State, fork: &DatabaseName) -> Option<ForkState> {
    state
        .database(fork)
        .and_then(|database| database.fork())
        .map(|record| record.state().clone())
}

/// Steps until the fork's state in the control plane satisfies `done`.
fn await_fork(
    fleet: &mut Fleet,
    fork: &DatabaseName,
    what: &str,
    done: impl Fn(&ForkState) -> bool,
) -> Result<(), Box<dyn Error>> {
    fleet.run_until(LIMIT, what, |fleet| {
        fleet
            .control_state()
            .and_then(|state| fork_state(&state, fork))
            .is_some_and(|state| done(&state))
    })?;
    Ok(())
}

fn scenario(seed: u64) -> TestResult {
    let mut fleet = Fleet::builder(seed).control(CONTROL).build()?;
    let operator = Operator::new();
    fleet.client("operator", operator.clone());
    let nodes: Vec<NodeId> = NODES
        .iter()
        .map(|node| node.parse())
        .collect::<Result<_, _>>()?;
    for node in &nodes {
        operator.push(Operation::JoinWith {
            node: node.clone(),
            features: vec![Feature::Fork],
        });
    }
    let source: DatabaseName = "notes".parse()?;
    let target: DatabaseName = "notes-exp".parse()?;
    operator.push(Operation::CreateDatabase {
        database: source.clone(),
        replication_factor: 3,
    });
    let mut id = [0_u8; 16];
    id[..8].copy_from_slice(&fleet.rng().next_u64().to_be_bytes());
    id[8..].copy_from_slice(&fleet.rng().next_u64().to_be_bytes());
    let operation = OperationId::from_bytes(id);
    let fork = Operation::Fork {
        source: source.clone(),
        target: target.clone(),
        operation,
        replication_factor: 3,
        expect_source_incarnation: None,
    };
    // Sent twice: the resend answers with the same operation.
    operator.push(fork.clone());
    operator.push(fork);
    await_fork(&mut fleet, &target, "the fork seeding", |state| {
        matches!(state, ForkState::Seeding { .. })
    })?;
    let state = fleet.control_state().ok_or("control is down")?;
    let incarnation = state
        .database(&target)
        .map(|database| database.incarnation())
        .ok_or("no fork")?;
    assert_ne!(incarnation, Incarnation::LEGACY);
    let identity = ForkIdentity::new(target.clone(), incarnation, operation);

    let at = Position::new([0, 0, 0, 0x2A]);
    let chain_hash = ChainHash::new([9; 32]);
    let staged = |identity: &ForkIdentity| {
        HostedReplica::new(target.clone(), ReplicaRole::Follower, Position::new([])).with_staged(
            StagedCopy::new(
                identity.clone(),
                1,
                at.clone(),
                chain_hash.clone(),
                Incarnation::LEGACY,
            ),
        )
    };
    let report = |node: &NodeId, replica: HostedReplica| Operation::Report {
        node: node.clone(),
        replicas: vec![replica],
        disk_free_bytes: Some(1 << 40),
    };

    // n3 reports a staged copy for another operation of the same name: it counts for nothing.
    let stranger = ForkIdentity::new(
        target.clone(),
        incarnation,
        OperationId::from_bytes([1; 16]),
    );
    operator.push(report(&nodes[2], staged(&stranger)));
    operator.push(report(&nodes[0], staged(&identity)));
    // n2's report is delayed, then duplicated.
    fleet.run_for(Duration::from_millis(300))?;
    operator.push(report(&nodes[1], staged(&identity)));
    operator.push(report(&nodes[1], staged(&identity)));
    await_fork(&mut fleet, &target, "the fork seeded", |state| {
        matches!(state, ForkState::Seeded { .. })
    })?;
    let state = fleet.control_state().ok_or("control is down")?;
    let Some(ForkState::Seeded {
        holders,
        at: seeded_at,
        ..
    }) = fork_state(&state, &target)
    else {
        return Err("not seeded".into());
    };
    let expected: BTreeSet<NodeId> = nodes[..2].iter().cloned().collect();
    assert_eq!(holders, expected, "the stranger's report counted");
    assert_eq!(seeded_at, at);

    // n1, a holder still reporting its copy, takes the first lease, and the reset is reported
    // committed at its epoch: by n1 at once, by n2 late and twice.
    operator.push(report(&nodes[0], staged(&identity)));
    operator.push(Operation::AcquireLease {
        database: target.clone(),
        node: nodes[0].clone(),
    });
    fleet.run_until(LIMIT, "the first lease", |fleet| {
        fleet.control_state().is_some_and(|state| {
            state
                .database(&target)
                .and_then(|database| database.lease())
                .is_some_and(|lease| lease.holder() == &nodes[0])
        })
    })?;
    let epoch = LeaseEpoch::new(1);
    let reset_at = Position::new([0, 0, 0, 0x2B]);
    let reset = ResetReport::committed(identity.clone(), epoch, reset_at.clone());
    operator.push(report(
        &nodes[0],
        HostedReplica::new(target.clone(), ReplicaRole::Primary, reset_at.clone())
            .with_log_epoch(epoch)
            .with_reset(reset.clone()),
    ));
    fleet.run_for(Duration::from_millis(200))?;
    let follower = staged(&identity).with_reset(reset);
    operator.push(report(&nodes[1], follower.clone()));
    operator.push(report(&nodes[1], follower));
    await_fork(&mut fleet, &target, "the fork ready", |state| {
        matches!(state, ForkState::Ready { .. })
    })?;
    let state = fleet.control_state().ok_or("control is down")?;
    let Some(ForkState::Ready {
        reset_epoch,
        reset_position,
        ..
    }) = fork_state(&state, &target)
    else {
        return Err("not ready".into());
    };
    assert_eq!((reset_epoch, reset_position), (epoch, reset_at));

    let report = fleet.finish()?;
    let forked: Vec<Incarnation> = report
        .history()
        .acknowledged()
        .filter_map(|(_, ack)| match ack {
            Ack::Forked { incarnation } => Some(*incarnation),
            _ => None,
        })
        .collect();
    assert_eq!(
        forked,
        [incarnation, incarnation],
        "a resend made another fork"
    );
    Ok(())
}

#[test]
fn reports_fed_without_data_nodes_drive_a_fork_to_ready() {
    seeds::run(BINARY, &[85], scenario);
}
