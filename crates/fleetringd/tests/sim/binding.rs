//! FR-050 on the harness: node calls bound to placement (ADR-0031).
//!
//! An impostor holding a stolen node certificate attacks a database that node doesn't host,
//! while writes flow: it streams, fences, and asks for snapshots on each replica, at the current
//! epoch and the next. Every call is refused for its binding, and the database's epoch, its
//! writes, and its replicas are untouched. A membership change that moves replicas while writes
//! flow completes with no binding refusal at all.

use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::time::Duration;

use fleetring::{NodeId, Weight};
use fleetring_control::{DatabaseName, LeaseEpoch};
use fleetringd::sim::{
    Ack, CreateAndWrite, Fleet, Operation, OutcomeClass, ReplicaRows, Report, SimRng, Step,
    Workload, check, seeds,
};

use crate::{BINARY, TestResult};

const CONTROL: &str = "control";

/// How long the impostor waits between rounds of calls.
const ROUND: Duration = Duration::from_millis(200);

/// How many rounds it makes: three seconds of attack.
const ROUNDS: usize = 15;

fn database() -> Result<DatabaseName, Box<dyn Error>> {
    Ok("db-0".parse()?)
}

/// The impostor's client host when it acts as `node`.
fn impostor(node: &str) -> String {
    format!("impostor-{node}")
}

/// Repeats `calls` every [`ROUND`], [`ROUNDS`] times.
#[derive(Debug)]
struct Attack {
    calls: Vec<Operation>,
    rounds: usize,
    queued: Vec<Operation>,
    sleep: bool,
}

impl Attack {
    fn new(calls: Vec<Operation>) -> Self {
        Self {
            calls,
            rounds: ROUNDS,
            queued: Vec::new(),
            sleep: false,
        }
    }
}

impl Workload for Attack {
    fn next(&mut self, _rng: &mut SimRng) -> Step {
        if self.queued.is_empty() {
            if self.sleep {
                self.sleep = false;
                return Step::Sleep(ROUND);
            }
            if self.rounds == 0 {
                return Step::Done;
            }
            self.rounds -= 1;
            self.sleep = true;
            self.queued = self.calls.iter().rev().cloned().collect();
        }
        self.queued.pop().map_or(Step::Done, Step::Run)
    }
}

/// The node an impostor operation targets.
fn target(operation: &Operation) -> Option<&NodeId> {
    match operation {
        Operation::ImpostorReplicate { target, .. }
        | Operation::ImpostorFence { target, .. }
        | Operation::ImpostorSnapshot { target, .. } => Some(target),
        _ => None,
    }
}

/// Steps until every member of `placed`, `database`'s replica set, holds the same rows, and
/// checks that they do. [`Fleet::await_replicas_converged`] compares every data host, which
/// holds only while every data host is a replica; here one isn't.
fn await_placed_converged(
    fleet: &mut Fleet,
    database: &DatabaseName,
    placed: &[NodeId],
) -> TestResult {
    let placed: BTreeSet<String> = placed.iter().map(ToString::to_string).collect();
    let rows = |fleet: &Fleet| -> Result<ReplicaRows, Box<dyn Error>> {
        Ok(fleet
            .replica_rows(database)?
            .into_iter()
            .filter(|(host, _)| placed.contains(host))
            .collect())
    };
    fleet.run_until(
        Duration::from_secs(60),
        "the replica set converging",
        |fleet| {
            fleet.replicas_caught_up()
                && rows(fleet).is_ok_and(|rows| {
                    rows.len() == placed.len() && check::replicas_converged(database, &rows).is_ok()
                })
        },
    )?;
    check::replicas_converged(database, &rows(fleet)?)?;
    fleet.check_chains()?;
    fleet.check_history()?;
    Ok(())
}

fn attacked(seed: u64) -> Result<Report, Box<dyn Error>> {
    const DATA: [&str; 4] = ["node-1", "node-2", "node-3", "node-4"];
    let mut builder = Fleet::builder(seed).control(CONTROL);
    for node in DATA {
        builder = builder.data(node).impostor(&impostor(node), node);
    }
    let mut fleet = builder.operation_timeout(Duration::from_secs(2)).build()?;
    fleet.await_ring(Duration::from_secs(5))?;
    fleet.client("writer", CreateAndWrite::new(1, 3, 300));
    fleet.run_until(Duration::from_secs(20), "writes flowing", |fleet| {
        fleet.history().acknowledged_writes().count() >= 20
    })?;

    let db = database()?;
    let state = fleet.control_state().ok_or("the control node is down")?;
    let epoch = state.database(&db).ok_or("no db-0")?.epoch();
    let replicas = state.replica_set(&db)?;
    let outsider = DATA
        .into_iter()
        .find(|node| !replicas.iter().any(|replica| replica.as_str() == *node))
        .ok_or("every node is a replica")?;
    let next = LeaseEpoch::new(epoch.get() + 1);
    let mut calls = Vec::new();
    for replica in &replicas {
        for at in [epoch, next] {
            calls.push(Operation::ImpostorReplicate {
                database: db.clone(),
                target: replica.clone(),
                epoch: at,
            });
            calls.push(Operation::ImpostorFence {
                database: db.clone(),
                target: replica.clone(),
                epoch: at,
                candidate: outsider.parse()?,
            });
        }
        calls.push(Operation::ImpostorSnapshot {
            database: db.clone(),
            target: replica.clone(),
            epoch,
        });
    }
    let attacker = impostor(outsider);
    let began = fleet.elapsed();
    fleet.client(&attacker, Attack::new(calls));
    fleet.run_until(Duration::from_secs(30), "the attack ending", |fleet| {
        fleet
            .history()
            .records()
            .iter()
            .filter(|record| record.client() == attacker)
            .count()
            >= ROUNDS * replicas.len() * 5
    })?;
    let ended = fleet.elapsed();

    let history = fleet.history();
    check::impostor_refused(&history)?;
    let state = fleet.control_state().ok_or("the control node is down")?;
    assert_eq!(
        state.database(&db).map(|database| database.epoch()),
        Some(epoch),
        "the attack changed db-0's epoch"
    );
    let refused_writes: Vec<String> = history
        .records()
        .iter()
        .filter(|record| {
            matches!(record.operation(), Operation::Write { .. })
                && record.invoked_at() >= began
                && record.invoked_at() <= ended
                && record.class() != OutcomeClass::Ok
        })
        .map(|record| format!("{} {}", record.operation(), record.outcome()))
        .collect();
    assert!(
        refused_writes.is_empty(),
        "writes failed during the attack: {refused_writes:?}"
    );
    // Every refusal a node made was the impostor's: none was a legitimate node's.
    let mut expected: BTreeMap<String, u64> = BTreeMap::new();
    for record in history.records() {
        if let (
            Some(target),
            Some(Ack::Refused {
                binding: Some(_), ..
            }),
        ) = (target(record.operation()), record.outcome().ack())
        {
            *expected.entry(target.to_string()).or_default() += 1;
        }
    }
    for node in DATA {
        let refused = fleet.binding_refusals(node).ok_or("a node is down")?;
        let theirs = expected.get(node).copied().unwrap_or(0);
        assert_eq!(refused, theirs, "{node} refused legitimate calls");
        if replicas.iter().any(|replica| replica.as_str() == node) {
            assert!(refused > 0, "{node} refused none of the impostor's calls");
        }
    }

    fleet.await_workloads(Duration::from_secs(120))?;
    await_placed_converged(&mut fleet, &db, &replicas)?;
    Ok(fleet.finish()?)
}

/// Both scenarios run four data hosts and seconds of writes, so CI runs them on the heavy
/// scenarios' fewer seeds; the nightly sweep covers many more.
#[test]
fn an_impostor_node_is_refused_on_databases_it_does_not_host() {
    seeds::run(BINARY, &seeds::FIXED_HEAVY, |seed| -> TestResult {
        attacked(seed)?;
        Ok(())
    });
}

/// The databases whose placement differs between `before` and `after`, two control states. A
/// membership change moves placement at once; the replica set promotions fence follows once the
/// primary reports that placement's write quorum holds its log (ADR-0031, amendment 2).
fn moved(
    before: &fleetring_control::State,
    after: &fleetring_control::State,
) -> Result<BTreeSet<DatabaseName>, Box<dyn Error>> {
    let mut moved = BTreeSet::new();
    for database in before.databases() {
        let name = database.name();
        if before.placed_replica_set(name)? != after.placed_replica_set(name)? {
            moved.insert(name.clone());
        }
    }
    Ok(moved)
}

fn joined(seed: u64) -> Result<Report, Box<dyn Error>> {
    const DATA: [&str; 3] = ["node-1", "node-2", "node-3"];
    const JOINING: &str = "node-4";
    let mut fleet = Fleet::builder(seed)
        .control(CONTROL)
        .data(DATA[0])
        .data(DATA[1])
        .data(DATA[2])
        .data(JOINING)
        .configure(JOINING, |config| {
            config.node.weight = Weight::new(4).unwrap_or(Weight::ONE);
        })
        .operation_timeout(Duration::from_secs(2))
        .build()?;
    // The fourth host can't reach the control node, so it joins only once the link heals.
    fleet.partition(JOINING, CONTROL)?;
    fleet.await_ring_on(&DATA, Duration::from_secs(5))?;
    fleet.client("writer", CreateAndWrite::new(10, 3, 300));
    fleet.run_until(Duration::from_secs(30), "every database written", |fleet| {
        fleet.history().acknowledged_writes().count() >= 30
    })?;
    let before = fleet.control_state().ok_or("the control node is down")?;
    assert_eq!(before.members().len(), DATA.len());

    fleet.repair(JOINING, CONTROL)?;
    fleet.run_until(
        Duration::from_secs(10),
        "the fourth host joining",
        |fleet| {
            fleet
                .control_state()
                .is_some_and(|state| state.members().len() == DATA.len() + 1)
        },
    )?;
    let after = fleet.control_state().ok_or("the control node is down")?;
    let moved = moved(&before, &after)?;
    let joining: NodeId = JOINING.parse()?;
    let database = moved
        .iter()
        .find(|name| {
            after
                .placed_replica_set(name)
                .is_ok_and(|set| set.contains(&joining))
        })
        .ok_or("the join moved no replica")?
        .clone();
    fleet.note(format!("{database} moves to {JOINING}"));

    fleet.await_workloads(Duration::from_secs(120))?;
    fleet.await_ring(Duration::from_secs(5))?;
    // The replica the join moved away keeps the copy it had; only the new replica set must
    // agree.
    let placed = after.placed_replica_set(&database)?;
    await_placed_converged(&mut fleet, &database, &placed)?;
    // Placement's write quorum holds the log, so the control plane releases the old set.
    let released = database.clone();
    fleet.run_until(
        Duration::from_secs(30),
        "the kept replica set released",
        |fleet| {
            fleet
                .control_state()
                .is_some_and(|state| state.replica_set(&released).ok().as_ref() == Some(&placed))
        },
    )?;
    let copy = fleet
        .replication(JOINING)
        .and_then(|replication| replication.follower_status(&database))
        .ok_or("the new member holds no copy")?;
    assert!(
        copy.position.is_some(),
        "{JOINING}'s copy of {database}: {copy:?}"
    );
    fleet.assert_no_binding_refusals()?;
    Ok(fleet.finish()?)
}

#[test]
fn a_membership_change_that_moves_a_replica_completes_without_refusals() {
    seeds::run(BINARY, &seeds::FIXED_HEAVY, |seed| -> TestResult {
        joined(seed)?;
        Ok(())
    });
}
