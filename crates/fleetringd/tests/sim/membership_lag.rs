//! #157's probe (FR-102): a primary's view of a membership change lags by one poll, so a write
//! acknowledged on the old replica set may miss the new set's promotion quorum.
//!
//! An ordinary database on three of four nodes, {P, A, B}, with R = 3 and W = 2. P is cut off from
//! the control node, so it keeps its view of the ring, and from A. A fifth node, X, joins, which
//! moves the replica set to {P, A, X}; B stays a member, and still follows P. Before P polls
//! again, it acknowledges a write that P and B hold. Then P crashes, the control node promotes a
//! quorum of {A, X}, and a read asks whether the acknowledged write survived.

use std::time::Duration;

use fleetring::ReplicationFactor;
use fleetring_control::{DatabaseName, Member, membership_ring};
use fleetringd::sim::{Ack, Fleet, Operation, Operator, Outcome, Script, Traffic, seeds};

use crate::{BINARY, TestResult};

const CONTROL: &str = "control";
const NODES: [&str; 4] = ["n1", "n2", "n3", "n4"];

/// The node whose join moves the replica set.
const JOINER: &str = "n5";
const LIMIT: Duration = Duration::from_secs(60);

/// The value the probe writes.
const PROBE: i64 = 157_157;

/// Runs `operations` on a client named `name`, each until answered, and returns the answers.
fn script(
    fleet: &mut Fleet,
    name: &str,
    operations: Vec<Operation>,
) -> Result<Vec<Outcome>, Box<dyn std::error::Error>> {
    let count = operations.len();
    fleet.client(name, Script::new(operations));
    let answers = |fleet: &Fleet| -> Vec<Outcome> {
        fleet
            .history()
            .records()
            .iter()
            .filter(|record| record.client() == name)
            .map(|record| record.outcome().clone())
            .filter(|outcome| !matches!(outcome, Outcome::Indeterminate { .. }))
            .collect()
    };
    fleet.run_until(LIMIT, &format!("{name} answered"), |fleet| {
        answers(fleet).len() >= count
    })?;
    Ok(answers(fleet))
}

/// A name whose replica set over the four nodes is {P, A, B} and, once [`JOINER`] joins, is
/// {P, A, JOINER}: returns it with P, A, and B.
fn moved_name(
    state: &fleetring_control::State,
) -> Result<(DatabaseName, String, String, String), Box<dyn std::error::Error>> {
    let four: Vec<Member> = state.members().cloned().collect();
    let mut five = four.clone();
    five.push(Member::new(JOINER.parse()?));
    let epoch = state.membership_epoch();
    let (before, after) = (
        membership_ring(&four, epoch)?,
        membership_ring(&five, epoch)?,
    );
    let factor = ReplicationFactor::new(3).ok_or("zero")?;
    for n in 0..500 {
        let name = format!("lag-{n}");
        let old: Vec<String> = before
            .replicas_n(&name, factor)
            .to_ids()
            .iter()
            .map(ToString::to_string)
            .collect();
        let new: Vec<String> = after
            .replicas_n(&name, factor)
            .to_ids()
            .iter()
            .map(ToString::to_string)
            .collect();
        let lost: Vec<&String> = old.iter().filter(|node| !new.contains(node)).collect();
        if new.iter().any(|node| node == JOINER) && lost.len() == 1 && *lost[0] != old[0] {
            let primary = old[0].clone();
            let b = lost[0].clone();
            let a = old
                .iter()
                .find(|node| **node != primary && **node != b)
                .cloned()
                .ok_or("no A")?;
            return Ok((name.parse()?, primary, a, b));
        }
    }
    Err("no name the join moves as the probe needs".into())
}

fn scenario(seed: u64) -> TestResult {
    let mut builder = Fleet::builder(seed).control(CONTROL);
    for node in NODES.iter().chain([&JOINER]) {
        builder = builder.data(node);
    }
    let mut fleet = builder.build()?;
    // The fifth node joins only once the database lives on the other four.
    fleet.partition(JOINER, CONTROL)?;
    fleet.await_ring_on(&NODES, Duration::from_secs(10))?;
    let state = fleet.control_state().ok_or("the control node is down")?;
    let (database, primary, a, b) = moved_name(&state)?;
    let (traffic, handle) = Traffic::new([(database.clone(), 3)], 0);
    fleet.client("traffic", traffic);
    let operator = Operator::new();
    fleet.client("operator", operator.clone());
    let watched = handle.clone();
    let name = database.clone();
    fleet.run_until(LIMIT, "writes to the database", |_| {
        watched.acknowledged(&name) >= 20
    })?;
    handle.stop();
    fleet.run_for(Duration::from_millis(300))?;
    let state = fleet.control_state().ok_or("the control node is down")?;
    let leader = state
        .database(&database)
        .and_then(|db| db.lease())
        .map(|lease| lease.holder().to_string());
    if leader.as_deref() != Some(primary.as_str()) {
        return Err(format!("{database} is led by {leader:?}, not {primary}").into());
    }
    fleet.note(format!(
        "{database} on {primary}, {a}, {b}; {JOINER} joins and displaces {b}"
    ));

    // P keeps its view of the ring and can't reach A; X joins.
    fleet.partition(&primary, CONTROL)?;
    fleet.partition(&primary, &a)?;
    fleet.repair(JOINER, CONTROL)?;
    let moved = database.clone();
    let lost = b.clone();
    fleet.run_until(LIMIT, "the replica set moving", |fleet| {
        fleet.control_state().is_some_and(|state| {
            state
                .replica_set(&moved)
                .is_ok_and(|set| set.iter().all(|node| node.as_str() != lost))
        })
    })?;
    // Before P polls again: a write P and B hold.
    let answers = script(
        &mut fleet,
        "probe",
        vec![Operation::Write {
            database: database.clone(),
            value: PROBE,
        }],
    )?;
    let acknowledged = matches!(
        answers.first(),
        Some(Outcome::Acknowledged(Ack::Written { .. }))
    );
    fleet.note(format!("the probe write: {:?}", answers.first()));
    fleet.crash(&primary)?;
    fleet.repair(&primary, &a)?;
    let promoted = database.clone();
    let old = primary.clone();
    fleet.run_until(LIMIT, "a promotion", |fleet| {
        fleet.control_state().is_some_and(|state| {
            state
                .database(&promoted)
                .and_then(|db| db.lease())
                .is_some_and(|lease| lease.holder().as_str() != old)
        })
    })?;
    let read = script(
        &mut fleet,
        "reader",
        vec![Operation::Read {
            database: database.clone(),
            value: Some(PROBE),
        }],
    )?;
    let survived = matches!(read.first(), Some(Outcome::Acknowledged(Ack::Read { values })) if values.contains(&PROBE));
    if acknowledged && !survived {
        return Err(format!(
            "#157 reproduced: write {PROBE} was acknowledged by {primary} with {b} on the old replica set, and is gone after the promotion ({:?})",
            read.first()
        )
        .into());
    }
    fleet.repair(&primary, CONTROL)?;
    fleet.restart(&primary)?;
    fleet.await_workloads(LIMIT)?;
    fleet.finish()?;
    Ok(())
}

/// Reproduced on seed 157: the write is acknowledged by P and B, and the promoted node of {A, X}
/// doesn't hold it.
#[test]
#[ignore = "#157"]
fn a_write_acknowledged_on_the_old_replica_set_survives_a_promotion() {
    seeds::run(BINARY, &[157], scenario);
}
