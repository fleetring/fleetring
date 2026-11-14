//! #157's probe (FR-102): a primary's view of a membership change lags by one poll, so a write
//! acknowledged on the old replica set may miss the new set's promotion quorum.
//!
//! An ordinary database on three of four nodes, {P, A, B}, with R = 3 and W = 2. P is cut off from
//! the control node, so it keeps its view of the ring, and from A. A fifth node, X, joins, which
//! moves placement to {P, A, X}; B stays a member, and still follows P. Before P polls again, it
//! acknowledges a write that P and B hold. Then P crashes, and a read asks whether the
//! acknowledged write survived the promotion.
//!
//! Before the fix the promotion fenced a quorum of {A, X}, neither of which held the write. The
//! join now keeps {P, A, B} as the replica set promotions fence, so the promotion fences A and B,
//! and B holds the write; once the new primary reports that placement's write quorum holds its
//! log, the control plane releases {P, A, B} and the database follows placement (ADR-0031,
//! amendment 2). The history checker's `AcknowledgedWritesFenced` property catches the loss
//! whatever the read says.

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

pub(crate) fn scenario(seed: u64) -> TestResult {
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
    // Placement moves at once; the replica set promotions fence stays {P, A, B} until P
    // reports that placement's write quorum holds its log, which P, cut off, can't.
    fleet.run_until(LIMIT, "placement moving", |fleet| {
        fleet.control_state().is_some_and(|state| {
            state
                .placed_replica_set(&moved)
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
    // The history checker holds every acknowledged write to a write quorum of the replica set
    // the promotion fences (`AcknowledgedWritesFenced`).
    fleet.check_history()?;
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
    fleet.await_ring_on(&[primary.as_str()], LIMIT)?;
    // The new primary counts both write quorums; once placement's holds its log, the control
    // plane releases the kept set and the database follows placement.
    let released = database.clone();
    fleet.run_until(LIMIT, "the kept replica set released", |fleet| {
        fleet.control_state().is_some_and(|state| {
            state
                .database(&released)
                .is_some_and(|db| db.kept_replicas().is_none())
                && state.replica_set(&released).ok() == state.placed_replica_set(&released).ok()
        })
    })?;
    fleet.await_workloads(LIMIT)?;
    fleet.finish()?;
    Ok(())
}

/// Runs `operations` on a client named `name` and returns every answer once each arrived, a
/// timeout included. The client goes on sending an operation whose answer was uncertain.
fn answers_of(
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
            .collect()
    };
    fleet.run_until(LIMIT, &format!("{name} answered"), |fleet| {
        answers(fleet).len() >= count
    })?;
    Ok(answers(fleet))
}

/// The upgrade path (FR-102's review, finding 1): data nodes are upgraded before the control
/// node, so a primary may start leading under a control node that shows no replica sets. It
/// counts placement by the ring pinned at the last answer; once the control node shows sets,
/// the first membership change keeps the old set, and the primary, reading control state again
/// when its ring moves, counts it too. Before the fix it counted its new ring alone, so a write
/// acknowledged by P and the joiner was lost when the promotion fenced A and B.
#[cfg(feature = "fault-hooks")]
pub(crate) fn upgraded_control_node(seed: u64) -> TestResult {
    let mut builder = Fleet::builder(seed).control(CONTROL);
    for node in NODES.iter().chain([&JOINER]) {
        builder = builder.data(node);
    }
    let mut fleet = builder.build()?;
    let control = fleet
        .fork_faults(CONTROL)
        .ok_or("the control host has no hooks")?;
    // The control node answers as 0.7 does until the test upgrades it.
    control.hide_replica_sets(true);
    fleet.partition(JOINER, CONTROL)?;
    fleet.await_ring_on(&NODES, Duration::from_secs(10))?;
    let state = fleet.control_state().ok_or("the control node is down")?;
    let (database, primary, a, b) = moved_name(&state)?;
    let (traffic, handle) = Traffic::new([(database.clone(), 3)], 0);
    fleet.client("traffic", traffic);
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
        "{database} on {primary}, {a}, {b}, led under a control node that shows no replica sets; {JOINER} joins once it shows them"
    ));

    // The control node is upgraded. P reaches it, but neither A, nor B, nor yet the joiner, so
    // that placement can't catch up, and P can't report, before P is cut off from the control
    // node too.
    control.hide_replica_sets(false);
    fleet.partition(&primary, &a)?;
    fleet.partition(&primary, &b)?;
    fleet.partition(&primary, JOINER)?;
    // A write no replica can acknowledge leaves P's log ahead of every follower's, so that
    // placement doesn't hold it yet.
    let ahead = answers_of(
        &mut fleet,
        "ahead",
        vec![Operation::Write {
            database: database.clone(),
            value: PROBE - 1,
        }],
    )?;
    fleet.note(format!("the write ahead: {:?}", ahead.first()));
    fleet.repair(JOINER, CONTROL)?;
    let moved = database.clone();
    let lost = b.clone();
    fleet.run_until(LIMIT, "placement moving", |fleet| {
        fleet.control_state().is_some_and(|state| {
            state
                .placed_replica_set(&moved)
                .is_ok_and(|set| set.iter().all(|node| node.as_str() != lost))
                && state
                    .database(&moved)
                    .is_some_and(|db| db.kept_replicas().is_some())
        })
    })?;
    // P publishes the new ring and reads control state again; then it is cut off from the
    // control node, so that no report of its arrives and the set stays kept, and reaches the
    // joiner, which catches up.
    fleet.await_ring_on(&[primary.as_str(), JOINER], LIMIT)?;
    fleet.run_for(Duration::from_secs(1))?;
    fleet.partition(&primary, CONTROL)?;
    fleet.repair(&primary, JOINER)?;
    fleet.run_for(Duration::from_secs(2))?;
    let answers = answers_of(
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
    fleet.repair(&primary, &b)?;
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
    fleet.check_history()?;
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
            "write {PROBE} was acknowledged by {primary}, which led under a control node that showed no replica sets, and is gone after the promotion ({:?})",
            read.first()
        )
        .into());
    }
    fleet.repair(&primary, CONTROL)?;
    fleet.restart(&primary)?;
    fleet.await_ring_on(&[primary.as_str()], LIMIT)?;
    let released = database.clone();
    fleet.run_until(LIMIT, "the kept replica set released", |fleet| {
        fleet.control_state().is_some_and(|state| {
            state
                .database(&released)
                .is_some_and(|db| db.kept_replicas().is_none())
        })
    })?;
    fleet.await_workloads(LIMIT)?;
    fleet.finish()?;
    Ok(())
}

/// A primary that started leading under a control node that shows no replica sets, as data
/// nodes upgraded before their control node do, never counts a newer ring than the control
/// node last answered for, so the first membership change after the control node's upgrade
/// loses no acknowledged write (FR-102's review, finding 1).
#[cfg(feature = "fault-hooks")]
#[test]
fn a_primary_started_under_an_older_control_node_counts_the_kept_set() {
    seeds::run(BINARY, &[1, 24, 29, 157, 2_026], upgraded_control_node);
}

/// Reproduced on seed 157 before the fix: the write was acknowledged by P and B, and the
/// promotion fenced {A, X}, neither of which held it. A membership change now keeps the old
/// replica set as the one promotions fence until the primary reports that the new set's write
/// quorum holds its log (ADR-0031, amendment 2).
#[test]
fn a_write_acknowledged_on_the_old_replica_set_survives_a_promotion() {
    seeds::run(BINARY, &[1, 24, 29, 157, 2_026], scenario);
}
