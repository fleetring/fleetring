//! A follower seeded by snapshot from a primary that held an unacknowledged write, which then
//! follows a new primary that never had it, keeps a hash chain that agrees with the new
//! primary's (FR-044, ADR-0018 point 5, ADR-0022).
//!
//! An ordinary database on all five of five nodes, {P, A, B, C, X}, with R = 5 and W = 3, so
//! that a promotion fences three. X can't reach P while the database is created and written to.
//! Then P is cut off from A, B, and C, and takes a write no quorum can acknowledge. P reaches X,
//! which has no copy yet, so X is seeded from a snapshot of P's log, the unacknowledged write
//! included; P and X are two of the three a write quorum needs. X is cut off and P crashes, so
//! the promotion fences A, B, and C, none of which holds the write. X then follows the new
//! primary: it truncates the write from its copy, so its rows match, and its chain must match
//! too, before the new primary writes again: `db verify` compares the replicas at the position
//! they all hold, and the history checker's `hash chains agree` at every position.

use std::error::Error;
use std::time::Duration;

use fleetring::ReplicationFactor;
use fleetring_control::{DatabaseName, Member, membership_ring};
use fleetringd::sim::{Ack, DurabilityMode, Fleet, Operation, Operator, Outcome, Script, seeds};

use crate::{BINARY, TestResult};

const CONTROL: &str = "control";
const NODES: [&str; 5] = ["n1", "n2", "n3", "n4", "n5"];
const LIMIT: Duration = Duration::from_secs(60);

/// The value of the write no quorum acknowledges.
const AHEAD: i64 = 9_999;

/// How many writes every replica set member but X takes before the write ahead.
const WRITES: i64 = 5;

fn database() -> Result<DatabaseName, Box<dyn Error>> {
    Ok("seeded".parse()?)
}

/// The primary and the last replica of [`database`]'s replica set over all five nodes.
fn primary_and_last(fleet: &Fleet) -> Result<(String, String), Box<dyn Error>> {
    let state = fleet.control_state().ok_or("the control node is down")?;
    let members: Vec<Member> = state.members().cloned().collect();
    let ring = membership_ring(&members, state.membership_epoch())?;
    let factor = ReplicationFactor::new(5).ok_or("zero")?;
    let replicas: Vec<String> = ring
        .replicas_n(database()?.as_str(), factor)
        .to_ids()
        .iter()
        .map(ToString::to_string)
        .collect();
    let first = replicas.first().cloned().ok_or("no replicas")?;
    let last = replicas.last().cloned().ok_or("no replicas")?;
    Ok((first, last))
}

/// The node the control plane names as the database's primary, while one serves.
fn leader(fleet: &Fleet) -> Option<String> {
    let state = fleet.control_state()?;
    let record = state.database(&database().ok()?)?;
    let lease = record.lease()?;
    (lease.epoch() == record.epoch()).then(|| lease.holder().to_string())
}

/// Whether `primary` streams to `follower`, which reported its position and needs no snapshot.
fn follows(fleet: &Fleet, primary: &str, follower: &str) -> bool {
    database().ok().is_some_and(|database| {
        fleet
            .replication(primary)
            .and_then(|replication| replication.primary_status(&database))
            .is_some_and(|status| {
                status.followers.iter().any(|progress| {
                    progress.node.as_str() == follower
                        && progress.connected
                        && progress.reported
                        && !progress.needs_snapshot
                })
            })
    })
}

fn scenario(seed: u64) -> TestResult {
    let mut builder = Fleet::builder(seed).control(CONTROL);
    for node in NODES {
        builder = builder.data(node);
    }
    let mut fleet = builder.build()?;
    fleet.await_ring(Duration::from_secs(10))?;
    let database = database()?;
    let (primary, seeded) = primary_and_last(&fleet)?;
    let others: Vec<&str> = NODES
        .into_iter()
        .filter(|node| *node != primary && *node != seeded)
        .collect();

    // X gets no copy while the database is created and written to.
    fleet.partition(&primary, &seeded)?;
    let mut operations = vec![Operation::Create {
        database: database.clone(),
        replication_factor: 5,
        durability: DurabilityMode::Quorum,
    }];
    operations.extend((0..WRITES).map(|value| Operation::Write {
        database: database.clone(),
        value,
    }));
    fleet.client("writer", Script::new(operations));
    fleet.await_workloads(LIMIT)?;
    if leader(&fleet).as_deref() != Some(primary.as_str()) {
        return Err(format!("{database} is led by {:?}, not {primary}", leader(&fleet)).into());
    }
    fleet.note(format!(
        "{database} led by {primary}; {seeded} has no copy; the promotion will fence {others:?}"
    ));

    // A write only P holds, sent once.
    for other in &others {
        fleet.partition(&primary, other)?;
    }
    let ahead = Operator::new();
    fleet.client("ahead", ahead.clone());
    ahead.push(Operation::Write {
        database: database.clone(),
        value: AHEAD,
    });
    fleet.run_until(LIMIT, "the write ahead answered", |fleet| {
        fleet
            .history()
            .records()
            .iter()
            .any(|record| record.client() == "ahead")
    })?;

    // X is seeded from P's snapshot, the write ahead included: P and X are two of three.
    fleet.repair(&primary, &seeded)?;
    fleet.run_until(LIMIT, "X seeded from P", |fleet| {
        follows(fleet, &primary, &seeded)
    })?;
    let rows = fleet.replica_rows(&database)?;
    if !rows.get(&seeded).is_some_and(|rows| rows.contains(&AHEAD)) {
        return Err(format!("{seeded} wasn't seeded with the write ahead: {rows:?}").into());
    }
    fleet.note(format!("{seeded} seeded from {primary}'s snapshot"));

    // X is cut off and P crashes: the promotion fences A, B, and C.
    for other in others.iter().copied().chain([CONTROL]) {
        fleet.partition(&seeded, other)?;
    }
    fleet.crash(&primary)?;
    let old = primary.clone();
    fleet.run_until(LIMIT, "a promotion", |fleet| {
        leader(fleet).is_some_and(|leader| leader != old)
    })?;
    let promoted = leader(&fleet).ok_or("no leader")?;
    fleet.note(format!("{promoted} promoted"));

    // X follows the new primary, which takes no write meanwhile.
    for other in others.iter().copied().chain([CONTROL]) {
        fleet.repair(&seeded, other)?;
    }
    fleet.run_until(LIMIT, "X following the new primary", |fleet| {
        follows(fleet, &promoted, &seeded)
    })?;
    fleet.run_for(Duration::from_secs(1))?;
    let rows = fleet.replica_rows(&database)?;
    if rows.get(&seeded) != rows.get(&promoted) {
        return Err(format!("{seeded}'s rows differ from {promoted}'s: {rows:?}").into());
    }
    fleet.check_chains()?;

    let answered = fleet
        .history()
        .records()
        .iter()
        .filter(|record| record.client() == "ahead")
        .map(|record| record.outcome().clone())
        .next();
    if let Some(Outcome::Acknowledged(Ack::Written { .. })) = answered {
        return Err("the write ahead was acknowledged".into());
    }
    // P comes back and truncates the write ahead too.
    for other in &others {
        fleet.repair(&primary, other)?;
    }
    fleet.restart(&primary)?;
    fleet.await_ring(LIMIT)?;
    fleet.await_replicas_converged(LIMIT)?;
    fleet.finish()?;
    Ok(())
}

/// Before the fix, X's chain record sat at the snapshot's end, past where X truncated to, so X
/// derived its chain from where its log begins and disagreed with every other replica at the
/// first frame until the new primary's next batch. The stream's open now carries the primary's
/// hash at the epoch's start, which X takes.
#[test]
fn a_follower_seeded_with_an_unacknowledged_write_agrees_with_the_new_primarys_chain() {
    seeds::run(BINARY, &seeds::FIXED, scenario);
}
