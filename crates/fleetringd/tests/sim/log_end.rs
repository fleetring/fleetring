//! A primary that can't read where its log ends when it starts serving (FR-106).
//!
//! An ordinary database on three of four nodes, R = 3, takes writes until its followers hold
//! them all, and the writes stop. Every data node's reads of its log's end then fail, and the
//! primary crashes, so the promoted primary starts serving without knowing where its log ends.
//! A fifth node joins and moves the replica set, so control state keeps the old one until the
//! primary reports that the new one's write quorum holds its log.
//!
//! An unknown end once read as an empty log: the new primary's hash-chain build, which reads the
//! end too, failed, so no hash proved a follower's position, and every follower that held a copy
//! was past the "empty" log, a copy of another log, and was re-seeded on every stream, again and
//! again, until a write. Now the primary reads the end again until it can, and opens no stream,
//! judges no follower, and reports no hand-over before it knows it: nothing is re-seeded and the
//! kept set stays kept. Once the reads succeed, the hand-over completes and nothing acknowledged
//! is lost.

use std::time::Duration;

use fleetring::ReplicationFactor;
use fleetring_control::{DatabaseName, Member, membership_ring};
use fleetringd::DatabaseStatuses as _;
use fleetringd::sim::{Fleet, Traffic, seeds};

use crate::{BINARY, TestResult};

const CONTROL: &str = "control";
const NODES: [&str; 4] = ["n1", "n2", "n3", "n4"];
const JOINER: &str = "n5";
const LIMIT: Duration = Duration::from_secs(60);

/// How long the reads fail after the promotion: many retry intervals, in which the loop re-seeded
/// each follower again and again.
const FAILING: Duration = Duration::from_secs(5);

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

/// The first name `end-N` the join of [`JOINER`] moves.
fn moved_name(fleet: &Fleet) -> Result<DatabaseName> {
    let state = fleet.control_state().ok_or("the control node is down")?;
    let now: Vec<Member> = state.members().cloned().collect();
    let mut joined = now.clone();
    joined.push(Member::new(JOINER.parse()?));
    let epoch = state.membership_epoch();
    let (before, after) = (
        membership_ring(&now, epoch)?,
        membership_ring(&joined, epoch)?,
    );
    let factor = ReplicationFactor::new(3).ok_or("zero")?;
    for n in 0..500 {
        let name = format!("end-{n}");
        let moves = after
            .replicas_n(&name, factor)
            .to_ids()
            .iter()
            .any(|node| node.as_str() == JOINER);
        if moves
            && before.replicas_n(&name, factor).to_ids() != after.replicas_n(&name, factor).to_ids()
        {
            return Ok(name.parse()?);
        }
    }
    Err("no end-N is placed as the scenario needs".into())
}

/// The holder of `database`'s lease now.
fn leader(fleet: &Fleet, database: &DatabaseName) -> Option<String> {
    fleet
        .control_state()?
        .database(database)?
        .lease()
        .map(|lease| lease.holder().to_string())
}

/// Whether control state keeps a replica set for `database`.
fn keeps(fleet: &Fleet, database: &DatabaseName) -> bool {
    fleet
        .control_state()
        .and_then(|state| {
            state
                .database(database)
                .map(|db| db.kept_replicas().is_some())
        })
        .unwrap_or(false)
}

/// How many re-seeds `host` has started since it last started.
fn reseeds(fleet: &Fleet, host: &str) -> Result<u64> {
    let exposition = fleet.metrics(host).ok_or(format!("{host} isn't running"))?;
    Ok(exposition
        .lines()
        .find_map(|line| {
            line.strip_prefix("fleetring_replication_reseeds_total")?
                .strip_prefix(' ')
        })
        .and_then(|value| value.parse().ok())
        .unwrap_or(0))
}

/// Makes every read of a primary's log end fail on every data node, or, with `count` 0, lets
/// them run.
fn fail_reads(fleet: &Fleet, count: u32) -> Result<()> {
    for host in NODES.iter().chain([&JOINER]) {
        fleet
            .fork_faults(host)
            .ok_or(format!("no fault hooks on {host}"))?
            .fail_next_position_reads(count);
    }
    Ok(())
}

fn scenario(seed: u64) -> TestResult {
    let mut builder = Fleet::builder(seed).control(CONTROL);
    for node in NODES.iter().chain([&JOINER]) {
        builder = builder.data(node);
    }
    let mut fleet = builder.build()?;
    fleet.partition(JOINER, CONTROL)?;
    fleet.await_ring_on(&NODES, Duration::from_secs(10))?;
    let database = moved_name(&fleet)?;
    let (traffic, handle) = Traffic::new([(database.clone(), 3)], 0);
    fleet.client("traffic", traffic);
    let watched = handle.clone();
    let name = database.clone();
    fleet.run_until(LIMIT, "writes to the database", |_| {
        watched.acknowledged(&name) >= 20
    })?;
    // No write may tell the next primary where its log ends.
    handle.stop();
    fleet.await_workloads(LIMIT)?;
    fleet.run_until(LIMIT, "the followers caught up", Fleet::replicas_caught_up)?;
    let primary = leader(&fleet, &database).ok_or("no primary")?;

    fail_reads(&fleet, u32::MAX)?;
    fleet.crash(&primary)?;
    let old = primary.clone();
    let name = database.clone();
    fleet.run_until(LIMIT, "a promotion", |fleet| {
        leader(fleet, &name).is_some_and(|holder| holder != old)
    })?;
    let promoted = leader(&fleet, &database).ok_or("no primary")?;
    fleet.note(format!(
        "{database}: {primary} crashed; {promoted} serves it, and can't read where its log ends"
    ));
    fleet.repair(JOINER, CONTROL)?;
    let name = database.clone();
    fleet.run_until(LIMIT, "a kept replica set", |fleet| keeps(fleet, &name))?;
    fleet.run_for(FAILING)?;
    let reported = fleet
        .replication(&promoted)
        .ok_or("the promoted primary isn't running")?
        .statuses()
        .into_iter()
        .find(|status| status.database == database.as_str());
    if let Some(status) = reported {
        return Err(format!(
            "{promoted} reported {database} while it couldn't read where its log ends: {status:?}"
        )
        .into());
    }
    let reseeded = reseeds(&fleet, &promoted)?;
    let released = !keeps(&fleet, &database);
    if reseeded > 0 || released {
        return Err(format!(
            "while {promoted} couldn't read where the log of {database} ends, it re-seeded its \
             followers {reseeded} times, and control state released the kept replica set: \
             {released}"
        )
        .into());
    }

    fail_reads(&fleet, 0)?;
    let name = database.clone();
    fleet.run_until(LIMIT, "the kept replica set released", |fleet| {
        !keeps(fleet, &name)
    })?;
    fleet.check_history()?;
    fleet.restart(&primary)?;
    fleet.await_ring_on(&[primary.as_str()], LIMIT)?;
    fleet.finish()?;
    Ok(())
}

#[test]
fn a_primary_that_cant_read_its_log_end_judges_no_follower() {
    seeds::run(BINARY, &seeds::FIXED, scenario);
}
