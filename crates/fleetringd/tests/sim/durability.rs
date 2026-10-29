//! FR-035 on the harness: each `acks` mode keeps its own guarantee under faults (ADR-0023).
//!
//! One control host and three data hosts. A client creates databases through the data plane in
//! the mode under test and writes to them, while the test crashes and isolates nodes. The history
//! checker in [`Cluster::finish`] holds each database to the guarantee its creation reported:
//! `quorum` and `all` never lose an acknowledged write; `local` may, across a failover, and is
//! held only to single-writer and no-phantom-write invariants.

use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::time::Duration;

use fleetring::ReplicationFactor;
use fleetring_control::{Acks, DatabaseName};
use fleetringd::sim::check::Invariant;
use fleetringd::sim::history::{self, Guarantee, Replicated};
use fleetringd::sim::{
    Ack, Cluster, CreateAndWrite, DurabilityMode, Operation, Outcome, Timings, seeds,
};

use crate::{BINARY, TestResult};

const CONTROL: &str = "control";
const DATA: [&str; 3] = ["node-1", "node-2", "node-3"];
const CLIENT: &str = "writer";

fn database(index: usize) -> Result<DatabaseName, Box<dyn Error>> {
    Ok(format!("db-{index}").parse()?)
}

fn cluster(seed: u64) -> Result<Cluster, Box<dyn Error>> {
    Ok(Cluster::builder(seed)
        .control(CONTROL)
        .data(DATA[0])
        .data(DATA[1])
        .data(DATA[2])
        .timings(Timings::default())
        .operation_timeout(Duration::from_millis(500))
        .build()?)
}

/// The node the control plane names as `database`'s primary, while one serves.
fn primary(cluster: &Cluster, database: &DatabaseName) -> Option<String> {
    let state = cluster.control_state()?;
    let record = state.database(database)?;
    let lease = record.lease()?;
    (lease.epoch() == record.epoch()).then(|| lease.holder().to_string())
}

/// Every acknowledged write to `database`, as (when it completed, the primary, its value).
fn acknowledged_writes(cluster: &Cluster, database: &DatabaseName) -> Vec<(Duration, String, i64)> {
    cluster
        .history()
        .records()
        .iter()
        .filter_map(|record| match (record.operation(), record.outcome()) {
            (
                Operation::Write {
                    database: written,
                    value,
                },
                Outcome::Acknowledged(Ack::Written {
                    primary: Some(primary),
                    ..
                }),
            ) if written == database => Some((record.completed_at(), primary.to_string(), *value)),
            _ => None,
        })
        .collect()
}

/// Runs until a node other than `old` acknowledges a write to `database` after `since`.
fn await_new_primary(
    cluster: &mut Cluster,
    database: &DatabaseName,
    old: &str,
    since: Duration,
    limit: Duration,
) -> Result<(), Box<dyn Error>> {
    cluster.run_until(limit, "writes moving to a new primary", |cluster| {
        acknowledged_writes(cluster, database)
            .iter()
            .any(|(at, primary, _)| *at > since && primary != old)
    })?;
    Ok(())
}

/// Starts the writer and waits until a write to `db-0` is acknowledged.
fn start_writing(
    cluster: &mut Cluster,
    workload: CreateAndWrite,
) -> Result<String, Box<dyn Error>> {
    cluster.await_ring(Duration::from_secs(30))?;
    cluster.client(CLIENT, workload);
    let first = database(0)?;
    cluster.run_until(Duration::from_secs(30), "writes acknowledged", |cluster| {
        !acknowledged_writes(cluster, &first).is_empty()
    })?;
    Ok(primary(cluster, &first).ok_or("no primary")?)
}

/// Heals, lets the writer finish, and runs the final checks.
fn heal_and_finish(mut cluster: Cluster) -> Result<fleetringd::sim::Report, Box<dyn Error>> {
    cluster.await_workloads(Duration::from_secs(300))?;
    cluster.await_ring(Duration::from_secs(60))?;
    cluster.await_replicas_converged(Duration::from_secs(60))?;
    Ok(cluster.finish()?)
}

/// A `local` primary cut off from its followers keeps acknowledging writes, since W = 1. When
/// the lease expires and a follower takes over, that tail is lost: the checker lets it pass for
/// a `local` database, and would flag the same history for a `quorum` one.
fn local_loses_a_cut_off_primarys_tail(seed: u64) -> TestResult {
    let timings = Timings::default();
    let mut cluster = cluster(seed)?;
    let db = database(0)?;
    let old = start_writing(
        &mut cluster,
        CreateAndWrite::new(1, 3, 400).with_durability(DurabilityMode::Local),
    )?;
    cluster.run_for(Duration::from_secs(1))?;
    let others: Vec<&str> = DATA
        .into_iter()
        .filter(|node| *node != old.as_str())
        .collect();
    for other in &others {
        cluster.partition(&old, other)?;
    }
    // The control node still reaches the primary: no failover yet, and the primary alone
    // acknowledges every write.
    let cut_at = cluster.elapsed();
    cluster.run_for(timings.lease_duration / 2)?;
    let during_cut: BTreeSet<i64> = acknowledged_writes(&cluster, &db)
        .into_iter()
        .filter(|(at, _, _)| *at > cut_at + Duration::from_millis(100))
        .map(|(_, _, value)| value)
        .collect();
    assert!(
        !during_cut.is_empty(),
        "a local primary cut off from its followers acknowledged nothing"
    );

    // Cut it off from the control node too: the lease expires and a follower takes over,
    // adopting the most advanced log among the fenced majority, which lacks the tail.
    cluster.partition(&old, CONTROL)?;
    let since = cluster.elapsed();
    let failover = timings.lease_duration + timings.failover_margin + Duration::from_secs(2);
    await_new_primary(&mut cluster, &db, &old, since, failover * 2)?;
    for other in others.iter().copied().chain([CONTROL]) {
        cluster.repair(&old, other)?;
    }
    cluster.await_workloads(Duration::from_secs(300))?;
    cluster.await_ring(Duration::from_secs(60))?;
    cluster.await_replicas_converged(Duration::from_secs(60))?;

    let rows = cluster.replica_rows(&db)?;
    let everywhere: BTreeSet<i64> = rows.values().flatten().copied().collect();
    let lost: Vec<&i64> = during_cut
        .iter()
        .filter(|value| !everywhere.contains(value))
        .collect();
    cluster.note(format!(
        "{} of {} writes acknowledged during the cut were lost",
        lost.len(),
        during_cut.len()
    ));
    assert!(
        !lost.is_empty(),
        "no acknowledged write was lost, so the scenario didn't exercise local's trade-off"
    );
    // The same history and rows, held to `quorum`, is a durability violation.
    let three = ReplicationFactor::new(3).ok_or("three")?;
    let history = cluster.history();
    let as_quorum = BTreeMap::from([(
        db.clone(),
        Replicated::new(Guarantee::quorum(three), rows.clone()),
    )]);
    let violation =
        history::check(&history, &as_quorum).expect_err("a quorum database lost a write");
    assert!(
        matches!(
            violation.invariant(),
            Invariant::AcknowledgedWritesReplicated
                | Invariant::AcknowledgedWritesRead
                | Invariant::MonotonicReads
        ),
        "{violation}"
    );
    let as_local = BTreeMap::from([(
        db.clone(),
        Replicated::new(Guarantee::new(three, DurabilityMode::Local), rows),
    )]);
    history::check(&history, &as_local)?;
    let report = cluster.finish()?;
    let record = report.state().database(&db).ok_or("lost database")?;
    assert_eq!(record.durability().acks(), Acks::Local);
    Ok(())
}

#[test]
fn a_local_database_may_lose_acknowledged_writes_on_failover_and_only_then_passes() {
    seeds::run(BINARY, &[31], local_loses_a_cut_off_primarys_tail);
}

/// In `all`, every replica must hold a write before it is acknowledged, and the floor is R:
/// while a follower is down, nothing is acknowledged; once it is back, writes resume and every
/// acknowledged write is on all three replicas.
fn all_blocks_writes_while_a_replica_is_down(seed: u64) -> TestResult {
    let timings = Timings::default();
    let mut cluster = cluster(seed)?;
    let db = database(0)?;
    let primary = start_writing(
        &mut cluster,
        CreateAndWrite::new(1, 3, 300).with_durability(DurabilityMode::All),
    )?;
    let follower = DATA
        .into_iter()
        .find(|node| *node != primary.as_str())
        .ok_or("a follower")?;
    cluster.run_for(Duration::from_millis(500))?;
    cluster.crash(follower)?;
    let crashed_at = cluster.elapsed();
    // The primary notices within the heartbeat timeout; after that nothing is acknowledged.
    let grace = timings.heartbeat_timeout + Duration::from_millis(500);
    cluster.run_for(grace + Duration::from_secs(2))?;
    let blocked = acknowledged_writes(&cluster, &db)
        .into_iter()
        .filter(|(at, _, _)| *at > crashed_at + grace)
        .count();
    assert_eq!(
        blocked, 0,
        "an `all` database acknowledged writes with a replica down"
    );
    cluster.restart(follower)?;
    let restarted_at = cluster.elapsed();
    cluster.run_until(Duration::from_secs(30), "writes resuming", |cluster| {
        acknowledged_writes(cluster, &db)
            .iter()
            .any(|(at, _, _)| *at > restarted_at)
    })?;
    let report = heal_and_finish(cluster)?;
    for (operation, ack) in report.history().acknowledged() {
        if let (
            Operation::Write { .. },
            Ack::Written {
                acknowledged_by, ..
            },
        ) = (operation, ack)
        {
            assert_eq!(*acknowledged_by, 3, "{operation}: {ack}");
        }
    }
    Ok(())
}

#[test]
fn an_all_database_acknowledges_nothing_while_a_replica_is_down() {
    seeds::run(BINARY, &[31], all_blocks_writes_while_a_replica_is_down);
}

/// Two databases in one cluster, one `quorum` and one `local`, under a primary crash and a
/// partition: the checker holds each to its own guarantee, from the mode its creation reported.
fn mixed_modes_keep_their_own_guarantees(seed: u64) -> TestResult {
    let timings = Timings::default();
    let mut cluster = cluster(seed)?;
    let workload = CreateAndWrite::new(2, 3, 300).with_durability_for(1, DurabilityMode::Local);
    let first = start_writing(&mut cluster, workload)?;
    cluster.run_for(Duration::from_secs(1))?;
    let failover = timings.lease_duration + timings.failover_margin + Duration::from_secs(2);

    // Crash the first database's primary, and bring it back once writes have moved.
    cluster.crash(&first)?;
    let since = cluster.elapsed();
    await_new_primary(&mut cluster, &database(0)?, &first, since, failover * 2)?;
    cluster.restart(&first)?;
    cluster.run_for(Duration::from_secs(1))?;
    cluster.check_history()?;

    // Isolate whichever node leads the local database from everything, then heal.
    let local = database(1)?;
    let isolated = primary(&cluster, &local).ok_or("no primary for db-1")?;
    let others: Vec<&str> = DATA
        .into_iter()
        .filter(|node| *node != isolated.as_str())
        .collect();
    for other in others.iter().copied().chain([CONTROL]) {
        cluster.partition(&isolated, other)?;
    }
    let since = cluster.elapsed();
    await_new_primary(&mut cluster, &local, &isolated, since, failover * 2)?;
    for other in others.iter().copied().chain([CONTROL]) {
        cluster.repair(&isolated, other)?;
    }
    let report = heal_and_finish(cluster)?;

    let modes: BTreeMap<DatabaseName, DurabilityMode> = report
        .history()
        .acknowledged()
        .filter_map(|(operation, ack)| match (operation, ack) {
            (Operation::Create { database, .. }, Ack::Opened { durability, .. }) => {
                Some((database.clone(), *durability))
            }
            _ => None,
        })
        .collect();
    assert_eq!(
        modes,
        BTreeMap::from([
            (database(0)?, DurabilityMode::Quorum),
            (database(1)?, DurabilityMode::Local)
        ])
    );
    for (name, acks) in [(database(0)?, Acks::Quorum), (database(1)?, Acks::Local)] {
        let record = report.state().database(&name).ok_or("lost database")?;
        assert_eq!(record.durability().acks(), acks, "{name}");
        assert!(record.epoch().get() >= 2, "{name} failed over");
    }
    Ok(())
}

#[test]
fn databases_with_different_modes_keep_their_own_guarantees_under_faults() {
    seeds::run(BINARY, &[31], mixed_modes_keep_their_own_guarantees);
}
