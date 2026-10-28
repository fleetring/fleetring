//! FR-031 on the harness: leases, epochs, quorums, and failover (ADR-0018).
//!
//! One control host and three data hosts, and a client that creates a database with three
//! replicas through the data plane and writes to it, as `fleetringd db exec` does: each write
//! locates the primary and is sent once. The tests crash, isolate, and flap primaries and the
//! control node, then check what ADR-0018 promises, all in simulated time:
//!
//! - no acknowledged write is ever lost: every one is on a write quorum, and the replicas
//!   converge ([`Cluster::finish`] and [`Cluster::await_replicas_converged`]);
//! - at most one node acknowledges writes for a database in any epoch
//!   ([`Cluster::check`], from the primary and epoch each acknowledgement names);
//! - a failover moves writes within the lease duration plus the failover margin plus one second;
//! - while the control node is down, the primary keeps acknowledging writes.

use std::collections::BTreeSet;
use std::error::Error;
use std::time::Duration;

use fleetring_control::DatabaseName;
use fleetringd::sim::{Ack, Cluster, CreateAndWrite, Operation, Outcome, Script, Timings, seeds};

use crate::{BINARY, TestResult};

const CONTROL: &str = "control";
const DATA: [&str; 3] = ["node-1", "node-2", "node-3"];
const CLIENT: &str = "writer";

/// A client that writes through the primary's node, without asking the control node where the
/// primary is.
const DIRECT_CLIENT: &str = "direct-writer";

/// How many writes [`DIRECT_CLIENT`] sends, and the first value it writes: past every value
/// [`CLIENT`] writes, so the two never collide.
const DIRECT_WRITES: i64 = 20;
const DIRECT_FIRST_VALUE: i64 = 1_000_000;

/// The one database [`CreateAndWrite`] creates.
fn database() -> Result<DatabaseName, Box<dyn Error>> {
    Ok("db-0".parse()?)
}

fn cluster(seed: u64, timings: Timings) -> Result<Cluster, Box<dyn Error>> {
    Ok(Cluster::builder(seed)
        .control(CONTROL)
        .data(DATA[0])
        .data(DATA[1])
        .data(DATA[2])
        .timings(timings)
        .operation_timeout(Duration::from_millis(500))
        .build()?)
}

/// The node the control plane names as the database's primary, while it runs and one serves.
fn primary(cluster: &Cluster) -> Option<String> {
    let state = cluster.control_state()?;
    let database = state.database(&database().ok()?)?;
    let lease = database.lease()?;
    (lease.epoch() == database.epoch()).then(|| lease.holder().to_string())
}

/// The database's epoch, as the control plane last published it.
fn epoch(cluster: &Cluster) -> Option<u64> {
    let state = cluster.control_state()?;
    Some(state.database(&database().ok()?)?.epoch().get())
}

/// Every acknowledged write, as (when it completed, the primary that acknowledged it, epoch).
fn acknowledged_writes(cluster: &Cluster) -> Vec<(Duration, String, u64)> {
    cluster
        .history()
        .records()
        .iter()
        .filter_map(|record| match (record.operation(), record.outcome()) {
            (
                Operation::Write { .. },
                Outcome::Acknowledged(Ack::Written {
                    primary: Some(primary),
                    epoch,
                    ..
                }),
            ) => Some((record.completed_at(), primary.to_string(), epoch.get())),
            _ => None,
        })
        .collect()
}

/// Creates the database and waits until writes to it are acknowledged.
fn start_writing(cluster: &mut Cluster, writes: u32) -> Result<String, Box<dyn Error>> {
    cluster.await_ring(Duration::from_secs(30))?;
    cluster.client(CLIENT, CreateAndWrite::new(1, 3, writes));
    cluster.run_until(Duration::from_secs(30), "writes acknowledged", |cluster| {
        !acknowledged_writes(cluster).is_empty()
    })?;
    Ok(primary(cluster).ok_or("no primary")?)
}

/// Runs until a node other than `old` acknowledges a write after `since`, and returns how long
/// after `since` that write completed.
fn await_new_primary(
    cluster: &mut Cluster,
    old: &str,
    since: Duration,
    limit: Duration,
) -> Result<Duration, Box<dyn Error>> {
    let moved = |cluster: &Cluster| {
        acknowledged_writes(cluster)
            .into_iter()
            .find(|(at, primary, _)| *at > since && primary != old)
            .map(|(at, _, _)| at)
    };
    cluster.run_until(limit, "writes moving to a new primary", |cluster| {
        moved(cluster).is_some()
    })?;
    let at = moved(cluster).ok_or("no write moved")?;
    Ok(at - since)
}

/// Heals everything, lets the writer finish, and runs the final checks: every acknowledged
/// write is on a write quorum, the replicas converge, and one primary per epoch.
fn heal_and_finish(mut cluster: Cluster) -> Result<fleetringd::sim::Report, Box<dyn Error>> {
    cluster.await_workloads(Duration::from_secs(300))?;
    cluster.await_ring(Duration::from_secs(60))?;
    cluster.await_replicas_converged(Duration::from_secs(60))?;
    cluster.check()?;
    Ok(cluster.finish()?)
}

/// With the daemon's default timings, killing the primary of a three-node cluster moves writes
/// to a new primary within the lease duration plus the failover margin plus one second, and the
/// client gets through by retrying, as `fleetringd db exec` does.
fn failover_timing(seed: u64) -> TestResult {
    let timings = Timings::daemon_defaults();
    let mut cluster = cluster(seed, timings)?;
    let old = start_writing(&mut cluster, 900)?;
    cluster.run_for(Duration::from_secs(3))?;
    cluster.crash(&old)?;
    let crashed_at = cluster.elapsed();
    let budget = timings.lease_duration + timings.failover_margin + Duration::from_secs(1);
    let took = await_new_primary(&mut cluster, &old, crashed_at, budget * 2)?;
    assert!(
        took <= budget,
        "writes moved {took:?} after the crash, more than {budget:?}"
    );
    assert_eq!(epoch(&cluster), Some(2), "one promotion");
    cluster.note(format!(
        "writes moved {} ms after the crash",
        took.as_millis()
    ));
    cluster.restart(&old)?;
    let report = heal_and_finish(cluster)?;
    let state = report
        .state()
        .database(&database()?)
        .ok_or("lost database")?;
    let lease = state.lease().ok_or("no lease")?;
    assert_eq!(lease.epoch().get(), 2);
    assert_ne!(lease.holder().as_str(), old);
    Ok(())
}

#[test]
fn a_crashed_primary_is_replaced_within_the_lease_and_margin() {
    seeds::run(BINARY, &[31], failover_timing);
}

/// Crashes, isolations, a control node outage, and flapping, one after another, while the client
/// writes. Every acknowledged write survives, on a write quorum, and the replicas converge.
fn no_acknowledged_write_lost(seed: u64) -> TestResult {
    let timings = Timings::default();
    let mut cluster = cluster(seed, timings)?;
    let first = start_writing(&mut cluster, 700)?;
    let failover = timings.lease_duration + timings.failover_margin + Duration::from_secs(2);

    // A primary crash: the lease expires and another replica takes over.
    cluster.crash(&first)?;
    let since = cluster.elapsed();
    await_new_primary(&mut cluster, &first, since, failover * 2)?;
    cluster.restart(&first)?;
    cluster.run_for(Duration::from_secs(1))?;

    // A partition that isolates the primary from the control node and its followers, while the
    // client can still reach it: what it commits meanwhile is never acknowledged, and is
    // truncated once it rejoins as a follower.
    let isolated = primary(&cluster).ok_or("no primary")?;
    let others: Vec<&str> = DATA
        .into_iter()
        .filter(|node| *node != isolated.as_str())
        .collect();
    for other in others.iter().copied().chain([CONTROL]) {
        cluster.partition(&isolated, other)?;
    }
    let since = cluster.elapsed();
    await_new_primary(&mut cluster, &isolated, since, failover * 2)?;
    for other in others.iter().copied().chain([CONTROL]) {
        cluster.repair(&isolated, other)?;
    }
    cluster.run_for(Duration::from_secs(1))?;
    cluster.check_writes()?;

    // A control node outage longer than a lease: the primary keeps serving.
    cluster.crash(CONTROL)?;
    cluster.run_for(timings.lease_duration * 2)?;
    cluster.restart(CONTROL)?;
    cluster.run_for(Duration::from_secs(1))?;

    // Flapping: one replica after another goes down and comes back, quicker than a lease.
    for round in 0..3 {
        let node = DATA[round % DATA.len()];
        cluster.crash(node)?;
        let down = Duration::from_millis(300 + cluster.rng().below(400));
        cluster.run_for(down)?;
        cluster.restart(node)?;
        let up = Duration::from_millis(300 + cluster.rng().below(400));
        cluster.run_for(up)?;
        cluster.check_writes()?;
    }

    let epochs = epoch(&cluster).ok_or("no epoch")?;
    let report = heal_and_finish(cluster)?;
    assert!(epochs >= 3, "at least two failovers, at epoch {epochs}");
    let primaries: BTreeSet<(String, u64)> = report
        .history()
        .acknowledged()
        .filter_map(|(_, ack)| match ack {
            Ack::Written {
                primary: Some(primary),
                epoch,
                ..
            } => Some((primary.to_string(), epoch.get())),
            _ => None,
        })
        .collect();
    assert!(
        primaries.len() >= 3,
        "writes were acknowledged by three primaries: {primaries:?}"
    );
    Ok(())
}

#[test]
fn no_acknowledged_write_is_lost_across_crashes_partitions_outages_and_flapping() {
    seeds::run(BINARY, &[1, 2_026], no_acknowledged_write_lost);
}

/// While the control node is down for several lease durations, the primary keeps acknowledging
/// writes, and no failover follows when it comes back.
///
/// The writer that locates the primary through the control node can't route while it is down,
/// so a second client writes through the primary's node directly, as `fleetringd db exec
/// --node` does.
fn control_outage(seed: u64) -> TestResult {
    let timings = Timings::default();
    let mut cluster = cluster(seed, timings)?;
    let serving = start_writing(&mut cluster, 400)?;
    cluster.run_for(Duration::from_secs(1))?;
    cluster.crash(CONTROL)?;
    let down_at = cluster.elapsed();
    let database = database()?;
    let direct =
        (DIRECT_FIRST_VALUE..DIRECT_FIRST_VALUE + DIRECT_WRITES).map(|value| Operation::Write {
            database: database.clone(),
            value,
        });
    cluster.client_through(DIRECT_CLIENT, &serving, Script::new(direct));
    cluster.run_for(timings.lease_duration * 3)?;
    let during = acknowledged_writes(&cluster)
        .into_iter()
        .filter(|(at, primary, _)| *at > down_at && *primary == serving)
        .count();
    assert!(
        during > 0,
        "no write was acknowledged while control was down"
    );
    cluster.restart(CONTROL)?;
    cluster.run_for(timings.lease_duration * 3)?;
    assert_eq!(epoch(&cluster), Some(1), "no failover after the outage");
    assert_eq!(primary(&cluster).as_deref(), Some(serving.as_str()));
    heal_and_finish(cluster)?;
    Ok(())
}

#[test]
fn primaries_keep_serving_while_the_control_node_is_down() {
    seeds::run(BINARY, &[31], control_outage);
}

/// A primary cut off from both followers acknowledges nothing; what it commits meanwhile is a
/// divergent tail, which is truncated when it rejoins under the new primary, so the replicas
/// converge without it.
fn divergent_tail(seed: u64) -> TestResult {
    let timings = Timings::default();
    let mut cluster = cluster(seed, timings)?;
    let old = start_writing(&mut cluster, 500)?;
    cluster.run_for(Duration::from_secs(1))?;
    let others: Vec<&str> = DATA
        .into_iter()
        .filter(|node| *node != old.as_str())
        .collect();
    for other in &others {
        cluster.partition(&old, other)?;
    }
    // The control node still reaches it: no failover yet, and every write it takes is refused.
    let cut_at = cluster.elapsed();
    cluster.run_for(timings.lease_duration / 2)?;
    let acknowledged = acknowledged_writes(&cluster)
        .into_iter()
        .filter(|(at, _, _)| *at > cut_at + Duration::from_millis(100))
        .count();
    assert_eq!(
        acknowledged, 0,
        "a primary cut off from its followers acknowledged a write"
    );

    // Now cut it off from the control node too, so the lease expires and a follower takes over.
    cluster.partition(&old, CONTROL)?;
    let since = cluster.elapsed();
    let failover = timings.lease_duration + timings.failover_margin + Duration::from_secs(2);
    await_new_primary(&mut cluster, &old, since, failover * 2)?;
    let database = database()?;
    let rows = cluster.replica_rows(&database)?;
    let tail: BTreeSet<i64> = {
        let on_old = rows.get(&old).cloned().unwrap_or_default();
        let elsewhere: BTreeSet<i64> = others
            .iter()
            .flat_map(|node| rows.get(*node).cloned().unwrap_or_default())
            .collect();
        on_old.difference(&elsewhere).copied().collect()
    };
    cluster.note(format!("{} rows only on the old primary", tail.len()));
    for other in others.iter().copied().chain([CONTROL]) {
        cluster.repair(&old, other)?;
    }
    cluster.await_workloads(Duration::from_secs(300))?;
    cluster.await_ring(Duration::from_secs(60))?;
    cluster.await_replicas_converged(Duration::from_secs(60))?;
    // The tail is gone from every replica, the old primary's included.
    for (node, held) in cluster.replica_rows(&database)? {
        let survivors: Vec<&i64> = tail.intersection(&held).collect();
        assert!(
            survivors.is_empty(),
            "divergent rows survived on {node}: {survivors:?}"
        );
    }
    cluster.check()?;
    cluster.finish()?;
    Ok(())
}

#[test]
fn a_divergent_tail_is_truncated_and_the_old_primary_converges() {
    seeds::run(BINARY, &[31, 24], divergent_tail);
}
