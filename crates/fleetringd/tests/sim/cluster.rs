//! A cluster of real daemons: startup, heartbeats, faults, a workload, and shutdown.

use std::error::Error;
use std::time::Duration;

use fleetring::NodeId;
use fleetring_control::Liveness;
use fleetringd::sim::{
    Ack, Cluster, CreateAndAcquire, LossRate, Operation, Replay, Report, SimError, seeds,
};

use crate::{BINARY, TestResult};

const CONTROL: &str = "control";
const DATA: [&str; 3] = ["node-1", "node-2", "node-3"];

fn three_nodes(seed: u64) -> Result<Cluster, SimError> {
    Cluster::builder(seed)
        .control(CONTROL)
        .data(DATA[0])
        .data(DATA[1])
        .data(DATA[2])
        .build()
}

fn node_ids() -> Result<Vec<NodeId>, Box<dyn Error>> {
    Ok(DATA.iter().map(|id| id.parse()).collect::<Result<_, _>>()?)
}

/// Starts a control host and three data hosts, waits for every node to publish the control
/// plane's ring, lets heartbeats run, and shuts down.
fn start_and_stop(seed: u64) -> Result<Report, Box<dyn Error>> {
    let mut cluster = three_nodes(seed)?;
    cluster.await_ring(Duration::from_secs(5))?;
    cluster.note("converged");
    let converged_at = cluster.elapsed();
    cluster.run_for(Duration::from_secs(2))?;
    let state = cluster.control_state().ok_or("the control node is down")?;
    assert_eq!(state.membership_epoch().get(), 3);
    for id in node_ids()? {
        let last = state.last_heartbeat(&id).ok_or("no heartbeat")?;
        let since = u64::try_from(converged_at.as_millis())? + 1_500;
        assert!(last.as_millis() > since, "{id}: last heartbeat at {last:?}");
    }
    Ok(cluster.finish()?)
}

#[test]
fn three_nodes_start_exchange_heartbeats_and_stop_deterministically() {
    seeds::run(BINARY, &seeds::FIXED, |seed| -> TestResult {
        let first = start_and_stop(seed)?;
        let second = start_and_stop(seed)?;
        first.trace().compare(second.trace(), Replay::supported())?;
        let started = first
            .trace()
            .by_source()
            .values()
            .filter(|events| events.first().is_some_and(|e| e == "started"))
            .count();
        assert_eq!(started, 4, "{}", first.trace());
        Ok(())
    });
}

/// Partitions a data node from the control node and heals it, then crashes a data node and
/// restarts it, checking after each step that the live nodes agree on the ring.
fn partition_and_crash(seed: u64) -> Result<Report, Box<dyn Error>> {
    let mut cluster = three_nodes(seed)?;
    cluster.await_ring(Duration::from_secs(5))?;

    cluster.partition("node-2", CONTROL)?;
    let node_2: NodeId = "node-2".parse()?;
    cluster.run_until(Duration::from_secs(3), "node-2 marked down", |cluster| {
        cluster
            .control_state()
            .is_some_and(|state| state.liveness(&node_2, state.now()) == Some(Liveness::Down))
    })?;
    cluster.await_ring_on(&["node-1", "node-3"], Duration::from_secs(1))?;
    cluster.repair("node-2", CONTROL)?;
    cluster.await_ring(Duration::from_secs(2))?;

    cluster.crash("node-3")?;
    let node_3: NodeId = "node-3".parse()?;
    cluster.run_until(Duration::from_secs(3), "node-3 marked down", |cluster| {
        cluster
            .control_state()
            .is_some_and(|state| state.liveness(&node_3, state.now()) == Some(Liveness::Down))
    })?;
    cluster.await_ring(Duration::from_secs(1))?;
    cluster.restart("node-3")?;
    cluster.await_ring(Duration::from_secs(2))?;
    Ok(cluster.finish()?)
}

#[test]
fn partitions_heal_and_crashed_hosts_restart() {
    seeds::run(BINARY, &seeds::FIXED, |seed| -> TestResult {
        let first = partition_and_crash(seed)?;
        let second = partition_and_crash(seed)?;
        first.trace().compare(second.trace(), Replay::supported())?;
        Ok(())
    });
}

/// Runs a create-and-acquire workload while the control node crashes, stops, and restarts at
/// seeded times, a data node crashes, and the client's link to the control node drops messages.
fn workload_under_faults(seed: u64) -> Result<Report, Box<dyn Error>> {
    const DATABASES: u32 = 8;
    let mut cluster = three_nodes(seed)?;
    cluster.await_ring(Duration::from_secs(5))?;
    cluster.client(
        "client",
        CreateAndAcquire::new(node_ids()?, DATABASES, 3, 8),
    );
    cluster.set_loss("client", CONTROL, LossRate::per_mille(2))?;
    cluster.set_latency("node-1", CONTROL, Duration::from_millis(30))?;

    for round in 0..3 {
        let uptime = 200 + cluster.rng().below(800);
        cluster.run_for(Duration::from_millis(uptime))?;
        if round == 1 {
            cluster.stop(CONTROL)?;
        } else {
            cluster.crash(CONTROL)?;
        }
        let downtime = 50 + cluster.rng().below(400);
        cluster.run_for(Duration::from_millis(downtime))?;
        cluster.restart(CONTROL)?;
        cluster.check()?;
    }
    cluster.crash("node-2")?;
    cluster.run_for(Duration::from_millis(300))?;
    cluster.restart("node-2")?;

    cluster.set_loss("client", CONTROL, LossRate::NONE)?;
    cluster.await_workloads(Duration::from_secs(60))?;
    cluster.await_ring(Duration::from_secs(3))?;
    let report = cluster.finish()?;

    let history = report.history();
    let created = history
        .acknowledged()
        .filter(|(_, ack)| matches!(ack, Ack::Created))
        .count();
    let leased = report
        .state()
        .databases()
        .filter(|database| database.lease().is_some())
        .count();
    assert_eq!(
        report.state().databases().len(),
        usize::try_from(DATABASES)?
    );
    assert_eq!(
        leased,
        usize::try_from(DATABASES)?,
        "every database is leased"
    );
    assert!(created > 0, "no create was acknowledged");
    assert!(
        history
            .records()
            .iter()
            .any(|record| matches!(record.operation(), Operation::RenewLease { .. })),
        "no renewal was sent"
    );
    Ok(report)
}

#[test]
fn acknowledged_creates_and_leases_survive_control_crashes() {
    seeds::run(BINARY, &seeds::FIXED, |seed| -> TestResult {
        let first = workload_under_faults(seed)?;
        let second = workload_under_faults(seed)?;
        first.trace().compare(second.trace(), Replay::supported())?;
        Ok(())
    });
}
