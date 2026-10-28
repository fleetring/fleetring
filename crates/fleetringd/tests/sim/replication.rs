//! FR-030 on the harness: writes replicate to a write quorum through partitions and follower
//! restarts, and every follower converges once the network heals.
//!
//! A client creates one database with three replicas through the data plane, then writes to it
//! through the `Database` service, as the command line does. The daemons replicate it themselves.

use std::error::Error;
use std::time::Duration;

use fleetring_control::DatabaseName;
use fleetringd::replication::ReplicationSettings;
use fleetringd::sim::{Ack, Cluster, CreateAndWrite, Operation, Outcome, Replay, Report, seeds};

use crate::{BINARY, TestResult};

const CONTROL: &str = "control";
const DATA: [&str; 3] = ["node-1", "node-2", "node-3"];
const WRITES: u32 = 200;

/// The one database [`CreateAndWrite`] creates.
fn database() -> Result<DatabaseName, Box<dyn Error>> {
    Ok("db-0".parse()?)
}

fn cluster(seed: u64) -> Result<Cluster, Box<dyn Error>> {
    let settings = ReplicationSettings {
        stall_timeout: Duration::from_millis(400),
        ack_timeout: Duration::from_secs(2),
        open_timeout: Duration::from_secs(5),
        retry_interval: Duration::from_millis(100),
        // Small, so the run crosses several checkpoints and log generations.
        checkpoint_threshold: 8 * 1024,
        ..ReplicationSettings::new()
    };
    Ok(Cluster::builder(seed)
        .control(CONTROL)
        .data(DATA[0])
        .data(DATA[1])
        .data(DATA[2])
        .replication(settings)
        .operation_timeout(Duration::from_secs(4))
        .build()?)
}

/// The primary the database's creation reported, once it has.
fn primary(cluster: &Cluster) -> Option<String> {
    let database = database().ok()?;
    cluster
        .history()
        .acknowledged()
        .find_map(|(operation, ack)| match (operation, ack) {
            (
                Operation::Create {
                    database: created, ..
                },
                Ack::Opened { primary, .. },
            ) if *created == database => Some(primary.to_string()),
            _ => None,
        })
}

/// Writes while partitioning the primary from each follower in turn, crashing and restarting a
/// follower, stopping one gracefully, and taking the write quorum away for a while; then heals
/// everything and checks that the followers converge.
fn writes_through_faults(seed: u64) -> Result<Report, Box<dyn Error>> {
    let mut cluster = cluster(seed)?;
    cluster.await_ring(Duration::from_secs(5))?;
    cluster.client("writer", CreateAndWrite::new(1, 3, WRITES));
    cluster.run_until(Duration::from_secs(10), "database created", |cluster| {
        primary(cluster).is_some()
    })?;
    let primary = primary(&cluster).ok_or("no primary")?;
    let followers: Vec<&str> = DATA
        .into_iter()
        .filter(|node| *node != primary.as_str())
        .collect();
    let [first, second] = followers[..] else {
        return Err(format!("{primary} isn't one of the data hosts").into());
    };
    cluster.run_for(Duration::from_secs(1))?;

    cluster.partition(&primary, first)?;
    cluster.run_for(Duration::from_millis(1_500))?;
    cluster.check_writes()?;
    cluster.repair(&primary, first)?;
    cluster.run_for(Duration::from_millis(500))?;

    cluster.crash(second)?;
    cluster.run_for(Duration::from_secs(1))?;
    cluster.check_writes()?;
    cluster.restart(second)?;
    cluster.run_for(Duration::from_millis(500))?;

    // Without either follower there is no write quorum: writes fail rather than being
    // acknowledged.
    cluster.partition(&primary, first)?;
    cluster.crash(second)?;
    cluster.run_for(Duration::from_secs(2))?;
    cluster.check_writes()?;
    cluster.repair(&primary, first)?;
    cluster.restart(second)?;

    cluster.stop(first)?;
    cluster.run_for(Duration::from_millis(500))?;
    cluster.restart(first)?;

    cluster.await_workloads(Duration::from_secs(120))?;
    cluster.await_ring(Duration::from_secs(5))?;
    cluster.await_replicas_converged(Duration::from_secs(30))?;
    let status = cluster
        .replication(&primary)
        .and_then(|replication| replication.primary_status(&database().ok()?))
        .ok_or("the primary isn't serving the database")?;
    assert!(status.checkpointed.is_some(), "the run crossed checkpoints");
    assert!(
        status
            .followers
            .iter()
            .all(|follower| !follower.needs_snapshot)
    );
    cluster.check()?;
    Ok(cluster.finish()?)
}

#[test]
fn acknowledged_writes_survive_partitions_and_follower_restarts() {
    seeds::run(BINARY, &seeds::FIXED, |seed| -> TestResult {
        let first = writes_through_faults(seed)?;
        let history = first.history();
        let written = history
            .records()
            .iter()
            .filter(|record| matches!(record.operation(), Operation::Write { .. }))
            .count();
        let acknowledged = history.acknowledged_writes().count();
        let refused = history
            .records()
            .iter()
            .filter(|record| {
                matches!(record.operation(), Operation::Write { .. })
                    && !matches!(record.outcome(), Outcome::Acknowledged(_))
            })
            .count();
        assert_eq!(written, usize::try_from(WRITES)?);
        assert!(
            acknowledged > written / 2,
            "{acknowledged} of {written} acknowledged"
        );
        assert!(refused > 0, "losing the write quorum refused some writes");
        // A write acknowledged while a replica was unreachable says so.
        assert!(
            history.acknowledged().any(|(_, ack)| matches!(
                ack,
                Ack::Written {
                    under_replicated: true,
                    ..
                }
            )),
            "no write was acknowledged as under-replicated"
        );
        let second = writes_through_faults(seed)?;
        first.trace().compare(second.trace(), Replay::supported())?;
        Ok(())
    });
}
