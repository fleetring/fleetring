//! FR-033 on the harness: databases created and written through the data plane under faults.
//!
//! One control host and three data hosts. A client creates databases with three replicas through
//! the `Database` service and then writes to them, while the test partitions a data node from
//! the control node, cuts the client off from a data node, and crashes and restarts a primary.
//! Every acknowledgement lands in the history, for the "no acknowledged write is lost" check
//! that replication adds (FR-030).

use std::collections::BTreeMap;
use std::time::Duration;

use fleetring_control::DatabaseName;
use fleetringd::sim::{Ack, Cluster, CreateAndWrite, Operation, Outcome, seeds};

use crate::{BINARY, TestResult};

const CONTROL: &str = "control";
const DATA: [&str; 3] = ["node-1", "node-2", "node-3"];
const CLIENT: &str = "writer";
const DATABASES: u32 = 4;
const WRITES: u32 = 60;

fn writes_under_faults(seed: u64) -> TestResult {
    let mut cluster = Cluster::builder(seed)
        .control(CONTROL)
        .data(DATA[0])
        .data(DATA[1])
        .data(DATA[2])
        .operation_timeout(Duration::from_millis(500))
        .build()?;
    cluster.await_ring(Duration::from_secs(5))?;
    cluster.client(CLIENT, CreateAndWrite::new(DATABASES, 3, WRITES));

    // Let the databases get created, then disturb the writes.
    cluster.run_until(Duration::from_secs(10), "databases created", |cluster| {
        opened(cluster).len() == usize::try_from(DATABASES).unwrap_or(usize::MAX)
    })?;
    cluster.partition(DATA[1], CONTROL)?;
    cluster.run_for(Duration::from_millis(300))?;
    cluster.repair(DATA[1], CONTROL)?;
    cluster.partition(CLIENT, DATA[2])?;
    cluster.run_for(Duration::from_millis(300))?;
    cluster.repair(CLIENT, DATA[2])?;

    // Crash the primary of the first database, then bring it back: it must serve it again.
    let first: DatabaseName = "db-0".parse()?;
    let primary = opened(&cluster)
        .get(&first)
        .cloned()
        .ok_or("db-0 has no primary")?;
    cluster.crash(&primary)?;
    cluster.run_for(Duration::from_millis(300))?;
    cluster.restart(&primary)?;
    let restarted_at = cluster.elapsed();

    cluster.await_workloads(Duration::from_secs(60))?;
    cluster.await_ring(Duration::from_secs(5))?;
    let report = cluster.finish()?;

    let history = report.history();
    let mut written = 0;
    for (operation, ack) in history.acknowledged() {
        if let (Operation::Write { .. }, ack) = (operation, ack) {
            assert_eq!(
                *ack,
                Ack::Written {
                    acknowledged_by: 1,
                    replicas: 3,
                    under_replicated: true,
                },
                "{operation}"
            );
            written += 1;
        }
    }
    assert_eq!(history.acknowledged_writes().count(), written);
    assert!(written > 0, "no write was acknowledged");
    let writes_sent = history
        .records()
        .iter()
        .filter(|record| matches!(record.operation(), Operation::Write { .. }))
        .count();
    assert_eq!(writes_sent, usize::try_from(WRITES)?);
    // The restarted primary serves its database again.
    let after_restart = history.records().iter().any(|record| {
        matches!(record.operation(), Operation::Write { database, .. } if *database == first)
            && matches!(record.outcome(), Outcome::Acknowledged(_))
            && record.invoked_at() > restarted_at
    });
    assert!(
        after_restart,
        "no write to {first} was acknowledged after {primary} restarted"
    );

    // Every database has the primary its creation reported, at epoch 1.
    for (database, primary) in opened_in(history) {
        let state = report.state().database(&database).ok_or("lost database")?;
        let lease = state.lease().ok_or("no lease")?;
        assert_eq!(lease.holder().as_str(), primary, "{database}");
        assert_eq!(lease.epoch().get(), 1, "{database}");
    }
    Ok(())
}

/// The primary each acknowledged creation reported so far.
fn opened(cluster: &Cluster) -> BTreeMap<DatabaseName, String> {
    opened_in(&cluster.history())
}

fn opened_in(history: &fleetringd::sim::History) -> BTreeMap<DatabaseName, String> {
    history
        .acknowledged()
        .filter_map(|(operation, ack)| match (operation, ack) {
            (Operation::Create { database, .. }, Ack::Opened { primary, .. }) => {
                Some((database.clone(), primary.to_string()))
            }
            _ => None,
        })
        .collect()
}

#[test]
fn databases_are_created_and_written_through_the_data_plane_under_faults() {
    seeds::run(BINARY, &[33, 1, 24, 2_026], writes_under_faults);
}
