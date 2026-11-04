//! FR-063 on the harness: every simulated host keeps its own metrics, and they agree with the
//! history (ADR-0034).
//!
//! One control host and three data hosts in one process, with a writer. The admin listener
//! doesn't start on the simulated network; the harness reads each host's registry directly.

use std::time::Duration;

use fleetringd::sim::{Ack, CreateAndWrite, Fleet, Operation, seeds};

use crate::{BINARY, TestResult};

const CONTROL: &str = "control";
const DATA: [&str; 3] = ["node-1", "node-2", "node-3"];
const CLIENT: &str = "writer";
const DATABASES: u32 = 2;
const WRITES: u32 = 20;

/// The value of the sample line `series` in `exposition`.
fn sample(exposition: &str, series: &str) -> Option<u64> {
    exposition
        .lines()
        .find_map(|line| line.strip_prefix(series)?.strip_prefix(' '))
        .and_then(|value| value.parse().ok())
}

fn metrics_follow_the_writes(seed: u64) -> TestResult {
    let mut fleet = Fleet::builder(seed)
        .control(CONTROL)
        .data(DATA[0])
        .data(DATA[1])
        .data(DATA[2])
        .operation_timeout(Duration::from_millis(500))
        .build()?;
    fleet.await_ring(Duration::from_secs(5))?;
    fleet.client(CLIENT, CreateAndWrite::new(DATABASES, 3, WRITES));
    fleet.await_workloads(Duration::from_secs(60))?;

    let acknowledged = fleet
        .history()
        .acknowledged()
        .filter(|(operation, ack)| {
            matches!(operation, Operation::Write { .. }) && matches!(ack, Ack::Written { .. })
        })
        .count();
    let mut counted = 0;
    for host in DATA {
        let exposition = fleet.metrics(host).ok_or(format!("{host} isn't running"))?;
        assert!(
            exposition.contains(&format!("node_id=\"{host}\"")),
            "each host has its own registry: {exposition}"
        );
        counted += sample(
            &exposition,
            "fleetring_writes_total{outcome=\"acknowledged\"}",
        )
        .ok_or(format!("{host} has no write count"))?;
    }
    assert!(acknowledged > 0, "no write was acknowledged");
    assert!(
        counted >= u64::try_from(acknowledged)?,
        "the data hosts counted {counted} acknowledged writes; the history has {acknowledged}"
    );
    let control = fleet
        .metrics(CONTROL)
        .ok_or("the control host isn't running")?;
    assert!(
        sample(
            &control,
            "fleetring_control_commands_total{action=\"create_database\",outcome=\"applied\"}"
        )
        .is_some_and(|created| created >= u64::from(DATABASES)),
        "{control}"
    );
    fleet.finish()?;
    Ok(())
}

#[test]
fn metrics_count_the_acknowledged_writes() {
    seeds::run(BINARY, &seeds::FIXED, metrics_follow_the_writes);
}
