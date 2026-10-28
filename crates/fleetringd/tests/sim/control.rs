//! FR-028 on the harness: the control node survives crashes.
//!
//! A control-only cluster, a client that joins nodes, creates databases, and sends heartbeats,
//! and a control node that crashes and restarts every 40 to 120 milliseconds. Each crash checks
//! that the log on disk replays to the state recovery builds, and that the state holds every
//! acknowledged member and database.

use std::error::Error;
use std::num::NonZeroU64;
use std::time::Duration;

use fleetring::NodeId;
use fleetring_control::Action;
use fleetringd::sim::{Ack, Cluster, Operation, Script, Timings, seeds};

use crate::{BINARY, TestResult};

const CONTROL: &str = "control";
const NODES: u32 = 12;
const DOWN_STEPS: u64 = 25;
/// Shorter than the default, so the driver notices each crash quickly.
const OPERATION_TIMEOUT: Duration = Duration::from_millis(100);

fn script() -> Result<Script, Box<dyn Error>> {
    let mut operations = Vec::new();
    for k in 0..NODES {
        let node: NodeId = format!("node-{k}").parse()?;
        operations.push(Operation::Join { node: node.clone() });
        operations.push(Operation::CreateDatabase {
            database: format!("tenant:{k}").parse()?,
            replication_factor: 1,
        });
        operations.push(Operation::Heartbeat { node });
    }
    Ok(Script::new(operations))
}

fn crashes(seed: u64) -> TestResult {
    let timings = Timings {
        snapshot_every: NonZeroU64::new(5).ok_or("zero")?,
        ..Timings::default()
    };
    let mut cluster = Cluster::builder(seed)
        .control(CONTROL)
        .timings(timings)
        .operation_timeout(OPERATION_TIMEOUT)
        .build()?;
    cluster.client("driver", script()?);

    let mut step: u64 = 0;
    let mut crashes: u64 = 0;
    let mut next_crash: u64 = 30;
    let mut restart_at: Option<u64> = None;
    while !cluster.workloads_done() {
        cluster.step()?;
        step += 1;
        match restart_at {
            Some(at) if step >= at => {
                cluster.restart(CONTROL)?;
                restart_at = None;
                // Vary the uptime so crashes land at different points of the command cycle.
                next_crash = step + 40 + cluster.rng().below(80);
            }
            None if step >= next_crash => {
                // Crashing the control host checks its log.
                cluster.crash(CONTROL)?;
                crashes += 1;
                restart_at = Some(step + DOWN_STEPS);
            }
            _ => {}
        }
        if step > 300_000 {
            return Err("the driver didn't finish".into());
        }
    }
    if restart_at.is_some() {
        cluster.restart(CONTROL)?;
    }
    cluster.run_for(Duration::from_millis(100))?;
    let report = cluster.finish()?;

    let acknowledged = |wanted: fn(&Ack) -> bool| {
        report
            .history()
            .acknowledged()
            .filter(|(_, ack)| wanted(ack))
            .count()
    };
    let members = report.state().members().len();
    let databases = report.state().databases().len();
    assert_eq!(members, usize::try_from(NODES)?);
    assert_eq!(databases, usize::try_from(NODES)?);
    let heartbeats = acknowledged(|ack| matches!(ack, Ack::HeartbeatRecorded));
    let logged_heartbeats = report
        .commands()
        .iter()
        .filter(|command| matches!(command.action, Action::Heartbeat { .. }))
        .count();
    assert!(
        logged_heartbeats >= heartbeats,
        "{logged_heartbeats} < {heartbeats}"
    );
    assert!(crashes >= 5, "only {crashes} crashes");
    let logged = report.commands().len();
    assert!(
        logged >= usize::try_from(3 * NODES)?,
        "only {logged} commands logged"
    );
    Ok(())
}

#[test]
fn acknowledged_commands_survive_crashes_and_replay_to_the_same_state() {
    seeds::run(BINARY, &[28], crashes);
}
