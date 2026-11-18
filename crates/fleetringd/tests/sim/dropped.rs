//! Databases dropped right after the control node restarts (FR-121; ADR-0042, point 2).
//!
//! Three data nodes hold a few databases at R = 3. The control node restarts, and every
//! database is dropped at once, before the data nodes have all reached it again. A data node's
//! connection to the control node died with it, so a drop notice that arrives then may find the
//! node's first call to control state cut short, and the node can't check the notice yet. Before
//! the fix the node refused such a notice for good, and kept its copy as an orphan once its next
//! renewal or ended stream found the database unknown: a drop that left manual cleanup behind,
//! and a name that `db create` refused until `orphan remove`. Now the node answers that it can't
//! check the notice yet, the control node tells it again, and every copy goes.

use std::time::Duration;

use fleetring_control::DatabaseName;
use fleetringd::sim::{Ack, DurabilityMode, Fleet, Operation, Script, seeds};

use crate::{BINARY, TestResult};

const CONTROL: &str = "control";
const NODES: [&str; 3] = ["n1", "n2", "n3"];
const LIMIT: Duration = Duration::from_secs(60);
/// Several databases, so that several notices land on each node right after the restart.
const DATABASES: usize = 4;
const WRITES: i64 = 5;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

fn names() -> Result<Vec<DatabaseName>> {
    (0..DATABASES)
        .map(|index| Ok(format!("scratch-{index}").parse()?))
        .collect()
}

/// Creates each of `names` at R = 3 and writes to it.
fn lives(names: &[DatabaseName], first_value: i64) -> Vec<Operation> {
    names
        .iter()
        .flat_map(|database| {
            let create = Operation::Create {
                database: database.clone(),
                replication_factor: 3,
                durability: DurabilityMode::Quorum,
            };
            let writes = (first_value..first_value + WRITES).map(|value| Operation::Write {
                database: database.clone(),
                value,
            });
            std::iter::once(create).chain(writes)
        })
        .collect()
}

/// Runs `operations` on a client of their own, one after another, steps until each is
/// answered, and fails unless every one was acknowledged.
fn run_script(fleet: &mut Fleet, client: &str, operations: Vec<Operation>) -> Result<()> {
    let count = operations.len();
    fleet.client(client, Script::new(operations));
    let name = client.to_owned();
    fleet.run_until(LIMIT, "the script answered", |fleet| {
        fleet
            .history()
            .records()
            .iter()
            .filter(|record| record.client() == name)
            .count()
            == count
    })?;
    let history = fleet.history();
    let refused: Vec<String> = history
        .records()
        .iter()
        .filter(|record| record.client() == name && record.outcome().ack().is_none())
        .map(|record| format!("{:?}: {:?}", record.operation(), record.outcome()))
        .collect();
    if !refused.is_empty() {
        return Err(format!("{client} wasn't acknowledged: {refused:?}").into());
    }
    Ok(())
}

/// Where a copy of one of `names`, or an orphan of one, is left on a data node.
fn left_behind(fleet: &Fleet, names: &[DatabaseName]) -> Vec<String> {
    NODES
        .into_iter()
        .flat_map(|host| names.iter().map(move |name| (host, name)))
        .filter(|(host, name)| {
            let on_disk = fleet
                .data_dir(host)
                .is_some_and(|dir| dir.join("databases").join(name.as_str()).exists());
            let orphan = fleet
                .replication(host)
                .is_some_and(|replication| replication.is_orphan(name));
            on_disk || orphan
        })
        .map(|(host, name)| format!("{name} on {host}"))
        .collect()
}

fn drop_after_a_control_restart(seed: u64) -> TestResult {
    let mut builder = Fleet::builder(seed).control(CONTROL);
    for node in NODES {
        builder = builder.data(node);
    }
    let mut fleet = builder.build()?;
    fleet.await_ring(Duration::from_secs(10))?;
    let names = names()?;
    run_script(&mut fleet, "first", lives(&names, 0))?;
    fleet.await_replicas_converged(LIMIT)?;

    // The control node restarts, and every name is dropped at once.
    fleet.stop(CONTROL)?;
    fleet.restart(CONTROL)?;
    let drops = names
        .iter()
        .map(|database| Operation::Drop {
            database: database.clone(),
        })
        .collect();
    run_script(&mut fleet, "drop", drops)?;
    if let Some((operation, ack)) = fleet.history().acknowledged().find(|(operation, ack)| {
        matches!(operation, Operation::Drop { .. }) && **ack != Ack::Dropped
    }) {
        return Err(format!("{operation:?} was answered {ack:?}").into());
    }

    // Every copy goes, the primaries' included, and none is kept as an orphan.
    let watched = names.clone();
    let deleted = fleet.run_until(LIMIT, "every copy deleted", |fleet| {
        left_behind(fleet, &watched).is_empty()
    });
    if deleted.is_err() {
        return Err(format!(
            "the drops left copies behind: {:?}",
            left_behind(&fleet, &names)
        )
        .into());
    }

    // Each name is created again on the same nodes, with no `orphan remove`, and replicates.
    run_script(&mut fleet, "again", lives(&names, 1_000))?;
    fleet.await_replicas_converged(LIMIT)?;
    fleet.finish()?;
    Ok(())
}

/// A drop issued right after the control node restarts, before the data nodes reach it again,
/// deletes every copy, the primary's included, so the name can be created again at once.
#[test]
fn a_drop_right_after_a_control_restart_deletes_every_copy() {
    seeds::run(BINARY, &seeds::FIXED, drop_after_a_control_restart);
}
