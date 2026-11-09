//! FR-066 on the harness: writes and reads through the router survive a primary's crash and a
//! drain (ADR-0037).
//!
//! One control host, three data hosts, a router host, and two clients whose writes and reads go
//! through the router as Hrana pipelines, while their databases' creations go to the fleet
//! directly. Partway through, the test crashes the primary of the first database and restarts
//! it, then drains the primary of the second. The history checker then holds over the whole run:
//! no acknowledged write lost, no rejected write present, and one primary per epoch, which the
//! router's `fleetring-primary` and `fleetring-epoch` headers report for every write it
//! acknowledged.

use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::time::Duration;

use fleetring_control::{DatabaseName, LeaseEpoch};
use fleetringd::sim::{Ack, CreateAndWrite, Fleet, Operation, Operator, Timings, seeds};

use crate::{BINARY, TestResult};

const CONTROL: &str = "control";
const DATA: [&str; 3] = ["node-1", "node-2", "node-3"];
const ROUTER: &str = "router";
const CLIENTS: [&str; 2] = ["app-1", "app-2"];
const OPERATOR: &str = "operator";

/// How many writes each client sends.
const WRITES: u32 = 120;

fn fleet(seed: u64) -> Result<Fleet, Box<dyn Error>> {
    Ok(Fleet::builder(seed)
        .control(CONTROL)
        .data(DATA[0])
        .data(DATA[1])
        .data(DATA[2])
        .router(ROUTER)
        .timings(Timings::default())
        .operation_timeout(Duration::from_secs(2))
        .build()?)
}

/// The node that holds `database`'s lease, as the control plane last published it.
fn primary(fleet: &Fleet, database: &DatabaseName) -> Option<String> {
    let state = fleet.control_state()?;
    let database = state.database(database)?;
    let lease = database.lease()?;
    (lease.epoch() == database.epoch()).then(|| lease.holder().to_string())
}

/// The epochs at which the router acknowledged a write to each database.
fn written_epochs(fleet: &Fleet) -> BTreeMap<DatabaseName, BTreeSet<LeaseEpoch>> {
    let mut epochs: BTreeMap<DatabaseName, BTreeSet<LeaseEpoch>> = BTreeMap::new();
    for (operation, ack) in fleet.history().acknowledged() {
        if let (Operation::Write { database, .. }, Ack::Written { epoch, .. }) = (operation, ack) {
            epochs.entry(database.clone()).or_default().insert(*epoch);
        }
    }
    epochs
}

fn acknowledged_writes(fleet: &Fleet) -> usize {
    fleet.history().acknowledged_writes().count()
}

fn writes_survive_a_crash_and_a_drain(seed: u64) -> TestResult {
    let mut fleet = fleet(seed)?;
    let operator = Operator::new();
    fleet.client(OPERATOR, operator.clone());
    fleet.await_ring(Duration::from_secs(30))?;
    // Both clients write to the same two databases, `db-0` and `db-1`.
    for client in CLIENTS {
        fleet.client_via_router(client, CreateAndWrite::new(2, 3, WRITES))?;
    }
    let first: DatabaseName = "db-0".parse()?;
    let second: DatabaseName = "db-1".parse()?;
    let total = usize::try_from(WRITES)? * CLIENTS.len();

    // A third of the way in, the first database's primary crashes, and comes back.
    fleet.run_until(Duration::from_secs(120), "a third of the writes", |fleet| {
        acknowledged_writes(fleet) >= total / 3 && primary(fleet, &first).is_some()
    })?;
    let crashed = primary(&fleet, &first).ok_or("db-0 has no primary")?;
    fleet.note(format!("crashing {crashed}, db-0's primary"));
    fleet.crash(&crashed)?;
    fleet.run_for(Duration::from_secs(2))?;
    fleet.restart(&crashed)?;
    fleet.check()?;

    // Two thirds of the way in, the second database's primary drains.
    fleet.run_until(
        Duration::from_secs(120),
        "two thirds of the writes",
        |fleet| acknowledged_writes(fleet) >= 2 * total / 3 && primary(fleet, &second).is_some(),
    )?;
    let drained = primary(&fleet, &second).ok_or("db-1 has no primary")?;
    fleet.note(format!("draining {drained}, db-1's primary"));
    operator.push(Operation::Drain {
        node: drained.parse()?,
    });

    fleet.await_workloads(Duration::from_secs(300))?;
    fleet.await_ring(Duration::from_secs(30))?;
    let epochs = written_epochs(&fleet);
    for database in [&first, &second] {
        let seen = epochs.get(database).map_or(0, BTreeSet::len);
        assert!(
            seen >= 2,
            "the router acknowledged writes to {database} at {seen} epoch(s), not across its \
             failover: {epochs:?}"
        );
    }
    fleet.finish()?;
    Ok(())
}

#[test]
fn writes_through_the_router_survive_a_crash_and_a_drain() {
    seeds::run(
        BINARY,
        &seeds::FIXED_HEAVY,
        writes_survive_a_crash_and_a_drain,
    );
}

/// The router loses its link to a primary for a while: the calls it had sent there end with an
/// unknown outcome, never sent again, and the history checker holds them to that.
fn a_partition_breaks_streams_without_losing_writes(seed: u64) -> TestResult {
    let mut fleet = fleet(seed)?;
    fleet.await_ring(Duration::from_secs(30))?;
    for client in CLIENTS {
        fleet.client_via_router(client, CreateAndWrite::new(2, 3, WRITES / 2))?;
    }
    let first: DatabaseName = "db-0".parse()?;
    let total = usize::try_from(WRITES / 2)? * CLIENTS.len();
    fleet.run_until(Duration::from_secs(120), "half the writes", |fleet| {
        acknowledged_writes(fleet) >= total / 2 && primary(fleet, &first).is_some()
    })?;
    let cut = primary(&fleet, &first).ok_or("db-0 has no primary")?;
    fleet.note(format!("cutting the router off from {cut}"));
    fleet.partition(ROUTER, &cut)?;
    fleet.run_for(Duration::from_secs(3))?;
    fleet.repair(ROUTER, &cut)?;
    fleet.await_workloads(Duration::from_secs(300))?;
    fleet.await_ring(Duration::from_secs(30))?;
    fleet.finish()?;
    Ok(())
}

#[test]
fn a_partitioned_router_breaks_streams_without_losing_writes() {
    seeds::run(
        BINARY,
        &seeds::FIXED,
        a_partition_breaks_streams_without_losing_writes,
    );
}
