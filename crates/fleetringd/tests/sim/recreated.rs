//! A database dropped and created again under its name, on the same replicas, while one
//! follower's control calls are slow (FR-106; ADR-0016, point 12; ADR-0046's amendment, item 9).
//!
//! Three data nodes hold `reused` at R = 3, led by P, with followers S and T. Its first life
//! takes enough writes to checkpoint several times, so S's position leads with a later log
//! generation than a young database's will reach. S's link to the control node then gets slow
//! and the name is dropped; T deletes its copy and is cut off from P; and the name is created
//! again: the new database restarts its epochs and is leased at epoch 1 on P again. S judges the drop notice, and the end of P's old
//! stream, against a control state that already holds the new database at epoch 1, so neither
//! deletes its copy, and P's new stream arrives at S under the same epoch and holder its record
//! names. Before the fix S acknowledged the dropped database's last position, P counted it
//! toward the new database's writes, and P alone held writes acknowledged by two; when P then
//! failed, the promotion took S's log by position and served the dropped rows as the new
//! database's.
//!
//! Now P compares S's chain hash at its first acknowledgement with its own log's and re-seeds
//! the copy; and, once forks have given databases random incarnations, S refuses a stream for
//! another incarnation than its copy records, and the drop notice deletes the copy, since the
//! copy records the dropped incarnation and control state holds another.

use std::time::Duration;

use fleetring_control::fork::OperationId;
use fleetring_control::{DatabaseName, Feature};
use fleetringd::replication::ReplicationSettings;
use fleetringd::sim::{DurabilityMode, Fleet, History, Operation, Script, Timings, check, seeds};

use crate::{BINARY, TestResult};

const CONTROL: &str = "control";
const NODES: [&str; 3] = ["n1", "n2", "n3"];
const LIMIT: Duration = Duration::from_secs(60);
/// The delay on S's link to the control node: each of its control calls takes two, and each
/// heartbeat waits for its answer, so S counts as down after a few seconds of it.
const SLOW: Duration = Duration::from_secs(1);
/// The first life's writes: enough to cross several checkpoints.
const FIRST_LIFE: i64 = 40;
/// The second life's values start here, so they never meet the first's.
const SECOND_LIFE: i64 = 1_000;
const SECOND_WRITES: i64 = 6;
/// The forked database's one row, apart from every value the name's lives write, which the
/// fork properties tell apart by value.
const ORIGIN_VALUE: i64 = 9_000;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

/// Whether the fleet has made a fork, so that new databases have random incarnations.
#[derive(Debug, Clone, Copy)]
enum Forks {
    /// Every database has the legacy incarnation: only the chain hash tells the copies apart.
    Never,
    /// A fork enabled the `fork` feature before the name's first life.
    Made,
}

fn reused() -> Result<DatabaseName> {
    Ok("reused".parse()?)
}

fn settings() -> ReplicationSettings {
    ReplicationSettings {
        stall_timeout: Duration::from_millis(400),
        ack_timeout: Duration::from_secs(2),
        open_timeout: Duration::from_secs(5),
        retry_interval: Duration::from_millis(100),
        // Small, so the first life crosses several checkpoints and log generations.
        checkpoint_threshold: 8 * 1024,
        ..ReplicationSettings::new()
    }
}

/// The node holding `database`'s lease at its current epoch.
fn leader(fleet: &Fleet, database: &DatabaseName) -> Option<String> {
    let state = fleet.control_state()?;
    let record = state.database(database)?;
    let lease = record.lease()?;
    (lease.epoch() == record.epoch()).then(|| lease.holder().to_string())
}

/// Runs `operations` on a client of their own, one after another, and steps until each is
/// answered.
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
    Ok(())
}

/// Makes a fork, so that the `fork` feature is enabled and every database created from now on
/// has a random incarnation, and steps until it is ready.
fn make_a_fork(fleet: &mut Fleet) -> Result<()> {
    let origin: DatabaseName = "origin".parse()?;
    let copy: DatabaseName = "origin-copy".parse()?;
    run_script(
        fleet,
        "forker",
        vec![
            Operation::Create {
                database: origin.clone(),
                replication_factor: 3,
                durability: DurabilityMode::Quorum,
            },
            Operation::Write {
                database: origin.clone(),
                value: ORIGIN_VALUE,
            },
            Operation::Fork {
                source: origin,
                target: copy.clone(),
                operation: OperationId::from_bytes([0x5a; 16]),
                replication_factor: 3,
                expect_source_incarnation: None,
            },
        ],
    )?;
    fleet.run_until(LIMIT, "the fork ready", |fleet| {
        fleet.control_state().is_some_and(|state| {
            state.is_enabled(Feature::Fork) && state.database(&copy).is_some_and(|db| db.is_ready())
        })
    })?;
    Ok(())
}

/// The second life's acknowledged writes alone: the history checker leaves a dropped name out.
fn second_life(history: &History) -> History {
    let mut life = History::default();
    for record in history.records() {
        if let Operation::Write { value, .. } = record.operation()
            && (SECOND_LIFE..SECOND_LIFE + SECOND_WRITES).contains(value)
        {
            life.record(
                record.client(),
                record.invoked_at(),
                record.completed_at(),
                record.operation().clone(),
                record.outcome().clone(),
            );
        }
    }
    life
}

fn scenario(seed: u64, forks: Forks) -> TestResult {
    // A slow link holds each heartbeat for a round trip, so members stay live for longer than
    // a few of them; leases last longer than that too, and a promotion still comes in seconds.
    let timings = Timings {
        heartbeat_timeout: Duration::from_secs(5),
        lease_duration: Duration::from_secs(6),
        lease_renewal: Duration::from_secs(1),
        failover_margin: Duration::from_secs(1),
        ..Timings::default()
    };
    let mut builder = Fleet::builder(seed)
        .control(CONTROL)
        .timings(timings)
        .replication(settings())
        .operation_timeout(Duration::from_secs(4));
    for node in NODES {
        builder = builder.data(node);
    }
    let mut fleet = builder.build()?;
    fleet.await_ring(Duration::from_secs(10))?;
    match forks {
        Forks::Made => make_a_fork(&mut fleet)?,
        // The drop notice reaches S after control state holds the name at the dropped epoch
        // again, and a notice naming no random incarnation can't tell the two databases apart,
        // so S refuses it for its binding (ADR-0042, point 2), as it would a forged one.
        Forks::Never => fleet.allow_binding_refusals(),
    }
    let database = reused()?;

    // The first life, long enough to checkpoint several times, on every node.
    let mut first = vec![Operation::Create {
        database: database.clone(),
        replication_factor: 3,
        durability: DurabilityMode::Quorum,
    }];
    first.extend((0..FIRST_LIFE).map(|value| Operation::Write {
        database: database.clone(),
        value,
    }));
    run_script(&mut fleet, "first", first)?;
    fleet.await_replicas_converged(LIMIT)?;
    let primary = leader(&fleet, &database).ok_or("no primary for the first life")?;
    let mut followers = NODES.into_iter().filter(|node| *node != primary);
    let slow = followers.next().ok_or("no follower")?.to_owned();
    let cut = followers.next().ok_or("no second follower")?.to_owned();
    fleet.note(format!(
        "{database} led by {primary}; {slow}'s control calls are slow; {cut} is cut off from \
         {primary}"
    ));

    // S's control calls are slow when the name is dropped. T sees the drop and deletes its copy,
    // and is then cut off from P, so it can't follow the database created again.
    fleet.set_latency(&slow, CONTROL, SLOW)?;
    run_script(
        &mut fleet,
        "drop",
        vec![Operation::Drop {
            database: database.clone(),
        }],
    )?;
    let copy_on = |fleet: &Fleet, host: &str| {
        fleet
            .data_dir(host)
            .is_some_and(|dir| dir.join("databases").join("reused").exists())
    };
    let deleting = cut.clone();
    fleet.run_until(LIMIT, "T deleting its copy", |fleet| {
        !copy_on(fleet, &deleting)
    })?;
    if !copy_on(&fleet, &slow) {
        return Err(format!("{slow} deleted its copy before the name was created again").into());
    }
    fleet.partition(&primary, &cut)?;
    run_script(
        &mut fleet,
        "again",
        vec![Operation::Create {
            database: database.clone(),
            replication_factor: 3,
            durability: DurabilityMode::Quorum,
        }],
    )?;
    if leader(&fleet, &database).as_deref() != Some(primary.as_str()) {
        return Err(format!(
            "the second life is led by {:?}, not {primary}",
            leader(&fleet, &database)
        )
        .into());
    }
    // S has judged the drop notice and the old stream's end by now, each against the second
    // life at epoch 1. Its link heals before its heartbeats fall too far behind for it to count
    // as live.
    fleet.run_for(3 * SLOW)?;
    fleet.set_latency(&slow, CONTROL, Duration::from_millis(10))?;
    fleet.run_for(Duration::from_secs(2))?;
    run_script(
        &mut fleet,
        "second",
        (SECOND_LIFE..SECOND_LIFE + SECOND_WRITES)
            .map(|value| Operation::Write {
                database: database.clone(),
                value,
            })
            .collect(),
    )?;

    // Every write the second life acknowledged is on a write quorum of its copies.
    let rows = fleet.replica_rows(&database)?;
    let life = second_life(&fleet.history());
    check::acknowledged_writes_replicated(&life, &database, &rows, 2)?;
    let acknowledged: Vec<i64> = life.acknowledged_writes().map(|(_, value)| value).collect();
    if acknowledged.is_empty() {
        let outcomes: Vec<String> = life
            .records()
            .iter()
            .map(|record| format!("{}: {:?}", record.operation(), record.outcome()))
            .collect();
        return Err(
            format!("the second life acknowledged no write: {outcomes:?}; rows {rows:?}").into(),
        );
    }

    // P fails while T is still cut off from it: the promotion fences S and T.
    fleet.crash(&primary)?;
    let old = primary.clone();
    let name = database.clone();
    fleet.run_until(LIMIT, "a promotion", |fleet| {
        leader(fleet, &name).is_some_and(|holder| holder != old)
    })?;
    let promoted = leader(&fleet, &database).ok_or("no primary")?;
    let rows = fleet.replica_rows(&database)?;
    let served = rows.get(&promoted).cloned().unwrap_or_default();
    for value in &acknowledged {
        if !served.contains(value) {
            return Err(format!(
                "{promoted} was promoted without acknowledged write {value}: {rows:?}"
            )
            .into());
        }
    }
    if let Some(dropped) = served.iter().find(|value| **value < SECOND_LIFE) {
        return Err(format!("{promoted} serves the dropped database's row {dropped}").into());
    }

    fleet.repair(&primary, &cut)?;
    fleet.restart(&primary)?;
    fleet.await_ring(LIMIT)?;
    fleet.await_replicas_converged(LIMIT)?;
    fleet.finish()?;
    Ok(())
}

/// In a fleet that never forked, every database has the legacy incarnation: the follower's
/// copy of the dropped database is told apart by its chain hash at its first acknowledgement,
/// and re-seeded, before it counts toward any write.
#[test]
fn a_dropped_databases_copy_never_counts_toward_a_database_created_again() {
    seeds::run(BINARY, &seeds::FIXED, |seed| scenario(seed, Forks::Never));
}

/// Once forks give databases random incarnations, the follower refuses the new database's
/// stream for the copy it records as the dropped one's, and the drop notice deletes that copy
/// though control state holds the name at its epoch again, so the new database replicates there.
#[test]
fn a_dropped_databases_copy_is_neither_followed_nor_kept_for_one_created_again() {
    seeds::run(BINARY, &seeds::FIXED, |seed| scenario(seed, Forks::Made));
}
