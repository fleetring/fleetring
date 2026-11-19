//! A seed replays the same run (ADR-0020 and its amendment, FR-122).
//!
//! One short scenario runs twice in one process on each seed: writes at R = 3 across log
//! generations, the primary crashing and a promotion, and the old primary restarting and catching
//! up. Both runs must give the same trace, every event at the same simulated time, the same
//! history, and the same control log.
//!
//! Exact replay needs Tokio built with `--cfg tokio_unstable`, which the workspace's
//! `.cargo/config.toml` sets. A `RUSTFLAGS` variable replaces the configured flags, and a build
//! without the cfg diverges within the first second of a run, so this test fails in such a build
//! rather than compare less.

use std::error::Error;
use std::fmt::Debug;
use std::time::Duration;

use fleetring_control::DatabaseName;
use fleetringd::replication::ReplicationSettings;
use fleetringd::sim::{CreateAndWrite, Fleet, Replay, Report, seeds};

use crate::{BINARY, TestResult};

const CONTROL: &str = "control";
const DATA: [&str; 3] = ["node-1", "node-2", "node-3"];
const WRITES: u32 = 40;
/// Two seeds, so that one lucky seed can't hide a divergence.
const SEEDS: [u64; 2] = [1, 2_026];

/// The one database [`CreateAndWrite`] creates.
fn database() -> Result<DatabaseName, Box<dyn Error>> {
    Ok("db-0".parse()?)
}

/// The node control state names as the database's primary.
fn primary(fleet: &Fleet) -> Option<String> {
    let state = fleet.control_state()?;
    let database = state.database(&database().ok()?)?;
    let lease = database.lease()?;
    (lease.epoch() == database.epoch()).then(|| lease.holder().to_string())
}

/// Writes, crashes the primary once a few writes are acknowledged, and restarts it after the
/// promotion.
fn failover(seed: u64) -> Result<Report, Box<dyn Error>> {
    let settings = ReplicationSettings {
        // Small, so the run crosses log generations.
        checkpoint_threshold: 8 * 1024,
        ..ReplicationSettings::new()
    };
    let mut fleet = Fleet::builder(seed)
        .control(CONTROL)
        .data(DATA[0])
        .data(DATA[1])
        .data(DATA[2])
        .replication(settings)
        .operation_timeout(Duration::from_secs(1))
        .build()?;
    fleet.await_ring(Duration::from_secs(10))?;
    fleet.client("writer", CreateAndWrite::new(1, 3, WRITES));
    fleet.run_until(
        Duration::from_secs(10),
        "ten writes acknowledged",
        |fleet| fleet.history().acknowledged_writes().count() >= 10,
    )?;
    let old = primary(&fleet).ok_or("no primary")?;
    fleet.crash(&old)?;
    fleet.run_until(Duration::from_secs(20), "a promotion", |fleet| {
        primary(fleet).is_some_and(|holder| holder != old)
    })?;
    fleet.restart(&old)?;
    fleet.await_workloads(Duration::from_secs(60))?;
    fleet.await_ring(Duration::from_secs(10))?;
    fleet.await_replicas_converged(Duration::from_secs(30))?;
    Ok(fleet.finish()?)
}

/// The first place `first` and `second` differ, as a message naming `what`.
fn first_difference<T: PartialEq + Debug>(what: &str, first: &[T], second: &[T]) -> TestResult {
    let length = first.len().max(second.len());
    match (0..length).find(|index| first.get(*index) != second.get(*index)) {
        Some(index) => Err(format!(
            "the runs' {what} diverge at {index}: {:?} vs {:?}",
            first.get(index),
            second.get(index)
        )
        .into()),
        None => Ok(()),
    }
}

/// Each command of `commands`, written out with the WAL salts in its positions masked.
///
/// SQLite draws each log generation's salts from its own generator, which the whole process
/// shares and seeds from the operating system, so they differ between two runs of a seed
/// (ADR-0020's amendment). A SQLite position is 20 bytes: the checkpoint sequence number, the two
/// salts, and the offset; the rest of it must replay.
fn without_salts(commands: &[fleetring_control::Command]) -> Vec<String> {
    const OPEN: &str = "Position([";
    commands
        .iter()
        .map(|command| {
            let text = format!("{command:?}");
            let mut masked = String::with_capacity(text.len());
            let mut rest = text.as_str();
            while let Some(start) = rest.find(OPEN) {
                let (before, after) = rest.split_at(start + OPEN.len());
                masked.push_str(before);
                let end = after.find(']').unwrap_or(after.len());
                let (bytes, tail) = after.split_at(end);
                let bytes: Vec<&str> = bytes.split(", ").collect();
                let bytes: Vec<&str> = if bytes.len() == 20 {
                    bytes
                        .iter()
                        .enumerate()
                        .map(|(index, byte)| if (4..12).contains(&index) { "_" } else { byte })
                        .collect()
                } else {
                    bytes
                };
                masked.push_str(&bytes.join(", "));
                rest = tail;
            }
            masked.push_str(rest);
            masked
        })
        .collect()
}

#[test]
fn a_seed_replays_the_same_run() {
    if !cfg!(tokio_unstable) {
        panic!(
            "this build can't replay a seed: Tokio isn't built with `--cfg tokio_unstable`, which \
             `.cargo/config.toml` sets and a `RUSTFLAGS` variable replaces (ADR-0020's amendment)"
        );
    }
    seeds::run(BINARY, &SEEDS, |seed| -> TestResult {
        let first = failover(seed)?;
        let second = failover(seed)?;
        first.trace().compare(second.trace(), Replay::Exact)?;
        first_difference(
            "operations",
            first.history().records(),
            second.history().records(),
        )?;
        if first.history() != second.history() {
            return Err("the runs' histories differ outside their operations".into());
        }
        first_difference(
            "control logs",
            &without_salts(first.commands()),
            &without_salts(second.commands()),
        )?;
        Ok(())
    });
}
