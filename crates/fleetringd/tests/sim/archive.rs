//! FR-049 on the harness: every data host archives to one store outside the simulated network,
//! which fails a fixed schedule of calls, through a primary crash, a partition, and a flapping
//! replica. Restores to the latest point, to a position, and to a time each hold a prefix of
//! what the database committed, in commit order, with nothing refused or invented, and the
//! latest every write acknowledged ([`check::archive_restores_prefix`]).
//!
//! [`check::archive_restores_prefix`]: fleetringd::sim::check::archive_restores_prefix

use std::error::Error;
use std::num::{NonZeroU32, NonZeroU64};
use std::sync::Arc;
use std::time::Duration;

use fleetring_control::DatabaseName;
use fleetringd::durability::{
    ArchiveKeys, FaultyStore, Listing, MemoryStore, ObjectStore, RestoreTarget, Restored,
    restore_into,
};
use fleetringd::engine::{Execution, Position, SqliteEngine};
use fleetringd::sim::{
    Ack, CreateAndWrite, Fleet, Operation, Outcome, Timings, check, seeds, written_values,
};

use crate::{BINARY, TestResult};

const CONTROL: &str = "control";
const DATA: [&str; 3] = ["node-1", "node-2", "node-3"];
const CLIENT: &str = "writer";
const ARCHIVE: &str = "backups";
/// The archive's key prefix: `[[archive]]`'s default.
const PREFIX: &str = "fleetring";

fn database() -> Result<DatabaseName, Box<dyn Error>> {
    Ok("db-0".parse()?)
}

fn millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

fn primary(fleet: &Fleet) -> Option<String> {
    let state = fleet.control_state()?;
    let database = state.database(&database().ok()?)?;
    let lease = database.lease()?;
    (lease.epoch() == database.epoch()).then(|| lease.holder().to_string())
}

/// When each acknowledged write completed, and the primary that acknowledged it.
fn acknowledged_writes(fleet: &Fleet) -> Vec<(Duration, String)> {
    fleet
        .history()
        .records()
        .iter()
        .filter_map(|record| match (record.operation(), record.outcome()) {
            (
                Operation::Write { .. },
                Outcome::Acknowledged(Ack::Written {
                    primary: Some(primary),
                    ..
                }),
            ) => Some((record.completed_at(), primary.to_string())),
            _ => None,
        })
        .collect()
}

fn await_new_primary(
    fleet: &mut Fleet,
    old: &str,
    since: Duration,
    limit: Duration,
) -> Result<(), Box<dyn Error>> {
    fleet.run_until(limit, "writes moving to a new primary", |fleet| {
        acknowledged_writes(fleet)
            .iter()
            .any(|(at, primary)| *at > since && primary != old)
    })?;
    Ok(())
}

/// Restores the database from `store` at `target` into a new directory under `root`.
fn restore(
    runtime: &tokio::runtime::Runtime,
    store: &MemoryStore,
    target: &RestoreTarget,
    root: &std::path::Path,
    name: &str,
) -> Result<(Restored, Vec<i64>), Box<dyn Error>> {
    let database = database()?;
    let staging = root.join(name);
    let restored = runtime.block_on(restore_into(
        store,
        &ArchiveKeys::new(PREFIX, &database),
        &database,
        Arc::new(SqliteEngine::new(4096)?),
        target,
        &staging,
        Execution::Inline,
    ))?;
    Ok((restored, written_values(&staging)?))
}

/// Archives under a primary crash, a partition that isolates the next primary, and a flapping
/// replica, through a store that fails every seventh call, then restores three ways.
fn archives_restore_after_faults(seed: u64) -> TestResult {
    let timings = Timings::default();
    let store = MemoryStore::new();
    let faulty: Arc<dyn ObjectStore> = Arc::new(FaultyStore::new(
        Arc::new(store.clone()),
        NonZeroU32::new(7).ok_or("nonzero")?,
    ));
    let mut builder = Fleet::builder(seed)
        .control(CONTROL)
        .timings(timings)
        .operation_timeout(Duration::from_millis(500))
        .archive(ARCHIVE, faulty, |entry| {
            entry.segment_interval_ms = NonZeroU64::new(200).unwrap_or(NonZeroU64::MIN);
            entry.snapshot_after_bytes = NonZeroU64::new(64 * 1024).unwrap_or(NonZeroU64::MIN);
            // Enough to keep every epoch's segments, so restores cross the failovers.
            entry.keep_snapshots = NonZeroU32::new(100).unwrap_or(NonZeroU32::MIN);
        });
    for node in DATA {
        builder = builder.data(node);
    }
    let mut fleet = builder.build()?;
    fleet.await_ring(Duration::from_secs(30))?;
    fleet.client(CLIENT, CreateAndWrite::new(1, 3, 200));
    fleet.run_until(Duration::from_secs(30), "writes acknowledged", |fleet| {
        acknowledged_writes(fleet).len() >= 20
    })?;
    let failover = timings.lease_duration + timings.failover_margin + Duration::from_secs(2);

    // The primary crashes mid-segment; the new primary continues the archive from its mark.
    let first = primary(&fleet).ok_or("no primary")?;
    fleet.crash(&first)?;
    let since = fleet.elapsed();
    await_new_primary(&mut fleet, &first, since, failover * 2)?;
    fleet.restart(&first)?;
    fleet.run_for(Duration::from_secs(1))?;

    // The next primary is cut off: what it commits meanwhile is never acknowledged, so its
    // archive must not hold it.
    let isolated = primary(&fleet).ok_or("no primary")?;
    let others: Vec<&str> = DATA
        .into_iter()
        .filter(|node| *node != isolated.as_str())
        .collect();
    for other in others.iter().copied().chain([CONTROL]) {
        fleet.partition(&isolated, other)?;
    }
    let since = fleet.elapsed();
    await_new_primary(&mut fleet, &isolated, since, failover * 2)?;
    for other in others.iter().copied().chain([CONTROL]) {
        fleet.repair(&isolated, other)?;
    }
    fleet.run_for(Duration::from_secs(1))?;

    // A replica flaps.
    let node = DATA[fleet.rng().below(3).try_into().unwrap_or(0)];
    fleet.crash(node)?;
    fleet.run_for(Duration::from_millis(500))?;
    fleet.restart(node)?;

    fleet.await_workloads(Duration::from_secs(300))?;
    fleet.await_ring(Duration::from_secs(60))?;
    fleet.await_replicas_converged(Duration::from_secs(60))?;

    // The archive catches up with everything the primary committed.
    let database = database()?;
    let keys = ArchiveKeys::new(PREFIX, &database);
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()?;
    let listing = |runtime: &tokio::runtime::Runtime| {
        runtime.block_on(Listing::read(&store, &keys, &database))
    };
    fleet.run_until(
        Duration::from_secs(60),
        "the archive catching up",
        |fleet| {
            let committed = fleet
                .primary_replication(&database)
                .and_then(|replication| replication.primary_status(&database))
                .and_then(|status| status.committed);
            committed.is_some()
                && listing(&runtime).is_ok_and(|listing| listing.latest().0 == committed)
        },
    )?;
    let committed = fleet.committed_values(&database)?;
    let history = fleet.history();
    let last_acknowledged = acknowledged_writes(&fleet)
        .iter()
        .map(|(at, _)| *at)
        .max()
        .ok_or("no acknowledged write")?;
    let listing = listing(&runtime)?;
    let root = tempfile::tempdir()?;

    // The latest point: exactly what the database committed, every acknowledged write in it.
    let (latest, values) = restore(
        &runtime,
        &store,
        &RestoreTarget::Latest,
        root.path(),
        "latest",
    )?;
    assert_eq!(values, committed, "the latest point is the whole log");
    check::archive_restores_prefix(
        &history,
        &database,
        &values,
        &committed,
        Some(last_acknowledged),
    )?;
    assert_eq!(latest.position, listing.latest().0);

    // The end of a segment in the middle of what the archive keeps.
    let mut ends: Vec<Position> = listing
        .segments
        .iter()
        .map(|segment| segment.name.end.clone())
        .collect();
    ends.sort();
    ends.dedup();
    let middle = ends.get(ends.len() / 2).ok_or("no segment")?.clone();
    let (at_middle, values) = restore(
        &runtime,
        &store,
        &RestoreTarget::Position(middle.clone()),
        root.path(),
        "middle",
    )?;
    assert_eq!(at_middle.position.as_ref(), Some(&middle));
    assert!(values.len() < committed.len(), "a strict prefix");
    check::archive_restores_prefix(&history, &database, &values, &committed, None)?;

    // A time halfway through what the archive keeps.
    let oldest = listing
        .snapshots
        .iter()
        .map(|snapshot| snapshot.descriptor.taken_at_ms)
        .min()
        .ok_or("no snapshot")?;
    let newest = listing.latest().1.ok_or("no segment")?;
    let halfway = oldest + (newest - oldest) / 2;
    let (at_time, values) = restore(
        &runtime,
        &store,
        &RestoreTarget::Time(halfway),
        root.path(),
        "halfway",
    )?;
    assert!(at_time.archived_at_ms.is_none_or(|at| at <= halfway));
    check::archive_restores_prefix(&history, &database, &values, &committed, None)?;

    // Two primaries archived, and snapshots were taken as segments piled up.
    let epochs: std::collections::BTreeSet<u64> = listing
        .segments
        .iter()
        .map(|segment| segment.name.epoch.get())
        .collect();
    assert!(epochs.len() >= 2, "segments from epochs {epochs:?}");
    assert!(
        listing.snapshots.len() >= 2,
        "{} snapshots",
        listing.snapshots.len()
    );
    assert!(millis(last_acknowledged) > 0);
    let _report = fleet.finish()?;
    Ok(())
}

#[test]
fn archives_restore_an_acknowledged_prefix_after_faults() {
    // One fixed seed keeps this heavy scenario within the simulation job's budget; the nightly
    // sweep runs it under many more.
    seeds::run(
        BINARY,
        &seeds::FIXED_HEAVY[..1],
        archives_restore_after_faults,
    );
}
