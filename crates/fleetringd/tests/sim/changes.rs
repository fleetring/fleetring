//! FR-039 on the harness: a file sink on every data node, under crashes, partitions, and
//! failovers. At the end, every node's file for the database is an in-order, gapless stream of
//! writes the history knows, with duplicates allowed and nothing refused or invented
//! ([`Cluster::finish`] runs [`check::change_stream_consistent`] for every file sink).
//!
//! [`check::change_stream_consistent`]: fleetringd::sim::check::change_stream_consistent

use std::collections::BTreeSet;
use std::error::Error;
use std::time::Duration;

use fleetring_control::DatabaseName;
use fleetringd::changes::{Format, SinkConfig, SinkKind, database_file};
use fleetringd::engine::CaptureMode;
use fleetringd::sim::{Ack, Cluster, CreateAndWrite, Operation, Outcome, Timings, seeds};

use crate::{BINARY, TestResult};

const CONTROL: &str = "control";
const DATA: [&str; 3] = ["node-1", "node-2", "node-3"];
const CLIENT: &str = "writer";
const SINK: &str = "stream";

fn database() -> Result<DatabaseName, Box<dyn Error>> {
    Ok("db-0".parse()?)
}

/// A cluster whose data hosts each stream every database to a file sink under their data
/// directory.
fn cluster(seed: u64, timings: Timings) -> Result<Cluster, Box<dyn Error>> {
    let mut builder = Cluster::builder(seed)
        .control(CONTROL)
        .timings(timings)
        .operation_timeout(Duration::from_millis(500));
    for node in DATA {
        builder = builder.data(node).configure(node, |config| {
            config.changes.sink.push(SinkConfig {
                name: SINK.to_owned(),
                kind: SinkKind::File,
                format: Format::Logical,
                capture: CaptureMode::default(),
                databases: vec!["*".parse().expect("a star")],
                path: Some(config.data_dir.join(SINK)),
                url: None,
                timeout_ms: 1_000,
                retries: 1,
            });
        });
    }
    Ok(builder.build()?)
}

fn primary(cluster: &Cluster) -> Option<String> {
    let state = cluster.control_state()?;
    let database = state.database(&database().ok()?)?;
    let lease = database.lease()?;
    (lease.epoch() == database.epoch()).then(|| lease.holder().to_string())
}

fn acknowledged_writes(cluster: &Cluster) -> Vec<(Duration, String, i64)> {
    cluster
        .history()
        .records()
        .iter()
        .filter_map(|record| match (record.operation(), record.outcome()) {
            (
                Operation::Write { value, .. },
                Outcome::Acknowledged(Ack::Written {
                    primary: Some(primary),
                    ..
                }),
            ) => Some((record.completed_at(), primary.to_string(), *value)),
            _ => None,
        })
        .collect()
}

fn await_new_primary(
    cluster: &mut Cluster,
    old: &str,
    since: Duration,
    limit: Duration,
) -> Result<(), Box<dyn Error>> {
    cluster.run_until(limit, "writes moving to a new primary", |cluster| {
        acknowledged_writes(cluster)
            .iter()
            .any(|(at, primary, _)| *at > since && primary != old)
    })?;
    Ok(())
}

/// The values every node's sink file delivered for the database, as one set.
fn delivered(cluster: &Cluster) -> Result<BTreeSet<i64>, Box<dyn Error>> {
    let database = database()?;
    let mut values = BTreeSet::new();
    for node in DATA {
        let dir = cluster.data_dir(node).ok_or("a data host")?.join(SINK);
        let path = database_file(&dir, &database);
        let text = match std::fs::read_to_string(&path) {
            Ok(text) => text,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => return Err(error.into()),
        };
        for line in text.lines() {
            let record: fleetringd::changes::ChangeRecord = serde_json::from_str(line)?;
            if let fleetringd::changes::RecordContent::Logical { rows } = record.content {
                for row in rows {
                    if let Some(column) = row.values().iter().find(|column| column.name == "value")
                        && let fleetringd::engine::Value::Integer(value) = column.value
                    {
                        values.insert(value);
                    }
                }
            }
        }
    }
    Ok(values)
}

/// A primary crash, a partition that isolates the primary, and flapping replicas, while the
/// client writes and every node streams to its file sink.
fn streams_survive_faults_and_failover(seed: u64) -> TestResult {
    let timings = Timings::default();
    let mut cluster = cluster(seed, timings)?;
    cluster.await_ring(Duration::from_secs(30))?;
    cluster.client(CLIENT, CreateAndWrite::new(1, 3, 400));
    cluster.run_until(Duration::from_secs(30), "writes acknowledged", |cluster| {
        acknowledged_writes(cluster).len() >= 20
    })?;
    let first = primary(&cluster).ok_or("no primary")?;
    let failover = timings.lease_duration + timings.failover_margin + Duration::from_secs(2);

    // The primary crashes mid-stream: its sink stops where it was, and the new primary's sink
    // starts from its own position.
    cluster.crash(&first)?;
    let since = cluster.elapsed();
    await_new_primary(&mut cluster, &first, since, failover * 2)?;
    cluster.restart(&first)?;
    cluster.run_for(Duration::from_secs(1))?;

    // The primary is cut off from everyone but the client: what it commits meanwhile is never
    // acknowledged, so its sink must not deliver it.
    let isolated = primary(&cluster).ok_or("no primary")?;
    let others: Vec<&str> = DATA
        .into_iter()
        .filter(|node| *node != isolated.as_str())
        .collect();
    for other in others.iter().copied().chain([CONTROL]) {
        cluster.partition(&isolated, other)?;
    }
    let since = cluster.elapsed();
    await_new_primary(&mut cluster, &isolated, since, failover * 2)?;
    for other in others.iter().copied().chain([CONTROL]) {
        cluster.repair(&isolated, other)?;
    }
    cluster.run_for(Duration::from_secs(1))?;

    // Flapping replicas.
    for round in 0..3 {
        let node = DATA[round % DATA.len()];
        cluster.crash(node)?;
        let down = Duration::from_millis(300 + cluster.rng().below(400));
        cluster.run_for(down)?;
        cluster.restart(node)?;
        let up = Duration::from_millis(300 + cluster.rng().below(400));
        cluster.run_for(up)?;
        cluster.check_history()?;
    }

    cluster.await_workloads(Duration::from_secs(300))?;
    cluster.await_ring(Duration::from_secs(60))?;
    cluster.await_replicas_converged(Duration::from_secs(60))?;
    // Let the sinks drain before the hosts stop.
    cluster.run_for(Duration::from_secs(2))?;
    let checked = cluster.check_change_streams()?;
    let acknowledged: BTreeSet<i64> = acknowledged_writes(&cluster)
        .into_iter()
        .map(|(_, _, value)| value)
        .collect();
    let delivered = delivered(&cluster)?;
    assert!(checked > 0, "some records were checked");
    assert!(
        delivered.len() >= acknowledged.len() / 2,
        "the sinks delivered {} of {} acknowledged writes",
        delivered.len(),
        acknowledged.len()
    );
    let _report = cluster.finish()?;
    Ok(())
}

#[test]
fn file_sinks_stay_consistent_under_faults_and_failover() {
    seeds::run(
        BINARY,
        &seeds::FIXED_HEAVY,
        streams_survive_faults_and_failover,
    );
}
