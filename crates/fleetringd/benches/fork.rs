//! What a fork costs on a local three-node fleet (FR-087, ADR-0046 point 7): how long it takes to
//! be seeded and then ready, what it writes and holds on disk, how long it pins its source and how
//! far the source's WAL grows meanwhile, what it does to the source's writes, and how many
//! control-log entries it takes. Numbers live in `docs/perf.md` under "Forks". Run with
//!
//! ```sh
//! cargo bench -p fleetringd --bench fork
//! FORK_BENCH_MIB=10,100 cargo bench -p fleetringd --bench fork   # other source sizes, in MiB
//! ```
//!
//! Each size starts a control daemon and three data daemons in one process, over TLS on
//! loopback, with the shipped defaults except a small disk-admission margin, so that a laptop's
//! disk admits the copies. Their data directories share one temporary directory: set `TMPDIR`
//! to measure another disk. The source has three replicas and acknowledges at its write quorum,
//! W = 2; the fork has three, so its completion and bootstrap quorums are 2 as well. The source
//! is filled with 1 MiB transactions of random blobs. Four writers then write 64-byte rows to
//! it for ten seconds, the baseline, and again from the fork request until the fork is ready.

use std::collections::BTreeMap;
use std::error::Error;
use std::fmt::Write as _;
use std::net::SocketAddr;
use std::path::Path;
use std::time::Duration;

use fleetring::ReplicationFactor;
use fleetring_control::DatabaseName;
use fleetring_control::fork::OperationId;
use fleetring_proto::v1 as proto;
use fleetringd::data::{Client, DatabaseSpec, ForkSpec};
use fleetringd::engine::Statement;
use fleetringd::{Config, Daemon};
use tokio::sync::watch;
use tokio::task::JoinSet;
use tokio::time::Instant;

use crate::support::Tls;

#[path = "../tests/support/mod.rs"]
mod support;

type Fallible<T = ()> = Result<T, Box<dyn Error>>;

const DATA: [&str; 3] = ["data-1", "data-2", "data-3"];

/// The source sizes measured unless `FORK_BENCH_MIB` names others, in MiB.
const SIZES: [u64; 3] = [10, 100, 1024];

const MIB: u64 = 1 << 20;

/// How many writers write to the source at once.
const WRITERS: usize = 4;

/// How long the writers run with no fork, for the baseline.
const BASELINE: Duration = Duration::from_secs(10);

/// How often the fork's status, the disk, and the pin's WAL are sampled.
const SAMPLE: Duration = Duration::from_millis(20);

/// Guards against a hang only.
const HANG_GUARD: Duration = Duration::from_secs(30 * 60);

/// A disk-admission margin small enough for a laptop's disk, on every daemon.
const FORK: &str =
    "\n[fork]\nadmission_min_free_bytes = 67108864\nadmission_min_free_percent = 0\n";

/// One transaction of the source's fill: 16 random blobs of 64 KiB, 1 MiB.
const FILL: &str = "WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 16) \
                    INSERT INTO fill (body) SELECT randomblob(65536) FROM n";

/// One write of the writers: a 64-byte row.
const WRITE: &str = "INSERT INTO writes (body) VALUES (randomblob(64))";

fn main() -> Fallible {
    let sizes = match std::env::var("FORK_BENCH_MIB") {
        Ok(list) => list
            .split(',')
            .map(|mib| mib.trim().parse::<u64>())
            .collect::<Result<Vec<_>, _>>()?,
        Err(_) => SIZES.to_vec(),
    };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(8)
        .enable_all()
        .build()?;
    let mut measured = Vec::new();
    for mib in sizes {
        let one = runtime.block_on(measure(mib))?;
        eprintln!(
            "measured {mib} MiB: seeded after {:?}, ready {:?} later; the slowest write during the \
             fork began {:?} after the request and took {:?}",
            one.to_seeded, one.to_ready, one.during.slowest.0, one.during.slowest.1
        );
        measured.push(one);
    }
    print!("{}", tables(&measured));
    Ok(())
}

/// A control daemon and three data daemons.
struct Fleet {
    control: Daemon,
    data: Vec<Daemon>,
    tls: Tls,
}

fn config(dir: &Path, tls: &Tls, id: &str, control: Option<SocketAddr>) -> Fallible<String> {
    let (role, control) = match control {
        Some(address) => ("data", format!("\n[control]\naddress = \"{address}\"\n")),
        None => ("control", "\n[control]\n".to_owned()),
    };
    Ok(format!(
        "data_dir = {:?}\nlisten = \"127.0.0.1:0\"\nrole = \"{role}\"\n\n[node]\nid = \"{id}\"\n\
         {}{control}{FORK}",
        dir.join(id),
        tls.node_table(id)?
    ))
}

impl Fleet {
    async fn start(dir: &Path) -> Fallible<Self> {
        let tls = Tls::create(dir)?;
        let control =
            Daemon::start(Config::from_toml(&config(dir, &tls, "control", None)?)?).await?;
        let address = control.control_address().ok_or("no control address")?;
        let mut data = Vec::new();
        for id in DATA {
            let text = config(dir, &tls, id, Some(address))?;
            data.push(Daemon::start(Config::from_toml(&text)?).await?);
        }
        let fleet = Self { control, data, tls };
        fleet.converge().await?;
        Ok(fleet)
    }

    /// Waits until every data daemon publishes the control plane's ring with every member live.
    async fn converge(&self) -> Fallible {
        let handle = self.control.control().ok_or("no control handle")?;
        let mut states = handle.watch();
        let state = tokio::time::timeout(
            HANG_GUARD,
            states.wait_for(|state| {
                state.members().len() == DATA.len()
                    && state.members().all(|member| {
                        state.liveness(member.id(), state.now())
                            == Some(fleetring_control::Liveness::Live)
                    })
            }),
        )
        .await??
        .clone();
        let fingerprint = state.ring()?.fingerprint();
        for daemon in &self.data {
            let membership = daemon.membership().ok_or("no membership")?;
            tokio::time::timeout(
                HANG_GUARD,
                membership.wait_for(|view| view.fingerprint() == Some(fingerprint)),
            )
            .await?
            .ok_or("membership stopped")?;
        }
        Ok(())
    }

    fn client(&self) -> Fallible<Client> {
        let address = self
            .control
            .control_address()
            .ok_or("no control address")?
            .to_string();
        let network = self.tls.client_network_with(
            support::PRINCIPAL,
            fleetringd::transport::TransportConfig {
                request_timeout: Duration::from_secs(600),
                ..fleetringd::transport::TransportConfig::default()
            },
        )?;
        Ok(Client::new(address, network))
    }

    fn daemon(&self, id: &str) -> Fallible<&Daemon> {
        self.data
            .iter()
            .find(|daemon| daemon.config().node.id.as_str() == id)
            .ok_or_else(|| format!("no data daemon {id}").into())
    }

    async fn stop(self) {
        for daemon in self.data {
            daemon.run_until(async {}).await;
        }
        self.control.run_until(async {}).await;
    }
}

/// What one fork of one source size measured.
#[derive(Debug)]
struct Measured {
    /// The source's fill, in MiB.
    mib: u64,
    /// The bytes of the copy the seeder streamed.
    copy: u64,
    /// The fork's seeder, and whether it was the source's primary.
    seeder: String,
    seeder_is_source_primary: bool,
    /// From the fork's request to `Seeded`, and from `Seeded` to `Ready`, by the control node's
    /// clock.
    to_seeded: Duration,
    to_ready: Duration,
    /// How many pins the source's primary held for the fork, and for how long in all.
    pins: u64,
    pinned: Duration,
    /// The most WAL the source's pins held at once, by the source's own gauge, and how far the
    /// source's WAL file grew during the fork.
    pinned_wal: u64,
    wal_file_growth: u64,
    /// The bytes each data node staged, by node.
    staged: BTreeMap<String, u64>,
    /// The fleet's data directories: before the fork, at their largest during it, and once it was
    /// ready and the staged copies were gone.
    disk_before: u64,
    disk_peak: u64,
    disk_after: u64,
    /// Each node's data directory at its largest during the fork, by node.
    node_peaks: BTreeMap<String, u64>,
    /// The source's writes with no fork, and during it.
    baseline: Writes,
    during: Writes,
    /// The control-log entries the fork took, by command, and every entry from its request to
    /// its readiness, heartbeats included; and the entries in the baseline's ten seconds.
    fork_entries: BTreeMap<String, u64>,
    all_entries: u64,
    baseline_entries: u64,
}

/// The latencies of the source's writes over a span of time.
#[derive(Debug)]
struct Writes {
    elapsed: Duration,
    /// Sorted.
    latencies: Vec<Duration>,
    /// The slowest write: when it began, after the span's start, and how long it took.
    slowest: (Duration, Duration),
}

impl Writes {
    fn per_second(&self) -> u128 {
        let count = u128::try_from(self.latencies.len()).unwrap_or(u128::MAX);
        count * 1_000_000 / self.elapsed.as_micros().max(1)
    }

    fn percentile(&self, per_mille: usize) -> Duration {
        let index = (self.latencies.len() * per_mille / 1_000).min(self.latencies.len() - 1);
        self.latencies.get(index).copied().unwrap_or_default()
    }
}

/// Runs [`WRITERS`] writers on `database` until `stop` says so.
fn writers(
    client: &Client,
    database: &DatabaseName,
    stop: &watch::Receiver<bool>,
    started: Instant,
) -> JoinSet<Result<Vec<(Duration, Duration)>, tonic::Status>> {
    let mut tasks = JoinSet::new();
    for _ in 0..WRITERS {
        let client = client.clone();
        let database = database.clone();
        let stop = stop.clone();
        tasks.spawn(async move {
            let mut latencies = Vec::new();
            while !*stop.borrow() {
                let begun = Instant::now();
                client.execute(&database, Statement::new(WRITE)).await?;
                latencies.push((begun - started, begun.elapsed()));
            }
            Ok(latencies)
        });
    }
    tasks
}

async fn collect(
    mut tasks: JoinSet<Result<Vec<(Duration, Duration)>, tonic::Status>>,
    started: Instant,
) -> Fallible<Writes> {
    let mut writes = Vec::new();
    while let Some(done) = tasks.join_next().await {
        writes.extend(done??);
    }
    let elapsed = started.elapsed();
    let slowest = writes
        .iter()
        .copied()
        .max_by_key(|(_, latency)| *latency)
        .ok_or("no write finished")?;
    let mut latencies: Vec<Duration> = writes.into_iter().map(|(_, latency)| latency).collect();
    latencies.sort();
    Ok(Writes {
        elapsed,
        latencies,
        slowest,
    })
}

/// The bytes of every file under `root`.
fn bytes_under(root: &Path) -> u64 {
    let mut total = 0;
    let mut pending = vec![root.to_path_buf()];
    while let Some(dir) = pending.pop() {
        for entry in std::fs::read_dir(&dir).into_iter().flatten().flatten() {
            match entry.metadata() {
                Ok(metadata) if metadata.is_dir() => pending.push(entry.path()),
                Ok(metadata) => total += metadata.len(),
                Err(_) => {}
            }
        }
    }
    total
}

/// Each data node's data directory, in bytes, by node.
fn disk(dir: &Path) -> BTreeMap<String, u64> {
    DATA.iter()
        .map(|id| ((*id).to_owned(), bytes_under(&dir.join(id))))
        .collect()
}

/// A sample from `daemon`'s metrics: the value of the series named exactly `series`.
fn metric(daemon: &Daemon, series: &str) -> Option<f64> {
    daemon.metrics().encode().lines().find_map(|line| {
        let value = line.strip_prefix(series)?.strip_prefix(' ')?;
        value.trim().parse::<f64>().ok()
    })
}

#[expect(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    reason = "a byte count or a duration a gauge holds as a float, never negative"
)]
fn whole(value: f64) -> u64 {
    value.max(0.0).round() as u64
}

/// The control log's last index.
fn last_index(log: &Path) -> Fallible<i64> {
    let connection =
        rusqlite::Connection::open_with_flags(log, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    let last: Option<i64> =
        connection.query_row("SELECT max(log_index) FROM commands", [], |row| row.get(0))?;
    Ok(last.unwrap_or(0))
}

/// The control log's entries after `from` and up to `to` that concern `target`, by command: the
/// fork's own commands, and any other command but a heartbeat that names it; and how many
/// entries there are in all.
fn entries(log: &Path, from: i64, to: i64, target: &str) -> Fallible<(BTreeMap<String, u64>, u64)> {
    let connection =
        rusqlite::Connection::open_with_flags(log, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    let mut statement = connection
        .prepare("SELECT command FROM commands WHERE log_index > ?1 AND log_index <= ?2")?;
    let commands = statement
        .query_map([from, to], |row| row.get::<_, String>(0))?
        .collect::<Result<Vec<_>, _>>()?;
    let mut by_kind = BTreeMap::new();
    let named = format!("\"{target}\"");
    for text in &commands {
        let command: serde_json::Value = serde_json::from_str(text)?;
        let kind = match &command["action"] {
            serde_json::Value::Object(action) => action.keys().next().cloned().unwrap_or_default(),
            serde_json::Value::String(action) => action.clone(),
            _ => String::new(),
        };
        if kind.contains("fork") || (kind != "heartbeat" && text.contains(&named)) {
            *by_kind.entry(kind).or_default() += 1;
        }
    }
    Ok((by_kind, u64::try_from(commands.len())?))
}

async fn measure(mib: u64) -> Fallible<Measured> {
    let dir = tempfile::tempdir()?;
    let fleet = Fleet::start(dir.path()).await?;
    let client = fleet.client()?;
    let three = ReplicationFactor::new(3).ok_or("zero")?;
    let source: DatabaseName = "src".parse()?;
    let target: DatabaseName = "dst".parse()?;
    client.create_database(&source, three).await?;
    for table in ["fill", "writes"] {
        let create = format!("CREATE TABLE {table} (id INTEGER PRIMARY KEY, body BLOB)");
        client.execute(&source, Statement::new(create)).await?;
    }
    let filling = Instant::now();
    for _ in 0..mib {
        client.execute(&source, Statement::new(FILL)).await?;
    }
    eprintln!("filled {mib} MiB in {:.1?}", filling.elapsed());
    let source_primary = client
        .locate(&source)
        .await?
        .primary
        .ok_or("the source has no primary")?
        .to_string();
    let log = dir.path().join("control").join("control.db");

    // The baseline: the writers with no fork.
    let from = last_index(&log)?;
    let (stop, stopped) = watch::channel(false);
    let started = Instant::now();
    let tasks = writers(&client, &source, &stopped, started);
    tokio::time::sleep(BASELINE).await;
    stop.send(true)?;
    let baseline = collect(tasks, started).await?;
    let baseline_entries = u64::try_from(last_index(&log)? - from)?;

    // The fork, with the writers running from its request until it is ready.
    let disk_before: u64 = disk(dir.path()).values().sum();
    let wal = dir
        .path()
        .join(&source_primary)
        .join("databases")
        .join(source.as_str())
        .join("data.db-wal");
    let wal_before = std::fs::metadata(&wal).map_or(0, |metadata| metadata.len());
    let primary = fleet.daemon(&source_primary)?;
    let from = last_index(&log)?;
    let (stop, stopped) = watch::channel(false);
    let started = Instant::now();
    let tasks = writers(&client, &source, &stopped, started);
    let mut operation = [0x5a; 16];
    operation[0] = u8::try_from(mib % 128)?;
    let id = OperationId::from_bytes(operation);
    let mut spec = ForkSpec::new(id);
    spec.database = DatabaseSpec::new(three);
    client.fork_database(&source, &target, &spec).await?;
    let mut disk_peak = disk_before;
    let mut node_peaks: BTreeMap<String, u64> = BTreeMap::new();
    let mut pinned_wal = 0;
    let mut wal_peak = wal_before;
    // A ready fork's status no longer names its seeder or its copy's size.
    let mut seeder = String::new();
    let mut copy = 0;
    let ready = tokio::time::timeout(HANG_GUARD, async {
        loop {
            let status = client.fork_operation(id).await?;
            if let Some(fork) = status
                .placement
                .as_ref()
                .and_then(|placement| placement.fork.as_ref())
            {
                if let Some(named) = &fork.seeder {
                    seeder.clone_from(named);
                }
                copy = copy.max(fork.total_bytes.unwrap_or_default());
            }
            let now = disk(dir.path());
            disk_peak = disk_peak.max(now.values().sum());
            for (node, bytes) in now {
                let peak = node_peaks.entry(node).or_default();
                *peak = (*peak).max(bytes);
            }
            if let Some(held) = metric(primary, "fleetring_fork_pinned_wal_bytes") {
                pinned_wal = pinned_wal.max(whole(held));
            }
            wal_peak = wal_peak.max(std::fs::metadata(&wal).map_or(0, |metadata| metadata.len()));
            match status.state() {
                proto::ForkState::Ready => return Ok::<_, Box<dyn Error>>(status),
                proto::ForkState::Failed | proto::ForkState::Cancelled => {
                    return Err(format!("the fork ended {status:?}").into());
                }
                _ => tokio::time::sleep(SAMPLE).await,
            }
        }
    })
    .await??;
    stop.send(true)?;
    let during = collect(tasks, started).await?;
    let to = last_index(&log)?;
    let (fork_entries, all_entries) = entries(&log, from, to, target.as_str())?;
    // The staged copies go once the fork is ready; give the nodes a moment to delete them.
    tokio::time::sleep(Duration::from_secs(3)).await;
    let disk_after: u64 = disk(dir.path()).values().sum();

    let fork = ready
        .placement
        .as_ref()
        .and_then(|placement| placement.fork.as_ref())
        .ok_or("a ready fork without its fork status")?;
    let millis = |from: u64, to: Option<u64>| -> Fallible<Duration> {
        let to = to.ok_or("a ready fork without its times")?;
        Ok(Duration::from_millis(to.saturating_sub(from)))
    };
    let completed = fork
        .completed_at_ms
        .ok_or("a ready fork without its completion")?;
    let to_seeded = millis(fork.requested_at_ms, Some(completed))?;
    let to_ready = millis(completed, fork.ready_at_ms)?;
    let staged: BTreeMap<String, u64> = DATA
        .iter()
        .map(|id| {
            let bytes = fleet
                .daemon(id)
                .ok()
                .and_then(|daemon| metric(daemon, "fleetring_fork_seed_bytes_sum"))
                .map_or(0, whole);
            ((*id).to_owned(), bytes)
        })
        .collect();
    let measured = Measured {
        mib,
        // A small copy may stage between two samples: its seeder's staged bytes are its size.
        copy: copy.max(staged.get(&seeder).copied().unwrap_or_default()),
        seeder_is_source_primary: seeder == source_primary,
        seeder,
        to_seeded,
        to_ready,
        pins: metric(primary, "fleetring_fork_pin_seconds_count").map_or(0, whole),
        pinned: Duration::from_millis(
            metric(primary, "fleetring_fork_pin_seconds_sum")
                .map_or(0, |seconds| whole(seconds * 1_000.0)),
        ),
        pinned_wal,
        wal_file_growth: wal_peak.saturating_sub(wal_before),
        staged,
        disk_before,
        disk_peak,
        disk_after,
        node_peaks,
        baseline,
        during,
        fork_entries,
        all_entries,
        baseline_entries,
    };
    fleet.stop().await;
    Ok(measured)
}

/// `bytes` in MiB, or GiB from 1 GiB, to one decimal.
fn size(bytes: u64) -> String {
    let (unit, name) = if bytes >= 1 << 30 {
        (1 << 30, "GiB")
    } else {
        (MIB, "MiB")
    };
    let tenths = (u128::from(bytes) * 10 + u128::from(unit) / 2) / u128::from(unit);
    format!("{}.{} {name}", tenths / 10, tenths % 10)
}

/// `duration` in milliseconds below a second, and seconds to one decimal from one.
fn span(duration: Duration) -> String {
    if duration < Duration::from_secs(1) {
        format!("{} ms", duration.as_millis())
    } else {
        let tenths = (duration.as_millis() + 50) / 100;
        format!("{}.{} s", tenths / 10, tenths % 10)
    }
}

/// `duration` in milliseconds, to one decimal.
fn latency(duration: Duration) -> String {
    let tenths = (duration.as_micros() + 50) / 100;
    format!("{}.{} ms", tenths / 10, tenths % 10)
}

/// The measurements as `docs/perf.md`'s tables.
fn tables(measured: &[Measured]) -> String {
    let mut out = String::new();
    let _ = writeln!(
        out,
        "| Source | Copy | To `Seeded` | `Seeded` to `Ready` | Pins, held | WAL held by the pin, peak; WAL file growth during the fork | Staged per node | Fleet disk: before, peak, after | Per-node peak |"
    );
    let _ = writeln!(out, "|---|---|---|---|---|---|---|---|---|");
    for one in measured {
        let staged: Vec<String> = one
            .staged
            .iter()
            .map(|(node, bytes)| format!("{node} {}", size(*bytes)))
            .collect();
        let peaks: Vec<String> = one.node_peaks.values().map(|bytes| size(*bytes)).collect();
        let _ = writeln!(
            out,
            "| {} MiB | {} | {} | {} | {}, {} | {}, {} | {} | {}, {}, {} | {} |",
            one.mib,
            size(one.copy),
            span(one.to_seeded),
            span(one.to_ready),
            one.pins,
            span(one.pinned),
            size(one.pinned_wal),
            size(one.wal_file_growth),
            staged.join("; "),
            size(one.disk_before),
            size(one.disk_peak),
            size(one.disk_after),
            peaks.join(", "),
        );
    }
    let _ = writeln!(out);
    let _ = writeln!(
        out,
        "| Source | Seeder | Baseline: tx/s, p50, p99 | During the fork: tx/s, p50, p99 | Slowest write: baseline; during the fork, began after the request | Fork's control entries | All control entries during the fork (baseline's 10 s) |"
    );
    let _ = writeln!(out, "|---|---|---|---|---|---|---|");
    for one in measured {
        let kinds: Vec<String> = one
            .fork_entries
            .iter()
            .map(|(kind, count)| format!("{kind} {count}"))
            .collect();
        let total: u64 = one.fork_entries.values().sum();
        let _ = writeln!(
            out,
            "| {} MiB | {}{} | {}, {}, {} | {}, {}, {} | {}; {} at {} | {total}: {} | {} ({}) |",
            one.mib,
            one.seeder,
            if one.seeder_is_source_primary {
                " (the source's primary)"
            } else {
                ""
            },
            one.baseline.per_second(),
            latency(one.baseline.percentile(500)),
            latency(one.baseline.percentile(990)),
            one.during.per_second(),
            latency(one.during.percentile(500)),
            latency(one.during.percentile(990)),
            latency(one.baseline.slowest.1),
            latency(one.during.slowest.1),
            span(one.during.slowest.0),
            kinds.join(", "),
            one.all_entries,
            one.baseline_entries,
        );
    }
    out
}
