//! A whole fault-injection run: start the fleet, run the workload and the faults, heal, read
//! every replica's rows, and judge the history (ADR-0036, points 6 to 8).

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use fleetring_control::{DatabaseName, Liveness};
use fleetringd::Network;
use fleetringd::data::Client;
use fleetringd::sim::check::{self, read_replica_rows};
use fleetringd::sim::{History, Invariant, OutcomeClass, ReplicaRows, Replicated, history};
use fleetringd::tls::{Identity, Security};
use serde::Serialize;
use tokio::time::{Instant, sleep, sleep_until, timeout};

use crate::config::render_config;
use crate::docker::{
    Docker, DockerError, Runner, build_args, cert_client_args, cert_init_args, cert_node_args,
    logs_args, network_create_args, network_rm_args, rm_args, run_args, stop_args,
};
use crate::inject::{AppliedFault, InjectError, Injector, LocatePrimary, clock_dir, write_clock};
use crate::node::{Node, PRINCIPAL};
use crate::schedule::{Scenario, Schedule, workload_rng};
use crate::workload::{
    DATABASES, Recorder, WorkloadError, checked_guarantee, create_databases, database_name,
    final_reads, run_clients,
};
use crate::{Build, Mode, Teardown};

/// A run with fewer acknowledged writes than this tested too little to judge: a harness failure.
pub const MIN_ACKNOWLEDGED_WRITES: usize = 100;
/// How long the fleet may take to have five live members and a ring.
pub const READY_WITHIN: Duration = Duration::from_secs(60);
/// How long after the certificates are issued the fault phase may start, so that the first
/// fault, [`FIRST_FAULT`](crate::schedule::FIRST_FAULT) later, starts at least 65 s after them
/// and no clock skew puts a certificate outside its validity (ADR-0036, point 4).
pub const CERTIFICATE_SETTLE: Duration = Duration::from_secs(55);
/// How long the fleet runs quiet after the faults heal, before the final reads.
pub const QUIET_AFTER_HEAL: Duration = Duration::from_secs(30);
/// How long the harness waits for each database's replicas to agree with its primary.
pub const CONVERGE_WITHIN: Duration = Duration::from_secs(60);
/// How often the harness polls while it waits.
const POLL: Duration = Duration::from_millis(500);
/// The exit code of a run interrupted with Ctrl-C.
pub const INTERRUPTED: u8 = 130;

/// What to run.
#[derive(Debug, Clone)]
pub struct RunOptions {
    /// The fault kinds to inject.
    pub scenario: Scenario,
    /// The seed for the fault schedule and the workload.
    pub seed: u64,
    /// How long the fault phase runs.
    pub time_limit: Duration,
    /// Whether the run checks the fleet or the harness.
    pub mode: Mode,
    /// The node image.
    pub image: String,
    /// Whether to build the image first.
    pub build: Build,
    /// Where run directories go.
    pub out: PathBuf,
    /// What to leave behind.
    pub teardown: Teardown,
}

impl RunOptions {
    /// Returns the command that reruns this run's fault schedule and workload choices.
    #[must_use]
    pub fn rerun_command(&self) -> String {
        let mut command = format!(
            "cargo run -p fleetring-chaos --features harness --release -- run --scenario {} --seed {} --time-limit {}s",
            self.scenario,
            self.seed,
            self.time_limit.as_secs()
        );
        if self.mode == Mode::SelfTest {
            command.push_str(" --self-test");
        }
        command
    }
}

/// How many operations ended each way.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize)]
pub struct Counts {
    /// Acknowledged.
    pub ok: usize,
    /// Refused, with no effect.
    pub fail: usize,
    /// Unknown: they may or may not have taken effect.
    pub info: usize,
}

impl Counts {
    /// Counts `history`'s operations by outcome class.
    #[must_use]
    pub fn of(history: &History) -> Self {
        let mut counts = Self::default();
        for record in history.records() {
            match record.class() {
                OutcomeClass::Ok => counts.ok += 1,
                OutcomeClass::Fail => counts.fail += 1,
                OutcomeClass::Info => counts.info += 1,
            }
        }
        counts
    }
}

/// What the checks concluded.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum VerdictKind {
    /// Every check passed.
    Valid,
    /// A check found a violation.
    Invalid,
    /// The harness failed, so nothing was judged.
    Unknown,
}

/// What `verdict.json` holds.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Verdict {
    /// The scenario, in kebab case.
    pub scenario: String,
    /// The seed.
    pub seed: u64,
    /// Whether the run was a self-test.
    pub self_test: bool,
    /// The conclusion.
    pub verdict: VerdictKind,
    /// The broken invariant, when invalid.
    pub invariant: Option<String>,
    /// The violation's or the harness failure's text.
    pub detail: Option<String>,
    /// How the history's operations ended.
    pub operations: Counts,
    /// How many faults ran.
    pub faults: usize,
    /// How long the fault phase was planned to run, in seconds.
    pub time_limit_s: u64,
}

/// What [`judge`] concluded from a history and the rows on disk.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Judgement {
    /// The conclusion.
    pub kind: VerdictKind,
    /// The broken invariant, when invalid.
    pub invariant: Option<Invariant>,
    /// The violation's or the failure's text.
    pub detail: Option<String>,
    /// How the history's operations ended.
    pub operations: Counts,
    /// The process exit code the conclusion means in the run's mode.
    pub exit_code: u8,
}

/// The invariants a lost acknowledged write breaks: the self-test passes on any of them.
const LOST_WRITE: [Invariant; 4] = [
    Invariant::AcknowledgedWritesReplicated,
    Invariant::AcknowledgedWritesRead,
    Invariant::ReadYourWrites,
    Invariant::MonotonicReads,
];

/// Judges a run: FR-036's history checker over `history` and the rows each database's copies
/// hold, the convergence of each database's final replica set (`final_sets`, by node name),
/// and the replay of the control node's `control.db`, a one-voter control group's format-2 log,
/// from its oldest retained control snapshot or from genesis (`check::control_log_replays`). The
/// first violation decides. A run without a violation and
/// with fewer than [`MIN_ACKNOWLEDGED_WRITES`] acknowledged writes tested too little and is a
/// harness failure.
///
/// The exit code is 0 for valid, 1 for invalid, and 2 for a harness failure; in the
/// self-test, 0 for a lost acknowledged write, 1 for anything else the checks conclude, and 2
/// for a harness failure.
#[must_use]
pub fn judge(
    history: &History,
    databases: &BTreeMap<DatabaseName, Replicated>,
    final_sets: &BTreeMap<DatabaseName, Vec<String>>,
    control_log: Option<&Path>,
    mode: Mode,
) -> Judgement {
    let operations = Counts::of(history);
    let violation = history::check(history, databases)
        .and_then(|()| {
            for (database, replicated) in databases {
                let Some(set) = final_sets.get(database) else {
                    continue;
                };
                let rows: ReplicaRows = replicated
                    .rows()
                    .iter()
                    .filter(|(node, _)| set.contains(node))
                    .map(|(node, rows)| (node.clone(), rows.clone()))
                    .collect();
                check::replicas_converged(database, &rows)?;
            }
            Ok(())
        })
        .and_then(|()| match control_log {
            Some(path) => check::control_log_replays(path).map(drop),
            None => Ok(()),
        })
        .err();
    let (kind, invariant, detail) = match violation {
        Some(violation) => (
            VerdictKind::Invalid,
            Some(violation.invariant()),
            Some(violation.to_string()),
        ),
        None => {
            let acknowledged = acknowledged_writes(history);
            if acknowledged < MIN_ACKNOWLEDGED_WRITES {
                (
                    VerdictKind::Unknown,
                    None,
                    Some(format!(
                        "the workload acknowledged only {acknowledged} writes"
                    )),
                )
            } else {
                (VerdictKind::Valid, None, None)
            }
        }
    };
    Judgement {
        kind,
        invariant,
        detail,
        operations,
        exit_code: exit_code(kind, invariant, mode),
    }
}

/// How many writes `history` acknowledged.
fn acknowledged_writes(history: &History) -> usize {
    history
        .records()
        .iter()
        .filter(|record| {
            matches!(record.operation(), fleetringd::sim::Operation::Write { .. })
                && record.class() == OutcomeClass::Ok
        })
        .count()
}

/// The exit code for `kind` and the broken `invariant` in `mode`.
fn exit_code(kind: VerdictKind, invariant: Option<Invariant>, mode: Mode) -> u8 {
    match (mode, kind) {
        (_, VerdictKind::Unknown) => 2,
        (Mode::Normal, VerdictKind::Valid) => 0,
        (Mode::Normal, VerdictKind::Invalid) => 1,
        (Mode::SelfTest, VerdictKind::Invalid)
            if invariant.is_some_and(|invariant| LOST_WRITE.contains(&invariant)) =>
        {
            0
        }
        (Mode::SelfTest, _) => 1,
    }
}

/// Why the harness couldn't finish a run.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum RunError {
    /// The host isn't Linux.
    #[error("fleetring-chaos run needs Linux with Docker; this host runs {os}")]
    NotLinux {
        /// The host's OS.
        os: &'static str,
    },
    /// A docker command failed.
    #[error(transparent)]
    Docker(#[from] DockerError),
    /// The fault injector failed.
    #[error("the fault injector failed: {0}")]
    Inject(#[from] InjectError),
    /// The workload couldn't set up or finish.
    #[error(transparent)]
    Workload(#[from] WorkloadError),
    /// A file of the run directory couldn't be written or read.
    #[error("can't use {path}: {source}")]
    Io {
        /// The file.
        path: PathBuf,
        /// The error.
        source: io::Error,
    },
    /// The client's certificate couldn't be loaded.
    #[error("can't load the client certificate: {0}")]
    Identity(String),
    /// The fleet didn't come up in time.
    #[error("the fleet didn't have five live members and a ring within {seconds} s")]
    NotReady {
        /// How long the harness waited.
        seconds: u64,
    },
}

fn io_error(path: &Path) -> impl FnOnce(io::Error) -> RunError + '_ {
    move |source| RunError::Io {
        path: path.to_owned(),
        source,
    }
}

/// Runs everything and returns the process exit code: 0, 1, or 2 (ADR-0036, point 6), or 130
/// after Ctrl-C, once every fault is undone and the fleet removed.
pub async fn run(options: RunOptions) -> u8 {
    let dir = run_dir(&options);
    tokio::select! {
        code = run_to_verdict(&options, &dir) => code,
        _ = tokio::signal::ctrl_c() => {
            eprintln!("fleetring-chaos: interrupted; undoing every fault and removing the fleet");
            let runner: Arc<dyn Runner> = Arc::new(Docker);
            let nobody: LocatePrimary = Arc::new(|_| Box::pin(async { None }));
            let _ = Injector::new(runner, dir.clone(), nobody).heal_all().await;
            if options.teardown == Teardown::Remove {
                remove_fleet(&Docker);
            }
            INTERRUPTED
        }
    }
}

/// The run's directory: `<out>/<scenario>-<seed>`, absolute, since Docker mounts from it.
fn run_dir(options: &RunOptions) -> PathBuf {
    let relative = options
        .out
        .join(format!("{}-{}", options.scenario, options.seed));
    std::path::absolute(&relative).unwrap_or(relative)
}

/// What a run produced, for its files and its exit code.
struct Outcome {
    history: History,
    applied: Vec<AppliedFault>,
    /// When the fault phase started, in the history's time.
    phase_start: Duration,
    judgement: Judgement,
}

async fn run_to_verdict(options: &RunOptions, dir: &Path) -> u8 {
    let docker = Docker;
    let recorder = Recorder::new(Instant::now());
    let mut applied = Vec::new();
    let mut phase_start = Duration::ZERO;
    let mut started = false;
    let result = phases(
        options,
        dir,
        &recorder,
        &mut applied,
        &mut phase_start,
        &mut started,
    )
    .await;
    let history = recorder.history();
    let judgement = match result {
        Ok(judgement) => judgement,
        Err(error) => {
            if started {
                collect_logs(&docker, dir);
            }
            Judgement {
                kind: VerdictKind::Unknown,
                invariant: None,
                detail: Some(error.to_string()),
                operations: Counts::of(&history),
                exit_code: 2,
            }
        }
    };
    let outcome = Outcome {
        history,
        applied,
        phase_start,
        judgement,
    };
    if let Err(error) = write_files(options, dir, &outcome) {
        eprintln!("fleetring-chaos: {error}");
    }
    if started && options.teardown == Teardown::Remove {
        remove_fleet(&docker);
    }
    report(options, dir, &outcome.judgement);
    outcome.judgement.exit_code
}

/// Steps 1 to 17: everything from the host check to the judgement. `started` is set once
/// containers may exist, and `applied` and `phase_start` are filled as the faults run, so the
/// caller writes them even when a later step fails.
async fn phases(
    options: &RunOptions,
    dir: &Path,
    recorder: &Recorder,
    applied: &mut Vec<AppliedFault>,
    phase_start: &mut Duration,
    started: &mut bool,
) -> Result<Judgement, RunError> {
    let docker = Docker;
    if std::env::consts::OS != "linux" {
        return Err(RunError::NotLinux {
            os: std::env::consts::OS,
        });
    }
    let version = docker
        .run_async(vec![
            "version".to_owned(),
            "--format".to_owned(),
            "{{.Server.Version}}".to_owned(),
        ])
        .await?;
    eprintln!(
        "fleetring-chaos: docker {version}; run directory {}",
        dir.display()
    );
    let user = prepare_dir(dir)?;
    let dir_text = dir.to_string_lossy().into_owned();
    let tls_text = format!("{dir_text}/tls");
    if options.build == Build::Image {
        eprintln!("fleetring-chaos: building {}", options.image);
        let root = Path::new(concat!(env!("CARGO_MANIFEST_DIR"), "/../.."));
        let args = build_args(&options.image);
        let root = root.to_owned();
        tokio::task::spawn_blocking(move || docker.run_in(&root, &args))
            .await
            .map_err(|error| DockerError::Join {
                reason: error.to_string(),
            })??;
    }
    *started = true;
    remove_fleet(&docker);
    docker.run_async(network_create_args()).await?;
    eprintln!("fleetring-chaos: issuing the fleet's certificates");
    docker
        .run_async(cert_init_args(&tls_text, &options.image, &user))
        .await?;
    for node in Node::ALL {
        docker
            .run_async(cert_node_args(node, &tls_text, &options.image, &user))
            .await?;
    }
    docker
        .run_async(cert_client_args(&tls_text, &options.image, &user))
        .await?;
    let issued = Instant::now();
    for node in Node::ALL {
        docker
            .run_async(run_args(node, &dir_text, &options.image, &user))
            .await?;
    }
    let tls = dir.join("tls");
    let identity = Identity::load(
        &tls.join("ca.crt"),
        &tls.join(format!("client.{PRINCIPAL}.crt")),
        &tls.join(format!("client.{PRINCIPAL}.key")),
    )
    .map_err(|error| RunError::Identity(error.to_string()))?;
    let network = Network::default().with_security(Security::Tls(identity));
    let harness = Client::new(Node::CONTROL.address(), network.clone());
    await_ready(&harness).await?;
    eprintln!("fleetring-chaos: five members are live; creating the databases");
    create_databases(recorder, &harness, options.mode).await?;
    sleep_until(issued + CERTIFICATE_SETTLE).await;

    let schedule = Schedule::generate(
        options.scenario,
        options.seed,
        options.time_limit,
        options.mode,
    );
    let runner: Arc<dyn Runner> = Arc::new(docker);
    let injector = Injector::new(runner, dir.to_owned(), locate_primary(&harness));
    let t0 = Instant::now();
    *phase_start = recorder.elapsed();
    let deadline = t0 + options.time_limit;
    eprintln!(
        "fleetring-chaos: fault phase of {} s with {} planned faults",
        options.time_limit.as_secs(),
        schedule.faults.len()
    );
    let mut root = workload_rng(options.seed);
    let clients = run_clients(recorder.clone(), network.clone(), &mut root, deadline);
    let faults = injector.clone().run(&schedule, t0, deadline);
    let ((), injected) = tokio::join!(clients, faults);
    let heal = injector.heal_all().await;
    *applied = injected?;
    heal?;

    eprintln!(
        "fleetring-chaos: healed; quiet for {} s",
        QUIET_AFTER_HEAL.as_secs()
    );
    sleep(QUIET_AFTER_HEAL).await;
    final_reads(recorder, &harness).await?;
    let final_sets = converge(&harness).await;
    collect_logs(&docker, dir);
    for node in Node::ALL {
        docker.run_async(stop_args(&node.container())).await?;
    }

    let mut databases = BTreeMap::new();
    for index in 0..DATABASES {
        let database = database_name(index);
        let mut rows = ReplicaRows::new();
        for node in Node::ALL {
            let data_dir = dir.join("nodes").join(node.name()).join("data");
            let held = read_replica_rows(&data_dir, &database).map_err(|error| RunError::Io {
                path: data_dir.clone(),
                source: io::Error::other(error.to_string()),
            })?;
            rows.insert(node.name().to_owned(), held);
        }
        databases.insert(database, Replicated::new(checked_guarantee(index), rows));
    }
    let control_log = dir
        .join("nodes")
        .join(Node::CONTROL.name())
        .join("data")
        .join("control.db");
    Ok(judge(
        &recorder.history(),
        &databases,
        &final_sets,
        Some(&control_log),
        options.mode,
    ))
}

/// Creates the run directory afresh and returns its owner as `uid:gid`, the user the
/// containers run as.
fn prepare_dir(dir: &Path) -> Result<String, RunError> {
    if dir.exists() {
        std::fs::remove_dir_all(dir).map_err(io_error(dir))?;
    }
    let tls = dir.join("tls");
    std::fs::create_dir_all(&tls).map_err(io_error(&tls))?;
    for node in Node::ALL {
        let node_dir = dir.join("nodes").join(node.name());
        let data = node_dir.join("data");
        std::fs::create_dir_all(&data).map_err(io_error(&data))?;
        let clock = clock_dir(dir, node);
        std::fs::create_dir_all(&clock).map_err(io_error(&clock))?;
        write_clock(&clock, 0)?;
        let config = node_dir.join("fleetringd.toml");
        std::fs::write(&config, render_config(node)).map_err(io_error(&config))?;
    }
    owner(dir)
}

#[cfg(unix)]
fn owner(dir: &Path) -> Result<String, RunError> {
    use std::os::unix::fs::MetadataExt;
    let metadata = std::fs::metadata(dir).map_err(io_error(dir))?;
    Ok(format!("{}:{}", metadata.uid(), metadata.gid()))
}

#[cfg(not(unix))]
fn owner(_dir: &Path) -> Result<String, RunError> {
    Err(RunError::NotLinux {
        os: std::env::consts::OS,
    })
}

/// Waits until the control node reports five live members and a ring.
async fn await_ready(harness: &Client) -> Result<(), RunError> {
    let give_up = Instant::now() + READY_WITHIN;
    loop {
        if let Ok(Ok(view)) = timeout(POLL * 4, harness.status()).await {
            let live = view
                .members()
                .filter(|(_, liveness)| *liveness == Liveness::Live)
                .count();
            if live == Node::ALL.len() && view.ring().is_some() {
                return Ok(());
            }
        }
        if Instant::now() > give_up {
            return Err(RunError::NotReady {
                seconds: READY_WITHIN.as_secs(),
            });
        }
        sleep(POLL).await;
    }
}

/// A [`LocatePrimary`] over the harness's own client, whose calls aren't in the history.
fn locate_primary(harness: &Client) -> LocatePrimary {
    let harness = harness.clone();
    Arc::new(move |index| {
        let harness = harness.clone();
        Box::pin(async move {
            let location = timeout(
                Duration::from_secs(2),
                harness.locate(&database_name(index)),
            )
            .await
            .ok()?
            .ok()?;
            Node::from_node_id(location.primary.as_ref()?)
        })
    })
}

/// Waits, for up to [`CONVERGE_WITHIN`] per database, until every replica's hash chain agrees
/// with its primary's at the primary's position, and returns each database's replica set from
/// the last answer. A database that doesn't converge in time isn't an error here: the checks
/// judge its copies.
async fn converge(harness: &Client) -> BTreeMap<DatabaseName, Vec<String>> {
    let mut sets = BTreeMap::new();
    for index in 0..DATABASES {
        let database = database_name(index);
        let give_up = Instant::now() + CONVERGE_WITHIN;
        loop {
            let (converged, set) = converged(harness, &database).await;
            if let Some(set) = set {
                sets.insert(database.clone(), set);
            }
            if converged || Instant::now() > give_up {
                break;
            }
            sleep(POLL).await;
        }
    }
    sets
}

/// Whether `database`'s replicas agree with its primary now, and its replica set, if known.
async fn converged(harness: &Client, database: &DatabaseName) -> (bool, Option<Vec<String>>) {
    let call = Duration::from_secs(5);
    let Ok(Ok(location)) = timeout(call, harness.locate(database)).await else {
        return (false, None);
    };
    let set: Vec<String> = location
        .replicas
        .iter()
        .map(|replica| replica.node.to_string())
        .collect();
    let Some(primary) = location.primary.as_ref().and_then(Node::from_node_id) else {
        return (false, Some(set));
    };
    let Ok(Ok(at_primary)) =
        timeout(call, harness.verify(&primary.address(), database, None)).await
    else {
        return (false, Some(set));
    };
    for replica in &location.replicas {
        let Some(node) = Node::from_node_id(&replica.node) else {
            return (false, Some(set));
        };
        if node == primary {
            continue;
        }
        let answer = timeout(
            call,
            harness.verify(&node.address(), database, at_primary.position.as_ref()),
        )
        .await;
        match answer {
            Ok(Ok(verification)) if verification.chain_hash == at_primary.chain_hash => {}
            _ => return (false, Some(set)),
        }
    }
    (true, Some(set))
}

/// Writes each container's output to `nodes/<node>.log`; a container that is gone is skipped.
fn collect_logs(docker: &Docker, dir: &Path) {
    for node in Node::ALL {
        if let Ok(text) = docker.run_combined(&logs_args(&node.container())) {
            let _ = std::fs::write(dir.join("nodes").join(format!("{}.log", node.name())), text);
        }
    }
}

/// Removes the five containers and the network, whichever exist.
pub fn remove_fleet(docker: &Docker) {
    let containers: Vec<String> = Node::ALL.into_iter().map(Node::container).collect();
    docker.run_ignoring_errors(&rm_args(&containers));
    docker.run_ignoring_errors(&network_rm_args());
}

/// Writes `history.txt`, `schedule.txt`, and `verdict.json` into the run directory.
fn write_files(options: &RunOptions, dir: &Path, outcome: &Outcome) -> Result<(), RunError> {
    std::fs::create_dir_all(dir).map_err(io_error(dir))?;
    let mut history = String::new();
    for event in outcome.history.events() {
        let _ = writeln!(history, "{event}");
    }
    let path = dir.join("history.txt");
    std::fs::write(&path, history).map_err(io_error(&path))?;

    let schedule = Schedule::generate(
        options.scenario,
        options.seed,
        options.time_limit,
        options.mode,
    );
    let mut text = format!(
        "# {} seed {}: times count from the fault phase's start, {} ms into history.txt\n",
        options.scenario,
        options.seed,
        outcome.phase_start.as_millis()
    );
    for fault in &outcome.applied {
        let _ = writeln!(text, "{fault}");
    }
    for planned in schedule.faults.iter().skip(outcome.applied.len()) {
        let _ = writeln!(text, "{planned} (not run)");
    }
    let path = dir.join("schedule.txt");
    std::fs::write(&path, text).map_err(io_error(&path))?;

    let judgement = &outcome.judgement;
    let verdict = Verdict {
        scenario: options.scenario.to_string(),
        seed: options.seed,
        self_test: options.mode == Mode::SelfTest,
        verdict: judgement.kind,
        invariant: judgement.invariant.map(|invariant| invariant.to_string()),
        detail: judgement.detail.clone(),
        operations: judgement.operations,
        faults: outcome.applied.len(),
        time_limit_s: options.time_limit.as_secs(),
    };
    let json = serde_json::to_string_pretty(&verdict).map_err(|error| RunError::Io {
        path: dir.join("verdict.json"),
        source: io::Error::other(error),
    })?;
    let path = dir.join("verdict.json");
    std::fs::write(&path, json + "\n").map_err(io_error(&path))
}

/// Prints the verdict, and on a nonzero exit the seed, the run directory, and the rerun command.
fn report(options: &RunOptions, dir: &Path, judgement: &Judgement) {
    let verdict = match judgement.kind {
        VerdictKind::Valid => "valid",
        VerdictKind::Invalid => "invalid",
        VerdictKind::Unknown => "unknown",
    };
    let detail = judgement.detail.as_deref().unwrap_or("");
    if options.mode == Mode::SelfTest {
        if judgement.exit_code == 0 {
            eprintln!(
                "self-test passed: the checker found {}",
                judgement
                    .invariant
                    .map_or_else(String::new, |invariant| invariant.to_string())
            );
        } else if judgement.exit_code == 1 {
            eprintln!("self-test failed: a run that loses acknowledged writes looked valid");
        }
    }
    let operations = judgement.operations;
    eprintln!(
        "fleetring-chaos: verdict {verdict} ({} ok, {} fail, {} info)",
        operations.ok, operations.fail, operations.info
    );
    if judgement.exit_code != 0 {
        eprintln!(
            "fleetring-chaos: {} seed {}: {verdict}: {detail}",
            options.scenario, options.seed
        );
        eprintln!("  run directory: {}", dir.display());
        eprintln!("  rerun: {}", options.rerun_command());
        eprintln!(
            "  the rerun replays the fault schedule and the workload's choices, not the processes' timing"
        );
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use fleetring::{NodeId, ReplicationFactor};
    use fleetring_control::LeaseEpoch;
    use fleetringd::sim::{Ack, DurabilityMode, Guarantee, Operation, Outcome};

    use super::*;

    fn db() -> DatabaseName {
        database_name(0)
    }

    fn n(name: &str) -> NodeId {
        name.parse().expect("node ID")
    }

    /// A history with the database created and `writes` acknowledged writes of 1, 2, ...
    fn history(writes: i64) -> History {
        let mut history = History::default();
        history.record(
            "setup",
            Duration::ZERO,
            Duration::from_millis(1),
            Operation::Create {
                database: db(),
                replication_factor: 3,
                durability: DurabilityMode::Quorum,
            },
            Outcome::Acknowledged(Ack::Opened {
                primary: n("n2"),
                epoch: LeaseEpoch::new(1),
                durability: DurabilityMode::Quorum,
            }),
        );
        for value in 1..=writes {
            let at = Duration::from_millis(u64::try_from(value).expect("positive") * 10);
            history.record(
                "client-0",
                at,
                at + Duration::from_millis(5),
                Operation::Write {
                    database: db(),
                    value,
                },
                Outcome::Acknowledged(Ack::Written {
                    acknowledged_by: 2,
                    replicas: 3,
                    under_replicated: false,
                    primary: Some(n("n2")),
                    epoch: LeaseEpoch::new(1),
                }),
            );
        }
        history
    }

    fn databases(rows: [BTreeSet<i64>; 3]) -> BTreeMap<DatabaseName, Replicated> {
        let rows: ReplicaRows = ["n2", "n3", "n4"]
            .into_iter()
            .map(str::to_owned)
            .zip(rows)
            .collect();
        let guarantee = Guarantee::new(
            ReplicationFactor::new(3).expect("factor"),
            DurabilityMode::Quorum,
        );
        BTreeMap::from([(db(), Replicated::new(guarantee, rows))])
    }

    fn final_sets() -> BTreeMap<DatabaseName, Vec<String>> {
        BTreeMap::from([(
            db(),
            vec!["n2".to_owned(), "n3".to_owned(), "n4".to_owned()],
        )])
    }

    #[test]
    fn a_lost_acknowledged_write_is_invalid() {
        let history = history(7);
        let held: BTreeSet<i64> = (1..=6).collect();
        let judged = judge(
            &history,
            &databases([held.clone(), held.clone(), held]),
            &final_sets(),
            None,
            Mode::Normal,
        );
        assert_eq!(judged.kind, VerdictKind::Invalid, "{judged:?}");
        assert_eq!(
            judged
                .invariant
                .map(|invariant| invariant.to_string())
                .as_deref(),
            Some("acknowledged writes are on a write quorum")
        );
        assert_eq!(judged.exit_code, 1);
    }

    #[test]
    fn the_self_test_passes_only_on_a_lost_write() {
        let lost = history(7);
        let held: BTreeSet<i64> = (1..=6).collect();
        let judged = judge(
            &lost,
            &databases([held.clone(), held.clone(), held]),
            &final_sets(),
            None,
            Mode::SelfTest,
        );
        assert_eq!(judged.exit_code, 0, "{judged:?}");

        let kept = history(150);
        let all: BTreeSet<i64> = (1..=150).collect();
        let judged = judge(
            &kept,
            &databases([all.clone(), all.clone(), all]),
            &final_sets(),
            None,
            Mode::SelfTest,
        );
        assert_eq!(judged.kind, VerdictKind::Valid, "{judged:?}");
        assert_eq!(judged.exit_code, 1);
    }

    #[test]
    fn a_quiet_workload_is_a_harness_failure() {
        let history = history(99);
        let all: BTreeSet<i64> = (1..=99).collect();
        let judged = judge(
            &history,
            &databases([all.clone(), all.clone(), all]),
            &final_sets(),
            None,
            Mode::Normal,
        );
        assert_eq!(judged.kind, VerdictKind::Unknown, "{judged:?}");
        assert_eq!(judged.exit_code, 2);
        assert_eq!(
            judged.detail.as_deref(),
            Some("the workload acknowledged only 99 writes")
        );
        assert_eq!(
            judged.operations,
            Counts {
                ok: 100,
                fail: 0,
                info: 0
            }
        );
    }

    #[test]
    fn a_valid_run_exits_zero() {
        let history = history(100);
        let all: BTreeSet<i64> = (1..=100).collect();
        let judged = judge(
            &history,
            &databases([all.clone(), all.clone(), all]),
            &final_sets(),
            None,
            Mode::Normal,
        );
        assert_eq!(judged.kind, VerdictKind::Valid, "{judged:?}");
        assert_eq!(judged.exit_code, 0);
    }

    #[test]
    fn replicas_of_the_final_set_must_converge() {
        let history = history(100);
        let all: BTreeSet<i64> = (1..=100).collect();
        let mut extra = all.clone();
        extra.insert(1_000);
        let judged = judge(
            &history,
            &databases([all.clone(), all, extra]),
            &final_sets(),
            None,
            Mode::Normal,
        );
        assert_eq!(
            judged.invariant,
            Some(Invariant::ReplicasConverged),
            "{judged:?}"
        );
    }

    #[test]
    fn verdicts_serialize_in_lowercase() {
        let verdict = Verdict {
            scenario: "mixed".to_owned(),
            seed: 7,
            self_test: false,
            verdict: VerdictKind::Valid,
            invariant: None,
            detail: None,
            operations: Counts::default(),
            faults: 3,
            time_limit_s: 600,
        };
        let json = serde_json::to_string(&verdict).expect("json");
        assert!(json.contains("\"verdict\":\"valid\""), "{json}");
    }

    #[test]
    fn the_rerun_command_names_the_seed_and_mode() {
        let options = RunOptions {
            scenario: Scenario::Partitions,
            seed: 42,
            time_limit: Duration::from_secs(600),
            mode: Mode::SelfTest,
            image: "img".to_owned(),
            build: Build::Skip,
            out: PathBuf::from("target/chaos"),
            teardown: Teardown::Remove,
        };
        assert_eq!(
            options.rerun_command(),
            "cargo run -p fleetring-chaos --features harness --release -- run --scenario partitions --seed 42 --time-limit 600s --self-test"
        );
    }
}
