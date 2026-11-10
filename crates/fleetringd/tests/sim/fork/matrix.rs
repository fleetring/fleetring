//! FR-087's failure matrix: the harness every row runs on.
//!
//! A run is a control host and five data hosts running the real daemon, a [`Traffic`] client
//! that writes continuously to the source `src`, and from the fork request on to the fork too,
//! with reads between, and an operator. The fork's work is held at one seeding step by the
//! data hosts' fault hooks, the row's fault is injected there, and the work goes on; the run
//! ends once the fork reaches the outcome the row expects, the traffic stops, and
//! [`Fleet::finish`] runs every invariant, the fork properties (a) to (i) among them.
//!
//! Each row runs once per [`Point`] on CI's fixed seed, and the ignored sweep runs 1 000 seeds per
//! row, its point chosen by the seed ([`sweep`]).

use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::num::{NonZeroU32, NonZeroU64};
use std::time::Duration;

use fleetring::{NodeId, ReplicationFactor};
use fleetring_control::fork::{ForkIdentity, ForkState, OperationId, OperationOutcome};
use fleetring_control::{DatabaseName, State};
use fleetringd::ForkConfig;
use fleetringd::changes::{Format, SinkConfig, SinkKind};
use fleetringd::engine::CaptureMode;
use fleetringd::replication::{FaultPoint, ForkFaults};
use fleetringd::sim::{
    Expected, Fleet, Operation, Operator, Outcome, Script, Traffic, TrafficHandle, seeds,
};

/// The control host.
pub(super) const CONTROL: &str = "control";

/// The data hosts.
pub(super) const NODES: [&str; 5] = ["n1", "n2", "n3", "n4", "n5"];

/// How long a run may wait for any one thing, in simulated time.
pub(super) const LIMIT: Duration = Duration::from_secs(90);

/// The fork's replication factor.
pub(super) const REPLICAS: u32 = 3;

/// The test binary, for rerun commands.
const BINARY: &str = crate::BINARY;

/// The seed every row runs each point on in CI.
pub(super) const FIXED: [u64; 1] = [87];

/// The environment variable that runs a row at one point alone, named as `{:?}` prints it.
const POINT_VAR: &str = "FLEETRING_SIM_POINT";

/// The environment variable that prints the daemons' logs through this filter.
const LOG_VAR: &str = "FLEETRING_SIM_LOG";

/// Prints the daemons' logs when [`LOG_VAR`] names a filter, for debugging a row.
fn logs() {
    if let Ok(filter) = std::env::var(LOG_VAR) {
        let _ = tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::new(filter))
            .with_test_writer()
            .try_init();
    }
}

/// The environment variable that sets how many seeds [`sweep`] runs per row.
const SWEEP_SEEDS: &str = "FLEETRING_SIM_SWEEP_SEEDS";

/// The environment variable that sets the first seed [`sweep`] runs.
const SWEEP_FROM: &str = "FLEETRING_SIM_SWEEP_FROM";

/// The seeding steps a fault is injected at (FR-087).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(super) enum Point {
    /// Before the seeder asks the source's primary for its copy: before the pin.
    BeforePin,
    /// While the source's primary holds the pin and the seeder's stream is open.
    DuringStream,
    /// After the seeder staged its copy, before it reports it.
    AfterSeederStaged,
    /// While the peers copy the seeder's staged copy: after they staged it, before they report.
    DuringPeerCopies,
    /// Between `CompleteFork` and the first lease.
    BeforeFirstLease,
    /// During the fork-reset write.
    DuringReset,
}

impl Point {
    /// Every point, in seeding order.
    pub(super) const ALL: [Self; 6] = [
        Self::BeforePin,
        Self::DuringStream,
        Self::AfterSeederStaged,
        Self::DuringPeerCopies,
        Self::BeforeFirstLease,
        Self::DuringReset,
    ];

    /// The points while the fork is seeding.
    pub(super) const SEEDING: [Self; 4] = [
        Self::BeforePin,
        Self::DuringStream,
        Self::AfterSeederStaged,
        Self::DuringPeerCopies,
    ];

    /// The hook that holds the fork's work at the point.
    pub(super) fn hook(self) -> FaultPoint {
        match self {
            Self::BeforePin => FaultPoint::Fetch,
            Self::DuringStream => FaultPoint::Streaming,
            Self::AfterSeederStaged | Self::DuringPeerCopies => FaultPoint::Staged,
            Self::BeforeFirstLease => FaultPoint::FirstLease,
            Self::DuringReset => FaultPoint::DuringReset,
        }
    }

    /// Whether the fork is still seeding there.
    pub(super) fn is_seeding(self) -> bool {
        Self::SEEDING.contains(&self)
    }
}

/// What a run starts with, beyond the defaults.
#[derive(Debug, Clone, Default)]
pub(super) struct Setup {
    /// A logical file sink of every database on every data host.
    pub(super) sinks: bool,
    /// Bytes of padding written to the source before the fork, so that its copy streams in
    /// several chunks.
    pub(super) pad: Option<u32>,
    /// The longest a source's primary pins a copy, in milliseconds.
    pub(super) max_pin_ms: Option<u64>,
}

/// The `[fork]` limits every host runs the matrix with: attempts and timeouts short enough for
/// simulated minutes, and no admission margin beyond the copy and a tenth.
fn fork_config(setup: &Setup) -> ForkConfig {
    let nonzero = |value: u64| NonZeroU64::new(value).expect("nonzero");
    ForkConfig {
        max_attempts: NonZeroU32::new(6).expect("nonzero"),
        deadline_ms: nonzero(120_000),
        attempt_timeout_ms: nonzero(20_000),
        seeder_timeout_ms: nonzero(3_000),
        transfer_retries: 3,
        transfer_backoff_ms: nonzero(100),
        max_pin_ms: nonzero(setup.max_pin_ms.unwrap_or(15_000)),
        max_pin_ms_per_hour: nonzero(30 * 60 * 1_000),
        admission_min_free_bytes: 0,
        admission_min_free_percent: 0,
        ..ForkConfig::default()
    }
}

/// A run of one row at one point.
pub(super) struct Run {
    pub(super) fleet: Fleet,
    pub(super) traffic: TrafficHandle,
    pub(super) operator: Operator,
    pub(super) source: DatabaseName,
    /// Each fork's target and operation, in the order they were asked for.
    pub(super) forks: Vec<(DatabaseName, OperationId)>,
    /// The seeder of each fork's first attempt.
    pub(super) seeders: BTreeMap<OperationId, String>,
    /// Clients made so far, for unique names.
    clients: usize,
    /// Hosts crashed and not restarted.
    pub(super) down: BTreeSet<String>,
}

pub(super) type Result<T> = std::result::Result<T, Box<dyn Error>>;

impl Run {
    /// Starts a fleet, writes to the source until 20 writes are acknowledged, and pads it if
    /// `setup` says.
    pub(super) fn start(seed: u64, setup: &Setup) -> Result<Self> {
        let mut builder = Fleet::builder(seed).control(CONTROL);
        for node in NODES {
            builder = builder.data(node);
        }
        let config = fork_config(setup);
        let sinks = setup.sinks;
        for host in NODES.iter().chain([&CONTROL]) {
            builder = builder.configure(host, move |host| {
                host.fork = config;
                if sinks && host.role.hosts_data() {
                    host.changes.sink.push(SinkConfig {
                        name: "log".to_owned(),
                        kind: SinkKind::File,
                        format: Format::Logical,
                        capture: CaptureMode::default(),
                        databases: vec!["*".parse().expect("a star")],
                        path: Some(host.data_dir.join("sink")),
                        url: None,
                        timeout_ms: 1_000,
                        retries: 1,
                        ca: None,
                        cert: None,
                        key: None,
                        token_file: None,
                        token_env: None,
                        allow_http: false,
                    });
                }
            });
        }
        let mut fleet = builder.build()?;
        fleet.allow_binding_refusals();
        fleet.await_ring(Duration::from_secs(10))?;
        let source: DatabaseName = "src".parse()?;
        let (traffic, handle) = Traffic::new([(source.clone(), 3)], 0);
        fleet.client("traffic", traffic);
        let operator = Operator::new();
        fleet.client("operator", operator.clone());
        let mut run = Self {
            fleet,
            traffic: handle,
            operator,
            source,
            forks: Vec::new(),
            seeders: BTreeMap::new(),
            clients: 0,
            down: BTreeSet::new(),
        };
        let source = run.source.clone();
        run.await_writes(&source, 20)?;
        if let Some(bytes) = setup.pad {
            run.script([Operation::Pad {
                database: source.clone(),
                bytes,
            }])?;
            run.await_writes(&source, 30)?;
        }
        Ok(run)
    }

    /// Runs `operations` on a client of their own, each until it is answered, and waits for
    /// them.
    pub(super) fn script(&mut self, operations: impl IntoIterator<Item = Operation>) -> Result<()> {
        let operations: Vec<Operation> = operations.into_iter().collect();
        let count = operations.len();
        let name = self.spawn_script(operations);
        self.fleet.run_until(LIMIT, "a script finishing", |fleet| {
            fleet
                .history()
                .records()
                .iter()
                .filter(|record| {
                    record.client() == name
                        && !matches!(record.outcome(), Outcome::Indeterminate { .. })
                })
                .count()
                >= count
        })?;
        Ok(())
    }

    /// Holds a fork's work at `point`, asks for the fork of the source as `target`, and waits
    /// until its work arrives there. Returns the operation and the host held there: for
    /// [`Point::DuringPeerCopies`], a peer, once the seeder's copy has been let go.
    pub(super) fn fork_to(
        &mut self,
        target: &DatabaseName,
        point: Point,
    ) -> Result<(OperationId, String)> {
        let operation = self.operation_id();
        self.hold(point.hook(), operation)?;
        self.fork_as(target, operation)?;
        let host = self.reach(operation, point)?;
        Ok((operation, host))
    }

    /// Waits until `operation`'s work, held at `point`, arrives there, and returns the host.
    pub(super) fn reach(&mut self, operation: OperationId, point: Point) -> Result<String> {
        let hook = point.hook();
        let first = self.await_arrival(hook, &BTreeMap::new())?;
        if point != Point::DuringPeerCopies {
            return Ok(first);
        }
        let counts = self.arrivals(hook)?;
        self.faults(&first)?.release_operation(hook, operation);
        self.await_arrival(hook, &counts)
    }

    /// Starts `operations` on a client of their own, each run until it is answered, without
    /// waiting, and returns the client's name.
    pub(super) fn spawn_script(
        &mut self,
        operations: impl IntoIterator<Item = Operation>,
    ) -> String {
        self.clients += 1;
        let name = format!("script-{}", self.clients);
        self.fleet.client(&name, Script::new(operations));
        name
    }

    /// Waits until `count` writes to `database` were acknowledged.
    pub(super) fn await_writes(&mut self, database: &DatabaseName, count: usize) -> Result<()> {
        let traffic = self.traffic.clone();
        let what = format!("{count} writes to {database}");
        let database = database.clone();
        let start = traffic.acknowledged(&database);
        self.fleet.run_until(LIMIT, &what, |_| {
            traffic.acknowledged(&database) >= start.max(count)
        })?;
        Ok(())
    }

    /// Waits until `count` more writes to `database` are acknowledged.
    pub(super) fn await_more_writes(
        &mut self,
        database: &DatabaseName,
        count: usize,
    ) -> Result<()> {
        let target = self.traffic.acknowledged(database) + count;
        self.await_writes(database, target)
    }

    /// A fresh fork operation ID, from the run's generator.
    pub(super) fn operation_id(&mut self) -> OperationId {
        let mut bytes = [0_u8; 16];
        bytes[..8].copy_from_slice(&self.fleet.rng().next_u64().to_be_bytes());
        bytes[8..].copy_from_slice(&self.fleet.rng().next_u64().to_be_bytes());
        OperationId::from_bytes(bytes)
    }

    /// The control plane's state now.
    pub(super) fn state(&self) -> Result<std::sync::Arc<State>> {
        Ok(self
            .fleet
            .control_state()
            .ok_or("the control node is down")?)
    }

    /// The replica set the ring gives `name` with `replicas` replicas, in preference order.
    pub(super) fn placement(&self, name: &str, replicas: u32) -> Result<Vec<String>> {
        let state = self.state()?;
        let factor = ReplicationFactor::new(u8::try_from(replicas)?).ok_or("zero replicas")?;
        Ok(state
            .ring()?
            .replicas_n(name, factor)
            .to_ids()
            .into_iter()
            .map(|id| id.to_string())
            .collect())
    }

    /// The first name `prefix-N` whose placement with `replicas` replicas satisfies `wanted`.
    pub(super) fn pick(
        &self,
        prefix: &str,
        replicas: u32,
        wanted: impl Fn(&[String]) -> bool,
    ) -> Result<DatabaseName> {
        for n in 0..500 {
            let name = format!("{prefix}-{n}");
            if wanted(&self.placement(&name, replicas)?) {
                return Ok(name.parse()?);
            }
        }
        Err(format!("no {prefix}-N is placed as the row needs").into())
    }

    /// The host that holds `database`'s lease now.
    pub(super) fn primary(&self, database: &DatabaseName) -> Result<String> {
        Ok(self
            .state()?
            .database(database)
            .and_then(|db| db.lease())
            .map(|lease| lease.holder().to_string())
            .ok_or_else(|| format!("{database} has no primary"))?)
    }

    /// The followers of the source in its replica set now.
    pub(super) fn source_followers(&self) -> Result<Vec<String>> {
        let state = self.state()?;
        let primary = self.primary(&self.source)?;
        Ok(state
            .replica_set(&self.source)?
            .into_iter()
            .map(|id| id.to_string())
            .filter(|id| *id != primary)
            .collect())
    }

    /// Data host `host`'s fork hooks.
    pub(super) fn faults(&self, host: &str) -> Result<ForkFaults> {
        Ok(self
            .fleet
            .fork_faults(host)
            .ok_or_else(|| format!("no hooks for {host}"))?)
    }

    /// Holds `operation`'s work at `point` on every data host.
    pub(super) fn hold(&self, point: FaultPoint, operation: OperationId) -> Result<()> {
        for node in NODES {
            self.faults(node)?.hold_operation(point, operation);
        }
        Ok(())
    }

    /// Lets `operation`'s work held at `point` go on, on every data host.
    pub(super) fn release(&self, point: FaultPoint, operation: OperationId) -> Result<()> {
        for node in NODES {
            self.faults(node)?.release_operation(point, operation);
        }
        Ok(())
    }

    /// How many times work arrived at `point` while held, on each data host.
    fn arrivals(&self, point: FaultPoint) -> Result<BTreeMap<String, u64>> {
        NODES
            .iter()
            .map(|node| Ok(((*node).to_owned(), self.faults(node)?.arrivals(point))))
            .collect()
    }

    /// Waits until some data host's work arrives at `point` while held, more often than
    /// `before` counted, and returns the host.
    pub(super) fn await_arrival(
        &mut self,
        point: FaultPoint,
        before: &BTreeMap<String, u64>,
    ) -> Result<String> {
        let mut found = None;
        let hooks: Vec<(String, ForkFaults)> = NODES
            .iter()
            .map(|node| Ok(((*node).to_owned(), self.faults(node)?)))
            .collect::<Result<_>>()?;
        self.fleet
            .run_until(LIMIT, &format!("fork work held at {point:?}"), |_| {
                found = hooks
                    .iter()
                    .find(|(node, hooks)| {
                        hooks.arrivals(point) > before.get(node).copied().unwrap_or(0)
                    })
                    .map(|(node, _)| node.clone());
                found.is_some()
            })?;
        Ok(found.ok_or("no host arrived")?)
    }

    /// Asks for a fork of the source as `target` under `operation`, on a client of its own
    /// that resends it until it is answered, and waits until the fork is seeding. Records the
    /// first attempt's seeder.
    pub(super) fn fork_as(&mut self, target: &DatabaseName, operation: OperationId) -> Result<()> {
        let source = self.source.clone();
        self.fork_of(&source, target, operation, REPLICAS)
    }

    /// As [`fork_as`](Self::fork_as), of `source` with `replicas` replicas, under `operation`.
    pub(super) fn fork_of(
        &mut self,
        source: &DatabaseName,
        target: &DatabaseName,
        operation: OperationId,
        replicas: u32,
    ) -> Result<()> {
        self.forks.push((target.clone(), operation));
        self.spawn_script([fork_request(source, target, operation, replicas)]);
        let mut seeder = None;
        let target = target.clone();
        self.fleet.run_until(LIMIT, "the fork seeding", |fleet| {
            seeder = fleet.control_state().and_then(|state| {
                let db = state.database(&target)?;
                let fork = db.fork().filter(|fork| fork.operation() == operation)?;
                match fork.state() {
                    ForkState::Seeding { seeder, .. } => Some(seeder.to_string()),
                    _ => None,
                }
            });
            seeder.is_some()
        })?;
        if let Some(seeder) = seeder {
            self.seeders.insert(operation, seeder);
        }
        // Clients write to the fork from its request on: until it is ready, each is refused.
        self.traffic.add(target);
        Ok(())
    }

    /// The seeder of `operation`'s first attempt.
    pub(super) fn seeder(&self, operation: OperationId) -> Result<String> {
        Ok(self
            .seeders
            .get(&operation)
            .cloned()
            .ok_or("no seeder recorded")?)
    }

    /// The fork `operation` made, as control state shows it now.
    pub(super) fn fork_state(&self, target: &DatabaseName) -> Option<ForkState> {
        let state = self.fleet.control_state()?;
        let db = state.database(target)?;
        Some(db.fork()?.state().clone())
    }

    /// The fork identity `operation` made `target` with.
    pub(super) fn identity(
        &self,
        target: &DatabaseName,
        operation: OperationId,
    ) -> Result<ForkIdentity> {
        let state = self.state()?;
        let incarnation = state
            .fork_operations()
            .find(|(id, _)| **id == operation)
            .map(|(_, record)| record.target_incarnation())
            .ok_or("no such operation")?;
        Ok(ForkIdentity::new(target.clone(), incarnation, operation))
    }

    /// The fork's recorded holders, once it is seeded or ready with them kept.
    pub(super) fn holders(&self, target: &DatabaseName) -> Result<Vec<String>> {
        match self.fork_state(target) {
            Some(
                ForkState::Seeded { holders, .. }
                | ForkState::Ready {
                    holders: Some(holders),
                    ..
                },
            ) => Ok(holders.iter().map(ToString::to_string).collect()),
            other => Err(format!("{target} has no holders: {other:?}").into()),
        }
    }

    /// Waits until `operation`'s outcome is `expected`. [`Expected::Seeded`] waits a while, then
    /// requires the fork still seeded.
    pub(super) fn await_outcome(
        &mut self,
        operation: OperationId,
        expected: Expected,
    ) -> Result<()> {
        let outcome = |fleet: &Fleet| {
            fleet.control_state().and_then(|state| {
                state
                    .fork_operations()
                    .find(|(id, _)| **id == operation)
                    .map(|(_, record)| record.outcome().clone())
            })
        };
        if expected == Expected::Seeded {
            self.fleet.run_for(Duration::from_secs(8))?;
            return match outcome(&self.fleet) {
                Some(OperationOutcome::Seeded) => Ok(()),
                other => {
                    Err(format!("expected the fork to stay seeded, but it is {other:?}").into())
                }
            };
        }
        let ended =
            self.fleet
                .run_until(LIMIT, &format!("the fork ending {expected:?}"), |fleet| {
                    matches!(
                        (expected, outcome(fleet)),
                        (Expected::Ready, Some(OperationOutcome::Ready { .. }))
                            | (Expected::Failed, Some(OperationOutcome::Failed { .. }))
                            | (
                                Expected::Cancelled,
                                Some(OperationOutcome::Cancelled { .. })
                            )
                    ) || (expected == Expected::Terminal
                        && outcome(fleet).is_some_and(|outcome| outcome.is_terminal()))
                });
        if let Err(error) = ended {
            let target = self
                .forks
                .iter()
                .find(|(_, id)| *id == operation)
                .map(|(target, _)| target.clone());
            let state = target.and_then(|target| self.fork_state(&target));
            return Err(format!("{error}; the fork is {state:?}").into());
        }
        Ok(())
    }

    /// Crashes `host`.
    pub(super) fn crash(&mut self, host: &str) -> Result<()> {
        self.fleet.crash(host)?;
        if host != CONTROL {
            self.down.insert(host.to_owned());
        }
        Ok(())
    }

    /// Restarts `host`.
    pub(super) fn restart(&mut self, host: &str) -> Result<()> {
        self.fleet.restart(host)?;
        self.down.remove(host);
        Ok(())
    }

    /// Runs on for `duration`.
    pub(super) fn wait(&mut self, duration: Duration) -> Result<()> {
        Ok(self.fleet.run_for(duration)?)
    }

    /// Expects `operation` to end as `expected`, waits until it does, writes to the fork a little
    /// more if it is ready, stops the traffic, and finishes the run with every check.
    pub(super) fn finish(mut self, operation: OperationId, expected: Expected) -> Result<()> {
        self.fleet.fork_expectations().expect(operation, expected);
        if let Err(error) = self.await_outcome(operation, expected) {
            // The invariants say why, if one broke on the way.
            self.traffic.stop();
            let _ = self.fleet.await_workloads(Duration::from_secs(5));
            return Err(match self.fleet.finish() {
                Err(violation) => format!("{violation}; and {error}").into(),
                Ok(_) => error,
            });
        }
        if expected == Expected::Ready
            && let Some((target, _)) = self.forks.iter().find(|(_, id)| *id == operation).cloned()
        {
            self.await_more_writes(&target, 5)?;
        }
        self.end()
    }

    /// Stops the traffic, waits for every client, and finishes the run with every check.
    pub(super) fn end(mut self) -> Result<()> {
        self.traffic.stop();
        self.fleet.await_workloads(LIMIT)?;
        self.fleet.finish()?;
        Ok(())
    }

    /// The node IDs of `hosts`.
    pub(super) fn ids(hosts: &[String]) -> Result<Vec<NodeId>> {
        hosts.iter().map(|host| Ok(host.parse()?)).collect()
    }
}

/// A fork request of `source` as `target` with `replicas` replicas under `operation`.
pub(super) fn fork_request(
    source: &DatabaseName,
    target: &DatabaseName,
    operation: OperationId,
    replicas: u32,
) -> Operation {
    Operation::Fork {
        source: source.clone(),
        target: target.clone(),
        operation,
        replication_factor: replicas,
        expect_source_incarnation: None,
    }
}

/// The bootstrap quorum of a fork with `replicas` replicas under `quorum`: a majority.
pub(super) fn bootstrap(replicas: usize) -> usize {
    replicas / 2 + 1
}

/// What a fork whose holders are `holders` may end as once `lost` of them are gone for good:
/// ready if a bootstrap quorum of its replica set remains, else seeded, unusable but safe.
pub(super) fn after_losing(holders: &[String], lost: &[String]) -> Expected {
    let left = holders
        .iter()
        .filter(|holder| !lost.contains(holder))
        .count();
    if left >= bootstrap(usize::try_from(REPLICAS).unwrap_or(3)) {
        Expected::Ready
    } else {
        Expected::Seeded
    }
}

/// Runs `scenario` at every one of `points` on CI's fixed seed.
pub(super) fn fixed(points: &[Point], scenario: impl Fn(u64, Point) -> Result<()>) {
    logs();
    let only = std::env::var(POINT_VAR).ok();
    seeds::run(BINARY, &FIXED, |seed| {
        let mut failed = Vec::new();
        for point in points {
            if only
                .as_ref()
                .is_some_and(|only| *only != format!("{point:?}"))
            {
                continue;
            }
            if let Err(error) = scenario(seed, *point) {
                failed.push(format!("at {point:?}: {error}"));
            }
        }
        if failed.is_empty() {
            Ok(())
        } else {
            Err(failed.join("\n"))
        }
    });
}

/// Runs `scenario` over [`SWEEP_SEEDS`] seeds (1 000 by default) from [`SWEEP_FROM`], each at
/// the point its seed picks, and fails listing every failing seed and point.
pub(super) fn sweep(points: &[Point], scenario: impl Fn(u64, Point) -> Result<()>) {
    let count: u64 = std::env::var(SWEEP_SEEDS)
        .ok()
        .and_then(|text| text.parse().ok())
        .unwrap_or(1_000);
    let from: u64 = std::env::var(SWEEP_FROM)
        .ok()
        .and_then(|text| text.parse().ok())
        .unwrap_or(1);
    let mut failures = Vec::new();
    for seed in from..from + count {
        let index = usize::try_from(seed).unwrap_or(0) % points.len();
        let point = points[index];
        let result =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| scenario(seed, point)));
        match result {
            Ok(Ok(())) => {}
            Ok(Err(error)) => failures.push(format!("seed {seed} at {point:?}: {error}")),
            Err(_) => failures.push(format!("seed {seed} at {point:?}: panicked")),
        }
    }
    assert!(
        failures.is_empty(),
        "{} of {count} seeds failed:\n{}",
        failures.len(),
        failures.join("\n")
    );
}

/// Declares a row's fixed-seed test over `points`, and its ignored sweep.
macro_rules! row {
    ($test:ident, $sweep:ident, $points:expr, $scenario:path) => {
        #[test]
        fn $test() {
            $crate::fork::matrix::fixed(&$points, $scenario);
        }

        #[test]
        #[ignore = "the sweep: 1 000 seeds per row, run with --ignored"]
        fn $sweep() {
            $crate::fork::matrix::sweep(&$points, $scenario);
        }
    };
    ($test:ident, $sweep:ident, $points:expr, $scenario:path, $bug:literal) => {
        #[test]
        #[ignore = $bug]
        fn $test() {
            $crate::fork::matrix::fixed(&$points, $scenario);
        }

        #[test]
        #[ignore = $bug]
        fn $sweep() {
            $crate::fork::matrix::sweep(&$points, $scenario);
        }
    };
}

pub(super) use row;
