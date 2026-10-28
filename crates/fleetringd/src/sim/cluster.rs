//! A simulated cluster of real daemons, one turmoil host per node.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::io;
use std::net::{Ipv4Addr, SocketAddr};
use std::num::NonZeroU64;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use fleetring::{NodeId, ReplicationFactor, Weight};
use fleetring_control::{DatabaseName, Quorums, State};
use tempfile::TempDir;
use tokio::sync::watch;

use crate::control::ControlHandle;
use crate::daemon::{CONTROL_DB, DATABASES_DIR};
use crate::engine::{EngineConfig, Execution};
use crate::layout;
use crate::replication::{Replication, ReplicationSettings};
use crate::sim::SimRng;
use crate::sim::check::{self, ControlLog, ReplicaRows, Violation};
use crate::sim::client::{Session, run_client};
use crate::sim::fault::{Fault, LossRate};
use crate::sim::host::{Shared, WorkloadState, run_host};
use crate::sim::seeds::millis;
use crate::sim::trace::{DRIVER, Divergence, Event, EventKind, Trace};
use crate::sim::workload::{Ack, History, Operation, WRITES_TABLE, Workload};
use crate::{
    Config, ControlSettings, Membership, MembershipSettings, NodeConfig, ReplicationConfig, Role,
};

/// The port every simulated daemon listens on.
pub const PORT: u16 = 7400;

/// The environment variable that makes every cluster print its trace when it is dropped.
pub const TRACE_VAR: &str = "FLEETRING_SIM_TRACE";

/// How long a client waits for one operation before it counts as indeterminate, by default.
const OPERATION_TIMEOUT: Duration = Duration::from_secs(1);

/// The longest random delay of a message, by default.
const MAX_LATENCY: Duration = Duration::from_millis(20);

/// How long a graceful stop may take, in simulated time.
const STOP_LIMIT: Duration = Duration::from_secs(10);

/// Mixed into the seed for the workload generator, so it doesn't mirror turmoil's.
const WORKLOAD_SALT: u64 = 0x5EED_F1EE_7219_0024;

/// The error returned when a simulation can't be built or run, or breaks an invariant.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum SimError {
    /// The cluster's description is invalid.
    #[error("invalid cluster: {0}")]
    Invalid(String),
    /// The hosts' data directories can't be created.
    #[error("can't create the data directories")]
    Io(#[from] io::Error),
    /// A host's software failed, or the simulation ran past its duration.
    #[error("the simulation failed: {0}")]
    Simulation(String),
    /// A condition the test waited for didn't hold in time.
    #[error("{what}: not done within {limit:?}")]
    Timeout {
        /// What the test waited for.
        what: String,
        /// How long it waited, in simulated time.
        limit: Duration,
    },
    /// An invariant broke.
    #[error(transparent)]
    Violation(#[from] Violation),
    /// Two runs of one seed differ.
    #[error(transparent)]
    Diverged(#[from] Divergence),
}

/// The timings every host in a cluster runs with.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Timings {
    /// How often data nodes send heartbeats.
    pub heartbeat_interval: Duration,
    /// How long a member stays live after its last heartbeat.
    pub heartbeat_timeout: Duration,
    /// How long a lease lasts after it is granted or renewed.
    pub lease_duration: Duration,
    /// How often a primary renews each lease.
    pub lease_renewal: Duration,
    /// How long past a lease's expiry the control node waits before failing over.
    pub failover_margin: Duration,
    /// How often the control node looks for expired leases.
    pub tick_interval: Duration,
    /// How many commands the control node applies between snapshots.
    pub snapshot_every: NonZeroU64,
}

impl Default for Timings {
    /// A 200 ms heartbeat, a 1 s heartbeat timeout, 2 s leases renewed every 400 ms, a 400 ms
    /// failover margin, a 100 ms tick, and a snapshot every 50 commands, so a short run crosses
    /// several snapshots.
    fn default() -> Self {
        Self {
            heartbeat_interval: Duration::from_millis(200),
            heartbeat_timeout: Duration::from_millis(1_000),
            lease_duration: Duration::from_millis(2_000),
            lease_renewal: Duration::from_millis(400),
            failover_margin: Duration::from_millis(400),
            tick_interval: Duration::from_millis(100),
            snapshot_every: NonZeroU64::new(50).unwrap_or(NonZeroU64::MIN),
        }
    }
}

impl Timings {
    /// The daemon's own defaults, as a LAN deployment runs them: 1 s heartbeats, a 5 s heartbeat
    /// timeout, 10 s leases renewed every 2 s, a 2 s failover margin, and a 250 ms tick.
    #[must_use]
    pub fn daemon_defaults() -> Self {
        let control = ControlSettings::default();
        let membership = MembershipSettings::default();
        Self {
            heartbeat_interval: membership.heartbeat_interval(),
            heartbeat_timeout: Duration::from_millis(control.heartbeat_timeout_ms.get()),
            lease_duration: Duration::from_millis(control.lease_duration_ms.get()),
            lease_renewal: control.lease_renewal(),
            failover_margin: Duration::from_millis(control.failover_margin_ms),
            tick_interval: control.tick_interval(),
            snapshot_every: control.snapshot_every,
        }
    }
}

type Configure = Box<dyn Fn(&mut Config)>;

/// Describes a [`Cluster`]: its seed, its hosts and their roles, and its timings.
pub struct ClusterBuilder {
    seed: u64,
    hosts: Vec<(String, Role)>,
    timings: Timings,
    duration: Duration,
    operation_timeout: Duration,
    max_latency: Duration,
    configure: Vec<(String, Configure)>,
    replication: Option<ReplicationSettings>,
}

impl fmt::Debug for ClusterBuilder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ClusterBuilder")
            .field("seed", &self.seed)
            .field("hosts", &self.hosts)
            .field("timings", &self.timings)
            .field("duration", &self.duration)
            .field("operation_timeout", &self.operation_timeout)
            .field("max_latency", &self.max_latency)
            .field("replication", &self.replication)
            .finish_non_exhaustive()
    }
}

impl ClusterBuilder {
    /// Runs every host's replication with `settings`, through its `[replication]` table. The
    /// default is [`ReplicationSettings::new`].
    #[must_use]
    pub fn replication(mut self, settings: ReplicationSettings) -> Self {
        self.replication = Some(settings);
        self
    }

    /// Adds a host named `name` that runs `role`. The name is also the node's ID.
    #[must_use]
    pub fn host(mut self, name: &str, role: Role) -> Self {
        self.hosts.push((name.to_owned(), role));
        self
    }

    /// Adds a host that runs only the control plane.
    #[must_use]
    pub fn control(self, name: &str) -> Self {
        self.host(name, Role::Control)
    }

    /// Adds a host that only hosts data.
    #[must_use]
    pub fn data(self, name: &str) -> Self {
        self.host(name, Role::Data)
    }

    /// Adds a host that runs both roles.
    #[must_use]
    pub fn both(self, name: &str) -> Self {
        self.host(name, Role::Both)
    }

    /// Runs every host with `timings`.
    #[must_use]
    pub fn timings(mut self, timings: Timings) -> Self {
        self.timings = timings;
        self
    }

    /// Lets the simulation run for up to `duration` of simulated time. The default is ten
    /// minutes; turmoil fails a run that goes past it while a workload is still running.
    #[must_use]
    pub fn duration(mut self, duration: Duration) -> Self {
        self.duration = duration;
        self
    }

    /// Delays each message by a random time up to `max_latency`. The default is 20 ms, a busy
    /// local network; [`Cluster::set_latency`] changes single links during a run.
    #[must_use]
    pub fn max_latency(mut self, max_latency: Duration) -> Self {
        self.max_latency = max_latency;
        self
    }

    /// Lets a client wait `timeout` for each operation before it counts the operation as
    /// indeterminate and dials a new connection. The default is one second.
    ///
    /// A crashed host's connections stall rather than fail (see [`Fault::Crash`]), so this also
    /// bounds how long a client takes to notice a crash.
    #[must_use]
    pub fn operation_timeout(mut self, timeout: Duration) -> Self {
        self.operation_timeout = timeout;
        self
    }

    /// Changes the configuration of host `name` with `configure`, after the harness fills it in.
    #[must_use]
    pub fn configure(mut self, name: &str, configure: impl Fn(&mut Config) + 'static) -> Self {
        self.configure.push((name.to_owned(), Box::new(configure)));
        self
    }

    /// Creates the hosts' data directories and starts every host.
    ///
    /// # Errors
    ///
    /// [`SimError::Invalid`] if there are no hosts, a name repeats or isn't a valid node ID, or
    /// not exactly one host runs the control plane; [`SimError::Io`] if the directories can't
    /// be created.
    pub fn build(self) -> Result<Cluster, SimError> {
        let control = self.validate()?;
        let root = tempfile::tempdir()?;
        let sim = turmoil::Builder::new()
            .rng_seed(self.seed)
            .simulation_duration(self.duration)
            .min_message_latency(Duration::ZERO)
            .max_message_latency(self.max_latency)
            .build();
        let control_address = SocketAddr::new(sim.lookup(control.as_str()), PORT);
        let mut cluster = Cluster {
            sim,
            seed: self.seed,
            operation_timeout: self.operation_timeout,
            rng: SimRng::new(self.seed ^ WORKLOAD_SALT),
            shared: Shared::default(),
            hosts: Vec::new(),
            control,
            down: BTreeSet::new(),
            clients: Vec::new(),
            partitions: BTreeSet::new(),
            root,
        };
        for (name, role) in &self.hosts {
            let data_dir = cluster.root.path().join(name);
            std::fs::create_dir_all(&data_dir)?;
            let mut config = host_config(name, *role, data_dir.clone(), self.timings)?;
            config.node.advertise = Some(SocketAddr::new(cluster.sim.lookup(name.as_str()), PORT));
            if !role.runs_control() {
                config.control.address = Some(control_address);
            }
            if let Some(settings) = &self.replication {
                config.replication = ReplicationConfig::from(settings);
            }
            for (_, configure) in self.configure.iter().filter(|(host, _)| host == name) {
                configure(&mut config);
            }
            cluster.add_host(name, *role, config, data_dir);
        }
        Ok(cluster)
    }

    /// Returns the one host that runs the control plane.
    fn validate(&self) -> Result<String, SimError> {
        let invalid = |reason: String| Err(SimError::Invalid(reason));
        let mut names = BTreeSet::new();
        for (name, _) in &self.hosts {
            if !names.insert(name.as_str()) {
                return invalid(format!("host '{name}' appears twice"));
            }
            if let Err(error) = name.parse::<NodeId>() {
                return invalid(format!("host '{name}' is not a node ID: {error}"));
            }
        }
        let control: Vec<&str> = self
            .hosts
            .iter()
            .filter(|(_, role)| role.runs_control())
            .map(|(name, _)| name.as_str())
            .collect();
        match control.as_slice() {
            [one] => Ok((*one).to_owned()),
            [] => invalid("no host runs the control plane".into()),
            many => invalid(format!("several hosts run the control plane: {many:?}")),
        }
    }
}

fn host_config(
    name: &str,
    role: Role,
    data_dir: PathBuf,
    timings: Timings,
) -> Result<Config, SimError> {
    let nonzero = |what: &str, duration: Duration| {
        NonZeroU64::new(millis(duration))
            .ok_or_else(|| SimError::Invalid(format!("{what} must be at least 1 ms")))
    };
    let id = name
        .parse()
        .map_err(|error| SimError::Invalid(format!("host '{name}': {error}")))?;
    Ok(Config {
        node: NodeConfig {
            id,
            zone: None,
            host: None,
            weight: Weight::ONE,
            advertise: None,
        },
        data_dir,
        listen: SocketAddr::new(Ipv4Addr::UNSPECIFIED.into(), PORT),
        control: ControlSettings {
            address: None,
            lease_duration_ms: nonzero("the lease duration", timings.lease_duration)?,
            lease_renewal_ms: Some(nonzero("the lease renewal", timings.lease_renewal)?),
            failover_margin_ms: millis(timings.failover_margin),
            heartbeat_timeout_ms: nonzero("the heartbeat timeout", timings.heartbeat_timeout)?,
            tick_interval_ms: nonzero("the tick interval", timings.tick_interval)?,
            snapshot_every: timings.snapshot_every,
        },
        role,
        engine: EngineConfig {
            execution: Execution::Inline,
            ..EngineConfig::new()
        },
        membership: MembershipSettings {
            heartbeat_interval_ms: nonzero("the heartbeat interval", timings.heartbeat_interval)?,
            ..MembershipSettings::default()
        },
        replication: ReplicationConfig::default(),
    })
}

/// A host the cluster runs.
#[derive(Debug, Clone)]
struct HostInfo {
    name: String,
    role: Role,
    data_dir: PathBuf,
}

/// A running cluster of simulated hosts, each running the real [`Daemon`](crate::Daemon).
///
/// Every host is a turmoil host with its own data directory, which survives crashes. The test
/// drives simulated time with [`Cluster::step`], [`Cluster::run_for`], and [`Cluster::run_until`],
/// injects [`Fault`]s between steps, adds [`Workload`]s with [`Cluster::client`], and checks
/// invariants with [`Cluster::check`], [`Cluster::await_ring`], and [`Cluster::finish`]. Every
/// fault, publication, and operation lands in the [`Trace`].
pub struct Cluster {
    // Dropped first: hosts close their files before the data directories go.
    sim: turmoil::Sim<'static>,
    seed: u64,
    operation_timeout: Duration,
    rng: SimRng,
    shared: Shared,
    hosts: Vec<HostInfo>,
    control: String,
    /// Hosts the test crashed or stopped and hasn't restarted.
    down: BTreeSet<String>,
    /// Client hosts, in the order they were added.
    clients: Vec<String>,
    /// Links the test has partitioned and not repaired, each as an ordered pair.
    partitions: BTreeSet<(String, String)>,
    root: TempDir,
}

impl fmt::Debug for Cluster {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Cluster")
            .field("seed", &self.seed)
            .field("elapsed", &self.sim.elapsed())
            .field("hosts", &self.hosts)
            .field("control", &self.control)
            .field("down", &self.down)
            .finish_non_exhaustive()
    }
}

impl Cluster {
    /// Describes a cluster whose network and workloads are driven by `seed`.
    #[must_use]
    pub fn builder(seed: u64) -> ClusterBuilder {
        ClusterBuilder {
            seed,
            hosts: Vec::new(),
            timings: Timings::default(),
            duration: Duration::from_secs(600),
            operation_timeout: OPERATION_TIMEOUT,
            max_latency: MAX_LATENCY,
            configure: Vec::new(),
            replication: None,
        }
    }

    fn add_host(&mut self, name: &str, role: Role, config: Config, data_dir: PathBuf) {
        let (stop, _) = watch::channel(0_u64);
        self.shared.lock().stops.insert(name.to_owned(), stop);
        let shared = self.shared.clone();
        let host = name.to_owned();
        self.sim.host(name, move || {
            run_host(host.clone(), config.clone(), shared.clone())
        });
        self.hosts.push(HostInfo {
            name: name.to_owned(),
            role,
            data_dir,
        });
    }

    /// Returns the seed.
    #[must_use]
    pub fn seed(&self) -> u64 {
        self.seed
    }

    /// Returns the simulated time since the cluster was built.
    #[must_use]
    pub fn elapsed(&self) -> Duration {
        self.sim.elapsed()
    }

    /// Returns the name of the host that runs the control plane.
    #[must_use]
    pub fn control_host(&self) -> &str {
        &self.control
    }

    /// Returns the names of the hosts that host data, in the order they were added.
    pub fn data_hosts(&self) -> impl Iterator<Item = &str> {
        self.hosts
            .iter()
            .filter(|host| host.role.hosts_data())
            .map(|host| host.name.as_str())
    }

    /// Returns host `name`'s data directory.
    #[must_use]
    pub fn data_dir(&self, name: &str) -> Option<&Path> {
        self.host(name).map(|host| host.data_dir.as_path())
    }

    /// Returns the control node's log file.
    #[must_use]
    pub fn control_log(&self) -> PathBuf {
        self.root.path().join(&self.control).join(CONTROL_DB)
    }

    fn host(&self, name: &str) -> Option<&HostInfo> {
        self.hosts.iter().find(|host| host.name == name)
    }

    /// Returns whether host `name`'s daemon is running.
    #[must_use]
    pub fn is_running(&self, name: &str) -> bool {
        self.shared.lock().running.contains_key(name)
    }

    /// Returns a handle to the control node, while it runs.
    #[must_use]
    pub fn control_handle(&self) -> Option<ControlHandle> {
        self.shared.control_handle()
    }

    /// Returns the control node's published state, while it runs.
    #[must_use]
    pub fn control_state(&self) -> Option<Arc<State>> {
        self.control_handle().map(|handle| handle.state())
    }

    /// Returns host `name`'s membership, while its daemon runs and hosts data.
    #[must_use]
    pub fn membership(&self, name: &str) -> Option<Membership> {
        self.shared
            .lock()
            .running
            .get(name)
            .and_then(|running| running.membership.clone())
    }

    /// Returns a copy of the trace so far.
    #[must_use]
    pub fn trace(&self) -> Trace {
        self.shared.lock().trace.clone()
    }

    /// Returns a copy of every operation clients have finished so far.
    #[must_use]
    pub fn history(&self) -> History {
        self.shared.lock().history.clone()
    }

    /// Returns the generator for the test's own random choices, such as when to inject faults.
    ///
    /// It is seeded from the cluster's seed; clients added later fork from it too.
    pub fn rng(&mut self) -> &mut SimRng {
        &mut self.rng
    }

    /// Records a note in the trace, at the current simulated time.
    pub fn note(&self, text: impl Into<String>) {
        self.record(EventKind::Note(text.into()));
    }

    fn record(&self, kind: EventKind) {
        let event = Event::new(self.sim.elapsed(), DRIVER, kind);
        self.shared.lock().trace.push(event);
    }

    /// Adds a client named `name` that runs `workload` against the control plane.
    ///
    /// The client is a turmoil host of its own, so faults can name it. Its generator is forked
    /// from the cluster's seed in the order clients are added.
    pub fn client(&mut self, name: &str, workload: impl Workload) {
        self.add_client(name, None, workload);
    }

    /// Adds a client named `name` that runs `workload`, sending each database call to the data
    /// node `entry` first instead of locating the primary through the control plane, as
    /// `fleetringd db exec --node` does. It follows redirects from there, and reaches a primary
    /// while the control node is down.
    pub fn client_through(&mut self, name: &str, entry: &str, workload: impl Workload) {
        self.add_client(name, Some(format!("{entry}:{PORT}")), workload);
    }

    fn add_client(&mut self, name: &str, entry: Option<String>, workload: impl Workload) {
        let rng = self.rng.fork();
        let shared = self.shared.clone();
        let control = format!("{}:{PORT}", self.control);
        let client = name.to_owned();
        self.clients.push(client.clone());
        if !workload.is_endless() {
            shared
                .lock()
                .workloads
                .insert(client.clone(), WorkloadState::Running);
        }
        let session = Session {
            name: client,
            control,
            entry,
            timeout: self.operation_timeout,
            shared,
        };
        self.sim
            .client(name, run_client(session, Box::new(workload), rng));
    }

    /// Advances the simulation by one tick (1 ms of simulated time).
    ///
    /// # Errors
    ///
    /// [`SimError::Simulation`] if a host's software failed or the run went past its duration.
    pub fn step(&mut self) -> Result<(), SimError> {
        self.sim
            .step()
            .map(|_| ())
            .map_err(|error| SimError::Simulation(error.to_string()))
    }

    /// Advances the simulation by `duration`.
    ///
    /// # Errors
    ///
    /// As [`Cluster::step`].
    pub fn run_for(&mut self, duration: Duration) -> Result<(), SimError> {
        let end = self.sim.elapsed() + duration;
        while self.sim.elapsed() < end {
            self.step()?;
        }
        Ok(())
    }

    /// Steps until `done` holds, and returns how much simulated time that took.
    ///
    /// # Errors
    ///
    /// [`SimError::Timeout`] naming `what` if `done` doesn't hold within `limit`, or an error
    /// from [`Cluster::step`].
    pub fn run_until(
        &mut self,
        limit: Duration,
        what: &str,
        mut done: impl FnMut(&Self) -> bool,
    ) -> Result<Duration, SimError> {
        let start = self.sim.elapsed();
        loop {
            if done(self) {
                return Ok(self.sim.elapsed() - start);
            }
            if self.sim.elapsed() - start > limit {
                return Err(SimError::Timeout {
                    what: what.to_owned(),
                    limit,
                });
            }
            self.step()?;
        }
    }

    /// Steps until every workload added with [`Cluster::client`] has finished, except endless
    /// ones such as an [`Operator`](crate::sim::Operator).
    ///
    /// # Errors
    ///
    /// As [`Cluster::run_until`].
    pub fn await_workloads(&mut self, limit: Duration) -> Result<Duration, SimError> {
        self.run_until(limit, "workloads finishing", Self::workloads_done)
    }

    /// Returns whether every workload added with [`Cluster::client`] has finished, except
    /// endless ones.
    #[must_use]
    pub fn workloads_done(&self) -> bool {
        self.shared
            .lock()
            .workloads
            .values()
            .all(|state| *state == WorkloadState::Done)
    }

    /// Steps until every data node the test hasn't crashed or stopped is a live member and
    /// publishes the control plane's ring and liveness, and returns how long that took.
    ///
    /// # Errors
    ///
    /// As [`Cluster::await_ring_on`].
    pub fn await_ring(&mut self, limit: Duration) -> Result<Duration, SimError> {
        let nodes: Vec<String> = self
            .data_hosts()
            .filter(|name| !self.down.contains(*name))
            .map(str::to_owned)
            .collect();
        self.await_ring_on(&nodes, limit)
    }

    /// Steps until every node in `nodes` is a live member and publishes the control plane's
    /// ring and liveness, and returns how long that took.
    ///
    /// # Errors
    ///
    /// [`SimError::Violation`] for [`Invariant::RingConverged`](check::Invariant::RingConverged)
    /// with the last disagreement if the nodes don't converge within `limit`, or an error from
    /// [`Cluster::step`].
    pub fn await_ring_on(
        &mut self,
        nodes: &[impl AsRef<str>],
        limit: Duration,
    ) -> Result<Duration, SimError> {
        let start = self.sim.elapsed();
        loop {
            let Err(violation) = self.ring_converged(nodes) else {
                return Ok(self.sim.elapsed() - start);
            };
            if self.sim.elapsed() - start > limit {
                return Err(Violation::new(
                    violation.invariant(),
                    format!("not converged within {limit:?}: {}", violation.detail()),
                )
                .into());
            }
            self.step()?;
        }
    }

    /// Checks right now that every node in `nodes` is a live member and publishes the control
    /// plane's ring.
    ///
    /// # Errors
    ///
    /// [`Violation`] naming the first node that disagrees or isn't running, or saying the
    /// control node is down.
    pub fn ring_converged(&self, nodes: &[impl AsRef<str>]) -> Result<(), Violation> {
        let violation = |detail: String| Violation::new(check::Invariant::RingConverged, detail);
        let state = self
            .control_state()
            .ok_or_else(|| violation("the control node is down".into()))?;
        let mut views = Vec::new();
        for name in nodes {
            let name = name.as_ref();
            let membership = self
                .membership(name)
                .ok_or_else(|| violation(format!("{name} isn't running")))?;
            views.push((name, membership.view()));
        }
        check::ring_converged(
            &state,
            views.iter().map(|(name, view)| (*name, view.as_ref())),
        )
    }

    /// Runs the invariants that hold at any moment: no two primaries per epoch, and, while the
    /// control node runs, no acknowledged control operation missing from its state.
    ///
    /// # Errors
    ///
    /// The first [`Violation`].
    pub fn check(&self) -> Result<(), Violation> {
        let history = self.history();
        check::one_primary_per_epoch(&history)?;
        if let Some(state) = self.control_state() {
            check::acknowledged_control_survives(&state, &history)?;
        }
        Ok(())
    }

    /// Runs the invariants that need the control node down: its log replays to the state
    /// recovery builds, and that state holds every acknowledged control operation. Returns what
    /// it read from the log.
    ///
    /// # Errors
    ///
    /// The first [`Violation`], or [`SimError::Invalid`] if the control node is running.
    pub fn check_control_log(&self) -> Result<ControlLog, SimError> {
        if self.is_running(&self.control) {
            return Err(SimError::Invalid(
                "the control log is checked only while the control node is down".into(),
            ));
        }
        let log = check::control_log_replays(&self.control_log())?;
        let history = self.history();
        check::acknowledged_control_survives(log.state(), &history)?;
        check::one_primary_per_epoch(&history)?;
        Ok(log)
    }

    /// Returns host `name`'s replication, while its daemon runs and hosts data.
    #[must_use]
    pub fn replication(&self, name: &str) -> Option<Replication> {
        self.shared
            .lock()
            .running
            .get(name)
            .and_then(|running| running.replication.clone())
    }

    /// Returns the rows each data host's copy of `database` holds, read from its files on disk.
    ///
    /// A host without a copy holds no rows. Reading copies the files first, and SQLite's recovery
    /// replays a follower's mirrored log as a promotion would, so the copy shows exactly what the
    /// host would serve.
    ///
    /// # Errors
    ///
    /// [`SimError::Simulation`] if a copy can't be read.
    pub fn replica_rows(&self, database: &DatabaseName) -> Result<ReplicaRows, SimError> {
        let mut rows = ReplicaRows::new();
        for host in self.hosts.iter().filter(|host| host.role.hosts_data()) {
            let dir = host
                .data_dir
                .join(DATABASES_DIR)
                .join(layout::database_dir(database));
            let held = read_rows(&dir).map_err(|error| {
                SimError::Simulation(format!(
                    "can't read {}'s copy of {database}: {error}",
                    host.name
                ))
            })?;
            rows.insert(host.name.clone(), held);
        }
        Ok(rows)
    }

    /// Checks that every acknowledged write is on at least W replicas of its database, reading
    /// each host's copy from disk. It holds at any moment.
    ///
    /// # Errors
    ///
    /// A [`Violation`] of
    /// [`Invariant::AcknowledgedWritesReplicated`](check::Invariant::AcknowledgedWritesReplicated),
    /// or [`SimError::Simulation`] if a copy can't be read.
    pub fn check_writes(&self) -> Result<(), SimError> {
        let history = self.history();
        for (database, factor) in created(&history) {
            let rows = self.replica_rows(&database)?;
            let required = usize::from(Quorums::new(factor).write());
            check::acknowledged_writes_replicated(&history, &database, &rows, required)?;
        }
        Ok(())
    }

    /// Steps until the followers of every database created through the data plane have
    /// acknowledged everything its primary committed, then checks that every replica holds the
    /// same rows and that every acknowledged write survived.
    ///
    /// # Errors
    ///
    /// [`SimError::Timeout`] if the followers don't catch up within `limit`, or a [`Violation`]
    /// of [`Invariant::ReplicasConverged`](check::Invariant::ReplicasConverged) or
    /// [`Invariant::AcknowledgedWritesReplicated`](check::Invariant::AcknowledgedWritesReplicated).
    pub fn await_replicas_converged(&mut self, limit: Duration) -> Result<Duration, SimError> {
        let took = self.run_until(limit, "replicas catching up", Self::replicas_caught_up)?;
        for (database, _) in created(&self.history()) {
            let rows = self.replica_rows(&database)?;
            check::replicas_converged(&database, &rows)?;
        }
        self.check_writes()?;
        Ok(took)
    }

    /// Returns whether the followers of every database created through the data plane have
    /// acknowledged everything its primary committed.
    #[must_use]
    pub fn replicas_caught_up(&self) -> bool {
        created(&self.history()).iter().all(|(database, _)| {
            self.primary_replication(database)
                .and_then(|replication| replication.primary_status(database))
                .is_some_and(|status| {
                    status.followers.iter().all(|follower| {
                        follower.reported && follower.acknowledged == status.committed
                    })
                })
        })
    }

    /// Returns the replication of the running host that serves `database` as its primary.
    #[must_use]
    pub fn primary_replication(&self, database: &DatabaseName) -> Option<Replication> {
        self.shared
            .lock()
            .running
            .values()
            .filter_map(|running| running.replication.clone())
            .find(|replication| replication.primary_status(database).is_some())
    }

    /// Injects `fault`, records it in the trace, and, for a crash or stop of the control host,
    /// checks its log.
    ///
    /// # Errors
    ///
    /// [`SimError::Invalid`] if the fault names an unknown host, an error from stepping while a
    /// host stops, or a [`Violation`] from the control log checks.
    pub fn inject(&mut self, fault: Fault) -> Result<(), SimError> {
        let (names, clients_too) = fault_hosts(&fault);
        for name in names {
            let is_client = self.clients.iter().any(|client| client == name);
            if self.host(name).is_none() && !(clients_too && is_client) {
                return Err(SimError::Invalid(format!(
                    "no host named '{name}' for {fault}"
                )));
            }
        }
        self.record(EventKind::Fault(fault.clone()));
        match &fault {
            Fault::Partition { a, b } => {
                self.partitions.insert(link(a, b));
                self.sim.partition(a.as_str(), b.as_str());
            }
            Fault::Repair { a, b } => {
                self.partitions.remove(&link(a, b));
                self.sim.repair(a.as_str(), b.as_str());
            }
            Fault::Latency { a, b, latency } => {
                self.sim.set_link_latency(a.as_str(), b.as_str(), *latency);
            }
            Fault::Loss { a, b, rate } => {
                self.sim
                    .set_link_fail_rate(a.as_str(), b.as_str(), rate.probability());
            }
            Fault::Crash { host } => {
                self.isolated(host, |sim| sim.crash(host.as_str()));
                self.down.insert(host.clone());
                self.shared.lock().running.remove(host);
                if *host == self.control {
                    self.check_control_log()?;
                }
            }
            Fault::Stop { host } => {
                self.down.insert(host.clone());
                self.shared.signal_stop(host);
                let what = format!("{host} stopping");
                let name = host.clone();
                self.run_until(STOP_LIMIT, &what, |cluster| !cluster.is_running(&name))?;
                if *host == self.control {
                    self.check_control_log()?;
                }
            }
            Fault::Restart { host } => {
                self.isolated(host, |sim| sim.bounce(host.as_str()));
                self.down.remove(host);
            }
        }
        Ok(())
    }

    /// Runs `action`, which tears down `host`'s runtime, while `host` is cut off from every other
    /// host, then restores the links the test hasn't partitioned.
    ///
    /// Tearing down a runtime drops its tasks in an order that depends on Tokio's process-wide
    /// task IDs, which differ between runs. Each connection a dropped task closes would send a
    /// message, and each message draws a delay from turmoil's generator, so the order would leak
    /// into the rest of the run. Cut off, the messages are dropped unsent and draw nothing; peers
    /// see their connections stall, as they would if the machine lost power.
    fn isolated(&mut self, host: &str, action: impl FnOnce(&mut turmoil::Sim<'static>)) {
        let others: Vec<String> = self
            .hosts
            .iter()
            .map(|other| other.name.clone())
            .chain(self.clients.iter().cloned())
            .filter(|other| other != host)
            .collect();
        for other in &others {
            self.sim.partition(host, other.as_str());
        }
        action(&mut self.sim);
        for other in &others {
            if !self.partitions.contains(&link(host, other)) {
                self.sim.repair(host, other.as_str());
            }
        }
    }

    /// Drops every message between `a` and `b` until [`Cluster::repair`].
    ///
    /// # Errors
    ///
    /// As [`Cluster::inject`].
    pub fn partition(&mut self, a: &str, b: &str) -> Result<(), SimError> {
        self.inject(Fault::Partition {
            a: a.to_owned(),
            b: b.to_owned(),
        })
    }

    /// Ends a partition between `a` and `b`.
    ///
    /// # Errors
    ///
    /// As [`Cluster::inject`].
    pub fn repair(&mut self, a: &str, b: &str) -> Result<(), SimError> {
        self.inject(Fault::Repair {
            a: a.to_owned(),
            b: b.to_owned(),
        })
    }

    /// Delays every message between `a` and `b` by `latency`.
    ///
    /// # Errors
    ///
    /// As [`Cluster::inject`].
    pub fn set_latency(&mut self, a: &str, b: &str, latency: Duration) -> Result<(), SimError> {
        self.inject(Fault::Latency {
            a: a.to_owned(),
            b: b.to_owned(),
            latency,
        })
    }

    /// Breaks the link between `a` and `b` at random, at `rate`. [`LossRate::NONE`] ends it.
    ///
    /// # Errors
    ///
    /// As [`Cluster::inject`].
    pub fn set_loss(&mut self, a: &str, b: &str, rate: LossRate) -> Result<(), SimError> {
        self.inject(Fault::Loss {
            a: a.to_owned(),
            b: b.to_owned(),
            rate,
        })
    }

    /// Kills host `name`'s daemon at once; see [`Fault::Crash`].
    ///
    /// # Errors
    ///
    /// As [`Cluster::inject`].
    pub fn crash(&mut self, name: &str) -> Result<(), SimError> {
        self.inject(Fault::Crash {
            host: name.to_owned(),
        })
    }

    /// Shuts host `name`'s daemon down gracefully and steps until it has stopped.
    ///
    /// # Errors
    ///
    /// As [`Cluster::inject`].
    pub fn stop(&mut self, name: &str) -> Result<(), SimError> {
        self.inject(Fault::Stop {
            host: name.to_owned(),
        })
    }

    /// Starts host `name`'s daemon again on the same data directory.
    ///
    /// # Errors
    ///
    /// As [`Cluster::inject`].
    pub fn restart(&mut self, name: &str) -> Result<(), SimError> {
        self.inject(Fault::Restart {
            host: name.to_owned(),
        })
    }

    /// Stops every host gracefully and runs the final checks: the control log replays to the
    /// state the control node last published, and that state holds every acknowledged control
    /// operation, with one primary per epoch.
    ///
    /// # Errors
    ///
    /// A [`Violation`], or an error from stepping while hosts stop.
    pub fn finish(mut self) -> Result<Report, SimError> {
        self.check()?;
        let control = self.control_handle();
        self.record(EventKind::Note("shutting down".into()));
        let running: Vec<String> = self.shared.lock().running.keys().cloned().collect();
        for name in &running {
            self.shared.signal_stop(name);
        }
        let stopped = self.run_until(STOP_LIMIT, "every host stopping", |cluster| {
            cluster.shared.lock().running.is_empty()
        });
        if let Err(SimError::Timeout { limit, .. }) = stopped {
            let running: Vec<String> = self.shared.lock().running.keys().cloned().collect();
            return Err(SimError::Timeout {
                what: format!("stopping every host ({running:?} still running)"),
                limit,
            });
        }
        stopped?;
        self.check_writes()?;
        let log = self.check_control_log()?;
        if let Some(control) = control
            && *control.state() != *log.state()
        {
            return Err(Violation::new(
                check::Invariant::ControlLogReplays,
                "the recovered state differs from the state the control node last published",
            )
            .into());
        }
        let trace = self.trace();
        let history = self.history();
        Ok(Report {
            trace,
            history,
            log,
        })
    }
}

impl Drop for Cluster {
    /// Prints the trace when [`TRACE_VAR`] is set, so a failing seed can be followed event by
    /// event.
    fn drop(&mut self) {
        if std::env::var_os(TRACE_VAR).is_some() {
            eprintln!("trace of seed {}:\n{}", self.seed, self.trace());
        }
    }
}

/// Every database an acknowledged [`Operation::Create`] created, with its replication factor.
fn created(history: &History) -> Vec<(DatabaseName, ReplicationFactor)> {
    let databases: BTreeMap<DatabaseName, ReplicationFactor> = history
        .acknowledged()
        .filter_map(|(operation, ack)| match (operation, ack) {
            (
                Operation::Create {
                    database,
                    replication_factor,
                },
                Ack::Opened { .. },
            ) => {
                let factor = u8::try_from(*replication_factor)
                    .ok()
                    .and_then(ReplicationFactor::new)?;
                Some((database.clone(), factor))
            }
            _ => None,
        })
        .collect();
    databases.into_iter().collect()
}

/// Reads the values in the [`WRITES_TABLE`] of the SQLite copy in `dir`, from a scratch copy of
/// its files.
fn read_rows(dir: &Path) -> Result<BTreeSet<i64>, Box<dyn std::error::Error>> {
    let database = dir.join("data.db");
    if !database.try_exists()? {
        return Ok(BTreeSet::new());
    }
    let scratch = tempfile::tempdir()?;
    let copy = scratch.path().join("data.db");
    std::fs::copy(&database, &copy)?;
    let wal = dir.join("data.db-wal");
    if wal.try_exists()? {
        std::fs::copy(&wal, scratch.path().join("data.db-wal"))?;
    }
    let connection = rusqlite::Connection::open(&copy)?;
    let tables: i64 = connection.query_row(
        "SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name = ?1",
        [WRITES_TABLE],
        |row| row.get(0),
    )?;
    if tables == 0 {
        return Ok(BTreeSet::new());
    }
    let mut statement = connection.prepare(&format!("SELECT value FROM {WRITES_TABLE}"))?;
    let keys = statement
        .query_map([], |row| row.get::<_, i64>(0))?
        .collect::<Result<BTreeSet<i64>, _>>()?;
    Ok(keys)
}

/// A link as an ordered pair of host names, so both directions name it the same way.
fn link(a: &str, b: &str) -> (String, String) {
    if a <= b {
        (a.to_owned(), b.to_owned())
    } else {
        (b.to_owned(), a.to_owned())
    }
}

/// The names a fault refers to, and whether they may be clients: link faults may name clients,
/// but only daemon hosts crash, stop, and restart.
fn fault_hosts(fault: &Fault) -> (Vec<&str>, bool) {
    match fault {
        Fault::Partition { a, b }
        | Fault::Repair { a, b }
        | Fault::Latency { a, b, .. }
        | Fault::Loss { a, b, .. } => (vec![a, b], true),
        Fault::Crash { host } | Fault::Stop { host } | Fault::Restart { host } => {
            (vec![host], false)
        }
    }
}

/// What a finished run produced.
#[derive(Debug, Clone)]
pub struct Report {
    trace: Trace,
    history: History,
    log: ControlLog,
}

impl Report {
    /// Returns the run's trace.
    #[must_use]
    pub fn trace(&self) -> &Trace {
        &self.trace
    }

    /// Returns every operation clients finished.
    #[must_use]
    pub fn history(&self) -> &History {
        &self.history
    }

    /// Returns the control state recovered from the log after the run.
    #[must_use]
    pub fn state(&self) -> &State {
        self.log.state()
    }

    /// Returns every command in the control log after the run.
    #[must_use]
    pub fn commands(&self) -> &[fleetring_control::Command] {
        self.log.commands()
    }
}
