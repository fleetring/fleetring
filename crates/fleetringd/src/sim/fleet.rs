//! A simulated fleet of real daemons, one turmoil host per node.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::io;
use std::net::{Ipv4Addr, SocketAddr};
use std::num::NonZeroU64;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use fleetring::{NodeId, ReplicationFactor, Weight};
use fleetring_control::fork::{ForkIdentity, ForkState, OperationId};
use fleetring_control::{DatabaseName, State, Timestamp};
use tempfile::TempDir;
use tokio::sync::watch;

use crate::changes::{ChangeRecord, ChangesConfig, SinkKind, database_file, unhex};
use crate::control::ControlHandle;
use crate::daemon::{CONTROL_DB, DATABASES_DIR};
use crate::durability::{ArchiveConfig, ObjectStore};
use crate::engine::{EngineConfig, Execution, Position};
use crate::layout;
use crate::replication::{FaultPoint, ForkFaults, Replication, ReplicationSettings};
use crate::sim::SimRng;
use crate::sim::check::{self, ControlLog, ReplicaChains, ReplicaRows, Violation};
use crate::sim::client::{Session, Via, WatcherSession, run_client, run_lease_watcher};
use crate::sim::fault::{Fault, LossRate};
use crate::sim::forks::{CopyOnDisk, ForkDisk, ForkExpectations, expected_replica_set};
use crate::sim::history::{self, Guarantee, Replicated};
use crate::sim::host::{Shared, WorkloadState, run_host};
use crate::sim::seeds::millis;
use crate::sim::trace::{DRIVER, Divergence, Event, EventKind, Trace};
use crate::sim::workload::{
    Ack, ForkMarkerSeen, ForkPass, ForkPhaseKind, ForkTransition, History, Operation, WRITES_TABLE,
    Workload,
};
use crate::tls::{Authority, AuthorityError, FleetName, Identity, IdentityError, Validity};
use crate::{
    Config, ControlSettings, Membership, MembershipSettings, NodeConfig, NodeRole,
    ReplicationConfig,
};

/// The port a simulated router serves Hrana on ([`FleetBuilder::router`]).
pub const ROUTER_PORT: u16 = 8080;

/// The principal a simulated router's certificate names. Every data host lists it with
/// `forward_tokens = true` and no scopes, as ADR-0037 deploys a router.
const ROUTER_PRINCIPAL: &str = "router";

/// The port every simulated daemon listens on.
pub const PORT: u16 = 7400;

/// The environment variable that makes every fleet print its trace when it is dropped.
pub const TRACE_VAR: &str = "FLEETRING_SIM_TRACE";

/// How long a client waits for one operation before it counts as indeterminate, by default.
const OPERATION_TIMEOUT: Duration = Duration::from_secs(1);

/// The longest random delay of a message, by default.
const MAX_LATENCY: Duration = Duration::from_millis(20);

/// How long a graceful stop may take, in simulated time: a drain of every lease the host holds,
/// then the leave and the shutdown.
const STOP_LIMIT: Duration = Duration::from_secs(30);

/// Mixed into the seed for the workload generator, so it doesn't mirror turmoil's.
const WORKLOAD_SALT: u64 = 0x5EED_F1EE_7219_0024;

/// Mixed into a host's seed for its randomness, with its name.
const HOST_SALT: u64 = 0x7A11_C0DE_0046_0001;

/// The seed a host draws its randomness from: the fleet's seed mixed with the host's name
/// (FNV-1a), so that two hosts never share a sequence and a seed replays each.
fn host_seed(seed: u64, name: &str) -> u64 {
    let hashed = name.bytes().fold(0xCBF2_9CE4_8422_2325_u64, |hash, byte| {
        (hash ^ u64::from(byte)).wrapping_mul(0x0000_0100_0000_01B3)
    });
    seed ^ HOST_SALT ^ hashed
}

/// The free bytes every data host's fork admission believes it has, by default, so that a run
/// never depends on the disk of the machine running it: the fork tests' hooks stand in for it.
const ROOMY_DISK: u64 = 1 << 40;

/// A second principal every host knows, with the same scopes as the workload's own: another
/// requester for the same requests ([`Fleet::client_as`]).
pub const OTHER_PRINCIPAL: &str = "other";

/// The fleet name the test authority issues for.
const FLEET: &str = "sim";

/// How long the test authority's certificates last. rustls checks validity against the wall
/// clock, not simulated time, so they span from the epoch to well past any run.
const CERTIFICATE_LIFETIME: Duration = Duration::from_secs(100 * 365 * 24 * 60 * 60);

/// The error returned when a simulation can't be built or run, or breaks an invariant.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum SimError {
    /// The fleet's description is invalid.
    #[error("invalid fleet: {0}")]
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
    /// The test authority can't issue a certificate.
    #[error("can't issue a certificate")]
    Authority(#[from] AuthorityError),
    /// An issued certificate can't be used as an identity.
    #[error("can't load an issued certificate")]
    Identity(#[from] IdentityError),
    /// An invariant broke.
    #[error(transparent)]
    Violation(#[from] Violation),
    /// Two runs of one seed differ.
    #[error(transparent)]
    Diverged(#[from] Divergence),
}

/// The timings every host in a fleet runs with.
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

/// An archive every data host writes: its name, its shared store, and what to change in its
/// `[[archive]]` entry.
type ArchiveSetup = (
    String,
    Arc<dyn ObjectStore>,
    Box<dyn Fn(&mut ArchiveConfig)>,
);

/// Each host's records of one sink's stream of one database, in file order.
type HostStreams = Vec<(String, Vec<ChangeRecord>)>;

/// Describes a [`Fleet`]: its seed, its hosts and their roles, and its timings.
pub struct FleetBuilder {
    seed: u64,
    hosts: Vec<(String, NodeRole)>,
    timings: Timings,
    duration: Duration,
    operation_timeout: Duration,
    max_latency: Duration,
    configure: Vec<(String, Configure)>,
    replication: Option<ReplicationSettings>,
    /// Client hosts that act as a node with that node's certificate, by name.
    impostors: BTreeMap<String, NodeId>,
    /// The archives every data host writes.
    archives: Vec<ArchiveSetup>,
    /// The router host, if the fleet has one.
    router: Option<String>,
}

impl fmt::Debug for FleetBuilder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FleetBuilder")
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

impl FleetBuilder {
    /// Runs every host's replication with `settings`, through its `[replication]` table. The
    /// default is [`ReplicationSettings::new`].
    #[must_use]
    pub fn replication(mut self, settings: ReplicationSettings) -> Self {
        self.replication = Some(settings);
        self
    }

    /// Adds a host named `name` that runs `role`. The name is also the node's ID.
    #[must_use]
    pub fn host(mut self, name: &str, role: NodeRole) -> Self {
        self.hosts.push((name.to_owned(), role));
        self
    }

    /// Adds a host that runs only the control plane.
    #[must_use]
    pub fn control(self, name: &str) -> Self {
        self.host(name, NodeRole::Control)
    }

    /// Adds a host that only hosts data.
    #[must_use]
    pub fn data(self, name: &str) -> Self {
        self.host(name, NodeRole::Data)
    }

    /// Adds a host that runs both roles.
    #[must_use]
    pub fn both(self, name: &str) -> Self {
        self.host(name, NodeRole::Both)
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
    /// local network; [`Fleet::set_latency`] changes single links during a run.
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

    /// Makes the client host `name`, once [`Fleet::client`] adds it, present a node
    /// certificate for `as_node` from the fleet's authority instead of a principal's: an
    /// impostor that stole that node's key (FR-050). Its workload's node calls
    /// ([`Operation::ImpostorReplicate`](crate::sim::Operation::ImpostorReplicate) and the
    /// others) go out with that certificate.
    ///
    /// # Panics
    ///
    /// If `as_node` isn't a valid node ID.
    #[must_use]
    pub fn impostor(mut self, name: &str, as_node: &str) -> Self {
        let node = as_node
            .parse()
            .unwrap_or_else(|error| panic!("'{as_node}' is not a node ID: {error}"));
        self.impostors.insert(name.to_owned(), node);
        self
    }

    /// Gives every data host an `[[archive]]` named `name`, of every database, that writes to
    /// `store` whatever its entry says: one store outside the simulated network, which every host
    /// shares and which survives every crash, as a bucket outlives a fleet (ADR-0029, point 16).
    /// `configure` changes the entry, as for its pacing and retention.
    #[must_use]
    pub fn archive(
        mut self,
        name: &str,
        store: Arc<dyn ObjectStore>,
        configure: impl Fn(&mut ArchiveConfig) + 'static,
    ) -> Self {
        self.archives
            .push((name.to_owned(), store, Box::new(configure)));
        self
    }

    /// Adds a router host named `name`, serving Hrana on [`ROUTER_PORT`] in plaintext, with a
    /// client certificate for the principal `router` that every data host lets forward its
    /// callers' tokens (ADR-0037). Clients reach it with [`Fleet::client_via_router`].
    #[must_use]
    pub fn router(mut self, name: &str) -> Self {
        self.router = Some(name.to_owned());
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
    pub fn build(self) -> Result<Fleet, SimError> {
        let control = self.validate()?;
        let root = tempfile::tempdir()?;
        let sim = turmoil::Builder::new()
            .rng_seed(self.seed)
            .simulation_duration(self.duration)
            .min_message_latency(Duration::ZERO)
            .max_message_latency(self.max_latency)
            .build();
        let control_address = SocketAddr::new(sim.lookup(control.as_str()), PORT);
        let fleet_name: FleetName = FLEET
            .parse()
            .map_err(|error: crate::tls::NameError| SimError::Invalid(error.to_string()))?;
        let authority = Arc::new(Authority::create(fleet_name, certificate_validity())?);
        let mut fleet = Fleet {
            sim,
            authority,
            seed: self.seed,
            operation_timeout: self.operation_timeout,
            rng: SimRng::new(self.seed ^ WORKLOAD_SALT),
            shared: Shared::default(),
            hosts: Vec::new(),
            control,
            down: BTreeSet::new(),
            clients: Vec::new(),
            watchers: Vec::new(),
            partitions: BTreeSet::new(),
            impostors: self.impostors,
            router: None,
            fork_faults: BTreeMap::new(),
            fork_seen: BTreeMap::new(),
            fork_phases: BTreeMap::new(),
            fork_markers: BTreeSet::new(),
            fork_stamps: BTreeMap::new(),
            principals: BTreeMap::new(),
            binding_refusals_allowed: false,
            fork_expectations: ForkExpectations::default(),
            root,
        };
        for (name, role) in &self.hosts {
            let data_dir = fleet.root.path().join(name);
            std::fs::create_dir_all(&data_dir)?;
            let mut config = host_config(name, *role, data_dir.clone(), self.timings)?;
            config.node.advertise = Some(SocketAddr::new(fleet.sim.lookup(name.as_str()), PORT));
            if !role.runs_control() {
                config.control.address = Some(control_address);
            }
            if let Some(settings) = &self.replication {
                config.replication = ReplicationConfig::from(settings);
            }
            if self.router.is_some() {
                config.principals.push(router_principal());
            }
            if role.hosts_data() {
                for (archive, _, configure) in &self.archives {
                    let star =
                        "*".parse()
                            .map_err(|error: crate::changes::ParseMatcherError| {
                                SimError::Invalid(error.to_string())
                            })?;
                    // The store stands in for the directory, which is never written.
                    let mut entry =
                        ArchiveConfig::directory(archive.clone(), [star], data_dir.join("archive"));
                    configure(&mut entry);
                    config.archive.push(entry);
                }
            }
            for (_, configure) in self.configure.iter().filter(|(host, _)| host == name) {
                configure(&mut config);
            }
            let address = fleet.sim.lookup(name.as_str()).to_string();
            let issued = fleet.authority.issue_node(
                &config.node.id,
                &[name.clone(), address],
                certificate_validity(),
            )?;
            let identity = Identity::from_issued(fleet.authority.certificate_pem(), &issued)?;
            fleet.add_host(name, *role, config, identity, data_dir);
        }
        let data_hosts: Vec<String> = fleet.data_hosts().map(str::to_owned).collect();
        {
            let mut shared = fleet.shared.lock();
            for (archive, store, _) in &self.archives {
                shared
                    .object_stores
                    .insert(archive.clone(), Arc::clone(store));
            }
        }
        for name in data_hosts {
            fleet.add_lease_watcher(&name);
        }
        if let Some(router) = &self.router {
            fleet.add_router(router, control_address)?;
        }
        Ok(fleet)
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

/// The validity of every certificate the test authority issues; see [`CERTIFICATE_LIFETIME`].
pub(super) fn certificate_validity() -> Validity {
    Validity::new(Timestamp::from_millis(0), CERTIFICATE_LIFETIME)
}

/// The principal every simulated client acts as. Every host's configuration lets it do all a
/// principal may: administer, write, read, and stream every database, and run fleet
/// operations (ADR-0025).
pub(super) const WORKLOAD_PRINCIPAL: &str = "workload";

/// The router's `[[principal]]` entry: no scopes of its own, and its callers' tokens forwarded.
fn router_principal() -> crate::PrincipalConfig {
    crate::PrincipalConfig {
        name: ROUTER_PRINCIPAL
            .parse()
            .unwrap_or_else(|error| panic!("'{ROUTER_PRINCIPAL}' is not a principal: {error}")),
        scopes: crate::auth::Scopes::none(),
        forward_tokens: crate::TokenForwarding::Allowed,
    }
}

fn workload_principal() -> crate::PrincipalConfig {
    crate::PrincipalConfig {
        name: WORKLOAD_PRINCIPAL
            .parse()
            .unwrap_or_else(|error| panic!("'{WORKLOAD_PRINCIPAL}' is not a principal: {error}")),
        scopes: ["admin:*", "stream:*", "fleet"]
            .into_iter()
            .map(|scope| {
                scope
                    .parse()
                    .unwrap_or_else(|error| panic!("'{scope}' is not a scope: {error}"))
            })
            .collect(),
        forward_tokens: crate::TokenForwarding::Refused,
    }
}

fn other_principal() -> crate::PrincipalConfig {
    crate::PrincipalConfig {
        name: OTHER_PRINCIPAL
            .parse()
            .unwrap_or_else(|error| panic!("'{OTHER_PRINCIPAL}' is not a principal: {error}")),
        ..workload_principal()
    }
}

/// Fork hooks for a data host: with the `fault-hooks` feature, ones the test can hold and
/// release, with room for any fork; without it, inert ones.
fn new_fork_faults() -> ForkFaults {
    #[cfg(feature = "fault-hooks")]
    {
        let faults = ForkFaults::new();
        faults.set_disk(ROOMY_DISK, 2 * ROOMY_DISK);
        faults
    }
    #[cfg(not(feature = "fault-hooks"))]
    {
        let _ = ROOMY_DISK;
        ForkFaults::default()
    }
}

fn host_config(
    name: &str,
    role: NodeRole,
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
        changes: ChangesConfig::default(),
        archive: Vec::new(),
        tls: None,
        insecure_plaintext: crate::Plaintext::Refused,
        dev: crate::dev::DevMode::Off,
        principals: vec![workload_principal(), other_principal()],
        // The admin listener doesn't start on a simulated network; the sampler runs on its clock.
        admin: crate::AdminConfig::default(),
        log: crate::LogConfig::default(),
        limits: crate::LimitsConfig::default(),
        sessions: crate::SessionsConfig::default(),
        fork: crate::ForkConfig::default(),
    })
}

/// A host the fleet runs.
#[derive(Debug, Clone)]
struct HostInfo {
    name: String,
    role: NodeRole,
    data_dir: PathBuf,
    config: Config,
}

/// A running fleet of simulated hosts, each running the real [`Daemon`](crate::Daemon).
///
/// Every host is a turmoil host with its own data directory, which survives crashes. The test
/// drives simulated time with [`Fleet::step`], [`Fleet::run_for`], and [`Fleet::run_until`],
/// injects [`Fault`]s between steps, adds [`Workload`]s with [`Fleet::client`], and checks
/// invariants with [`Fleet::check`], [`Fleet::await_ring`], and [`Fleet::finish`]. Every
/// fault, publication, and operation lands in the [`Trace`].
pub struct Fleet {
    // Dropped first: hosts close their files before the data directories go.
    sim: turmoil::Sim<'static>,
    /// The authority every host's and client's certificate chains to.
    authority: Arc<Authority>,
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
    /// The lease watcher hosts, one per data host.
    watchers: Vec<String>,
    /// Links the test has partitioned and not repaired, each as an ordered pair.
    partitions: BTreeSet<(String, String)>,
    /// Client hosts that act as a node with that node's certificate.
    impostors: BTreeMap<String, NodeId>,
    /// The router host, if the fleet has one.
    router: Option<String>,
    /// Each host's fork fault hooks, which survive its restarts: inert unless the
    /// `fault-hooks` feature is on.
    fork_faults: BTreeMap<String, ForkFaults>,
    /// The passes of each host's fork work through each hook the fleet has recorded.
    fork_seen: BTreeMap<(String, FaultPoint, Option<OperationId>), u64>,
    /// Each fork's identity and phase as the fleet last saw it in control state.
    fork_phases: BTreeMap<DatabaseName, (ForkIdentity, ForkPhaseKind)>,
    /// Each fork's holders whose copy on disk the fleet has seen hold its marker.
    fork_markers: BTreeSet<(ForkIdentity, String)>,
    /// The size and modification time of each holder's copy files of each fork when the fleet
    /// last read them, so that it reads a copy again only once it changed.
    fork_stamps: BTreeMap<(DatabaseName, String), CopyStamp>,
    /// The principal each client acts as, if not [`WORKLOAD_PRINCIPAL`].
    principals: BTreeMap<String, String>,
    /// Whether data hosts may refuse peer calls for their bindings in this run.
    binding_refusals_allowed: bool,
    /// What the scenario expects of its forks.
    fork_expectations: ForkExpectations,
    root: TempDir,
}

impl fmt::Debug for Fleet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Fleet")
            .field("seed", &self.seed)
            .field("elapsed", &self.sim.elapsed())
            .field("hosts", &self.hosts)
            .field("control", &self.control)
            .field("down", &self.down)
            .finish_non_exhaustive()
    }
}

impl Fleet {
    /// Describes a fleet whose network and workloads are driven by `seed`.
    #[must_use]
    pub fn builder(seed: u64) -> FleetBuilder {
        FleetBuilder {
            seed,
            hosts: Vec::new(),
            timings: Timings::default(),
            duration: Duration::from_secs(600),
            operation_timeout: OPERATION_TIMEOUT,
            max_latency: MAX_LATENCY,
            configure: Vec::new(),
            replication: None,
            impostors: BTreeMap::new(),
            archives: Vec::new(),
            router: None,
        }
    }

    fn add_host(
        &mut self,
        name: &str,
        role: NodeRole,
        config: Config,
        identity: Identity,
        data_dir: PathBuf,
    ) {
        let (stop, _) = watch::channel(0_u64);
        self.shared.lock().stops.insert(name.to_owned(), stop);
        let shared = self.shared.clone();
        let host = name.to_owned();
        let started = config.clone();
        let faults = new_fork_faults();
        self.fork_faults.insert(name.to_owned(), faults.clone());
        // From the fleet's seed and the host's name, without drawing from the workloads'
        // generator, so that adding a host's randomness moves no workload.
        let seed = host_seed(self.seed, name);
        self.sim.host(name, move || {
            run_host(
                host.clone(),
                started.clone(),
                identity.clone(),
                seed,
                shared.clone(),
                faults.clone(),
            )
        });
        self.hosts.push(HostInfo {
            name: name.to_owned(),
            role,
            data_dir,
            config,
        });
    }

    /// Adds the router host `name`, which asks the control node at `control` where databases
    /// live.
    fn add_router(&mut self, name: &str, control: SocketAddr) -> Result<(), SimError> {
        let principal: crate::tls::Principal = ROUTER_PRINCIPAL
            .parse()
            .map_err(|error: crate::tls::NameError| SimError::Invalid(error.to_string()))?;
        let issued = self
            .authority
            .issue_client(&principal, certificate_validity())?;
        let identity = Identity::from_issued(self.authority.certificate_pem(), &issued)?;
        // Plaintext on every address: the simulated network has no eavesdropper.
        let config = crate::router::RouterConfig::from_toml(&format!(
            "listen = \"0.0.0.0:{ROUTER_PORT}\"\n[http]\nallow_plaintext = true\n\
             [control]\naddress = \"{control}\"\n"
        ))
        .map_err(|error| SimError::Invalid(error.to_string()))?;
        let seed = self.rng.next_u64();
        let (stop, _) = watch::channel(0_u64);
        self.shared.lock().stops.insert(name.to_owned(), stop);
        let shared = self.shared.clone();
        let host = name.to_owned();
        self.sim.host(name, move || {
            crate::sim::host::run_router(
                host.clone(),
                config.clone(),
                identity.clone(),
                seed,
                shared.clone(),
            )
        });
        self.router = Some(name.to_owned());
        Ok(())
    }

    /// Adds the lease watcher that follows data host `node`'s `WatchLeases` stream for the
    /// whole run (FR-042), on a client host of its own named [`lease_watcher`].
    fn add_lease_watcher(&mut self, node: &str) {
        let name = lease_watcher(node);
        self.watchers.push(name.clone());
        let session = WatcherSession {
            name: name.clone(),
            node: node.to_owned(),
            address: format!("{node}:{PORT}"),
            timeout: self.operation_timeout,
            shared: self.shared.clone(),
            identity: self.client_identity(&name),
        };
        self.sim.client(name, run_lease_watcher(session));
    }

    /// Returns the seed.
    #[must_use]
    pub fn seed(&self) -> u64 {
        self.seed
    }

    /// Returns the simulated time since the fleet was built.
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

    /// Returns the store the archive named `archive` writes to, if the fleet has one
    /// ([`FleetBuilder::archive`]).
    #[must_use]
    pub fn object_store(&self, archive: &str) -> Option<Arc<dyn ObjectStore>> {
        self.shared.lock().object_stores.get(archive).cloned()
    }

    /// Returns the values of `database`'s writes table in its running primary's copy, in the
    /// order they were committed, which is the order of the table's rows.
    ///
    /// # Errors
    ///
    /// [`SimError::Simulation`] if no running host serves the database or its copy can't be
    /// read.
    pub fn committed_values(&self, database: &DatabaseName) -> Result<Vec<i64>, SimError> {
        let primary = self
            .hosts
            .iter()
            .find(|host| {
                self.replication(&host.name)
                    .is_some_and(|replication| replication.primary_status(database).is_some())
            })
            .ok_or_else(|| SimError::Simulation(format!("no running host serves {database}")))?;
        let dir = primary
            .data_dir
            .join(DATABASES_DIR)
            .join(layout::database_dir(database));
        written_values(&dir)
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

    /// Returns host `name`'s metrics exposition, while its daemon runs (ADR-0034). The admin
    /// listener doesn't start on the simulated network; this reads the registry directly. Each
    /// incarnation counts from zero.
    #[must_use]
    pub fn metrics(&self, name: &str) -> Option<String> {
        self.shared
            .lock()
            .running
            .get(name)
            .map(|running| running.metrics.encode())
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
    /// It is seeded from the fleet's seed; clients added later fork from it too.
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
    /// from the fleet's seed in the order clients are added.
    pub fn client(&mut self, name: &str, workload: impl Workload) {
        self.add_client(name, None, workload);
    }

    /// Adds a client named `name` that runs `workload` as the principal `principal`, which every
    /// host knows: [`OTHER_PRINCIPAL`], or the workload's own, `workload`.
    pub fn client_as(&mut self, name: &str, principal: &str, workload: impl Workload) {
        self.principals
            .insert(name.to_owned(), principal.to_owned());
        self.fork_expectations.principal(name, principal);
        self.add_client(name, None, workload);
    }

    /// Returns host `name`'s fork fault hooks, which survive its restarts (FR-087): a data
    /// host's fork work passes them, and the control host's control service drops a fork
    /// request's reply through them.
    #[cfg(feature = "fault-hooks")]
    #[must_use]
    pub fn fork_faults(&self, name: &str) -> Option<ForkFaults> {
        self.fork_faults.get(name).cloned()
    }

    /// Sets every host's clock `by` ahead, as if that much time had passed in one step: how a
    /// run reaches a boundary days away, such as a fork operation's retention, in simulated time.
    pub fn shift_clocks(&mut self, by: Duration) {
        let total = self.shared.shift_clocks(by);
        self.note(format!("clocks set {} ms ahead", millis(by)));
        let at = self.sim.elapsed();
        self.shared.lock().history.observe_clock_shift(at, total);
    }

    /// Lets data hosts refuse peer calls for their bindings in this run, as a fork scenario's
    /// late requests and retries legitimately are (ADR-0046, point 4.1).
    pub fn allow_binding_refusals(&mut self) {
        self.binding_refusals_allowed = true;
    }

    /// What the run's forks are expected to do; [`Fleet::finish`] checks it.
    pub fn fork_expectations(&mut self) -> &mut ForkExpectations {
        &mut self.fork_expectations
    }

    /// Adds a client named `name` that runs `workload`, sending each database call to the data
    /// node `entry` first instead of locating the primary through the control plane, as
    /// `fleetringd db exec --address` does. It follows redirects from there, and reaches a primary
    /// while the control node is down.
    pub fn client_through(&mut self, name: &str, entry: &str, workload: impl Workload) {
        self.add_client(name, Some(format!("{entry}:{PORT}")), workload);
    }

    /// Adds a client named `name` that runs `workload`, sending its writes and reads through the
    /// fleet's router as Hrana pipelines, with a token for the principal it acts as (ADR-0037).
    /// Every other operation, a database's creation included, goes to the fleet directly, since
    /// Hrana can't create databases.
    ///
    /// # Errors
    ///
    /// [`SimError::Invalid`] if the fleet has no router; an error if the authority can't sign
    /// the token.
    pub fn client_via_router(
        &mut self,
        name: &str,
        workload: impl Workload,
    ) -> Result<(), SimError> {
        let router = self
            .router
            .clone()
            .ok_or_else(|| SimError::Invalid("the fleet has no router".into()))?;
        let signer = self
            .authority
            .token_signer()
            .map_err(|error| SimError::Invalid(error.to_string()))?;
        let scopes = ["admin:*"]
            .into_iter()
            .map(str::parse)
            .collect::<Result<_, _>>()
            .map_err(|error: crate::auth::ParseScopeError| SimError::Invalid(error.to_string()))?;
        // Tokens are judged by the hosts' simulated clocks, which start near the epoch.
        let token = signer
            .issue(&crate::auth::TokenSpec {
                principal: WORKLOAD_PRINCIPAL
                    .parse()
                    .map_err(|error: crate::tls::NameError| SimError::Invalid(error.to_string()))?,
                scopes,
                id: format!("sim-{name}").parse().map_err(
                    |error: fleetring_control::ParseCredentialIdError| {
                        SimError::Invalid(error.to_string())
                    },
                )?,
                not_before: Timestamp::ZERO,
                expires_at: Timestamp::ZERO
                    .saturating_add(fleetring_control::Millis::new(29 * 24 * 60 * 60 * 1000)),
            })
            .map_err(|error| SimError::Invalid(error.to_string()))?;
        let via = Via {
            address: format!("{router}:{ROUTER_PORT}"),
            token: token.expose().to_owned(),
        };
        self.add_client_with(name, None, Some(via), workload);
        Ok(())
    }

    /// Issues the client `name` a certificate for the principal [`WORKLOAD_PRINCIPAL`] from the
    /// fleet's authority.
    ///
    /// # Panics
    ///
    /// If the authority can't issue the certificate.
    fn client_identity(&self, name: &str) -> Identity {
        if let Some(node) = self.impostors.get(name) {
            let issued = self
                .authority
                .issue_node(node, &[name.to_owned()], certificate_validity())
                .unwrap_or_else(|error| {
                    panic!("can't issue the certificate of node {node} for '{name}': {error}")
                });
            return Identity::from_issued(self.authority.certificate_pem(), &issued)
                .unwrap_or_else(|error| {
                    panic!("can't load the certificate of node {node} for '{name}': {error}")
                });
        }
        let principal = self
            .principals
            .get(name)
            .map_or(WORKLOAD_PRINCIPAL, String::as_str);
        let principal = principal
            .parse()
            .unwrap_or_else(|error| panic!("'{principal}' is not a principal: {error}"));
        let issued = self
            .authority
            .issue_client(&principal, certificate_validity())
            .unwrap_or_else(|error| {
                panic!("can't issue a certificate for client '{name}': {error}")
            });
        Identity::from_issued(self.authority.certificate_pem(), &issued).unwrap_or_else(|error| {
            panic!("can't load the certificate of client '{name}': {error}")
        })
    }

    /// Adds a client acting as the principal `name`, with a certificate from the fleet's
    /// authority.
    ///
    /// # Panics
    ///
    /// If `name` isn't a valid principal name, or the authority can't issue for it.
    fn add_client(&mut self, name: &str, entry: Option<String>, workload: impl Workload) {
        self.add_client_with(name, entry, None, workload);
    }

    /// As [`add_client`](Self::add_client), with its writes and reads `via` a router if given.
    fn add_client_with(
        &mut self,
        name: &str,
        entry: Option<String>,
        via: Option<Via>,
        workload: impl Workload,
    ) {
        let identity = self.client_identity(name);
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
            via,
            timeout: self.operation_timeout,
            shared,
            identity,
            authority: Arc::clone(&self.authority),
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
            .map_err(|error| SimError::Simulation(error.to_string()))?;
        self.observe_forks();
        Ok(())
    }

    /// The passes of each data host's fork work through its hooks since the last step.
    #[cfg(feature = "fault-hooks")]
    fn hook_passes(&mut self, at: Duration) -> Vec<ForkPass> {
        let mut passes = Vec::new();
        for (host, faults) in &self.fork_faults {
            for ((point, operation), count) in faults.passes_by_operation() {
                let seen = self
                    .fork_seen
                    .entry((host.clone(), point, operation))
                    .or_default();
                if count > *seen {
                    *seen = count;
                    passes.push(ForkPass {
                        at,
                        host: host.clone(),
                        point,
                        operation,
                        count,
                    });
                }
            }
        }
        passes
    }

    /// Without the `fault-hooks` feature, the hooks count nothing.
    #[cfg(not(feature = "fault-hooks"))]
    fn hook_passes(&mut self, at: Duration) -> Vec<ForkPass> {
        let _ = (at, &self.fork_seen);
        Vec::new()
    }

    /// Records what changed since the last step in each host's fork work at its hooks, and in
    /// each fork's phase in control state, with every data host's rows of a fork that moved to a
    /// new replica set; and when each holder's copy on disk first holds its fork's marker
    /// (FR-087).
    fn observe_forks(&mut self) {
        let at = self.sim.elapsed();
        let passes = self.hook_passes(at);
        let mut transitions = Vec::new();
        let mut markers = Vec::new();
        if let Some(state) = self.control_state() {
            for db in state.databases() {
                let Some(fork) = db.fork() else {
                    continue;
                };
                let identity =
                    ForkIdentity::new(db.name().clone(), db.incarnation(), fork.operation());
                let phase = match fork.state() {
                    ForkState::Seeding { .. } => ForkPhaseKind::Seeding,
                    ForkState::Seeded { .. } => ForkPhaseKind::Seeded,
                    ForkState::Ready {
                        holders: Some(_), ..
                    } => ForkPhaseKind::ReadyKept,
                    ForkState::Ready { holders: None, .. } => ForkPhaseKind::Released,
                    ForkState::Failed { .. } => ForkPhaseKind::Failed,
                    _ => continue,
                };
                if let ForkState::Seeded { holders, .. }
                | ForkState::Ready {
                    holders: Some(holders),
                    ..
                } = fork.state()
                {
                    markers.extend(self.markers_seen(at, &identity, holders));
                }
                let now = (identity.clone(), phase);
                if self.fork_phases.get(db.name()) == Some(&now) {
                    continue;
                }
                self.fork_phases.insert(db.name().clone(), now);
                let moved = matches!(
                    phase,
                    ForkPhaseKind::Seeded | ForkPhaseKind::ReadyKept | ForkPhaseKind::Released
                );
                let rows = if moved {
                    self.replica_rows(db.name()).unwrap_or_default()
                } else {
                    check::ReplicaRows::new()
                };
                transitions.push(ForkTransition {
                    at,
                    // Worked out from the holders and the ring, not asked of control state.
                    replica_set: expected_replica_set(&state, db),
                    identity,
                    phase,
                    rows,
                });
            }
        }
        if passes.is_empty() && transitions.is_empty() && markers.is_empty() {
            return;
        }
        let mut shared = self.shared.lock();
        for pass in passes {
            shared.history.observe_fork_pass(pass);
        }
        for seen in markers {
            shared.history.observe_fork_marker(seen);
        }
        for transition in transitions {
            shared.history.observe_fork_transition(transition);
        }
    }

    /// The holders of `identity` whose copy on disk holds its marker for the first time now. A
    /// copy is read again only once its files changed.
    fn markers_seen(
        &mut self,
        at: Duration,
        identity: &ForkIdentity,
        holders: &BTreeSet<NodeId>,
    ) -> Vec<ForkMarkerSeen> {
        let marker = (
            identity.incarnation.get(),
            identity.operation.to_bytes().to_vec(),
        );
        let mut seen = Vec::new();
        for holder in holders {
            let host = holder.to_string();
            let key = (identity.clone(), host.clone());
            if self.fork_markers.contains(&key) {
                continue;
            }
            let Some(info) = self.host(&host) else {
                continue;
            };
            let dir = info
                .data_dir
                .join(DATABASES_DIR)
                .join(layout::database_dir(&identity.name));
            let stamp = CopyStamp::of(&dir);
            let stamps = self
                .fork_stamps
                .entry((identity.name.clone(), host.clone()))
                .or_default();
            if *stamps == stamp {
                continue;
            }
            *stamps = stamp;
            if read_copy(&dir).is_ok_and(|copy| copy.markers.contains(&marker)) {
                self.fork_markers.insert(key);
                seen.push(ForkMarkerSeen {
                    at,
                    identity: identity.clone(),
                    host,
                });
            }
        }
        seen
    }

    /// Advances the simulation by `duration`.
    ///
    /// # Errors
    ///
    /// As [`Fleet::step`].
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
    /// from [`Fleet::step`].
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

    /// Steps until every workload added with [`Fleet::client`] has finished, except endless
    /// ones such as an [`Operator`](crate::sim::Operator).
    ///
    /// # Errors
    ///
    /// As [`Fleet::run_until`].
    pub fn await_workloads(&mut self, limit: Duration) -> Result<Duration, SimError> {
        self.run_until(limit, "workloads finishing", Self::workloads_done)
    }

    /// Returns whether every workload added with [`Fleet::client`] has finished, except
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
    /// As [`Fleet::await_ring_on`].
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
    /// [`Fleet::step`].
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

    /// How many peer calls the running data host `name` has refused since it started because
    /// they failed their placement binding (FR-050), or `None` if it isn't running.
    #[must_use]
    pub fn binding_refusals(&self, name: &str) -> Option<u64> {
        self.replication(name)
            .map(|replication| replication.binding_refusals())
    }

    /// Checks that no running data host has refused a peer call for failing its binding: in a
    /// fleet without impostors, every call is legitimate, membership changes and failovers
    /// included (FR-050).
    ///
    /// # Errors
    ///
    /// [`Invariant::NodeCallsBound`](check::Invariant::NodeCallsBound) naming the hosts that
    /// refused, and how many calls each.
    pub fn assert_no_binding_refusals(&self) -> Result<(), Violation> {
        let refused: Vec<String> = self
            .data_hosts()
            .filter_map(|host| {
                self.binding_refusals(host)
                    .filter(|count| *count > 0)
                    .map(|count| format!("{host} refused {count}"))
            })
            .collect();
        if refused.is_empty() {
            return Ok(());
        }
        Err(Violation::new(
            check::Invariant::NodeCallsBound,
            format!(
                "legitimate node calls were refused for their binding: {}",
                refused.join(", ")
            ),
        ))
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

    /// Returns the values of the writes-table rows every data host's file sinks delivered for
    /// `database`, read from their files: by host, in the order each wrote them.
    ///
    /// # Errors
    ///
    /// [`SimError::Simulation`] if a sink's file can't be read.
    pub fn sink_values(
        &self,
        database: &DatabaseName,
    ) -> Result<BTreeMap<String, Vec<i64>>, SimError> {
        let mut delivered: BTreeMap<String, Vec<i64>> = BTreeMap::new();
        for host in self.hosts.iter().filter(|host| host.role.hosts_data()) {
            for sink in &host.config.changes.sink {
                let (SinkKind::File, Some(dir)) = (sink.kind, &sink.path) else {
                    continue;
                };
                let path = database_file(dir, database);
                let records = read_records(&path).map_err(|error| {
                    SimError::Simulation(format!(
                        "can't read {}'s sink file {}: {error}",
                        host.name,
                        path.display()
                    ))
                })?;
                let values = delivered.entry(host.name.clone()).or_default();
                for record in records {
                    let crate::changes::RecordContent::Logical { rows } = record.content else {
                        continue;
                    };
                    values.extend(
                        rows.iter()
                            .filter(|row| row.table == WRITES_TABLE)
                            .filter_map(|row| {
                                row.values().iter().find_map(|column| {
                                    match (column.name.as_str(), &column.value) {
                                        ("value", crate::engine::Value::Integer(value)) => {
                                            Some(*value)
                                        }
                                        _ => None,
                                    }
                                })
                            }),
                    );
                }
            }
        }
        Ok(delivered)
    }

    /// Returns the hash chain each running replica of `database` holds, by host: the primary's,
    /// and every follower's that the primary counts as connected and on its log, so a follower
    /// cut off with a tail the next epoch will truncate isn't compared.
    #[must_use]
    pub fn replica_chains(&self, database: &DatabaseName) -> ReplicaChains {
        let mut chains = ReplicaChains::new();
        let Some(primary) = self.primary_replication(database) else {
            return chains;
        };
        let Some(status) = primary.primary_status(database) else {
            return chains;
        };
        let primary_host = self
            .hosts
            .iter()
            .map(|host| host.name.clone())
            .find(|host| {
                self.replication(host)
                    .is_some_and(|replication| replication.primary_status(database).is_some())
            });
        if let (Some(host), Some(chain)) = (primary_host, primary.chain_history(database)) {
            chains.insert(host, chain);
        }
        for follower in status
            .followers
            .iter()
            .filter(|follower| follower.connected && follower.reported && !follower.needs_snapshot)
        {
            let host = follower.node.to_string();
            if let Some(chain) = self
                .replication(&host)
                .and_then(|replication| replication.chain_history(database))
            {
                chains.insert(host, chain);
            }
        }
        chains
    }

    /// Checks that the replicas of every database created through the data plane agree on
    /// their hash chains at every position they have in common ([`check::chains_agree`]), as
    /// [`Fleet::replica_chains`] gathers them. Returns how many databases were checked.
    ///
    /// # Errors
    ///
    /// A [`Violation`] of [`Invariant::ChainAgrees`](check::Invariant::ChainAgrees).
    pub fn check_chains(&self) -> Result<usize, Violation> {
        let mut checked = 0;
        for (database, _) in created(&self.history()) {
            let chains = self.replica_chains(&database);
            if chains.len() > 1 {
                check::chains_agree(&database, &chains)?;
                checked += 1;
            }
        }
        Ok(checked)
    }

    /// Runs the history checker ([`history::check`]) over every operation so far and each
    /// host's copy of every database created through the data plane, read from disk: every
    /// acknowledged write is on a write quorum and in every later read, no refused write is
    /// anywhere, one primary per epoch, and each client reads its writes and never loses one it
    /// read. It holds at any moment.
    ///
    /// Every database is held to the [`Guarantee`] its creation reported: its replication
    /// factor and the durability mode the control plane placed it with.
    ///
    /// # Errors
    ///
    /// A [`Violation`] with the history around it, or [`SimError::Simulation`] if a copy can't
    /// be read.
    pub fn check_history(&self) -> Result<(), SimError> {
        let history = self.history();
        // A name dropped during the run may name another database afterwards: the fork
        // properties hold its lives apart ([`crate::sim::forks`]).
        let history = history.without_databases(&history.dropped());
        let mut databases = BTreeMap::new();
        for (database, guarantee) in created(&history) {
            let rows = self.replica_rows(&database)?;
            databases.insert(database, Replicated::new(guarantee, rows));
        }
        history::check(&history, &databases)?;
        Ok(())
    }

    /// Checks every file sink the data hosts are configured with
    /// ([`check::change_stream_consistent`]): for each database created through the data plane,
    /// the lines each host's sink wrote are an in-order, gapless stream of writes the history
    /// knows, with duplicates allowed and nothing refused or invented, and a stream that moved to
    /// a new primary continued at a mark the old one replicated
    /// ([`check::change_streams_resume`], with [`check::REPEATS_PER_FAILOVER`]). Returns how many
    /// records were checked.
    ///
    /// # Errors
    ///
    /// A [`Violation`], or [`SimError::Simulation`] if a sink's file can't be read.
    pub fn check_change_streams(&self) -> Result<usize, SimError> {
        let history = self.history();
        // Each sink's stream of each database, per host: a sink's marks are its own.
        let mut streams: BTreeMap<(String, DatabaseName), HostStreams> = BTreeMap::new();
        for host in self.hosts.iter().filter(|host| host.role.hosts_data()) {
            for sink in &host.config.changes.sink {
                let (SinkKind::File, Some(dir)) = (sink.kind, &sink.path) else {
                    continue;
                };
                for (database, _) in created(&history) {
                    let path = database_file(dir, &database);
                    let records = read_records(&path).map_err(|error| {
                        SimError::Simulation(format!(
                            "can't read {}'s sink file {}: {error}",
                            host.name,
                            path.display()
                        ))
                    })?;
                    streams
                        .entry((sink.name.clone(), database))
                        .or_default()
                        .push((host.name.clone(), records));
                }
            }
        }
        let mut checked = 0;
        for ((_, database), hosts) in &streams {
            // The `(epoch, end)` of every record on any host: where a new primary may resume.
            let mut marks = BTreeSet::new();
            for record in hosts.iter().flat_map(|(_, records)| records) {
                let end = unhex(&record.position).map_err(|_| {
                    SimError::Simulation(format!("{database} has a position that isn't hex"))
                })?;
                marks.insert((record.epoch, Position::from(end)));
            }
            for (host, records) in hosts {
                check::change_stream_consistent(&history, database, host, records, &marks)?;
                checked += records.len();
            }
            check::change_streams_resume(database, hosts, check::REPEATS_PER_FAILOVER)?;
        }
        Ok(checked)
    }

    /// Steps until the followers of every database created through the data plane have
    /// acknowledged everything its primary committed, then checks that every replica holds the
    /// same rows and the same hash chain ([`Fleet::check_chains`]), and runs the history
    /// checker ([`Fleet::check_history`]).
    ///
    /// # Errors
    ///
    /// [`SimError::Timeout`] if the followers don't catch up within `limit`, or a [`Violation`]
    /// of [`Invariant::ReplicasConverged`](check::Invariant::ReplicasConverged),
    /// [`Invariant::ChainAgrees`](check::Invariant::ChainAgrees), or from the history checker.
    pub fn await_replicas_converged(&mut self, limit: Duration) -> Result<Duration, SimError> {
        let took = self.run_until(limit, "replicas catching up", Self::replicas_caught_up)?;
        for (database, _) in created(&self.history()) {
            let rows = self.replica_rows(&database)?;
            check::replicas_converged(&database, &rows)?;
        }
        self.check_chains()?;
        self.check_history()?;
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
            // The router is a client of the fleet: its links can fail, as clients' can.
            let is_client = self.clients.iter().any(|client| client == name)
                || self.router.as_deref() == Some(name);
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
                self.run_until(STOP_LIMIT, &what, |fleet| !fleet.is_running(&name))?;
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
            .chain(self.watchers.iter().cloned())
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

    /// Drops every message between `a` and `b` until [`Fleet::repair`].
    ///
    /// # Errors
    ///
    /// As [`Fleet::inject`].
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
    /// As [`Fleet::inject`].
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
    /// As [`Fleet::inject`].
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
    /// As [`Fleet::inject`].
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
    /// As [`Fleet::inject`].
    pub fn crash(&mut self, name: &str) -> Result<(), SimError> {
        self.inject(Fault::Crash {
            host: name.to_owned(),
        })
    }

    /// Shuts host `name`'s daemon down gracefully and steps until it has stopped.
    ///
    /// # Errors
    ///
    /// As [`Fleet::inject`].
    pub fn stop(&mut self, name: &str) -> Result<(), SimError> {
        self.inject(Fault::Stop {
            host: name.to_owned(),
        })
    }

    /// Tells host `name`'s daemon to shut down gracefully, as a signal would, without waiting
    /// for it: the daemon drains and leaves while the test goes on, and may be crashed meanwhile.
    ///
    /// # Errors
    ///
    /// [`SimError::Invalid`] if there is no such host.
    pub fn begin_stop(&mut self, name: &str) -> Result<(), SimError> {
        if self.host(name).is_none() {
            return Err(SimError::Invalid(format!("no host named '{name}'")));
        }
        self.record(EventKind::Note(format!("stopping {name}")));
        self.down.insert(name.to_owned());
        self.shared.signal_stop(name);
        Ok(())
    }

    /// Starts host `name`'s daemon again on the same data directory.
    ///
    /// # Errors
    ///
    /// As [`Fleet::inject`].
    pub fn restart(&mut self, name: &str) -> Result<(), SimError> {
        self.inject(Fault::Restart {
            host: name.to_owned(),
        })
    }

    /// Stops every host gracefully and runs the final checks: in a fleet without impostors, no
    /// running data host refused a peer call for its binding
    /// ([`Fleet::assert_no_binding_refusals`]); the hash chains of every replica
    /// still following agree ([`Fleet::check_chains`]), the history checker
    /// ([`Fleet::check_history`]) over the whole history and every host's copy on disk, the
    /// control log replays to the state the control node last published, which holds every
    /// acknowledged control operation, and what the lease watchers learned matches the leases
    /// the log granted ([`check::lease_events_match_control`]).
    ///
    /// # Errors
    ///
    /// A [`Violation`], or an error from stepping while hosts stop.
    pub fn finish(mut self) -> Result<Report, SimError> {
        self.check()?;
        // Only an impostor's calls are refused for their bindings.
        if self.impostors.is_empty() && !self.binding_refusals_allowed {
            self.assert_no_binding_refusals()?;
        }
        self.check_chains()?;
        let control = self.control_handle();
        self.record(EventKind::Note("shutting down".into()));
        let running: Vec<String> = self.shared.lock().running.keys().cloned().collect();
        for name in &running {
            self.shared.signal_stop(name);
        }
        let stopped = self.run_until(STOP_LIMIT, "every host stopping", |fleet| {
            fleet.shared.lock().running.is_empty()
        });
        if let Err(SimError::Timeout { limit, .. }) = stopped {
            let running: Vec<String> = self.shared.lock().running.keys().cloned().collect();
            return Err(SimError::Timeout {
                what: format!("stopping every host ({running:?} still running)"),
                limit,
            });
        }
        stopped?;
        self.check_history()?;
        let _records = self.check_change_streams()?;
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
        check::copies_kept_until_dropped(&log, &self.history(), |node, database| {
            self.data_dir(node).is_some_and(|dir| {
                dir.join(crate::daemon::DATABASES_DIR)
                    .join(crate::layout::database_dir(database))
                    .exists()
            })
        })?;
        let trace = self.trace();
        let history = self.history();
        check::lease_events_match_control(&history, log.lease_holders())?;
        let disk = self.fork_disk(&log);
        crate::sim::forks::check(
            log.genesis(),
            log.commands(),
            &history,
            &disk,
            &self.fork_expectations,
        )?;
        Ok(Report {
            trace,
            history,
            log,
        })
    }
}

impl Fleet {
    /// Every data host's copies of each database a fork in `log` involves, and its staged
    /// copies, read from disk.
    fn fork_disk(&self, log: &ControlLog) -> ForkDisk {
        let mut names = BTreeSet::new();
        for command in log.commands() {
            if let fleetring_control::Action::ForkDatabase { intent, .. } = &command.action {
                names.insert(intent.name.clone());
                names.insert(intent.source.clone());
            }
        }
        let mut disk = ForkDisk {
            hosts: self
                .hosts
                .iter()
                .filter(|host| host.role.hosts_data())
                .map(|host| host.name.clone())
                .collect(),
            ..ForkDisk::default()
        };
        if names.is_empty() {
            return disk;
        }
        for host in self.hosts.iter().filter(|host| host.role.hosts_data()) {
            for name in &names {
                let dir = host
                    .data_dir
                    .join(DATABASES_DIR)
                    .join(layout::database_dir(name));
                if let Ok(copy) = read_copy(&dir) {
                    disk.copies.insert((name.clone(), host.name.clone()), copy);
                }
                let staged = host
                    .data_dir
                    .join("staged")
                    .join(layout::database_dir(name));
                for identity in std::fs::read_dir(&staged).into_iter().flatten().flatten() {
                    let dir_name = identity.file_name().to_string_lossy().into_owned();
                    for attempt in std::fs::read_dir(identity.path())
                        .into_iter()
                        .flatten()
                        .flatten()
                    {
                        let copy = attempt.path().join("copy");
                        if let Ok(read) = read_copy(&copy) {
                            disk.staged.push((
                                host.name.clone(),
                                name.clone(),
                                dir_name.clone(),
                                read.values,
                            ));
                        }
                    }
                }
            }
        }
        disk
    }
}

/// The size and modification time of each file of a SQLite copy, or `None` for one that is
/// missing: what tells the fleet a copy changed since it last read it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct CopyStamp([Option<(u64, std::time::SystemTime)>; 2]);

impl CopyStamp {
    fn of(dir: &Path) -> Self {
        let stamp = |file: &str| {
            let meta = std::fs::metadata(dir.join(file)).ok()?;
            Some((meta.len(), meta.modified().ok()?))
        };
        Self([stamp("data.db"), stamp("data.db-wal")])
    }
}

/// Reads the writes table and the fork markers of the SQLite copy in `dir`, from a scratch copy
/// of its files. A directory without a copy is an error.
fn read_copy(dir: &Path) -> Result<CopyOnDisk, Box<dyn std::error::Error>> {
    if !dir.join("data.db").try_exists()? {
        return Err("no copy".into());
    }
    let values = written_values(dir)?;
    let scratch = tempfile::tempdir()?;
    for file in ["data.db", "data.db-wal"] {
        if dir.join(file).try_exists()? {
            std::fs::copy(dir.join(file), scratch.path().join(file))?;
        }
    }
    let connection = rusqlite::Connection::open(scratch.path().join("data.db"))?;
    let tables: i64 = connection.query_row(
        "SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name = '_fleetring_fork'",
        [],
        |row| row.get(0),
    )?;
    let markers = if tables == 0 {
        Vec::new()
    } else {
        let mut statement =
            connection.prepare("SELECT fork_incarnation, operation FROM _fleetring_fork")?;
        statement
            .query_map([], |row| {
                Ok((
                    row.get::<_, i64>(0)?.cast_unsigned(),
                    row.get::<_, Vec<u8>>(1)?,
                ))
            })?
            .collect::<Result<Vec<_>, _>>()?
    };
    Ok(CopyOnDisk { values, markers })
}

/// The name of the client host that watches data host `node`'s leases.
#[must_use]
pub fn lease_watcher(node: &str) -> String {
    format!("{node}-leases")
}

impl Drop for Fleet {
    /// Prints the trace when [`TRACE_VAR`] is set, so a failing seed can be followed event by
    /// event.
    fn drop(&mut self) {
        if std::env::var_os(TRACE_VAR).is_some() {
            eprintln!("trace of seed {}:\n{}", self.seed, self.trace());
        }
    }
}

/// Every database an acknowledged [`Operation::Create`] created, with the guarantee its
/// creation reported.
fn created(history: &History) -> Vec<(DatabaseName, Guarantee)> {
    let databases: BTreeMap<DatabaseName, Guarantee> = history
        .acknowledged()
        .filter_map(|(operation, ack)| match (operation, ack) {
            (
                Operation::Create {
                    database,
                    replication_factor,
                    ..
                },
                Ack::Opened { durability, .. },
            ) => {
                let factor = u8::try_from(*replication_factor)
                    .ok()
                    .and_then(ReplicationFactor::new)?;
                Some((database.clone(), Guarantee::new(factor, *durability)))
            }
            _ => None,
        })
        .collect();
    databases.into_iter().collect()
}

/// Reads the records a file sink wrote to `path`, or none if there is no file.
fn read_records(path: &Path) -> Result<Vec<ChangeRecord>, Box<dyn std::error::Error>> {
    let text = match std::fs::read_to_string(path) {
        Ok(text) => text,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(error.into()),
    };
    text.lines()
        .filter(|line| !line.is_empty())
        .map(|line| Ok(serde_json::from_str(line)?))
        .collect()
}

/// Reads the values in the [`WRITES_TABLE`] of the SQLite copy in `dir`, from a scratch copy of
/// its files.
/// The values of the writes table in the copy of a database at `dir`, such as a restored copy, in
/// the order they were committed: the order of the table's rows. The copy is read from a scratch
/// copy of its files, so that `dir` is left as it was.
///
/// # Errors
///
/// [`SimError::Simulation`] if the copy can't be read.
pub fn written_values(dir: &Path) -> Result<Vec<i64>, SimError> {
    let read = || -> Result<Vec<i64>, Box<dyn std::error::Error>> {
        let scratch = tempfile::tempdir()?;
        for file in ["data.db", "data.db-wal"] {
            if dir.join(file).try_exists()? {
                std::fs::copy(dir.join(file), scratch.path().join(file))?;
            }
        }
        let connection = rusqlite::Connection::open(scratch.path().join("data.db"))?;
        let tables: i64 = connection.query_row(
            "SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name = ?1",
            [WRITES_TABLE],
            |row| row.get(0),
        )?;
        if tables == 0 {
            return Ok(Vec::new());
        }
        let mut statement =
            connection.prepare(&format!("SELECT value FROM {WRITES_TABLE} ORDER BY rowid"))?;
        let values = statement
            .query_map([], |row| row.get::<_, i64>(0))?
            .collect::<Result<Vec<i64>, _>>()?;
        Ok(values)
    };
    read().map_err(|error| {
        SimError::Simulation(format!("can't read the copy at {}: {error}", dir.display()))
    })
}

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
