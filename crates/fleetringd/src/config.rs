//! Daemon configuration: a TOML file, optionally overridden by command-line flags.

use std::fs;
use std::io;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr};
use std::num::{NonZeroU64, NonZeroUsize};
use std::path::{Path, PathBuf};
use std::time::Duration;

use fleetring::{Host, NodeId, Weight, Zone};
use fleetring_control::{Acks, ControlConfig, Member, Millis};
use serde::Deserialize;

use crate::changes::ChangesConfig;
use crate::control::DurabilityDefaults;
use crate::engine::EngineConfig;
use crate::replication::ReplicationSettings;

/// Everything a daemon needs to start.
///
/// ```toml
/// data_dir = "/var/lib/fleetring"
/// listen = "0.0.0.0:7400"
/// role = "data"
///
/// [node]
/// id = "laptop-1"
/// zone = "home"
/// weight = 2
/// advertise = "10.0.0.2:7400"
///
/// [control]
/// address = "10.0.0.1:7400"
/// ```
///
/// The `[control]` table also holds the control node's timings; see [`ControlSettings`]. The
/// `[membership]` table sets how often a data node sends heartbeats; see [`MembershipSettings`].
/// The `[replication]` table paces replication; see [`ReplicationConfig`]. The `[changes]`
/// table lists the sinks that receive each database's changes; see [`ChangesConfig`].
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// This node's identity and placement attributes.
    pub node: NodeConfig,
    /// Where the node keeps its databases. One daemon locks it exclusively.
    pub data_dir: PathBuf,
    /// The address this node serves on.
    pub listen: SocketAddr,
    /// How to reach the control plane, and how the control node runs.
    #[serde(default)]
    pub control: ControlSettings,
    /// Which roles this daemon runs.
    #[serde(default)]
    pub role: Role,
    /// How the data role runs its open databases.
    #[serde(default)]
    pub engine: EngineConfig,
    /// How the data role reports to the control plane.
    #[serde(default)]
    pub membership: MembershipSettings,
    /// How the data role replicates its databases.
    #[serde(default)]
    pub replication: ReplicationConfig,
    /// Where the data role streams its databases' changes (FR-039).
    #[serde(default)]
    pub changes: ChangesConfig,
}

/// A node's identity and the attributes placement uses.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeConfig {
    /// The node's stable ID.
    pub id: NodeId,
    /// The failure domain replicas spread across first.
    #[serde(default)]
    pub zone: Option<Zone>,
    /// The machine, when several nodes share one.
    #[serde(default)]
    pub host: Option<Host>,
    /// The node's share of databases, relative to other nodes.
    #[serde(default = "default_weight")]
    pub weight: Weight,
    /// The address peers and clients reach this node at. It defaults to the address the node
    /// listens on, with the loopback address in place of an unspecified one, which only works
    /// when every node runs on one machine; set it when they don't.
    #[serde(default)]
    pub advertise: Option<SocketAddr>,
}

fn default_weight() -> Weight {
    Weight::ONE
}

impl NodeConfig {
    /// Returns the member this node joins the cluster as, without an address; the daemon adds the
    /// advertised one.
    #[must_use]
    pub fn to_member(&self) -> Member {
        let mut member = Member::new(self.id.clone()).with_weight(self.weight);
        if let Some(zone) = &self.zone {
            member = member.with_zone(zone.clone());
        }
        if let Some(host) = &self.host {
            member = member.with_host(host.clone());
        }
        member
    }
}

/// How many heartbeat intervals must fit in the control plane's heartbeat timeout, so that a node
/// survives two lost heartbeats without being marked down.
pub const HEARTBEATS_PER_TIMEOUT: u64 = 3;

/// How many lease renewals must fit in the lease duration, so that a primary survives two lost
/// renewals without its lease expiring.
pub const LEASE_RENEWALS_PER_DURATION: u64 = 3;

/// How many times a primary renews a lease within its duration, unless `lease_renewal_ms` says
/// otherwise.
const DEFAULT_RENEWALS_PER_DURATION: u64 = 5;

/// How often the control node looks for expired leases, by default.
const DEFAULT_TICK_INTERVAL: Millis = Millis::new(250);

/// The `[membership]` table: how a data node reports to the control plane.
///
/// ```toml
/// [membership]
/// heartbeat_interval_ms = 1000
/// on_shutdown = "leave"           # or "stay"
/// ```
///
/// [`HEARTBEATS_PER_TIMEOUT`] intervals must fit in `control.heartbeat_timeout_ms`. A data-only
/// node checks this against its own `[control]` table, so keep that table's timeout equal to the
/// control node's.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct MembershipSettings {
    /// How often the node sends a heartbeat, in milliseconds. The node also retries a failed
    /// join, and reopens a broken membership stream, at this interval.
    pub heartbeat_interval_ms: NonZeroU64,
    /// Whether a graceful shutdown leaves the cluster, or stays a member that is marked down
    /// until it starts again.
    pub on_shutdown: OnShutdown,
}

/// What a data node does with its membership when it shuts down gracefully.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "lowercase")]
#[non_exhaustive]
pub enum OnShutdown {
    /// Leave the cluster: the control plane removes the member at once and rebuilds the ring
    /// without it, so its replicas move to other nodes. The default: a planned departure is no
    /// failure.
    #[default]
    Leave,
    /// Stay a member: the control plane marks it down once its heartbeats stop, and its
    /// replica sets keep their places for when it starts again, as across a restart.
    Stay,
}

impl MembershipSettings {
    /// Returns the heartbeat interval.
    #[must_use]
    pub fn heartbeat_interval(&self) -> Duration {
        Duration::from_millis(self.heartbeat_interval_ms.get())
    }
}

impl Default for MembershipSettings {
    fn default() -> Self {
        Self {
            heartbeat_interval_ms: NonZeroU64::new(1_000).unwrap_or(NonZeroU64::MIN),
            on_shutdown: OnShutdown::Leave,
        }
    }
}

/// The `[replication]` table: how a data node paces and bounds replication, and the durability
/// the control node gives databases created without explicit settings (ADR-0023).
///
/// Every field is optional; the pacing defaults are those of [`ReplicationSettings::new`], and
/// the durability defaults are `quorum` with `min_replicas` at the write quorum W.
///
/// ```toml
/// [replication]
/// acks = "quorum"                    # or "all", or "local"; the default for new databases
/// min_replicas = 2                   # the default floor on reachable replicas; W if unset
/// max_in_flight = 8                  # batches in flight to one follower
/// ack_timeout_ms = 5000              # how long a write waits for its quorum
/// stall_timeout_ms = 2000            # how long a stream may go without an acknowledgement
/// open_timeout_ms = 30000            # how long a stream may take to open
/// retry_interval_ms = 200            # how long to wait before reopening a failed stream
/// checkpoint_threshold_bytes = 4194304
/// lag_limit_ms = 60000               # how long a follower may block checkpoints
/// retained_bytes = 67108864          # published batches kept for followers
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ReplicationConfig {
    /// The acknowledgement mode a database is created with unless its creation names one.
    pub acks: Acks,
    /// The floor on reachable replicas a database is created with unless its creation names
    /// one; the mode's write quorum W if unset. A value outside W ≤ `min_replicas` ≤ R for a
    /// database is clamped into that range.
    pub min_replicas: Option<u8>,
    /// How many batches may be in flight to one follower.
    pub max_in_flight: NonZeroUsize,
    /// How long a write waits for its quorum before it fails unacknowledged, in milliseconds.
    pub ack_timeout_ms: u64,
    /// How long a stream with batches in flight may go without an acknowledgement before it is
    /// reopened, in milliseconds.
    pub stall_timeout_ms: u64,
    /// How long a stream may take to open, the follower's initial copy included, in milliseconds.
    pub open_timeout_ms: u64,
    /// How long the primary waits before reopening a failed stream, in milliseconds.
    pub retry_interval_ms: u64,
    /// How many bytes of batches may be published after a checkpoint before the next one is due.
    pub checkpoint_threshold_bytes: u64,
    /// How long a follower may stay behind before it stops blocking checkpoints, in milliseconds.
    pub lag_limit_ms: u64,
    /// How many bytes of published batches the primary keeps for followers that are behind.
    pub retained_bytes: usize,
}

impl ReplicationConfig {
    /// Returns the durability the control node gives databases created without settings.
    #[must_use]
    pub fn durability_defaults(&self) -> DurabilityDefaults {
        DurabilityDefaults {
            acks: self.acks,
            min_replicas: self.min_replicas,
        }
    }

    /// Returns the settings replication runs with.
    #[must_use]
    pub fn settings(&self) -> ReplicationSettings {
        ReplicationSettings {
            max_in_flight: self.max_in_flight,
            ack_timeout: Duration::from_millis(self.ack_timeout_ms),
            stall_timeout: Duration::from_millis(self.stall_timeout_ms),
            open_timeout: Duration::from_millis(self.open_timeout_ms),
            retry_interval: Duration::from_millis(self.retry_interval_ms),
            checkpoint_threshold: self.checkpoint_threshold_bytes,
            lag_limit: Duration::from_millis(self.lag_limit_ms),
            retained_bytes: self.retained_bytes,
        }
    }
}

impl From<&ReplicationSettings> for ReplicationConfig {
    fn from(settings: &ReplicationSettings) -> Self {
        let millis = |duration: Duration| u64::try_from(duration.as_millis()).unwrap_or(u64::MAX);
        Self {
            acks: Acks::Quorum,
            min_replicas: None,
            max_in_flight: settings.max_in_flight,
            ack_timeout_ms: millis(settings.ack_timeout),
            stall_timeout_ms: millis(settings.stall_timeout),
            open_timeout_ms: millis(settings.open_timeout),
            retry_interval_ms: millis(settings.retry_interval),
            checkpoint_threshold_bytes: settings.checkpoint_threshold,
            lag_limit_ms: millis(settings.lag_limit),
            retained_bytes: settings.retained_bytes,
        }
    }
}

impl Default for ReplicationConfig {
    fn default() -> Self {
        Self::from(&ReplicationSettings::new())
    }
}

/// The `[control]` table: the control node's address, and the settings it runs with.
///
/// Every field is optional. The timings are those of `fleetring_control::ControlConfig`; they
/// apply when the control node creates its log, and an existing log keeps the timings it was
/// created with.
///
/// ```toml
/// [control]
/// address = "10.0.0.1:7400"   # required on a data-only node
/// lease_duration_ms = 10000
/// lease_renewal_ms = 2000     # how often a primary renews each lease; a fifth of the lease
///                             # duration if unset
/// failover_margin_ms = 2000
/// heartbeat_timeout_ms = 5000
/// tick_interval_ms = 250      # how often the control node looks for expired leases
/// snapshot_every = 1000       # commands between snapshots
/// ```
///
/// The defaults suit a local network: a primary that dies is replaced about
/// `lease_duration_ms + failover_margin_ms` after its last renewal. A data node checks that
/// [`LEASE_RENEWALS_PER_DURATION`] renewals fit in the lease duration, against its own
/// `[control]` table, so keep that table's timings equal to the control node's.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ControlSettings {
    /// The control node's address. Required unless this node runs the control role itself.
    pub address: Option<SocketAddr>,
    /// How long a lease lasts after it is granted or renewed, in milliseconds.
    pub lease_duration_ms: NonZeroU64,
    /// How often a primary renews each lease it holds, in milliseconds. Unset, it is a fifth of
    /// the lease duration.
    pub lease_renewal_ms: Option<NonZeroU64>,
    /// How long past a lease's expiry the control node waits before failing over, in
    /// milliseconds.
    pub failover_margin_ms: u64,
    /// How long a member stays live after its last heartbeat, in milliseconds.
    pub heartbeat_timeout_ms: NonZeroU64,
    /// How often the control node looks for expired leases, and how long it lets pass without a
    /// command before it ticks, in milliseconds.
    pub tick_interval_ms: NonZeroU64,
    /// How many commands the control node applies between snapshots.
    pub snapshot_every: NonZeroU64,
}

impl ControlSettings {
    /// Returns the timing rules for a new control state.
    #[must_use]
    pub fn control_config(&self) -> ControlConfig {
        ControlConfig::new()
            .with_lease_duration(Millis::new(self.lease_duration_ms.get()))
            .with_failover_margin(Millis::new(self.failover_margin_ms))
            .with_heartbeat_timeout(Millis::new(self.heartbeat_timeout_ms.get()))
    }

    /// Returns how often a primary renews each lease: `lease_renewal_ms`, or a fifth of the lease
    /// duration.
    #[must_use]
    pub fn lease_renewal(&self) -> Duration {
        let millis = self.lease_renewal_ms.map_or_else(
            || (self.lease_duration_ms.get() / DEFAULT_RENEWALS_PER_DURATION).max(1),
            NonZeroU64::get,
        );
        Duration::from_millis(millis)
    }

    /// Returns how often the control node looks for expired leases.
    #[must_use]
    pub fn tick_interval(&self) -> Duration {
        Duration::from_millis(self.tick_interval_ms.get())
    }
}

impl Default for ControlSettings {
    fn default() -> Self {
        let config = ControlConfig::new();
        let nonzero = |millis: Millis| NonZeroU64::new(millis.get()).unwrap_or(NonZeroU64::MIN);
        Self {
            address: None,
            lease_duration_ms: nonzero(config.lease_duration()),
            lease_renewal_ms: None,
            failover_margin_ms: config.failover_margin().get(),
            heartbeat_timeout_ms: nonzero(config.heartbeat_timeout()),
            tick_interval_ms: nonzero(DEFAULT_TICK_INTERVAL),
            snapshot_every: crate::control::ControlOptions::default().snapshot_every(),
        }
    }
}

/// Which roles a daemon runs (ADR-0015, point 2).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    /// Both roles in one process: a single-machine cluster.
    #[default]
    Both,
    /// Hosts databases, and reaches the control node at `control`.
    Data,
    /// Runs the control plane only.
    Control,
}

impl Role {
    /// Whether this role hosts databases.
    #[must_use]
    pub fn hosts_data(self) -> bool {
        matches!(self, Self::Both | Self::Data)
    }

    /// Whether this role runs the control plane.
    #[must_use]
    pub fn runs_control(self) -> bool {
        matches!(self, Self::Both | Self::Control)
    }
}

/// The error returned when configuration can't be read or is invalid.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ConfigError {
    /// The configuration file couldn't be read.
    #[error("can't read config file {path}")]
    Read {
        /// The file that couldn't be read.
        path: PathBuf,
        /// Why it couldn't be read.
        source: io::Error,
    },
    /// The file isn't valid TOML or has an invalid or unknown field. The message names it.
    #[error("invalid config: {0}")]
    Parse(#[from] toml::de::Error),
    /// A field that this role needs is missing.
    #[error("config field `{field}` is required: {reason}")]
    Missing {
        /// The missing field.
        field: &'static str,
        /// Why the field is needed.
        reason: &'static str,
    },
    /// A field's value conflicts with another field.
    #[error("config field `{field}` is invalid: {reason}")]
    Invalid {
        /// The invalid field.
        field: &'static str,
        /// What is wrong with it.
        reason: String,
    },
}

impl Config {
    /// The address a client on this machine dials to reach the control plane: `control.address`,
    /// or `listen` if this node runs the control role. `None` if neither applies, or if `listen`
    /// names port 0, which only the running daemon can resolve.
    #[must_use]
    pub fn control_target(&self) -> Option<SocketAddr> {
        self.control.address.or_else(|| {
            (self.role.runs_control() && self.listen.port() != 0).then(|| reachable(self.listen))
        })
    }

    /// Reads and validates a configuration file.
    ///
    /// # Errors
    ///
    /// [`ConfigError`] if the file can't be read, isn't valid, or misses a field its role needs.
    pub fn load(path: impl AsRef<Path>) -> Result<Self, ConfigError> {
        let path = path.as_ref();
        let text = fs::read_to_string(path).map_err(|source| ConfigError::Read {
            path: path.to_owned(),
            source,
        })?;
        Self::from_toml(&text)
    }

    /// Parses and validates configuration from TOML text.
    ///
    /// # Errors
    ///
    /// [`ConfigError`] if the text isn't valid or misses a field its role needs.
    pub fn from_toml(text: &str) -> Result<Self, ConfigError> {
        let config: Self = toml::from_str(text)?;
        config.validate()?;
        Ok(config)
    }

    /// Checks the rules that span fields. Call it again after applying overrides.
    ///
    /// # Errors
    ///
    /// [`ConfigError::Missing`] if a field this role needs is absent, and
    /// [`ConfigError::Invalid`] if `replication.retry_interval_ms` is 0, or a data node's heartbeat
    /// interval isn't well below the heartbeat timeout, or its lease renewal interval isn't well
    /// below the lease duration, or a `[[changes.sink]]` entry is invalid.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.role == Role::Data && self.control.address.is_none() {
            return Err(ConfigError::Missing {
                field: "control.address",
                reason: "a data-only node must know the control node's address",
            });
        }
        let interval = self.membership.heartbeat_interval_ms.get();
        let timeout = self.control.heartbeat_timeout_ms.get();
        if self.role.hosts_data() && interval.saturating_mul(HEARTBEATS_PER_TIMEOUT) > timeout {
            return Err(ConfigError::Invalid {
                field: "membership.heartbeat_interval_ms",
                reason: format!(
                    "{interval} ms leaves room for fewer than {HEARTBEATS_PER_TIMEOUT} heartbeats \
                     in control.heartbeat_timeout_ms ({timeout} ms)"
                ),
            });
        }
        let renewal = u64::try_from(self.control.lease_renewal().as_millis()).unwrap_or(u64::MAX);
        let lease = self.control.lease_duration_ms.get();
        if self.role.hosts_data() && renewal.saturating_mul(LEASE_RENEWALS_PER_DURATION) > lease {
            return Err(ConfigError::Invalid {
                field: "control.lease_renewal_ms",
                reason: format!(
                    "{renewal} ms leaves room for fewer than {LEASE_RENEWALS_PER_DURATION} renewals \
                     in control.lease_duration_ms ({lease} ms)"
                ),
            });
        }
        if self.replication.retry_interval_ms == 0 {
            return Err(ConfigError::Invalid {
                field: "replication.retry_interval_ms",
                reason: "must be at least 1 ms".to_owned(),
            });
        }
        self.changes.validate()
    }
}

/// The address a client on this machine dials to reach a server bound to `address`: the loopback
/// address in place of an unspecified one, which not every platform accepts as a destination.
pub(crate) fn reachable(address: SocketAddr) -> SocketAddr {
    match address {
        SocketAddr::V4(v4) if v4.ip().is_unspecified() => {
            SocketAddr::new(Ipv4Addr::LOCALHOST.into(), v4.port())
        }
        SocketAddr::V6(v6) if v6.ip().is_unspecified() => {
            SocketAddr::new(Ipv6Addr::LOCALHOST.into(), v6.port())
        }
        other => other,
    }
}
