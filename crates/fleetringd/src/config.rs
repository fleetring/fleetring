//! Daemon configuration: a TOML file, optionally overridden by command-line flags.

use std::collections::BTreeSet;
use std::fmt;
use std::fs;
use std::io;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr};
use std::num::{NonZeroU64, NonZeroUsize};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::Duration;

use fleetring::{Host, NodeId, Weight, Zone};
use fleetring_control::{Acks, ControlConfig, Member, Millis};
use serde::Deserialize;

use crate::auth::Scopes;
use crate::changes::ChangesConfig;
use crate::control::DurabilityDefaults;
use crate::dev::{self, DevMode};
use crate::durability::ArchiveConfig;
use crate::engine::EngineConfig;
use crate::replication::ReplicationSettings;
use crate::tls::Principal;

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
///
/// [tls]
/// ca = "/etc/fleetring/tls/ca.crt"
/// cert = "/etc/fleetring/tls/node.laptop-1.crt"
/// key = "/etc/fleetring/tls/node.laptop-1.key"
/// ```
///
/// The `[tls]` table names the node's certificate, key, and trusted authorities; see
/// [`TlsConfig`]. The `[control]` table also holds the control node's timings; see [`ControlSettings`]. The
/// `[membership]` table sets how often a data node sends heartbeats; see [`MembershipSettings`].
/// The `[replication]` table paces replication; see [`ReplicationConfig`]. The `[changes]`
/// table lists the sinks that receive each database's changes; see [`ChangesConfig`]. Each
/// `[[archive]]` entry writes databases to an object store; see [`ArchiveConfig`].
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
    pub role: NodeRole,
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
    /// The archives the data role writes its databases to (FR-049): `[[archive]]` entries.
    #[serde(default)]
    pub archive: Vec<ArchiveConfig>,
    /// The certificate, key, and trusted authorities every connection carries (ADR-0025).
    #[serde(default)]
    pub tls: Option<TlsConfig>,
    /// Whether the daemon may run without `[tls]`, in plaintext, which is for the simulation
    /// harness and local experiments only. `--insecure-plaintext` sets it from the command
    /// line.
    #[serde(default)]
    pub insecure_plaintext: Plaintext,
    /// Whether this is a dev mode node (ADR-0039): `dev = true` keeps every address on loopback
    /// and the control node reports it. `fleetringd dev` writes it.
    #[serde(default)]
    pub dev: DevMode,
    /// The principals client certificates may name, with what each may do (ADR-0025). A
    /// principal not listed may do nothing with a certificate alone. Keep the list identical on
    /// every node.
    #[serde(default, rename = "principal")]
    pub principals: Vec<PrincipalConfig>,
    /// The admin listener: metrics, health, and readiness over HTTP (ADR-0034).
    #[serde(default)]
    pub admin: AdminConfig,
    /// How the daemon writes its log lines.
    #[serde(default)]
    pub log: LogConfig,
}

/// The `[log]` table: how the daemon writes its log lines (ADR-0034 point 8).
///
/// ```toml
/// [log]
/// format = "json"   # or "text", the default
/// ```
///
/// `--log-format` and `FLEETRING_LOG_FORMAT` override it, in that order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct LogConfig {
    /// The format of every log line.
    pub format: LogFormat,
}

/// How log lines are written.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "lowercase")]
#[non_exhaustive]
pub enum LogFormat {
    /// Human-readable lines, the default.
    #[default]
    Text,
    /// One JSON object per line, with the current span and its parents.
    Json,
}

impl LogFormat {
    /// The format's name, as the configuration and the command line spell it.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Text => "text",
            Self::Json => "json",
        }
    }
}

impl fmt::Display for LogFormat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// The error returned when a log format isn't `text` or `json`.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("unknown log format '{0}'; use text or json")]
pub struct ParseLogFormatError(String);

impl FromStr for LogFormat {
    type Err = ParseLogFormatError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        match text {
            "text" => Ok(Self::Text),
            "json" => Ok(Self::Json),
            other => Err(ParseLogFormatError(other.to_owned())),
        }
    }
}

/// The default admin listener: loopback, next to the gRPC port's default 7400.
const DEFAULT_ADMIN_LISTEN: SocketAddr =
    SocketAddr::new(std::net::IpAddr::V4(Ipv4Addr::LOCALHOST), 7401);

/// How often sampled gauges are refreshed, by default.
const DEFAULT_SAMPLE_INTERVAL: NonZeroU64 = NonZeroU64::new(10_000).expect("10000 is nonzero");

/// The `[admin]` table: the HTTP listener for metrics and probes (ADR-0034).
///
/// ```toml
/// [admin]
/// enabled = true                 # false: no admin listener
/// listen = "127.0.0.1:7401"
/// tls = false                    # true: HTTPS with the node's [tls] certificate
/// scrapers = ["prometheus"]      # principals whose client certificates may read /metrics
/// allow_plaintext = false        # true: plaintext on a non-loopback address
/// sample_interval_ms = 10000     # how often sampled gauges are refreshed
/// ```
///
/// The listener serves `/metrics`, `/healthz`, and `/readyz`. A `listen` address that isn't
/// loopback needs `tls = true` or `allow_plaintext = true`. With `tls`, `/metrics` answers only
/// client certificates whose principal `scrapers` lists; the probes answer anyone.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct AdminConfig {
    /// Whether the daemon serves the admin listener.
    pub enabled: AdminListener,
    /// The address it listens on.
    pub listen: SocketAddr,
    /// Whether it serves HTTPS with the node's `[tls]` certificate.
    pub tls: AdminTls,
    /// The principals whose client certificates may read `/metrics` over TLS.
    pub scrapers: Vec<Principal>,
    /// Whether it may serve plaintext on an address that isn't loopback.
    pub allow_plaintext: AdminPlaintext,
    /// How often the sampled gauges are refreshed, in milliseconds.
    pub sample_interval_ms: NonZeroU64,
}

impl AdminConfig {
    /// Returns how often the sampled gauges are refreshed.
    #[must_use]
    pub fn sample_interval(&self) -> Duration {
        Duration::from_millis(self.sample_interval_ms.get())
    }
}

impl Default for AdminConfig {
    fn default() -> Self {
        Self {
            enabled: AdminListener::Enabled,
            listen: DEFAULT_ADMIN_LISTEN,
            tls: AdminTls::Off,
            scrapers: Vec::new(),
            allow_plaintext: AdminPlaintext::Refused,
            sample_interval_ms: DEFAULT_SAMPLE_INTERVAL,
        }
    }
}

/// Whether the daemon serves the admin listener: `[admin] enabled`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(from = "bool")]
#[non_exhaustive]
pub enum AdminListener {
    /// The default: the daemon serves it.
    #[default]
    Enabled,
    /// `enabled = false`: no admin listener. Metrics are still kept.
    Disabled,
}

impl From<bool> for AdminListener {
    fn from(enabled: bool) -> Self {
        if enabled {
            Self::Enabled
        } else {
            Self::Disabled
        }
    }
}

/// Whether the admin listener serves HTTPS: `[admin] tls`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(from = "bool")]
#[non_exhaustive]
pub enum AdminTls {
    /// The default: plaintext HTTP.
    #[default]
    Off,
    /// `tls = true`: HTTPS with the node's own `[tls]` certificate, reloaded with it, asking
    /// for a client certificate from the fleet's authority without requiring one.
    NodeIdentity,
}

impl From<bool> for AdminTls {
    fn from(tls: bool) -> Self {
        if tls { Self::NodeIdentity } else { Self::Off }
    }
}

/// Whether the admin listener may serve plaintext on an address that isn't loopback:
/// `[admin] allow_plaintext`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(from = "bool")]
#[non_exhaustive]
pub enum AdminPlaintext {
    /// The default: such an address fails validation unless `tls = true`.
    #[default]
    Refused,
    /// `allow_plaintext = true`: for a network the operator controls, where anyone who can
    /// connect may read the metrics. The daemon warns at every start.
    Allowed,
}

impl From<bool> for AdminPlaintext {
    fn from(allowed: bool) -> Self {
        if allowed {
            Self::Allowed
        } else {
            Self::Refused
        }
    }
}

/// One `[[principal]]` entry: a principal's scopes when it connects with a client certificate
/// (ADR-0025).
///
/// ```toml
/// [[principal]]
/// name = "ops"
/// scopes = ["admin:*", "stream:*", "fleet"]
///
/// [[principal]]
/// name = "acme-app"
/// scopes = ["write:tenant:acme/*"]
///
/// [[principal]]
/// name = "router"
/// scopes = []
/// forward_tokens = true   # may carry its callers' tokens, and then acts as them
/// ```
///
/// Each scope is `<action>:<pattern>` with the action `read`, `write`, `admin`, or `stream`,
/// and the pattern an exact database name or a prefix ending in `*`; or the word `fleet`, for
/// control operations that name no database. `write` implies `read`, and `admin` implies both.
/// A request with a bearer token acts with the token's scopes instead.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PrincipalConfig {
    /// The principal, as its client certificates name it.
    pub name: Principal,
    /// What it may do.
    #[serde(default)]
    pub scopes: Scopes,
    /// Whether its connections may carry tokens that name other principals.
    #[serde(default)]
    pub forward_tokens: TokenForwarding,
}

/// Whether a principal's connections may carry bearer tokens that name another principal.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(from = "bool")]
#[non_exhaustive]
pub enum TokenForwarding {
    /// The default: a token must name the principal the client certificate names.
    #[default]
    Refused,
    /// `forward_tokens = true`: the principal relays its callers' requests, such as a router,
    /// and a request with a caller's token acts as that caller, with the token's scopes only.
    Allowed,
}

impl From<bool> for TokenForwarding {
    fn from(allowed: bool) -> Self {
        if allowed {
            Self::Allowed
        } else {
            Self::Refused
        }
    }
}

/// The `[tls]` table: the files the node's identity is loaded from, and reloaded from when they
/// change (ADR-0025).
///
/// ```toml
/// [tls]
/// ca = "/etc/fleetring/tls/ca.crt"        # the trusted authority certificates; two during a rotation
/// cert = "/etc/fleetring/tls/node.crt"    # this node's certificate, issued by `cert node`
/// key = "/etc/fleetring/tls/node.key"     # its private key
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TlsConfig {
    /// The PEM file with the authority certificates to trust. It may hold two, so a rotation
    /// can overlap.
    pub ca: PathBuf,
    /// The PEM file with this node's certificate chain.
    pub cert: PathBuf,
    /// The PEM file with this node's private key.
    pub key: PathBuf,
}

/// Whether a daemon without a `[tls]` table may start in plaintext.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(from = "bool")]
#[non_exhaustive]
pub enum Plaintext {
    /// The default: a daemon without `[tls]` refuses to start.
    #[default]
    Refused,
    /// `insecure_plaintext = true`: the daemon serves and dials in plaintext, and warns at
    /// every start. Nothing on the wire is authenticated or encrypted.
    Allowed,
}

impl From<bool> for Plaintext {
    fn from(allowed: bool) -> Self {
        if allowed {
            Self::Allowed
        } else {
            Self::Refused
        }
    }
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
    /// Returns the member this node joins the fleet as, without an address; the daemon adds the
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
    /// Whether a graceful shutdown leaves the fleet, or stays a member that is marked down
    /// until it starts again.
    pub on_shutdown: OnShutdown,
}

/// What a data node does with its membership when it shuts down gracefully.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "lowercase")]
#[non_exhaustive]
pub enum OnShutdown {
    /// Leave the fleet: the control plane removes the member at once and rebuilds the ring
    /// without it, so its replicas move to other nodes. The default: a planned leave is no
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
pub enum NodeRole {
    /// Both roles in one process: a single-machine fleet.
    #[default]
    Both,
    /// Hosts databases, and reaches the control node at `control`.
    Data,
    /// Runs the control plane only.
    Control,
}

impl NodeRole {
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
    /// below the lease duration, or a `[[changes.sink]]` or `[[archive]]` entry is invalid, or two
    /// share a name, or `[admin] listen` isn't a loopback address while neither `admin.tls` nor
    /// `admin.allow_plaintext` is set, or `[admin] scrapers` names a principal twice, or, with
    /// `dev = true`, an address isn't loopback, the role isn't `both`, or plaintext is allowed.
    ///
    /// Whether the daemon has an identity is checked when it starts, because the environment
    /// may supply one; see [`Daemon::start_with`](crate::Daemon::start_with).
    pub fn validate(&self) -> Result<(), ConfigError> {
        dev::validate(self)?;
        if self.role == NodeRole::Data && self.control.address.is_none() {
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
        let mut names = BTreeSet::new();
        for principal in &self.principals {
            if !names.insert(&principal.name) {
                return Err(ConfigError::Invalid {
                    field: "principal",
                    reason: format!("'{}' is listed more than once", principal.name),
                });
            }
        }
        if self.replication.retry_interval_ms == 0 {
            return Err(ConfigError::Invalid {
                field: "replication.retry_interval_ms",
                reason: "must be at least 1 ms".to_owned(),
            });
        }
        self.validate_admin()?;
        self.changes.validate()?;
        let mut names: BTreeSet<&str> = self
            .changes
            .sink
            .iter()
            .map(|sink| sink.name.as_str())
            .collect();
        for archive in &self.archive {
            archive.validate()?;
            if !names.insert(&archive.name) {
                return Err(ConfigError::Invalid {
                    field: "archive",
                    reason: format!(
                        "`{}` names another archive or sink: marks are kept by name, so each \
                         must be unique",
                        archive.name
                    ),
                });
            }
        }
        Ok(())
    }

    fn validate_admin(&self) -> Result<(), ConfigError> {
        let admin = &self.admin;
        if admin.enabled == AdminListener::Enabled
            && !admin.listen.ip().is_loopback()
            && admin.tls == AdminTls::Off
            && admin.allow_plaintext == AdminPlaintext::Refused
        {
            return Err(ConfigError::Invalid {
                field: "admin.listen",
                reason: format!(
                    "{} isn't a loopback address; set admin.tls = true, or \
                     admin.allow_plaintext = true on a network you control",
                    admin.listen
                ),
            });
        }
        let mut scrapers = BTreeSet::new();
        for scraper in &admin.scrapers {
            if !scrapers.insert(scraper) {
                return Err(ConfigError::Invalid {
                    field: "admin.scrapers",
                    reason: format!("'{scraper}' is listed more than once"),
                });
            }
        }
        Ok(())
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

#[cfg(test)]
mod tests {
    use super::*;

    const MINIMAL: &str = "data_dir = \"/tmp/fleetring-test\"\nlisten = \"127.0.0.1:7400\"\n\n\
                           [node]\nid = \"node-1\"\n";

    fn with(tables: &str) -> Result<Config, ConfigError> {
        Config::from_toml(&format!("{MINIMAL}{tables}"))
    }

    #[test]
    fn the_admin_listener_defaults_to_loopback_plaintext() {
        let defaults = with("").expect("a minimal config");
        let empty = with("\n[admin]\n").expect("an empty [admin] table");
        assert_eq!(defaults.admin, empty.admin);
        let admin = defaults.admin;
        assert_eq!(admin.enabled, AdminListener::Enabled);
        assert_eq!(admin.listen, "127.0.0.1:7401".parse().expect("an address"));
        assert_eq!(admin.tls, AdminTls::Off);
        assert!(admin.scrapers.is_empty());
        assert_eq!(admin.allow_plaintext, AdminPlaintext::Refused);
        assert_eq!(admin.sample_interval(), Duration::from_secs(10));
    }

    #[test]
    fn a_non_loopback_admin_address_needs_tls_or_plaintext() {
        let refused = with("\n[admin]\nlisten = \"0.0.0.0:7401\"\n").expect_err("not loopback");
        let ConfigError::Invalid { field, reason } = &refused else {
            panic!("{refused}");
        };
        assert_eq!(*field, "admin.listen");
        assert!(reason.contains("admin.tls"), "{reason}");
        assert!(reason.contains("admin.allow_plaintext"), "{reason}");
        let refused = with("\n[admin]\nlisten = \"10.0.0.1:7401\"\nallow_plaintext = false\n");
        assert!(refused.is_err(), "an explicit false refuses too");
        for allowed in [
            "listen = \"0.0.0.0:7401\"\ntls = true",
            "listen = \"0.0.0.0:7401\"\nallow_plaintext = true",
            "listen = \"0.0.0.0:7401\"\nenabled = false",
            "listen = \"[::1]:7401\"",
        ] {
            let config = with(&format!("\n[admin]\n{allowed}\n"));
            assert!(config.is_ok(), "{allowed}: {config:?}");
        }
    }

    #[test]
    fn duplicate_scrapers_are_refused() {
        let refused = with("\n[admin]\nscrapers = [\"prometheus\", \"ops\", \"prometheus\"]\n")
            .expect_err("a duplicate");
        let ConfigError::Invalid { field, reason } = &refused else {
            panic!("{refused}");
        };
        assert_eq!(*field, "admin.scrapers");
        assert!(reason.contains("'prometheus'"), "{reason}");
        let unknown = with("\n[admin]\nport = 7401\n");
        assert!(unknown.is_err(), "unknown fields are refused");
    }

    #[test]
    fn the_log_format_defaults_to_text_and_reads_json() {
        assert_eq!(with("").expect("a config").log.format, LogFormat::Text);
        let json = with("\n[log]\nformat = \"json\"\n").expect("a config");
        assert_eq!(json.log.format, LogFormat::Json);
        assert!(with("\n[log]\nformat = \"yaml\"\n").is_err());
        assert_eq!("json".parse::<LogFormat>(), Ok(LogFormat::Json));
        assert_eq!("text".parse::<LogFormat>(), Ok(LogFormat::Text));
        let unknown = "JSON".parse::<LogFormat>().expect_err("case matters");
        assert_eq!(
            unknown.to_string(),
            "unknown log format 'JSON'; use text or json"
        );
    }
}
