//! Daemon configuration: a TOML file, optionally overridden by command-line flags.

use std::collections::BTreeSet;
use std::fmt;
use std::fs;
use std::io;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr};
use std::num::{NonZeroU32, NonZeroU64, NonZeroUsize};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::Duration;

use fleetring::{Host, NodeId, ReplicationFactor, Weight, Zone};
use fleetring_control::fork::ForkLimits;
use fleetring_control::{Acks, ControlConfig, Member, Millis};
use serde::Deserialize;

use crate::actor::SessionLimits;
use crate::auth::{Action, Scopes};
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
/// The `[replication]` table paces replication, and sets the replication factor and durability
/// of databases created without them; see [`ReplicationConfig`]. The `[changes]`
/// table lists the sinks that receive each database's changes; see [`ChangesConfig`]. Each
/// `[[archive]]` entry writes databases to an object store; see [`ArchiveConfig`]. The
/// `[limits]` table bounds how long a statement runs on a data node; see [`LimitsConfig`]. The
/// `[fork]` table sets forks' budgets and bounds; see [`ForkConfig`].
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
    /// What one request may take on a data node (FR-078).
    #[serde(default)]
    pub limits: LimitsConfig,
    /// How long a SQL session's transaction may hold a database's writer, and how many writes
    /// may wait for it (ADR-0037).
    #[serde(default)]
    pub sessions: SessionsConfig,
    /// Forks' budgets and timeouts on the control node, and what seeding one may cost a data
    /// node (ADR-0046, point 7).
    #[serde(default)]
    pub fork: ForkConfig,
}

/// The `[fork]` table: what a fork may take (ADR-0046, point 7). New in 0.8.0. Every default is
/// provisional: FR-087 measures it, and ADR-0046 is amended with the measured value.
///
/// ```toml
/// [fork]
/// # The control node's, recorded with each fork it accepts: changing them affects new forks only.
/// max_attempts = 5                    # attempts before a fork fails, retries exhausted
/// deadline_ms = 3600000               # how long a fork may stay seeding
/// attempt_timeout_ms = 1800000        # how long one attempt may run before the next starts
/// seeder_timeout_ms = 120000          # how long a seeder may be down before the next attempt
/// operation_retention_ms = 604800000  # how long an operation's record outlives its outcome
/// # Each data node's.
/// transfer_retries = 3                # a seeder's retries of a broken transfer, per attempt
/// transfer_backoff_ms = 1000          # the first retry's wait, doubling after each
/// max_pin_ms = 600000                 # how long a source's primary pins a copy for one seed
/// max_pin_wal_bytes = 1073741824      # how much the source's WAL may grow while it is pinned
/// max_pin_ms_per_hour = 1200000       # pins of one source database in any hour
/// max_pins_per_node = 2               # pins a source's primary holds at once
/// max_pins_per_database = 1           # pins of one source database at once
/// max_seeds_per_node = 2              # staged copies a node downloads at once
/// admission_min_free_bytes = 1073741824  # free bytes a copy must leave, at least
/// admission_min_free_percent = 5      # free share of the volume a copy must leave, at least
/// ```
///
/// A pin past `max_pin_ms` or `max_pin_wal_bytes` ends its stream with `ABORTED` and is released:
/// the source's writes always win over a fork. A request past `max_pin_ms_per_hour`,
/// `max_pins_per_node`, `max_pins_per_database`, or `max_seeds_per_node` is refused with
/// `RESOURCE_EXHAUSTED` and a retry-after, which doesn't fail the attempt. A data node admits a
/// copy only with room for it and a tenth more beside the larger of `admission_min_free_bytes`
/// and `admission_min_free_percent` of its volume; the control node's best-effort check counts
/// its own `admission_min_free_bytes`, which it records with each fork. `fleetring dev` writes a
/// small margin, since a laptop's disk is rarely 95% free. The control node's cap on fork
/// operations it keeps, 10,000, is fixed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ForkConfig {
    /// The most attempts a fork makes before it fails. At least 1.
    pub max_attempts: NonZeroU32,
    /// How long a fork may stay seeding before it fails, in milliseconds.
    pub deadline_ms: NonZeroU64,
    /// How long one attempt may run before the control node starts the next, in milliseconds.
    /// At most `deadline_ms`.
    pub attempt_timeout_ms: NonZeroU64,
    /// How long an attempt's seeder may be down before the control node starts the next attempt
    /// without it, in milliseconds. At most `attempt_timeout_ms`.
    pub seeder_timeout_ms: NonZeroU64,
    /// How long a fork operation's record, and so the deduplication of its ID, outlives its
    /// final outcome, in milliseconds.
    pub operation_retention_ms: NonZeroU64,
    /// How many times a seeder fetches again after a transfer breaks, within one attempt,
    /// before it reports the attempt failed.
    pub transfer_retries: u32,
    /// How long a seeder waits before its first retry, in milliseconds; each retry waits twice
    /// as long as the last.
    pub transfer_backoff_ms: NonZeroU64,
    /// The longest a source's primary pins a copy for one seeding stream, in milliseconds,
    /// including any checkpoint it holds back meanwhile.
    pub max_pin_ms: NonZeroU64,
    /// How many bytes a source's WAL may grow while a seeding stream pins it.
    pub max_pin_wal_bytes: NonZeroU64,
    /// How long, in milliseconds, one source database may be pinned in any hour, across forks.
    /// At least `max_pin_ms`.
    pub max_pin_ms_per_hour: NonZeroU64,
    /// How many seeding pins a source's primary holds at once.
    pub max_pins_per_node: NonZeroUsize,
    /// How many seeding pins of one source database a primary holds at once. At most
    /// `max_pins_per_node`.
    pub max_pins_per_database: NonZeroUsize,
    /// How many staged copies a node downloads at once.
    pub max_seeds_per_node: NonZeroUsize,
    /// The fewest free bytes a data node keeps beside a fork's copy and a tenth more, whatever
    /// its volume's size. The control node records its own with each fork it accepts, for its
    /// best-effort check.
    pub admission_min_free_bytes: u64,
    /// The smallest share of its volume, in percent, a data node keeps free beside a fork's copy
    /// and a tenth more. At most 100.
    pub admission_min_free_percent: u8,
}

/// The `[fork]` defaults of ADR-0046, point 7, all provisional.
const DEFAULT_FORK_MAX_ATTEMPTS: NonZeroU32 = NonZeroU32::new(5).expect("5 is nonzero");
const DEFAULT_FORK_DEADLINE: NonZeroU64 = NonZeroU64::new(60 * 60 * 1_000).expect("nonzero");
const DEFAULT_FORK_ATTEMPT_TIMEOUT: NonZeroU64 = NonZeroU64::new(30 * 60 * 1_000).expect("nonzero");
const DEFAULT_FORK_SEEDER_TIMEOUT: NonZeroU64 = NonZeroU64::new(2 * 60 * 1_000).expect("nonzero");
const DEFAULT_FORK_OPERATION_RETENTION: NonZeroU64 =
    NonZeroU64::new(7 * 24 * 60 * 60 * 1_000).expect("nonzero");
const DEFAULT_FORK_TRANSFER_RETRIES: u32 = 3;
const DEFAULT_FORK_TRANSFER_BACKOFF: NonZeroU64 = NonZeroU64::new(1_000).expect("nonzero");
const DEFAULT_FORK_MAX_PIN: NonZeroU64 = NonZeroU64::new(10 * 60 * 1_000).expect("nonzero");
const DEFAULT_FORK_MAX_PIN_WAL_BYTES: NonZeroU64 = NonZeroU64::new(1 << 30).expect("nonzero");
const DEFAULT_FORK_MAX_PIN_PER_HOUR: NonZeroU64 =
    NonZeroU64::new(20 * 60 * 1_000).expect("nonzero");
const DEFAULT_FORK_MAX_PINS_PER_NODE: NonZeroUsize = NonZeroUsize::new(2).expect("nonzero");
const DEFAULT_FORK_MAX_PINS_PER_DATABASE: NonZeroUsize = NonZeroUsize::new(1).expect("nonzero");
const DEFAULT_FORK_MAX_SEEDS_PER_NODE: NonZeroUsize = NonZeroUsize::new(2).expect("nonzero");
const DEFAULT_FORK_ADMISSION_MIN_FREE_BYTES: u64 = 1 << 30;
const DEFAULT_FORK_ADMISSION_MIN_FREE_PERCENT: u8 = 5;

impl ForkConfig {
    /// The limits the control node records with each fork it accepts (ADR-0046, point 3.1).
    #[must_use]
    pub fn limits(&self) -> ForkLimits {
        ForkLimits {
            max_attempts: self.max_attempts.get(),
            deadline: Millis::new(self.deadline_ms.get()),
            attempt_timeout: Millis::new(self.attempt_timeout_ms.get()),
            seeder_timeout: Millis::new(self.seeder_timeout_ms.get()),
            operation_retention: Millis::new(self.operation_retention_ms.get()),
            // The default isn't recorded, so that a fork made under it encodes as before.
            admission_min_free_bytes: (self.admission_min_free_bytes
                != DEFAULT_FORK_ADMISSION_MIN_FREE_BYTES)
                .then_some(self.admission_min_free_bytes),
        }
    }

    /// Returns how long a seeder waits before its first retry of a broken transfer.
    #[must_use]
    pub fn transfer_backoff(&self) -> Duration {
        Duration::from_millis(self.transfer_backoff_ms.get())
    }

    /// Returns the longest a source's primary pins a copy for one seeding stream.
    #[must_use]
    pub fn max_pin(&self) -> Duration {
        Duration::from_millis(self.max_pin_ms.get())
    }

    /// Returns how long one source database may be pinned in any hour.
    #[must_use]
    pub fn max_pin_per_hour(&self) -> Duration {
        Duration::from_millis(self.max_pin_ms_per_hour.get())
    }

    /// Checks the rules between the table's fields.
    ///
    /// # Errors
    ///
    /// [`ConfigError::Invalid`] if `seeder_timeout_ms` is above `attempt_timeout_ms`,
    /// `attempt_timeout_ms` above `deadline_ms`, `max_pin_ms` above `max_pin_ms_per_hour`,
    /// `max_pins_per_database` above `max_pins_per_node`, or `admission_min_free_percent` above
    /// 100.
    fn validate(&self) -> Result<(), ConfigError> {
        let above = |field: &'static str, value: u64, other: &str, limit: u64| {
            Err(ConfigError::Invalid {
                field,
                reason: format!("{value} is above fork.{other} ({limit})"),
            })
        };
        if self.seeder_timeout_ms > self.attempt_timeout_ms {
            return above(
                "fork.seeder_timeout_ms",
                self.seeder_timeout_ms.get(),
                "attempt_timeout_ms",
                self.attempt_timeout_ms.get(),
            );
        }
        if self.attempt_timeout_ms > self.deadline_ms {
            return above(
                "fork.attempt_timeout_ms",
                self.attempt_timeout_ms.get(),
                "deadline_ms",
                self.deadline_ms.get(),
            );
        }
        if self.max_pin_ms > self.max_pin_ms_per_hour {
            return above(
                "fork.max_pin_ms",
                self.max_pin_ms.get(),
                "max_pin_ms_per_hour",
                self.max_pin_ms_per_hour.get(),
            );
        }
        if self.max_pins_per_database > self.max_pins_per_node {
            return above(
                "fork.max_pins_per_database",
                u64::try_from(self.max_pins_per_database.get()).unwrap_or(u64::MAX),
                "max_pins_per_node",
                u64::try_from(self.max_pins_per_node.get()).unwrap_or(u64::MAX),
            );
        }
        if self.admission_min_free_percent > 100 {
            return Err(ConfigError::Invalid {
                field: "fork.admission_min_free_percent",
                reason: format!(
                    "{} is above 100, the whole volume",
                    self.admission_min_free_percent
                ),
            });
        }
        Ok(())
    }
}

impl Default for ForkConfig {
    fn default() -> Self {
        Self {
            max_attempts: DEFAULT_FORK_MAX_ATTEMPTS,
            deadline_ms: DEFAULT_FORK_DEADLINE,
            attempt_timeout_ms: DEFAULT_FORK_ATTEMPT_TIMEOUT,
            seeder_timeout_ms: DEFAULT_FORK_SEEDER_TIMEOUT,
            operation_retention_ms: DEFAULT_FORK_OPERATION_RETENTION,
            transfer_retries: DEFAULT_FORK_TRANSFER_RETRIES,
            transfer_backoff_ms: DEFAULT_FORK_TRANSFER_BACKOFF,
            max_pin_ms: DEFAULT_FORK_MAX_PIN,
            max_pin_wal_bytes: DEFAULT_FORK_MAX_PIN_WAL_BYTES,
            max_pin_ms_per_hour: DEFAULT_FORK_MAX_PIN_PER_HOUR,
            max_pins_per_node: DEFAULT_FORK_MAX_PINS_PER_NODE,
            max_pins_per_database: DEFAULT_FORK_MAX_PINS_PER_DATABASE,
            max_seeds_per_node: DEFAULT_FORK_MAX_SEEDS_PER_NODE,
            admission_min_free_bytes: DEFAULT_FORK_ADMISSION_MIN_FREE_BYTES,
            admission_min_free_percent: DEFAULT_FORK_ADMISSION_MIN_FREE_PERCENT,
        }
    }
}

/// How long a statement may run on a data node unless `[limits] statement_timeout_ms` says
/// otherwise: 30 seconds.
const DEFAULT_STATEMENT_TIMEOUT: NonZeroU64 = NonZeroU64::new(30_000).expect("30000 is nonzero");

/// The `[limits]` table: what one request may take on a data node (FR-078). New in 0.8.0.
///
/// ```toml
/// [limits]
/// statement_timeout_ms = 30000   # the longest a write, query, or schema read runs
/// ```
///
/// A statement still running at its deadline is interrupted on the node: a query or a schema
/// read fails, and a write rolls back, committing and replicating nothing. The client gets
/// `DEADLINE_EXCEEDED`. A request whose call sets its own deadline (the gRPC `grpc-timeout`
/// header, which `db exec`, `db query`, and `fleetring mcp` send) stops at the earlier of this
/// limit and that deadline less a tenth of it, at most 100 ms, so that the node answers before
/// the call's deadline passes. One whose call is cancelled stops at once. The deadline counts
/// from when the node takes the request, so it includes any wait behind the database's other
/// requests.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct LimitsConfig {
    /// The longest a statement may run, in milliseconds: the default deadline of every write,
    /// query, and schema read, and the cap on a deadline a client asks for. At least 1.
    pub statement_timeout_ms: NonZeroU64,
}

impl LimitsConfig {
    /// Returns the longest a statement may run.
    #[must_use]
    pub fn statement_timeout(&self) -> Duration {
        Duration::from_millis(self.statement_timeout_ms.get())
    }
}

impl Default for LimitsConfig {
    fn default() -> Self {
        Self {
            statement_timeout_ms: DEFAULT_STATEMENT_TIMEOUT,
        }
    }
}

/// How long a SQL session's transaction may hold a database's writer unless `[sessions]
/// transaction_timeout_ms` says otherwise: 30 seconds.
const DEFAULT_TRANSACTION_TIMEOUT: NonZeroU64 = NonZeroU64::new(30_000).expect("30000 is nonzero");

/// How many writes may wait for a SQL session's transaction, per database, unless `[sessions]
/// max_waiting_writes` says otherwise.
const DEFAULT_MAX_WAITING_WRITES: NonZeroUsize = NonZeroUsize::new(64).expect("64 is nonzero");

/// How many SQL sessions one principal may hold open on a node, unless `[sessions]
/// max_sessions_per_principal` says otherwise.
const DEFAULT_MAX_SESSIONS_PER_PRINCIPAL: NonZeroUsize =
    NonZeroUsize::new(256).expect("256 is nonzero");

/// How long a SQL session may go without a call, unless `[sessions] idle_timeout_ms` says
/// otherwise: 60 seconds, longer than a transaction may stay open between calls.
const DEFAULT_SESSION_IDLE_TIMEOUT: NonZeroU64 = NonZeroU64::new(60_000).expect("nonzero");

/// The `[sessions]` table: how SQL sessions, the router's Hrana streams on a primary, may hold a
/// database's writer (ADR-0037, point 14). New in 0.8.0.
///
/// ```toml
/// [sessions]
/// transaction_timeout_ms = 30000      # a session transaction open longer is rolled back
/// max_waiting_writes = 64             # writes queued behind an open transaction, per database
/// max_sessions_per_principal = 256    # sessions one principal may hold open on this node
/// idle_timeout_ms = 60000             # a session with no call for this long is ended
/// ```
///
/// While a session's transaction is open, other writes to its database wait, and reads go on. A
/// write past `max_waiting_writes` is refused with `RESOURCE_EXHAUSTED`, and never runs; one
/// principal's sessions may take at most half the places. A transaction open longer than
/// `transaction_timeout_ms` is rolled back, and its session's next call fails, so a client that
/// walks away can't hold a database. A write's statement time, `[limits] statement_timeout_ms`,
/// counts from when it starts, after any wait behind a transaction, and it may wait for up to
/// `transaction_timeout_ms` besides. A principal past `max_sessions_per_principal` open sessions
/// gets `RESOURCE_EXHAUSTED`; a forwarded token's sessions count as its own principal's.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct SessionsConfig {
    /// The longest a session's transaction may stay open, in milliseconds. At least 1.
    pub transaction_timeout_ms: NonZeroU64,
    /// How many writes may wait for a session's transaction, per database. At least 1.
    pub max_waiting_writes: NonZeroUsize,
    /// How many sessions one principal may hold open on this node. At least 1.
    pub max_sessions_per_principal: NonZeroUsize,
    /// How long a session may go without a call before the node ends it, in milliseconds.
    pub idle_timeout_ms: NonZeroU64,
}

impl SessionsConfig {
    /// The limits the registry holds sessions to.
    #[must_use]
    pub fn limits(&self) -> SessionLimits {
        SessionLimits {
            transaction_timeout: Duration::from_millis(self.transaction_timeout_ms.get()),
            max_waiting_writes: self.max_waiting_writes.get(),
            max_sessions_per_principal: self.max_sessions_per_principal.get(),
            idle_timeout: Duration::from_millis(self.idle_timeout_ms.get()),
        }
    }
}

impl Default for SessionsConfig {
    fn default() -> Self {
        Self {
            transaction_timeout_ms: DEFAULT_TRANSACTION_TIMEOUT,
            max_waiting_writes: DEFAULT_MAX_WAITING_WRITES,
            max_sessions_per_principal: DEFAULT_MAX_SESSIONS_PER_PRINCIPAL,
            idle_timeout_ms: DEFAULT_SESSION_IDLE_TIMEOUT,
        }
    }
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
/// Each scope is `<action>:<pattern>` with the action `read`, `write`, `fork`, `admin`, or
/// `stream`, and the pattern an exact database name or a prefix ending in `*`; or the word
/// `fleet`, for control operations that name no database. `write` and `fork` each imply `read`,
/// and `admin` implies all three. `grant` is refused here: only an agent's token delegates.
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

/// The `[replication]` table: how a data node paces and bounds replication, and the replication
/// factor and durability the control node gives databases created without explicit settings
/// (ADR-0023, ADR-0039).
///
/// Every field is optional; the pacing defaults are those of [`ReplicationSettings::new`], the
/// replication factor is 3, and the durability defaults are `quorum` with `min_replicas` at the
/// write quorum W.
///
/// ```toml
/// [replication]
/// replication_factor = 3             # replicas, the primary included; the default for new databases
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
    /// The replication factor a database is created with unless its creation names one: 3 by
    /// default, and 1 in the configuration `fleetringd dev` writes. The control node resolves it
    /// before it logs the creation, so the log always holds an explicit factor. At least 1.
    pub replication_factor: ReplicationFactor,
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
            replication_factor: ReplicationFactor::default(),
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
            if principal.name.is_reserved_agent_name() {
                return Err(ConfigError::Invalid {
                    field: "principal",
                    reason: reserved_reason(&principal.name),
                });
            }
            // Only an agent's token delegates (ADR-0044, point 2; ADR-0047), so `grant` in a
            // certificate's scopes would mean nothing; refusing it keeps anyone from thinking
            // it does.
            if principal.scopes.names(Action::Grant) {
                return Err(ConfigError::Invalid {
                    field: "principal",
                    reason: format!("'{}': {GRANT_IN_PRINCIPAL}", principal.name),
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
        self.fork.validate()?;
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
            if scraper.is_reserved_agent_name() {
                return Err(ConfigError::Invalid {
                    field: "admin.scrapers",
                    reason: reserved_reason(scraper),
                });
            }
        }
        Ok(())
    }
}

/// Why a `[[principal]]` entry may not hold a `grant` scope (ADR-0044, point 2).
const GRANT_IN_PRINCIPAL: &str =
    "grant has no effect in a certificate's scopes; it is carried by an agent's token";

/// Why a configuration may not name `name`: a reserved dotted agent name (ADR-0044, point 1.8).
fn reserved_reason(name: &Principal) -> String {
    format!(
        "'{name}' is reserved for delegated agents ('.' after agent- marks a lineage), so no \
         certificate or token for it is accepted; rename the entry and reissue its certificate"
    )
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
    fn reserved_dotted_agent_names_are_refused_at_load() {
        let refused = with("\n[[principal]]\nname = \"agent-42.c7\"\nscopes = [\"read:*\"]\n")
            .expect_err("a reserved name");
        let ConfigError::Invalid { field, reason } = &refused else {
            panic!("{refused}");
        };
        assert_eq!(*field, "principal");
        assert!(reason.contains("'agent-42.c7'"), "{reason}");
        assert!(reason.contains("rename"), "{reason}");
        let scraper = with("\n[admin]\nscrapers = [\"agent-1.x\"]\n").expect_err("a scraper");
        assert!(
            matches!(&scraper, ConfigError::Invalid { field, .. } if *field == "admin.scrapers"),
            "{scraper}"
        );
        for kept in ["workload.prod", "agent-42", "agent_x.y"] {
            let config = with(&format!("\n[[principal]]\nname = \"{kept}\"\n"));
            assert!(config.is_ok(), "{kept}: {config:?}");
        }
    }

    #[test]
    fn grant_in_a_principal_entry_is_refused_and_fork_is_kept() {
        for scopes in [
            r#"["grant:agent-42.*"]"#,
            r#"["read:*", "grant:*"]"#,
            r#"["admin:x-*", "grant:x-*"]"#,
        ] {
            let refused = with(&format!(
                "\n[[principal]]\nname = \"ops\"\nscopes = {scopes}\n"
            ))
            .expect_err("grant in a certificate's scopes");
            let ConfigError::Invalid { field, reason } = &refused else {
                panic!("{scopes}: {refused}");
            };
            assert_eq!(*field, "principal");
            assert_eq!(
                reason,
                "'ops': grant has no effect in a certificate's scopes; it is carried by an \
                 agent's token"
            );
        }
        let kept = with("\n[[principal]]\nname = \"forker\"\nscopes = [\"fork:src-*\"]\n")
            .expect("fork in a certificate's scopes");
        assert_eq!(
            kept.principals[0].scopes.as_slice()[0].to_string(),
            "fork:src-*"
        );
        // `grant` in a pattern is just a name.
        assert!(with("\n[[principal]]\nname = \"ops\"\nscopes = [\"read:grant-*\"]\n").is_ok());
    }

    #[test]
    fn the_default_replication_factor_is_three_and_at_least_one() {
        let defaults = with("").expect("a minimal config");
        assert_eq!(defaults.replication.replication_factor.get(), 3);
        let one = with("\n[replication]\nreplication_factor = 1\n").expect("a factor of 1");
        assert_eq!(one.replication.replication_factor, ReplicationFactor::ONE);
        let zero = with("\n[replication]\nreplication_factor = 0\n").expect_err("a factor of 0");
        let message = zero.to_string();
        assert!(matches!(zero, ConfigError::Parse(_)), "{message}");
        assert!(message.contains("replication_factor"), "{message}");
        assert!(message.contains("nonzero replication factor"), "{message}");
        let large = with("\n[replication]\nreplication_factor = 256\n");
        assert!(large.is_err(), "a factor above 255: {large:?}");
    }

    #[test]
    fn sessions_default_to_thirty_seconds_and_sixty_four_waiting_writes() {
        let defaults = with("").expect("defaults");
        assert_eq!(
            defaults.sessions.limits(),
            SessionLimits {
                transaction_timeout: Duration::from_secs(30),
                max_waiting_writes: 64,
                max_sessions_per_principal: 256,
                idle_timeout: Duration::from_secs(60),
            }
        );
        let set = with("\n[sessions]\ntransaction_timeout_ms = 500\nmax_waiting_writes = 2\n")
            .expect("set limits");
        assert_eq!(
            set.sessions.limits(),
            SessionLimits {
                transaction_timeout: Duration::from_millis(500),
                max_waiting_writes: 2,
                ..SessionLimits::default()
            }
        );
        let zero = with("\n[sessions]\nmax_waiting_writes = 0\n").expect_err("zero writes");
        assert!(zero.to_string().contains("max_waiting_writes"), "{zero}");
    }

    #[test]
    fn the_statement_timeout_defaults_to_thirty_seconds_and_is_at_least_one_millisecond() {
        let defaults = with("").expect("a minimal config");
        let empty = with("\n[limits]\n").expect("an empty [limits] table");
        assert_eq!(defaults.limits, empty.limits);
        assert_eq!(defaults.limits.statement_timeout(), Duration::from_secs(30));
        let short = with("\n[limits]\nstatement_timeout_ms = 250\n").expect("a short timeout");
        assert_eq!(short.limits.statement_timeout(), Duration::from_millis(250));
        let zero = with("\n[limits]\nstatement_timeout_ms = 0\n").expect_err("a zero timeout");
        let message = zero.to_string();
        assert!(matches!(zero, ConfigError::Parse(_)), "{message}");
        assert!(message.contains("statement_timeout_ms"), "{message}");
        let unknown = with("\n[limits]\nstatement_timeout = \"30s\"\n");
        assert!(unknown.is_err(), "unknown fields are refused: {unknown:?}");
    }

    #[test]
    fn fork_defaults_are_adr_0046s_and_the_timeouts_nest() {
        let defaults = with("").expect("a minimal config");
        assert_eq!(defaults.fork, ForkConfig::default());
        assert_eq!(
            with("\n[fork]\n").expect("an empty [fork]").fork,
            defaults.fork
        );
        // The control rows travel in every fork command; they match the control crate's own.
        assert_eq!(defaults.fork.limits(), ForkLimits::default());
        assert_eq!(defaults.fork.transfer_retries, 3);
        assert_eq!(defaults.fork.transfer_backoff(), Duration::from_secs(1));
        assert_eq!(defaults.fork.max_pin(), Duration::from_secs(600));
        assert_eq!(defaults.fork.max_pin_wal_bytes.get(), 1 << 30);
        assert_eq!(defaults.fork.max_pin_per_hour(), Duration::from_secs(1_200));
        assert_eq!(defaults.fork.max_pins_per_node.get(), 2);
        assert_eq!(defaults.fork.max_pins_per_database.get(), 1);
        assert_eq!(defaults.fork.max_seeds_per_node.get(), 2);

        let set = with(
            "\n[fork]\nmax_attempts = 2\ndeadline_ms = 9000\nattempt_timeout_ms = 3000\n\
             seeder_timeout_ms = 1000\noperation_retention_ms = 60000\n",
        )
        .expect("set limits");
        assert_eq!(
            set.fork.limits(),
            ForkLimits {
                max_attempts: 2,
                deadline: Millis::new(9_000),
                attempt_timeout: Millis::new(3_000),
                seeder_timeout: Millis::new(1_000),
                operation_retention: Millis::new(60_000),
                admission_min_free_bytes: None,
            }
        );
        let small =
            with("\n[fork]\nadmission_min_free_bytes = 67108864\nadmission_min_free_percent = 0\n")
                .expect("a small margin");
        assert_eq!(small.fork.admission_min_free_bytes, 64 << 20);
        assert_eq!(small.fork.admission_min_free_percent, 0);
        assert_eq!(
            small.fork.limits().admission_min_free_bytes,
            Some(64 << 20),
            "recorded with each fork"
        );
        for (table, field) in [
            ("seeder_timeout_ms = 3600000", "fork.seeder_timeout_ms"),
            ("attempt_timeout_ms = 7200000", "fork.attempt_timeout_ms"),
            ("max_pin_ms = 3600000", "fork.max_pin_ms"),
            ("max_pins_per_database = 3", "fork.max_pins_per_database"),
            (
                "admission_min_free_percent = 101",
                "fork.admission_min_free_percent",
            ),
        ] {
            let refused = with(&format!("\n[fork]\n{table}\n")).expect_err(table);
            let ConfigError::Invalid { field: named, .. } = &refused else {
                panic!("{table}: {refused}");
            };
            assert_eq!(*named, field, "{refused}");
        }
        for zero in [
            "max_attempts = 0",
            "deadline_ms = 0",
            "max_seeds_per_node = 0",
        ] {
            let refused = with(&format!("\n[fork]\n{zero}\n")).expect_err(zero);
            assert!(
                matches!(refused, ConfigError::Parse(_)),
                "{zero}: {refused}"
            );
        }
        let unknown = with("\n[fork]\ndeadline = \"1h\"\n");
        assert!(unknown.is_err(), "unknown fields are refused: {unknown:?}");
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
