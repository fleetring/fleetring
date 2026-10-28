//! Daemon configuration: a TOML file, optionally overridden by command-line flags.

use std::fs;
use std::io;
use std::net::SocketAddr;
use std::num::NonZeroU64;
use std::path::{Path, PathBuf};
use std::time::Duration;

use fleetring::{Host, NodeId, Weight, Zone};
use fleetring_control::{ControlConfig, Member, Millis};
use serde::Deserialize;

use crate::engine::EngineConfig;

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
///
/// [control]
/// address = "10.0.0.1:7400"
/// ```
///
/// The `[control]` table also holds the control node's timings; see [`ControlSettings`]. The
/// `[membership]` table sets how often a data node sends heartbeats; see [`MembershipSettings`].
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
}

fn default_weight() -> Weight {
    Weight::ONE
}

impl NodeConfig {
    /// Returns the member this node joins the cluster as.
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

/// The `[membership]` table: how a data node reports to the control plane.
///
/// ```toml
/// [membership]
/// heartbeat_interval_ms = 1000
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
        }
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
/// failover_margin_ms = 2000
/// heartbeat_timeout_ms = 5000
/// snapshot_every = 1000       # commands between snapshots
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ControlSettings {
    /// The control node's address. Required unless this node runs the control role itself.
    pub address: Option<SocketAddr>,
    /// How long a lease lasts after it is granted or renewed, in milliseconds.
    pub lease_duration_ms: NonZeroU64,
    /// How long past a lease's expiry the control node waits before failing over, in
    /// milliseconds.
    pub failover_margin_ms: u64,
    /// How long a member stays live after its last heartbeat, in milliseconds.
    pub heartbeat_timeout_ms: NonZeroU64,
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
}

impl Default for ControlSettings {
    fn default() -> Self {
        let config = ControlConfig::new();
        let nonzero = |millis: Millis| NonZeroU64::new(millis.get()).unwrap_or(NonZeroU64::MIN);
        Self {
            address: None,
            lease_duration_ms: nonzero(config.lease_duration()),
            failover_margin_ms: config.failover_margin().get(),
            heartbeat_timeout_ms: nonzero(config.heartbeat_timeout()),
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
    /// [`ConfigError::Invalid`] if a data node's heartbeat interval isn't well below the heartbeat
    /// timeout.
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
        Ok(())
    }
}
