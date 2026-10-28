//! Daemon configuration: a TOML file, optionally overridden by command-line flags.

use std::fs;
use std::io;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use fleetring::{Host, NodeId, Weight, Zone};
use serde::Deserialize;

/// Everything a daemon needs to start.
///
/// ```toml
/// data_dir = "/var/lib/fleetring"
/// listen = "0.0.0.0:7400"
/// control = "10.0.0.1:7400"
/// role = "data"
///
/// [node]
/// id = "laptop-1"
/// zone = "home"
/// weight = 2
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// This node's identity and placement attributes.
    pub node: NodeConfig,
    /// Where the node keeps its databases. One daemon locks it exclusively.
    pub data_dir: PathBuf,
    /// The address this node serves on.
    pub listen: SocketAddr,
    /// The control node's address. Required unless this node runs the control role itself.
    #[serde(default)]
    pub control: Option<SocketAddr>,
    /// Which roles this daemon runs.
    #[serde(default)]
    pub role: Role,
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
    /// [`ConfigError::Missing`] if a field this role needs is absent.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.role == Role::Data && self.control.is_none() {
            return Err(ConfigError::Missing {
                field: "control",
                reason: "a data-only node must know the control node's address",
            });
        }
        Ok(())
    }
}
