//! The FleetRing daemon (ADR-0015).
//!
//! `fleetringd` keeps each database on a primary with followers, on machines you own. The
//! `fleetring` crate decides where each database should live; the daemon makes it so. One binary
//! runs as a data node, as the control node, or as both.
//!
//! The library exposes the daemon's building blocks so that tests, including the simulation
//! harness (ADR-0020), run the same code as the binary.

mod config;
mod daemon;
mod data_dir;

pub use config::{Config, ConfigError, NodeConfig, Role};
pub use daemon::{Daemon, StartError};
pub use data_dir::{DataDir, DataDirError};
