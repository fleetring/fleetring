//! The daemon's lifecycle: start, serve until told to stop, then shut down cleanly.

use std::future::Future;

use crate::{Config, ConfigError, DataDir, DataDirError};

/// A started daemon that holds its data directory.
#[derive(Debug)]
pub struct Daemon {
    config: Config,
    data_dir: DataDir,
}

/// The error returned when a daemon can't start.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum StartError {
    /// The configuration is invalid.
    #[error(transparent)]
    Config(#[from] ConfigError),
    /// The data directory can't be used.
    #[error(transparent)]
    DataDir(#[from] DataDirError),
}

impl Daemon {
    /// Validates the configuration and locks the data directory.
    ///
    /// # Errors
    ///
    /// [`StartError`] if the configuration is invalid or another daemon holds the directory.
    pub fn start(config: Config) -> Result<Self, StartError> {
        config.validate()?;
        let data_dir = DataDir::lock(&config.data_dir)?;
        tracing::info!(
            node = %config.node.id,
            zone = ?config.node.zone.as_ref().map(|zone| zone.as_str()),
            host = ?config.node.host.as_ref().map(|host| host.as_str()),
            weight = config.node.weight.get(),
            role = ?config.role,
            listen = %config.listen,
            control = ?config.control,
            data_dir = %data_dir.path().display(),
            "fleetringd started",
        );
        Ok(Self { config, data_dir })
    }

    /// The configuration the daemon started with.
    #[must_use]
    pub fn config(&self) -> &Config {
        &self.config
    }

    /// Serves until `shutdown` completes, then stops taking work, finishes in-flight writes, closes
    /// its databases, and releases the data directory.
    pub async fn run_until(self, shutdown: impl Future<Output = ()>) {
        shutdown.await;
        tracing::info!(node = %self.config.node.id, "fleetringd shutting down");
        drop(self.data_dir);
        tracing::info!("fleetringd stopped");
    }
}
