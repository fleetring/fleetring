//! The daemon's lifecycle: start, serve until told to stop, then shut down cleanly.

use std::future::Future;
use std::io;
use std::net::SocketAddr;

use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tonic::transport::Server;
use tonic::transport::server::TcpIncoming;

use crate::clock::SystemClock;
use crate::control::{ControlHandle, ControlNode, ControlOptions, ControlService, OpenError};
use crate::transport::control_server;
use crate::{Config, ConfigError, DataDir, DataDirError};

/// The control node's database file, inside the data directory.
const CONTROL_DB: &str = "control.db";

/// A started daemon that holds its data directory.
#[derive(Debug)]
pub struct Daemon {
    config: Config,
    data_dir: DataDir,
    control: Option<RunningControl>,
}

/// The control node and the server in front of it.
#[derive(Debug)]
struct RunningControl {
    node: ControlNode,
    address: SocketAddr,
    stop: oneshot::Sender<()>,
    server: JoinHandle<Result<(), tonic::transport::Error>>,
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
    /// The control node can't recover its state.
    #[error(transparent)]
    Control(#[from] OpenError),
    /// The listen address can't be bound.
    #[error("can't listen on {address}")]
    Listen {
        /// The address.
        address: SocketAddr,
        /// The underlying error.
        source: io::Error,
    },
}

impl Daemon {
    /// Validates the configuration, locks the data directory, and starts the roles it runs.
    ///
    /// A role that runs control recovers the control node from the data directory and serves
    /// the `Control` service on `listen`.
    ///
    /// # Errors
    ///
    /// [`StartError`] if the configuration is invalid, another daemon holds the directory, the
    /// control log can't be recovered, or `listen` can't be bound.
    pub async fn start(config: Config) -> Result<Self, StartError> {
        config.validate()?;
        let data_dir = DataDir::lock(&config.data_dir)?;
        let control = if config.role.runs_control() {
            Some(start_control(&config, &data_dir).await?)
        } else {
            None
        };
        tracing::info!(
            node = %config.node.id,
            zone = ?config.node.zone.as_ref().map(|zone| zone.as_str()),
            host = ?config.node.host.as_ref().map(|host| host.as_str()),
            weight = config.node.weight.get(),
            role = ?config.role,
            listen = %control.as_ref().map_or(config.listen, |control| control.address),
            control = ?config.control.address,
            data_dir = %data_dir.path().display(),
            "fleetringd started",
        );
        Ok(Self {
            config,
            data_dir,
            control,
        })
    }

    /// The configuration the daemon started with.
    #[must_use]
    pub fn config(&self) -> &Config {
        &self.config
    }

    /// The address the `Control` service listens on, if this daemon runs control.
    ///
    /// It differs from the configured `listen` when that names port 0.
    #[must_use]
    pub fn control_address(&self) -> Option<SocketAddr> {
        self.control.as_ref().map(|control| control.address)
    }

    /// A handle to the control node, if this daemon runs control.
    #[must_use]
    pub fn control(&self) -> Option<ControlHandle> {
        self.control.as_ref().map(|control| control.node.handle())
    }

    /// Serves until `shutdown` completes, then stops taking work, finishes in-flight writes, closes
    /// its databases, and releases the data directory.
    pub async fn run_until(self, shutdown: impl Future<Output = ()>) {
        shutdown.await;
        tracing::info!(node = %self.config.node.id, "fleetringd shutting down");
        if let Some(control) = self.control {
            // Stop accepting calls, then stop the writer, which also ends membership streams so
            // the server can finish.
            let _server_gone = control.stop.send(());
            control.node.shutdown().await;
            match control.server.await {
                Ok(Ok(())) => {}
                Ok(Err(error)) => tracing::error!(%error, "control service failed"),
                Err(error) => tracing::error!(%error, "control service task failed"),
            }
        }
        drop(self.data_dir);
        tracing::info!("fleetringd stopped");
    }
}

async fn start_control(config: &Config, data_dir: &DataDir) -> Result<RunningControl, StartError> {
    let options = ControlOptions::new(config.control.control_config())
        .with_snapshot_every(config.control.snapshot_every);
    let node = ControlNode::open(data_dir.path().join(CONTROL_DB), options, SystemClock).await?;
    let listen_error = |source| StartError::Listen {
        address: config.listen,
        source,
    };
    let listener = tokio::net::TcpListener::bind(config.listen)
        .await
        .map_err(listen_error)?;
    let address = listener.local_addr().map_err(listen_error)?;
    let (stop, stopped) = oneshot::channel::<()>();
    let server = Server::builder()
        .add_service(control_server(ControlService::new(node.handle())))
        .serve_with_incoming_shutdown(TcpIncoming::from(listener), async {
            // A dropped sender also means stop.
            let _stop = stopped.await;
        });
    Ok(RunningControl {
        node,
        address,
        stop,
        server: tokio::spawn(server),
    })
}
