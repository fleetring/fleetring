//! The daemon's lifecycle: start, serve until told to stop, then shut down cleanly.

use std::future::Future;
use std::io;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr};

use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tonic::transport::Server;
use tonic::transport::server::TcpIncoming;

use crate::clock::SystemClock;
use crate::control::{
    ControlClient, ControlHandle, ControlNode, ControlOptions, ControlService, OpenError,
};
use crate::transport::{TransportConfig, control_server};
use crate::{Config, ConfigError, DataDir, DataDirError, Membership, MembershipTasks};

/// The control node's database file, inside the data directory.
const CONTROL_DB: &str = "control.db";

/// A started daemon that holds its data directory.
#[derive(Debug)]
pub struct Daemon {
    config: Config,
    data_dir: DataDir,
    server: RunningServer,
    control: Option<ControlNode>,
    membership: Option<(Membership, MembershipTasks)>,
}

/// The gRPC server on `listen`, which every role runs.
#[derive(Debug)]
struct RunningServer {
    address: SocketAddr,
    stop: oneshot::Sender<()>,
    task: JoinHandle<Result<(), tonic::transport::Error>>,
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
    /// Every role serves gRPC on `listen`. A role that runs control recovers the control node
    /// from the data directory and serves the `Control` service there. A role that hosts data
    /// joins the cluster, sends heartbeats, and publishes the ring (see [`Membership`]); a
    /// data-only node serves no services yet, so peers can already reach it. A node that runs
    /// both roles reaches its own control node through `listen`.
    ///
    /// # Errors
    ///
    /// [`StartError`] if the configuration is invalid, another daemon holds the directory, the
    /// control log can't be recovered, or `listen` can't be bound.
    pub async fn start(config: Config) -> Result<Self, StartError> {
        config.validate()?;
        let data_dir = DataDir::lock(&config.data_dir)?;
        let control = if config.role.runs_control() {
            Some(open_control(&config, &data_dir).await?)
        } else {
            None
        };
        let server = serve(&config, control.as_ref()).await?;
        let membership = config.role.hosts_data().then(|| {
            let address = config
                .control
                .address
                .unwrap_or_else(|| reachable(server.address));
            let client = ControlClient::new(address.to_string(), TransportConfig::default());
            Membership::start(config.node.to_member(), client, &config.membership)
        });
        tracing::info!(
            node = %config.node.id,
            zone = ?config.node.zone.as_ref().map(|zone| zone.as_str()),
            host = ?config.node.host.as_ref().map(|host| host.as_str()),
            weight = config.node.weight.get(),
            role = ?config.role,
            listen = %server.address,
            control = ?config.control.address,
            heartbeat_interval_ms = config.membership.heartbeat_interval_ms.get(),
            data_dir = %data_dir.path().display(),
            "fleetringd started",
        );
        Ok(Self {
            config,
            data_dir,
            server,
            control,
            membership,
        })
    }

    /// The configuration the daemon started with.
    #[must_use]
    pub fn config(&self) -> &Config {
        &self.config
    }

    /// The address the daemon serves on.
    ///
    /// It differs from the configured `listen` when that names port 0.
    #[must_use]
    pub fn listen_address(&self) -> SocketAddr {
        self.server.address
    }

    /// The address the `Control` service listens on, if this daemon runs control.
    ///
    /// It differs from the configured `listen` when that names port 0.
    #[must_use]
    pub fn control_address(&self) -> Option<SocketAddr> {
        self.control.as_ref().map(|_| self.server.address)
    }

    /// A handle to the control node, if this daemon runs control.
    #[must_use]
    pub fn control(&self) -> Option<ControlHandle> {
        self.control.as_ref().map(ControlNode::handle)
    }

    /// The node's membership and ring, if this daemon hosts data.
    #[must_use]
    pub fn membership(&self) -> Option<Membership> {
        self.membership
            .as_ref()
            .map(|(membership, _)| membership.clone())
    }

    /// Serves until `shutdown` completes, then stops taking work, finishes in-flight writes, closes
    /// its databases, and releases the data directory.
    ///
    /// The node stays a member; the control plane marks it down once its heartbeats stop.
    pub async fn run_until(self, shutdown: impl Future<Output = ()>) {
        shutdown.await;
        tracing::info!(node = %self.config.node.id, "fleetringd shutting down");
        if let Some((_, tasks)) = self.membership {
            tasks.stop().await;
        }
        // Stop accepting calls, then stop the control writer, which also ends membership streams
        // so the server can finish.
        let _server_gone = self.server.stop.send(());
        if let Some(control) = self.control {
            control.shutdown().await;
        }
        match self.server.task.await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => tracing::error!(%error, "gRPC server failed"),
            Err(error) => tracing::error!(%error, "gRPC server task failed"),
        }
        drop(self.data_dir);
        tracing::info!("fleetringd stopped");
    }
}

async fn open_control(config: &Config, data_dir: &DataDir) -> Result<ControlNode, StartError> {
    let options = ControlOptions::new(config.control.control_config())
        .with_snapshot_every(config.control.snapshot_every);
    Ok(ControlNode::open(data_dir.path().join(CONTROL_DB), options, SystemClock).await?)
}

/// Binds `listen` and serves the `Control` service if `control` runs here. The data role's
/// `Replication` and `Database` services will join it.
async fn serve(
    config: &Config,
    control: Option<&ControlNode>,
) -> Result<RunningServer, StartError> {
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
        .add_optional_service(
            control.map(|node| control_server(ControlService::new(node.handle()))),
        )
        .serve_with_incoming_shutdown(TcpIncoming::from(listener), async {
            // A dropped sender also means stop.
            let _stop = stopped.await;
        });
    Ok(RunningServer {
        address,
        stop,
        task: tokio::spawn(server),
    })
}

/// The address a client on this machine dials to reach a server bound to `address`: the loopback
/// address in place of an unspecified one, which not every platform accepts as a destination.
fn reachable(address: SocketAddr) -> SocketAddr {
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
