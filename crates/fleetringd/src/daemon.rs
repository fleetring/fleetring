//! The daemon's lifecycle: start, serve until told to stop, then shut down cleanly.

use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::time::Duration;

use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tonic::transport::Server;
use tonic::transport::server::TcpIncoming;

use crate::config::reachable;
use crate::control::{ControlHandle, ControlNode, ControlOptions, ControlService, OpenError};
use crate::data::{DataNode, DatabaseService};
use crate::engine::{Execution, InvalidPageSizeError, SqliteEngine};
use crate::environment::{Environment, Network};
use crate::sink::UnreplicatedSink;
use crate::transport::{control_server, database_server};
use crate::{Config, ConfigError, DataDir, DataDirError, Membership, MembershipTasks, Registry};

/// How long shutdown waits for open connections to close before it drops them.
///
/// HTTP/2's graceful shutdown waits for the peer to acknowledge it, so a peer that vanished, such
/// as a machine that lost power, would otherwise hold shutdown until TCP gives up on it.
const SHUTDOWN_GRACE: Duration = Duration::from_secs(5);

/// The control node's database file, inside the data directory.
pub(crate) const CONTROL_DB: &str = "control.db";

/// The directory, inside the data directory, that holds a data node's databases.
pub(crate) const DATABASES_DIR: &str = "databases";

/// A started daemon that holds its data directory.
#[derive(Debug)]
pub struct Daemon {
    config: Config,
    data_dir: DataDir,
    server: RunningServer,
    control: Option<ControlNode>,
    data: Option<DataRole>,
}

/// What the data role runs: membership, and the databases this node hosts.
#[derive(Debug)]
struct DataRole {
    membership: Membership,
    tasks: MembershipTasks,
    registry: Registry,
}

/// The replication sink every database on this node publishes its committed batches to.
///
/// **Replication plugs in here (FR-030).** No follower stores anything yet, so the sink
/// acknowledges every batch at once, and a write's reply counts the primary alone (see
/// `data::service::ACKNOWLEDGED_BY`). Replication replaces [`UnreplicatedSink`] with a sink that
/// ships each batch to the database's followers and completes its acknowledgement once the write
/// quorum holds it, and makes the reply count the replicas that acknowledged.
fn replication_sink() -> UnreplicatedSink {
    UnreplicatedSink
}

/// The gRPC listener, bound before any service starts so its address is known.
enum Listener {
    Tcp(tokio::net::TcpListener),
    #[cfg(feature = "simulation")]
    Simulated(turmoil::net::TcpListener),
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
    /// The `[engine]` configuration is invalid.
    #[error("invalid engine configuration")]
    Engine(#[from] InvalidPageSizeError),
}

impl Daemon {
    /// Validates the configuration, locks the data directory, and starts the roles it runs.
    ///
    /// Every role serves gRPC on `listen`. A role that runs control recovers the control node
    /// from the data directory and serves the `Control` service there. A role that hosts data
    /// keeps its databases under `databases/` in the data directory, serves the `Database`
    /// service (see [`data`](crate::data)), joins the cluster with its advertised address, sends
    /// heartbeats, and publishes the ring (see [`Membership`]). A node that runs both roles
    /// reaches its own control node through `listen`.
    ///
    /// It runs in [`Environment::production`]; see [`Daemon::start_with`].
    ///
    /// # Errors
    ///
    /// [`StartError`] if the configuration is invalid, another daemon holds the directory, the
    /// control log can't be recovered, `listen` can't be bound, or `[engine] page_size` isn't a
    /// valid page size.
    pub async fn start(config: Config) -> Result<Self, StartError> {
        Self::start_with(config, Environment::production()).await
    }

    /// Starts the daemon as [`Daemon::start`] does, in `environment`: its listener and peer
    /// connections use the environment's network, the control node stamps commands with its
    /// clock, and blocking storage calls run as its execution says.
    ///
    /// With [`Execution::Inline`], the returned [`Daemon::config`] has `[engine] execution` set to
    /// inline as well.
    ///
    /// # Errors
    ///
    /// [`StartError`] if the configuration is invalid, another daemon holds the directory, the
    /// control log can't be recovered, `listen` can't be bound, or `[engine] page_size` isn't a
    /// valid page size.
    pub async fn start_with(
        mut config: Config,
        environment: Environment,
    ) -> Result<Self, StartError> {
        config.validate()?;
        if environment.execution() == Execution::Inline {
            config.engine.execution = Execution::Inline;
        }
        let data_dir = DataDir::lock(&config.data_dir)?;
        let control = if config.role.runs_control() {
            Some(open_control(&config, &data_dir, &environment).await?)
        } else {
            None
        };
        let network = environment.network();
        let (listener, address) = bind(&config, network).await?;
        let data = if config.role.hosts_data() {
            Some(start_data(&config, &data_dir, network, address)?)
        } else {
            None
        };
        let server = serve(
            listener,
            address,
            control.as_ref(),
            data.as_ref().map(|(_, node)| node),
        );
        let data = data.map(|(role, _)| role);
        tracing::info!(
            node = %config.node.id,
            zone = ?config.node.zone.as_ref().map(|zone| zone.as_str()),
            host = ?config.node.host.as_ref().map(|host| host.as_str()),
            weight = config.node.weight.get(),
            role = ?config.role,
            listen = %server.address,
            advertise = %advertised(&config, server.address),
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
            data,
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
        self.data.as_ref().map(|data| data.membership.clone())
    }

    /// The databases this node hosts, if this daemon hosts data.
    #[must_use]
    pub fn registry(&self) -> Option<Registry> {
        self.data.as_ref().map(|data| data.registry.clone())
    }

    /// Serves until `shutdown` completes, then stops taking work, finishes in-flight writes, closes
    /// its databases, and releases the data directory.
    ///
    /// Connections get a few seconds to close; any still open after that are dropped.
    ///
    /// The node stays a member; the control plane marks it down once its heartbeats stop.
    pub async fn run_until(self, shutdown: impl Future<Output = ()>) {
        shutdown.await;
        tracing::info!(node = %self.config.node.id, "fleetringd shutting down");
        let registry = match self.data {
            Some(data) => {
                data.tasks.stop().await;
                Some(data.registry)
            }
            None => None,
        };
        // Stop accepting calls, then stop the control writer, which also ends membership streams
        // so the server can finish.
        let _server_gone = self.server.stop.send(());
        if let Some(control) = self.control {
            control.shutdown().await;
        }
        let mut task = self.server.task;
        match tokio::time::timeout(SHUTDOWN_GRACE, &mut task).await {
            Ok(Ok(Ok(()))) => {}
            Ok(Ok(Err(error))) => tracing::error!(%error, "gRPC server failed"),
            Ok(Err(error)) => tracing::error!(%error, "gRPC server task failed"),
            Err(_) => {
                tracing::warn!(
                    grace = ?SHUTDOWN_GRACE,
                    "connections didn't close in time; dropping them",
                );
                task.abort();
                // The task was aborted, so it can only end cancelled.
                let _aborted = task.await;
            }
        }
        // No call is running any more: finish the writes databases accepted, then close them.
        if let Some(registry) = registry {
            registry.close_all().await;
        }
        drop(self.data_dir);
        tracing::info!("fleetringd stopped");
    }
}

async fn open_control(
    config: &Config,
    data_dir: &DataDir,
    environment: &Environment,
) -> Result<ControlNode, StartError> {
    let options = ControlOptions::new(config.control.control_config())
        .with_snapshot_every(config.control.snapshot_every)
        .with_storage_io(environment.control_storage_io());
    let path = data_dir.path().join(CONTROL_DB);
    Ok(ControlNode::open(path, options, environment.clock()).await?)
}

/// The data role's address for peers: `[node] advertise`, or else the address it serves on.
fn advertised(config: &Config, bound: SocketAddr) -> SocketAddr {
    config.node.advertise.unwrap_or_else(|| reachable(bound))
}

/// Starts the data role: the registry under `databases/`, membership with the advertised
/// address, and the node behind the `Database` service.
fn start_data(
    config: &Config,
    data_dir: &DataDir,
    network: Network,
    bound: SocketAddr,
) -> Result<(DataRole, DataNode), StartError> {
    let engine = SqliteEngine::new(config.engine.page_size)?;
    let registry = Registry::new(
        engine,
        replication_sink(),
        data_dir.path().join(DATABASES_DIR),
        config.engine.clone(),
    );
    let control = network.control_client(
        config
            .control
            .address
            .unwrap_or_else(|| reachable(bound))
            .to_string(),
    );
    let member = config
        .node
        .to_member()
        .with_address(advertised(config, bound).to_string());
    let (membership, tasks) = Membership::start(member, control.clone(), &config.membership);
    let node = DataNode::new(config.node.id.clone(), registry.clone(), control);
    let role = DataRole {
        membership,
        tasks,
        registry,
    };
    Ok((role, node))
}

/// Binds `listen` on `network`, and returns the listener with the address it bound.
async fn bind(config: &Config, network: Network) -> Result<(Listener, SocketAddr), StartError> {
    let listen_error = |source| StartError::Listen {
        address: config.listen,
        source,
    };
    match network {
        Network::Tcp(_) => {
            let listener = tokio::net::TcpListener::bind(config.listen)
                .await
                .map_err(listen_error)?;
            let address = listener.local_addr().map_err(listen_error)?;
            Ok((Listener::Tcp(listener), address))
        }
        #[cfg(feature = "simulation")]
        Network::Simulated => {
            let listener = turmoil::net::TcpListener::bind(config.listen)
                .await
                .map_err(listen_error)?;
            let address = listener.local_addr().map_err(listen_error)?;
            Ok((Listener::Simulated(listener), address))
        }
    }
}

/// Serves on `listener`: the `Control` service if `control` runs here, and the `Database`
/// service if `data` does. The data role's `Replication` service will join them.
fn serve(
    listener: Listener,
    address: SocketAddr,
    control: Option<&ControlNode>,
    data: Option<&DataNode>,
) -> RunningServer {
    let (stop, stopped) = oneshot::channel::<()>();
    // A dropped sender also means stop.
    let shutdown = async {
        let _stop = stopped.await;
    };
    let router = Server::builder()
        .add_optional_service(
            control.map(|node| control_server(ControlService::new(node.handle()))),
        )
        .add_optional_service(data.map(|node| database_server(DatabaseService::new(node.clone()))));
    let task = match listener {
        Listener::Tcp(listener) => {
            tokio::spawn(router.serve_with_incoming_shutdown(TcpIncoming::from(listener), shutdown))
        }
        #[cfg(feature = "simulation")]
        Listener::Simulated(listener) => {
            let incoming = crate::transport::sim::incoming(listener);
            tokio::spawn(router.serve_with_incoming_shutdown(incoming, shutdown))
        }
    };
    RunningServer {
        address,
        stop,
        task,
    }
}
