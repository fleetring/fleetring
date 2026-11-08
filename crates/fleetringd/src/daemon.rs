//! The daemon's lifecycle: start, serve until told to stop, then shut down cleanly.

use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use fleetring_proto::v1::DatabaseStatus;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tonic::transport::Server;

use crate::auth::{AuthLayer, Authorizer};
use crate::changes::{BuildSinkError, ChangeParts, ChangeSettings, ChangeStreams, build_sink};
use crate::config::{HEARTBEATS_PER_TIMEOUT, reachable};
use crate::control::{
    ControlClient, ControlHandle, ControlNode, ControlOptions, ControlService, Failover,
    FailoverSettings, NoControl, OpenError,
};
use crate::data::{DataNode, DatabaseService, DrainSettings, LeaseKeeper, LeaseSettings};
use crate::dev::DevMode;
use crate::durability::{Archive, ArchiveError, ArchiveParts, ControlMetadata, build_store};
use crate::engine::{DatabaseEngine, Execution, InvalidPageSizeError, SqliteEngine};
use crate::environment::{Environment, Network};
use crate::observe::admin::{self, AdminParts, AdminTlsParts, RunningAdmin};
use crate::observe::health::Probes;
use crate::observe::sampler::{Sampler, Sources};
use crate::observe::{Metrics, NodeInfo, TraceLayer};
use crate::replication::{Replication, ReplicationParts};
use crate::tls::{Identity, IdentityError, Security};
use crate::transport::{Listener, Peers, control_server, database_server, incoming};
use crate::{
    AdminListener, AdminPlaintext, AdminTls, Config, ConfigError, DataDir, DataDirError,
    DatabaseStatuses, Membership, MembershipTasks, OnShutdown, Plaintext, Registry,
};

/// How long shutdown waits for open connections to close before it drops them.
///
/// HTTP/2's graceful shutdown waits for the peer to acknowledge it, so a peer that vanished, such
/// as a machine that lost power, would otherwise hold shutdown until TCP gives up on it.
const SHUTDOWN_GRACE: Duration = Duration::from_secs(5);

/// The control node's database file, inside the data directory.
pub(crate) const CONTROL_DB: &str = "control.db";

/// The directory, inside the data directory, that holds a data node's databases.
pub(crate) const DATABASES_DIR: &str = "databases";

/// The directory, inside the data directory, where replication keeps its own durable state,
/// such as each database's highest epoch.
pub(crate) const REPLICATION_DIR: &str = "replication";

/// The directory, inside the data directory, where change streams keep each sink's positions.
pub(crate) const CHANGES_DIR: &str = "changes";

/// How a daemon stops: for good, or to restart in place.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Stop {
    /// Drains and leaves, or stays, as `[membership] on_shutdown` says.
    AsConfigured,
    /// Drains as `on_shutdown` says, but stays a member: the process executes its binary again
    /// at once, as after `SIGHUP` (ADR-0038, point 8).
    Restart,
}

/// A started daemon that holds its data directory.
#[derive(Debug)]
pub struct Daemon {
    config: Config,
    data_dir: DataDir,
    server: RunningServer,
    control: Option<ControlRole>,
    data: Option<DataRole>,
    metrics: Metrics,
    admin: Option<RunningAdmin>,
    sampler: Sampler,
}

/// What the control role runs: the control node, and its failover timer.
#[derive(Debug)]
struct ControlRole {
    node: ControlNode,
    failover: Failover,
}

/// What the data role runs: membership, the databases this node hosts and replicates, and the
/// task that keeps their leases.
#[derive(Debug)]
struct DataRole {
    membership: Membership,
    tasks: MembershipTasks,
    replication: Replication,
    changes: ChangeStreams,
    node: DataNode,
    keeper: LeaseKeeper,
    drain: DrainSettings,
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
    /// A `[[changes.sink]]` entry can't be built.
    #[error("invalid change sink configuration")]
    Sink(#[from] BuildSinkError),
    /// An `[[archive]]` entry can't be built.
    #[error("invalid archive configuration")]
    Archive(#[from] ArchiveError),
    /// The `[tls]` files can't be loaded.
    #[error("can't load the [tls] identity")]
    Identity(#[from] IdentityError),
}

impl Daemon {
    /// Validates the configuration, locks the data directory, and starts the roles it runs.
    ///
    /// Every role serves gRPC on `listen`. A role that runs control recovers the control node
    /// from the data directory and serves the `Control` service there. A role that hosts data
    /// keeps its databases under `databases/` in the data directory, serves the `Database`
    /// service (see [`data`](crate::data)), joins the fleet with its advertised address, sends
    /// heartbeats, and publishes the ring (see [`Membership`]). It also serves the `Replication`
    /// service: it streams every database it is primary for to that database's followers, keeps
    /// copies of those it follows, and reports both in its heartbeats (see
    /// [`replication`](crate::replication)). A node that runs both roles reaches its own control
    /// node through `listen`.
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

    /// Checks `config` as [`Daemon::start`] does before it reads anything else: the rules
    /// [`Config::validate`] applies, that the node has an identity or may run in plaintext, that
    /// `[admin] tls` has an identity to serve, and that `[engine] page_size` is a page size.
    ///
    /// It reads no file the configuration names (certificates, keys, token or credentials
    /// files), no environment variable, and no data directory, and binds nothing: the node reads
    /// those when it starts. So the answer is the same for whoever asks, which is what
    /// `fleetring serve --check` and an upgrade checking a node's configuration need.
    ///
    /// # Errors
    ///
    /// The [`StartError`] [`Daemon::start`] would fail with for these rules.
    pub fn check(config: &Config) -> Result<(), StartError> {
        check_with(config, &Security::Plaintext)
    }

    /// Starts the daemon as [`Daemon::start`] does, in `environment`: its listener and peer
    /// connections, replication's included, use the environment's network, the control node and
    /// replication read its clock, and blocking storage calls run as its execution says.
    ///
    /// Every connection carries the identity the `[tls]` table names, loaded here and reloaded
    /// when its files change. Without a `[tls]` table, the environment's network must carry an
    /// identity, as the simulation harness's does, or `insecure_plaintext` must be set, in which
    /// case the daemon warns and runs in plaintext.
    ///
    /// With [`Execution::Inline`], the returned [`Daemon::config`] has `[engine] execution` set to
    /// inline as well.
    ///
    /// # Errors
    ///
    /// [`StartError`] if the configuration is invalid, there is no identity and plaintext isn't
    /// allowed, the identity can't be loaded, another daemon holds the directory, the control
    /// log can't be recovered, `listen` or `[admin] listen` can't be bound, `[admin] tls` is set
    /// without an identity, or `[engine] page_size` isn't a valid page size.
    ///
    /// Over TCP it also serves the admin listener (`[admin]`, ADR-0034); under simulation it
    /// doesn't, and [`Daemon::metrics`] reads the same registry.
    pub async fn start_with(
        mut config: Config,
        environment: Environment,
    ) -> Result<Self, StartError> {
        check_with(&config, environment.network().security())?;
        // A prefix without a separator covers more than it seems to (ADR-0041, point 5); the
        // scope still loads, since changing what it covers would break existing principals.
        for principal in &config.principals {
            for warning in principal.scopes.separator_warnings() {
                tracing::warn!(principal = %principal.name, "{warning}");
            }
        }
        let ack_timeout = config.replication.settings().ack_timeout;
        let network = environment.network().clone().outlasting(ack_timeout);
        let environment = environment.with_network(network);
        if environment.execution() == Execution::Inline {
            config.engine.execution = Execution::Inline;
        }
        let security = security(&config, &environment)?;
        let environment = environment
            .clone()
            .with_network(environment.network().clone().with_security(security));
        let fleet = match environment.network().security() {
            Security::Tls(identity) => Some(identity.fleet()),
            Security::Plaintext => None,
        };
        let admin_tls = admin_tls(&config, environment.network().security())?;
        let metrics = Metrics::new(&NodeInfo::new(config.node.id.clone(), config.role, fleet));
        let data_dir = DataDir::lock(&config.data_dir)?;
        let control = if config.role.runs_control() {
            let node = open_control(&config, &data_dir, &environment, &metrics).await?;
            let failover = Failover::start(
                node.handle(),
                environment.clock(),
                environment.network().clone(),
                FailoverSettings::new().with_tick_interval(config.control.tick_interval()),
            );
            Some(ControlRole { node, failover })
        } else {
            None
        };
        let network = environment.network();
        let (listener, address) = bind(&config, network).await?;
        let admin_listener = bind_admin(&config, network).await?;
        let data = if config.role.hosts_data() {
            Some(start_data(
                &config,
                &data_dir,
                &environment,
                address,
                &metrics,
            )?)
        } else {
            None
        };
        let authorizer =
            Authorizer::new(&config.principals, network.security(), environment.clock());
        // The control node's own state is the freshest denylist; a data-only node keeps the
        // copy its heartbeats bring.
        let authorizer = match (&control, &data) {
            (Some(role), _) => authorizer.with_control(role.node.handle()),
            (None, Some((role, _))) => authorizer.with_denylist(
                role.membership.denylist().clone(),
                denylist_max_age(&config),
            ),
            (None, None) => authorizer,
        };
        let server = serve(
            listener,
            address,
            control.as_ref().map(|role| {
                ControlService::new(role.node.handle())
                    .with_default_replication_factor(config.replication.replication_factor)
                    .with_durability_defaults(config.replication.durability_defaults())
                    .with_clock(environment.clock())
                    .with_dev_mode(config.dev)
                    .with_network(network.clone())
            }),
            data.as_ref().map(|(_, node)| node),
            control_address(&config, address),
            Guard {
                security: network.security().clone(),
                authorizer,
            },
            TraceLayer::new(config.node.id.clone(), &metrics),
        );
        let data = data.map(|(role, _)| role);
        let sampler = Sampler::start(
            Sources {
                membership: data.as_ref().map(|role| role.membership.clone()),
                replication: data.as_ref().map(|role| role.replication.clone()),
                changes: data.as_ref().map(|role| role.changes.clone()),
                sinks: data
                    .as_ref()
                    .map(|role| {
                        role.changes
                            .sinks()
                            .iter()
                            .map(|configured| configured.sink.name().to_owned())
                            .collect()
                    })
                    .unwrap_or_default(),
                control: control.as_ref().map(|role| role.node.handle()),
            },
            metrics.sampled().clone(),
            config.admin.sample_interval(),
        );
        let admin = match admin_listener {
            Some(listener) => {
                let admin_address = config.admin.listen;
                let running = admin::start(AdminParts {
                    listener,
                    metrics: metrics.clone(),
                    probes: Probes {
                        node: config.node.id.clone(),
                        membership: data.as_ref().map(|role| role.membership.clone()),
                        denylist_max_age: denylist_max_age(&config),
                        control: control.as_ref().map(|role| role.node.handle()),
                    },
                    tls: admin_tls,
                })
                .map_err(|source| StartError::Listen {
                    address: admin_address,
                    source,
                })?;
                Some(running)
            }
            None => None,
        };
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
            tls = !network.security().is_plaintext(),
            admin = %admin
                .as_ref()
                .map_or_else(|| "off".to_owned(), |admin| admin.address().to_string()),
            "fleetringd started",
        );
        if config.dev == DevMode::On {
            tracing::warn!(
                "dev mode: one node on loopback, with its authority's key beside it; for \
                 development only (ADR-0039)",
            );
        }
        Ok(Self {
            config,
            data_dir,
            server,
            control,
            data,
            metrics,
            admin,
            sampler,
        })
    }

    /// The daemon's metrics: the registry the admin listener serves at `/metrics`.
    #[must_use]
    pub fn metrics(&self) -> &Metrics {
        &self.metrics
    }

    /// The address the admin listener serves on, if it runs (ADR-0034).
    ///
    /// It differs from the configured `[admin] listen` when that names port 0. It is `None`
    /// when `[admin] enabled = false`, and under simulation, whose network isn't TCP.
    #[must_use]
    pub fn admin_address(&self) -> Option<SocketAddr> {
        self.admin.as_ref().map(RunningAdmin::address)
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
        self.control.as_ref().map(|role| role.node.handle())
    }

    /// The node's membership and ring, if this daemon hosts data.
    #[must_use]
    pub fn membership(&self) -> Option<Membership> {
        self.data.as_ref().map(|data| data.membership.clone())
    }

    /// The databases this node hosts, if this daemon hosts data.
    #[must_use]
    pub fn registry(&self) -> Option<Registry> {
        self.data
            .as_ref()
            .map(|data| data.replication.registry().clone())
    }

    /// The node's replication, if this daemon hosts data: the streams of the databases it is
    /// primary for, and its copies of those it follows.
    #[must_use]
    pub fn replication(&self) -> Option<Replication> {
        self.data.as_ref().map(|data| data.replication.clone())
    }

    /// The node's change streams, if this daemon hosts data: the sinks fed from the databases
    /// it is primary for (FR-039).
    #[must_use]
    pub fn change_streams(&self) -> Option<ChangeStreams> {
        self.data.as_ref().map(|data| data.changes.clone())
    }

    /// Serves until `shutdown` completes, then stops taking work, finishes in-flight writes, closes
    /// its databases, and releases the data directory.
    ///
    /// Connections get a few seconds to close; any still open after that are dropped.
    ///
    /// A data node that leaves on shutdown (`[membership] on_shutdown = "leave"`, the default)
    /// drains first: it hands every lease it holds to a follower, so that clients see a short
    /// redirect instead of a failover, and then leaves, so the control plane removes it at once
    /// instead of waiting for its heartbeats to stop. A lease that can't be handed over expires
    /// and fails over as usual. One that stays does neither.
    pub async fn run_until(self, shutdown: impl Future<Output = ()>) {
        self.run_until_stop(async {
            shutdown.await;
            Stop::AsConfigured
        })
        .await;
    }

    /// Serves until `stop` completes, then stops as [`Daemon::run_until`] does, or, for
    /// [`Stop::Restart`], drains as it does but stays a member, since the node comes straight
    /// back (ADR-0038, point 8).
    pub async fn run_until_stop(self, stop: impl Future<Output = Stop>) {
        let stop = stop.await;
        tracing::info!(node = %self.config.node.id, ?stop, "fleetringd shutting down");
        drop(self.sampler);
        let replication = match self.data {
            Some(data) => {
                if self.config.membership.on_shutdown == OnShutdown::Leave {
                    // Heartbeats go on meanwhile, and the server still answers, so peers can
                    // fence this node and clients get redirected.
                    data.node.drain_before_leaving(data.drain).await;
                }
                drop(data.keeper);
                match stop {
                    Stop::Restart => data.tasks.stop_without_leaving().await,
                    Stop::AsConfigured => data.tasks.stop().await,
                }
                // Sinks get what is acknowledged so far; a pump that can't finish continues
                // from its recorded position next time.
                data.changes.stop(data.drain.handover).await;
                Some(data.replication)
            }
            None => None,
        };
        // Stop accepting calls, then stop the control writer, which also ends membership streams
        // so the server can finish.
        let _server_gone = self.server.stop.send(());
        // Probes answer until here, so a draining node reports `draining` rather than going
        // silent; they stop before the control writer, which `/healthz` would report.
        if let Some(admin) = self.admin {
            admin.stop().await;
        }
        if let Some(control) = self.control {
            control.failover.stop().await;
            control.node.shutdown().await;
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
        // No call is running any more: stop the primaries' streams, finish the writes databases
        // accepted, then close them.
        if let Some(replication) = replication {
            replication.stop();
            replication.registry().close_all().await;
        }
        drop(self.data_dir);
        tracing::info!("fleetringd stopped");
    }
}

/// What [`Daemon::check`] checks, with `provided`, the identity the environment's network
/// already carries, if any, in place of a `[tls]` table.
fn check_with(config: &Config, provided: &Security) -> Result<(), StartError> {
    config.validate()?;
    let identity = config.tls.is_some() || !provided.is_plaintext();
    if !identity && config.insecure_plaintext == Plaintext::Refused {
        return Err(no_identity());
    }
    if !identity
        && config.admin.enabled == AdminListener::Enabled
        && config.admin.tls != AdminTls::Off
    {
        return Err(no_admin_identity());
    }
    SqliteEngine::new(config.engine.page_size)?;
    Ok(())
}

/// A node with no identity that may not run in plaintext.
fn no_identity() -> StartError {
    StartError::Config(ConfigError::Missing {
        field: "tls",
        reason: "every connection carries a certificate; add a [tls] table with ca, cert, and key \
                 (see `fleetringd cert`), or pass --insecure-plaintext to run without one",
    })
}

/// `[admin] tls = true` with no identity to serve.
fn no_admin_identity() -> StartError {
    StartError::Config(ConfigError::Missing {
        field: "tls",
        reason: "admin.tls = true serves the node's certificate; add a [tls] table",
    })
}

/// The identity every connection carries: the `[tls]` files, or else the one the environment's
/// network already has, or else plaintext if the configuration allows it.
fn security(config: &Config, environment: &Environment) -> Result<Security, StartError> {
    if let Some(tls) = &config.tls {
        let identity = Identity::load(&tls.ca, &tls.cert, &tls.key)?;
        tracing::info!(
            subject = ?identity.peer(),
            fleet = %identity.fleet(),
            ca = %tls.ca.display(),
            cert = %tls.cert.display(),
            "loaded the node's identity",
        );
        return Ok(Security::Tls(identity));
    }
    if let Security::Tls(identity) = environment.network().security() {
        return Ok(Security::Tls(identity.clone()));
    }
    match config.insecure_plaintext {
        Plaintext::Allowed => {
            tracing::warn!(
                "insecure_plaintext is set: serving and dialing in plaintext, so nothing on the \
                 wire is authenticated or encrypted; for local experiments only",
            );
            Ok(Security::Plaintext)
        }
        Plaintext::Refused => Err(no_identity()),
    }
}

async fn open_control(
    config: &Config,
    data_dir: &DataDir,
    environment: &Environment,
    metrics: &Metrics,
) -> Result<ControlNode, StartError> {
    let options = ControlOptions::new(config.control.control_config())
        .with_snapshot_every(config.control.snapshot_every)
        .with_storage_io(environment.control_storage_io());
    let path = data_dir.path().join(CONTROL_DB);
    Ok(ControlNode::open_with_metrics(
        path,
        options,
        environment.clock(),
        metrics.control().clone(),
    )
    .await?)
}

/// What the admin listener serves over TLS, if `[admin] tls = true`: the node's own identity,
/// which `[admin] tls` needs.
fn admin_tls(config: &Config, security: &Security) -> Result<Option<AdminTlsParts>, StartError> {
    if config.admin.enabled == AdminListener::Disabled || config.admin.tls == AdminTls::Off {
        return Ok(None);
    }
    match security {
        Security::Tls(identity) => Ok(Some(AdminTlsParts {
            identity: identity.clone(),
            scrapers: config.admin.scrapers.clone(),
        })),
        Security::Plaintext => Err(no_admin_identity()),
    }
}

/// Binds `[admin] listen`, unless the admin listener is disabled or the network isn't TCP, as
/// under simulation (ADR-0034 point 1).
async fn bind_admin(
    config: &Config,
    network: &Network,
) -> Result<Option<tokio::net::TcpListener>, StartError> {
    let admin = &config.admin;
    if admin.enabled == AdminListener::Disabled || !network.is_tcp() {
        return Ok(None);
    }
    if admin.tls == AdminTls::Off && !admin.listen.ip().is_loopback() {
        // Validation lets this through only with `allow_plaintext`.
        debug_assert_eq!(admin.allow_plaintext, AdminPlaintext::Allowed);
        tracing::warn!(
            listen = %admin.listen,
            "admin.allow_plaintext is set: serving metrics and probes in plaintext on a \
             non-loopback address, to anyone who can connect",
        );
    }
    let listener = tokio::net::TcpListener::bind(admin.listen)
        .await
        .map_err(|source| StartError::Listen {
            address: admin.listen,
            source,
        })?;
    Ok(Some(listener))
}

/// The address this node reaches the control plane at: `[control] address`, or else its own
/// listener, which the control role shares.
fn control_address(config: &Config, bound: SocketAddr) -> SocketAddr {
    config.control.address.unwrap_or_else(|| reachable(bound))
}

/// The data role's address for peers: `[node] advertise`, or else the address it serves on.
fn advertised(config: &Config, bound: SocketAddr) -> SocketAddr {
    config.node.advertise.unwrap_or_else(|| reachable(bound))
}

/// Starts the data role: membership with the advertised address, the registry under
/// `databases/` with replication around it, and the node behind the `Database` service.
///
/// Replication finds peers through membership, whose members carry the addresses they
/// advertised, dials them on the environment's network, and keeps its own state under
/// `replication/`. Heartbeats report every database it is primary for or follows.
fn start_data(
    config: &Config,
    data_dir: &DataDir,
    environment: &Environment,
    bound: SocketAddr,
    metrics: &Metrics,
) -> Result<(DataRole, DataNode), StartError> {
    let engine = SqliteEngine::new(config.engine.page_size)?;
    let archive_engine: Arc<dyn DatabaseEngine> = Arc::new(engine.clone());
    let network = environment.network();
    let control = network.control_client(control_address(config, bound).to_string());
    let member = config
        .node
        .to_member()
        .with_address(advertised(config, bound).to_string());
    // Replication reads membership, and heartbeats report replication's databases, so membership
    // starts first and learns its source of statuses as soon as replication exists.
    let statuses = ReplicationStatuses::default();
    let (membership, tasks) = Membership::start_observed(
        member,
        control.clone(),
        &config.membership,
        statuses.clone(),
        metrics.membership().clone(),
    );
    let settings = config.replication.settings();
    let databases = data_dir.path().join(DATABASES_DIR);
    let engine_config = config.engine.clone();
    let recorded = config.changes.capture_rules();
    let replication = Replication::start(
        ReplicationParts {
            node: config.node.id.clone(),
            engine: Arc::new(engine.clone()),
            membership: membership.clone(),
            peers: Arc::new(Peers::with_network(network.clone())),
            addresses: Arc::new(membership.clone()),
            authority: Arc::new(control.clone()),
            clock: environment.clock(),
            execution: config.engine.execution,
            state_dir: data_dir.path().join(REPLICATION_DIR),
            settings: settings.clone(),
            metrics: metrics.replication().clone(),
        },
        move |publisher| {
            Registry::new(engine, publisher, databases, engine_config)
                .with_recorded_changes(recorded)
        },
    );
    statuses.set(replication.clone());
    let mut sinks = config
        .changes
        .sink
        .iter()
        .map(|sink| build_sink(sink, config.engine.execution))
        .collect::<Result<Vec<_>, _>>()?;
    let archives = start_archives(config, environment, &replication, &archive_engine, &control)?;
    sinks.extend(archives.iter().map(Archive::sink));
    let changes = ChangeStreams::start(ChangeParts {
        sinks,
        replication: replication.clone(),
        state_dir: data_dir.path().join(CHANGES_DIR),
        settings: ChangeSettings::new(),
        metrics: metrics.changes().clone(),
    });
    let node = DataNode::new(
        config.node.id.clone(),
        replication.clone(),
        changes.clone(),
        control,
        archives,
        settings.ack_timeout,
        config.limits.statement_timeout(),
        metrics.data().clone(),
    );
    let drain = DrainSettings::new(settings.stall_timeout, settings.ack_timeout);
    let keeper = LeaseKeeper::start(
        node.clone(),
        LeaseSettings {
            renew_interval: config.control.lease_renewal(),
            fence_timeout: settings.stall_timeout,
            transfer_timeout: settings.open_timeout,
            drain,
            drop_flush_limit: environment.drop_flush_limit(),
        },
    );
    let role = DataRole {
        membership,
        tasks,
        replication,
        changes,
        node: node.clone(),
        keeper,
        drain,
    };
    Ok((role, node))
}

/// Builds every `[[archive]]` entry's archive, writing to the store the environment puts in
/// its place, if any, else to the one the entry names.
fn start_archives(
    config: &Config,
    environment: &Environment,
    replication: &Replication,
    engine: &Arc<dyn DatabaseEngine>,
    control: &ControlClient,
) -> Result<Vec<Archive>, StartError> {
    config
        .archive
        .iter()
        .map(|entry| {
            let store = match environment.object_store(&entry.name) {
                Some(store) => store,
                None => build_store(entry, environment.clock(), config.engine.execution)?,
            };
            let parts = ArchiveParts {
                replication: replication.clone(),
                engine: Arc::clone(engine),
                clock: environment.clock(),
                execution: config.engine.execution,
                metadata: Arc::new(ControlMetadata::new(control.clone())),
            };
            let archive = Archive::new(entry, store, parts)?;
            tracing::info!(archive = %entry.name, store = ?entry.store, prefix = %entry.prefix, "archiving databases");
            Ok(archive)
        })
        .collect()
}

/// The databases a data node's heartbeats report: those of its replication, once it exists.
#[derive(Debug, Clone, Default)]
struct ReplicationStatuses(Arc<OnceLock<Replication>>);

impl ReplicationStatuses {
    fn set(&self, replication: Replication) {
        // Set once, right after replication starts.
        let _set = self.0.set(replication);
    }
}

impl DatabaseStatuses for ReplicationStatuses {
    fn statuses(&self) -> Vec<DatabaseStatus> {
        self.0
            .get()
            .map(DatabaseStatuses::statuses)
            .unwrap_or_default()
    }
}

/// Binds `listen` on `network`, and returns the listener with the address it bound.
async fn bind(config: &Config, network: &Network) -> Result<(Listener, SocketAddr), StartError> {
    let listen_error = |source| StartError::Listen {
        address: config.listen,
        source,
    };
    let listener = if network.is_tcp() {
        Listener::Tcp(
            tokio::net::TcpListener::bind(config.listen)
                .await
                .map_err(listen_error)?,
        )
    } else {
        #[cfg(feature = "simulation")]
        {
            Listener::Simulated(
                turmoil::net::TcpListener::bind(config.listen)
                    .await
                    .map_err(listen_error)?,
            )
        }
        #[cfg(not(feature = "simulation"))]
        {
            unreachable!("every network is TCP without the simulation feature")
        }
    };
    let address = listener.local_addr().map_err(listen_error)?;
    Ok((listener, address))
}

/// How long a data node's token denylist may go without a fresh copy before the node refuses
/// tokens (ADR-0026, amended).
///
/// That is the longer of the control plane's heartbeat timeout, after which it marks a member
/// down, and [`HEARTBEATS_PER_TIMEOUT`] heartbeat intervals. A node the control plane still counts
/// as live therefore never refuses tokens for want of a list, and a node it counts as down does
/// within the same time: a token is trusted only as long as the node is a member in good standing.
/// A bare multiple of the interval would shrink with it: a short interval, as a test or a tight
/// deployment sets, would make a scheduling stall of a few hundred milliseconds refuse every
/// token.
///
/// [`HEARTBEATS_PER_TIMEOUT`]: crate::HEARTBEATS_PER_TIMEOUT
fn denylist_max_age(config: &Config) -> Duration {
    let timeout = Duration::from_millis(config.control.heartbeat_timeout_ms.get());
    let intervals = config
        .membership
        .heartbeat_interval()
        .saturating_mul(u32::try_from(HEARTBEATS_PER_TIMEOUT).unwrap_or(u32::MAX));
    timeout.max(intervals)
}

/// Who may connect and what they may do: every connection is authenticated under `security`,
/// and every request authorized by `authorizer` (ADR-0025).
struct Guard {
    security: Security,
    authorizer: Authorizer,
}

/// Serves on `listener`, guarded by `guard`: the `Control` service if `control` runs here, and the `Database` and `Replication` services if `data`
/// does. A node without the control role answers control calls with [`NoControl`], which names
/// the control node at `control_address`.
fn serve(
    listener: Listener,
    address: SocketAddr,
    control: Option<ControlService>,
    data: Option<&DataNode>,
    control_address: SocketAddr,
    guard: Guard,
    trace: TraceLayer,
) -> RunningServer {
    let Guard {
        security,
        authorizer,
    } = guard;
    let (stop, stopped) = oneshot::channel::<()>();
    // A dropped sender also means stop.
    let shutdown = async {
        let _stop = stopped.await;
    };
    let no_control = control.is_none();
    // The trace layer is outermost, so its span covers the boundary's checks and audit lines.
    let router = Server::builder()
        .layer(
            tower::ServiceBuilder::new()
                .layer(trace)
                .layer(AuthLayer::new(authorizer))
                .into_inner(),
        )
        .add_optional_service(control.map(control_server))
        .add_optional_service(
            no_control.then(|| control_server(NoControl::new(control_address.to_string()))),
        )
        .add_optional_service(data.map(|node| database_server(DatabaseService::new(node.clone()))))
        .add_optional_service(data.map(|node| node.replication().server()));
    let task =
        tokio::spawn(router.serve_with_incoming_shutdown(incoming(listener, security), shutdown));
    RunningServer {
        address,
        stop,
        task,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn data_node(tables: &str) -> Config {
        Config::from_toml(&format!(
            "data_dir = \"/tmp/fleetring-test\"\nlisten = \"127.0.0.1:0\"\nrole = \"data\"\n\n\
             [node]\nid = \"data-1\"\n\n[control]\naddress = \"127.0.0.1:7400\"\n{tables}"
        ))
        .expect("a valid data node config")
    }

    #[test]
    fn the_denylist_goes_stale_when_the_control_plane_would_mark_the_node_down() {
        // The defaults: a 5 s heartbeat timeout, not three 1 s intervals.
        assert_eq!(denylist_max_age(&data_node("")), Duration::from_secs(5));
        // A short interval does not shorten it: the timeout is what the operator sizes against
        // the network's jitter.
        let fast =
            data_node("heartbeat_timeout_ms = 1500\n\n[membership]\nheartbeat_interval_ms = 100\n");
        assert_eq!(denylist_max_age(&fast), Duration::from_millis(1_500));
        // Never less than three intervals, whatever the timeout.
        let slow = data_node(
            "heartbeat_timeout_ms = 3000\n\n[membership]\nheartbeat_interval_ms = 1000\n",
        );
        assert_eq!(denylist_max_age(&slow), Duration::from_secs(3));
    }
}
