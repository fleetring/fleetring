//! The router: a Hrana-over-HTTP front door that lets unchanged libSQL and Turso clients reach
//! each database's primary through FleetRing (ADR-0037).
//!
//! `fleetring router --config <file>` runs it, as a separate long-running process with its own
//! identity: a client certificate for a principal, conventionally `router`, that every data
//! node lists with `forward_tokens = true` and no scopes of its own. It acts only with its
//! callers' tokens.
//!
//! - **Requests:** Hrana over HTTP with JSON, versions 2 and 3: `GET v2` and `GET v3`, `POST
//!   v2/pipeline` and `v3/pipeline`, and `POST v3/cursor`, under `/db/<percent-encoded
//!   database>/`, or under a host `<database>.<host_suffix>`.
//! - **Authentication first:** every request carries a FleetRing token in `Authorization:
//!   Bearer`, which the router verifies and checks for `read` on the database before it contacts
//!   any node; the nodes check the token again, with their denylist, on every call.
//! - **Sessions:** a Hrana stream runs on one `Database.Session` on its database's primary, so a
//!   transaction can span requests. The primary is found through the control plane, with the
//!   caller's token, cached, and corrected by redirects.
//! - **Batons** are unguessable, accepted once, and bound to the token that opened the stream.
//! - **Bounds:** request size, statements per request, streams held, stored SQL per stream, and
//!   idle streams expire; nodes bound open transactions and the writes waiting for them.

mod auth;
mod config;
mod hrana;
mod http;
mod naming;
mod pipeline;
mod placement;
mod session;
mod streams;
#[cfg(test)]
mod tests;

use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use rustls_pki_types::pem::PemObject as _;
use rustls_pki_types::{CertificateDer, PrivateKeyDer};
use tokio::sync::watch;
use tokio::task::JoinSet;
use tokio_rustls::TlsAcceptor;
use zeroize::Zeroizing;

pub use self::config::{HttpConfig, RouterConfig, RouterControl, RoutingConfig, StreamsConfig};
pub use self::streams::{Entropy, SystemEntropy};

use self::placement::PlacementCache;
use self::streams::Streams;
use crate::clock::Clock;
use crate::control::ControlClient;
use crate::environment::{Environment, KeepAlive};
use crate::tls::{Identity, IdentityError, Security};
use crate::transport::Peers;

/// How often idle streams are looked for.
const EXPIRY_INTERVAL: Duration = Duration::from_secs(1);

/// HTTP/2 keep-alive on the router's channels to nodes: a ping every 30 seconds while a session is
/// open, and the connection closed if one goes 20 seconds unanswered.
const PEER_KEEP_ALIVE: KeepAlive = KeepAlive {
    interval: Duration::from_secs(30),
    timeout: Duration::from_secs(20),
};

/// What every request the router serves shares.
#[derive(Debug)]
pub(crate) struct Context {
    pub(crate) config: RouterConfig,
    /// The router's identity toward the fleet, whose authority keys verify callers' tokens.
    pub(crate) identity: Identity,
    pub(crate) clock: Arc<dyn Clock>,
    pub(crate) control: ControlClient,
    /// One connection per data node, shared by every session to it.
    pub(crate) peers: Peers,
    pub(crate) placement: PlacementCache,
    pub(crate) streams: Streams,
    pub(crate) entropy: Arc<dyn Entropy>,
}

/// A running router.
#[derive(Debug)]
pub struct Router {
    address: SocketAddr,
    stop: watch::Sender<bool>,
    tasks: JoinSet<()>,
}

/// Why a router couldn't start.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum StartError {
    /// The router has no identity toward the fleet: `[tls]` names none.
    #[error(
        "the router needs an identity toward the fleet: set [tls] ca, cert, and key to a client \
         certificate for a principal that every data node lists with forward_tokens = true"
    )]
    NoIdentity,
    /// The identity named by `[tls]` couldn't be loaded.
    #[error("can't load the router's identity")]
    Identity(#[from] IdentityError),
    /// The identity names a node: a router acts for principals only.
    #[error("the router's certificate names a node; issue it a client certificate instead")]
    NodeIdentity,
    /// The listen address couldn't be bound.
    #[error("can't listen on {address}")]
    Listen {
        /// The address.
        address: SocketAddr,
        /// Why.
        source: io::Error,
    },
    /// The server certificate or key couldn't be used.
    #[error("can't serve TLS with {path}: {reason}")]
    ServerTls {
        /// The file at fault.
        path: String,
        /// Why.
        reason: String,
    },
}

impl Router {
    /// Starts a router from `config` on real TCP with the system clock and the operating
    /// system's randomness.
    ///
    /// # Errors
    ///
    /// [`StartError`] if its identity or server certificate can't be loaded, or its address
    /// can't be bound.
    pub async fn start(config: RouterConfig) -> Result<Self, StartError> {
        Self::start_with(
            config,
            Environment::production(),
            Arc::new(SystemEntropy::new()),
        )
        .await
    }

    /// Starts a router from `config` in `environment`, with batons' secrets from `entropy`: the
    /// simulation harness passes its own network, clock, identity, and seeded generator
    /// (ADR-0020).
    ///
    /// # Errors
    ///
    /// As [`start`](Self::start).
    pub async fn start_with(
        config: RouterConfig,
        environment: Environment,
        entropy: Arc<dyn Entropy>,
    ) -> Result<Self, StartError> {
        let identity = match (environment.network().security(), &config.tls) {
            (Security::Tls(identity), _) => identity.clone(),
            (_, Some(tls)) => Identity::load(&tls.ca, &tls.cert, &tls.key)?,
            _ => return Err(StartError::NoIdentity),
        };
        let principal = match identity.peer() {
            crate::tls::Peer::Principal(principal) => principal,
            _ => return Err(StartError::NodeIdentity),
        };
        // A session's stream to a primary that went away, behind a partition or with its power,
        // fails within the keep-alive's bound instead of holding its Hrana stream.
        let network = environment
            .network()
            .clone()
            .with_security(Security::Tls(identity.clone()))
            .with_keep_alive(PEER_KEEP_ALIVE);
        let tls = match (&config.http.cert, &config.http.key) {
            (Some(cert), Some(key)) => Some(server_tls(cert, key)?),
            _ => None,
        };
        let listener = RouterListener::bind(config.listen, &network).await?;
        let address = listener.local_addr().map_err(|source| StartError::Listen {
            address: config.listen,
            source,
        })?;
        let control = network.control_client(config.control.address.to_string());
        let context = Arc::new(Context {
            placement: PlacementCache::new(
                config.routing.placement_ttl(),
                config.routing.placement_cache_size.get(),
            ),
            streams: Streams::new(config.streams.clone()),
            peers: Peers::with_network(network),
            control,
            clock: environment.clock(),
            identity: identity.clone(),
            entropy,
            config,
        });
        let (stop, stopping) = watch::channel(false);
        let mut tasks = JoinSet::new();
        tasks.spawn(http::serve(
            listener,
            tls.clone(),
            Arc::clone(&context),
            stopping.clone(),
        ));
        tasks.spawn(expire(Arc::clone(&context), stopping));
        tracing::info!(
            listen = %address,
            fleet = %identity.fleet(),
            principal = %principal,
            tls = tls.is_some(),
            "router started"
        );
        Ok(Self {
            address,
            stop,
            tasks,
        })
    }

    /// The address the router serves on.
    #[must_use]
    pub fn listen_address(&self) -> SocketAddr {
        self.address
    }

    /// Serves until `shutdown` completes, then stops: every connection closes, and with it
    /// every session the router holds, so that nodes roll back what they left open.
    pub async fn run_until(self, shutdown: impl Future<Output = ()>) {
        shutdown.await;
        self.stop().await;
    }

    /// Stops serving, and waits until every connection and session has closed.
    pub async fn stop(mut self) {
        let _ = self.stop.send(true);
        while self.tasks.join_next().await.is_some() {}
        tracing::info!("router stopped");
    }
}

/// Looks for idle streams every [`EXPIRY_INTERVAL`] until `stop` turns true.
async fn expire(context: Arc<Context>, mut stop: watch::Receiver<bool>) {
    let mut ticks = tokio::time::interval(EXPIRY_INTERVAL);
    ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = stop.wait_for(|stop| *stop) => return,
            _ = ticks.tick() => context.streams.expire(),
        }
    }
}

/// The server side of the router's TLS: its certificate chain and key, from any authority.
fn server_tls(cert: &Path, key: &Path) -> Result<TlsAcceptor, StartError> {
    let invalid = |path: &Path, reason: String| StartError::ServerTls {
        path: path.display().to_string(),
        reason,
    };
    let pem = std::fs::read(cert).map_err(|error| invalid(cert, error.to_string()))?;
    let chain = CertificateDer::pem_slice_iter(&pem)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| invalid(cert, format!("not a PEM certificate chain: {error}")))?;
    if chain.is_empty() {
        return Err(invalid(cert, "holds no certificate".to_owned()));
    }
    let pem = Zeroizing::new(std::fs::read(key).map_err(|error| invalid(key, error.to_string()))?);
    let private = PrivateKeyDer::from_pem_slice(&pem)
        .map_err(|error| invalid(key, format!("not a PEM private key: {error}")))?;
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let mut server = rustls::ServerConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|error| invalid(cert, error.to_string()))?
        .with_no_client_auth()
        .with_single_cert(chain, private)
        .map_err(|error| invalid(cert, error.to_string()))?;
    server.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
    Ok(TlsAcceptor::from(Arc::new(server)))
}

/// The listener the router serves on: real TCP, or turmoil's.
pub(crate) enum RouterListener {
    Tcp(tokio::net::TcpListener),
    #[cfg(feature = "simulation")]
    Simulated(turmoil::net::TcpListener),
}

/// A connection the router accepted.
pub(crate) enum Accepted {
    Tcp(tokio::net::TcpStream),
    #[cfg(feature = "simulation")]
    Simulated(turmoil::net::TcpStream),
}

impl std::fmt::Debug for RouterListener {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("RouterListener")
            .field(&self.local_addr().ok())
            .finish()
    }
}

impl RouterListener {
    async fn bind(address: SocketAddr, network: &crate::Network) -> Result<Self, StartError> {
        let failed = |source| StartError::Listen { address, source };
        if network.is_tcp() {
            return Ok(Self::Tcp(
                tokio::net::TcpListener::bind(address)
                    .await
                    .map_err(failed)?,
            ));
        }
        #[cfg(feature = "simulation")]
        {
            Ok(Self::Simulated(
                turmoil::net::TcpListener::bind(address)
                    .await
                    .map_err(failed)?,
            ))
        }
        #[cfg(not(feature = "simulation"))]
        {
            unreachable!("every network is TCP without the simulation feature")
        }
    }

    fn local_addr(&self) -> io::Result<SocketAddr> {
        match self {
            Self::Tcp(listener) => listener.local_addr(),
            #[cfg(feature = "simulation")]
            Self::Simulated(listener) => listener.local_addr(),
        }
    }

    /// The next connection, and the address it comes from.
    pub(crate) async fn accept(&self) -> io::Result<(Accepted, SocketAddr)> {
        match self {
            Self::Tcp(listener) => {
                let (stream, peer) = listener.accept().await?;
                let _ = stream.set_nodelay(true);
                Ok((Accepted::Tcp(stream), peer))
            }
            #[cfg(feature = "simulation")]
            Self::Simulated(listener) => {
                let (stream, peer) = listener.accept().await?;
                Ok((Accepted::Simulated(stream), peer))
            }
        }
    }
}

impl Accepted {
    pub(crate) async fn serve(self, tls: Option<TlsAcceptor>, context: Arc<Context>) {
        match self {
            Self::Tcp(stream) => http::connection(stream, tls, context).await,
            #[cfg(feature = "simulation")]
            Self::Simulated(stream) => http::connection(stream, tls, context).await,
        }
    }
}

const _: () = {
    const fn assert_debug_send_sync<T: std::fmt::Debug + Send + Sync>() {}
    assert_debug_send_sync::<Router>();
    assert_debug_send_sync::<RouterConfig>();
    assert_debug_send_sync::<HttpConfig>();
    assert_debug_send_sync::<RouterControl>();
    assert_debug_send_sync::<RoutingConfig>();
    assert_debug_send_sync::<StreamsConfig>();
    assert_debug_send_sync::<StartError>();
    assert_debug_send_sync::<SystemEntropy>();
};
