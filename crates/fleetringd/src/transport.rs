//! Node-to-node transport: gRPC over tonic (ADR-0019).
//!
//! Every server and client here accepts messages up to [`MAX_MESSAGE_BYTES`], so one replication
//! batch or snapshot chunk always fits in one message. Retries back off without jitter, and wait
//! with `tokio::time`, so they stay deterministic under simulation (ADR-0020).

use std::collections::BTreeMap;
use std::fmt;
use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::sync::Mutex;
use std::time::Duration;

use fleetring_proto::MAX_MESSAGE_BYTES;
use fleetring_proto::v1::control_client::ControlClient;
use fleetring_proto::v1::control_server::{Control, ControlServer};
use fleetring_proto::v1::database_client::DatabaseClient;
use fleetring_proto::v1::database_server::{Database, DatabaseServer};
use fleetring_proto::v1::replication_client::ReplicationClient;
use fleetring_proto::v1::replication_server::{Replication, ReplicationServer};
use tokio_stream::Stream;
use tonic::transport::Channel;
use tonic::{Code, Status};

use crate::environment::Network;
use crate::tls::{Accepted, Raw, Security};

/// Timeouts for connections to peers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TransportConfig {
    /// How long to wait for a connection to a peer.
    pub connect_timeout: Duration,
    /// How long to wait for a unary call. Streams aren't limited.
    pub request_timeout: Duration,
}

impl Default for TransportConfig {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_secs(3),
            request_timeout: Duration::from_secs(10),
        }
    }
}

/// One channel per peer, created on first use and shared by every call to that peer.
///
/// A tonic channel multiplexes calls over one HTTP/2 connection and reconnects on its own, so the
/// pool only has to remember which channel belongs to which address.
#[derive(Debug)]
pub struct Peers {
    network: Network,
    channels: Mutex<BTreeMap<SocketAddr, Channel>>,
    named: Mutex<BTreeMap<String, Channel>>,
}

impl Peers {
    /// An empty pool over real TCP, in plaintext.
    #[must_use]
    pub fn new(config: TransportConfig) -> Self {
        Self::with_network(Network::tcp(config))
    }

    /// An empty pool that dials over `network`, so that the same code runs over real TCP and
    /// under simulation (ADR-0020), with the identity the network carries.
    #[must_use]
    pub fn with_network(network: Network) -> Self {
        Self {
            network,
            channels: Mutex::new(BTreeMap::new()),
            named: Mutex::new(BTreeMap::new()),
        }
    }

    /// The channel to `address`, given as `host:port`, over the pool's network. It connects
    /// lazily, on the first call, and is shared until [`forget`](Self::forget) drops it.
    ///
    /// # Errors
    ///
    /// The transport error if `address` doesn't form a valid endpoint.
    ///
    /// # Panics
    ///
    /// If the pool's lock was poisoned by a panic in another thread.
    pub fn channel_to(&self, address: &str) -> Result<Channel, tonic::transport::Error> {
        let mut named = self.named.lock().expect("peer pool lock poisoned");
        if let Some(channel) = named.get(address) {
            return Ok(channel.clone());
        }
        let channel = self.network.channel(address)?;
        named.insert(address.to_owned(), channel.clone());
        Ok(channel)
    }

    /// Drops the shared channel to `address`, so that the next call dials a new connection.
    ///
    /// A connection can stall without failing, behind a partition or a peer that lost power;
    /// a caller that sees a call stall forgets the channel rather than queueing behind it.
    ///
    /// # Panics
    ///
    /// If the pool's lock was poisoned by a panic in another thread.
    pub fn forget(&self, address: &str) {
        self.named
            .lock()
            .expect("peer pool lock poisoned")
            .remove(address);
    }

    /// The channel to `peer`. It connects lazily, on the first call.
    ///
    /// # Panics
    ///
    /// If the pool's lock was poisoned by a panic in another thread, or if `peer` doesn't form
    /// a valid endpoint, which no socket address fails.
    #[must_use]
    pub fn channel(&self, peer: SocketAddr) -> Channel {
        let mut channels = self.channels.lock().expect("peer pool lock poisoned");
        channels
            .entry(peer)
            .or_insert_with(|| {
                self.network
                    .channel(&peer.to_string())
                    .unwrap_or_else(|error| {
                        panic!("'{peer}' is a valid endpoint for any socket address: {error}")
                    })
            })
            .clone()
    }

    /// A `Control` client for `peer`.
    #[must_use]
    pub fn control(&self, peer: SocketAddr) -> ControlClient<Channel> {
        control_client(self.channel(peer))
    }

    /// A `Replication` client for `peer`.
    #[must_use]
    pub fn replication(&self, peer: SocketAddr) -> ReplicationClient<Channel> {
        replication_client(self.channel(peer))
    }

    /// A `Database` client for `peer`.
    #[must_use]
    pub fn database(&self, peer: SocketAddr) -> DatabaseClient<Channel> {
        database_client(self.channel(peer))
    }
}

/// A `Control` client over `channel`, with FleetRing's message limits.
#[must_use]
pub fn control_client(channel: Channel) -> ControlClient<Channel> {
    ControlClient::new(channel)
        .max_decoding_message_size(MAX_MESSAGE_BYTES)
        .max_encoding_message_size(MAX_MESSAGE_BYTES)
}

/// A `Replication` client over `channel`, with FleetRing's message limits.
#[must_use]
pub fn replication_client(channel: Channel) -> ReplicationClient<Channel> {
    ReplicationClient::new(channel)
        .max_decoding_message_size(MAX_MESSAGE_BYTES)
        .max_encoding_message_size(MAX_MESSAGE_BYTES)
}

/// A `Database` client over `channel`, with FleetRing's message limits.
#[must_use]
pub fn database_client(channel: Channel) -> DatabaseClient<Channel> {
    DatabaseClient::new(channel)
        .max_decoding_message_size(MAX_MESSAGE_BYTES)
        .max_encoding_message_size(MAX_MESSAGE_BYTES)
}

/// A `Control` server for `service`, with FleetRing's message limits.
pub fn control_server<S: Control>(service: S) -> ControlServer<S> {
    ControlServer::new(service)
        .max_decoding_message_size(MAX_MESSAGE_BYTES)
        .max_encoding_message_size(MAX_MESSAGE_BYTES)
}

/// A `Replication` server for `service`, with FleetRing's message limits.
pub fn replication_server<S: Replication>(service: S) -> ReplicationServer<S> {
    ReplicationServer::new(service)
        .max_decoding_message_size(MAX_MESSAGE_BYTES)
        .max_encoding_message_size(MAX_MESSAGE_BYTES)
}

/// A `Database` server for `service`, with FleetRing's message limits.
pub fn database_server<S: Database>(service: S) -> DatabaseServer<S> {
    DatabaseServer::new(service)
        .max_decoding_message_size(MAX_MESSAGE_BYTES)
        .max_encoding_message_size(MAX_MESSAGE_BYTES)
}

/// Exponential backoff for retrying idempotent calls.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Backoff {
    /// The wait before the first retry.
    pub initial: Duration,
    /// The longest wait between retries.
    pub max: Duration,
    /// How many times to try in all, the first call included.
    pub attempts: u32,
}

impl Default for Backoff {
    fn default() -> Self {
        Self {
            initial: Duration::from_millis(50),
            max: Duration::from_secs(2),
            attempts: 6,
        }
    }
}

/// Runs `call` until it succeeds, fails with a status that retrying can't fix, or runs out of
/// attempts. Use it only for idempotent calls.
///
/// Retries `UNAVAILABLE`, `DEADLINE_EXCEEDED`, and `RESOURCE_EXHAUSTED`; every other status is
/// returned at once.
///
/// # Errors
///
/// The last status `call` returned.
pub async fn retry<T, F, Fut>(backoff: Backoff, mut call: F) -> Result<T, Status>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, Status>>,
{
    let mut wait = backoff.initial;
    let mut attempt = 1;
    loop {
        match call().await {
            Ok(value) => return Ok(value),
            Err(status) if attempt < backoff.attempts && is_retryable(&status) => {
                tracing::debug!(attempt, code = ?status.code(), "retrying after {wait:?}");
                tokio::time::sleep(wait).await;
                wait = wait.saturating_mul(2).min(backoff.max);
                attempt += 1;
            }
            Err(status) => return Err(status),
        }
    }
}

fn is_retryable(status: &Status) -> bool {
    matches!(
        status.code(),
        Code::Unavailable | Code::DeadlineExceeded | Code::ResourceExhausted
    )
}

/// The listener a daemon serves on: bound before any service starts, so its address is known.
pub enum Listener {
    /// A real TCP listener.
    Tcp(tokio::net::TcpListener),
    /// A listener on turmoil's simulated network.
    #[cfg(feature = "simulation")]
    Simulated(turmoil::net::TcpListener),
}

impl fmt::Debug for Listener {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (kind, address) = match self {
            Self::Tcp(listener) => ("Tcp", listener.local_addr().ok()),
            #[cfg(feature = "simulation")]
            Self::Simulated(listener) => ("Simulated", listener.local_addr().ok()),
        };
        f.debug_struct("Listener")
            .field("kind", &kind)
            .field("address", &address)
            .finish()
    }
}

impl Listener {
    /// Returns the address the listener is bound to.
    ///
    /// # Errors
    ///
    /// The I/O error if the socket can't report its address.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        match self {
            Self::Tcp(listener) => listener.local_addr(),
            #[cfg(feature = "simulation")]
            Self::Simulated(listener) => listener.local_addr(),
        }
    }

    async fn accept(&self) -> io::Result<Raw> {
        match self {
            Self::Tcp(listener) => {
                let (stream, _peer) = listener.accept().await?;
                // Consistent with tonic's own listener: a failure to set the option isn't fatal.
                let _ = stream.set_nodelay(true);
                Ok(Raw::Tcp(stream))
            }
            #[cfg(feature = "simulation")]
            Self::Simulated(listener) => {
                let (stream, _peer) = listener.accept().await?;
                Ok(Raw::Simulated(stream))
            }
        }
    }
}

/// The connections accepted on `listener`, each after its handshake under `security`, for
/// `serve_with_incoming`.
///
/// Handshakes run concurrently and never hold up the accept loop; one that fails is logged
/// and its connection dropped, so the server only ever sees peers that authenticated. An
/// error from the listener itself is passed on, as tonic's own listener does.
///
/// # Panics
///
/// If called outside a Tokio runtime.
pub fn incoming(
    listener: Listener,
    security: Security,
) -> impl Stream<Item = io::Result<Accepted>> {
    let (sender, receiver) = tokio::sync::mpsc::channel(16);
    tokio::spawn(async move {
        loop {
            let raw = match listener.accept().await {
                Ok(raw) => raw,
                Err(error) => {
                    if sender.send(Err(error)).await.is_err() {
                        break;
                    }
                    continue;
                }
            };
            let sender = sender.clone();
            let security = security.clone();
            tokio::spawn(async move {
                match security.accept(raw).await {
                    Ok(accepted) => {
                        tracing::debug!(peer = ?accepted.peer(), "connection accepted");
                        // The server went away, so the connection goes with it.
                        let _gone = sender.send(Ok(accepted)).await;
                    }
                    Err(error) => tracing::warn!(%error, "connection refused"),
                }
            });
        }
    });
    tokio_stream::wrappers::ReceiverStream::new(receiver)
}
