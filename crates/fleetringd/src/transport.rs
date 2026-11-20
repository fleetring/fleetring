//! Node-to-node transport: gRPC over tonic (ADR-0019).
//!
//! Every server and client here accepts messages up to [`MAX_MESSAGE_BYTES`], so one replication
//! batch or snapshot chunk always fits in one message. Retries back off without jitter, and wait
//! with `tokio::time`, so they stay deterministic under simulation (ADR-0020).

use std::collections::{BTreeMap, VecDeque};
use std::fmt;
use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use fleetring_proto::MAX_MESSAGE_BYTES;
use fleetring_proto::v1::control_client::ControlClient;
use fleetring_proto::v1::control_server::{Control, ControlServer};
use fleetring_proto::v1::database_client::DatabaseClient;
use fleetring_proto::v1::database_server::{Database, DatabaseServer};
use fleetring_proto::v1::replication_client::ReplicationClient;
use fleetring_proto::v1::replication_server::{Replication, ReplicationServer};
use tokio_stream::Stream;
use tonic::metadata::{Ascii, MetadataValue};
use tonic::service::Interceptor;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::Channel;
use tonic::{Code, Status};

use crate::auth::{AUTHORIZATION, BearerToken, TokenError};
use crate::environment::Network;
use crate::observe::request_id::{self, REQUEST_ID_KEY};
use crate::per_ip::PerIp;
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

/// A channel whose requests carry a client's [`Credentials`].
pub type CredentialedChannel = InterceptedService<Channel, Credentials>;

/// What a client's requests carry besides its certificate: a bearer token, or nothing.
///
/// A node's requests carry nothing; an application's carry the token that names its principal
/// and scopes (ADR-0025). `Debug` never shows the token.
#[derive(Clone, Default)]
pub struct Credentials {
    authorization: Option<MetadataValue<Ascii>>,
}

impl Credentials {
    /// Requests carry nothing but the connection's certificate.
    #[must_use]
    pub fn none() -> Self {
        Self::default()
    }

    /// Requests carry `token` as `authorization: Bearer <token>`.
    ///
    /// # Errors
    ///
    /// [`TokenError::Malformed`] if the token holds characters a header can't carry.
    pub fn bearer(token: &BearerToken) -> Result<Self, TokenError> {
        let value = format!("Bearer {}", token.expose());
        let mut authorization =
            MetadataValue::try_from(value).map_err(|_| TokenError::Malformed)?;
        authorization.set_sensitive(true);
        Ok(Self {
            authorization: Some(authorization),
        })
    }

    /// Whether requests carry a token.
    #[must_use]
    pub fn has_token(&self) -> bool {
        self.authorization.is_some()
    }
}

impl fmt::Debug for Credentials {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let shown = if self.has_token() {
            "Bearer(..)"
        } else {
            "None"
        };
        f.debug_tuple("Credentials").field(&shown).finish()
    }
}

/// Requests also carry the ID of the request the calling task serves, if any, unless they name
/// one already (ADR-0034 point 8): a data node's control call on behalf of a request carries
/// the request's ID.
impl Interceptor for Credentials {
    fn call(&mut self, mut request: tonic::Request<()>) -> Result<tonic::Request<()>, Status> {
        let metadata = request.metadata_mut();
        if let Some(authorization) = &self.authorization {
            metadata.insert(AUTHORIZATION, authorization.clone());
        }
        if !metadata.contains_key(REQUEST_ID_KEY)
            && let Some(id) = request_id::current()
            && let Ok(value) = MetadataValue::try_from(id.as_str())
        {
            metadata.insert(REQUEST_ID_KEY, value);
        }
        Ok(request)
    }
}

/// A `Control` client over `channel` whose requests carry `credentials`, with FleetRing's
/// message limits.
#[must_use]
pub fn control_client_with(
    channel: Channel,
    credentials: &Credentials,
) -> ControlClient<CredentialedChannel> {
    ControlClient::with_interceptor(channel, credentials.clone())
        .max_decoding_message_size(MAX_MESSAGE_BYTES)
        .max_encoding_message_size(MAX_MESSAGE_BYTES)
}

/// A `Database` client over `channel` whose requests carry `credentials`, with FleetRing's
/// message limits.
#[must_use]
pub fn database_client_with(
    channel: Channel,
    credentials: &Credentials,
) -> DatabaseClient<CredentialedChannel> {
    DatabaseClient::with_interceptor(channel, credentials.clone())
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

    /// The next connection, and the address it comes from.
    async fn accept(&self) -> io::Result<(Raw, SocketAddr)> {
        match self {
            Self::Tcp(listener) => {
                let (stream, peer) = listener.accept().await?;
                // Consistent with tonic's own listener: a failure to set the option isn't fatal.
                let _ = stream.set_nodelay(true);
                Ok((Raw::Tcp(stream), peer))
            }
            #[cfg(feature = "simulation")]
            Self::Simulated(listener) => {
                let (stream, peer) = listener.accept().await?;
                Ok((Raw::Simulated(stream), peer))
            }
        }
    }
}

/// The connections accepted on `listener`, each after its handshake under `security`, for
/// `serve_with_incoming`.
///
/// Handshakes run concurrently and never hold up the accept loop; one that fails is logged
/// and its connection dropped, so the server only ever sees peers that authenticated. At most
/// 128 run at once, 16 of them from one client address: a connection beyond an address's share
/// is closed at once, and one beyond the total closes the oldest handshake in progress. An error
/// from the listener itself is logged and passed on, as tonic's own listener does, and the loop
/// waits a tenth of a second before it accepts again.
///
/// # Panics
///
/// If called outside a Tokio runtime.
pub fn incoming(
    listener: Listener,
    security: Security,
) -> impl Stream<Item = io::Result<Accepted>> {
    incoming_and_accept_loop(listener, security).0
}

/// The task behind [`incoming`]'s connections that accepts them and owns the listener. It ends,
/// closing the listener, once the connections' stream is dropped, as a server drops it when it
/// stops accepting.
#[derive(Debug)]
pub(crate) struct AcceptLoop(tokio::task::JoinHandle<()>);

impl AcceptLoop {
    /// Waits until the accept loop has ended and its listener is closed, so that the port is
    /// free. Call it once the connections' stream is dropped; until then the loop goes on.
    pub(crate) async fn closed(self) {
        // The loop only ends, never fails; a panic in it has been reported already.
        let _ended = self.0.await;
    }
}

/// The connections [`incoming`] returns, with the accept loop that owns `listener`, which a
/// caller awaits to know the port is free.
///
/// # Panics
///
/// If called outside a Tokio runtime.
pub(crate) fn incoming_and_accept_loop(
    listener: Listener,
    security: Security,
) -> (impl Stream<Item = io::Result<Accepted>>, AcceptLoop) {
    incoming_with_limits(listener, security, HandshakeLimits::NODE)
}

/// How many TLS handshakes a node's listener runs at once. A handshake holds its socket until it
/// completes or [`HANDSHAKE_TIMEOUT`](crate::tls::HANDSHAKE_TIMEOUT) ends it, before the peer has
/// shown a certificate, so this bounds the file descriptors anyone who reaches the port can hold.
/// A connection beyond it closes the oldest handshake in progress. It is half the lowest common
/// default descriptor limit (256 on macOS), and far above what a fleet's peers need, since a
/// handshake takes milliseconds.
pub(crate) const MAX_HANDSHAKES: usize = 128;

/// How many of [`MAX_HANDSHAKES`] one client address can hold, so that one address can't take
/// every handshake from the fleet's own peers. An IPv6 client's /64 counts as one address.
pub(crate) const MAX_HANDSHAKES_PER_ADDRESS: usize = 16;

/// How long the accept loop waits after a failed accept, such as when the process is out of file
/// descriptors, rather than retrying at once.
pub(crate) const ACCEPT_RETRY: Duration = Duration::from_millis(100);

/// The caps on handshakes in progress on one listener.
#[derive(Debug, Clone, Copy)]
struct HandshakeLimits {
    total: usize,
    per_address: usize,
}

impl HandshakeLimits {
    const NODE: Self = Self {
        total: MAX_HANDSHAKES,
        per_address: MAX_HANDSHAKES_PER_ADDRESS,
    };
}

fn incoming_with_limits(
    listener: Listener,
    security: Security,
    limits: HandshakeLimits,
) -> (impl Stream<Item = io::Result<Accepted>>, AcceptLoop) {
    let (sender, receiver) = tokio::sync::mpsc::channel(16);
    let accepting = tokio::spawn(async move {
        let per_address = Arc::new(PerIp::new(limits.per_address));
        let mut handshakes = tokio::task::JoinSet::new();
        // The handshakes in the order they started, some of them finished, to close the oldest.
        let mut started: VecDeque<tokio::task::AbortHandle> = VecDeque::new();
        let mut closed_oldest = Tally::default();
        loop {
            let accepted = tokio::select! {
                // A finished handshake is reaped before the next accept, so a full listener
                // counts only handshakes in progress.
                biased;
                () = sender.closed() => break,
                Some(_) = handshakes.join_next(), if !handshakes.is_empty() => continue,
                accepted = listener.accept() => accepted,
            };
            let (raw, peer) = match accepted {
                Ok(accepted) => accepted,
                // The client gave up before it was accepted: nothing is wrong with the listener.
                Err(error) if is_connection_error(&error) => {
                    tracing::debug!(%error, "a connection closed before it was accepted");
                    continue;
                }
                Err(error) => {
                    tracing::warn!(%error, "the node's listener couldn't accept a connection");
                    if sender.send(Err(error)).await.is_err() {
                        break;
                    }
                    // Out of file descriptors, say: wait rather than spin on the same error.
                    tokio::select! {
                        () = sender.closed() => break,
                        () = tokio::time::sleep(ACCEPT_RETRY) => continue,
                    }
                }
            };
            let Some(held) = PerIp::hold(&per_address, peer) else {
                tracing::debug!(
                    %peer,
                    "too many TLS handshakes in progress from one address; closing a connection"
                );
                continue;
            };
            if handshakes.len() >= limits.total {
                // A peer's handshake takes milliseconds, so the oldest in progress is the likeliest
                // to be one that never finishes, and closing it, rather than the new connection,
                // keeps the slots from being held against the fleet's own peers.
                while let Some(oldest) = started.pop_front() {
                    if !oldest.is_finished() {
                        oldest.abort();
                        break;
                    }
                }
                if let Some(count) = closed_oldest.add() {
                    tracing::warn!(
                        max = limits.total,
                        closed = count,
                        "too many TLS handshakes in progress; closed the oldest"
                    );
                }
            }
            let sender = sender.clone();
            let security = security.clone();
            started.push_back(handshakes.spawn(async move {
                let accepted = security.accept(raw).await;
                drop(held);
                match accepted {
                    Ok(accepted) => {
                        tracing::debug!(peer = ?accepted.peer(), "connection accepted");
                        // The server went away, so the connection goes with it.
                        let _gone = sender.send(Ok(accepted)).await;
                    }
                    Err(error) => tracing::warn!(%error, "connection refused"),
                }
            }));
            if started.len() > 2 * limits.total {
                started.retain(|handshake| !handshake.is_finished());
            }
        }
        // Once the server stops, the listener closes, so the port comes free for a node started
        // again in the same process, and so does every connection still in its handshake.
        drop(listener);
        handshakes.shutdown().await;
    });
    (
        tokio_stream::wrappers::ReceiverStream::new(receiver),
        AcceptLoop(accepting),
    )
}

/// Whether an accept failed for that one connection, which the client reset or gave up on, rather
/// than for the listener, as tonic's own listener tells them apart.
fn is_connection_error(error: &io::Error) -> bool {
    matches!(
        error.kind(),
        io::ErrorKind::ConnectionRefused
            | io::ErrorKind::ConnectionAborted
            | io::ErrorKind::ConnectionReset
    )
}

/// How often the accept loop reports the handshakes it closed to make room, at most.
const TALLY_EVERY: Duration = Duration::from_secs(10);

/// A count of events reported at most once per [`TALLY_EVERY`], so that a flood of connections
/// doesn't flood the log.
#[derive(Debug, Default)]
struct Tally {
    count: u64,
    reported: Option<tokio::time::Instant>,
}

impl Tally {
    /// Counts one more, and returns the count to report, if one is due, starting again from zero.
    fn add(&mut self) -> Option<u64> {
        self.count += 1;
        let now = tokio::time::Instant::now();
        if self
            .reported
            .is_some_and(|reported| now.duration_since(reported) < TALLY_EVERY)
        {
            return None;
        }
        self.reported = Some(now);
        Some(std::mem::take(&mut self.count))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Once the connections' stream is dropped, the accept loop's end means the port is free:
    /// a listener binds it at once, with no turn of the runtime in between.
    #[tokio::test(flavor = "current_thread")]
    async fn the_port_is_free_once_the_accept_loop_ends() -> io::Result<()> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let (connections, accepting) =
            incoming_and_accept_loop(Listener::Tcp(listener), Security::Plaintext);
        // The loop has started, and waits for a connection.
        tokio::task::yield_now().await;
        drop(connections);
        accepting.closed().await;
        std::net::TcpListener::bind(address).map(drop)
    }

    /// A TLS listener on a free loopback port, with `limits`.
    async fn tls_listener(
        limits: HandshakeLimits,
    ) -> io::Result<(
        SocketAddr,
        impl Stream<Item = io::Result<Accepted>>,
        AcceptLoop,
    )> {
        use fleetring_control::Timestamp;

        use crate::tls::{Authority, Identity, Validity};

        let validity = Validity::new(Timestamp::ZERO, Duration::from_secs(100 * 365 * 24 * 3600));
        let authority =
            Authority::create("test".parse().expect("a fleet"), validity).expect("an authority");
        let issued = authority
            .issue_node(
                &"n1".parse().expect("a node id"),
                &["127.0.0.1".to_owned()],
                validity,
            )
            .expect("a node certificate");
        let identity =
            Identity::from_issued(authority.certificate_pem(), &issued).expect("an identity");
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let (connections, accepting) =
            incoming_with_limits(Listener::Tcp(listener), Security::Tls(identity), limits);
        Ok((address, connections, accepting))
    }

    /// How long a test waits to see a connection closed: far less than the handshake timeout,
    /// so a close it sees is the listener's doing, not the timeout's.
    const CLOSE_WAIT: Duration = Duration::from_secs(2);

    /// Whether the server has closed `client`'s connection, seen within [`CLOSE_WAIT`].
    async fn closed(client: &mut tokio::net::TcpStream) -> bool {
        use tokio::io::AsyncReadExt as _;

        let mut byte = [0; 1];
        match tokio::time::timeout(CLOSE_WAIT, client.read(&mut byte)).await {
            Ok(Ok(0) | Err(_)) => true,
            Ok(Ok(_)) => panic!("the server sent bytes before the client said anything"),
            Err(_) => false,
        }
    }

    /// Connections that each start a handshake and never send a byte, once the listener has
    /// accepted them all.
    async fn silent_clients(
        address: SocketAddr,
        count: usize,
    ) -> io::Result<Vec<tokio::net::TcpStream>> {
        let mut clients = Vec::with_capacity(count);
        for _ in 0..count {
            clients.push(tokio::net::TcpStream::connect(address).await?);
        }
        // Long enough for the accept loop to take each from the backlog.
        tokio::time::sleep(Duration::from_millis(200)).await;
        Ok(clients)
    }

    /// A handshake still in progress when the server stops ends with the accept loop and closes
    /// its connection, rather than holding it until the handshake times out.
    #[tokio::test(flavor = "current_thread")]
    async fn a_handshake_in_progress_ends_with_the_accept_loop() -> io::Result<()> {
        let (address, connections, accepting) = tls_listener(HandshakeLimits::NODE).await?;
        let mut clients = silent_clients(address, 1).await?;
        drop(connections);
        accepting.closed().await;
        assert!(
            closed(&mut clients[0]).await,
            "the connection is still open"
        );
        Ok(())
    }

    /// Beyond the handshakes one address may hold, a new connection from it is closed at once,
    /// while the ones it holds stay open.
    #[tokio::test(flavor = "current_thread")]
    async fn a_connection_beyond_an_addresss_handshakes_is_closed() -> io::Result<()> {
        let limits = HandshakeLimits {
            total: 8,
            per_address: 2,
        };
        let (address, _connections, _accepting) = tls_listener(limits).await?;
        let mut clients = silent_clients(address, 3).await?;
        assert!(
            closed(&mut clients[2]).await,
            "the third connection is open"
        );
        assert!(
            !closed(&mut clients[0]).await,
            "the first connection was closed"
        );
        Ok(())
    }

    /// Beyond the handshakes a listener runs at once, a new connection closes the oldest
    /// handshake in progress, and starts its own.
    #[tokio::test(flavor = "current_thread")]
    async fn a_connection_beyond_the_listeners_handshakes_closes_the_oldest() -> io::Result<()> {
        let limits = HandshakeLimits {
            total: 2,
            per_address: 8,
        };
        let (address, _connections, _accepting) = tls_listener(limits).await?;
        let mut clients = silent_clients(address, 3).await?;
        assert!(
            closed(&mut clients[0]).await,
            "the oldest connection is open"
        );
        assert!(
            !closed(&mut clients[2]).await,
            "the newest connection was closed"
        );
        Ok(())
    }
}
