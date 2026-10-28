//! Node-to-node transport: gRPC over tonic (ADR-0019).
//!
//! Every server and client here accepts messages up to [`MAX_MESSAGE_BYTES`], so one replication
//! batch or snapshot chunk always fits in one message. Retries back off without jitter, and wait
//! with `tokio::time`, so they stay deterministic under simulation (ADR-0020).

use std::collections::BTreeMap;
use std::future::Future;
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
use tonic::transport::{Channel, Endpoint};
use tonic::{Code, Status};

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
    config: TransportConfig,
    channels: Mutex<BTreeMap<SocketAddr, Channel>>,
}

impl Peers {
    /// An empty pool.
    #[must_use]
    pub fn new(config: TransportConfig) -> Self {
        Self {
            config,
            channels: Mutex::new(BTreeMap::new()),
        }
    }

    /// The channel to `peer`. It connects lazily, on the first call.
    ///
    /// # Panics
    ///
    /// If the pool's lock was poisoned by a panic in another thread.
    #[must_use]
    pub fn channel(&self, peer: SocketAddr) -> Channel {
        let mut channels = self.channels.lock().expect("peer pool lock poisoned");
        channels
            .entry(peer)
            .or_insert_with(|| {
                Endpoint::from(http_uri(peer))
                    .connect_timeout(self.config.connect_timeout)
                    .timeout(self.config.request_timeout)
                    .connect_lazy()
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

fn http_uri(peer: SocketAddr) -> tonic::codegen::http::Uri {
    let uri = format!("http://{peer}");
    uri.parse()
        .unwrap_or_else(|error| panic!("'{uri}' is a valid URI for any socket address: {error}"))
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

/// The transport over turmoil's simulated network (ADR-0020).
#[cfg(feature = "simulation")]
pub mod sim {
    use std::io;
    use std::pin::Pin;
    use std::task::{Context, Poll};

    use hyper_util::rt::TokioIo;
    use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
    use tokio_stream::Stream;
    use tonic::transport::server::Connected;
    use tonic::transport::{Channel, Endpoint};

    /// A simulated TCP connection that tonic's server accepts.
    #[derive(Debug)]
    pub struct SimStream(turmoil::net::TcpStream);

    impl Connected for SimStream {
        type ConnectInfo = ();

        fn connect_info(&self) -> Self::ConnectInfo {}
    }

    impl AsyncRead for SimStream {
        fn poll_read(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            Pin::new(&mut self.0).poll_read(cx, buf)
        }
    }

    impl AsyncWrite for SimStream {
        fn poll_write(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            Pin::new(&mut self.0).poll_write(cx, buf)
        }

        fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Pin::new(&mut self.0).poll_flush(cx)
        }

        fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Pin::new(&mut self.0).poll_shutdown(cx)
        }
    }

    /// The connections accepted by a simulated listener, for `serve_with_incoming`.
    pub fn incoming(
        listener: turmoil::net::TcpListener,
    ) -> impl Stream<Item = io::Result<SimStream>> {
        tokio_stream::StreamExt::map(
            accept_loop(listener),
            |accepted: io::Result<turmoil::net::TcpStream>| accepted.map(SimStream),
        )
    }

    fn accept_loop(
        listener: turmoil::net::TcpListener,
    ) -> impl Stream<Item = io::Result<turmoil::net::TcpStream>> {
        let (sender, receiver) = tokio::sync::mpsc::channel(16);
        tokio::spawn(async move {
            loop {
                let accepted = listener.accept().await.map(|(stream, _peer)| stream);
                if sender.send(accepted).await.is_err() {
                    break;
                }
            }
        });
        tokio_stream::wrappers::ReceiverStream::new(receiver)
    }

    /// A channel to `host:port` on the simulated network.
    ///
    /// # Errors
    ///
    /// The transport error if the endpoint can't be built.
    pub fn channel(host: &'static str, port: u16) -> Result<Channel, tonic::transport::Error> {
        Ok(
            Endpoint::try_from("http://simulated")?.connect_with_connector_lazy(tower::service_fn(
                move |_uri| async move {
                    let stream = turmoil::net::TcpStream::connect((host, port)).await?;
                    Ok::<_, io::Error>(TokioIo::new(stream))
                },
            )),
        )
    }

    /// A channel to `address`, given as `host:port`, on the simulated network.
    ///
    /// Its signature fits [`ControlClient::with_connector`](crate::control::ControlClient::with_connector),
    /// so a control client can follow redirects between simulated hosts.
    ///
    /// # Errors
    ///
    /// The transport error if the endpoint can't be built.
    pub fn channel_to(address: &str) -> Result<Channel, tonic::transport::Error> {
        let address = address.to_owned();
        Ok(
            Endpoint::try_from("http://simulated")?.connect_with_connector_lazy(tower::service_fn(
                move |_uri| {
                    let address = address.clone();
                    async move {
                        let stream = turmoil::net::TcpStream::connect(address.as_str()).await?;
                        Ok::<_, io::Error>(TokioIo::new(stream))
                    }
                },
            )),
        )
    }
}
