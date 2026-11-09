//! What a daemon takes from the world it runs in: its network, its clock, its randomness, and
//! where its blocking storage calls run (ADR-0020).
//!
//! [`Daemon::start`](crate::Daemon::start) runs in [`Environment::production`]: real TCP, the
//! system clock, and storage on dedicated threads. The simulation harness passes
//! [`Environment::simulated`] to [`Daemon::start_with`](crate::Daemon::start_with), so the same
//! daemon code runs over turmoil's network and clock, on one deterministic thread.

use std::collections::BTreeMap;
use std::io;
use std::sync::Arc;
use std::time::Duration;

use hyper_util::rt::TokioIo;
use tonic::transport::{Channel, Endpoint};

use crate::clock::{Clock, SystemClock};
use crate::control::{ControlClient, StorageIo};
use crate::data::DROP_FLUSH_LIMIT;
use crate::durability::ObjectStore;
use crate::engine::Execution;
use crate::router::{Entropy, SystemEntropy};
use crate::tls::{Conn, Raw, Security};
use crate::transport::{Credentials, TransportConfig};

/// How a daemon reaches its peers: real TCP, or turmoil's simulated network.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Link {
    /// Real TCP, with these timeouts for outgoing connections.
    Tcp(TransportConfig),
    /// turmoil's simulated network. The daemon must run inside a turmoil host.
    #[cfg(feature = "simulation")]
    Simulated,
}

/// How much longer than the longest wait inside a call a daemon's own request deadline runs, so
/// the callee answers before the caller gives up.
const REQUEST_MARGIN: Duration = Duration::from_secs(5);

impl Link {
    async fn connect(self, address: &str) -> io::Result<Raw> {
        match self {
            Self::Tcp(_) => {
                let stream = tokio::net::TcpStream::connect(address).await?;
                stream.set_nodelay(true)?;
                Ok(Raw::Tcp(stream))
            }
            #[cfg(feature = "simulation")]
            Self::Simulated => Ok(Raw::Simulated(
                turmoil::net::TcpStream::connect(address).await?,
            )),
        }
    }
}

/// The network a daemon listens on and dials its peers over, and the identity its connections
/// carry.
///
/// Cloning is cheap; clones share the identity, so they reload certificates together.
#[derive(Debug, Clone)]
pub struct Network {
    link: Link,
    security: Security,
    credentials: Credentials,
    keep_alive: Option<KeepAlive>,
}

/// HTTP/2 keep-alive on a network's channels: while a call is open, the connection is pinged
/// every `interval`, and closed if a ping goes `timeout` unanswered, so that a call on a peer that
/// went away fails instead of waiting on a connection nothing will ever answer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct KeepAlive {
    /// How often an open connection is pinged.
    pub interval: Duration,
    /// How long a ping may go unanswered.
    pub timeout: Duration,
}

impl Network {
    /// Real TCP with `transport`'s timeouts, in plaintext until
    /// [`with_security`](Self::with_security) gives it an identity.
    #[must_use]
    pub fn tcp(transport: TransportConfig) -> Self {
        Self {
            link: Link::Tcp(transport),
            security: Security::Plaintext,
            credentials: Credentials::none(),
            keep_alive: None,
        }
    }

    /// turmoil's simulated network, in plaintext until [`with_security`](Self::with_security)
    /// gives it an identity. The daemon must run inside a turmoil host.
    #[cfg(feature = "simulation")]
    #[must_use]
    pub fn simulated() -> Self {
        Self {
            link: Link::Simulated,
            security: Security::Plaintext,
            credentials: Credentials::none(),
            keep_alive: None,
        }
    }

    /// Carries `security` on every connection: an identity, or plaintext.
    #[must_use]
    pub fn with_security(mut self, security: Security) -> Self {
        self.security = security;
        self
    }

    /// Pings this network's channels as `keep_alive` says while a call is open on them.
    #[must_use]
    pub fn with_keep_alive(mut self, keep_alive: KeepAlive) -> Self {
        self.keep_alive = Some(keep_alive);
        self
    }

    /// Sends `credentials`, such as a bearer token, with every request a client built from this
    /// network makes. Nodes send none.
    #[must_use]
    pub fn with_credentials(mut self, credentials: Credentials) -> Self {
        self.credentials = credentials;
        self
    }

    /// Returns the credentials clients' requests carry.
    #[must_use]
    pub fn credentials(&self) -> &Credentials {
        &self.credentials
    }

    /// Returns the identity connections carry, or plaintext.
    #[must_use]
    pub fn security(&self) -> &Security {
        &self.security
    }

    /// Returns whether the network is real TCP.
    #[must_use]
    pub fn is_tcp(&self) -> bool {
        matches!(self.link, Link::Tcp(_))
    }

    /// Returns the timeouts for outgoing connections over TCP; none under simulation, where
    /// every caller bounds its own calls.
    #[must_use]
    pub fn transport(&self) -> Option<TransportConfig> {
        match self.link {
            Link::Tcp(transport) => Some(transport),
            #[cfg(feature = "simulation")]
            Link::Simulated => None,
        }
    }

    /// Returns this network with its request deadline raised, if needed, to outlast `wait` by a
    /// margin.
    ///
    /// Some calls between daemons wait inside the callee for up to the acknowledgement window: a
    /// lease transfer waits for the target to catch up, and a create waits for followers to
    /// connect. A deadline shorter than that window cancels the call midway, and the cancellation
    /// reaches the client as `Timeout expired`. The simulated network has no deadline.
    #[must_use]
    pub fn outlasting(mut self, wait: Duration) -> Self {
        match &mut self.link {
            Link::Tcp(transport) => {
                transport.request_timeout = transport
                    .request_timeout
                    .max(wait.saturating_add(REQUEST_MARGIN));
            }
            #[cfg(feature = "simulation")]
            Link::Simulated => {}
        }
        self
    }

    /// Opens a lazily connecting channel to `address`, given as `host:port`: a connection on
    /// this network, with the TLS handshake on top when the network has an identity.
    ///
    /// # Errors
    ///
    /// The transport error if `address` doesn't form a valid endpoint.
    pub fn channel(&self, address: &str) -> Result<Channel, tonic::transport::Error> {
        let mut endpoint = Endpoint::from_shared(format!("http://{address}"))?;
        if let Some(transport) = self.transport() {
            endpoint = endpoint
                .connect_timeout(transport.connect_timeout)
                .timeout(transport.request_timeout);
        }
        if let Some(keep_alive) = self.keep_alive {
            endpoint = endpoint
                .http2_keep_alive_interval(keep_alive.interval)
                .keep_alive_timeout(keep_alive.timeout);
        }
        let link = self.link;
        let security = self.security.clone();
        let address = address.to_owned();
        let connector = tower::service_fn(move |_uri| {
            let security = security.clone();
            let address = address.clone();
            async move {
                let raw = link.connect(&address).await?;
                let conn: Conn = security.connect(&address, raw).await?;
                Ok::<_, io::Error>(TokioIo::new(conn))
            }
        });
        Ok(endpoint.connect_with_connector_lazy(connector))
    }

    /// A control client that starts at `address` and dials every leader over this network.
    #[must_use]
    pub fn control_client(&self, address: impl Into<String>) -> ControlClient {
        let network = self.clone();
        let credentials = self.credentials.clone();
        ControlClient::with_connector(address, move |address| network.channel(address))
            .with_credentials(credentials)
    }
}

impl Default for Network {
    /// Real TCP with default timeouts, in plaintext.
    fn default() -> Self {
        Self::tcp(TransportConfig::default())
    }
}

/// The network, clock, randomness, and storage execution a daemon runs with.
///
/// Cloning is cheap; clones share the clock and the randomness.
#[derive(Debug, Clone)]
pub struct Environment {
    network: Network,
    clock: Arc<dyn Clock>,
    /// Where the control node draws database incarnations from (ADR-0046, point 1.1).
    entropy: Arc<dyn Entropy>,
    execution: Execution,
    /// Object stores that replace the ones `[[archive]]` entries name, by archive name.
    object_stores: BTreeMap<String, Arc<dyn ObjectStore>>,
    /// How long a dropped database's change streams have to flush before its copy is deleted.
    drop_flush_limit: Duration,
}

impl Environment {
    /// Real TCP with default timeouts, the system clock, and storage on dedicated threads.
    #[must_use]
    pub fn production() -> Self {
        Self {
            network: Network::default(),
            clock: Arc::new(SystemClock),
            entropy: Arc::new(SystemEntropy::new()),
            execution: Execution::Threads,
            object_stores: BTreeMap::new(),
            drop_flush_limit: DROP_FLUSH_LIMIT,
        }
    }

    /// turmoil's network, `clock`, and every storage call inline on the host's task. Its
    /// randomness is the operating system's until [`Environment::with_entropy`] replaces it, as
    /// the simulation harness does with a seeded generator.
    ///
    /// Build it inside the turmoil host, so that a clock which follows `tokio::time` starts from
    /// the host's simulated time.
    #[cfg(feature = "simulation")]
    #[must_use]
    pub fn simulated(clock: impl Clock + 'static) -> Self {
        Self {
            network: Network::simulated(),
            clock: Arc::new(clock),
            entropy: Arc::new(SystemEntropy::new()),
            execution: Execution::Inline,
            object_stores: BTreeMap::new(),
            drop_flush_limit: DROP_FLUSH_LIMIT,
        }
    }

    /// Uses `network` for the listener and for every outgoing connection.
    #[must_use]
    pub fn with_network(mut self, network: Network) -> Self {
        self.network = network;
        self
    }

    /// Stamps control commands with `clock`.
    #[must_use]
    pub fn with_clock(mut self, clock: impl Clock + 'static) -> Self {
        self.clock = Arc::new(clock);
        self
    }

    /// Draws randomness from `entropy`: a simulation's seeded generator, so that a seed replays
    /// the same incarnations (ADR-0020).
    #[must_use]
    pub fn with_entropy(mut self, entropy: Arc<dyn Entropy>) -> Self {
        self.entropy = entropy;
        self
    }

    /// Runs blocking storage calls as `execution` says.
    ///
    /// [`Execution::Threads`] runs the control log on its own thread and leaves the engine to
    /// `[engine] execution`. [`Execution::Inline`] runs both inline, whatever the configuration
    /// says, as a simulation needs.
    #[must_use]
    pub fn with_execution(mut self, execution: Execution) -> Self {
        self.execution = execution;
        self
    }

    /// Writes the archive named `archive` to `store`, whatever store its `[[archive]]` entry
    /// names: a simulation's one store that every host shares and that outlives them (ADR-0029,
    /// point 16).
    #[must_use]
    pub fn with_object_store(
        mut self,
        archive: impl Into<String>,
        store: Arc<dyn ObjectStore>,
    ) -> Self {
        self.object_stores.insert(archive.into(), store);
        self
    }

    /// Gives a dropped database's change streams `limit`, instead of 30 seconds, to deliver
    /// what its copy acknowledged before the primary deletes the copy (ADR-0041, point 4): for
    /// tests that run out the limit.
    #[must_use]
    pub fn with_drop_flush_limit(mut self, limit: Duration) -> Self {
        self.drop_flush_limit = limit;
        self
    }

    /// How long a dropped database's change streams have to flush before its copy is deleted.
    #[must_use]
    pub fn drop_flush_limit(&self) -> Duration {
        self.drop_flush_limit
    }

    /// The store that replaces the one the archive named `archive` names, if any.
    #[must_use]
    pub fn object_store(&self, archive: &str) -> Option<Arc<dyn ObjectStore>> {
        self.object_stores.get(archive).cloned()
    }

    /// Returns the network.
    #[must_use]
    pub fn network(&self) -> &Network {
        &self.network
    }

    /// Returns the clock.
    #[must_use]
    pub fn clock(&self) -> Arc<dyn Clock> {
        Arc::clone(&self.clock)
    }

    /// Returns where randomness comes from.
    #[must_use]
    pub fn entropy(&self) -> Arc<dyn Entropy> {
        Arc::clone(&self.entropy)
    }

    /// Returns where blocking storage calls run.
    #[must_use]
    pub fn execution(&self) -> Execution {
        self.execution
    }

    /// Where the control node runs its SQLite calls.
    pub(crate) fn control_storage_io(&self) -> StorageIo {
        match self.execution {
            Execution::Threads => StorageIo::DedicatedThread,
            Execution::Inline => StorageIo::Inline,
        }
    }
}

impl Default for Environment {
    fn default() -> Self {
        Self::production()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn deadline(network: &Network) -> Duration {
        network
            .transport()
            .map_or(Duration::ZERO, |transport| transport.request_timeout)
    }

    #[test]
    fn the_request_deadline_outlasts_the_longest_wait_inside_a_call() {
        let network = Network::default();
        let default = deadline(&network);
        assert_eq!(
            deadline(&network.clone().outlasting(Duration::from_secs(1))),
            default
        );
        assert_eq!(
            deadline(&network.outlasting(Duration::from_secs(20))),
            Duration::from_secs(25)
        );
    }
}
