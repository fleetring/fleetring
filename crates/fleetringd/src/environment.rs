//! What a daemon takes from the world it runs in: its network, its clock, and where its blocking
//! storage calls run (ADR-0020).
//!
//! [`Daemon::start`](crate::Daemon::start) runs in [`Environment::production`]: real TCP, the
//! system clock, and storage on dedicated threads. The simulation harness passes
//! [`Environment::simulated`] to [`Daemon::start_with`](crate::Daemon::start_with), so the same
//! daemon code runs over turmoil's network and clock, on one deterministic thread.

use std::sync::Arc;
use std::time::Duration;

use tonic::transport::{Channel, Endpoint};

use crate::clock::{Clock, SystemClock};
use crate::control::{ControlClient, StorageIo};
use crate::engine::Execution;
use crate::transport::TransportConfig;

/// The network a daemon listens on and dials its peers over.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Network {
    /// Real TCP, with these timeouts for outgoing connections.
    Tcp(TransportConfig),
    /// turmoil's simulated network. The daemon must run inside a turmoil host.
    #[cfg(feature = "simulation")]
    Simulated,
}

/// How much longer than the longest wait inside a call a daemon's own request deadline runs, so
/// the callee answers before the caller gives up.
const REQUEST_MARGIN: Duration = Duration::from_secs(5);

impl Network {
    /// Returns this network with its request deadline raised, if needed, to outlast `wait` by a
    /// margin.
    ///
    /// Some calls between daemons wait inside the callee for up to the acknowledgement window: a
    /// lease transfer waits for the target to catch up, and a create waits for followers to
    /// connect. A deadline shorter than that window cancels the call midway, and the cancellation
    /// reaches the client as `Timeout expired`. The simulated network has no deadline.
    #[must_use]
    pub fn outlasting(self, wait: Duration) -> Self {
        match self {
            Self::Tcp(mut transport) => {
                transport.request_timeout = transport
                    .request_timeout
                    .max(wait.saturating_add(REQUEST_MARGIN));
                Self::Tcp(transport)
            }
            #[cfg(feature = "simulation")]
            Self::Simulated => self,
        }
    }

    /// Opens a lazily connecting channel to `address`, given as `host:port`.
    ///
    /// # Errors
    ///
    /// The transport error if `address` doesn't form a valid endpoint.
    pub fn channel(&self, address: &str) -> Result<Channel, tonic::transport::Error> {
        match *self {
            Self::Tcp(transport) => Ok(Endpoint::from_shared(format!("http://{address}"))?
                .connect_timeout(transport.connect_timeout)
                .timeout(transport.request_timeout)
                .connect_lazy()),
            #[cfg(feature = "simulation")]
            Self::Simulated => crate::transport::sim::channel_to(address),
        }
    }

    /// A control client that starts at `address` and dials every leader over this network.
    #[must_use]
    pub fn control_client(&self, address: impl Into<String>) -> ControlClient {
        match *self {
            Self::Tcp(transport) => ControlClient::new(address, transport),
            #[cfg(feature = "simulation")]
            Self::Simulated => {
                ControlClient::with_connector(address, crate::transport::sim::channel_to)
            }
        }
    }
}

impl Default for Network {
    fn default() -> Self {
        Self::Tcp(TransportConfig::default())
    }
}

/// The network, clock, and storage execution a daemon runs with.
///
/// Cloning is cheap; clones share the clock.
#[derive(Debug, Clone)]
pub struct Environment {
    network: Network,
    clock: Arc<dyn Clock>,
    execution: Execution,
}

impl Environment {
    /// Real TCP with default timeouts, the system clock, and storage on dedicated threads.
    #[must_use]
    pub fn production() -> Self {
        Self {
            network: Network::default(),
            clock: Arc::new(SystemClock),
            execution: Execution::Threads,
        }
    }

    /// turmoil's network, `clock`, and every storage call inline on the host's task.
    ///
    /// Build it inside the turmoil host, so that a clock which follows `tokio::time` starts from
    /// the host's simulated time.
    #[cfg(feature = "simulation")]
    #[must_use]
    pub fn simulated(clock: impl Clock + 'static) -> Self {
        Self {
            network: Network::Simulated,
            clock: Arc::new(clock),
            execution: Execution::Inline,
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

    /// Returns the network.
    #[must_use]
    pub fn network(&self) -> Network {
        self.network
    }

    /// Returns the clock.
    #[must_use]
    pub fn clock(&self) -> Arc<dyn Clock> {
        Arc::clone(&self.clock)
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

    fn deadline(network: Network) -> Duration {
        match network {
            Network::Tcp(transport) => transport.request_timeout,
            #[cfg(feature = "simulation")]
            Network::Simulated => Duration::ZERO,
        }
    }

    #[test]
    fn the_request_deadline_outlasts_the_longest_wait_inside_a_call() {
        let network = Network::default();
        let default = deadline(network);
        assert_eq!(
            deadline(network.outlasting(Duration::from_secs(1))),
            default
        );
        assert_eq!(
            deadline(network.outlasting(Duration::from_secs(20))),
            Duration::from_secs(25)
        );
    }
}
