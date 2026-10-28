//! The data nodes' client for the control plane: retries, and follows leader redirects.

use std::collections::BTreeMap;
use std::fmt;
use std::future::Future;
use std::sync::{Arc, Mutex};

use fleetring_proto::v1::control_client::ControlClient as GrpcControlClient;
use fleetring_proto::v1::{
    AcquireLeaseRequest, CreateDatabaseRequest, CreateDatabaseResponse, HeartbeatRequest,
    HeartbeatResponse, JoinRequest, JoinResponse, LeaseResponse, LocateRequest, LocateResponse,
    MembershipUpdate, RenewLeaseRequest, WatchMembershipRequest,
};
use tonic::transport::{Channel, Endpoint};
use tonic::{Response, Status, Streaming};

use crate::control::redirect::leader_hint;
use crate::transport::{Backoff, TransportConfig, control_client, retry};

/// How many redirects one attempt follows before it gives up and lets retries take over.
const MAX_REDIRECTS: usize = 4;

type Connect = dyn Fn(&str) -> Result<Channel, tonic::transport::Error> + Send + Sync;

/// A `Control` client that finds the leader on its own.
///
/// - **Redirects:** a reply of "not the leader, try X" (see [`not_leader`](super::not_leader))
///   switches the client to X and repeats the call there, so callers never see it. Later calls
///   go straight to X.
/// - **Retries:** idempotent calls (`join`, `heartbeat`, `locate`, `renew_lease`, and opening
///   `watch_membership`) retry transient failures with [`retry`]. `create_database` and
///   `acquire_lease` are sent once per leader, because a lost reply would make a retry fail with
///   "already exists".
///
/// Cloning is cheap; clones share the current leader and connections.
#[derive(Clone)]
pub struct ControlClient {
    shared: Arc<Shared>,
    backoff: Backoff,
}

struct Shared {
    connect: Box<Connect>,
    leader: Mutex<String>,
    channels: Mutex<BTreeMap<String, Channel>>,
}

impl fmt::Debug for ControlClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ControlClient")
            .field("leader", &self.leader())
            .field("backoff", &self.backoff)
            .finish_non_exhaustive()
    }
}

impl ControlClient {
    /// A client that reaches the control node at `address` (`host:port`) over TCP.
    #[must_use]
    pub fn new(address: impl Into<String>, transport: TransportConfig) -> Self {
        Self::with_connector(address, move |address| {
            Ok(Endpoint::from_shared(format!("http://{address}"))?
                .connect_timeout(transport.connect_timeout)
                .timeout(transport.request_timeout)
                .connect_lazy())
        })
    }

    /// A client that opens channels with `connect`, starting at `address`.
    ///
    /// `connect` receives the `host:port` to reach: the starting address, or a leader a redirect
    /// named. Use it to run the client over a simulated network.
    #[must_use]
    pub fn with_connector(
        address: impl Into<String>,
        connect: impl Fn(&str) -> Result<Channel, tonic::transport::Error> + Send + Sync + 'static,
    ) -> Self {
        Self {
            shared: Arc::new(Shared {
                connect: Box::new(connect),
                leader: Mutex::new(address.into()),
                channels: Mutex::new(BTreeMap::new()),
            }),
            backoff: Backoff::default(),
        }
    }

    /// Retries idempotent calls with `backoff` instead of the default.
    #[must_use]
    pub fn with_backoff(mut self, backoff: Backoff) -> Self {
        self.backoff = backoff;
        self
    }

    /// Returns the address the client currently sends calls to.
    ///
    /// # Panics
    ///
    /// If the client's lock was poisoned by a panic in another thread.
    #[must_use]
    pub fn leader(&self) -> String {
        self.shared
            .leader
            .lock()
            .expect("control client lock poisoned")
            .clone()
    }

    /// Drops every connection the client holds, so the next call dials again.
    ///
    /// Use it when calls time out without failing: a connection can stay open while nothing
    /// crosses it, for example after a network partition or a machine's sleep. Streams already
    /// open keep their connection.
    ///
    /// # Panics
    ///
    /// If the client's lock was poisoned by a panic in another thread.
    pub fn reconnect(&self) {
        self.shared
            .channels
            .lock()
            .expect("control client lock poisoned")
            .clear();
    }

    /// Joins the cluster. Retried.
    ///
    /// # Errors
    ///
    /// The status of the last attempt.
    pub async fn join(&self, request: JoinRequest) -> Result<JoinResponse, Status> {
        self.retrying(|mut client| {
            let request = request.clone();
            async move { client.join(request).await }
        })
        .await
    }

    /// Sends a heartbeat. Retried.
    ///
    /// # Errors
    ///
    /// The status of the last attempt.
    pub async fn heartbeat(&self, request: HeartbeatRequest) -> Result<HeartbeatResponse, Status> {
        self.retrying(|mut client| {
            let request = request.clone();
            async move { client.heartbeat(request).await }
        })
        .await
    }

    /// Opens a stream of membership changes after `request.after_epoch`. Opening is retried; a
    /// broken stream is the caller's to reopen.
    ///
    /// # Errors
    ///
    /// The status of the last attempt.
    pub async fn watch_membership(
        &self,
        request: WatchMembershipRequest,
    ) -> Result<Streaming<MembershipUpdate>, Status> {
        self.retrying(|mut client| async move { client.watch_membership(request).await })
            .await
    }

    /// Creates a database. Sent once per leader, not retried.
    ///
    /// # Errors
    ///
    /// The status the control node returned, or `UNAVAILABLE` if it couldn't be reached.
    pub async fn create_database(
        &self,
        request: CreateDatabaseRequest,
    ) -> Result<CreateDatabaseResponse, Status> {
        self.following(&|mut client: GrpcControlClient<Channel>| {
            let request = request.clone();
            async move { client.create_database(request).await }
        })
        .await
    }

    /// Finds where a database lives. Retried.
    ///
    /// # Errors
    ///
    /// The status of the last attempt.
    pub async fn locate(&self, request: LocateRequest) -> Result<LocateResponse, Status> {
        self.retrying(|mut client| {
            let request = request.clone();
            async move { client.locate(request).await }
        })
        .await
    }

    /// Takes a database's first lease. Sent once per leader, not retried.
    ///
    /// # Errors
    ///
    /// The status the control node returned, or `UNAVAILABLE` if it couldn't be reached.
    pub async fn acquire_lease(
        &self,
        request: AcquireLeaseRequest,
    ) -> Result<LeaseResponse, Status> {
        self.following(&|mut client: GrpcControlClient<Channel>| {
            let request = request.clone();
            async move { client.acquire_lease(request).await }
        })
        .await
    }

    /// Renews a lease. Retried: renewing twice only extends the lease from a later time.
    ///
    /// # Errors
    ///
    /// The status of the last attempt.
    pub async fn renew_lease(&self, request: RenewLeaseRequest) -> Result<LeaseResponse, Status> {
        self.retrying(|mut client| {
            let request = request.clone();
            async move { client.renew_lease(request).await }
        })
        .await
    }

    async fn retrying<T, F, Fut>(&self, call: F) -> Result<T, Status>
    where
        F: Fn(GrpcControlClient<Channel>) -> Fut,
        Fut: Future<Output = Result<Response<T>, Status>>,
    {
        retry(self.backoff, || self.following(&call)).await
    }

    /// Calls the current leader, and follows up to [`MAX_REDIRECTS`] redirects.
    async fn following<T, F, Fut>(&self, call: &F) -> Result<T, Status>
    where
        F: Fn(GrpcControlClient<Channel>) -> Fut,
        Fut: Future<Output = Result<Response<T>, Status>>,
    {
        let mut redirects = 0;
        loop {
            match call(self.current()?).await {
                Ok(response) => return Ok(response.into_inner()),
                Err(status) => match leader_hint(&status) {
                    Some(leader) if redirects < MAX_REDIRECTS => {
                        tracing::debug!(%leader, "following a control leader redirect");
                        self.set_leader(leader);
                        redirects += 1;
                    }
                    _ => return Err(status),
                },
            }
        }
    }

    fn current(&self) -> Result<GrpcControlClient<Channel>, Status> {
        let leader = self.leader();
        let mut channels = self
            .shared
            .channels
            .lock()
            .expect("control client lock poisoned");
        if let Some(channel) = channels.get(&leader) {
            return Ok(control_client(channel.clone()));
        }
        let channel = (self.shared.connect)(&leader).map_err(|error| {
            Status::unavailable(format!("can't reach control node {leader}: {error}"))
        })?;
        channels.insert(leader, channel.clone());
        Ok(control_client(channel))
    }

    fn set_leader(&self, leader: String) {
        *self
            .shared
            .leader
            .lock()
            .expect("control client lock poisoned") = leader;
    }
}
