//! The data nodes' client for the control plane: retries, and follows leader redirects.

use std::collections::BTreeMap;
use std::fmt;
use std::future::Future;
use std::sync::{Arc, Mutex};

use fleetring_client::redirect::leader_hint;
use fleetring_proto::v1::control_client::ControlClient as GrpcControlClient;
use fleetring_proto::v1::{
    AcquireLeaseRequest, ChangeDurabilityRequest, ChangeDurabilityResponse,
    CompletePromotionRequest, CompletePromotionResponse, ControlNode, CreateDatabaseRequest,
    CreateDatabaseResponse, CreateDatabaseWithMetadataRequest, DelegateTokenRequest,
    DelegateTokenResponse, DrainRequest, DrainResponse, DropDatabaseRequest, DropDatabaseResponse,
    ForkDatabaseRequest, ForkOperationStatus, GetForkOperationRequest, HeartbeatRequest,
    HeartbeatResponse, JoinRequest, JoinResponse, LeaseResponse, LeaveRequest, LeaveResponse,
    ListDatabasesRequest, ListDatabasesResponse, ListDelegationsRequest, ListDelegationsResponse,
    LocateRequest, LocateResponse, MembershipUpdate, PromotionResponse, RecordFencedRequest,
    RecordFencedResponse, RenewLeaseRequest, RevokeTokenRequest, RevokeTokenResponse,
    StartPromotionRequest, TransferLeaseRequest, UndrainRequest, UndrainResponse,
    UpdateDatabaseRequest, UpdateDatabaseResponse, WatchMembershipRequest,
};
use tonic::transport::{Channel, Endpoint};
use tonic::{Code, Response, Status, Streaming};

use crate::transport::{
    Backoff, CredentialedChannel, Credentials, TransportConfig, control_client_with, retry,
};

/// How many redirects one attempt follows before it gives up and lets retries take over.
const MAX_REDIRECTS: usize = 4;

type Connect = dyn Fn(&str) -> Result<Channel, tonic::transport::Error> + Send + Sync;

/// A `Control` client that finds the leader on its own.
///
/// - **Redirects:** a reply of "not the leader, try X" (see
///   [`not_leader`](fleetring_client::redirect::not_leader)) switches the client to X and repeats
///   the call there, so callers never see it. Later calls go straight to X.
/// - **Retries:** idempotent calls (`join`, `heartbeat`, `locate`, `list_databases`,
///   `renew_lease`, `record_fenced`, `drain`, `fork_database`, whose operation ID makes a resend
///   return the same operation, `get_fork_operation`, `list_delegations`, and opening
///   `watch_membership`) retry transient failures with [`retry`]. `delegate_token` resends on
///   `UNAVAILABLE` or a lost reply alone, under the same operation ID, so a resend returns the delegation the
///   first attempt recorded. `create_database`, `create_database_with_metadata`,
///   `update_database`, `drop_database`, `acquire_lease`, `leave`, `start_promotion`,
///   `complete_promotion`, and `transfer_lease` are sent once per leader, because a lost reply
///   would make a retry fail as already done, or apply a patch twice.
///
/// Cloning is cheap; clones share the current leader and connections.
#[derive(Clone)]
pub struct ControlClient {
    shared: Arc<Shared>,
    backoff: Backoff,
}

struct Shared {
    connect: Box<Connect>,
    credentials: Credentials,
    addresses: Mutex<Addresses>,
    channels: Mutex<BTreeMap<String, Channel>>,
}

/// Where the client sends calls: the addresses it was configured with, those the control group's
/// replies named, and the one it uses now.
#[derive(Debug, Clone, Default)]
struct Addresses {
    configured: Vec<String>,
    learned: Vec<String>,
    current: String,
}

impl Addresses {
    fn new(configured: Vec<String>) -> Self {
        Self {
            current: configured.first().cloned().unwrap_or_default(),
            configured,
            learned: Vec::new(),
        }
    }

    /// Every address to try, the configured ones first, then the learned ones, without repeats.
    fn candidates(&self) -> Vec<String> {
        let mut candidates: Vec<String> = Vec::new();
        for address in self.configured.iter().chain(&self.learned) {
            if !candidates.contains(address) {
                candidates.push(address.clone());
            }
        }
        candidates
    }

    /// Moves on from `failed` to the next candidate, if it is still the current address.
    fn advance(&mut self, failed: &str) {
        if self.current != failed {
            return;
        }
        let candidates = self.candidates();
        let next = match candidates.iter().position(|address| address == failed) {
            Some(at) => candidates.get((at + 1) % candidates.len()),
            None => candidates.first(),
        };
        if let Some(next) = next {
            self.current.clone_from(next);
        }
    }
}

/// What a call does when an address answers `UNAVAILABLE` without naming a leader.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Unavailable {
    /// Try the next address, until every one has been tried: for calls a resend can't harm.
    NextAddress,
    /// Return the status; the next call starts at the next address.
    Return,
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
        Self::with_addresses(vec![address.into()], transport)
    }

    /// A client that tries the control group's `addresses` (`host:port`) in order over TCP.
    #[must_use]
    pub fn with_addresses(addresses: Vec<String>, transport: TransportConfig) -> Self {
        Self::with_connector_at(addresses, move |address| {
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
        Self::with_connector_at(vec![address.into()], connect)
    }

    /// A client that opens channels with `connect`, trying `addresses` in order (ADR-0040 point
    /// 12): an address that answers `UNAVAILABLE` without naming a leader moves the client to the
    /// next, and the control group's members, learned from join and heartbeat replies, join the
    /// list.
    #[must_use]
    pub fn with_connector_at(
        addresses: Vec<String>,
        connect: impl Fn(&str) -> Result<Channel, tonic::transport::Error> + Send + Sync + 'static,
    ) -> Self {
        Self {
            shared: Arc::new(Shared {
                connect: Box::new(connect),
                credentials: Credentials::none(),
                addresses: Mutex::new(Addresses::new(addresses)),
                channels: Mutex::new(BTreeMap::new()),
            }),
            backoff: Backoff::default(),
        }
    }

    /// Sends `credentials`, such as a bearer token, with every call.
    ///
    /// # Panics
    ///
    /// If the client is already shared with a clone; set credentials before cloning.
    #[must_use]
    pub fn with_credentials(mut self, credentials: Credentials) -> Self {
        Arc::get_mut(&mut self.shared)
            .expect("credentials are set before the client is cloned")
            .credentials = credentials;
        self
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
        self.addresses().current.clone()
    }

    fn addresses(&self) -> std::sync::MutexGuard<'_, Addresses> {
        self.shared
            .addresses
            .lock()
            .expect("control client lock poisoned")
    }

    /// Returns every address the client may try, in order: the configured ones, then those it
    /// learned.
    ///
    /// # Panics
    ///
    /// If the client's lock was poisoned by a panic in another thread.
    #[must_use]
    pub fn addresses_to_try(&self) -> Vec<String> {
        self.addresses().candidates()
    }

    /// Learns the control group's members from a join or heartbeat reply (ADR-0040 point 12):
    /// their addresses join the ones the client tries once an address fails, the leader's first.
    /// The client keeps calling the address that answers: a member's advertised address may not
    /// be one this node reaches it at, behind a proxy or after it moved, and a redirect already
    /// finds the leader. An empty list, from a control node that predates the field, changes
    /// nothing.
    ///
    /// # Panics
    ///
    /// If the client's lock was poisoned by a panic in another thread.
    pub fn learn(&self, nodes: &[ControlNode]) {
        let leaders = nodes.iter().filter(|node| node.leader);
        let others = nodes.iter().filter(|node| !node.leader);
        let learned: Vec<String> = leaders
            .chain(others)
            .map(|node| node.address.clone())
            .filter(|address| !address.is_empty())
            .collect();
        if learned.is_empty() {
            return;
        }
        self.addresses().learned = learned;
    }

    /// Moves the client on from the address it calls now to the next one it may try: for a caller
    /// whose own deadline ended a call before the address answered, as a hung control node, or one
    /// behind a partition that drops packets, makes it do. A redirect brings the client back if
    /// that address still leads.
    ///
    /// # Panics
    ///
    /// If the client's lock was poisoned by a panic in another thread.
    pub fn advance(&self) {
        let mut addresses = self.addresses();
        let current = addresses.current.clone();
        addresses.advance(&current);
    }

    /// Drops every connection the client holds, so the next call dials again, and moves on to
    /// the next address, as [`ControlClient::advance`] does.
    ///
    /// Use it when calls time out without failing: a connection can stay open while nothing
    /// crosses it, for example after a network partition or a machine's sleep, and the control
    /// node behind it may hang. Streams already open keep their connection.
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
        self.advance();
    }

    /// Joins the fleet. Retried.
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
        self.following(&|mut client: GrpcControlClient<CredentialedChannel>| {
            let request = request.clone();
            async move { client.create_database(request).await }
        })
        .await
    }

    /// Creates a database with a description, labels, or an expiry. Sent once per leader, not
    /// retried.
    ///
    /// # Errors
    ///
    /// The status the control node returned, or `UNAVAILABLE` if it couldn't be reached. A
    /// control node that predates the call refuses it.
    pub async fn create_database_with_metadata(
        &self,
        request: CreateDatabaseWithMetadataRequest,
    ) -> Result<CreateDatabaseResponse, Status> {
        self.following(&|mut client: GrpcControlClient<CredentialedChannel>| {
            let request = request.clone();
            async move { client.create_database_with_metadata(request).await }
        })
        .await
    }

    /// Changes a database's description, labels, or expiry. Sent once per leader, not retried: a
    /// retry after a lost reply would apply the patch again at the next version.
    ///
    /// # Errors
    ///
    /// The status the control node returned, or `UNAVAILABLE` if it couldn't be reached. A
    /// control node that predates the call refuses it.
    pub async fn update_database(
        &self,
        request: UpdateDatabaseRequest,
    ) -> Result<UpdateDatabaseResponse, Status> {
        self.following(&|mut client: GrpcControlClient<CredentialedChannel>| {
            let request = request.clone();
            async move { client.update_database(request).await }
        })
        .await
    }

    /// Records new durability settings for a database. Sent once per leader, not retried.
    ///
    /// # Errors
    ///
    /// The status the control node returned, or `UNAVAILABLE` if it couldn't be reached.
    pub async fn change_durability(
        &self,
        request: ChangeDurabilityRequest,
    ) -> Result<ChangeDurabilityResponse, Status> {
        self.following(&|mut client: GrpcControlClient<CredentialedChannel>| {
            let request = request.clone();
            async move { client.change_durability(request).await }
        })
        .await
    }

    /// Drops a database. Sent once per leader, not retried: a retry after a lost reply would fail
    /// with `NOT_FOUND`, which the caller can read as done.
    ///
    /// # Errors
    ///
    /// The status the control node returned, or `UNAVAILABLE` if it couldn't be reached.
    pub async fn drop_database(
        &self,
        request: DropDatabaseRequest,
    ) -> Result<DropDatabaseResponse, Status> {
        self.following(&|mut client: GrpcControlClient<CredentialedChannel>| {
            let request = request.clone();
            async move { client.drop_database(request).await }
        })
        .await
    }

    /// Forks a database (ADR-0046). Retried: the request's operation ID makes a resend, from the
    /// same principal with the same request, return the same operation and change nothing.
    ///
    /// # Errors
    ///
    /// The status of the last attempt. A control node that predates forks answers
    /// `UNIMPLEMENTED`, or refuses the call as one it doesn't serve.
    pub async fn fork_database(
        &self,
        request: ForkDatabaseRequest,
    ) -> Result<ForkOperationStatus, Status> {
        self.retrying(|mut client| {
            let request = request.clone();
            async move { client.fork_database(request).await }
        })
        .await
    }

    /// Reads a fork operation's status (ADR-0046, point 5.4). Retried.
    ///
    /// # Errors
    ///
    /// The status of the last attempt: `NOT_FOUND` for an operation that is unknown, past its
    /// retention, or not the caller's to see.
    pub async fn get_fork_operation(
        &self,
        request: GetForkOperationRequest,
    ) -> Result<ForkOperationStatus, Status> {
        self.retrying(|mut client| {
            let request = request.clone();
            async move { client.get_fork_operation(request).await }
        })
        .await
    }

    /// Lists every database's placement. Retried.
    ///
    /// # Errors
    ///
    /// The status of the last attempt.
    pub async fn list_databases(
        &self,
        request: ListDatabasesRequest,
    ) -> Result<ListDatabasesResponse, Status> {
        self.retrying(|mut client| {
            let request = request.clone();
            async move { client.list_databases(request).await }
        })
        .await
    }

    /// Reads the tombstones control state keeps of the named databases (ADR-0050). Retried.
    ///
    /// # Errors
    ///
    /// The status of the last attempt.
    pub async fn list_tombstones(
        &self,
        request: fleetring_proto::v1::ListTombstonesRequest,
    ) -> Result<fleetring_proto::v1::ListTombstonesResponse, Status> {
        self.retrying(|mut client| {
            let request = request.clone();
            async move { client.list_tombstones(request).await }
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
        self.following(&|mut client: GrpcControlClient<CredentialedChannel>| {
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

    /// Leaves the fleet. Sent once per leader, not retried.
    ///
    /// # Errors
    ///
    /// The status the control node returned, or `UNAVAILABLE` if it couldn't be reached.
    pub async fn leave(&self, request: LeaveRequest) -> Result<LeaveResponse, Status> {
        self.following(&|mut client: GrpcControlClient<CredentialedChannel>| {
            let request = request.clone();
            async move { client.leave(request).await }
        })
        .await
    }

    /// Starts a promotion. Sent once per leader, not retried.
    ///
    /// # Errors
    ///
    /// The status the control node returned, or `UNAVAILABLE` if it couldn't be reached.
    pub async fn start_promotion(
        &self,
        request: StartPromotionRequest,
    ) -> Result<PromotionResponse, Status> {
        self.following(&|mut client: GrpcControlClient<CredentialedChannel>| {
            let request = request.clone();
            async move { client.start_promotion(request).await }
        })
        .await
    }

    /// Records a fenced member. Retried: recording one twice fails with `ALREADY_EXISTS`, which
    /// means it is recorded.
    ///
    /// # Errors
    ///
    /// The status of the last attempt.
    pub async fn record_fenced(
        &self,
        request: RecordFencedRequest,
    ) -> Result<RecordFencedResponse, Status> {
        self.retrying(|mut client| {
            let request = request.clone();
            async move { client.record_fenced(request).await }
        })
        .await
    }

    /// Completes a promotion. Sent once per leader, not retried.
    ///
    /// # Errors
    ///
    /// The status the control node returned, or `UNAVAILABLE` if it couldn't be reached.
    pub async fn complete_promotion(
        &self,
        request: CompletePromotionRequest,
    ) -> Result<CompletePromotionResponse, Status> {
        self.following(&|mut client: GrpcControlClient<CredentialedChannel>| {
            let request = request.clone();
            async move { client.complete_promotion(request).await }
        })
        .await
    }

    /// Marks a member as draining. Retried: draining twice changes nothing.
    ///
    /// # Errors
    ///
    /// The status of the last attempt.
    pub async fn drain(&self, request: DrainRequest) -> Result<DrainResponse, Status> {
        self.retrying(|mut client| {
            let request = request.clone();
            async move { client.drain(request).await }
        })
        .await
    }

    /// Clears a member's draining mark. Retried: clearing it twice changes nothing.
    ///
    /// # Errors
    ///
    /// The status of the last attempt: `FAILED_PRECONDITION` while a member doesn't support the
    /// `undrain` feature.
    pub async fn undrain(&self, request: UndrainRequest) -> Result<UndrainResponse, Status> {
        self.retrying(|mut client| {
            let request = request.clone();
            async move { client.undrain(request).await }
        })
        .await
    }

    /// Hands a lease over from its draining holder. Sent once per leader, not retried: a retry
    /// after a lost reply would fail as a promotion in progress, which the caller can read as
    /// started.
    ///
    /// # Errors
    ///
    /// The status the control node returned, or `UNAVAILABLE` if it couldn't be reached.
    pub async fn transfer_lease(
        &self,
        request: TransferLeaseRequest,
    ) -> Result<PromotionResponse, Status> {
        self.following(&|mut client: GrpcControlClient<CredentialedChannel>| {
            let request = request.clone();
            async move { client.transfer_lease(request).await }
        })
        .await
    }

    /// Revokes a bearer token or signing key. Sent once per leader; repeating a revocation is
    /// harmless, so a caller may retry it.
    ///
    /// # Errors
    ///
    /// The status the control node returned, or `UNAVAILABLE` if it couldn't be reached.
    pub async fn revoke_token(
        &self,
        request: RevokeTokenRequest,
    ) -> Result<RevokeTokenResponse, Status> {
        self.following(&|mut client: GrpcControlClient<CredentialedChannel>| {
            let request = request.clone();
            async move { client.revoke_token(request).await }
        })
        .await
    }

    /// Asks for a delegated agent's token (ADR-0047, point 5). The request's operation ID makes a
    /// resend safe: an attempt that ends in `UNAVAILABLE` or `DEADLINE_EXCEEDED`, a lost reply
    /// or a refusal that says to retry, is sent again unchanged, and returns the delegation the
    /// first attempt recorded, signed again, or records it now. Any other refusal is returned at
    /// once: `RESOURCE_EXHAUSTED`, a limit, waits for the caller to decide.
    ///
    /// The reply holds the child's token: keep it out of logs.
    ///
    /// # Errors
    ///
    /// The status of the last attempt. A control node that predates delegation answers
    /// `UNIMPLEMENTED`.
    pub async fn delegate_token(
        &self,
        request: DelegateTokenRequest,
    ) -> Result<DelegateTokenResponse, Status> {
        let call = |mut client: GrpcControlClient<CredentialedChannel>| {
            let request = request.clone();
            async move { client.delegate_token(request).await }
        };
        let mut wait = self.backoff.initial;
        let mut attempt = 1;
        loop {
            match self.following(&call).await {
                Err(status)
                    if attempt < self.backoff.attempts
                        && matches!(status.code(), Code::Unavailable | Code::DeadlineExceeded) =>
                {
                    tracing::debug!(attempt, code = ?status.code(), "resending a delegation after {wait:?}");
                    tokio::time::sleep(wait).await;
                    wait = wait.saturating_mul(2).min(self.backoff.max);
                    attempt += 1;
                }
                result => return result,
            }
        }
    }

    /// Lists the live delegation records the request's filters select, never tokens (ADR-0047,
    /// point 6.4). Retried.
    ///
    /// # Errors
    ///
    /// The status of the last attempt: `PERMISSION_DENIED` without `fleet`.
    pub async fn list_delegations(
        &self,
        request: ListDelegationsRequest,
    ) -> Result<ListDelegationsResponse, Status> {
        self.retrying(|mut client| {
            let request = request.clone();
            async move { client.list_delegations(request).await }
        })
        .await
    }

    /// Finds where a database lives, as [`locate`](Self::locate) does, but with `credentials`
    /// instead of the client's own: a router locates with each caller's token, so the control
    /// plane answers only what the caller may read (ADR-0037). Retried.
    ///
    /// # Errors
    ///
    /// The status of the last attempt.
    pub async fn locate_as(
        &self,
        request: LocateRequest,
        credentials: &Credentials,
    ) -> Result<LocateResponse, Status> {
        let call = |mut client: GrpcControlClient<CredentialedChannel>| {
            let request = request.clone();
            async move { client.locate(request).await }
        };
        retry(self.backoff, || {
            self.following_as(credentials, &call, Unavailable::NextAddress)
        })
        .await
    }

    async fn retrying<T, F, Fut>(&self, call: F) -> Result<T, Status>
    where
        F: Fn(GrpcControlClient<CredentialedChannel>) -> Fut,
        Fut: Future<Output = Result<Response<T>, Status>>,
    {
        retry(self.backoff, || {
            self.following_as(&self.shared.credentials, &call, Unavailable::NextAddress)
        })
        .await
    }

    /// Calls the current leader once, and follows up to [`MAX_REDIRECTS`] redirects; an address
    /// that answers `UNAVAILABLE` without naming a leader moves the next call to the next
    /// address.
    async fn following<T, F, Fut>(&self, call: &F) -> Result<T, Status>
    where
        F: Fn(GrpcControlClient<CredentialedChannel>) -> Fut,
        Fut: Future<Output = Result<Response<T>, Status>>,
    {
        self.following_as(&self.shared.credentials, call, Unavailable::Return)
            .await
    }

    /// Calls the current address with `credentials`, follows up to [`MAX_REDIRECTS`] redirects,
    /// and on `UNAVAILABLE` without a leader to name, moves to the next address, trying each
    /// once if `unavailable` says to.
    async fn following_as<T, F, Fut>(
        &self,
        credentials: &Credentials,
        call: &F,
        unavailable: Unavailable,
    ) -> Result<T, Status>
    where
        F: Fn(GrpcControlClient<CredentialedChannel>) -> Fut,
        Fut: Future<Output = Result<Response<T>, Status>>,
    {
        let mut redirects = 0;
        let mut moved = 0;
        loop {
            let address = self.leader();
            let answer = match self.current(credentials) {
                Ok(client) => call(client).await.map(Response::into_inner),
                Err(status) => Err(status),
            };
            let status = match answer {
                Ok(response) => return Ok(response),
                Err(status) => status,
            };
            match leader_hint(&status) {
                Some(leader) if redirects < MAX_REDIRECTS => {
                    tracing::debug!(%leader, "following a control leader redirect");
                    self.set_leader(leader);
                    redirects += 1;
                }
                // `CANCELLED` and `DEADLINE_EXCEEDED` are what a call to an address that hangs ends
                // with, once the channel's request timeout passes.
                None if matches!(
                    status.code(),
                    Code::Unavailable | Code::Cancelled | Code::DeadlineExceeded
                ) =>
                {
                    let candidates = {
                        let mut addresses = self.addresses();
                        addresses.advance(&address);
                        addresses.candidates().len()
                    };
                    moved += 1;
                    // A call that never reached a server can go to the next address whatever it
                    // is: a resend can't repeat it.
                    let resend =
                        unavailable == Unavailable::NextAddress || never_delivered(&status);
                    if !resend || moved >= candidates {
                        return Err(status);
                    }
                    tracing::debug!(%address, next = %self.leader(), "a control address is unavailable; trying the next");
                }
                _ => return Err(status),
            }
        }
    }

    fn current(
        &self,
        credentials: &Credentials,
    ) -> Result<GrpcControlClient<CredentialedChannel>, Status> {
        let leader = self.leader();
        let mut channels = self
            .shared
            .channels
            .lock()
            .expect("control client lock poisoned");
        if let Some(channel) = channels.get(&leader) {
            return Ok(control_client_with(channel.clone(), credentials));
        }
        let channel = (self.shared.connect)(&leader).map_err(|error| {
            Status::unavailable(format!("can't reach control node {leader}: {error}"))
        })?;
        channels.insert(leader, channel.clone());
        Ok(control_client_with(channel, credentials))
    }

    fn set_leader(&self, leader: String) {
        self.addresses().current = leader;
    }
}

/// Whether `status` says the call never reached a server: the connection was refused, so nothing
/// listened at the address and the request went nowhere.
fn never_delivered(status: &Status) -> bool {
    let mut cause: Option<&(dyn std::error::Error + 'static)> = std::error::Error::source(status);
    while let Some(error) = cause {
        if error
            .downcast_ref::<std::io::Error>()
            .is_some_and(|error| error.kind() == std::io::ErrorKind::ConnectionRefused)
        {
            return true;
        }
        cause = error.source();
    }
    false
}
