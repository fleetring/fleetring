//! A client for a whole cluster: the control plane, and each database's primary.

use std::collections::BTreeMap;
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use fleetring::{NodeId, ReplicationFactor};
use fleetring_control::{DatabaseName, Engine, LeaseEpoch, Liveness, Role};
use fleetring_proto::v1::database_client::DatabaseClient;
use fleetring_proto::v1::{
    self as proto, CreateDatabaseRequest, CreateRequest, ExecuteRequest, LocateRequest,
    QueryRequest, WatchMembershipRequest,
};
use tonic::transport::Channel;
use tonic::{Code, Response, Status};

use crate::MembershipView;
use crate::control::ControlClient;
use crate::data::convert::{location, params_to_proto, rows_from_proto};
use crate::data::redirect::primary_hint;
use crate::engine::{Position, Rows, Statement};
use crate::environment::Network;
use crate::transport::{Backoff, database_client};

/// How many redirects one request follows before it gives up.
const MAX_REDIRECTS: usize = 4;

/// How long [`Client::status`] waits for the first membership update. The control plane sends
/// none while the cluster has no members.
const FIRST_UPDATE: Duration = Duration::from_secs(2);

/// Where a database lives, as the control plane knows it.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct Location {
    /// The database.
    pub database: DatabaseName,
    /// The engine it runs on.
    pub engine: Engine,
    /// Its current lease epoch, or 0 before its first lease.
    pub epoch: LeaseEpoch,
    /// Its lease holder, which serves its requests, once it has one.
    pub primary: Option<NodeId>,
    /// Its replica set in the ring's preference order, the primary included.
    pub replicas: Vec<ReplicaInfo>,
}

impl Location {
    /// The replicas other than the primary, in preference order.
    pub fn followers(&self) -> impl Iterator<Item = &ReplicaInfo> {
        self.replicas
            .iter()
            .filter(|replica| Some(&replica.node) != self.primary.as_ref())
    }

    /// The primary's replica entry, once the database has a primary.
    #[must_use]
    pub fn primary_replica(&self) -> Option<&ReplicaInfo> {
        let primary = self.primary.as_ref()?;
        self.replicas
            .iter()
            .find(|replica| &replica.node == primary)
    }
}

/// One member of a database's replica set.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct ReplicaInfo {
    /// The member.
    pub node: NodeId,
    /// The address it advertised, as `host:port`.
    pub address: Option<String>,
    /// Whether the control plane counts it as live.
    pub liveness: Liveness,
    /// The role it last reported for the database in a heartbeat, if it has reported one.
    pub reported_role: Option<Role>,
    /// The log position it last reported for the database, if it has reported one.
    pub position: Option<Position>,
}

/// The result of a write.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct Written {
    /// The rows the statement inserted, updated, or deleted.
    pub rows_affected: u64,
    /// How many replicas, the primary included, held the write when it was acknowledged: the
    /// write quorum W or more.
    pub acknowledged_by: u32,
    /// R, the size the database's replica set should have.
    pub replicas: u32,
    /// Whether the database is under-replicated: fewer than R of its replicas were reachable, so
    /// the write can't reach them all until they come back.
    pub under_replicated: bool,
    /// The primary that acknowledged the write, if it said.
    pub primary: Option<NodeId>,
    /// The epoch it acknowledged the write at, or 0 if it didn't say.
    pub epoch: LeaseEpoch,
}

/// A client for a whole cluster.
///
/// - **Control calls** (`create_database`, `locate`, `status`) go to the control plane through a
///   [`ControlClient`], which follows leader redirects and retries what is safe to retry.
/// - **Database calls** (`execute`, `query`) go to the database's primary, which the client
///   locates through the control plane. A node that isn't the primary answers with a redirect
///   ([`not_primary`](crate::data::not_primary)), and the client follows up to four of them.
///   Database calls are sent once and never retried, because a lost reply leaves a write's
///   outcome unknown.
/// - **Creation** registers the database with the control plane, which places it with the ring,
///   then asks the first live member of its replica set, in preference order, to become its
///   primary: that node takes the first lease and creates the database.
///
/// Cloning is cheap; clones share connections.
#[derive(Debug, Clone)]
pub struct Client {
    control: ControlClient,
    network: Network,
    entry: Option<String>,
    channels: Arc<Mutex<BTreeMap<String, Channel>>>,
}

impl Client {
    /// A client that reaches the control plane at `control` (`host:port`) over `network`.
    #[must_use]
    pub fn new(control: impl Into<String>, network: Network) -> Self {
        Self {
            control: network.control_client(control),
            network,
            entry: None,
            channels: Arc::default(),
        }
    }

    /// Retries idempotent control calls with `backoff` instead of the default.
    #[must_use]
    pub fn with_backoff(mut self, backoff: Backoff) -> Self {
        self.control = self.control.with_backoff(backoff);
        self
    }

    /// Sends database calls to the data node at `address` (`host:port`) instead of the primary
    /// the control plane names, and follows redirects from there.
    #[must_use]
    pub fn with_entry(mut self, address: impl Into<String>) -> Self {
        self.entry = Some(address.into());
        self
    }

    /// The client's control-plane client.
    #[must_use]
    pub fn control(&self) -> &ControlClient {
        &self.control
    }

    /// Drops every connection, so the next call dials again. See [`ControlClient::reconnect`].
    ///
    /// # Panics
    ///
    /// If the client's lock was poisoned by a panic in another thread.
    pub fn reconnect(&self) {
        self.control.reconnect();
        self.channels
            .lock()
            .expect("client channel lock poisoned")
            .clear();
    }

    /// Creates a SQLite database with `replication_factor` replicas, and opens it on its
    /// primary.
    ///
    /// If the control plane already has the database but no primary, because an earlier
    /// creation stopped halfway, this finishes it.
    ///
    /// # Errors
    ///
    /// `ALREADY_EXISTS` if the database exists and has a primary; `UNAVAILABLE` if no member of
    /// its replica set has an address; and the status of a failed control or database call.
    pub async fn create_database(
        &self,
        name: &DatabaseName,
        replication_factor: ReplicationFactor,
    ) -> Result<Location, Status> {
        let request = CreateDatabaseRequest {
            database: name.to_string(),
            replication_factor: u32::from(replication_factor.get()),
            engine: proto::Engine::Sqlite.into(),
        };
        let placement = match self.control.create_database(request).await {
            Ok(response) => placement_of(response.placement)?,
            Err(status) if status.code() == Code::AlreadyExists => {
                let placement = self.placement(name).await?;
                if placement.primary.is_some() {
                    return Err(status);
                }
                placement
            }
            Err(status) => return Err(status),
        };
        let start = match &self.entry {
            Some(entry) => entry.clone(),
            None => first_candidate(&placement).ok_or_else(|| {
                Status::unavailable(format!(
                    "no member of the replica set of {name} has an address"
                ))
            })?,
        };
        let created = self
            .following(start, |mut client| {
                let request = CreateRequest {
                    database: name.to_string(),
                };
                async move { client.create(request).await }
            })
            .await?;
        location(placement_of(created.placement)?)
    }

    /// Finds where a database lives. Retried.
    ///
    /// # Errors
    ///
    /// `NOT_FOUND` if there is no such database, or the status of the last attempt.
    pub async fn locate(&self, name: &DatabaseName) -> Result<Location, Status> {
        location(self.placement(name).await?)
    }

    /// Runs one write statement on the database's primary, and waits until a write quorum of its
    /// replicas holds it.
    ///
    /// # Errors
    ///
    /// `INVALID_ARGUMENT` if the statement fails, `FAILED_PRECONDITION` if the database has no
    /// primary yet, `UNAVAILABLE` if the primary can't be reached or the database is failing
    /// over, and `ABORTED` if the write may or may not have taken effect, as when it committed on
    /// the primary but too few replicas were reachable to acknowledge it. `UNAVAILABLE` and
    /// `ABORTED` are worth retrying after a failover; an `ABORTED` write may then apply twice.
    pub async fn execute(
        &self,
        name: &DatabaseName,
        statement: Statement,
    ) -> Result<Written, Status> {
        let params = params_to_proto(&statement);
        let response = self
            .on_primary(name, |mut client| {
                let request = ExecuteRequest {
                    database: name.to_string(),
                    sql: statement.sql().to_owned(),
                    params: params.clone(),
                };
                async move { client.execute(request).await }
            })
            .await?;
        Ok(Written {
            rows_affected: response.rows_affected,
            acknowledged_by: response.acknowledged_by,
            replicas: response.replicas,
            under_replicated: response.under_replicated,
            primary: response.primary.parse().ok(),
            epoch: LeaseEpoch::new(response.epoch),
        })
    }

    /// Runs a read-only query on the database's primary.
    ///
    /// # Errors
    ///
    /// As [`Client::execute`].
    pub async fn query(&self, name: &DatabaseName, statement: Statement) -> Result<Rows, Status> {
        let params = params_to_proto(&statement);
        let response = self
            .on_primary(name, |mut client| {
                let request = QueryRequest {
                    database: name.to_string(),
                    sql: statement.sql().to_owned(),
                    params: params.clone(),
                };
                async move { client.query(request).await }
            })
            .await?;
        rows_from_proto(response)
    }

    /// The cluster's membership as the control plane sees it: the members with their addresses
    /// and liveness, the membership epoch, and the ring, rebuilt with the function every node
    /// uses, so its fingerprint matches theirs.
    ///
    /// A cluster without members has an empty view at epoch 0.
    ///
    /// # Errors
    ///
    /// The status of the last attempt to reach the control plane, or `INTERNAL` if its reply is
    /// invalid.
    pub async fn status(&self) -> Result<MembershipView, Status> {
        let mut stream = self
            .control
            .watch_membership(WatchMembershipRequest { after_epoch: 0 })
            .await?;
        match tokio::time::timeout(FIRST_UPDATE, stream.message()).await {
            Ok(Ok(Some(update))) => MembershipView::from_update(update),
            Ok(Ok(None)) => Err(Status::unavailable("the membership stream ended")),
            Ok(Err(status)) => Err(status),
            // The control plane is reachable and has nothing to send: no members yet.
            Err(_) => Ok(MembershipView::default()),
        }
    }

    async fn placement(&self, name: &DatabaseName) -> Result<proto::Placement, Status> {
        let response = self
            .control
            .locate(LocateRequest {
                database: name.to_string(),
            })
            .await?;
        placement_of(response.placement)
    }

    /// Calls `call` on the database's primary, or on the entry node, following redirects.
    async fn on_primary<T, F, Fut>(&self, name: &DatabaseName, call: F) -> Result<T, Status>
    where
        F: Fn(DatabaseClient<Channel>) -> Fut,
        Fut: Future<Output = Result<Response<T>, Status>>,
    {
        let start = match &self.entry {
            Some(entry) => entry.clone(),
            None => {
                let placement = self.placement(name).await?;
                primary_address(name, &placement)?
            }
        };
        self.following(start, call).await
    }

    /// Calls the node at `start`, and follows up to [`MAX_REDIRECTS`] redirects.
    async fn following<T, F, Fut>(&self, start: String, call: F) -> Result<T, Status>
    where
        F: Fn(DatabaseClient<Channel>) -> Fut,
        Fut: Future<Output = Result<Response<T>, Status>>,
    {
        let mut address = start;
        let mut redirects = 0;
        loop {
            match call(self.database(&address)?).await {
                Ok(response) => return Ok(response.into_inner()),
                Err(status) => match primary_hint(&status) {
                    Some(primary) if redirects < MAX_REDIRECTS && primary != address => {
                        tracing::debug!(%primary, "following a primary redirect");
                        address = primary;
                        redirects += 1;
                    }
                    _ => return Err(status),
                },
            }
        }
    }

    fn database(&self, address: &str) -> Result<DatabaseClient<Channel>, Status> {
        let mut channels = self.channels.lock().expect("client channel lock poisoned");
        if let Some(channel) = channels.get(address) {
            return Ok(database_client(channel.clone()));
        }
        let channel = self
            .network
            .channel(address)
            .map_err(|error| Status::unavailable(format!("can't reach {address}: {error}")))?;
        channels.insert(address.to_owned(), channel.clone());
        Ok(database_client(channel))
    }
}

fn placement_of(placement: Option<proto::Placement>) -> Result<proto::Placement, Status> {
    placement.ok_or_else(|| Status::internal("the reply has no placement"))
}

/// The address of the database's primary.
fn primary_address(name: &DatabaseName, placement: &proto::Placement) -> Result<String, Status> {
    let primary = placement.primary.as_deref().ok_or_else(|| {
        if placement.epoch > 0 {
            Status::unavailable(format!(
                "database {name} is failing over to epoch {}; retry",
                placement.epoch
            ))
        } else {
            Status::failed_precondition(format!("database {name} has no primary yet"))
        }
    })?;
    placement
        .replicas
        .iter()
        .find(|replica| replica.node_id == primary)
        .and_then(|replica| replica.address.clone())
        .ok_or_else(|| {
            Status::unavailable(format!(
                "the primary of {name}, {primary}, has no known address"
            ))
        })
}

/// The node to make primary: the first live replica with an address, in preference order, or
/// else the first replica with an address.
fn first_candidate(placement: &proto::Placement) -> Option<String> {
    let with_address = || {
        placement
            .replicas
            .iter()
            .filter(|replica| replica.address.is_some())
    };
    with_address()
        .find(|replica| replica.live)
        .or_else(|| with_address().next())
        .and_then(|replica| replica.address.clone())
}
