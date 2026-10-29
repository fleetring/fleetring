//! A client for a whole cluster: the control plane, and each database's primary.

use std::collections::BTreeMap;
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use fleetring::{Epoch, Fingerprint, NodeId, ReplicationFactor};
use fleetring_control::{Acks, DatabaseName, Durability, Engine, LeaseEpoch, Liveness, Role};
use fleetring_proto::v1::database_client::DatabaseClient;
use fleetring_proto::v1::{
    self as proto, ChangeDurabilityRequest, CreateDatabaseRequest, CreateRequest, DrainRequest,
    DropDatabaseRequest, ExecuteRequest, ListDatabasesRequest, LocateRequest, NodeStatusRequest,
    QueryRequest, WatchMembershipRequest,
};
use tonic::transport::Channel;
use tonic::{Code, Response, Status};

use crate::MembershipView;
use crate::changes::SinkStatus;
use crate::control::ControlClient;
use crate::data::convert::{
    acks_from_proto, acks_to_proto, location, node_status, params_to_proto, rows_from_proto,
};
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
    /// How many replicas it should have, R.
    pub replication_factor: ReplicationFactor,
    /// The durability settings in effect: its acknowledgement mode and its floor on reachable
    /// replicas (ADR-0023).
    pub durability: Durability,
}

/// What to create a database with: its replication factor and, optionally, its durability
/// settings. Settings left unset take the cluster's defaults (`[replication] acks` and
/// `min_replicas` on the control node).
///
/// # Examples
///
/// ```
/// use fleetring::ReplicationFactor;
/// use fleetring_control::Acks;
/// use fleetringd::data::DatabaseSpec;
///
/// let spec = DatabaseSpec::new(ReplicationFactor::new(3).ok_or("zero")?)
///     .with_acks(Acks::All)
///     .with_min_replicas(3);
/// assert_eq!(spec.acks, Some(Acks::All));
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct DatabaseSpec {
    /// How many replicas the database should have.
    pub replication_factor: ReplicationFactor,
    /// The acknowledgement mode, or `None` for the cluster's default.
    pub acks: Option<Acks>,
    /// The floor on reachable replicas, or `None` for the cluster's default: the mode's W.
    pub min_replicas: Option<u8>,
}

impl DatabaseSpec {
    /// A database with `replication_factor` replicas and the cluster's default durability.
    #[must_use]
    pub const fn new(replication_factor: ReplicationFactor) -> Self {
        Self {
            replication_factor,
            acks: None,
            min_replicas: None,
        }
    }

    /// Acknowledges writes in `acks`.
    #[must_use]
    pub const fn with_acks(mut self, acks: Acks) -> Self {
        self.acks = Some(acks);
        self
    }

    /// Refuses writes while fewer than `min_replicas` replicas, the primary included, are
    /// reachable.
    #[must_use]
    pub const fn with_min_replicas(mut self, min_replicas: u8) -> Self {
        self.min_replicas = Some(min_replicas);
        self
    }
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

/// What one data node knows on its own, as [`Client::node_status`] reports it.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct NodeStatus {
    /// The node.
    pub node: NodeId,
    /// The membership epoch it has published, or 0 before it has heard from the control plane.
    pub membership_epoch: Epoch,
    /// The fingerprint of the ring it places by, once it has one.
    pub fingerprint: Option<Fingerprint>,
    /// The databases it serves, in name order.
    pub databases: Vec<ServedDatabase>,
    /// Each change sink's progress on each database the node feeds it (FR-039).
    pub sinks: Vec<SinkStatus>,
}

/// A database one node serves, as primary or follower.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct ServedDatabase {
    /// The database.
    pub database: DatabaseName,
    /// The node's role for it.
    pub role: Role,
    /// The epoch of the primary that wrote the last frames of the node's copy: its own epoch on
    /// a primary.
    pub epoch: LeaseEpoch,
    /// Where its copy is, if it knows.
    pub position: Option<Position>,
}

/// A recorded durability change, as [`Client::change_durability`] reports it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct DurabilityChange {
    /// The mode that takes effect at the next lease.
    pub acks: Acks,
    /// The floor that takes effect at the next lease.
    pub min_replicas: u8,
    /// The mode in effect until then.
    pub current_acks: Acks,
    /// The floor in effect until then.
    pub current_min_replicas: u8,
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
/// - **Control calls** (`create_database`, `drop_database`, `locate`, `list_databases`, `status`)
///   go to the control plane through a [`ControlClient`], which follows leader redirects and
///   retries what is safe to retry.
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

    /// Creates a SQLite database with `replication_factor` replicas and the cluster's default
    /// durability, and opens it on its primary.
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
        self.create_database_with(name, DatabaseSpec::new(replication_factor))
            .await
    }

    /// Creates a SQLite database as `spec` says, and opens it on its primary; see
    /// [`Client::create_database`].
    ///
    /// # Errors
    ///
    /// As [`Client::create_database`], and `INVALID_ARGUMENT` if the durability settings don't
    /// fit the replication factor.
    pub async fn create_database_with(
        &self,
        name: &DatabaseName,
        spec: DatabaseSpec,
    ) -> Result<Location, Status> {
        let request = CreateDatabaseRequest {
            database: name.to_string(),
            replication_factor: u32::from(spec.replication_factor.get()),
            engine: proto::Engine::Sqlite.into(),
            acks: spec
                .acks
                .map_or(proto::Acks::Unspecified, acks_to_proto)
                .into(),
            min_replicas: u32::from(spec.min_replicas.unwrap_or(0)),
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

    /// Drops a database for good, and returns the epoch it was at. Its primary stops serving it
    /// within one lease renewal, and every replica deletes its copy once it learns of the drop.
    ///
    /// # Errors
    ///
    /// `NOT_FOUND` if there is no such database, or the status of the failed control call.
    pub async fn drop_database(&self, name: &DatabaseName) -> Result<LeaseEpoch, Status> {
        let response = self
            .control
            .drop_database(DropDatabaseRequest {
                database: name.to_string(),
            })
            .await?;
        Ok(LeaseEpoch::new(response.epoch))
    }

    /// Marks `node` as draining, and returns the databases it holds the lease on, in name order.
    ///
    /// The node sees the mark in its next membership update and hands each of those leases to
    /// a follower, without waiting for it to expire; it gets no new lease meanwhile, and stays a
    /// member and a follower until it leaves or restarts. Retried: draining twice changes
    /// nothing.
    ///
    /// # Errors
    ///
    /// `NOT_FOUND` if there is no such member, or the status of the last attempt.
    pub async fn drain_node(&self, node: &NodeId) -> Result<Vec<DatabaseName>, Status> {
        let response = self
            .control
            .drain(DrainRequest {
                node_id: node.to_string(),
            })
            .await?;
        response
            .primaries
            .iter()
            .map(|name| {
                name.parse().map_err(|error| {
                    Status::internal(format!("the reply has an invalid database name: {error}"))
                })
            })
            .collect()
    }

    /// Records new durability settings for a database, which take effect at its next lease
    /// (ADR-0023), and returns the settings in effect until then.
    ///
    /// # Errors
    ///
    /// `NOT_FOUND` if there is no such database, `INVALID_ARGUMENT` if the settings don't fit
    /// its replication factor, or the status of the failed control call.
    pub async fn change_durability(
        &self,
        name: &DatabaseName,
        acks: Acks,
        min_replicas: Option<u8>,
    ) -> Result<DurabilityChange, Status> {
        let response = self
            .control
            .change_durability(ChangeDurabilityRequest {
                database: name.to_string(),
                acks: acks_to_proto(acks).into(),
                min_replicas: u32::from(min_replicas.unwrap_or(0)),
            })
            .await?;
        let settings = |acks: i32, min_replicas: u32| {
            let acks = acks_from_proto(acks)
                .ok_or_else(|| Status::internal("the reply names no acks mode"))?;
            let min_replicas = u8::try_from(min_replicas)
                .map_err(|_| Status::internal("the reply has an invalid min_replicas"))?;
            Ok::<_, Status>((acks, min_replicas))
        };
        let (pending_acks, pending_min_replicas) = settings(response.acks, response.min_replicas)?;
        let (current_acks, current_min_replicas) =
            settings(response.current_acks, response.current_min_replicas)?;
        Ok(DurabilityChange {
            acks: pending_acks,
            min_replicas: pending_min_replicas,
            current_acks,
            current_min_replicas,
        })
    }

    /// Finds where a database lives. Retried.
    ///
    /// # Errors
    ///
    /// `NOT_FOUND` if there is no such database, or the status of the last attempt.
    pub async fn locate(&self, name: &DatabaseName) -> Result<Location, Status> {
        location(self.placement(name).await?)
    }

    /// Lists where every database lives, in name order. Retried.
    ///
    /// # Errors
    ///
    /// The status of the last attempt, or `INTERNAL` if a reply is invalid.
    pub async fn list_databases(&self) -> Result<Vec<Location>, Status> {
        self.control
            .list_databases(ListDatabasesRequest {})
            .await?
            .databases
            .into_iter()
            .map(location)
            .collect()
    }

    /// Asks the data node at `address` (`host:port`) what it knows on its own: its membership
    /// epoch, ring fingerprint, and the databases it serves. Nothing is asked of the control
    /// plane, so the answer shows whether the node agrees with it.
    ///
    /// # Errors
    ///
    /// `UNAVAILABLE` if the node can't be reached, `UNIMPLEMENTED` if it hosts no data, or
    /// `INTERNAL` if its reply is invalid.
    pub async fn node_status(&self, address: &str) -> Result<NodeStatus, Status> {
        let response = self
            .database(address)?
            .node_status(NodeStatusRequest {})
            .await?
            .into_inner();
        node_status(response)
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
