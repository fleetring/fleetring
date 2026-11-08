//! A client for a whole fleet: the control plane, and each database's primary.

use std::collections::BTreeMap;
use std::future::Future;
use std::num::NonZeroU32;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use fleetring::{Epoch, Fingerprint, NodeId, ReplicationFactor};
use fleetring_client::redirect::primary_hint;
use fleetring_control::{
    Acks, DatabaseName, Durability, Engine, LeaseEpoch, Liveness, MetadataChange, ReplicaRole,
    Timestamp,
};
use fleetring_proto::v1::database_client::DatabaseClient;
use fleetring_proto::v1::restore_point::Point;
use fleetring_proto::v1::{
    self as proto, ChangeDurabilityRequest, CreateDatabaseRequest,
    CreateDatabaseWithMetadataRequest, CreateRequest, DescribeArchiveRequest, DrainRequest,
    DropDatabaseRequest, ExecuteRequest, LeaveRequest, ListDatabasesRequest, LocateRequest,
    NodeStatusRequest, QueryRequest, RemoveOrphanRequest, RestoreLatest, RestoreRequest,
    UndrainRequest, VerifyArchiveRequest, VerifyRequest, WatchChangesRequest, WatchLeasesRequest,
    WatchMembershipRequest,
};
use tonic::transport::Channel;
use tonic::{Code, Response, Status, Streaming};

use crate::MembershipView;
use crate::changes::SinkStatus;
use crate::control::ControlClient;
use crate::data::convert::{
    acks_from_proto, acks_to_proto, change_to_proto, change_update_from_proto,
    lease_update_from_proto, location, metadata_view, new_metadata_to_proto, node_status,
    params_to_proto, rows_from_proto, schema_from_proto, schema_request_to_proto,
};
use crate::data::metadata::{
    Descriptions, Expiry, ExpiryRequest, ListFilter, Listing, MetadataView, NewMetadata,
};
use crate::data::node::BEING_DROPPED;
use crate::data::schema::SchemaView;
use crate::durability::RestoreTarget;
use crate::engine::{CaptureMode, Position, RowChange, Rows, SchemaRequest, Statement};
use crate::environment::Network;
use crate::replication::{ChainHash, LeaseEvent};
use crate::transport::{Backoff, CredentialedChannel, database_client_with};

/// How many redirects one request follows before it gives up.
const MAX_REDIRECTS: usize = 4;

/// How long [`Client::status`] waits for the first membership update. The control plane sends
/// none while the fleet has no members.
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
    /// The node carrying out its promotion to `epoch`, while one is in progress (FR-050).
    pub candidate: Option<NodeId>,
    /// Its replica set in the ring's preference order, the primary included.
    pub replicas: Vec<ReplicaInfo>,
    /// How many replicas it should have, R.
    pub replication_factor: ReplicationFactor,
    /// The durability settings in effect: its acknowledgement mode and its floor on reachable
    /// replicas (ADR-0023).
    pub durability: Durability,
    /// Its description, labels, and creator (ADR-0041), or `None` for a database without
    /// metadata, or from a control node that predates it.
    pub metadata: Option<MetadataView>,
}

/// What to create a database with: optionally, its replication factor and its durability
/// settings. Settings left unset take the fleet's defaults (`[replication] replication_factor`,
/// `acks`, and `min_replicas` on the control node).
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
///
/// // Every setting the fleet's default.
/// assert_eq!(DatabaseSpec::default().replication_factor, None);
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[non_exhaustive]
pub struct DatabaseSpec {
    /// How many replicas the database should have, or `None` for the fleet's default.
    pub replication_factor: Option<ReplicationFactor>,
    /// The acknowledgement mode, or `None` for the fleet's default.
    pub acks: Option<Acks>,
    /// The floor on reachable replicas, or `None` for the fleet's default: the mode's W.
    pub min_replicas: Option<u8>,
}

impl DatabaseSpec {
    /// A database with `replication_factor` replicas and the fleet's default durability. For the
    /// fleet's default replication factor too, use [`DatabaseSpec::default`].
    #[must_use]
    pub const fn new(replication_factor: ReplicationFactor) -> Self {
        Self {
            replication_factor: Some(replication_factor),
            acks: None,
            min_replicas: None,
        }
    }

    /// The creation request for `name` as this spec says: 0 for an unset replication factor or
    /// floor, and an unspecified acknowledgement mode, so the control node fills in its defaults.
    fn to_request(self, name: &DatabaseName) -> CreateDatabaseRequest {
        CreateDatabaseRequest {
            database: name.to_string(),
            replication_factor: self
                .replication_factor
                .map_or(0, |factor| u32::from(factor.get())),
            engine: proto::Engine::Sqlite.into(),
            acks: self
                .acks
                .map_or(proto::Acks::Unspecified, acks_to_proto)
                .into(),
            min_replicas: u32::from(self.min_replicas.unwrap_or(0)),
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
    pub reported_role: Option<ReplicaRole>,
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
    /// The orphan copies it keeps, in name order (ADR-0042); none from a node that predates
    /// them, which never keeps one.
    pub orphans: Vec<Orphan>,
}

/// A copy a data node keeps, closed, as an orphan (ADR-0042): the control plane didn't know its
/// database, or held it only at a lower epoch, and the node saw no drop.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct Orphan {
    /// The database the copy is of.
    pub database: DatabaseName,
    /// The highest epoch the copy recorded.
    pub epoch: LeaseEpoch,
    /// The size of its files on disk, in bytes.
    pub bytes: u64,
}

/// A database one node serves, as primary or follower.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct ServedDatabase {
    /// The database.
    pub database: DatabaseName,
    /// The node's role for it.
    pub role: ReplicaRole,
    /// The epoch of the primary that wrote the last frames of the node's copy: its own epoch on
    /// a primary.
    pub epoch: LeaseEpoch,
    /// Where its copy is, if it knows.
    pub position: Option<Position>,
}

/// What one replica's log hashes to at a position, as [`Client::verify`] reports it (FR-044).
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct Verification {
    /// The database.
    pub database: DatabaseName,
    /// The epoch of the primary that wrote the last frames of the replica's log.
    pub epoch: LeaseEpoch,
    /// The position the hash is at, or `None` for an empty log.
    pub position: Option<Position>,
    /// The chain hash there.
    pub chain_hash: ChainHash,
}

/// A database restored from an archive, as [`Client::restore_database`] reports it (FR-049).
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct Restoration {
    /// Where the new database lives.
    pub location: Location,
    /// The position it starts at, or `None` for the log's beginning.
    pub position: Option<Position>,
    /// The hash chain there, verified over every batch applied.
    pub chain_hash: ChainHash,
    /// The archive's snapshot the restore started from.
    pub snapshot: String,
    /// How many segments it applied.
    pub segments: u32,
    /// When the last segment applied was archived, or `None` if none was.
    pub archived_at_ms: Option<u64>,
    /// The metadata the archive holds for the source, if the restore's snapshot has an object
    /// that checked out. Everything in it is the archive's claim: the archive isn't signed. It
    /// is not applied to the new database; [`restored_metadata`](crate::data::restored_metadata)
    /// says what of it to set.
    pub archived_metadata: Option<MetadataView>,
    /// Why the snapshot's metadata object was skipped, if it was. The data restored either way.
    pub archived_metadata_skipped: Option<String>,
}

/// What became of the expiry a restore asked for (ADR-0041, point 10), which
/// [`Client::set_restored_expiry`] sets once the restore has returned, so a short one can't
/// drop the database while it is being restored; an update that fails leaves the database
/// restored without one.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum RestoredExpiry {
    /// The restore asked for none.
    NotAsked,
    /// It was set to this time.
    Set(Timestamp),
    /// Setting it failed, so the database doesn't expire.
    Failed {
        /// The status code of the failed update.
        code: Code,
        /// The control node's message.
        message: String,
    },
}

/// One complete snapshot in an archive.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct ArchivedSnapshot {
    /// Its ID.
    pub id: String,
    /// The epoch of the primary that took it.
    pub epoch: LeaseEpoch,
    /// The position it holds the log up to.
    pub position: Option<Position>,
    /// When it was taken, by the archiving node's clock.
    pub taken_at_ms: u64,
    /// The packed stream's length.
    pub bytes: u64,
    /// Whether the archive holds a metadata object for it.
    pub has_metadata: bool,
}

/// What an archive holds for a database, as [`Client::describe_archive`] reports it.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct ArchiveDescription {
    /// The database.
    pub database: DatabaseName,
    /// Its complete snapshots, oldest first.
    pub snapshots: Vec<ArchivedSnapshot>,
    /// The furthest position a snapshot or segment reaches.
    pub latest: Option<Position>,
    /// When the last segment was archived.
    pub latest_ms: Option<u64>,
    /// How many segments it holds.
    pub segments: u32,
    /// Their bytes.
    pub segment_bytes: u64,
    /// How many snapshots are incomplete.
    pub incomplete: u32,
    /// The objects a restore won't use, with why.
    pub damaged: Vec<String>,
}

/// An archive restored into a scratch copy and verified, as [`Client::verify_archive`] reports
/// it.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct ArchiveVerification {
    /// The position the restored copy reached.
    pub position: Option<Position>,
    /// The hash chain there, verified over every batch applied.
    pub chain_hash: ChainHash,
    /// The snapshot it started from.
    pub snapshot: String,
    /// How many segments it applied.
    pub segments: u32,
}

/// The protocol's form of a restore target.
fn restore_point(target: &RestoreTarget) -> proto::RestorePoint {
    let point = match target {
        RestoreTarget::Position(position) => Point::Position(position.as_bytes().to_vec()),
        RestoreTarget::Time(ms) => Point::TimeMs(*ms),
        _ => Point::Latest(RestoreLatest {}),
    };
    proto::RestorePoint { point: Some(point) }
}

fn optional_position(bytes: Vec<u8>) -> Option<Position> {
    (!bytes.is_empty()).then(|| Position::from(bytes))
}

fn chain_hash(bytes: &[u8]) -> Result<ChainHash, Status> {
    ChainHash::from_bytes(bytes)
        .ok_or_else(|| Status::internal("the reply has an invalid chain hash"))
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

/// One message of a `WatchLeases` stream, as [`Client::watch_leases`] delivers it (FR-042).
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum LeaseUpdate {
    /// The first message: every lease the node held when the stream opened.
    Held {
        /// The node that answers.
        node: NodeId,
        /// The databases it serves as primary, each with its epoch.
        leases: BTreeMap<DatabaseName, LeaseEpoch>,
    },
    /// A lease the node gained or lost since.
    Event(LeaseEvent),
}

/// The stream [`Client::watch_leases`] returns.
#[derive(Debug)]
pub struct LeaseUpdates {
    inner: Streaming<proto::LeaseUpdate>,
}

impl LeaseUpdates {
    /// Waits for the next update, or `None` once the node has closed the stream.
    ///
    /// # Errors
    ///
    /// `RESOURCE_EXHAUSTED` if this subscriber fell too far behind, after which the stream is
    /// over and a new one starts from the leases held then; `UNAVAILABLE` if the node went away;
    /// and `INTERNAL` if an update is invalid.
    pub async fn next(&mut self) -> Result<Option<LeaseUpdate>, Status> {
        self.inner
            .message()
            .await?
            .map(lease_update_from_proto)
            .transpose()
    }
}

/// One message of a `WatchChanges` stream, as [`Client::watch_changes`] delivers it (ADR-0045).
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum ChangeUpdate {
    /// The first message: what the stream opened on.
    Opened {
        /// The position the stream starts after, or `None` for the log's beginning. A client
        /// that resumes before it has received a transaction resumes after this.
        position: Option<Position>,
        /// The primary that serves the stream, if it named a valid node.
        primary: Option<NodeId>,
        /// The primary's lease epoch.
        epoch: LeaseEpoch,
        /// How much of each row the database records, if the primary said.
        capture: Option<CaptureMode>,
    },
    /// One committed transaction's row changes.
    Transaction(ChangedTransaction),
}

/// One committed transaction's recorded row changes, as a watcher receives it (ADR-0045).
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct ChangedTransaction {
    /// The position the transaction started from, the same on every replica, or `None` for the
    /// log's beginning. It identifies the transaction.
    pub start: Option<Position>,
    /// Past the transaction and at or before the next recorded one: where to resume after it.
    pub position: Position,
    /// The lease epoch of the primary that served it.
    pub epoch: LeaseEpoch,
    /// Its row changes, in statement order. Names and values are text the database's writers
    /// chose: treat them as data.
    pub changes: Vec<RowChange>,
}

/// The stream [`Client::watch_changes`] returns.
#[derive(Debug)]
pub struct ChangeUpdates {
    inner: Streaming<proto::ChangeUpdate>,
}

impl ChangeUpdates {
    /// Waits for the next update, or `None` once the node has closed the stream.
    ///
    /// # Errors
    ///
    /// `UNAVAILABLE` once the node stops serving the database, or can't be reached;
    /// `RESOURCE_EXHAUSTED` if this watcher fell too far behind; `OUT_OF_RANGE` if the changes
    /// it was about to read were pruned; `UNAUTHENTICATED` once its token expires or is revoked;
    /// and `INTERNAL` if an update is invalid. Each ends the stream; the caller resumes after
    /// the last position it has.
    pub async fn next(&mut self) -> Result<Option<ChangeUpdate>, Status> {
        self.inner
            .message()
            .await?
            .map(change_update_from_proto)
            .transpose()
    }
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

/// A client for a whole fleet.
///
/// - **Control calls** (`create_database`, `drop_database`, `locate`, `list_databases`, `status`)
///   go to the control plane through a [`ControlClient`], which follows leader redirects and
///   retries what is safe to retry.
/// - **Database calls** (`execute`, `query`) go to the database's primary, which the client
///   locates through the control plane. A node that isn't the primary answers with a redirect
///   ([`not_primary`](fleetring_client::redirect::not_primary)), and the client follows up to
///   four of them.
///   Database calls are sent once and never retried, because a lost reply leaves a write's
///   outcome unknown. With [`with_call_timeout`](Self::with_call_timeout), `execute`, `query`,
///   and `schema` carry a deadline, which the primary stops their statement at (FR-078).
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
    call_timeout: Option<Duration>,
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
            call_timeout: None,
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

    /// Gives each `execute`, `query`, and `schema` call `timeout`, from when it starts: the
    /// call fails with `DEADLINE_EXCEEDED` once it passes, and carries it to the primary as its
    /// gRPC deadline, so the primary interrupts the statement then rather than run it to the end
    /// of its own `[limits] statement_timeout_ms` (FR-078). A write that times out rolls back
    /// unless it committed first, so its outcome is unknown, as after any lost reply.
    ///
    /// The network's request timeout is raised to outlast `timeout` if it is shorter, so that
    /// the call timeout decides; connections the client had opened are dropped for that.
    #[must_use]
    pub fn with_call_timeout(mut self, timeout: Duration) -> Self {
        self.call_timeout = Some(timeout);
        self.network = self.network.outlasting(timeout);
        self.channels = Arc::default();
        self
    }

    /// The timeout [`with_call_timeout`](Self::with_call_timeout) set, if any.
    #[must_use]
    pub fn call_timeout(&self) -> Option<Duration> {
        self.call_timeout
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

    /// Creates a SQLite database with `replication_factor` replicas and the fleet's default
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
    /// [`Client::create_database`]. What `spec` leaves unset, the replication factor included,
    /// takes the fleet's default.
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
        let placement = self.place(name, spec, None).await?;
        self.open_on_primary(name, &placement).await
    }

    /// Creates a SQLite database as `spec` says with a description, labels, or an expiry, and
    /// opens it on its primary; see [`Client::create_database`]. The control node records the
    /// caller as the creator, and turns an expiry's duration into a time. With no metadata, this
    /// is [`Client::create_database_with`].
    ///
    /// The creation goes through `CreateDatabaseWithMetadata`, which a control node without
    /// database metadata refuses, so a database is never created without the metadata asked
    /// for.
    ///
    /// # Errors
    ///
    /// As [`Client::create_database_with`]; `FAILED_PRECONDITION` while a member of the fleet
    /// lacks the `metadata` feature, or `expiry` for an expiry, naming it, or when the control
    /// node runs a release without database metadata; `INVALID_ARGUMENT` for a description or
    /// labels it refuses, or an expiry less than a minute ahead; and `UNAVAILABLE` if the
    /// control plane's time passed the expiry before the creation was logged, which a retry
    /// resolves.
    pub async fn create_database_with_metadata(
        &self,
        name: &DatabaseName,
        spec: DatabaseSpec,
        metadata: &NewMetadata,
    ) -> Result<Location, Status> {
        let metadata = (!metadata.is_empty()).then_some(metadata);
        let placement = self.place(name, spec, metadata).await?;
        self.open_on_primary(name, &placement).await
    }

    /// Asks the first live member of `placement`'s replica set, or the entry node, to become the
    /// new database's primary and create it.
    async fn open_on_primary(
        &self,
        name: &DatabaseName,
        placement: &proto::Placement,
    ) -> Result<Location, Status> {
        let start = match &self.entry {
            Some(entry) => entry.clone(),
            None => first_candidate(placement).ok_or_else(|| {
                Status::unavailable(format!(
                    "no member of the replica set of {name} has an address"
                ))
            })?,
        };
        let created = retrying_while_dropping(name, || {
            self.following(start.clone(), |mut client| {
                let request = CreateRequest {
                    database: name.to_string(),
                };
                async move { client.create(request).await }
            })
        })
        .await?;
        location(placement_of(created.placement)?)
    }

    /// Creates `name` as `spec` says, and builds it from `archive`'s copy of `source` at
    /// `target` instead of empty (FR-049): the control plane places it, then the first live
    /// member of its replica set restores it, verifying every batch against the hash chain, takes
    /// the first lease, and serves it. If the control plane already has the database but it was
    /// never leased, because an earlier restore failed, this retries it.
    ///
    /// # Errors
    ///
    /// `ALREADY_EXISTS` if the database exists and has a primary; `FAILED_PRECONDITION` if it
    /// has a lease or the archive can't reach the target; `DATA_LOSS` naming the object if one
    /// is missing or altered; `NOT_FOUND` if the node has no such archive; and the status of a
    /// failed control or database call.
    pub async fn restore_database(
        &self,
        name: &DatabaseName,
        spec: DatabaseSpec,
        archive: &str,
        source: &DatabaseName,
        target: &RestoreTarget,
    ) -> Result<Restoration, Status> {
        self.restore_database_with_metadata(
            name,
            spec,
            &NewMetadata::default(),
            archive,
            source,
            target,
        )
        .await
    }

    /// Restores as [`Client::restore_database`] does, and creates the new database with
    /// `metadata`'s description and labels, as [`Client::create_database_with_metadata`] does,
    /// before the restore runs. Its expiry is left out: the caller sets it with
    /// [`Client::set_restored_expiry`] once the restore has returned, and after the archive's
    /// description and labels ([`restored_metadata`](crate::data::restored_metadata)), so that a
    /// short one can't drop the database while it is being restored (ADR-0041, point 10).
    ///
    /// # Errors
    ///
    /// As [`Client::restore_database`] and [`Client::create_database_with_metadata`].
    pub async fn restore_database_with_metadata(
        &self,
        name: &DatabaseName,
        spec: DatabaseSpec,
        metadata: &NewMetadata,
        archive: &str,
        source: &DatabaseName,
        target: &RestoreTarget,
    ) -> Result<Restoration, Status> {
        let creation = NewMetadata {
            expiry: None,
            ..metadata.clone()
        };
        let metadata = (!creation.is_empty()).then_some(&creation);
        let placement = self.place(name, spec, metadata).await?;
        let start = match &self.entry {
            Some(entry) => entry.clone(),
            None => first_candidate(&placement).ok_or_else(|| {
                Status::unavailable(format!(
                    "no member of the replica set of {name} has an address"
                ))
            })?,
        };
        let point = restore_point(target);
        let restored = retrying_while_dropping(name, || {
            self.following(start.clone(), |mut client| {
                let request = RestoreRequest {
                    database: name.to_string(),
                    archive: archive.to_owned(),
                    source: source.to_string(),
                    point: Some(point.clone()),
                };
                async move { client.restore(request).await }
            })
        })
        .await?;
        Ok(Restoration {
            location: location(placement_of(restored.placement)?)?,
            position: optional_position(restored.position),
            chain_hash: chain_hash(&restored.chain_hash)?,
            snapshot: restored.snapshot,
            segments: restored.segments,
            archived_at_ms: restored.archived_at_ms,
            archived_metadata: restored.archived_metadata.map(metadata_view),
            archived_metadata_skipped: restored.archived_metadata_skipped,
        })
    }

    /// Sets the expiry a restore of `name` asked for, `expiry` from its own flags, once the
    /// restore has returned (ADR-0041, point 10): never one from the archive. Call it last, after
    /// the archive's description and labels are set. A failure leaves the database restored
    /// without an expiry, and is reported rather than returned.
    pub async fn set_restored_expiry(
        &self,
        name: &DatabaseName,
        expiry: Option<Expiry>,
    ) -> RestoredExpiry {
        let Some(expiry) = expiry else {
            return RestoredExpiry::NotAsked;
        };
        let updated = self
            .update_database_with(
                name,
                &MetadataChange::default(),
                ExpiryRequest::from(expiry),
                None,
            )
            .await;
        match updated {
            Ok(MetadataView {
                expires_at: Some(at),
                ..
            }) => RestoredExpiry::Set(at),
            Ok(_) => RestoredExpiry::Failed {
                code: Code::Internal,
                message: "the update's reply has no expiry".to_owned(),
            },
            Err(status) => RestoredExpiry::Failed {
                code: status.code(),
                message: status.message().to_owned(),
            },
        }
    }

    /// Asks the data node at `address` (`host:port`) what `archive` holds for `name`.
    ///
    /// # Errors
    ///
    /// `NOT_FOUND` if the node has no such archive, `UNAVAILABLE` if it or the store can't be
    /// reached, and `INTERNAL` if its reply is invalid.
    pub async fn describe_archive(
        &self,
        address: &str,
        archive: &str,
        name: &DatabaseName,
    ) -> Result<ArchiveDescription, Status> {
        let response = self
            .database(address)?
            .describe_archive(DescribeArchiveRequest {
                database: name.to_string(),
                archive: archive.to_owned(),
            })
            .await?
            .into_inner();
        Ok(ArchiveDescription {
            database: name.clone(),
            snapshots: response
                .snapshots
                .into_iter()
                .map(|snapshot| ArchivedSnapshot {
                    id: snapshot.id,
                    epoch: LeaseEpoch::new(snapshot.epoch),
                    position: optional_position(snapshot.position),
                    taken_at_ms: snapshot.taken_at_ms,
                    bytes: snapshot.bytes,
                    has_metadata: snapshot.metadata,
                })
                .collect(),
            latest: optional_position(response.latest),
            latest_ms: response.latest_ms,
            segments: response.segments,
            segment_bytes: response.segment_bytes,
            incomplete: response.incomplete,
            damaged: response.damaged,
        })
    }

    /// Asks the data node at `address` (`host:port`) to restore `archive`'s copy of `name` at
    /// `target` into a scratch directory, verifying every batch, and to discard it.
    ///
    /// # Errors
    ///
    /// As [`Client::restore_database`]'s restore.
    pub async fn verify_archive(
        &self,
        address: &str,
        archive: &str,
        name: &DatabaseName,
        target: &RestoreTarget,
    ) -> Result<ArchiveVerification, Status> {
        let response = self
            .database(address)?
            .verify_archive(VerifyArchiveRequest {
                database: name.to_string(),
                archive: archive.to_owned(),
                point: Some(restore_point(target)),
            })
            .await?
            .into_inner();
        Ok(ArchiveVerification {
            position: optional_position(response.position),
            chain_hash: chain_hash(&response.chain_hash)?,
            snapshot: response.snapshot,
            segments: response.segments,
        })
    }

    /// Drops a database for good, and returns the epoch it was at. The control node tells every
    /// replica, which deletes its copy, the primary after flushing its change streams; a replica
    /// that misses the notice keeps its copy as an orphan (ADR-0042).
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

    /// Removes `node` from the fleet, as a node that leaves on shutdown does, and returns the
    /// membership epoch after it: for a member that is gone for good. A live node that is removed
    /// joins again at its next heartbeat.
    ///
    /// # Errors
    ///
    /// `NOT_FOUND` if there is no such member, or the status of the failed control call.
    pub async fn remove_node(&self, node: &NodeId) -> Result<Epoch, Status> {
        let response = self
            .control
            .leave(LeaveRequest {
                node_id: node.to_string(),
            })
            .await?;
        Ok(Epoch::new(response.membership_epoch))
    }

    /// Clears `node`'s draining mark, so that it may take leases again without a restart.
    ///
    /// # Errors
    ///
    /// `NOT_FOUND` if there is no such member, `FAILED_PRECONDITION` while a member runs a release
    /// without the `undrain` feature (ADR-0038), or the status of the last attempt.
    pub async fn undrain_node(&self, node: &NodeId) -> Result<(), Status> {
        self.control
            .undrain(UndrainRequest {
                node_id: node.to_string(),
            })
            .await
            .map(|_| ())
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

    /// Lists where every database the caller may read lives, in name order, without
    /// descriptions. Retried.
    ///
    /// # Errors
    ///
    /// The status of the last attempt, or `INTERNAL` if a reply is invalid.
    pub async fn list_databases(&self) -> Result<Vec<Location>, Status> {
        Ok(self
            .list_databases_where(&ListFilter::default())
            .await?
            .databases)
    }

    /// Lists the databases the caller may read that `filter` admits, in name order, one page at
    /// a time: pass the page's `next_after` as the next filter's `after`. Retried.
    ///
    /// The control node filters among the databases the caller may read; a control node that
    /// predates the filters returns them all, so the client applies them again to the reply.
    ///
    /// # Errors
    ///
    /// `INVALID_ARGUMENT` for a label requirement the control node refuses, the status of the
    /// last attempt, or `INTERNAL` if a reply is invalid.
    pub async fn list_databases_where(&self, filter: &ListFilter) -> Result<Listing, Status> {
        let response = self
            .control
            .list_databases(ListDatabasesRequest {
                labels: filter.labels.iter().map(ToString::to_string).collect(),
                prefix: filter.prefix.clone().unwrap_or_default(),
                descriptions: filter.descriptions == Descriptions::Include,
                after: filter
                    .after
                    .as_ref()
                    .map(ToString::to_string)
                    .unwrap_or_default(),
                limit: filter.limit.unwrap_or(0),
            })
            .await?;
        let next_after = if response.next_after.is_empty() {
            None
        } else {
            Some(
                response
                    .next_after
                    .parse::<DatabaseName>()
                    .map_err(|error| {
                        Status::internal(format!("the reply has an invalid next_after: {error}"))
                    })?,
            )
        };
        let databases = response
            .databases
            .into_iter()
            .map(location)
            .collect::<Result<Vec<_>, _>>()?;
        let mut listing = filter.apply(databases);
        if listing.next_after.is_none() {
            listing.next_after = next_after;
        }
        Ok(listing)
    }

    /// Changes `name`'s description, labels, or expiry with `change`, a patch: what it doesn't
    /// name stays as it is. With `if_version`, the change applies only if the metadata is still
    /// at that version. The control node records the caller as the description's author when
    /// the description changes. Sent once per leader, not retried. Returns the metadata after
    /// the change.
    ///
    /// # Errors
    ///
    /// `NOT_FOUND` if there is no such database; `ABORTED` if the metadata isn't at
    /// `if_version`; `INVALID_ARGUMENT` for a change that changes nothing, sets and removes one
    /// key, leaves more than 32 labels, or sets an expiry less than a minute ahead;
    /// `UNAVAILABLE` if the control plane's time passed the expiry meanwhile, which a retry
    /// resolves; `FAILED_PRECONDITION` while a member lacks the `metadata` feature, or `expiry`
    /// for a change to the expiry, or when the control node runs a release without database
    /// metadata; or the status of the failed control call.
    pub async fn update_database(
        &self,
        name: &DatabaseName,
        change: &MetadataChange,
        if_version: Option<u64>,
    ) -> Result<MetadataView, Status> {
        self.update_database_with(name, change, ExpiryRequest::Keep, if_version)
            .await
    }

    /// Changes `name`'s metadata as [`Client::update_database`] does, with the expiry as
    /// `expiry` says: a duration is turned into a time by the control node, against its own
    /// clock, before it logs the update (ADR-0041, point 4).
    ///
    /// # Errors
    ///
    /// As [`Client::update_database`], and `INVALID_ARGUMENT` if `change` changes the expiry
    /// too.
    pub async fn update_database_with(
        &self,
        name: &DatabaseName,
        change: &MetadataChange,
        expiry: ExpiryRequest,
        if_version: Option<u64>,
    ) -> Result<MetadataView, Status> {
        let request = change_to_proto(name, change, expiry, if_version)?;
        let response = self
            .control
            .update_database(request)
            .await
            .map_err(without_metadata)?;
        response
            .metadata
            .map(metadata_view)
            .ok_or_else(|| Status::internal("the reply has no metadata"))
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

    /// Deletes the orphan copy of `name` that the data node at `address` (`host:port`) keeps
    /// (ADR-0042), and returns the highest epoch the copy had recorded. It needs the `fleet`
    /// scope.
    ///
    /// # Errors
    ///
    /// `NOT_FOUND` if the node holds no copy of `name`; `FAILED_PRECONDITION` if it serves or
    /// follows the name, or is retiring a copy of it; `UNIMPLEMENTED` if the node runs a
    /// release that keeps no orphans; and `UNAVAILABLE` if it can't be reached.
    pub async fn remove_orphan(
        &self,
        address: &str,
        name: &DatabaseName,
    ) -> Result<LeaseEpoch, Status> {
        let response = self
            .database(address)?
            .remove_orphan(RemoveOrphanRequest {
                database: name.to_string(),
            })
            .await
            .map_err(|status| {
                if unknown_call(&status) {
                    Status::unimplemented(WITHOUT_ORPHANS)
                } else {
                    status
                }
            })?
            .into_inner();
        Ok(LeaseEpoch::new(response.epoch))
    }

    /// Follows the leases the data node at `address` (`host:port`) holds: the first update holds
    /// every database it serves with its epoch, and each later one a lease it gained
    /// or lost, in the order the node records them (FR-042). The node reports a gained lease
    /// before it acknowledges a write under that epoch, and a lost one before it stops answering
    /// for the database.
    ///
    /// A node that hosts no data answers `UNIMPLEMENTED`, as it does every database call.
    ///
    /// # Errors
    ///
    /// `UNAVAILABLE` if the node can't be reached, or `UNIMPLEMENTED` if it hosts no data.
    pub async fn watch_leases(&self, address: &str) -> Result<LeaseUpdates, Status> {
        let inner = self
            .database(address)?
            .watch_leases(WatchLeasesRequest {})
            .await?
            .into_inner();
        Ok(LeaseUpdates { inner })
    }

    /// Asks the replica at `address` (`host:port`) what its copy of `name` hashes to at `at`,
    /// or at the latest position it holds, recomputed from its log on disk. Every replica
    /// answers for its own copy; `db verify` compares them.
    ///
    /// # Errors
    ///
    /// `UNAVAILABLE` if the node can't be reached or its chain isn't built yet, `NOT_FOUND` if
    /// it holds no copy or no hash at `at`, `FAILED_PRECONDITION` if its log can't be read back
    /// to `at`, and `INTERNAL` if its reply is invalid.
    pub async fn verify(
        &self,
        address: &str,
        name: &DatabaseName,
        at: Option<&Position>,
    ) -> Result<Verification, Status> {
        let response = self
            .database(address)?
            .verify(VerifyRequest {
                database: name.to_string(),
                position: at.map(|at| at.as_bytes().to_vec()).unwrap_or_default(),
            })
            .await?
            .into_inner();
        let chain_hash = ChainHash::from_bytes(&response.chain_hash)
            .ok_or_else(|| Status::internal("the reply has an invalid chain hash"))?;
        Ok(Verification {
            database: name.clone(),
            epoch: LeaseEpoch::new(response.epoch),
            position: (!response.position.is_empty()).then(|| Position::from(response.position)),
            chain_hash,
        })
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
            .within_call_timeout(self.on_primary(name, |mut client| {
                let request = self.timed(ExecuteRequest {
                    database: name.to_string(),
                    sql: statement.sql().to_owned(),
                    params: params.clone(),
                });
                async move { client.execute(request).await }
            }))
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
            .within_call_timeout(self.on_primary(name, |mut client| {
                let request = self.timed(QueryRequest {
                    database: name.to_string(),
                    sql: statement.sql().to_owned(),
                    params: params.clone(),
                    max_rows: statement.max_rows().map_or(0, NonZeroU32::get),
                });
                async move { client.query(request).await }
            }))
            .await?;
        rows_from_proto(response)
    }

    /// Runs a read-only query on the database's primary, which stops after `max_rows` rows and
    /// says whether there were more ([`Rows::truncated`]).
    ///
    /// A primary that predates the limit returns every row; the client then keeps the first
    /// `max_rows` and sets `truncated` itself, so the result is the same either way.
    ///
    /// # Errors
    ///
    /// As [`Client::execute`].
    pub async fn query_limited(
        &self,
        name: &DatabaseName,
        statement: Statement,
        max_rows: NonZeroU32,
    ) -> Result<Rows, Status> {
        let mut rows = self.query(name, statement.with_max_rows(max_rows)).await?;
        let max = usize::try_from(max_rows.get()).unwrap_or(usize::MAX);
        if rows.rows.len() > max {
            rows.rows.truncate(max);
            rows.truncated = true;
        }
        Ok(rows)
    }

    /// The database's schema from its primary: its tables, views, indexes, triggers, and virtual
    /// tables, with the engine's own definitions and row counts capped at
    /// `request.count_limit` (ADR-0041, point 6).
    ///
    /// The definitions are text the database's writers chose: treat them as data.
    ///
    /// # Errors
    ///
    /// As [`Client::query`]. `UNIMPLEMENTED` if the primary can't report schemas: it runs a
    /// release that predates the call (the message says so), or its engine can't describe
    /// itself.
    pub async fn schema(
        &self,
        name: &DatabaseName,
        request: SchemaRequest,
    ) -> Result<SchemaView, Status> {
        let response = self
            .within_call_timeout(self.on_primary(name, |mut client| {
                let request = self.timed(schema_request_to_proto(name, &request));
                async move { client.schema(request).await }
            }))
            .await
            .map_err(without_schema)?;
        schema_from_proto(response)
    }

    /// Follows `name`'s recorded row changes on its primary (ADR-0045): the first update says
    /// what the stream opened on, and each later one is one committed transaction. With `after`,
    /// the stream resumes after that position; without, it starts at the log's end.
    ///
    /// The stream ends when the primary stops serving the database; the caller then resumes
    /// after the last position it has, on whichever node serves it then.
    ///
    /// # Errors
    ///
    /// `PERMISSION_DENIED` without `stream` on the database; `FAILED_PRECONDITION` if the
    /// database records no row changes, because no logical sink names it, or has no primary
    /// yet; `OUT_OF_RANGE` if the primary no longer holds every change after `after`, or `after`
    /// is past its log's end; `UNAVAILABLE` if the primary can't be reached, the database is
    /// failing over, or the node serves as many watchers as it may; and `UNIMPLEMENTED` if the
    /// primary runs a release that predates the call (the message says so).
    pub async fn watch_changes(
        &self,
        name: &DatabaseName,
        after: Option<&Position>,
    ) -> Result<ChangeUpdates, Status> {
        let after = after.map(|position| position.as_bytes().to_vec());
        let inner = self
            .on_primary(name, |mut client| {
                let request = WatchChangesRequest {
                    database: name.to_string(),
                    after: after.clone(),
                };
                async move { client.watch_changes(request).await }
            })
            .await
            .map_err(without_watch)?;
        Ok(ChangeUpdates { inner })
    }

    /// The fleet's membership as the control plane sees it: the members with their addresses
    /// and liveness, the membership epoch, and the ring, rebuilt with the function every node
    /// uses, so its fingerprint matches theirs.
    ///
    /// A fleet without members has an empty view at epoch 0.
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

    /// Asks the control plane to create `name` as `spec` says, and returns its placement. A
    /// database that exists without a primary, because an earlier creation stopped halfway, is
    /// returned as it is, so the caller can finish it.
    ///
    /// A control node before 0.5.0 refuses a request that leaves the replication factor to the
    /// fleet, as it refuses any factor of 0; the request is then sent again with 3, the factor
    /// commands asked for before the fleet had a default of its own.
    ///
    /// With `metadata`, the creation goes through `CreateDatabaseWithMetadata`, which only
    /// control nodes that accept a factor of 0 know.
    async fn place(
        &self,
        name: &DatabaseName,
        spec: DatabaseSpec,
        metadata: Option<&NewMetadata>,
    ) -> Result<proto::Placement, Status> {
        let mut request = spec.to_request(name);
        let created = if let Some(metadata) = metadata {
            self.control
                .create_database_with_metadata(CreateDatabaseWithMetadataRequest {
                    database: Some(request),
                    metadata: Some(new_metadata_to_proto(metadata)),
                })
                .await
                .map_err(without_metadata)
        } else {
            let mut created = self.control.create_database(request.clone()).await;
            if request.replication_factor == 0 && created.as_ref().is_err_and(refuses_fleet_default)
            {
                request.replication_factor = u32::from(ReplicationFactor::default().get());
                created = self.control.create_database(request).await;
            }
            created
        };
        match created {
            Ok(response) => placement_of(response.placement),
            Err(status) if status.code() == Code::AlreadyExists => {
                let placement = self.placement(name).await?;
                if placement.primary.is_some() {
                    return Err(status);
                }
                Ok(placement)
            }
            Err(status) => Err(status),
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

    /// `message` as a request that carries the client's call timeout, if it has one, as its
    /// gRPC deadline.
    pub(crate) fn timed<T>(&self, message: T) -> tonic::Request<T> {
        let mut request = tonic::Request::new(message);
        if let Some(timeout) = self.call_timeout {
            request.set_timeout(timeout);
        }
        request
    }

    /// Runs `call`, failing it with `DEADLINE_EXCEEDED` once the client's call timeout, if it
    /// has one, passes. Dropping the call cancels it, which interrupts its statement on the node
    /// if the deadline it carried hasn't already.
    async fn within_call_timeout<T>(
        &self,
        call: impl Future<Output = Result<T, Status>>,
    ) -> Result<T, Status> {
        // A timeout past the clock's range is as good as none.
        let Some((timeout, deadline)) = self
            .call_timeout
            .and_then(|timeout| Some((timeout, tokio::time::Instant::now().checked_add(timeout)?)))
        else {
            return call.await;
        };
        let exceeded = || {
            Status::deadline_exceeded(format!(
                "the call took longer than its timeout of {timeout:?}"
            ))
        };
        match tokio::time::timeout_at(deadline, call).await {
            // The channel enforces the deadline the call carries too, and reports it as a
            // cancellation, which can arrive first.
            Ok(Err(status))
                if status.code() == Code::Cancelled && tokio::time::Instant::now() >= deadline =>
            {
                Err(exceeded())
            }
            Ok(result) => result,
            Err(_elapsed) => Err(exceeded()),
        }
    }

    /// Calls `call` on the database's primary, or on the entry node, following redirects.
    async fn on_primary<T, F, Fut>(&self, name: &DatabaseName, call: F) -> Result<T, Status>
    where
        F: Fn(DatabaseClient<CredentialedChannel>) -> Fut,
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
        F: Fn(DatabaseClient<CredentialedChannel>) -> Fut,
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

    fn database(&self, address: &str) -> Result<DatabaseClient<CredentialedChannel>, Status> {
        let mut channels = self.channels.lock().expect("client channel lock poisoned");
        if let Some(channel) = channels.get(address) {
            return Ok(database_client_with(
                channel.clone(),
                self.network.credentials(),
            ));
        }
        let channel = self
            .network
            .channel(address)
            .map_err(|error| Status::unavailable(format!("can't reach {address}: {error}")))?;
        channels.insert(address.to_owned(), channel.clone());
        Ok(database_client_with(channel, self.network.credentials()))
    }
}

/// Whether `status` is a control node before 0.5.0 refusing a creation that leaves the
/// replication factor to the fleet: those read 0 as a factor, and refuse it with this message.
fn refuses_fleet_default(status: &Status) -> bool {
    status.code() == Code::InvalidArgument
        && status
            .message()
            .starts_with("invalid replication factor 0: expected 1 to 255")
}

/// How long a creation or restore is retried while its node is still deleting a dropped
/// database of the same name: the node's flush limit, a renewal interval for it to notice the
/// drop, and a margin.
const DROPPING_RETRY_LIMIT: Duration = Duration::from_secs(45);

/// The longest wait between two of those retries.
const DROPPING_RETRY_MAX: Duration = Duration::from_secs(2);

/// Calls `call` until it succeeds, fails for another reason, or [`DROPPING_RETRY_LIMIT`] passes,
/// while its node answers that a dropped database named `name` is still being flushed and
/// deleted there ([`BEING_DROPPED`]), with backoff.
async fn retrying_while_dropping<T, F, Fut>(name: &DatabaseName, call: F) -> Result<T, Status>
where
    F: Fn() -> Fut,
    Fut: Future<Output = Result<T, Status>>,
{
    let deadline = tokio::time::Instant::now() + DROPPING_RETRY_LIMIT;
    let mut wait = Duration::from_millis(100);
    loop {
        match call().await {
            Err(status)
                if status.code() == Code::Unavailable
                    && status.message().ends_with(BEING_DROPPED)
                    && tokio::time::Instant::now() + wait < deadline =>
            {
                tracing::debug!(database = %name, retry_in = ?wait, "a dropped database of the name is still being deleted");
                tokio::time::sleep(wait).await;
                wait = (wait * 2).min(DROPPING_RETRY_MAX);
            }
            outcome => return outcome,
        }
    }
}

/// What a metadata call answers when the control node runs a release without database
/// metadata (ADR-0041, point 13).
const WITHOUT_METADATA: &str =
    "the control plane runs a release without database metadata; upgrade it first";

/// What `RemoveOrphan` answers when the node runs a release that predates orphans (ADR-0042).
const WITHOUT_ORPHANS: &str =
    "the node runs an older fleetringd, which keeps no orphans: it deleted such copies instead";

/// Whether `status` is a node refusing a call it doesn't serve: `UNIMPLEMENTED`, or, from a
/// release whose request boundary refuses every call its scope table doesn't list,
/// `PERMISSION_DENIED` saying the call has no scope requirement.
pub(crate) fn unknown_call(status: &Status) -> bool {
    match status.code() {
        Code::Unimplemented => true,
        Code::PermissionDenied => status.message().ends_with(" has no scope requirement"),
        _ => false,
    }
}

/// What a `Schema` call answers when the primary runs a release that predates it (ADR-0041,
/// point 6).
const WITHOUT_SCHEMA: &str = "the primary runs an older fleetringd, which can't report schemas";

/// Turns a refusal of a `Schema` call by a node that doesn't know it into `UNIMPLEMENTED` saying
/// so. A node that knows the call and whose engine can't describe itself already answers
/// `UNIMPLEMENTED` with a message of its own, which passes unchanged, as does any other status.
fn without_schema(status: Status) -> Status {
    let unknown = match status.code() {
        Code::Unimplemented => status.message().is_empty(),
        _ => unknown_call(&status),
    };
    if unknown {
        Status::unimplemented(WITHOUT_SCHEMA)
    } else {
        status
    }
}

/// What a `WatchChanges` call answers when the primary runs a release that predates it
/// (ADR-0045, point 11).
pub const WITHOUT_WATCH: &str =
    "the primary runs an older release of fleetring, which can't stream changes; upgrade it";

/// Turns a refusal of a `WatchChanges` call by a node that doesn't know it into `UNIMPLEMENTED`
/// saying so. A node that knows the call and whose engine can't record changes answers
/// `UNIMPLEMENTED` with a message of its own, which passes unchanged, as does any other status.
fn without_watch(status: Status) -> Status {
    let unknown = match status.code() {
        Code::Unimplemented => status.message().is_empty(),
        _ => unknown_call(&status),
    };
    if unknown {
        Status::unimplemented(WITHOUT_WATCH)
    } else {
        status
    }
}

/// Turns a refusal of a metadata call by a control node that doesn't know it into
/// `FAILED_PRECONDITION` saying so; any other status passes unchanged.
fn without_metadata(status: Status) -> Status {
    if unknown_call(&status) {
        Status::failed_precondition(WITHOUT_METADATA)
    } else {
        status
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_an_old_control_nodes_refusal_of_a_zero_factor_is_retried_with_three() {
        // What 0.3.0 and 0.4.0 answer to a factor of 0.
        let old = Status::invalid_argument("invalid replication factor 0: expected 1 to 255");
        assert!(refuses_fleet_default(&old));
        let current = Status::invalid_argument(
            "invalid replication factor 256: expected 1 to 255, or 0 for the fleet's default",
        );
        assert!(!refuses_fleet_default(&current));
        let other = Status::failed_precondition("invalid replication factor 0: expected 1 to 255");
        assert!(!refuses_fleet_default(&other));
    }

    #[test]
    fn a_control_node_without_metadata_is_named_as_such() {
        // What 0.5.0's request boundary answers for a call its table doesn't list.
        let boundary = Status::permission_denied(
            "/fleetring.v1.Control/CreateDatabaseWithMetadata has no scope requirement",
        );
        let unimplemented = Status::unimplemented("");
        for refused in [boundary, unimplemented] {
            let status = without_metadata(refused);
            assert_eq!(status.code(), Code::FailedPrecondition);
            assert_eq!(status.message(), WITHOUT_METADATA);
        }
        let scope =
            Status::permission_denied("CreateDatabaseWithMetadata needs admin on 'agent-420'");
        assert_eq!(without_metadata(scope).code(), Code::PermissionDenied);
    }

    #[test]
    fn a_primary_without_schemas_is_named_as_such() {
        // What a 0.5 or phase-2 node answers for a call its table doesn't list, and what tonic
        // answers for a method its service lacks.
        let boundary =
            Status::permission_denied("/fleetring.v1.Database/Schema has no scope requirement");
        let missing = Status::unimplemented("");
        for refused in [boundary, missing] {
            let status = without_schema(refused);
            assert_eq!(status.code(), Code::Unimplemented);
            assert_eq!(status.message(), WITHOUT_SCHEMA);
        }
        // A real refusal, and an engine that can't describe itself, are told apart.
        let scope = Status::permission_denied("Schema needs read on 'notes'");
        assert_eq!(without_schema(scope).code(), Code::PermissionDenied);
        let engine = Status::unimplemented("the engine doesn't support schema");
        assert_eq!(
            without_schema(engine).message(),
            "the engine doesn't support schema"
        );
    }

    #[test]
    fn a_primary_that_cant_stream_changes_is_named_as_an_older_release() {
        // What a 0.7 node answers: its request boundary doesn't list the call, or, past it, its
        // service lacks the method.
        let boundary = Status::permission_denied(
            "/fleetring.v1.Database/WatchChanges has no scope requirement",
        );
        let missing = Status::unimplemented("");
        for refused in [boundary, missing] {
            let status = without_watch(refused);
            assert_eq!(status.code(), Code::Unimplemented);
            assert_eq!(status.message(), WITHOUT_WATCH);
        }
        let scope = Status::permission_denied("WatchChanges needs stream on 'notes'");
        assert_eq!(without_watch(scope).code(), Code::PermissionDenied);
        let engine = Status::unimplemented("the engine doesn't support watching changes");
        assert_eq!(
            without_watch(engine).message(),
            "the engine doesn't support watching changes"
        );
    }

    #[test]
    fn a_spec_leaves_unset_settings_to_the_fleet() {
        let name: DatabaseName = "notes".parse().expect("a name");
        let request = DatabaseSpec::default().to_request(&name);
        assert_eq!(request.replication_factor, 0);
        assert_eq!(request.acks, i32::from(proto::Acks::Unspecified));
        assert_eq!(request.min_replicas, 0);
        let two = ReplicationFactor::new(2).expect("nonzero");
        assert_eq!(
            DatabaseSpec::new(two).to_request(&name).replication_factor,
            2
        );
    }
}
