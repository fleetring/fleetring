//! The gRPC `Database` service over a data node (ADR-0019).

use fleetring_control::DatabaseName;
use fleetring_proto::v1::database_server::Database;
use fleetring_proto::v1::restore_point::Point;
use fleetring_proto::v1::{
    ArchivedSnapshot, ChangeSinkStatus, CreateRequest, CreateResponse, DescribeArchiveRequest,
    DescribeArchiveResponse, ExecuteRequest, ExecuteResponse, NodeStatusRequest,
    NodeStatusResponse, QueryRequest, QueryResponse, RestorePoint, RestoreRequest, RestoreResponse,
    VerifyArchiveRequest, VerifyArchiveResponse, VerifyRequest, VerifyResponse, WatchLeasesRequest,
};
use tonic::{Request, Response, Status};

use crate::auth::{Action, authorized, require_database};
use crate::changes::MarkHome;
use crate::data::convert::{parse, rows_to_proto, statement};
use crate::data::leases::{LeaseStream, STREAM_BUFFER, lease_stream};
use crate::data::node::{DataNode, restore_status};
use crate::durability::{Listing, RestoreTarget, restore_into};
use crate::engine::{EngineError, Position, Transaction};
use crate::{Acknowledged, DatabaseError, DatabaseStatuses};

/// The `Database` service: runs requests for databases this node is primary for, and redirects
/// the rest (see [`not_primary`](crate::data::not_primary)).
#[derive(Debug, Clone)]
pub(crate) struct DatabaseService {
    node: DataNode,
}

impl DatabaseService {
    pub(crate) fn new(node: DataNode) -> Self {
        Self { node }
    }
}

/// Maps a database error to the status a client receives, with the error's message.
///
/// - `INVALID_ARGUMENT`: the statement failed, or tried to control transactions; nothing
///   committed.
/// - `UNAVAILABLE`: the database closed before it took the request, or replication refused the
///   write because fewer than its `min_replicas` were reachable (ADR-0023); either way it never
///   ran; retry.
/// - `ABORTED`: the request may or may not have taken effect: the database stopped after taking
///   it, or the write committed on the primary but replication couldn't store it on a write
///   quorum, because too few replicas are reachable (under-replicated) or they didn't acknowledge
///   it in time. Such a write may still reach the followers later.
/// - `NOT_FOUND`: the database has no copy here.
/// - `UNIMPLEMENTED`: the engine can't run this kind of request.
/// - `INTERNAL`: storage failed.
pub(crate) fn database_status(error: &DatabaseError) -> Status {
    let message = error.to_string();
    match error {
        DatabaseError::Closed { .. } | DatabaseError::Refused { .. } => {
            Status::unavailable(message)
        }
        DatabaseError::Interrupted { .. } | DatabaseError::Replication(_) => {
            Status::aborted(message)
        }
        DatabaseError::Unsupported { .. } => Status::unimplemented(message),
        DatabaseError::Open { source, .. } => match **source {
            EngineError::NotFound => Status::not_found(message),
            EngineError::AlreadyExists => Status::already_exists(message),
            _ => Status::internal(format!("{message}: {source}")),
        },
        DatabaseError::Engine(source) => engine_status(source),
        DatabaseError::Failed { source, .. } => {
            Status::internal(format!("{message}: {source}; the write may have committed"))
        }
    }
}

fn engine_status(error: &EngineError) -> Status {
    let message = error.to_string();
    match error {
        EngineError::Statement { .. } | EngineError::TransactionControl => {
            Status::invalid_argument(message)
        }
        EngineError::NotFound => Status::not_found(message),
        EngineError::AlreadyExists => Status::already_exists(message),
        _ => Status::internal(message),
    }
}

/// The restore target a request's point names: the latest point if it names none.
///
/// # Errors
///
/// `INVALID_ARGUMENT` for an empty position.
fn restore_target(point: Option<RestorePoint>) -> Result<RestoreTarget, Status> {
    match point.and_then(|point| point.point) {
        None | Some(Point::Latest(_)) => Ok(RestoreTarget::Latest),
        Some(Point::Position(position)) if position.is_empty() => Err(Status::invalid_argument(
            "a restore point's position is empty",
        )),
        Some(Point::Position(position)) => Ok(RestoreTarget::Position(Position::from(position))),
        Some(Point::TimeMs(ms)) => Ok(RestoreTarget::Time(ms)),
    }
}

#[tonic::async_trait]
impl Database for DatabaseService {
    type WatchLeasesStream = LeaseStream;

    async fn execute(
        &self,
        request: Request<ExecuteRequest>,
    ) -> Result<Response<ExecuteResponse>, Status> {
        let name: DatabaseName = parse("database name", &request.get_ref().database)?;
        require_database(&request, &name)?;
        let request = request.into_inner();
        let statement = statement(request.sql, request.params)?;
        let lease = self.node.route(&name).await?;
        let stored = self
            .node
            .registry()
            .execute(&name, Transaction::from(statement))
            .await
            .map_err(|error| database_status(&error))?;
        // A write that committed no batch has nothing new to replicate: report how many replicas
        // hold everything committed so far.
        let acknowledged = stored
            .acknowledged
            .or_else(|| self.node.replication().holders(&name))
            .unwrap_or_else(|| Acknowledged::new(1, 1, lease.replicas()));
        Ok(Response::new(ExecuteResponse {
            rows_affected: stored.rows_affected,
            acknowledged_by: acknowledged.acknowledged_by,
            under_replicated: acknowledged.is_under_replicated(),
            replicas: acknowledged.replicas,
            primary: self.node.id().to_string(),
            epoch: lease.epoch.get(),
        }))
    }

    async fn query(
        &self,
        request: Request<QueryRequest>,
    ) -> Result<Response<QueryResponse>, Status> {
        let name: DatabaseName = parse("database name", &request.get_ref().database)?;
        require_database(&request, &name)?;
        let request = request.into_inner();
        let statement = statement(request.sql, request.params)?;
        self.node.route(&name).await?;
        let rows = self
            .node
            .registry()
            .query(&name, statement)
            .await
            .map_err(|error| database_status(&error))?;
        Ok(Response::new(rows_to_proto(rows)))
    }

    async fn create(
        &self,
        request: Request<CreateRequest>,
    ) -> Result<Response<CreateResponse>, Status> {
        let name: DatabaseName = parse("database name", &request.get_ref().database)?;
        require_database(&request, &name)?;
        let placement = self.node.create(&name).await?;
        Ok(Response::new(CreateResponse {
            placement: Some(placement),
        }))
    }

    /// Answers from this node alone: its published membership view and the databases its
    /// replication serves or follows, as its heartbeats report them.
    async fn node_status(
        &self,
        request: Request<NodeStatusRequest>,
    ) -> Result<Response<NodeStatusResponse>, Status> {
        let caller = authorized(&request)?;
        let replication = self.node.replication();
        let view = replication.membership_view();
        let sinks = self
            .node
            .change_streams()
            .status()
            .into_iter()
            .filter(|status| caller.sees(Action::Stream, &status.database))
            .map(|status| ChangeSinkStatus {
                sink: status.sink,
                database: status.database.to_string(),
                position: status
                    .position
                    .map(Position::into_bytes)
                    .unwrap_or_default(),
                acknowledged: status
                    .acknowledged
                    .map(Position::into_bytes)
                    .unwrap_or_default(),
                backlog: status.backlog,
                failures: status.failures,
                resets: status.resets,
                last_error: status.last_error.unwrap_or_default(),
                replicated: status.home == Some(MarkHome::Replicated),
            })
            .collect();
        Ok(Response::new(NodeStatusResponse {
            node_id: self.node.id().to_string(),
            membership_epoch: view.epoch().get(),
            ring_fingerprint: view.fingerprint().map(|fingerprint| fingerprint.get()),
            databases: replication
                .statuses()
                .into_iter()
                .filter(|status| {
                    status
                        .database
                        .parse::<DatabaseName>()
                        .is_ok_and(|name| caller.sees(Action::Read, &name))
                })
                .collect(),
            sinks,
        }))
    }

    /// Streams the leases this node holds, from its replication's own view (FR-042): a snapshot
    /// first, then one event per lease gained or lost. A subscriber more than
    /// [`STREAM_BUFFER`] updates behind is cut off with `RESOURCE_EXHAUSTED`.
    async fn watch_leases(
        &self,
        request: Request<WatchLeasesRequest>,
    ) -> Result<Response<Self::WatchLeasesStream>, Status> {
        let caller = authorized(&request)?.clone();
        let watch = self.node.replication().leases();
        let visible = caller.clone();
        let stream = lease_stream(
            self.node.id().clone(),
            watch,
            STREAM_BUFFER,
            move |database| visible.sees(Action::Read, database),
        );
        Ok(Response::new(caller.guard(stream)))
    }

    /// Restores `source` from the archive into the new database `database`, which needs admin
    /// on both: the call creates `database`, and copies `source`'s history into it, including
    /// what its caller's scopes may no longer show (ADR-0029, point 17).
    async fn restore(
        &self,
        request: Request<RestoreRequest>,
    ) -> Result<Response<RestoreResponse>, Status> {
        let name: DatabaseName = parse("database name", &request.get_ref().database)?;
        let source: DatabaseName = parse("source database name", &request.get_ref().source)?;
        require_database(&request, &name)?;
        require_database(&request, &source)?;
        let request = request.into_inner();
        let target = restore_target(request.point)?;
        let archive = self.node.archive(&request.archive)?;
        let (placement, restored) = self.node.restore(&name, archive, &source, &target).await?;
        Ok(Response::new(RestoreResponse {
            placement: Some(placement),
            position: restored
                .position
                .map(Position::into_bytes)
                .unwrap_or_default(),
            chain_hash: restored.chain.as_bytes().to_vec(),
            snapshot: restored.snapshot,
            segments: restored.segments,
            archived_at_ms: restored.archived_at_ms,
        }))
    }

    /// Lists what the archive holds for the database, from any node that has the archive.
    async fn describe_archive(
        &self,
        request: Request<DescribeArchiveRequest>,
    ) -> Result<Response<DescribeArchiveResponse>, Status> {
        let name: DatabaseName = parse("database name", &request.get_ref().database)?;
        require_database(&request, &name)?;
        let archive = self.node.archive(&request.get_ref().archive)?;
        let store = archive.store();
        let listing = Listing::read(store.as_ref(), &archive.keys(&name), &name)
            .await
            .map_err(|error| {
                Status::unavailable(format!("can't list archive `{}`: {error}", archive.name()))
            })?;
        let (latest, latest_ms) = listing.latest();
        let mut snapshots: Vec<ArchivedSnapshot> = listing
            .snapshots
            .iter()
            .map(|snapshot| ArchivedSnapshot {
                id: snapshot.id.clone(),
                epoch: snapshot.epoch.get(),
                position: snapshot
                    .position
                    .clone()
                    .map(Position::into_bytes)
                    .unwrap_or_default(),
                taken_at_ms: snapshot.descriptor.taken_at_ms,
                bytes: snapshot.descriptor.bytes,
            })
            .collect();
        snapshots.sort_by(|a, b| (&a.position, a.epoch).cmp(&(&b.position, b.epoch)));
        Ok(Response::new(DescribeArchiveResponse {
            database: name.to_string(),
            snapshots,
            latest: latest.map(Position::into_bytes).unwrap_or_default(),
            latest_ms,
            segments: u32::try_from(listing.segments.len()).unwrap_or(u32::MAX),
            segment_bytes: listing.segments.iter().map(|segment| segment.size).sum(),
            incomplete: u32::try_from(listing.incomplete.len()).unwrap_or(u32::MAX),
            damaged: listing
                .damaged
                .iter()
                .map(|damaged| format!("{}: {}", damaged.key, damaged.reason))
                .collect(),
        }))
    }

    /// Restores the archived database into a scratch directory beside this node's databases,
    /// verifying every batch, and removes the directory again.
    async fn verify_archive(
        &self,
        request: Request<VerifyArchiveRequest>,
    ) -> Result<Response<VerifyArchiveResponse>, Status> {
        let name: DatabaseName = parse("database name", &request.get_ref().database)?;
        require_database(&request, &name)?;
        let request = request.into_inner();
        let target = restore_target(request.point)?;
        let archive = self.node.archive(&request.archive)?;
        let replication = self.node.replication();
        let scratch = replication.restore_staging();
        let restored = restore_into(
            archive.store().as_ref(),
            &archive.keys(&name),
            &name,
            replication.engine(),
            &target,
            &scratch,
            replication.execution(),
        )
        .await
        .map_err(|error| restore_status(&name, archive.name(), &error));
        let removed = {
            let scratch = scratch.clone();
            crate::durability::blocking(replication.execution(), move || {
                crate::replication::remove_dir_if_present(&scratch)
            })
            .await
        };
        if !matches!(removed, Ok(Ok(()))) {
            tracing::warn!(dir = %scratch.display(), "can't remove a verified archive's scratch copy");
        }
        let restored = restored?;
        Ok(Response::new(VerifyArchiveResponse {
            position: restored
                .position
                .map(Position::into_bytes)
                .unwrap_or_default(),
            chain_hash: restored.chain.as_bytes().to_vec(),
            snapshot: restored.snapshot,
            segments: restored.segments,
        }))
    }

    /// Answers from this node's own copy, whatever its role: the chain hash at the requested
    /// position, or at the copy's end, recomputed from the log on disk.
    async fn verify(
        &self,
        request: Request<VerifyRequest>,
    ) -> Result<Response<VerifyResponse>, Status> {
        let name: DatabaseName = parse("database name", &request.get_ref().database)?;
        require_database(&request, &name)?;
        let request = request.into_inner();
        let at = (!request.position.is_empty()).then(|| Position::from(request.position));
        let verified = self.node.replication().verify(&name, at).await?;
        Ok(Response::new(VerifyResponse {
            database: name.to_string(),
            epoch: verified.epoch.get(),
            position: verified
                .position
                .map(Position::into_bytes)
                .unwrap_or_default(),
            chain_hash: verified.hash.as_bytes().to_vec(),
        }))
    }
}
