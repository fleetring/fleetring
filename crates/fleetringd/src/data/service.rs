//! The gRPC `Database` service over a data node (ADR-0019).

use fleetring_control::DatabaseName;
use fleetring_proto::v1::database_server::Database;
use fleetring_proto::v1::{
    CreateRequest, CreateResponse, ExecuteRequest, ExecuteResponse, NodeStatusRequest,
    NodeStatusResponse, QueryRequest, QueryResponse,
};
use tonic::{Request, Response, Status};

use crate::data::convert::{parse, rows_to_proto, statement};
use crate::data::node::DataNode;
use crate::engine::{EngineError, Transaction};
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

#[tonic::async_trait]
impl Database for DatabaseService {
    async fn execute(
        &self,
        request: Request<ExecuteRequest>,
    ) -> Result<Response<ExecuteResponse>, Status> {
        let request = request.into_inner();
        let name: DatabaseName = parse("database name", &request.database)?;
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
        let request = request.into_inner();
        let name: DatabaseName = parse("database name", &request.database)?;
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
        let placement = self.node.create(&name).await?;
        Ok(Response::new(CreateResponse {
            placement: Some(placement),
        }))
    }

    /// Answers from this node alone: its published membership view and the databases its
    /// replication serves or follows, as its heartbeats report them.
    async fn node_status(
        &self,
        _request: Request<NodeStatusRequest>,
    ) -> Result<Response<NodeStatusResponse>, Status> {
        let replication = self.node.replication();
        let view = replication.membership_view();
        Ok(Response::new(NodeStatusResponse {
            node_id: self.node.id().to_string(),
            membership_epoch: view.epoch().get(),
            ring_fingerprint: view.fingerprint().map(|fingerprint| fingerprint.get()),
            databases: replication.statuses(),
        }))
    }
}
