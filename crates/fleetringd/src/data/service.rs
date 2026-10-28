//! The gRPC `Database` service over a data node (ADR-0019).

use fleetring_control::DatabaseName;
use fleetring_proto::v1::database_server::Database;
use fleetring_proto::v1::{
    CreateRequest, CreateResponse, ExecuteRequest, ExecuteResponse, QueryRequest, QueryResponse,
};
use tonic::{Request, Response, Status};

use crate::DatabaseError;
use crate::data::convert::{parse, rows_to_proto, statement};
use crate::data::node::DataNode;
use crate::engine::{EngineError, Transaction};

/// How many replicas hold a write once its acknowledgement completes.
///
/// No follower stores writes yet, so only the primary does. Replication (FR-030) replaces this
/// with the count its acknowledgement reports.
const ACKNOWLEDGED_BY: u32 = 1;

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
/// - `UNAVAILABLE`: the database closed before it took the request, which never ran; retry.
/// - `ABORTED`: the request may or may not have taken effect: the database stopped after taking
///   it, or replication failed after the write committed locally.
/// - `NOT_FOUND`: the database has no copy here.
/// - `UNIMPLEMENTED`: the engine can't run this kind of request.
/// - `INTERNAL`: storage failed.
pub(crate) fn database_status(error: &DatabaseError) -> Status {
    let message = error.to_string();
    match error {
        DatabaseError::Closed { .. } => Status::unavailable(message),
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
        let executed = self
            .node
            .registry()
            .execute(&name, Transaction::from(statement))
            .await
            .map_err(|error| database_status(&error))?;
        Ok(Response::new(ExecuteResponse {
            rows_affected: executed.rows_affected,
            acknowledged_by: ACKNOWLEDGED_BY,
            under_replicated: ACKNOWLEDGED_BY < lease.replicas,
            replicas: lease.replicas,
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
}
