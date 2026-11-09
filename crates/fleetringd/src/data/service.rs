//! The gRPC `Database` service over a data node (ADR-0019).

use std::num::NonZeroU32;
use std::time::Duration;

use fleetring_client::redirect::primary_hint;
use fleetring_control::DatabaseName;
use fleetring_proto::v1::database_server::Database;
use fleetring_proto::v1::restore_point::Point;
use fleetring_proto::v1::{
    ArchivedSnapshot, ChangeSinkStatus, CreateRequest, CreateResponse, DescribeArchiveRequest,
    DescribeArchiveResponse, ExecuteRequest, ExecuteResponse, NodeStatusRequest,
    NodeStatusResponse, OrphanStatus, QueryRequest, QueryResponse, RemoveOrphanRequest,
    RemoveOrphanResponse, RestorePoint, RestoreRequest, RestoreResponse, SchemaRequest,
    SchemaResponse, VerifyArchiveRequest, VerifyArchiveResponse, VerifyRequest, VerifyResponse,
    WatchChangesRequest, WatchLeasesRequest,
};
use tokio::time::Instant;
use tonic::{Code, Request, Response, Status};

use crate::actor::{Admission, Admit};
use crate::auth::{Action, authorized, require_database};
use crate::changes::MarkHome;
use crate::data::convert::{
    archived_to_proto, parse, rows_to_proto, schema_request_from_proto, schema_to_proto, statement,
};
use crate::data::leases::{LeaseStream, STREAM_BUFFER, lease_stream};
use crate::data::node::{DataNode, restore_status};
use crate::data::session::{self, SessionStream};
use crate::data::watch::{self, ChangeStream};
use crate::durability::{Listing, RestoreTarget, restore_into};
use crate::engine::{EngineError, Interruption, Position, Transaction};
use crate::observe::DataMetrics;
use crate::{Acknowledged, DatabaseError, DatabaseStatuses, StatementLimit};

/// The `Database` service: runs requests for databases this node is primary for, and redirects
/// the rest (see [`not_primary`](fleetring_client::redirect::not_primary)).
#[derive(Debug, Clone)]
pub(crate) struct DatabaseService {
    node: DataNode,
}

impl DatabaseService {
    pub(crate) fn new(node: DataNode) -> Self {
        Self { node }
    }

    /// When the statement `request` carries must stop: the node's statement timeout from now,
    /// or the call's own deadline if it set an earlier one (FR-078).
    fn deadline<T>(&self, request: &Request<T>) -> Instant {
        statement_deadline(request, self.node.statement_timeout())
    }

    /// How long the write `request` carries may run, and wait: see [`write_limit`].
    fn write_limit<T>(&self, request: &Request<T>) -> StatementLimit {
        write_limit(
            request,
            self.node.statement_timeout(),
            self.node.registry().session_limits().transaction_timeout,
        )
    }

    async fn run_execute(
        &self,
        request: Request<ExecuteRequest>,
    ) -> Result<Response<ExecuteResponse>, Status> {
        let limit = self.write_limit(&request);
        let name: DatabaseName = parse("database name", &request.get_ref().database)?;
        require_database(&request, &name)?;
        let request = request.into_inner();
        let statement = statement(request.sql, request.params)?;
        let lease = self.node.route(&name).await?.lease();
        // Admitted again as it starts, under the same lease: a write that waits behind a SQL
        // session's transaction never holds up a drain while it waits.
        let node = &self.node;
        let admitting = name.clone();
        let admit: Admit<'_> = Box::new(move || {
            node.admit_now(&admitting, lease.epoch)
                .map(|admitted| Box::new(admitted) as Admission<'_>)
        });
        let (published, admitted) = self
            .node
            .registry()
            .commit_until(&name, Transaction::from(statement), Some(limit), admit)
            .await
            .map_err(|error| database_status(&error))?;
        // The write is in the database's log now, so a drain may step down: it ships the tail,
        // this write included, before it does.
        drop(admitted);
        let stored = published
            .stored()
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

    async fn run_query(
        &self,
        request: Request<QueryRequest>,
    ) -> Result<Response<QueryResponse>, Status> {
        let deadline = self.deadline(&request);
        let name: DatabaseName = parse("database name", &request.get_ref().database)?;
        require_database(&request, &name)?;
        let request = request.into_inner();
        let mut statement = statement(request.sql, request.params)?;
        if let Some(max) = NonZeroU32::new(request.max_rows) {
            statement = statement.with_max_rows(max);
        }
        let _admitted = self.node.route(&name).await?;
        let rows = self
            .node
            .registry()
            .query_until(&name, statement, deadline)
            .await
            .map_err(|error| database_status(&error))?;
        Ok(Response::new(rows_to_proto(rows)))
    }

    async fn run_schema(
        &self,
        request: Request<SchemaRequest>,
    ) -> Result<Response<SchemaResponse>, Status> {
        let deadline = self.deadline(&request);
        let name: DatabaseName = parse("database name", &request.get_ref().database)?;
        require_database(&request, &name)?;
        let schema_request = schema_request_from_proto(request.get_ref());
        let admitted = self.node.route(&name).await?;
        let lease = admitted.lease();
        let schema = self
            .node
            .registry()
            .schema_until(&name, schema_request, deadline)
            .await
            .map_err(|error| database_status(&error))?;
        Ok(Response::new(schema_to_proto(
            schema,
            self.node.replication().engine().kind(),
            self.node.id(),
            lease.epoch,
        )))
    }
}

/// The most a statement's deadline comes before its call's own: see [`statement_deadline`].
const DEADLINE_MARGIN: Duration = Duration::from_millis(100);

/// The deadline of the statement `request` carries: `limit` from now, or the call's own
/// `grpc-timeout` if that is shorter, less a margin. A malformed header is ignored, leaving the
/// limit.
///
/// tonic enforces a call's `grpc-timeout` itself, from a timer that starts when the request
/// arrives: once it fires it drops the handler and answers `CANCELLED`, `Timeout expired`. The
/// node's own deadline therefore comes a tenth of the asked time earlier, at most
/// [`DEADLINE_MARGIN`], so that the interrupt stops the statement and the handler answers
/// `DEADLINE_EXCEEDED`, saying why, before tonic's timer does.
fn statement_deadline<T>(request: &Request<T>, limit: Duration) -> Instant {
    let asked = request
        .metadata()
        .get(GRPC_TIMEOUT)
        .and_then(|value| value.to_str().ok())
        .and_then(grpc_timeout)
        .map(|asked| asked.saturating_sub((asked / 10).min(DEADLINE_MARGIN)));
    let timeout = asked.map_or(limit, |asked| asked.min(limit));
    let now = Instant::now();
    // A limit too large for the clock means no limit worth keeping: a year stands in for it.
    now.checked_add(timeout)
        .unwrap_or_else(|| now + Duration::from_secs(365 * 24 * 60 * 60))
}

/// How long the write `request` carries may run, and wait to start (FR-078, ADR-0037).
///
/// Without a deadline of the call's own, its statements may run for `limit`, the node's
/// statement timeout, from when the write starts, and it may wait to start for as long as a SQL
/// session's transaction may hold the writer, `transaction`, besides: a write queued behind a
/// transaction of full length still gets its whole statement time. With one, the call's own
/// deadline less its margin (see [`statement_deadline`]) bounds both.
fn write_limit<T>(request: &Request<T>, limit: Duration, transaction: Duration) -> StatementLimit {
    let now = Instant::now();
    let far = |timeout: Duration| {
        now.checked_add(timeout)
            .unwrap_or_else(|| now + Duration::from_secs(365 * 24 * 60 * 60))
    };
    match asked(request) {
        Some(asked) => StatementLimit {
            budget: asked.min(limit),
            latest: far(asked),
        },
        None => StatementLimit {
            budget: limit,
            latest: far(limit.saturating_add(transaction)),
        },
    }
}

/// The time a call's own `grpc-timeout` gives its statement, less the margin, if it set one.
fn asked<T>(request: &Request<T>) -> Option<Duration> {
    request
        .metadata()
        .get(GRPC_TIMEOUT)
        .and_then(|value| value.to_str().ok())
        .and_then(grpc_timeout)
        .map(|asked| asked.saturating_sub((asked / 10).min(DEADLINE_MARGIN)))
}

/// The header a gRPC client sends its call's timeout in.
const GRPC_TIMEOUT: &str = "grpc-timeout";

/// A `grpc-timeout` header's duration: at most eight digits and a unit, `H`, `M`, `S`, `m`
/// (milliseconds), `u` (microseconds), or `n` (nanoseconds), as the gRPC protocol over HTTP/2
/// defines it.
fn grpc_timeout(text: &str) -> Option<Duration> {
    let (digits, unit) = text.split_at(text.len().checked_sub(1)?);
    if digits.is_empty() || digits.len() > 8 || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    let value: u64 = digits.parse().ok()?;
    match unit {
        "H" => Some(Duration::from_secs(value.checked_mul(60 * 60)?)),
        "M" => Some(Duration::from_secs(value.checked_mul(60)?)),
        "S" => Some(Duration::from_secs(value)),
        "m" => Some(Duration::from_millis(value)),
        "u" => Some(Duration::from_micros(value)),
        "n" => Some(Duration::from_nanos(value)),
        _ => None,
    }
}

/// How an `Execute` that failed with `status` is counted: `redirected` to the primary,
/// `refused` before it ran (no primary here to name, or no such database), `aborted` when it may
/// or may not have taken effect, `cancelled` when its call was, and `failed` otherwise.
fn write_outcome(status: &Status) -> &'static str {
    if primary_hint(status).is_some() {
        return "redirected";
    }
    match status.code() {
        Code::Unavailable | Code::NotFound => "refused",
        Code::Aborted => "aborted",
        Code::Cancelled => "cancelled",
        _ => "failed",
    }
}

/// Which call [`CountedOnDrop`] counts.
#[derive(Debug, Clone, Copy)]
enum Call {
    Write,
    Query,
}

/// Counts its call as `cancelled` when dropped before [`disarm`](Self::disarm): the caller
/// cancelled the call, or its deadline passed, and tonic dropped the handler mid-flight.
struct CountedOnDrop<'m> {
    metrics: &'m DataMetrics,
    call: Call,
    armed: bool,
}

impl<'m> CountedOnDrop<'m> {
    fn new(metrics: &'m DataMetrics, call: Call) -> Self {
        Self {
            metrics,
            call,
            armed: true,
        }
    }

    /// The call ended with an outcome of its own, which its caller counts.
    fn disarm(mut self) {
        self.armed = false;
    }
}

impl Drop for CountedOnDrop<'_> {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        match self.call {
            Call::Write => self.metrics.write("cancelled", None),
            Call::Query => self.metrics.query("cancelled", None),
        }
    }
}

/// Maps a database error to the status a client receives, with the error's message.
///
/// - `INVALID_ARGUMENT`: the statement failed, or tried to control transactions; nothing
///   committed.
/// - `DEADLINE_EXCEEDED`: the statement ran past its deadline and was interrupted; nothing
///   committed (FR-078).
/// - `CANCELLED`: the call was cancelled, which interrupted the statement; nothing committed.
/// - `UNAVAILABLE`: the database closed before it took the request, or replication refused the
///   write because fewer than its `min_replicas` were reachable (ADR-0023); either way it never
///   ran; retry.
/// - `ABORTED`: the request may or may not have taken effect: the database stopped after taking
///   it, or the write committed on the primary but replication couldn't store it on a write
///   quorum, because too few replicas are reachable (under-replicated) or they didn't acknowledge
///   it in time. Such a write may still reach the followers later.
/// - `RESOURCE_EXHAUSTED`: a SQL session's transaction holds the writer, and too many writes
///   wait for it already; the request never ran (ADR-0037).
/// - `NOT_FOUND`: the database has no copy here.
/// - `UNIMPLEMENTED`: the engine can't run this kind of request.
/// - `INTERNAL`: storage failed.
pub(crate) fn database_status(error: &DatabaseError) -> Status {
    let message = error.to_string();
    match error {
        DatabaseError::Closed { .. } | DatabaseError::Refused { .. } => {
            Status::unavailable(message)
        }
        DatabaseError::Busy { .. } => Status::resource_exhausted(message),
        DatabaseError::TransactionTimedOut { .. } | DatabaseError::TransactionLost { .. } => {
            Status::aborted(message)
        }
        DatabaseError::NoSessions { .. } => Status::failed_precondition(message),
        DatabaseError::NotAdmitted { .. } => Status::unavailable(message),
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
        EngineError::Interrupted(Interruption::Deadline) => Status::deadline_exceeded(message),
        EngineError::Interrupted(_) => Status::cancelled(message),
        EngineError::NotFound => Status::not_found(message),
        EngineError::AlreadyExists => Status::already_exists(message),
        EngineError::Unsupported { .. } => Status::unimplemented(message),
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
    type WatchChangesStream = ChangeStream;
    type SessionStream = SessionStream;

    /// Opens a SQL session on the database's primary for one Hrana stream (ADR-0037). The
    /// request boundary has found the caller; this checks `read` on the database the first
    /// message names, and `write` to decide whether the session may write.
    async fn session(
        &self,
        request: Request<tonic::Streaming<fleetring_proto::v1::SessionRequest>>,
    ) -> Result<Response<Self::SessionStream>, Status> {
        let stream = session::open(&self.node, request).await?;
        Ok(Response::new(stream))
    }

    /// Streams the database's recorded row changes from its primary (ADR-0045). The request
    /// boundary has found the caller; this checks `stream` on the database the request names.
    async fn watch_changes(
        &self,
        request: Request<WatchChangesRequest>,
    ) -> Result<Response<Self::WatchChangesStream>, Status> {
        let name: DatabaseName = parse("database name", &request.get_ref().database)?;
        require_database(&request, &name)?;
        let caller = authorized(&request)?.clone();
        let after = match request.into_inner().after {
            None => None,
            Some(bytes) if bytes.is_empty() => {
                return Err(Status::invalid_argument(
                    "`after` is empty; leave it unset for a live tail",
                ));
            }
            Some(bytes) => Some(Position::from(bytes)),
        };
        let stream = watch::open(&self.node, &name, after).await?;
        Ok(Response::new(caller.guard(stream)))
    }

    async fn execute(
        &self,
        request: Request<ExecuteRequest>,
    ) -> Result<Response<ExecuteResponse>, Status> {
        let started = tokio::time::Instant::now();
        let metrics = self.node.metrics();
        let counted = CountedOnDrop::new(metrics, Call::Write);
        let result = self.run_execute(request).await;
        counted.disarm();
        match &result {
            Ok(_) => metrics.write("acknowledged", Some(started.elapsed())),
            Err(status) => metrics.write(write_outcome(status), None),
        }
        result
    }

    async fn query(
        &self,
        request: Request<QueryRequest>,
    ) -> Result<Response<QueryResponse>, Status> {
        let started = tokio::time::Instant::now();
        let metrics = self.node.metrics();
        let counted = CountedOnDrop::new(metrics, Call::Query);
        let result = self.run_query(request).await;
        counted.disarm();
        match &result {
            Ok(_) => metrics.query("ok", Some(started.elapsed())),
            Err(status) if primary_hint(status).is_some() => metrics.query("redirected", None),
            Err(status) if status.code() == Code::Cancelled => metrics.query("cancelled", None),
            Err(_) => metrics.query("failed", None),
        }
        result
    }

    async fn schema(
        &self,
        request: Request<SchemaRequest>,
    ) -> Result<Response<SchemaResponse>, Status> {
        self.run_schema(request).await
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

    /// Answers from this node alone: its published membership view, the databases its
    /// replication serves or follows, as its heartbeats report them, and the orphan copies it
    /// keeps (ADR-0042), each shown if the caller may read a database of its name.
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
        let orphans = replication
            .orphans()
            .await
            .into_iter()
            .filter(|orphan| caller.sees(Action::Read, &orphan.database))
            .map(|orphan| OrphanStatus {
                database: orphan.database.to_string(),
                epoch: orphan.epoch.get(),
                bytes: orphan.bytes,
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
            orphans,
        }))
    }

    /// Deletes this node's orphan copy of a database at an operator's request (ADR-0042). The
    /// request boundary has checked the `fleet` scope.
    async fn remove_orphan(
        &self,
        request: Request<RemoveOrphanRequest>,
    ) -> Result<Response<RemoveOrphanResponse>, Status> {
        let name: DatabaseName = parse("database name", &request.get_ref().database)?;
        let epoch = self.node.remove_orphan(&name).await?;
        Ok(Response::new(RemoveOrphanResponse { epoch: epoch.get() }))
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
            archived_metadata: restored.metadata.as_ref().map(archived_to_proto),
            archived_metadata_skipped: restored.metadata_skipped,
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
        let listing = Listing::read(
            store.as_ref(),
            &archive.keys(&name),
            &name,
            self.node.replication().engine().kind(),
        )
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
                metadata: snapshot.metadata.is_some(),
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
    /// verifying every batch, and removes the directory again. One at a time per node, with
    /// restores.
    async fn verify_archive(
        &self,
        request: Request<VerifyArchiveRequest>,
    ) -> Result<Response<VerifyArchiveResponse>, Status> {
        let name: DatabaseName = parse("database name", &request.get_ref().database)?;
        require_database(&request, &name)?;
        let request = request.into_inner();
        let target = restore_target(request.point)?;
        let archive = self.node.archive(&request.archive)?;
        let _staging = self.node.staging_permit()?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::{Internal, SchemaRequest};

    #[test]
    fn an_engine_that_cant_describe_itself_answers_unimplemented() {
        let status = database_status(&DatabaseError::Engine(EngineError::Unsupported {
            what: "schema",
        }));
        assert_eq!(status.code(), Code::Unimplemented);
        assert_eq!(status.message(), "the engine doesn't support schema");
    }

    #[test]
    fn an_interrupted_statement_answers_deadline_exceeded_or_cancelled() {
        let deadline = database_status(&DatabaseError::Engine(EngineError::Interrupted(
            Interruption::Deadline,
        )));
        assert_eq!(deadline.code(), Code::DeadlineExceeded);
        assert_eq!(
            deadline.message(),
            "statement interrupted: it ran past its deadline"
        );
        let cancelled = database_status(&DatabaseError::Engine(EngineError::Interrupted(
            Interruption::Cancelled,
        )));
        assert_eq!(cancelled.code(), Code::Cancelled);
    }

    #[test]
    fn a_dropped_call_is_counted_as_cancelled() {
        let metrics = DataMetrics::default();
        drop(CountedOnDrop::new(&metrics, Call::Write));
        assert_eq!(metrics.writes("cancelled"), 1);
        drop(CountedOnDrop::new(&metrics, Call::Query));
        assert_eq!(metrics.queries("cancelled"), 1);
        CountedOnDrop::new(&metrics, Call::Write).disarm();
        CountedOnDrop::new(&metrics, Call::Query).disarm();
        assert_eq!(
            metrics.writes("cancelled"),
            1,
            "a call with its own outcome"
        );
        assert_eq!(
            metrics.queries("cancelled"),
            1,
            "a call with its own outcome"
        );
        assert_eq!(
            write_outcome(&Status::cancelled("Timeout expired")),
            "cancelled"
        );
    }

    #[test]
    fn grpc_timeouts_parse_as_the_protocol_writes_them() {
        assert_eq!(grpc_timeout("2H"), Some(Duration::from_secs(7_200)));
        assert_eq!(grpc_timeout("3M"), Some(Duration::from_secs(180)));
        assert_eq!(grpc_timeout("60S"), Some(Duration::from_secs(60)));
        assert_eq!(grpc_timeout("250m"), Some(Duration::from_millis(250)));
        assert_eq!(
            grpc_timeout("99999999u"),
            Some(Duration::from_micros(99_999_999))
        );
        assert_eq!(grpc_timeout("5n"), Some(Duration::from_nanos(5)));
        for malformed in [
            "",
            "S",
            "100",
            "123456789S",
            "1s",
            "-1S",
            "1.5S",
            "+1S",
            "1 S",
        ] {
            assert_eq!(grpc_timeout(malformed), None, "{malformed:?}");
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_write_may_wait_for_a_session_transaction_besides_its_statement_time() {
        let limit = Duration::from_secs(30);
        let transaction = Duration::from_secs(30);
        let now = Instant::now();
        let mut request = Request::new(());
        assert_eq!(
            write_limit(&request, limit, transaction),
            StatementLimit {
                budget: limit,
                latest: now + limit + transaction,
            }
        );
        // A call's own deadline bounds the wait and the statement both.
        request.set_timeout(Duration::from_millis(1_500));
        assert_eq!(
            write_limit(&request, limit, transaction),
            StatementLimit {
                budget: Duration::from_millis(1_400),
                latest: now + Duration::from_millis(1_400),
            }
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_statement_stops_at_the_earlier_of_the_call_deadline_and_the_node_limit() {
        let limit = Duration::from_secs(30);
        let now = Instant::now();
        let mut request = Request::new(());
        assert_eq!(statement_deadline(&request, limit), now + limit);
        // A tenth of the asked time earlier, so the node answers before tonic's own timer.
        request.set_timeout(Duration::from_millis(500));
        assert_eq!(
            statement_deadline(&request, limit),
            now + Duration::from_millis(450)
        );
        // At most 100 ms earlier.
        request.set_timeout(Duration::from_millis(1_500));
        assert_eq!(
            statement_deadline(&request, limit),
            now + Duration::from_millis(1_400)
        );
        request.set_timeout(Duration::from_secs(600));
        assert_eq!(statement_deadline(&request, limit), now + limit);
        request
            .metadata_mut()
            .insert(GRPC_TIMEOUT, "soon".parse().expect("ascii"));
        assert_eq!(statement_deadline(&request, limit), now + limit);
        let endless = statement_deadline(&Request::new(()), Duration::MAX);
        assert!(
            endless > now + limit,
            "a limit past the clock's range stays far off"
        );
    }

    #[test]
    fn the_default_engine_schema_is_unsupported() {
        use crate::engine::{DatabaseEngine as _, MemoryEngine, OpenMode};

        let dir = tempfile::tempdir().expect("a temporary directory");
        let mut database = MemoryEngine::default()
            .open(dir.path(), OpenMode::Create)
            .expect("opens");
        let request = SchemaRequest {
            count_limit: 10,
            internal: Internal::Hide,
        };
        assert!(matches!(
            database.schema(&request, &crate::engine::Interrupt::new()),
            Err(EngineError::Unsupported { what: "schema" })
        ));
    }
}
