//! Hrana pipelines and cursors, run through a Hrana stream's session on the primary (ADR-0037,
//! points 9, 10, 21, and 22).
//!
//! A stream's requests run in order, one call each, on the session the stream opened on the
//! database's primary. The stream opens its session at its first call, and again at a later
//! one, wherever the primary is then, if it is outside a transaction and its session ended
//! between requests. A call is sent once: if the session breaks before its result arrives, the
//! request fails with `FLEETRING_OUTCOME_UNKNOWN` and the stream is broken, since what ran is
//! unknown; only a call the node says never ran is sent again, to the new primary. A stream
//! whose transaction's session is gone fails with `FLEETRING_TRANSACTION_ABORTED`.

use fleetring_client::redirect::primary_hint;
use fleetring_proto::v1::{
    self as proto, batch_step_result::Step, session_result::Result as Answer,
};
use hyper::StatusCode;

use crate::engine::{Autocommit, SessionCall};
use crate::router::Context;
use crate::router::auth::Caller;
use crate::router::hrana::{
    self, BatchStep, CursorEntry, CursorRequest, CursorResponse, Error, PipelineRequest,
    PipelineResponse, Stmt, StreamRequest, StreamResponse, StreamResult, code,
};
use crate::router::naming::{Hrana, Target};
use crate::router::session::{CallError, NodeSession, OpenError};
use crate::router::streams::{Locked, StreamRefusal};

/// How deeply a batch step's condition may nest; the node holds it to the same bound.
const MAX_CONDITION_DEPTH: usize = crate::data::calls::MAX_CONDITION_DEPTH;

/// How many times a call the node never ran is sent again, to the primary it moved to.
const MAX_RESENDS: usize = 3;

/// The code a node gives a commit no write quorum acknowledged.
const UNACKNOWLEDGED: &str = "FLEETRING_UNACKNOWLEDGED";

/// The most bytes of rows one response carries, over all its requests: a node bounds one call's
/// rows at half of it, and this bounds a pipeline of many.
const MAX_RESPONSE_BYTES: usize = 2 * crate::engine::MAX_RESULT_BYTES;

/// What the last call that committed reports, for the response's `fleetring-*` headers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Committed {
    /// The primary's node ID.
    pub(crate) primary: String,
    pub(crate) epoch: u64,
    pub(crate) acknowledged_by: u32,
    pub(crate) replicas: u32,
}

/// An answer, with what the last commit in it reports.
#[derive(Debug)]
pub(crate) struct Answered<T> {
    pub(crate) body: T,
    pub(crate) committed: Option<Committed>,
}

/// A request refused as a whole, with its HTTP status.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Refused {
    pub(crate) status: StatusCode,
    pub(crate) error: Error,
}

impl Refused {
    pub(crate) fn new(status: StatusCode, code: &str, message: impl Into<String>) -> Self {
        Self {
            status,
            error: Error::new(code, message),
        }
    }
}

impl From<StreamRefusal> for Refused {
    fn from(refusal: StreamRefusal) -> Self {
        let status = match refusal {
            StreamRefusal::Expired | StreamRefusal::Invalid(_) => StatusCode::BAD_REQUEST,
            StreamRefusal::Forbidden(_) => StatusCode::FORBIDDEN,
            StreamRefusal::Full => StatusCode::SERVICE_UNAVAILABLE,
        };
        Self {
            status,
            error: refusal.error(),
        }
    }
}

/// Runs a pipeline for `caller` on `target`'s database, over Hrana `version`.
///
/// # Errors
///
/// [`Refused`] if the request carries too many statements, or its baton doesn't continue a
/// stream of the caller's on the database.
pub(crate) async fn pipeline(
    context: &Context,
    target: &Target,
    version: Hrana,
    caller: &Caller,
    request: PipelineRequest,
) -> Result<Answered<PipelineResponse>, Refused> {
    let statements: usize = request.requests.iter().map(StreamRequest::statements).sum();
    check_size(context, statements)?;
    let mut stream = stream_for(context, request.baton.as_deref(), caller, target)?;
    let mut committed = None;
    let mut budget = MAX_RESPONSE_BYTES;
    let mut results = Vec::with_capacity(request.requests.len());
    for request in request.requests {
        let ran = if stream.closed {
            Err(Error::new(code::PROTOCOL_ERROR, "the stream is closed"))
        } else {
            let mut run = Run {
                context,
                caller,
                version,
                stream: &mut stream,
                committed: &mut committed,
                budget: &mut budget,
            };
            run.request(request).await
        };
        results.push(match ran {
            Ok(response) => StreamResult::Ok { response },
            Err(error) => StreamResult::Error { error },
        });
    }
    let (baton, base_url) = finish(context, target, stream);
    Ok(Answered {
        body: PipelineResponse {
            baton,
            base_url,
            results,
        },
        committed,
    })
}

/// Runs a cursor's batch for `caller` on `target`'s database: the header line's fields, and the
/// entries, in order.
///
/// # Errors
///
/// As [`pipeline`].
pub(crate) async fn cursor(
    context: &Context,
    target: &Target,
    caller: &Caller,
    request: CursorRequest,
) -> Result<Answered<(CursorResponse, Vec<CursorEntry>)>, Refused> {
    check_size(context, request.batch.steps.len())?;
    let mut stream = stream_for(context, request.baton.as_deref(), caller, target)?;
    let mut committed = None;
    let mut budget = MAX_RESPONSE_BYTES;
    let entries = if stream.closed {
        vec![CursorEntry::Error {
            error: Error::new(code::PROTOCOL_ERROR, "the stream is closed"),
        }]
    } else {
        let mut run = Run {
            context,
            caller,
            version: Hrana::V3,
            stream: &mut stream,
            committed: &mut committed,
            budget: &mut budget,
        };
        match run.batch(request.batch.steps).await {
            Ok(result) => entries(result),
            Err(error) => vec![CursorEntry::Error { error }],
        }
    };
    let (baton, base_url) = finish(context, target, stream);
    Ok(Answered {
        body: (CursorResponse { baton, base_url }, entries),
        committed,
    })
}

/// Refuses a request with more statements than one may carry.
fn check_size(context: &Context, statements: usize) -> Result<(), Refused> {
    let max = context.config.streams.max_statements.get();
    if statements > max {
        return Err(Refused::new(
            StatusCode::PAYLOAD_TOO_LARGE,
            code::TOO_LARGE,
            format!("the request carries {statements} statements; at most {max} may run in one"),
        ));
    }
    Ok(())
}

/// The stream `baton` continues, or a new one without a baton, locked for this request.
fn stream_for(
    context: &Context,
    baton: Option<&str>,
    caller: &Caller,
    target: &Target,
) -> Result<Locked, Refused> {
    Ok(match baton {
        Some(baton) => context.streams.resume(baton, caller, &target.database)?,
        None => context
            .streams
            .create(caller, &target.database, context.entropy.as_ref())?,
    })
}

/// Ends a request on `stream`: a closed stream goes, and an open one gets its next baton, with
/// the URL that reaches this router instance if one is advertised.
fn finish(
    context: &Context,
    target: &Target,
    mut stream: Locked,
) -> (Option<String>, Option<String>) {
    if stream.closed {
        let id = stream.id();
        drop(stream);
        context.streams.remove(id);
        return (None, None);
    }
    stream.advance();
    let baton = stream.baton();
    // The host form names the database in the host, which another URL would lose.
    let base_url = match (&context.config.http.advertise_url, target.prefix.is_empty()) {
        (Some(url), false) => Some(format!("{}{}/", url.trim_end_matches('/'), target.prefix)),
        _ => None,
    };
    (Some(baton), base_url)
}

/// One request's run on a locked stream.
struct Run<'r> {
    context: &'r Context,
    caller: &'r Caller,
    version: Hrana,
    stream: &'r mut Locked,
    committed: &'r mut Option<Committed>,
    /// What is left of the most bytes of rows one response may carry.
    budget: &'r mut usize,
}

impl Run<'_> {
    async fn request(&mut self, request: StreamRequest) -> Result<StreamResponse, Error> {
        match request {
            StreamRequest::Close => {
                self.stream.closed = true;
                // Dropping the session closes it; the node rolls back what it left open.
                self.stream.session = None;
                Ok(StreamResponse::Close)
            }
            StreamRequest::StoreSql { sql_id, sql } => {
                let max = self.context.config.streams.max_sql_per_stream.get();
                if !self.stream.sql.contains_key(&sql_id) && self.stream.sql.len() >= max {
                    return Err(Error::new(
                        code::RESOURCE_EXHAUSTED,
                        format!("a stream stores at most {max} SQL texts; close one first"),
                    ));
                }
                // Stored texts take no more than one request may carry.
                let max_bytes = self.context.config.streams.max_request_bytes.get();
                let stored: usize = self
                    .stream
                    .sql
                    .iter()
                    .filter(|(id, _)| **id != sql_id)
                    .map(|(_, text)| text.len())
                    .sum();
                if stored.saturating_add(sql.len()) > max_bytes {
                    return Err(Error::new(
                        code::RESOURCE_EXHAUSTED,
                        format!(
                            "a stream stores at most {max_bytes} bytes of SQL; close some first"
                        ),
                    ));
                }
                self.stream.sql.insert(sql_id, sql);
                Ok(StreamResponse::StoreSql)
            }
            StreamRequest::CloseSql { sql_id } => {
                self.stream.sql.remove(&sql_id);
                Ok(StreamResponse::CloseSql)
            }
            StreamRequest::GetAutocommit => {
                self.v3("get_autocommit")?;
                if let Some(error) = &self.stream.broken {
                    return Err(error.clone());
                }
                Ok(StreamResponse::GetAutocommit {
                    is_autocommit: self.stream.autocommit == Autocommit::On,
                })
            }
            StreamRequest::Execute { stmt } => {
                let statement = self.statement(stmt)?;
                let result = self.call(SessionCall::Execute(statement)).await?;
                match result.result {
                    Some(Answer::Statement(outcome)) => Ok(StreamResponse::Execute {
                        result: hrana::stmt_result(outcome),
                    }),
                    other => Err(failure(other)),
                }
            }
            StreamRequest::Batch { batch } => Ok(StreamResponse::Batch {
                result: hrana::batch_result(self.batch(batch.steps).await?),
            }),
            StreamRequest::Sequence { sql, sql_id } => {
                let sql = self.sql(sql, sql_id)?;
                let result = self.call(SessionCall::Sequence(sql)).await?;
                match result.result {
                    Some(Answer::Sequence(_)) => Ok(StreamResponse::Sequence),
                    other => Err(failure(other)),
                }
            }
            StreamRequest::Describe { sql, sql_id } => {
                let sql = self.sql(sql, sql_id)?;
                let result = self.call(SessionCall::Describe(sql)).await?;
                match result.result {
                    Some(Answer::Described(described)) => Ok(StreamResponse::Describe {
                        result: hrana::describe_result(described),
                    }),
                    other => Err(failure(other)),
                }
            }
        }
    }

    /// Runs a batch's steps as one call.
    async fn batch(&mut self, steps: Vec<BatchStep>) -> Result<proto::BatchResult, Error> {
        let steps = steps
            .into_iter()
            .map(|step| {
                let condition = step
                    .condition
                    .map(|condition| {
                        hrana::condition(condition, self.version == Hrana::V3, MAX_CONDITION_DEPTH)
                    })
                    .transpose()?;
                Ok(hrana::step(self.statement(step.stmt)?, condition))
            })
            .collect::<Result<Vec<_>, Error>>()?;
        let result = self.call(SessionCall::Batch(steps)).await?;
        match result.result {
            Some(Answer::Batch(batch)) => Ok(batch),
            other => Err(failure(other)),
        }
    }

    /// Fails unless the stream speaks Hrana 3, which `what` needs.
    fn v3(&self, what: &str) -> Result<(), Error> {
        match self.version {
            Hrana::V3 => Ok(()),
            Hrana::V2 => Err(Error::new(
                code::PROTOCOL_ERROR,
                format!("{what} is Hrana 3; use /v3/pipeline"),
            )),
        }
    }

    /// The session statement `stmt` asks for.
    fn statement(&self, stmt: Stmt) -> Result<crate::engine::SessionStatement, Error> {
        let sql = self.sql(stmt.sql.clone(), stmt.sql_id)?;
        hrana::statement(stmt, sql)
    }

    /// The SQL text a request names, by `sql` or by a stored `sql_id`: exactly one of them.
    fn sql(&self, sql: Option<String>, sql_id: Option<i32>) -> Result<String, Error> {
        match (sql, sql_id) {
            (Some(sql), None) => Ok(sql),
            (None, Some(id)) => self.stream.sql.get(&id).cloned().ok_or_else(|| {
                Error::new(
                    code::PROTOCOL_ERROR,
                    format!("no SQL text is stored as {id} on this stream"),
                )
            }),
            _ => Err(Error::new(
                code::PROTOCOL_ERROR,
                "a statement names its SQL by exactly one of sql and sql_id",
            )),
        }
    }

    /// Breaks the stream with `error`, which every later request gets, and returns it.
    fn break_with(&mut self, error: Error) -> Error {
        self.stream.session = None;
        self.stream.broken = Some(error.clone());
        error
    }

    /// Sends `call` on the stream's session, opening one first if it has none, and returns its
    /// result.
    async fn call(&mut self, call: SessionCall) -> Result<proto::SessionResult, Error> {
        if let Some(error) = &self.stream.broken {
            return Err(error.clone());
        }
        let database = self.stream.database.clone();
        let placement = &self.context.placement;
        let mut resends = 0;
        loop {
            // A session that ended between requests: what it ended with says where to go.
            let ended = self.stream.session.as_mut().and_then(NodeSession::ended);
            if let Some(status) = ended {
                self.stream.session = None;
                match primary_hint(&status) {
                    Some(primary) => placement.redirected(&database, primary),
                    None => placement.forget(&database),
                }
                if self.stream.autocommit == Autocommit::InTransaction {
                    return Err(self.break_with(aborted(status.message())));
                }
            }
            if self.stream.session.is_none() {
                if self.stream.autocommit == Autocommit::InTransaction {
                    return Err(self.break_with(aborted("its session ended")));
                }
                let opened = NodeSession::open(self.context, &database, self.caller).await;
                self.stream.session = Some(opened.map_err(open_error)?);
            }
            let timeout = self.context.config.routing.call_timeout();
            // The call owns the session while it runs: if the request goes away mid-call, the
            // session goes with it, which cancels the call on the node and rolls back its open
            // transaction, and the stream then has none.
            let mut session = self.stream.session.take().expect("a session is open");
            let primary = session.primary.clone();
            let called = session.call(&call, timeout).await;
            self.stream.session = Some(session);
            let error = match called {
                Ok(result) => match &result.result {
                    Some(Answer::NotRun(not_run)) => {
                        let error = Error::new(&not_run.code, not_run.message.clone());
                        if !not_run.ends_session {
                            return Err(error);
                        }
                        self.stream.session = None;
                        match &not_run.primary {
                            Some(primary) => placement.redirected(&database, primary.clone()),
                            None => placement.forget(&database),
                        }
                        if self.stream.autocommit == Autocommit::InTransaction {
                            let error = if not_run.code == code::TRANSACTION_ABORTED {
                                error
                            } else {
                                aborted(&not_run.message)
                            };
                            return Err(self.break_with(error));
                        }
                        if resends < MAX_RESENDS {
                            // It never ran: send it to where the database is now.
                            resends += 1;
                            continue;
                        }
                        error
                    }
                    Some(Answer::Error(failed)) if failed.code == UNACKNOWLEDGED => {
                        return Err(self.break_with(hrana::error(failed.clone())));
                    }
                    _ => {
                        let size = rows_size(&result);
                        if size > *self.budget && result.commit.is_none() {
                            // It ran, but read too much for one response to carry.
                            return Err(Error::new(
                                crate::engine::RESULT_TOO_LARGE,
                                format!(
                                    "the response's rows would exceed {MAX_RESPONSE_BYTES} \
                                     bytes; read them in smaller pieces"
                                ),
                            ));
                        }
                        *self.budget = self.budget.saturating_sub(size);
                        self.stream.autocommit = if result.autocommit {
                            Autocommit::On
                        } else {
                            Autocommit::InTransaction
                        };
                        if let Some(commit) = &result.commit {
                            *self.committed = Some(Committed {
                                primary,
                                epoch: commit.epoch,
                                acknowledged_by: commit.acknowledged_by,
                                replicas: commit.replicas,
                            });
                        }
                        return Ok(result);
                    }
                },
                Err(CallError::NotSent(status)) => {
                    self.stream.session = None;
                    match primary_hint(&status) {
                        Some(primary) => placement.redirected(&database, primary),
                        None => placement.forget(&database),
                    }
                    if self.stream.autocommit == Autocommit::InTransaction {
                        return Err(self.break_with(aborted(status.message())));
                    }
                    if resends < MAX_RESENDS {
                        resends += 1;
                        continue;
                    }
                    Error::new(code::UNAVAILABLE, status.message())
                }
                Err(CallError::OutcomeUnknown(why)) => {
                    placement.forget(&database);
                    return Err(self.break_with(Error::new(
                        code::OUTCOME_UNKNOWN,
                        format!(
                            "the connection to the primary broke before this request's result \
                             arrived ({why}); it may or may not have run, and it isn't sent again"
                        ),
                    )));
                }
            };
            return Err(error);
        }
    }
}

/// About how many bytes the rows of `result` hold.
fn rows_size(result: &proto::SessionResult) -> usize {
    let rows = |outcome: &proto::StatementResult| -> usize {
        outcome
            .rows
            .iter()
            .flat_map(|row| &row.values)
            .map(|value| match &value.value {
                Some(fleetring_proto::v1::value::Value::Text(text)) => text.len(),
                Some(fleetring_proto::v1::value::Value::Blob(blob)) => blob.len(),
                _ => 8,
            })
            .sum()
    };
    match &result.result {
        Some(Answer::Statement(outcome)) => rows(outcome),
        Some(Answer::Batch(batch)) => batch
            .steps
            .iter()
            .filter_map(|step| match &step.step {
                Some(Step::Ok(outcome)) => Some(rows(outcome)),
                _ => None,
            })
            .sum(),
        _ => 0,
    }
}

/// The error of a stream whose transaction's session is gone, saying why.
fn aborted(why: &str) -> Error {
    Error::new(
        code::TRANSACTION_ABORTED,
        format!(
            "the stream's transaction ended on the primary without its commit ({why}); nothing it \
             wrote was committed: begin it again"
        ),
    )
}

/// The Hrana error for an answer that isn't the result a request expected.
fn failure(answer: Option<Answer>) -> Error {
    match answer {
        Some(Answer::Error(error)) => hrana::error(error),
        _ => Error::new(
            code::INTERNAL,
            "the primary answered with the wrong kind of result",
        ),
    }
}

/// The Hrana error for a session that couldn't be opened; nothing ran.
fn open_error(error: OpenError) -> Error {
    match error {
        OpenError::NotFound(message) => Error::new(code::NOT_FOUND, message),
        OpenError::Denied(message) => Error::new(code::PERMISSION_DENIED, message),
        OpenError::Unavailable(message) => Error::new(code::UNAVAILABLE, message),
        OpenError::Failed(message) => Error::new(code::INTERNAL, message),
    }
}

/// A cursor's entries for a batch's result.
fn entries(result: proto::BatchResult) -> Vec<CursorEntry> {
    let mut entries = Vec::new();
    for (index, step) in (0_u32..).zip(result.steps) {
        match step.step {
            Some(Step::Ok(outcome)) => {
                let result = hrana::stmt_result(outcome);
                entries.push(CursorEntry::StepBegin {
                    step: index,
                    cols: result.cols,
                });
                entries.extend(result.rows.into_iter().map(|row| CursorEntry::Row { row }));
                entries.push(CursorEntry::StepEnd {
                    affected_row_count: result.affected_row_count,
                    last_insert_rowid: result.last_insert_rowid,
                });
            }
            Some(Step::Error(error)) => entries.push(CursorEntry::StepError {
                step: index,
                error: hrana::error(error),
            }),
            Some(Step::Skipped(_)) | None => {}
        }
    }
    entries
}
