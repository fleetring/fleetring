//! The `Database.Session` stream: one Hrana stream's statements on its database's primary
//! (ADR-0037, points 11 to 17).
//!
//! [`open`] reads the stream's first message, which names the database, checks `read` on it,
//! routes it to this node as every data call is routed, and answers `SessionOpened`. A caller
//! without `write` on the database gets a read-only session, which the engine enforces. A task
//! then runs the stream's calls one at a time:
//!
//! - **Each call is routed again** before it runs, and runs only under the lease the session
//!   opened under. A call refused before any of it ran (the lease moved, the replica floor isn't
//!   met, or too many writes wait for another session's transaction) is answered `not_run`, so
//!   that its caller knows it may send it elsewhere; a moved lease then ends the session with the
//!   redirect.
//! - **Each call is answered once its commits are acknowledged**: once every transaction it
//!   committed is held by its write quorum, with how many replicas hold the last one. A commit
//!   no quorum acknowledges is answered with `FLEETRING_UNACKNOWLEDGED`, and the session ends
//!   with `ABORTED`.
//! - **Statement deadlines apply** (FR-078): each call's statements stop at
//!   `[limits] statement_timeout_ms` from when the node takes the call, and a call whose
//!   caller goes away is cancelled, which stops its statement too.
//! - **The session ends** when its caller closes or drops the stream, when the node stops
//!   serving the database at the epoch it opened under (`UNAVAILABLE` with the redirect), when
//!   the token it was opened with expires or is revoked, or when its transaction outlived the
//!   node's limit (`ABORTED`). However it ends, the node rolls back the transaction it left open.

use fleetring_client::redirect::primary_hint;
use fleetring_control::{DatabaseName, LeaseEpoch};
use fleetring_proto::v1::{self as proto, session_request, session_response};
use std::time::Duration;

use tokio::sync::mpsc;
use tokio::time::Instant;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Code, Request, Status, Streaming};

use crate::actor::{SessionId, SessionWork};
use crate::auth::{Action, Authorized, BoxStream, authorized};
use crate::data::calls::{
    NotRun, Then, call_from_proto, commit_to_proto, error_result, not_run_result, refused_a_write,
    result_to_proto,
};
use crate::data::convert::parse;
use crate::data::node::{DataNode, SessionTicket};
use crate::data::service::database_status;
use crate::engine::{Autocommit, SessionAccess, SessionCall};
use crate::{DatabaseError, ReplicationError};

/// The stream `Session` returns.
pub(crate) type SessionStream = BoxStream<proto::SessionResponse>;

/// How many results wait for a caller that hasn't read them yet. A session answers one call at
/// a time, so one is all it ever buffers.
const RESPONSE_BUFFER: usize = 2;

/// How long a session's caller has to send the message that opens it.
const OPEN_TIMEOUT: Duration = Duration::from_secs(10);

/// How long a session may go without a call before the node ends it, so that callers who leave
/// sessions open can't hold the node's places for sessions. Longer than a transaction may stay
/// open, and than a router keeps an idle Hrana stream.
pub(crate) const SESSION_IDLE: Duration = Duration::from_secs(120);

/// The code of a commit no write quorum acknowledged (ADR-0037, point 13).
pub(crate) const UNACKNOWLEDGED: &str = "FLEETRING_UNACKNOWLEDGED";

/// The code of a call refused because the session's transaction is gone: it ran too long, or
/// the database closed, and it was rolled back (ADR-0037, point 21).
pub(crate) const TRANSACTION_ABORTED: &str = "FLEETRING_TRANSACTION_ABORTED";

/// Opens a session on `node` from `request`'s first message.
///
/// # Errors
///
/// `INVALID_ARGUMENT` unless the first message opens a session on a valid name;
/// `PERMISSION_DENIED` without `read` on the database; a redirect if another node is its
/// primary, and the other statuses of [`DataNode::route`]; `RESOURCE_EXHAUSTED` if the node
/// holds as many sessions as it may.
pub(crate) async fn open(
    node: &DataNode,
    mut request: Request<Streaming<proto::SessionRequest>>,
) -> Result<SessionStream, Status> {
    let caller = authorized(&request)?.clone();
    let first = tokio::time::timeout(OPEN_TIMEOUT, request.get_mut().message())
        .await
        .map_err(|_| Status::deadline_exceeded("a session's first message didn't arrive in time"))??
        .ok_or_else(opens_first)?;
    let Some(session_request::Request::Open(opening)) = first.request else {
        return Err(opens_first());
    };
    let name: DatabaseName = parse("database name", &opening.database)?;
    caller.require(&name)?;
    let access = if caller.may(Action::Write, &name) {
        SessionAccess::ReadWrite
    } else {
        SessionAccess::ReadOnly
    };
    let admitted = node.route(&name).await?;
    let epoch = admitted.lease().epoch;
    drop(admitted);
    let ticket = node.open_session()?;
    let (responses, stream) = mpsc::channel(RESPONSE_BUFFER);
    let opened = proto::SessionResponse {
        response: Some(session_response::Response::Opened(proto::SessionOpened {
            primary: node.id().to_string(),
            epoch: epoch.get(),
            read_only: access == SessionAccess::ReadOnly,
        })),
    };
    responses
        .try_send(Ok(opened))
        .map_err(|_| Status::internal("the session's first answer didn't fit its buffer"))?;
    tracing::debug!(database = %name, session = %ticket.id(), ?access, "session opened");
    let session = Session {
        node: node.clone(),
        name,
        ticket,
        access,
        epoch,
        caller: caller.clone(),
        holding: Autocommit::On,
    };
    tokio::spawn(session.serve(request.into_inner(), responses));
    Ok(caller.guard(Box::pin(ReceiverStream::new(stream))))
}

fn opens_first() -> Status {
    Status::invalid_argument("a session's first message opens it")
}

/// One open session, as its task serves it.
struct Session {
    node: DataNode,
    name: DatabaseName,
    ticket: SessionTicket,
    access: SessionAccess,
    /// The epoch of the lease the session opened under, the only one it runs calls under.
    epoch: LeaseEpoch,
    caller: Authorized,
    /// Whether the session is in a transaction, as its last call left it.
    holding: Autocommit,
}

/// What became of one call.
enum Answered {
    /// The call has a result, and the session goes on.
    Result(proto::SessionResult),
    /// The call has a result, and then the session ends with the status.
    Last(proto::SessionResult, Status),
    /// The session ends with the status, the call unanswered.
    End(Status),
}

type Responses = mpsc::Sender<Result<proto::SessionResponse, Status>>;

impl Session {
    fn id(&self) -> SessionId {
        self.ticket.id()
    }

    async fn serve(mut self, mut incoming: Streaming<proto::SessionRequest>, responses: Responses) {
        let mut served = self.node.replication().served();
        let ended = loop {
            if !self.is_current(&served) {
                break Some(self.moved().await);
            }
            let message = tokio::select! {
                () = responses.closed() => break None,
                changed = served.changed() => {
                    if changed.is_err() {
                        break Some(Status::unavailable("the node is stopping"));
                    }
                    continue;
                }
                () = tokio::time::sleep(SESSION_IDLE) => {
                    break Some(Status::deadline_exceeded(format!(
                        "the session had no call for {} s",
                        SESSION_IDLE.as_secs()
                    )));
                }
                message = incoming.message() => message,
            };
            let call = match message {
                Ok(Some(proto::SessionRequest {
                    request: Some(session_request::Request::Call(call)),
                })) => call,
                Ok(Some(_)) => {
                    break Some(Status::invalid_argument(
                        "a session is opened once; every later message is a call",
                    ));
                }
                // The caller closed the stream, or went away.
                Ok(None) | Err(_) => break None,
            };
            let (id, call) = match call_from_proto(call) {
                Ok(call) => call,
                Err(status) => break Some(status),
            };
            // A caller that goes away mid-call cancels it, which stops its statement.
            let answered = tokio::select! {
                () = responses.closed() => break None,
                answered = self.run(id, call) => answered,
            };
            let (result, last) = match answered {
                Answered::Result(result) => (result, None),
                Answered::Last(result, status) => (result, Some(status)),
                Answered::End(status) => break Some(status),
            };
            let response = proto::SessionResponse {
                response: Some(session_response::Response::Result(result)),
            };
            if responses.send(Ok(response)).await.is_err() {
                break None;
            }
            if let Some(status) = last {
                break Some(status);
            }
        };
        // However it ended, nothing the session left open survives it.
        self.node
            .registry()
            .session_end(&self.name, self.id())
            .await;
        tracing::debug!(database = %self.name, session = %self.id(), "session ended");
        if let Some(status) = ended {
            // Either the caller reads why, or it has gone.
            let _ = responses.send(Err(status)).await;
        }
    }

    /// Whether this node still serves the database at the session's epoch.
    fn is_current(
        &self,
        served: &tokio::sync::watch::Receiver<std::collections::BTreeMap<DatabaseName, LeaseEpoch>>,
    ) -> bool {
        served.borrow().get(&self.name) == Some(&self.epoch)
    }

    /// The status that ends a session whose database this node no longer serves at its epoch:
    /// the redirect to the new primary, if the node knows it.
    async fn moved(&self) -> Status {
        match self.node.route(&self.name).await {
            Err(status) => status,
            Ok(_) => Status::unavailable(format!(
                "database {} moved to a new lease epoch; open a new session",
                self.name
            )),
        }
    }

    /// Runs call `id`.
    async fn run(&mut self, id: u64, call: SessionCall) -> Answered {
        let autocommit = self.holding == Autocommit::On;
        let admitted = match self.node.route(&self.name).await {
            Ok(admitted) if admitted.lease().epoch == self.epoch => admitted,
            Ok(_) => {
                let status = Status::unavailable(format!(
                    "database {} moved to a new lease epoch; open a new session",
                    self.name
                ));
                let not_run = NotRun {
                    code: "UNAVAILABLE",
                    message: status.message().to_owned(),
                    primary: None,
                    then: Then::Ends,
                };
                return Answered::Last(not_run_result(id, not_run, autocommit), status);
            }
            Err(status) => {
                let not_run = NotRun {
                    code: code_name(status.code()),
                    message: status.message().to_owned(),
                    primary: primary_hint(&status),
                    then: Then::Ends,
                };
                return Answered::Last(not_run_result(id, not_run, autocommit), status);
            }
        };
        let deadline = Instant::now() + self.node.statement_timeout();
        let work = SessionWork {
            session: self.id(),
            call,
            access: self.access,
            holding: self.holding,
        };
        let published = self
            .node
            .registry()
            .session_publish(&self.name, work, Some(deadline))
            .await;
        // The call's commits are in the database's log now, so a drain may step down: it ships
        // the tail, these commits included, before it does.
        drop(admitted);
        let published = match published {
            Ok(published) => published,
            Err(error) => return self.refused(id, &error, autocommit),
        };
        if refused_a_write(published.result()) {
            self.caller.audit_refused(Action::Write, &self.name);
        }
        match published.stored().await {
            Ok(called) => {
                self.holding = called.result.autocommit;
                let commit = called
                    .acknowledged
                    .map(|acknowledged| commit_to_proto(acknowledged, self.epoch.get()));
                Answered::Result(result_to_proto(id, called.result, commit))
            }
            Err(DatabaseError::Replication(error)) => self.unacknowledged(id, &error),
            Err(error) => Answered::End(database_status(&error)),
        }
    }

    /// The answer to call `id`, which the database refused with `error` before running it, or
    /// which failed in a way that ends the session.
    fn refused(&self, id: u64, error: &DatabaseError, autocommit: bool) -> Answered {
        let message = error.to_string();
        let goes_on = |code| NotRun {
            code,
            message: message.clone(),
            primary: None,
            then: Then::GoesOn,
        };
        match error {
            DatabaseError::Busy { .. } => Answered::Result(not_run_result(
                id,
                goes_on("RESOURCE_EXHAUSTED"),
                autocommit,
            )),
            DatabaseError::Refused { .. } | DatabaseError::Closed { .. } => {
                Answered::Result(not_run_result(id, goes_on("UNAVAILABLE"), autocommit))
            }
            // The transaction is gone, rolled back: the call never ran, and the session ends,
            // so that nothing its caller sends later runs outside the transaction it meant.
            DatabaseError::TransactionTimedOut { .. } | DatabaseError::TransactionLost { .. } => {
                let not_run = NotRun {
                    code: TRANSACTION_ABORTED,
                    message: message.clone(),
                    primary: None,
                    then: Then::Ends,
                };
                Answered::Last(not_run_result(id, not_run, true), Status::aborted(message))
            }
            other => Answered::End(database_status(other)),
        }
    }

    /// The answer to call `id`, whose commit no write quorum acknowledged.
    fn unacknowledged(&self, id: u64, error: &ReplicationError) -> Answered {
        tracing::warn!(database = %self.name, session = %self.id(), %error, "a session's commit wasn't acknowledged");
        let message = format!(
            "the transaction committed on the primary, but no write quorum acknowledged it, so it \
             may or may not survive: {error}"
        );
        let result = error_result(id, UNACKNOWLEDGED, message.clone(), true);
        Answered::Last(result, Status::aborted(message))
    }
}

/// The name of a status code, as a `not_run` result carries it.
fn code_name(code: Code) -> &'static str {
    match code {
        Code::NotFound => "NOT_FOUND",
        Code::PermissionDenied => "PERMISSION_DENIED",
        Code::Unauthenticated => "UNAUTHENTICATED",
        Code::FailedPrecondition => "FAILED_PRECONDITION",
        Code::ResourceExhausted => "RESOURCE_EXHAUSTED",
        _ => "UNAVAILABLE",
    }
}
