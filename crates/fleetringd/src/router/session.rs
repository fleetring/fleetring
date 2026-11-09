//! A `Database.Session` the router holds open on a database's primary for one Hrana stream
//! (ADR-0037, points 11 and 21).
//!
//! Opening is retried: it follows up to [`MAX_REDIRECTS`] redirects, and retries a database
//! without a primary, as a promotion leaves it, with backoff for up to `open_retry_ms`. Nothing
//! has run until a session is open. A call is sent once: if the session breaks before the
//! call's result arrives, its outcome is unknown, and it is never sent again. The one exception
//! is a call the node answered `not_run`, which provably never ran.

use std::time::Duration;

use fleetring_client::redirect::primary_hint;
use fleetring_control::DatabaseName;
use fleetring_proto::v1::{
    self as proto, session_request::Request as Message, session_response::Response,
};
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TryRecvError;
use tokio::time::Instant;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Code, Status, Streaming};

use crate::data::calls::call_to_proto;
use crate::engine::SessionCall;
use crate::router::Context;
use crate::router::auth::Caller;
use crate::transport::database_client_with;

/// How many redirects opening a session follows before it gives up.
pub(crate) const MAX_REDIRECTS: usize = 3;

/// The first wait before opening again while a database has no primary.
const FIRST_BACKOFF: Duration = Duration::from_millis(50);

/// The longest wait between two tries at opening.
const MAX_BACKOFF: Duration = Duration::from_secs(1);

/// What the reader task hands on from the node.
#[derive(Debug)]
enum Item {
    Result(proto::SessionResult),
    Ended(Status),
}

/// An open session on a database's primary.
#[derive(Debug)]
pub(crate) struct NodeSession {
    calls: mpsc::Sender<proto::SessionRequest>,
    results: mpsc::Receiver<Item>,
    next_call: u64,
    /// The primary's node ID.
    pub(crate) primary: String,
}

/// Why a session couldn't be opened. Nothing ran.
#[derive(Debug)]
pub(crate) enum OpenError {
    /// The control plane or the node doesn't know the database.
    NotFound(String),
    /// The caller may not open it.
    Denied(String),
    /// No primary could be reached in time.
    Unavailable(String),
    /// Anything else.
    Failed(String),
}

/// Why a call has no result.
#[derive(Debug)]
pub(crate) enum CallError {
    /// The session had ended before the call was sent: it never ran.
    NotSent(Status),
    /// The session broke, or the answer was late, after the call was sent: it may or may not
    /// have run.
    OutcomeUnknown(String),
}

impl NodeSession {
    /// Opens a session on `database`'s primary for `caller`.
    ///
    /// # Errors
    ///
    /// [`OpenError`], after retrying what retrying can fix.
    pub(crate) async fn open(
        context: &Context,
        database: &DatabaseName,
        caller: &Caller,
    ) -> Result<Self, OpenError> {
        let give_up = Instant::now() + context.config.routing.open_retry();
        let mut backoff = FIRST_BACKOFF;
        let mut redirects = 0;
        loop {
            let retry = match context
                .placement
                .primary(database, &context.control, &caller.credentials)
                .await
            {
                Ok(Some(address)) => match Self::open_at(context, database, caller, &address).await
                {
                    Ok(session) => return Ok(session),
                    Err(status) => match primary_hint(&status) {
                        Some(primary) if redirects < MAX_REDIRECTS => {
                            redirects += 1;
                            context.placement.redirected(database, primary);
                            continue;
                        }
                        _ => {
                            context.placement.forget(database);
                            refused(database, &status)?
                        }
                    },
                },
                // The database is failing over: retry until it has a primary again.
                Ok(None) => format!("database {database} has no primary"),
                Err(status) => refused(database, &status)?,
            };
            if Instant::now() + backoff > give_up {
                return Err(OpenError::Unavailable(format!(
                    "{retry}; its primary couldn't be reached in time, and nothing ran; retry"
                )));
            }
            tokio::time::sleep(backoff).await;
            backoff = (backoff * 2).min(MAX_BACKOFF);
        }
    }

    /// Opens a session on the node at `address`.
    async fn open_at(
        context: &Context,
        database: &DatabaseName,
        caller: &Caller,
        address: &str,
    ) -> Result<Self, Status> {
        let channel = context
            .peers
            .channel_to(address)
            .map_err(|error| Status::unavailable(format!("can't reach {address}: {error}")))?;
        let mut client = database_client_with(channel, &caller.credentials);
        let (calls, requests) = mpsc::channel(2);
        let open = proto::SessionRequest {
            request: Some(Message::Open(proto::SessionOpen {
                database: database.to_string(),
            })),
        };
        calls
            .try_send(open)
            .map_err(|_| Status::internal("the session's first message didn't fit its buffer"))?;
        let opened = client.session(ReceiverStream::new(requests)).await;
        let mut responses = match opened {
            Ok(responses) => responses.into_inner(),
            Err(status) => {
                if status.code() == Code::Unavailable && primary_hint(&status).is_none() {
                    // A stalled or broken connection: dial afresh next time.
                    context.peers.forget(address);
                }
                return Err(status);
            }
        };
        let first = responses
            .message()
            .await?
            .and_then(|response| response.response);
        let Some(Response::Opened(opened)) = first else {
            return Err(Status::internal("the node didn't open the session"));
        };
        let (forward, results) = mpsc::channel(2);
        tokio::spawn(read(responses, forward));
        Ok(Self {
            calls,
            results,
            next_call: 0,
            primary: opened.primary,
        })
    }

    /// Why the session has ended, if it has, from what the node sent since the last call.
    pub(crate) fn ended(&mut self) -> Option<Status> {
        match self.results.try_recv() {
            Ok(Item::Ended(status)) => Some(status),
            Ok(Item::Result(_)) => Some(Status::internal(
                "the node sent a result for no call; the session can't be trusted",
            )),
            Err(TryRecvError::Empty) => None,
            Err(TryRecvError::Disconnected) => Some(Status::unavailable("the session ended")),
        }
    }

    /// Sends `call` and waits up to `timeout` for its result.
    ///
    /// # Errors
    ///
    /// [`CallError::NotSent`] if the session had already ended; [`CallError::OutcomeUnknown`]
    /// if it breaks before the result arrives, or the result is late. The call is never sent
    /// again.
    pub(crate) async fn call(
        &mut self,
        call: &SessionCall,
        timeout: Duration,
    ) -> Result<proto::SessionResult, CallError> {
        if let Some(status) = self.ended() {
            return Err(CallError::NotSent(status));
        }
        self.next_call += 1;
        let id = self.next_call;
        let request = proto::SessionRequest {
            request: Some(Message::Call(call_to_proto(id, call))),
        };
        if let Err(unsent) = self.calls.send(request).await {
            drop(unsent);
            return Err(CallError::NotSent(Status::unavailable("the session ended")));
        }
        match tokio::time::timeout(timeout, self.results.recv()).await {
            Ok(Some(Item::Result(result))) if result.call_id == id => Ok(result),
            Ok(Some(Item::Result(_))) => Err(CallError::OutcomeUnknown(
                "the node answered another call".to_owned(),
            )),
            Ok(Some(Item::Ended(status))) => Err(CallError::OutcomeUnknown(format!(
                "the session ended: {}",
                status.message()
            ))),
            Ok(None) => Err(CallError::OutcomeUnknown("the session ended".to_owned())),
            Err(_) => Err(CallError::OutcomeUnknown(format!(
                "no answer within {} ms",
                timeout.as_millis()
            ))),
        }
    }
}

/// Hands on what the node sends, until the session ends.
async fn read(mut responses: Streaming<proto::SessionResponse>, forward: mpsc::Sender<Item>) {
    loop {
        let item = match responses.message().await {
            Ok(Some(proto::SessionResponse {
                response: Some(Response::Result(result)),
            })) => Item::Result(result),
            Ok(Some(_)) => Item::Ended(Status::internal("the node sent an unexpected message")),
            Ok(None) => Item::Ended(Status::unavailable("the node ended the session")),
            Err(status) => Item::Ended(status),
        };
        let ended = matches!(item, Item::Ended(_));
        if forward.send(item).await.is_err() || ended {
            return;
        }
    }
}

/// What a refusal to open means: retry (`Ok`, with why) for what a moment may fix, or give up.
fn refused(database: &DatabaseName, status: &Status) -> Result<String, OpenError> {
    let message = status.message().to_owned();
    match status.code() {
        Code::Unavailable | Code::DeadlineExceeded | Code::Cancelled | Code::Unknown => Ok(message),
        Code::NotFound => Err(OpenError::NotFound(format!(
            "database {database} doesn't exist"
        ))),
        Code::PermissionDenied | Code::Unauthenticated => Err(OpenError::Denied(message)),
        // Not a fault of the router's: the fleet is part way through an upgrade, and the request
        // may be retried once the primary runs one with sessions.
        Code::Unimplemented => Err(OpenError::Unavailable(format!(
            "the primary of {database} runs a release without sessions (0.7 or older, protocol \
             1.3); upgrade it to 0.8 or later and retry"
        ))),
        _ => Err(OpenError::Failed(message)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_primary_without_sessions_is_unavailable_until_it_is_upgraded() {
        let database: DatabaseName = "old".parse().expect("a name");
        let refusal = refused(&database, &Status::unimplemented("unknown method Session"));
        let Err(OpenError::Unavailable(message)) = refusal else {
            panic!("{refusal:?}");
        };
        assert!(message.contains("upgrade it"), "{message}");
    }
}
