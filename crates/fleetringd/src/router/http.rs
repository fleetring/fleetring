//! The router's HTTP server: Hrana over HTTP/1.1 and HTTP/2, with JSON (ADR-0037, point 3).
//!
//! Each request is named, then authenticated, before its body is read and before any node is
//! contacted: an unknown endpoint is `404`, a request without a valid token `401`, one whose
//! token can't read the database `403`, and a body over `max_request_bytes` `413`. Refusals carry
//! a Hrana error body, `{"message": …, "code": …}`. Nothing a request carries, SQL, rows, or
//! tokens, is logged.

use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;

use http_body_util::{BodyExt as _, Full, Limited};
use hyper::body::Bytes;
use hyper::body::Incoming;
use hyper::header::{CONTENT_LENGTH, CONTENT_TYPE, HOST, HeaderValue, UPGRADE};
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::{TokioExecutor, TokioIo, TokioTimer};
use hyper_util::server::conn::auto;
use serde::Serialize;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::{Semaphore, watch};
use tokio::task::JoinSet;
use tokio_rustls::TlsAcceptor;

use crate::clock::Clock as _;
use crate::router::auth::{self, Refusal};
use crate::router::hrana::{CursorRequest, Error, PipelineRequest, code};
use crate::router::naming::{self, Endpoint, Upgrade};
use crate::router::pipeline::{self, Committed, Refused};
use crate::router::{Context, RouterListener};

/// How long a connection may take to send a request's headers, and so how long an idle HTTP/1.1
/// connection stays open.
const HEADER_READ_TIMEOUT: Duration = Duration::from_secs(30);

/// How long a TLS handshake may take.
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

/// How many connections the router serves at once; one more is closed as it is accepted.
const MAX_CONNECTIONS: usize = 16_384;

/// How many requests one HTTP/2 connection may have in flight.
const MAX_CONCURRENT_STREAMS: u32 = 256;

/// How long the accept loop waits after a failed accept.
const ACCEPT_RETRY: Duration = Duration::from_millis(100);

/// The content type of Hrana's JSON.
const JSON: &str = "application/json";

/// The headers that report the last commit of a request (ADR-0037, point 22).
const PRIMARY: &str = "fleetring-primary";
const EPOCH: &str = "fleetring-epoch";
const ACKNOWLEDGED_BY: &str = "fleetring-acknowledged-by";
const REPLICAS: &str = "fleetring-replicas";

type Body = Full<Bytes>;

/// Serves `listener` until `stop` turns true, each connection through `tls` if there is one.
pub(crate) async fn serve(
    listener: RouterListener,
    tls: Option<TlsAcceptor>,
    context: Arc<Context>,
    mut stop: watch::Receiver<bool>,
) {
    let slots = Arc::new(Semaphore::new(MAX_CONNECTIONS));
    let mut connections = JoinSet::new();
    loop {
        let accepted = tokio::select! {
            _ = stop.wait_for(|stop| *stop) => break,
            accepted = listener.accept() => accepted,
            Some(_) = connections.join_next(), if !connections.is_empty() => continue,
        };
        let stream = match accepted {
            Ok(stream) => stream,
            Err(error) => {
                tracing::warn!(%error, "the router couldn't accept a connection");
                tokio::time::sleep(ACCEPT_RETRY).await;
                continue;
            }
        };
        let Ok(slot) = Arc::clone(&slots).try_acquire_owned() else {
            tracing::warn!(max = MAX_CONNECTIONS, "too many connections; closing one");
            continue;
        };
        let context = Arc::clone(&context);
        let tls = tls.clone();
        connections.spawn(async move {
            stream.serve(tls, context).await;
            drop(slot);
        });
    }
    // Every connection goes with the server, and with it every request's work.
    connections.shutdown().await;
}

/// Serves one accepted connection, through TLS if there is an acceptor.
pub(crate) async fn connection<S>(stream: S, tls: Option<TlsAcceptor>, context: Arc<Context>)
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    match tls {
        Some(acceptor) => {
            match tokio::time::timeout(HANDSHAKE_TIMEOUT, acceptor.accept(stream)).await {
                Ok(Ok(stream)) => http(stream, context).await,
                Ok(Err(error)) => tracing::debug!(%error, "router TLS handshake failed"),
                Err(_) => tracing::debug!("router TLS handshake timed out"),
            }
        }
        None => http(stream, context).await,
    }
}

/// Serves HTTP/1.1 or HTTP/2 on `stream` until the client closes it.
async fn http<S>(stream: S, context: Arc<Context>)
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let service = service_fn(move |request: Request<Incoming>| {
        let context = Arc::clone(&context);
        async move { Ok::<_, Infallible>(answer(&context, request).await) }
    });
    let mut builder = auto::Builder::new(TokioExecutor::new());
    builder
        .http1()
        .timer(TokioTimer::new())
        .header_read_timeout(HEADER_READ_TIMEOUT);
    builder
        .http2()
        .timer(TokioTimer::new())
        .max_concurrent_streams(MAX_CONCURRENT_STREAMS);
    if let Err(error) = builder
        .serve_connection(TokioIo::new(stream), service)
        .await
    {
        tracing::debug!(%error, "router connection ended");
    }
}

/// Answers one request.
async fn answer(context: &Context, request: Request<Incoming>) -> Response<Body> {
    let (parts, body) = request.into_parts();
    let upgrade = match parts.headers.get(UPGRADE) {
        Some(value) if value.as_bytes().eq_ignore_ascii_case(b"websocket") => Upgrade::WebSocket,
        _ => Upgrade::None,
    };
    // HTTP/2 names the host in the URI's authority, HTTP/1.1 in the Host header.
    let host = parts
        .uri
        .authority()
        .map(|authority| authority.as_str().to_owned())
        .or_else(|| {
            parts
                .headers
                .get(HOST)
                .and_then(|host| host.to_str().ok())
                .map(str::to_owned)
        });
    let target = match naming::target(
        &parts.method,
        parts.uri.path(),
        host.as_deref(),
        context.config.routing.host_suffix.as_deref(),
        upgrade,
    ) {
        Ok(target) => target,
        Err(naming::NotFound(message)) => {
            return refusal(StatusCode::NOT_FOUND, Error::new(code::NOT_FOUND, message));
        }
    };
    if target.endpoint == Endpoint::Version {
        return response(StatusCode::OK, Full::new(Bytes::new()), None);
    }
    // Who it comes from, and whether it may read the database, before anything else.
    let verifier = context.identity.token_verifier();
    let caller = match auth::authenticate(&parts.headers, &verifier, context.clock.now())
        .and_then(|caller| auth::require_read(&caller, &target.database).map(|()| caller))
    {
        Ok(caller) => caller,
        Err(Refusal::Unauthenticated(message)) => {
            return refusal(
                StatusCode::UNAUTHORIZED,
                Error::new(code::UNAUTHENTICATED, message),
            );
        }
        Err(Refusal::PermissionDenied(message)) => {
            return refusal(
                StatusCode::FORBIDDEN,
                Error::new(code::PERMISSION_DENIED, message),
            );
        }
    };
    let max = context.config.streams.max_request_bytes.get();
    let declared = parts
        .headers
        .get(CONTENT_LENGTH)
        .and_then(|length| length.to_str().ok())
        .and_then(|length| length.parse::<u64>().ok());
    if declared.is_some_and(|length| length > u64::try_from(max).unwrap_or(u64::MAX)) {
        return too_large(max);
    }
    let bytes = match Limited::new(body, max).collect().await {
        Ok(collected) => collected.to_bytes(),
        Err(error) if error.is::<http_body_util::LengthLimitError>() => return too_large(max),
        Err(_) => {
            return refusal(
                StatusCode::BAD_REQUEST,
                Error::new(code::PROTOCOL_ERROR, "the request body couldn't be read"),
            );
        }
    };
    let answered = match target.endpoint {
        Endpoint::Pipeline(version) => match serde_json::from_slice::<PipelineRequest>(&bytes) {
            Ok(request) => pipeline::pipeline(context, &target, version, &caller, request)
                .await
                .map(|answered| (json_body(&answered.body), answered.committed)),
            Err(error) => Err(malformed(&error)),
        },
        Endpoint::Cursor => match serde_json::from_slice::<CursorRequest>(&bytes) {
            Ok(request) => pipeline::cursor(context, &target, &caller, request)
                .await
                .map(|answered| {
                    let (header, entries) = answered.body;
                    let mut lines = serde_json::to_vec(&header).unwrap_or_default();
                    lines.push(b'\n');
                    for entry in entries {
                        lines.extend(serde_json::to_vec(&entry).unwrap_or_default());
                        lines.push(b'\n');
                    }
                    (Bytes::from(lines), answered.committed)
                }),
            Err(error) => Err(malformed(&error)),
        },
        Endpoint::Version => unreachable!("answered above"),
    };
    match answered {
        Ok((body, committed)) => response(StatusCode::OK, Full::new(body), committed.as_ref()),
        Err(refused) => refusal(refused.status, refused.error),
    }
}

/// The refusal of a request body that isn't valid Hrana. serde's message names the field and
/// where it is, never its value.
fn malformed(error: &serde_json::Error) -> Refused {
    Refused::new(
        StatusCode::BAD_REQUEST,
        code::PROTOCOL_ERROR,
        format!("the request isn't valid Hrana JSON: {error}"),
    )
}

fn too_large(max: usize) -> Response<Body> {
    refusal(
        StatusCode::PAYLOAD_TOO_LARGE,
        Error::new(
            code::TOO_LARGE,
            format!("the request body is larger than {max} bytes"),
        ),
    )
}

fn json_body(value: &impl Serialize) -> Bytes {
    Bytes::from(serde_json::to_vec(value).unwrap_or_default())
}

/// A refusal with `status` and a Hrana error body.
fn refusal(status: StatusCode, error: Error) -> Response<Body> {
    tracing::debug!(
        status = status.as_u16(),
        code = error.code.as_deref(),
        "router request refused"
    );
    response(status, Full::new(json_body(&error)), None)
}

/// A response with `status` and `body`, and the headers of `committed` if a call committed.
fn response(status: StatusCode, body: Body, committed: Option<&Committed>) -> Response<Body> {
    let mut response = Response::new(body);
    *response.status_mut() = status;
    let headers = response.headers_mut();
    headers.insert(CONTENT_TYPE, HeaderValue::from_static(JSON));
    if let Some(committed) = committed {
        if let Ok(primary) = HeaderValue::from_str(&committed.primary) {
            headers.insert(PRIMARY, primary);
        }
        headers.insert(EPOCH, HeaderValue::from(committed.epoch));
        headers.insert(
            ACKNOWLEDGED_BY,
            HeaderValue::from(committed.acknowledged_by),
        );
        headers.insert(REPLICAS, HeaderValue::from(committed.replicas));
    }
    response
}
