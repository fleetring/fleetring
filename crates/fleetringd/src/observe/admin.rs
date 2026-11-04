//! The admin listener: `/metrics`, `/healthz`, and `/readyz` over HTTP/1.1, apart from the gRPC
//! port (ADR-0034 points 1 to 4).
//!
//! It serves `GET` and `HEAD` on the three paths, `405` for any other method on them, and `404`
//! for any other path. Query strings are ignored and request bodies never read. With TLS, the
//! listener presents the node's own certificate and asks for a client certificate without
//! requiring one; `/metrics` then answers only a principal `[admin] scrapers` lists, and the
//! probes answer anyone. It serves at most [`MAX_CONNECTIONS`] connections at once and closes any
//! beyond them as soon as it accepts them.

use std::convert::Infallible;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use http_body_util::Full;
use hyper::body::{Bytes, Incoming};
use hyper::header::{ALLOW, CONTENT_TYPE, HeaderValue};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::{TokioIo, TokioTimer};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpListener;
use tokio::sync::{Semaphore, oneshot};
use tokio::task::{JoinHandle, JoinSet};
use tokio_rustls::TlsAcceptor;

use crate::observe::Metrics;
use crate::observe::health::{Health, Probes};
use crate::tls::{HANDSHAKE_TIMEOUT, Identity, Peer, Principal, peer_of};

/// The audit log's target, as the request boundary's.
const AUDIT: &str = "fleetringd::audit";

/// How long a connection may take to send a request's headers, and so how long an idle one
/// stays open.
const HEADER_READ_TIMEOUT: Duration = Duration::from_secs(30);

/// How long the listener waits after a failed accept before it accepts again.
const ACCEPT_RETRY: Duration = Duration::from_millis(100);

/// How many connections the listener serves at once. A scraper and a few probes need a handful;
/// a connection over the cap is closed at once, so idle or slow clients can't pile up tasks.
pub(crate) const MAX_CONNECTIONS: usize = 64;

/// The most a connection's read buffer holds: a request here is a line and a few headers, and
/// its body is never read.
const MAX_BUF_SIZE: usize = 16 * 1024;

/// The content type of the exposition.
const OPENMETRICS: &str = "application/openmetrics-text; version=1.0.0; charset=utf-8";

/// The content type of `/healthz` and of refusals.
const TEXT: &str = "text/plain; charset=utf-8";

/// The content type of `/readyz`.
const JSON: &str = "application/json";

/// What the listener serves over TLS: the node's identity, and who may read `/metrics`.
#[derive(Debug, Clone)]
pub(crate) struct AdminTlsParts {
    pub(crate) identity: Identity,
    pub(crate) scrapers: Vec<Principal>,
}

/// Everything the listener needs.
#[derive(Debug)]
pub(crate) struct AdminParts {
    pub(crate) listener: TcpListener,
    pub(crate) metrics: Metrics,
    pub(crate) probes: Probes,
    pub(crate) tls: Option<AdminTlsParts>,
}

/// The running admin listener; dropping it stops it.
#[derive(Debug)]
pub(crate) struct RunningAdmin {
    address: SocketAddr,
    stop: oneshot::Sender<()>,
    task: JoinHandle<()>,
}

impl RunningAdmin {
    pub(crate) fn address(&self) -> SocketAddr {
        self.address
    }

    /// Stops accepting connections and closes the open ones.
    pub(crate) async fn stop(self) {
        let _gone = self.stop.send(());
        // The task only ends, never fails; a panic in it has been reported already.
        let _ended = self.task.await;
    }
}

/// What every connection's requests are answered from.
#[derive(Debug)]
struct Shared {
    metrics: Metrics,
    probes: Probes,
    /// The scrapers, when the listener serves TLS.
    scrapers: Option<Vec<Principal>>,
}

/// Starts serving on `parts.listener`.
///
/// # Errors
///
/// The I/O error if the listener can't report its address.
///
/// # Panics
///
/// If called outside a Tokio runtime.
pub(crate) fn start(parts: AdminParts) -> io::Result<RunningAdmin> {
    let AdminParts {
        listener,
        metrics,
        probes,
        tls,
    } = parts;
    let address = listener.local_addr()?;
    let identity = tls.as_ref().map(|tls| tls.identity.clone());
    let shared = Arc::new(Shared {
        metrics,
        probes,
        scrapers: tls.map(|tls| tls.scrapers),
    });
    let (stop, mut stopped) = oneshot::channel::<()>();
    let permits = Arc::new(Semaphore::new(MAX_CONNECTIONS));
    let task = tokio::spawn(async move {
        // Dropped with the loop, which aborts every open connection.
        let mut connections = JoinSet::new();
        loop {
            let accepted = tokio::select! {
                _ = &mut stopped => break,
                Some(_) = connections.join_next(), if !connections.is_empty() => continue,
                accepted = listener.accept() => accepted,
            };
            let stream = match accepted {
                Ok((stream, _from)) => stream,
                Err(error) => {
                    // Out of file descriptors, say: wait rather than spin on the same error.
                    tracing::warn!(%error, "admin listener can't accept a connection");
                    tokio::time::sleep(ACCEPT_RETRY).await;
                    continue;
                }
            };
            let Ok(permit) = Arc::clone(&permits).try_acquire_owned() else {
                tracing::debug!(
                    limit = MAX_CONNECTIONS,
                    "admin listener at its connection limit; closing a new connection"
                );
                drop(stream);
                continue;
            };
            let shared = Arc::clone(&shared);
            let identity = identity.clone();
            connections.spawn(async move {
                // Held while the connection lasts, handshake included.
                let _permit = permit;
                match identity {
                    None => serve(stream, None, shared).await,
                    Some(identity) => {
                        if let Some((stream, peer)) = handshake(stream, &identity).await {
                            serve(stream, peer, shared).await;
                        }
                    }
                }
            });
        }
    });
    Ok(RunningAdmin {
        address,
        stop,
        task,
    })
}

/// Finishes a TLS handshake on `stream`, and returns the stream with the peer its client
/// certificate names, if it sent one. `None` drops the connection: the handshake failed, or the
/// certificate chains to a trusted authority but names nothing of this fleet.
async fn handshake(
    stream: tokio::net::TcpStream,
    identity: &Identity,
) -> Option<(
    tokio_rustls::server::TlsStream<tokio::net::TcpStream>,
    Option<Peer>,
)> {
    let loaded = identity.current();
    let acceptor = TlsAcceptor::from(loaded.admin_server_config());
    let stream = match tokio::time::timeout(HANDSHAKE_TIMEOUT, acceptor.accept(stream)).await {
        Ok(Ok(stream)) => stream,
        Ok(Err(error)) => {
            tracing::debug!(%error, "admin TLS handshake failed");
            return None;
        }
        Err(_) => {
            tracing::debug!("admin TLS handshake timed out");
            return None;
        }
    };
    let peer = match stream.get_ref().1.peer_certificates() {
        None | Some([]) => None,
        Some(certificates) => match peer_of(certificates, loaded.fleet()) {
            Ok(peer) => Some(peer),
            Err(error) => {
                tracing::warn!(%error, "admin connection refused");
                return None;
            }
        },
    };
    Some((stream, peer))
}

/// Serves HTTP/1.1 requests on `stream` until the client closes it or goes idle.
async fn serve<S>(stream: S, peer: Option<Peer>, shared: Arc<Shared>)
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let service = service_fn(move |request: Request<Incoming>| {
        let response = answer(&request, peer.as_ref(), &shared);
        async move { Ok::<_, Infallible>(response) }
    });
    let mut builder = http1::Builder::new();
    builder
        .timer(TokioTimer::new())
        .header_read_timeout(HEADER_READ_TIMEOUT)
        .max_buf_size(MAX_BUF_SIZE);
    if let Err(error) = builder
        .serve_connection(TokioIo::new(stream), service)
        .await
    {
        tracing::debug!(%error, "admin connection ended");
    }
}

/// Which endpoint a request is for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Route {
    Metrics,
    Health,
    Ready,
    WrongMethod,
    NotFound,
}

fn route(method: &Method, path: &str) -> Route {
    let endpoint = match path {
        "/metrics" => Route::Metrics,
        "/healthz" => Route::Health,
        "/readyz" => Route::Ready,
        _ => return Route::NotFound,
    };
    if method == Method::GET || method == Method::HEAD {
        endpoint
    } else {
        Route::WrongMethod
    }
}

fn answer(
    request: &Request<Incoming>,
    peer: Option<&Peer>,
    shared: &Shared,
) -> Response<Full<Bytes>> {
    let head = request.method() == Method::HEAD;
    let (status, content_type, body) = match route(request.method(), request.uri().path()) {
        Route::Metrics => match may_scrape(peer, shared.scrapers.as_deref()) {
            Ok(()) => (StatusCode::OK, OPENMETRICS, shared.metrics.encode()),
            Err((status, message)) => (status, TEXT, message),
        },
        Route::Health => match shared.probes.health() {
            Health::Serving => (StatusCode::OK, TEXT, "ok\n".to_owned()),
            Health::ControlWriterStopped => (
                StatusCode::SERVICE_UNAVAILABLE,
                TEXT,
                "control writer stopped\n".to_owned(),
            ),
        },
        Route::Ready => {
            let readiness = shared.probes.readiness();
            let status = if readiness.ready {
                StatusCode::OK
            } else {
                StatusCode::SERVICE_UNAVAILABLE
            };
            let body = serde_json::to_string(&readiness)
                .unwrap_or_else(|_| "{\"ready\":false,\"checks\":[]}".to_owned());
            (status, JSON, body)
        }
        Route::WrongMethod => (
            StatusCode::METHOD_NOT_ALLOWED,
            TEXT,
            "use GET or HEAD\n".to_owned(),
        ),
        Route::NotFound => (StatusCode::NOT_FOUND, TEXT, "not found\n".to_owned()),
    };
    // A `HEAD` request gets the headers a `GET` would, without the body.
    let body = if head {
        Bytes::new()
    } else {
        Bytes::from(body)
    };
    let mut response = Response::new(Full::new(body));
    *response.status_mut() = status;
    let headers = response.headers_mut();
    headers.insert(CONTENT_TYPE, HeaderValue::from_static(content_type));
    if status == StatusCode::METHOD_NOT_ALLOWED {
        headers.insert(ALLOW, HeaderValue::from_static("GET, HEAD"));
    }
    response
}

/// Whether `peer` may read `/metrics`: anyone on a plaintext listener (`scrapers` is `None`),
/// and only a listed principal over TLS.
fn may_scrape(
    peer: Option<&Peer>,
    scrapers: Option<&[Principal]>,
) -> Result<(), (StatusCode, String)> {
    let Some(scrapers) = scrapers else {
        return Ok(());
    };
    let who = match peer {
        None => {
            return Err((
                StatusCode::UNAUTHORIZED,
                "a client certificate is required\n".to_owned(),
            ));
        }
        Some(Peer::Principal(name)) if scrapers.contains(name) => return Ok(()),
        Some(Peer::Principal(name)) => name.to_string(),
        Some(Peer::Node(id)) => format!("node:{id}"),
        Some(_) => "anonymous".to_owned(),
    };
    tracing::warn!(
        target: AUDIT,
        principal = %who,
        rpc = "admin /metrics",
        "denied",
    );
    Err((
        StatusCode::FORBIDDEN,
        format!("'{who}' may not read metrics\n"),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn routes_answer_get_and_head_only() {
        assert_eq!(route(&Method::GET, "/metrics"), Route::Metrics);
        assert_eq!(route(&Method::HEAD, "/healthz"), Route::Health);
        assert_eq!(route(&Method::GET, "/readyz"), Route::Ready);
        assert_eq!(route(&Method::POST, "/metrics"), Route::WrongMethod);
        assert_eq!(route(&Method::DELETE, "/readyz"), Route::WrongMethod);
        assert_eq!(route(&Method::GET, "/nope"), Route::NotFound);
        assert_eq!(route(&Method::POST, "/nope"), Route::NotFound);
        assert_eq!(route(&Method::GET, "/metrics/"), Route::NotFound);
    }

    #[test]
    fn only_listed_principals_may_scrape_over_tls() {
        let prometheus: Principal = "prometheus".parse().expect("a principal");
        let ops: Principal = "ops".parse().expect("a principal");
        let scrapers = [prometheus.clone()];
        assert!(may_scrape(None, None).is_ok(), "plaintext lets anyone read");
        assert_eq!(
            may_scrape(None, Some(&scrapers)).map_err(|(status, _)| status),
            Err(StatusCode::UNAUTHORIZED)
        );
        assert!(may_scrape(Some(&Peer::Principal(prometheus)), Some(&scrapers)).is_ok());
        assert_eq!(
            may_scrape(Some(&Peer::Principal(ops)), Some(&scrapers)).map_err(|(status, _)| status),
            Err(StatusCode::FORBIDDEN)
        );
        let node = Peer::Node("n1".parse().expect("a node id"));
        let refused = may_scrape(Some(&node), Some(&scrapers));
        assert_eq!(
            refused,
            Err((
                StatusCode::FORBIDDEN,
                "'node:n1' may not read metrics\n".to_owned()
            ))
        );
    }
}
