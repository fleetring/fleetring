//! The trace layer: an `rpc` span around every request, the RPC metrics, refusal counts, and the
//! request ID (ADR-0034 point 8).

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use fleetring::NodeId;
use http_body::{Body, Frame, SizeHint};
use tonic::codegen::Bytes;
use tonic::codegen::http::{self, HeaderMap, HeaderValue};
use tonic::{Code, Status};
use tower::{Layer, Service};
use tracing::Instrument as _;
use tracing::field::Empty;

use crate::auth::{BINDING_KEY, Refusal, rpc_at};
use crate::observe::Metrics;
use crate::observe::codes::code_label;
use crate::observe::metrics::{AuthMetrics, MethodLabels, RpcMetrics};
use crate::observe::request_id::{CURRENT, REQUEST_ID_KEY, RequestIds};

/// The methods that take or return a stream: their duration is how long the stream stays open,
/// which says nothing about latency, so only their counts are kept.
const STREAMING: &[&str] = &[
    "WatchMembership",
    "Replicate",
    "Snapshot",
    "WatchLeases",
    "WatchChanges",
];

/// The header a response's final status code is in: the response's own headers for a
/// trailers-only response, else its trailers.
const GRPC_STATUS: &str = "grpc-status";

/// The outermost layer on every service: an `rpc` span, RPC metrics, refusal counts, and the
/// request ID (ADR-0034 point 8).
///
/// For each request it resolves the request ID (an incoming `x-request-id`, or a new one, which
/// it puts back into the request's headers), opens an `rpc` span with `service`, `method`, and
/// `request_id`, and serves the request in that span with the ID in the task's scope. The
/// boundary records `principal` or `node`, and `database` when a handler checks one; this layer
/// records `code` and `duration_ms` once the response's status is known.
#[derive(Debug, Clone)]
pub struct TraceLayer {
    ids: Arc<RequestIds>,
    rpc: RpcMetrics,
    auth: AuthMetrics,
}

impl TraceLayer {
    /// A layer for the node `node`, counting in `metrics`: request IDs it makes start with
    /// `node`.
    #[must_use]
    pub fn new(node: NodeId, metrics: &Metrics) -> Self {
        Self {
            ids: Arc::new(RequestIds::new(node)),
            rpc: metrics.rpc().clone(),
            auth: metrics.auth().clone(),
        }
    }
}

impl<S> Layer<S> for TraceLayer {
    type Service = TraceService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        TraceService {
            inner,
            layer: self.clone(),
        }
    }
}

/// A service inside a [`TraceLayer`].
#[derive(Debug, Clone)]
pub struct TraceService<S> {
    inner: S,
    layer: TraceLayer,
}

type BoxFuture<T, E> = Pin<Box<dyn Future<Output = Result<T, E>> + Send>>;

/// The `service` label of a gRPC service's full name.
fn service_label(service: &str) -> &'static str {
    match service {
        "fleetring.v1.Control" => "control",
        "fleetring.v1.Replication" => "replication",
        "fleetring.v1.Database" => "database",
        _ => "unknown",
    }
}

impl<S, B> Service<http::Request<B>> for TraceService<S>
where
    S: Service<http::Request<B>, Response = http::Response<tonic::body::Body>>,
    S::Future: Send + 'static,
    S::Error: Send + 'static,
{
    type Response = http::Response<tonic::body::Body>;
    type Error = S::Error;
    type Future = BoxFuture<Self::Response, Self::Error>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, mut request: http::Request<B>) -> Self::Future {
        let (service, method) = rpc_at(request.uri().path())
            .map_or(("unknown", "unknown"), |rpc| {
                (service_label(rpc.service()), rpc.method())
            });
        let incoming = request
            .headers()
            .get(REQUEST_ID_KEY)
            .and_then(|value| value.to_str().ok());
        let id = self.layer.ids.resolve(incoming);
        if let Ok(value) = HeaderValue::from_str(id.as_str()) {
            request.headers_mut().insert(REQUEST_ID_KEY, value);
        }
        let span = tracing::info_span!(
            "rpc",
            service,
            method,
            request_id = %id,
            principal = Empty,
            node = Empty,
            database = Empty,
            code = Empty,
            duration_ms = Empty,
        );
        let labels = MethodLabels { service, method };
        self.layer.rpc.started(&labels);
        let observer = Observer {
            labels,
            streaming: STREAMING.contains(&method),
            started: tokio::time::Instant::now(),
            rpc: self.layer.rpc.clone(),
            auth: self.layer.auth.clone(),
            span: span.clone(),
            finished: false,
        };
        let future = span.in_scope(|| self.inner.call(request));
        let served = async move {
            let mut observer = observer;
            let response = future.await?;
            let (parts, body) = response.into_parts();
            let refusal = parts.extensions.get::<Refusal>().copied();
            observer.refusal(refusal);
            if let Some(code) = status_in(&parts.headers) {
                // A trailers-only response: a handler's error, or a refusal at the boundary.
                observer.finish(code, &parts.headers, refusal);
            }
            let body = ObservedBody {
                inner: body,
                observer: (!observer.finished).then_some(observer),
            };
            Ok(http::Response::from_parts(
                parts,
                tonic::body::Body::new(body),
            ))
        };
        Box::pin(CURRENT.scope(id, served.instrument(span)))
    }
}

/// The status code in `headers`' `grpc-status`, if it has one.
fn status_in(headers: &HeaderMap) -> Option<Code> {
    let value = headers
        .get(GRPC_STATUS)?
        .to_str()
        .ok()?
        .parse::<i32>()
        .ok()?;
    Some(Code::from_i32(value))
}

/// Watches one RPC until its final status is known, then records it once: in the span, the RPC
/// metrics, and the refusal count. An RPC whose response is dropped before a status counts as
/// `cancelled`.
struct Observer {
    labels: MethodLabels,
    streaming: bool,
    started: tokio::time::Instant,
    rpc: RpcMetrics,
    auth: AuthMetrics,
    span: tracing::Span,
    finished: bool,
}

impl Observer {
    /// Counts a refusal the boundary marked, whatever its code.
    fn refusal(&mut self, refusal: Option<Refusal>) {
        if let Some(refusal) = refusal {
            self.auth.refused(refusal.reason());
        }
    }

    /// Records `code`, the final status, with `headers` the headers or trailers that carried it,
    /// unless it was recorded already. `marked` is the boundary's refusal, which was counted.
    fn finish(&mut self, code: Code, headers: &HeaderMap, marked: Option<Refusal>) {
        if self.finished {
            return;
        }
        self.finished = true;
        let elapsed = self.started.elapsed();
        let label = code_label(code);
        if marked.is_none() {
            match code {
                Code::PermissionDenied if headers.contains_key(BINDING_KEY) => {
                    self.auth.refused("binding");
                }
                Code::PermissionDenied => self.auth.refused("scope"),
                Code::Unauthenticated => self.auth.refused("unauthenticated"),
                _ => {}
            }
        }
        self.rpc
            .finished(&self.labels, label, (!self.streaming).then_some(elapsed));
        // Only the debug line below follows these fields, so at info they would be formatted
        // for nothing on every request (docs/perf.md, "Request path").
        if tracing::enabled!(tracing::Level::DEBUG) {
            let duration_ms = u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX);
            self.span.record("code", label);
            self.span.record("duration_ms", duration_ms);
            tracing::debug!(parent: &self.span, "rpc finished");
        }
    }
}

impl Drop for Observer {
    fn drop(&mut self) {
        self.finish(Code::Cancelled, &HeaderMap::new(), None);
    }
}

/// A response body that records the RPC's final status when its trailers pass, or when it is
/// dropped first.
struct ObservedBody {
    inner: tonic::body::Body,
    observer: Option<Observer>,
}

impl ObservedBody {
    fn finish(&mut self, code: Code, headers: &HeaderMap) {
        if let Some(mut observer) = self.observer.take() {
            observer.finish(code, headers, None);
        }
    }
}

impl Body for ObservedBody {
    type Data = Bytes;
    type Error = Status;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let this = self.get_mut();
        let polled = Pin::new(&mut this.inner).poll_frame(cx);
        match &polled {
            Poll::Ready(Some(Ok(frame))) => {
                if let Some(trailers) = frame.trailers_ref() {
                    let code = status_in(trailers).unwrap_or(Code::Unknown);
                    this.finish(code, trailers);
                }
            }
            Poll::Ready(Some(Err(status))) => {
                this.finish(status.code(), status.metadata().as_ref())
            }
            Poll::Ready(None) => this.finish(Code::Unknown, &HeaderMap::new()),
            Poll::Pending => {}
        }
        polled
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> SizeHint {
        self.inner.size_hint()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::convert::Infallible;

    use super::*;
    use crate::NodeRole;
    use crate::observe::NodeInfo;

    fn metrics() -> Metrics {
        Metrics::new(&NodeInfo::new(
            "n1".parse().expect("a node id"),
            NodeRole::Both,
            None,
        ))
    }

    fn sample(text: &str, series: &str) -> Option<String> {
        text.lines()
            .find_map(|line| line.strip_prefix(series)?.strip_prefix(' '))
            .map(str::to_owned)
    }

    #[test]
    fn streaming_methods_match_the_proto() {
        let proto = include_str!("../../../fleetring-proto/proto/fleetring/v1/fleetring.proto");
        let streaming: BTreeSet<&str> = proto
            .lines()
            .filter_map(|line| line.trim().strip_prefix("rpc "))
            .filter(|rpc| rpc.contains("stream "))
            .filter_map(|rpc| rpc.split('(').next())
            .collect();
        assert_eq!(streaming, STREAMING.iter().copied().collect());
    }

    #[tokio::test]
    async fn a_trailers_only_error_is_counted_with_its_code() {
        let metrics = metrics();
        let mut service = TraceLayer::new("n1".parse().expect("a node id"), &metrics).layer(
            tower::service_fn(|_: http::Request<()>| async {
                Ok::<_, Infallible>(Status::not_found("no such database").into_http())
            }),
        );
        let request = http::Request::builder()
            .uri("/fleetring.v1.Control/Locate")
            .body(())
            .expect("a request");
        let response = service.call(request).await.expect("a response");
        drop(response);
        let text = metrics.encode();
        assert_eq!(
            sample(
                &text,
                "fleetring_rpc_requests_total{service=\"control\",method=\"Locate\",code=\"not_found\"}"
            )
            .as_deref(),
            Some("1"),
            "{text}"
        );
        assert_eq!(
            sample(
                &text,
                "fleetring_rpc_in_flight{service=\"control\",method=\"Locate\"}"
            )
            .as_deref(),
            Some("0"),
            "{text}"
        );
        assert_eq!(
            sample(
                &text,
                "fleetring_rpc_duration_seconds_count{service=\"control\",method=\"Locate\"}"
            )
            .as_deref(),
            Some("1"),
            "{text}"
        );
    }

    #[tokio::test]
    async fn refusals_are_counted_by_reason() {
        let metrics = metrics();
        let mut service = TraceLayer::new("n1".parse().expect("a node id"), &metrics).layer(
            tower::service_fn(|request: http::Request<()>| async move {
                let response = match request.uri().path() {
                    "/fleetring.v1.Database/Execute" => {
                        Status::permission_denied("no scope").into_http()
                    }
                    "/fleetring.v1.Replication/Fence" => {
                        let mut status = Status::permission_denied("not bound");
                        status
                            .metadata_mut()
                            .insert(BINDING_KEY, "replica".parse().expect("metadata"));
                        status.into_http()
                    }
                    _ => {
                        let mut response = Status::unavailable("stale").into_http();
                        response.extensions_mut().insert(Refusal::DenylistStale);
                        response
                    }
                };
                Ok::<_, Infallible>(response)
            }),
        );
        for path in [
            "/fleetring.v1.Database/Execute",
            "/fleetring.v1.Replication/Fence",
            "/fleetring.v1.Database/Query",
        ] {
            let request = http::Request::builder()
                .uri(path)
                .body(())
                .expect("a request");
            drop(service.call(request).await.expect("a response"));
        }
        let text = metrics.encode();
        for reason in ["scope", "binding", "denylist_stale"] {
            assert_eq!(
                sample(
                    &text,
                    &format!("fleetring_auth_refusals_total{{reason=\"{reason}\"}}")
                )
                .as_deref(),
                Some("1"),
                "{reason}: {text}"
            );
        }
    }

    /// A body that never yields a frame.
    struct Endless;

    impl Body for Endless {
        type Data = Bytes;
        type Error = Status;

        fn poll_frame(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
            Poll::Pending
        }
    }

    #[tokio::test]
    async fn a_dropped_stream_counts_as_cancelled() {
        let metrics = metrics();
        let mut service = TraceLayer::new("n1".parse().expect("a node id"), &metrics).layer(
            tower::service_fn(|_: http::Request<()>| async {
                Ok::<_, Infallible>(http::Response::new(tonic::body::Body::new(Endless)))
            }),
        );
        let request = http::Request::builder()
            .uri("/fleetring.v1.Control/WatchMembership")
            .body(())
            .expect("a request");
        let response = service.call(request).await.expect("a response");
        let in_flight = "fleetring_rpc_in_flight{service=\"control\",method=\"WatchMembership\"}";
        assert_eq!(sample(&metrics.encode(), in_flight).as_deref(), Some("1"));
        drop(response);
        let text = metrics.encode();
        assert_eq!(sample(&text, in_flight).as_deref(), Some("0"), "{text}");
        assert_eq!(
            sample(
                &text,
                "fleetring_rpc_requests_total{service=\"control\",method=\"WatchMembership\",code=\"cancelled\"}"
            )
            .as_deref(),
            Some("1"),
            "{text}"
        );
        assert!(
            !text.contains("fleetring_rpc_duration_seconds_count{service=\"control\",method=\"WatchMembership\"}"),
            "streams aren't timed: {text}"
        );
    }

    #[tokio::test]
    async fn the_request_id_is_kept_and_in_scope() {
        let metrics = metrics();
        let mut service = TraceLayer::new("n1".parse().expect("a node id"), &metrics).layer(
            tower::service_fn(|request: http::Request<()>| async move {
                let header = request
                    .headers()
                    .get(REQUEST_ID_KEY)
                    .and_then(|value| value.to_str().ok())
                    .map(str::to_owned);
                let scoped = crate::observe::request_id::current().map(|id| id.to_string());
                assert_eq!(header, scoped);
                let mut response = Status::ok("").into_http();
                if let Some(header) = header {
                    response.headers_mut().insert(
                        "x-seen",
                        HeaderValue::from_str(&header).expect("a header value"),
                    );
                }
                Ok::<_, Infallible>(response)
            }),
        );
        let request = http::Request::builder()
            .uri("/fleetring.v1.Control/Locate")
            .header(REQUEST_ID_KEY, "req-42")
            .body(())
            .expect("a request");
        let response = service.call(request).await.expect("a response");
        assert_eq!(response.headers()["x-seen"], "req-42");
        let request = http::Request::builder()
            .uri("/fleetring.v1.Control/Locate")
            .header(REQUEST_ID_KEY, "not valid!")
            .body(())
            .expect("a request");
        let response = service.call(request).await.expect("a response");
        assert_eq!(response.headers()["x-seen"], "n1-0000000000000001");
    }
}
