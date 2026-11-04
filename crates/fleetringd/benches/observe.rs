//! The request path's cost of the trace layer (FR-063, ADR-0034). Run with
//! `cargo bench -p fleetringd --bench observe`.
//!
//! Each case serves one unary request to a service that answers at once with one data frame
//! and `grpc-status: 0` trailers, and reads the whole response, as hyper does: `bare` calls the
//! service itself, and `traced` calls it through [`TraceLayer`], which resolves the request ID,
//! opens the `rpc` span, counts and times the RPC, and records its status from the trailers. The
//! daemon's log subscriber is installed at `info` (its default) or `debug`, writing to nowhere.
//! The difference between `traced` and `bare` is what every RPC pays. Numbers live in
//! `docs/perf.md`.

use std::convert::Infallible;
use std::fmt;
use std::pin::Pin;
use std::task::{Context, Poll};

use divan::Bencher;
use fleetringd::LogFormat;
use fleetringd::observe::{Metrics, NodeInfo, TraceLayer, log_subscriber};
use http_body::{Body, Frame};
use http_body_util::BodyExt as _;
use tonic::codegen::Bytes;
use tonic::codegen::http::{self, HeaderMap, HeaderValue};
use tower::{Layer as _, Service as _};

fn main() {
    divan::main();
}

/// A unary gRPC response body: one message, then the trailers with the status.
struct Unary {
    message: Option<Bytes>,
    trailers: Option<HeaderMap>,
}

impl Unary {
    fn new() -> Self {
        let mut trailers = HeaderMap::new();
        trailers.insert("grpc-status", HeaderValue::from_static("0"));
        Self {
            message: Some(Bytes::from_static(&[0, 0, 0, 0, 4, 10, 2, 104, 105])),
            trailers: Some(trailers),
        }
    }
}

impl Body for Unary {
    type Data = Bytes;
    type Error = tonic::Status;

    fn poll_frame(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, tonic::Status>>> {
        let this = self.get_mut();
        if let Some(message) = this.message.take() {
            return Poll::Ready(Some(Ok(Frame::data(message))));
        }
        Poll::Ready(
            this.trailers
                .take()
                .map(|trailers| Ok(Frame::trailers(trailers))),
        )
    }
}

fn request() -> http::Request<()> {
    http::Request::builder()
        .uri("/fleetring.v1.Control/Locate")
        .header("x-request-id", "bench")
        .body(())
        .unwrap_or_default()
}

/// The service a request reaches: answers at once.
fn inner() -> impl tower::Service<
    http::Request<()>,
    Response = http::Response<tonic::body::Body>,
    Error = Infallible,
    Future = std::future::Ready<Result<http::Response<tonic::body::Body>, Infallible>>,
> + Clone {
    tower::service_fn(|_request: http::Request<()>| {
        std::future::ready(Ok::<_, Infallible>(http::Response::new(
            tonic::body::Body::new(Unary::new()),
        )))
    })
}

/// The level the daemon's log subscriber runs at.
#[derive(Debug, Clone, Copy)]
enum Level {
    Info,
    Debug,
}

impl fmt::Display for Level {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Info => "info",
            Self::Debug => "debug",
        })
    }
}

const LEVELS: &[Level] = &[Level::Info, Level::Debug];

fn subscriber(level: Level) -> tracing::subscriber::DefaultGuard {
    let filter = tracing_subscriber::EnvFilter::new(level.to_string());
    tracing::subscriber::set_default(log_subscriber(LogFormat::Text, filter, std::io::sink))
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap_or_else(|error| panic!("a runtime: {error}"))
}

#[divan::bench(args = LEVELS, sample_count = 200, sample_size = 200)]
fn bare(bencher: Bencher, level: &Level) {
    let _subscriber = subscriber(*level);
    let runtime = runtime();
    let mut service = inner();
    bencher.bench_local(|| {
        runtime.block_on(async {
            let Ok(response) = service.call(request()).await;
            let _collected = response.into_body().collect().await;
        });
    });
}

#[divan::bench(args = LEVELS, sample_count = 200, sample_size = 200)]
fn traced(bencher: Bencher, level: &Level) {
    let _subscriber = subscriber(*level);
    let runtime = runtime();
    let metrics = Metrics::new(&NodeInfo::new(
        "bench"
            .parse()
            .unwrap_or_else(|error| panic!("a node id: {error}")),
        fleetringd::NodeRole::Both,
        None,
    ));
    let mut service = TraceLayer::new(
        "bench"
            .parse()
            .unwrap_or_else(|error| panic!("a node id: {error}")),
        &metrics,
    )
    .layer(inner());
    bencher.bench_local(|| {
        runtime.block_on(async {
            let Ok(response) = service.call(request()).await;
            let _collected = response.into_body().collect().await;
        });
    });
}
