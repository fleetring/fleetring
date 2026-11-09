//! The router against a fake data node, for what a real one rarely does on cue: lose a
//! session after it took a call.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use fleetring_control::{Millis, Timestamp};
use fleetring_proto::v1::database_server::{Database, DatabaseServer};
use fleetring_proto::v1::{self as proto, session_response::Response as Answer};
use hyper::HeaderMap;
use hyper::header::AUTHORIZATION;
use serde_json::json;
use tokio::sync::mpsc;
use tokio_stream::wrappers::{ReceiverStream, TcpListenerStream};
use tonic::{Request, Response, Status, Streaming};

use super::*;
use crate::auth::{BoxStream, Scope, TokenSpec};
use crate::clock::SimClock;
use crate::router::hrana::{PipelineRequest, StreamResult};
use crate::router::naming::{Endpoint, Hrana, Target};
use crate::tls::{Authority, Validity};
use crate::transport::TransportConfig;

/// Counts what a session receives, and ends each one with `UNAVAILABLE` as soon as it receives
/// a call, without answering it: the call's outcome is unknown to its caller.
#[derive(Debug, Default, Clone)]
struct Vanishing {
    opens: Arc<AtomicUsize>,
    calls: Arc<AtomicUsize>,
}

fn unimplemented<T>() -> Result<Response<T>, Status> {
    Err(Status::unimplemented("the fake serves sessions only"))
}

#[tonic::async_trait]
impl Database for Vanishing {
    type WatchLeasesStream = BoxStream<proto::LeaseUpdate>;
    type WatchChangesStream = BoxStream<proto::ChangeUpdate>;
    type SessionStream = BoxStream<proto::SessionResponse>;

    async fn execute(
        &self,
        _: Request<proto::ExecuteRequest>,
    ) -> Result<Response<proto::ExecuteResponse>, Status> {
        unimplemented()
    }
    async fn query(
        &self,
        _: Request<proto::QueryRequest>,
    ) -> Result<Response<proto::QueryResponse>, Status> {
        unimplemented()
    }
    async fn create(
        &self,
        _: Request<proto::CreateRequest>,
    ) -> Result<Response<proto::CreateResponse>, Status> {
        unimplemented()
    }
    async fn node_status(
        &self,
        _: Request<proto::NodeStatusRequest>,
    ) -> Result<Response<proto::NodeStatusResponse>, Status> {
        unimplemented()
    }
    async fn watch_leases(
        &self,
        _: Request<proto::WatchLeasesRequest>,
    ) -> Result<Response<Self::WatchLeasesStream>, Status> {
        unimplemented()
    }
    async fn verify(
        &self,
        _: Request<proto::VerifyRequest>,
    ) -> Result<Response<proto::VerifyResponse>, Status> {
        unimplemented()
    }
    async fn restore(
        &self,
        _: Request<proto::RestoreRequest>,
    ) -> Result<Response<proto::RestoreResponse>, Status> {
        unimplemented()
    }
    async fn describe_archive(
        &self,
        _: Request<proto::DescribeArchiveRequest>,
    ) -> Result<Response<proto::DescribeArchiveResponse>, Status> {
        unimplemented()
    }
    async fn verify_archive(
        &self,
        _: Request<proto::VerifyArchiveRequest>,
    ) -> Result<Response<proto::VerifyArchiveResponse>, Status> {
        unimplemented()
    }
    async fn schema(
        &self,
        _: Request<proto::SchemaRequest>,
    ) -> Result<Response<proto::SchemaResponse>, Status> {
        unimplemented()
    }
    async fn remove_orphan(
        &self,
        _: Request<proto::RemoveOrphanRequest>,
    ) -> Result<Response<proto::RemoveOrphanResponse>, Status> {
        unimplemented()
    }
    async fn watch_changes(
        &self,
        _: Request<proto::WatchChangesRequest>,
    ) -> Result<Response<Self::WatchChangesStream>, Status> {
        unimplemented()
    }

    async fn session(
        &self,
        request: Request<Streaming<proto::SessionRequest>>,
    ) -> Result<Response<Self::SessionStream>, Status> {
        let mut incoming = request.into_inner();
        let opened = incoming.message().await?;
        assert!(opened.is_some(), "a session opens first");
        self.opens.fetch_add(1, Ordering::SeqCst);
        let (sender, receiver) = mpsc::channel(4);
        let calls = Arc::clone(&self.calls);
        tokio::spawn(async move {
            let opened = proto::SessionResponse {
                response: Some(Answer::Opened(proto::SessionOpened {
                    primary: "fake".to_owned(),
                    epoch: 1,
                    read_only: false,
                })),
            };
            let _ = sender.send(Ok(opened)).await;
            if let Ok(Some(_)) = incoming.message().await {
                calls.fetch_add(1, Ordering::SeqCst);
            }
            let _ = sender
                .send(Err(Status::unavailable("the node went away")))
                .await;
        });
        Ok(Response::new(Box::pin(ReceiverStream::new(receiver))))
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_lost_reply_is_an_error_and_never_resent() {
    let fake = Vanishing::default();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("a port");
    let node = listener.local_addr().expect("an address");
    let server = tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(DatabaseServer::new(fake.clone()))
            .serve_with_incoming(TcpListenerStream::new(listener)),
    );

    let validity = Validity::new(Timestamp::ZERO, Duration::from_secs(100 * 365 * 24 * 3600));
    let authority = Authority::create("test".parse().expect("a fleet"), validity).expect("ca");
    let issued = authority
        .issue_client(&"router".parse().expect("a principal"), validity)
        .expect("a certificate");
    let identity = Identity::from_issued(authority.certificate_pem(), &issued).expect("identity");
    let now = Timestamp::from_millis(1_000);
    let token = authority
        .token_signer()
        .expect("a signer")
        .issue(&TokenSpec {
            principal: "app".parse().expect("a principal"),
            scopes: ["write:notes".parse::<Scope>().expect("a scope")]
                .into_iter()
                .collect(),
            id: "t1".parse().expect("an id"),
            not_before: now,
            expires_at: now.saturating_add(Millis::new(3_600_000)),
        })
        .expect("a token");
    let mut headers = HeaderMap::new();
    headers.insert(
        AUTHORIZATION,
        format!("Bearer {}", token.expose())
            .parse()
            .expect("a header"),
    );
    let caller = auth::authenticate(&headers, &identity.token_verifier(), now).expect("a caller");

    let config =
        RouterConfig::from_toml("listen = \"127.0.0.1:0\"\n[control]\naddress = \"127.0.0.1:1\"\n")
            .expect("a config");
    let context = Context {
        placement: PlacementCache::new(Duration::from_secs(60), 10),
        streams: Streams::new(config.streams.clone()),
        // Plaintext, to reach the fake; the control plane is never asked.
        peers: Peers::new(TransportConfig::default()),
        control: ControlClient::new("127.0.0.1:1", TransportConfig::default()),
        clock: Arc::new(SimClock::new(now)),
        identity,
        entropy: Arc::new(SystemEntropy::new()),
        config,
    };
    let database = "notes".parse().expect("a name");
    context.placement.redirected(&database, node.to_string());
    let target = Target {
        database,
        endpoint: Endpoint::Pipeline(Hrana::V3),
        prefix: "/db/notes".to_owned(),
    };
    let request = |baton: Option<String>| -> PipelineRequest {
        serde_json::from_value(json!({
            "baton": baton,
            "requests": [{"type": "execute", "stmt": {"sql": "INSERT INTO t VALUES (1)"}}],
        }))
        .expect("a pipeline")
    };
    let first = pipeline::pipeline(&context, &target, Hrana::V3, &caller, request(None))
        .await
        .expect("an answer");
    let StreamResult::Error { error } = &first.body.results[0] else {
        panic!("the insert's outcome is unknown: {:?}", first.body.results);
    };
    assert_eq!(
        error.code.as_deref(),
        Some("FLEETRING_OUTCOME_UNKNOWN"),
        "{error:?}"
    );
    // The stream is broken: its next request fails the same way, and opens nothing.
    let second = pipeline::pipeline(
        &context,
        &target,
        Hrana::V3,
        &caller,
        request(first.body.baton),
    )
    .await
    .expect("an answer");
    let StreamResult::Error { error } = &second.body.results[0] else {
        panic!("the stream is broken: {:?}", second.body.results);
    };
    assert_eq!(error.code.as_deref(), Some("FLEETRING_OUTCOME_UNKNOWN"));
    assert_eq!(fake.opens.load(Ordering::SeqCst), 1, "one session");
    assert_eq!(
        fake.calls.load(Ordering::SeqCst),
        1,
        "the call was sent once, never again"
    );
    server.abort();
}
