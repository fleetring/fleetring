//! FR-023: the gRPC transport over real TCP and over turmoil.

use std::error::Error;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use fleetring_proto::v1::control_server::Control;
use fleetring_proto::v1::replicate_message::Message;
use fleetring_proto::v1::replication_server::Replication;
use fleetring_proto::v1::{
    AcquireLeaseRequest, Batch, CompletePromotionRequest, CompletePromotionResponse,
    CreateDatabaseRequest, CreateDatabaseResponse, DropDatabaseRequest, DropDatabaseResponse,
    FenceRequest, FenceResponse, FetchRequest, FetchResponse, HeartbeatRequest, HeartbeatResponse,
    JoinRequest, JoinResponse, LeaseResponse, LeaveRequest, LeaveResponse, ListDatabasesRequest,
    ListDatabasesResponse, LocateRequest, LocateResponse, MembershipUpdate, Node, PromoteRequest,
    PromoteResponse, PromotionResponse, ProtocolVersion, RecordFencedRequest, RecordFencedResponse,
    RenewLeaseRequest, ReplicateAck, ReplicateMessage, ReplicateOpen, SnapshotChunk,
    SnapshotRequest, StartPromotionRequest, WatchMembershipRequest,
};
use fleetring_proto::{PROTOCOL_VERSION, check_protocol};
use fleetringd::transport::{
    Backoff, Peers, TransportConfig, control_server, replication_server, retry,
};
use tokio_stream::Stream;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Code, Request, Response, Status, Streaming};

type TestResult = Result<(), Box<dyn Error>>;
type BoxStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send>>;

const MEMBERSHIP_EPOCH: u64 = 7;

/// Answers `Join` and `Heartbeat` the way the control node will, and nothing else.
#[derive(Debug, Default)]
struct StubControl;

#[tonic::async_trait]
impl Control for StubControl {
    type WatchMembershipStream = BoxStream<MembershipUpdate>;

    async fn join(&self, request: Request<JoinRequest>) -> Result<Response<JoinResponse>, Status> {
        check_protocol(request.get_ref().protocol.as_ref())?;
        Ok(Response::new(JoinResponse {
            protocol: Some(PROTOCOL_VERSION),
            membership_epoch: MEMBERSHIP_EPOCH,
        }))
    }

    async fn heartbeat(
        &self,
        _request: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatResponse>, Status> {
        Ok(Response::new(HeartbeatResponse {
            membership_epoch: MEMBERSHIP_EPOCH,
            unknown_databases: Vec::new(),
        }))
    }

    async fn watch_membership(
        &self,
        _request: Request<WatchMembershipRequest>,
    ) -> Result<Response<Self::WatchMembershipStream>, Status> {
        Err(Status::unimplemented("stub"))
    }

    async fn create_database(
        &self,
        _request: Request<CreateDatabaseRequest>,
    ) -> Result<Response<CreateDatabaseResponse>, Status> {
        Err(Status::unimplemented("stub"))
    }

    async fn drop_database(
        &self,
        _request: Request<DropDatabaseRequest>,
    ) -> Result<Response<DropDatabaseResponse>, Status> {
        Err(Status::unimplemented("stub"))
    }

    async fn locate(
        &self,
        _request: Request<LocateRequest>,
    ) -> Result<Response<LocateResponse>, Status> {
        Err(Status::unimplemented("stub"))
    }

    async fn list_databases(
        &self,
        _request: Request<ListDatabasesRequest>,
    ) -> Result<Response<ListDatabasesResponse>, Status> {
        Err(Status::unimplemented("stub"))
    }

    async fn acquire_lease(
        &self,
        _request: Request<AcquireLeaseRequest>,
    ) -> Result<Response<LeaseResponse>, Status> {
        Err(Status::unimplemented("stub"))
    }

    async fn renew_lease(
        &self,
        _request: Request<RenewLeaseRequest>,
    ) -> Result<Response<LeaseResponse>, Status> {
        Err(Status::unimplemented("stub"))
    }

    async fn leave(
        &self,
        _request: Request<LeaveRequest>,
    ) -> Result<Response<LeaveResponse>, Status> {
        Err(Status::unimplemented("stub"))
    }

    async fn start_promotion(
        &self,
        _request: Request<StartPromotionRequest>,
    ) -> Result<Response<PromotionResponse>, Status> {
        Err(Status::unimplemented("stub"))
    }

    async fn record_fenced(
        &self,
        _request: Request<RecordFencedRequest>,
    ) -> Result<Response<RecordFencedResponse>, Status> {
        Err(Status::unimplemented("stub"))
    }

    async fn complete_promotion(
        &self,
        _request: Request<CompletePromotionRequest>,
    ) -> Result<Response<CompletePromotionResponse>, Status> {
        Err(Status::unimplemented("stub"))
    }
}

/// Acknowledges every batch with its end position.
#[derive(Debug, Default)]
struct StubReplication;

#[tonic::async_trait]
impl Replication for StubReplication {
    type ReplicateStream = BoxStream<ReplicateAck>;
    type SnapshotStream = BoxStream<SnapshotChunk>;

    async fn replicate(
        &self,
        request: Request<Streaming<ReplicateMessage>>,
    ) -> Result<Response<Self::ReplicateStream>, Status> {
        let mut incoming = request.into_inner();
        let (acks, receiver) = tokio::sync::mpsc::channel(16);
        tokio::spawn(async move {
            while let Ok(Some(message)) = incoming.message().await {
                if let Some(Message::Batch(batch)) = message.message {
                    let ack = ReplicateAck {
                        position: batch.end,
                        needs_snapshot: false,
                    };
                    if acks.send(Ok(ack)).await.is_err() {
                        break;
                    }
                }
            }
        });
        Ok(Response::new(Box::pin(ReceiverStream::new(receiver))))
    }

    async fn fence(
        &self,
        _request: Request<FenceRequest>,
    ) -> Result<Response<FenceResponse>, Status> {
        Err(Status::unimplemented("stub"))
    }

    async fn snapshot(
        &self,
        _request: Request<SnapshotRequest>,
    ) -> Result<Response<Self::SnapshotStream>, Status> {
        Err(Status::unimplemented("stub"))
    }

    async fn fetch(
        &self,
        _request: Request<FetchRequest>,
    ) -> Result<Response<FetchResponse>, Status> {
        Err(Status::unimplemented("stub"))
    }

    async fn promote(
        &self,
        _request: Request<PromoteRequest>,
    ) -> Result<Response<PromoteResponse>, Status> {
        Err(Status::unimplemented("stub"))
    }
}

fn join_request(major: u32) -> JoinRequest {
    JoinRequest {
        protocol: Some(ProtocolVersion { major, minor: 0 }),
        node: Some(Node {
            id: "node-1".into(),
            zone: Some("rack-a".into()),
            host: None,
            weight: 1,
            address: None,
        }),
    }
}

/// Serves the stubs on a random local port and returns the port's address.
async fn serve_locally() -> Result<std::net::SocketAddr, Box<dyn Error>> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let incoming = tonic::transport::server::TcpIncoming::from(listener);
    tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(control_server(StubControl))
            .add_service(replication_server(StubReplication))
            .serve_with_incoming(incoming),
    );
    Ok(address)
}

#[tokio::test]
async fn join_and_heartbeat_over_tcp() -> TestResult {
    let peers = Peers::new(TransportConfig::default());
    let control_node = serve_locally().await?;
    let joined = peers.control(control_node).join(join_request(1)).await?;
    assert_eq!(joined.get_ref().membership_epoch, MEMBERSHIP_EPOCH);
    assert_eq!(joined.get_ref().protocol, Some(PROTOCOL_VERSION));
    let beat = peers
        .control(control_node)
        .heartbeat(HeartbeatRequest {
            node_id: "node-1".into(),
            databases: Vec::new(),
        })
        .await?;
    assert_eq!(beat.get_ref().membership_epoch, MEMBERSHIP_EPOCH);
    Ok(())
}

#[tokio::test]
async fn an_unsupported_protocol_version_is_refused_at_join() -> TestResult {
    let peers = Peers::new(TransportConfig::default());
    let control_node = serve_locally().await?;
    let refused = peers
        .control(control_node)
        .join(join_request(2))
        .await
        .expect_err("major version 2 must be refused");
    assert_eq!(refused.code(), Code::FailedPrecondition);
    assert!(
        refused
            .message()
            .contains("unsupported protocol version 2.0"),
        "{refused}"
    );
    Ok(())
}

#[tokio::test]
async fn a_four_mib_batch_round_trips_as_one_message() -> TestResult {
    let peers = Peers::new(TransportConfig::default());
    let follower = serve_locally().await?;
    let end = 0x0102_0304_u64.to_be_bytes().to_vec();
    let messages = vec![
        ReplicateMessage {
            message: Some(Message::Open(ReplicateOpen {
                database: "tenant:acme".into(),
                epoch: 1,
                epoch_start: None,
            })),
        },
        ReplicateMessage {
            message: Some(Message::Batch(Batch {
                epoch: 1,
                start: 0_u64.to_be_bytes().to_vec(),
                end: end.clone(),
                payload: vec![0xa5; 4 * 1024 * 1024],
            })),
        },
    ];
    let mut acks = peers
        .replication(follower)
        .replicate(tokio_stream::iter(messages))
        .await?
        .into_inner();
    let ack = acks.message().await?.ok_or("no acknowledgement")?;
    assert_eq!(ack.position, end);
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn retry_repeats_only_retryable_failures() -> TestResult {
    let calls = Arc::new(AtomicU32::new(0));
    let counter = Arc::clone(&calls);
    let value = retry(Backoff::default(), || {
        let counter = Arc::clone(&counter);
        async move {
            match counter.fetch_add(1, Ordering::SeqCst) {
                0 | 1 => Err(Status::unavailable("peer restarting")),
                _ => Ok("joined"),
            }
        }
    })
    .await?;
    assert_eq!(value, "joined");
    assert_eq!(calls.load(Ordering::SeqCst), 3);

    calls.store(0, Ordering::SeqCst);
    let counter = Arc::clone(&calls);
    let refused = retry(Backoff::default(), || {
        counter.fetch_add(1, Ordering::SeqCst);
        async { Err::<(), _>(Status::failed_precondition("unsupported protocol")) }
    })
    .await
    .expect_err("not retryable");
    assert_eq!(refused.code(), Code::FailedPrecondition);
    assert_eq!(calls.load(Ordering::SeqCst), 1);

    calls.store(0, Ordering::SeqCst);
    let counter = Arc::clone(&calls);
    let backoff = Backoff {
        attempts: 3,
        ..Backoff::default()
    };
    retry(backoff, || {
        counter.fetch_add(1, Ordering::SeqCst);
        async { Err::<(), _>(Status::unavailable("down")) }
    })
    .await
    .expect_err("gives up");
    assert_eq!(calls.load(Ordering::SeqCst), 3);
    Ok(())
}

#[cfg(feature = "simulation")]
mod simulated {
    use fleetringd::transport::sim;
    use fleetringd::transport::{control_client, control_server};

    use super::{HeartbeatRequest, MEMBERSHIP_EPOCH, StubControl, join_request};

    #[test]
    fn join_and_heartbeat_over_turmoil() -> turmoil::Result {
        let mut simulation = turmoil::Builder::new().build();
        simulation.host("control", || async {
            let listener = turmoil::net::TcpListener::bind("0.0.0.0:7400").await?;
            tonic::transport::Server::builder()
                .add_service(control_server(StubControl))
                .serve_with_incoming(sim::incoming(listener))
                .await?;
            Ok(())
        });
        simulation.client("node-1", async {
            let mut control = control_client(sim::channel("control", 7400)?);
            let joined = control.join(join_request(1)).await?;
            assert_eq!(joined.get_ref().membership_epoch, MEMBERSHIP_EPOCH);
            let beat = control
                .heartbeat(HeartbeatRequest {
                    node_id: "node-1".into(),
                    databases: Vec::new(),
                })
                .await?;
            assert_eq!(beat.get_ref().membership_epoch, MEMBERSHIP_EPOCH);
            Ok(())
        });
        simulation.run()
    }
}
