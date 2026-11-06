//! FR-028: the control node's log, writer, service, and client, over real TCP.

use std::error::Error;
use std::net::SocketAddr;
use std::num::NonZeroU64;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

use fleetring_control::{Action, Command, ControlConfig, Member, Millis, State, Timestamp, apply};
use fleetring_proto::PROTOCOL_VERSION;
use fleetring_proto::v1::control_server::Control;
use fleetring_proto::v1::{
    Acks, AcquireLeaseRequest, ChangeDurabilityRequest, ChangeDurabilityResponse,
    CompletePromotionRequest, CompletePromotionResponse, CreateDatabaseRequest,
    CreateDatabaseResponse, DatabaseStatus, DrainRequest, DrainResponse, DropDatabaseRequest,
    DropDatabaseResponse, Engine, HeartbeatRequest, HeartbeatResponse, JoinRequest, JoinResponse,
    LeaseResponse, LeaveRequest, LeaveResponse, ListDatabasesRequest, ListDatabasesResponse,
    LocateRequest, LocateResponse, MembershipUpdate, Node, PromotionResponse, RecordFencedRequest,
    RecordFencedResponse, RenewLeaseRequest, ReplicaRole, RevokeTokenRequest, RevokeTokenResponse,
    StartPromotionRequest, TransferLeaseRequest, WatchMembershipRequest,
};
use fleetringd::auth::{AuthLayer, Authorizer, refused_binding};
use fleetringd::clock::{Clock, SystemClock};
use fleetringd::control::{
    CommandLog, Committed, ControlClient, ControlHandle, ControlNode, ControlOptions,
    ControlService, LEADER_METADATA_KEY, LogError, LogIndex, OpenError, StorageIo, leader_hint,
    not_leader,
};
use fleetringd::transport::{TransportConfig, control_server};
use fleetringd::{Config, ConfigError, Daemon, NodeRole};
use tokio_stream::Stream;

use crate::support::Tls;

mod support;
use tonic::{Code, Request, Response, Status};

type TestResult = Result<(), Box<dyn Error>>;

fn join_request(id: &str) -> JoinRequest {
    JoinRequest {
        protocol: Some(PROTOCOL_VERSION),
        node: Some(Node {
            id: id.into(),
            zone: None,
            host: None,
            weight: 1,
            address: None,
        }),
        software: None,
    }
}

/// A plaintext client, for the stub servers the tests serve themselves.
fn client(address: SocketAddr) -> ControlClient {
    ControlClient::new(address.to_string(), TransportConfig::default())
}

/// A client that reaches a daemon as the principal `tester`.
fn secure_client(address: SocketAddr, tls: &Tls) -> Result<ControlClient, Box<dyn Error>> {
    Ok(tls
        .client_network("tester")?
        .control_client(address.to_string()))
}

/// A client acting as the node `id`, with a certificate for it, as node-to-node calls need
/// (ADR-0025).
fn node_client(address: SocketAddr, tls: &Tls, id: &str) -> Result<ControlClient, Box<dyn Error>> {
    Ok(tls.node_network(id)?.control_client(address.to_string()))
}

/// Forwards TCP connections to `target` and holds every chunk back by `delay` in each
/// direction, so each round trip through it takes at least twice `delay`: a control plane whose
/// answers are slow, as a log sync on a loaded machine makes them, without a test sleeping in the
/// daemon. Returns the address to dial.
async fn delaying_proxy(
    target: SocketAddr,
    delay: std::time::Duration,
) -> Result<SocketAddr, Box<dyn Error>> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
    use tokio::time::Instant;

    /// Copies `from` to `to`, releasing each chunk `delay` after it arrived and in order.
    async fn pipe(mut from: OwnedReadHalf, mut to: OwnedWriteHalf, delay: std::time::Duration) {
        let (queue, mut chunks) = tokio::sync::mpsc::unbounded_channel::<(Instant, Vec<u8>)>();
        let writer = tokio::spawn(async move {
            while let Some((arrived, chunk)) = chunks.recv().await {
                tokio::time::sleep_until(arrived + delay).await;
                if to.write_all(&chunk).await.is_err() {
                    break;
                }
            }
        });
        let mut buffer = vec![0; 16 * 1024];
        while let Ok(read) = from.read(&mut buffer).await {
            if read == 0
                || queue
                    .send((Instant::now(), buffer[..read].to_vec()))
                    .is_err()
            {
                break;
            }
        }
        drop(queue);
        // The writer ends once it has released what was queued.
        if let Err(error) = writer.await {
            panic!("the proxy's writer failed: {error}");
        }
    }

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    tokio::spawn(async move {
        while let Ok((inbound, _)) = listener.accept().await {
            tokio::spawn(async move {
                let Ok(outbound) = tokio::net::TcpStream::connect(target).await else {
                    return;
                };
                let (inbound_read, inbound_write) = inbound.into_split();
                let (outbound_read, outbound_write) = outbound.into_split();
                tokio::join!(
                    pipe(inbound_read, outbound_write, delay),
                    pipe(outbound_read, inbound_write, delay),
                );
            });
        }
    });
    Ok(address)
}

/// Serves `node` on a random local port.
async fn serve(node: ControlHandle) -> Result<SocketAddr, Box<dyn Error>> {
    serve_service(ControlService::new(node)).await
}

/// Serves `service` on a random local port.
async fn serve_service(service: ControlService) -> Result<SocketAddr, Box<dyn Error>> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    tokio::spawn(
        tonic::transport::Server::builder()
            .layer(AuthLayer::new(Authorizer::plaintext()))
            .add_service(control_server(service))
            .serve_with_incoming(tonic::transport::server::TcpIncoming::from(listener)),
    );
    Ok(address)
}

/// Replays the whole log into a fresh state, returning it and each command's outcome.
fn replay(log: &CommandLog) -> Result<(State, Vec<fleetring_control::Outcome>), LogError> {
    let mut state = State::new(log.genesis());
    let outcomes = log
        .commands()?
        .into_iter()
        .map(|(_, command)| apply(&mut state, command))
        .collect();
    Ok((state, outcomes))
}

#[test]
fn the_control_table_configures_timings_and_snapshots() -> TestResult {
    let text = r#"
data_dir = "/tmp/fleetring-test"
listen = "127.0.0.1:7400"
role = "data"

[node]
id = "laptop-1"

[control]
address = "10.0.0.1:7400"
lease_duration_ms = 3000
failover_margin_ms = 0
heartbeat_timeout_ms = 1500
snapshot_every = 50

[membership]
heartbeat_interval_ms = 500
"#;
    let config = Config::from_toml(text)?;
    assert_eq!(config.membership.heartbeat_interval_ms.get(), 500);
    assert_eq!(config.control.address, Some("10.0.0.1:7400".parse()?));
    assert_eq!(config.control.snapshot_every.get(), 50);
    let timings = config.control.control_config();
    assert_eq!(timings.lease_duration(), Millis::new(3_000));
    assert_eq!(timings.failover_margin(), Millis::new(0));
    assert_eq!(timings.heartbeat_timeout(), Millis::new(1_500));

    let defaults = Config::from_toml(&text.replace("role = \"data\"\n", "").replace(
        "lease_duration_ms = 3000\nfailover_margin_ms = 0\nheartbeat_timeout_ms = 1500\nsnapshot_every = 50\n",
        "",
    ))?;
    assert_eq!(defaults.control.control_config(), ControlConfig::new());
    assert_eq!(defaults.role, NodeRole::Both);

    for (bad, field) in [
        (
            text.replace("lease_duration_ms = 3000", "lease_duration_ms = 0"),
            "lease_duration_ms",
        ),
        (
            text.replace("snapshot_every = 50", "snapshot_every = 0"),
            "snapshot_every",
        ),
        (
            text.replace("snapshot_every", "snapshot_evry"),
            "snapshot_evry",
        ),
    ] {
        let error = Config::from_toml(&bad).expect_err(field);
        assert!(matches!(error, ConfigError::Parse(_)), "{error}");
    }
    Ok(())
}

#[tokio::test]
async fn a_control_daemon_runs_the_lease_lifecycle_through_the_client() -> TestResult {
    let dir = tempfile::tempdir()?;
    let tls = Tls::create(dir.path())?;
    let text = format!(
        "data_dir = {:?}\nlisten = \"127.0.0.1:0\"\nrole = \"control\"\n\n[node]\nid = \"control-1\"\n{}",
        dir.path().join("data"),
        tls.node_table("control-1")?
    );
    let daemon = Daemon::start(Config::from_toml(&text)?).await?;
    let address = daemon.control_address().ok_or("no control address")?;
    let control = secure_client(address, &tls)?;

    for id in ["a", "b", "c"] {
        let joined = node_client(address, &tls, id)?
            .join(join_request(id))
            .await?;
        assert_eq!(joined.protocol, Some(PROTOCOL_VERSION));
    }
    let mut updates = control
        .watch_membership(WatchMembershipRequest { after_epoch: 0 })
        .await?;
    let update = updates.message().await?.ok_or("membership stream ended")?;
    assert_eq!(update.membership_epoch, 3);
    assert_eq!(update.members.len(), 3);
    assert!(update.members.iter().all(|member| member.live));

    let created = control
        .create_database(CreateDatabaseRequest {
            database: "tenant:acme".into(),
            replication_factor: 3,
            engine: Engine::Sqlite.into(),
            acks: Acks::Unspecified.into(),
            min_replicas: 0,
        })
        .await?;
    let placement = created.placement.ok_or("no placement")?;
    assert_eq!(placement.primary, None);
    assert_eq!(placement.followers.len(), 3);
    let primary = placement.followers[0].clone();
    let as_primary = node_client(address, &tls, &primary)?;

    let beat = as_primary
        .heartbeat(HeartbeatRequest {
            node_id: primary.clone(),
            databases: vec![DatabaseStatus {
                database: "tenant:acme".into(),
                role: ReplicaRole::Follower.into(),
                epoch: 0,
                position: vec![0, 0, 0, 1],
            }],
            software: None,
        })
        .await?;
    assert_eq!(beat.membership_epoch, 3);

    let lease = as_primary
        .acquire_lease(AcquireLeaseRequest {
            database: "tenant:acme".into(),
            node_id: primary.clone(),
        })
        .await?;
    assert_eq!(lease.epoch, 1);
    let renewed = as_primary
        .renew_lease(RenewLeaseRequest {
            database: "tenant:acme".into(),
            node_id: primary.clone(),
            epoch: 1,
        })
        .await?;
    assert!(renewed.expires_at_ms >= lease.expires_at_ms);

    let located = control
        .locate(LocateRequest {
            database: "tenant:acme".into(),
        })
        .await?
        .placement
        .ok_or("no placement")?;
    assert_eq!(located.primary.as_deref(), Some(primary.as_str()));
    assert_eq!(located.epoch, 1);
    assert_eq!(located.engine, i32::from(Engine::Sqlite));
    assert_eq!(located.followers.len(), 2);
    assert!(!located.followers.contains(&primary));

    // Rejections and bad input map to fitting statuses with lowercase messages.
    let again = control
        .create_database(CreateDatabaseRequest {
            database: "tenant:acme".into(),
            replication_factor: 3,
            engine: Engine::Sqlite.into(),
            acks: Acks::Unspecified.into(),
            min_replicas: 0,
        })
        .await
        .expect_err("duplicate database");
    assert_eq!(again.code(), Code::AlreadyExists, "{again}");
    assert_eq!(again.message(), "database 'tenant:acme' already exists");
    let missing = control
        .locate(LocateRequest {
            database: "tenant:none".into(),
        })
        .await
        .expect_err("unknown database");
    assert_eq!(missing.code(), Code::NotFound, "{missing}");
    let stale = as_primary
        .renew_lease(RenewLeaseRequest {
            database: "tenant:acme".into(),
            node_id: primary.clone(),
            epoch: 7,
        })
        .await
        .expect_err("stale epoch");
    assert_eq!(stale.code(), Code::FailedPrecondition, "{stale}");
    let invalid = as_primary
        .join(join_request("Not A Label"))
        .await
        .expect_err("invalid id");
    assert_eq!(invalid.code(), Code::InvalidArgument, "{invalid}");
    let large = control
        .create_database(CreateDatabaseRequest {
            database: "tenant:large".into(),
            replication_factor: 256,
            engine: Engine::Unspecified.into(),
            acks: Acks::Unspecified.into(),
            min_replicas: 0,
        })
        .await
        .expect_err("more than 255 replicas");
    assert_eq!(large.code(), Code::InvalidArgument, "{large}");
    assert_eq!(
        large.message(),
        "invalid replication factor 256: expected 1 to 255, or 0 for the fleet's default"
    );

    let before = daemon.control().ok_or("no control handle")?.state();
    daemon.run_until(async {}).await;
    // The membership stream ends when the control node stops.
    assert!(
        updates
            .message()
            .await
            .map_or(true, |update| update.is_none())
    );

    // A restarted daemon recovers the same state from its log.
    let daemon = Daemon::start(Config::from_toml(&text)?).await?;
    assert_eq!(
        *daemon.control().ok_or("no control handle")?.state(),
        *before
    );
    let located = secure_client(daemon.control_address().ok_or("no control address")?, &tls)?
        .locate(LocateRequest {
            database: "tenant:acme".into(),
        })
        .await?;
    assert_eq!(
        located.placement.and_then(|placement| placement.primary),
        Some(primary)
    );
    daemon.run_until(async {}).await;
    Ok(())
}

/// Why one attempt to drive a promotion to its end stopped.
#[derive(Debug)]
enum Attempt {
    /// The control node started a promotion at a newer epoch meanwhile, so this one's calls are
    /// refused as stale.
    Superseded(Status),
    /// Anything else, which is a failure.
    Failed(Status),
}

impl From<Status> for Attempt {
    fn from(status: Status) -> Self {
        if status.code() == Code::FailedPrecondition && status.message().contains("is stale") {
            Self::Superseded(status)
        } else {
            Self::Failed(status)
        }
    }
}

/// Drives the promotion of `database` at `epoch` to its end through candidate `b`, which also
/// reports the fences of `c`, checking each step's answer along the way.
async fn promote(
    b: &ControlClient,
    database: &str,
    epoch: u64,
) -> Result<CompletePromotionResponse, Attempt> {
    // A promotion in progress refuses another start. One that stalled is replaced by it, which
    // is the control node's supersession seen from here.
    match b
        .start_promotion(StartPromotionRequest {
            database: database.to_owned(),
        })
        .await
    {
        Err(busy) if busy.code() == Code::FailedPrecondition => {}
        Err(other) => return Err(other.into()),
        Ok(_) => {
            return Err(Attempt::Superseded(Status::failed_precondition(
                "a promotion that had stalled was replaced",
            )));
        }
    }
    let fence = |node: &str, position: u8| RecordFencedRequest {
        database: database.to_owned(),
        node_id: node.into(),
        epoch,
        position: vec![0, position],
    };
    let complete = || CompletePromotionRequest {
        database: database.to_owned(),
        epoch,
    };
    let first = b.record_fenced(fence("b", 2)).await?;
    assert_eq!((first.fenced, first.needed), (1, 2));
    let early = b
        .complete_promotion(complete())
        .await
        .expect_err("one fenced member is too few");
    if early.code() != Code::FailedPrecondition {
        return Err(Attempt::Failed(early));
    }
    if early.message().contains("is stale") {
        return Err(Attempt::Superseded(early));
    }
    let second = b.record_fenced(fence("c", 1)).await?;
    assert_eq!((second.fenced, second.needed), (2, 2));
    let twice = b
        .record_fenced(fence("c", 1))
        .await
        .expect_err("fenced already");
    if twice.code() != Code::AlreadyExists {
        return Err(Attempt::from(twice));
    }
    Ok(b.complete_promotion(complete()).await?)
}

#[tokio::test]
async fn the_control_node_starts_a_promotion_for_an_expired_lease_and_a_node_leaves() -> TestResult
{
    let dir = tempfile::tempdir()?;
    let tls = Tls::create(dir.path())?;
    let text = format!(
        "data_dir = {:?}\nlisten = \"127.0.0.1:0\"\nrole = \"control\"\n\n[node]\nid = \"control-1\"\n\n\
         [control]\nlease_duration_ms = 2000\nfailover_margin_ms = 0\ntick_interval_ms = 20\n{}",
        dir.path().join("data"),
        tls.node_table("control-1")?
    );
    let daemon = Daemon::start(Config::from_toml(&text)?).await?;
    let address = daemon.control_address().ok_or("no control address")?;
    let control = secure_client(address, &tls)?;
    let (a, b, c) = (
        node_client(address, &tls, "a")?,
        node_client(address, &tls, "b")?,
        node_client(address, &tls, "c")?,
    );
    for (id, node) in [("a", &a), ("b", &b), ("c", &c)] {
        node.join(join_request(id)).await?;
    }
    let database = "tenant:acme".to_owned();
    control
        .create_database(CreateDatabaseRequest {
            database: database.clone(),
            replication_factor: 3,
            engine: Engine::Sqlite.into(),
            acks: Acks::Unspecified.into(),
            min_replicas: 0,
        })
        .await?;
    a.acquire_lease(AcquireLeaseRequest {
        database: database.clone(),
        node_id: "a".into(),
    })
    .await?;
    // "a" stops renewing; "b" holds the longer log, from epoch 1's primary. The lease is long
    // enough that the timer doesn't supersede the promotion while the test drives it under load.
    let report = |node: &str, position: u8| HeartbeatRequest {
        node_id: node.into(),
        databases: vec![DatabaseStatus {
            database: database.clone(),
            role: ReplicaRole::Follower.into(),
            epoch: 1,
            position: vec![0, position],
        }],
        software: None,
    };
    let locate = || async {
        control
            .locate(LocateRequest {
                database: database.clone(),
            })
            .await?
            .placement
            .ok_or_else(|| Status::internal("no placement"))
    };
    // The timer notices the expiry on its own and starts a promotion: no primary until it
    // completes. The control node supersedes a promotion that stalls for a lease duration, with a
    // new one at the next epoch, which a loaded runner can cause between two of the calls below
    // (the daemon shares this test's one thread, and every command is fsynced). So the steps run
    // as one attempt at the epoch now in force, and start over if that promotion was superseded.
    let (epoch, completed) = tokio::time::timeout(std::time::Duration::from_secs(60), async {
        loop {
            b.heartbeat(report("b", 2)).await?;
            c.heartbeat(report("c", 1)).await?;
            let started = locate().await?;
            if started.epoch < 2 {
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                continue;
            }
            assert_eq!(started.primary, None, "no primary while the promotion runs");
            let epoch = started.epoch;
            match promote(&b, &database, epoch).await {
                Ok(completed) => return Ok::<_, Status>((epoch, completed)),
                Err(Attempt::Superseded(status)) => {
                    eprintln!("the promotion at epoch {epoch} was superseded: {status}");
                }
                Err(Attempt::Failed(status)) => return Err(status),
            }
        }
    })
    .await
    .map_err(|_| "no promotion completed")??;
    assert_eq!(completed.lease.map(|lease| lease.epoch), Some(epoch));
    assert_eq!(completed.adopted, vec![0, 2]);
    let promoted = locate().await?;
    assert_eq!(promoted.primary.as_deref(), Some("b"));
    assert_eq!(promoted.epoch, epoch);

    let left = control
        .leave(LeaveRequest {
            node_id: "a".into(),
        })
        .await?;
    assert_eq!(left.membership_epoch, 4);
    let gone = control
        .leave(LeaveRequest {
            node_id: "a".into(),
        })
        .await
        .expect_err("left already");
    assert_eq!(gone.code(), Code::NotFound, "{gone}");
    daemon.run_until(async {}).await;
    Ok(())
}

type BoxStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send>>;

/// A control node that isn't the leader: it answers every call with a redirect.
#[derive(Debug)]
struct Redirecting {
    leader: String,
    calls: Arc<AtomicU32>,
}

impl Redirecting {
    fn redirect<T>(&self) -> Result<Response<T>, Status> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Err(not_leader(&self.leader))
    }
}

#[tonic::async_trait]
impl Control for Redirecting {
    type WatchMembershipStream = BoxStream<MembershipUpdate>;

    async fn join(&self, _: Request<JoinRequest>) -> Result<Response<JoinResponse>, Status> {
        self.redirect()
    }

    async fn heartbeat(
        &self,
        _: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatResponse>, Status> {
        self.redirect()
    }

    async fn watch_membership(
        &self,
        _: Request<WatchMembershipRequest>,
    ) -> Result<Response<Self::WatchMembershipStream>, Status> {
        self.redirect()
    }

    async fn create_database(
        &self,
        _: Request<CreateDatabaseRequest>,
    ) -> Result<Response<CreateDatabaseResponse>, Status> {
        self.redirect()
    }

    async fn create_database_with_metadata(
        &self,
        _: Request<fleetring_proto::v1::CreateDatabaseWithMetadataRequest>,
    ) -> Result<Response<CreateDatabaseResponse>, Status> {
        self.redirect()
    }

    async fn update_database(
        &self,
        _: Request<fleetring_proto::v1::UpdateDatabaseRequest>,
    ) -> Result<Response<fleetring_proto::v1::UpdateDatabaseResponse>, Status> {
        self.redirect()
    }

    async fn drop_database(
        &self,
        _: Request<DropDatabaseRequest>,
    ) -> Result<Response<DropDatabaseResponse>, Status> {
        self.redirect()
    }

    async fn change_durability(
        &self,
        _: Request<ChangeDurabilityRequest>,
    ) -> Result<Response<ChangeDurabilityResponse>, Status> {
        self.redirect()
    }

    async fn locate(&self, _: Request<LocateRequest>) -> Result<Response<LocateResponse>, Status> {
        self.redirect()
    }

    async fn list_databases(
        &self,
        _: Request<ListDatabasesRequest>,
    ) -> Result<Response<ListDatabasesResponse>, Status> {
        self.redirect()
    }

    async fn acquire_lease(
        &self,
        _: Request<AcquireLeaseRequest>,
    ) -> Result<Response<LeaseResponse>, Status> {
        self.redirect()
    }

    async fn renew_lease(
        &self,
        _: Request<RenewLeaseRequest>,
    ) -> Result<Response<LeaseResponse>, Status> {
        self.redirect()
    }

    async fn leave(&self, _: Request<LeaveRequest>) -> Result<Response<LeaveResponse>, Status> {
        self.redirect()
    }

    async fn start_promotion(
        &self,
        _: Request<StartPromotionRequest>,
    ) -> Result<Response<PromotionResponse>, Status> {
        self.redirect()
    }

    async fn record_fenced(
        &self,
        _: Request<RecordFencedRequest>,
    ) -> Result<Response<RecordFencedResponse>, Status> {
        self.redirect()
    }

    async fn complete_promotion(
        &self,
        _: Request<CompletePromotionRequest>,
    ) -> Result<Response<CompletePromotionResponse>, Status> {
        self.redirect()
    }

    async fn drain(&self, _: Request<DrainRequest>) -> Result<Response<DrainResponse>, Status> {
        self.redirect()
    }

    async fn undrain(
        &self,
        _: Request<fleetring_proto::v1::UndrainRequest>,
    ) -> Result<Response<fleetring_proto::v1::UndrainResponse>, Status> {
        self.redirect()
    }

    async fn transfer_lease(
        &self,
        _: Request<TransferLeaseRequest>,
    ) -> Result<Response<PromotionResponse>, Status> {
        self.redirect()
    }

    async fn revoke_token(
        &self,
        _: Request<RevokeTokenRequest>,
    ) -> Result<Response<RevokeTokenResponse>, Status> {
        self.redirect()
    }
}

async fn serve_redirect(
    leader: SocketAddr,
) -> Result<(SocketAddr, Arc<AtomicU32>), Box<dyn Error>> {
    let calls = Arc::new(AtomicU32::new(0));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(control_server(Redirecting {
                leader: leader.to_string(),
                calls: Arc::clone(&calls),
            }))
            .serve_with_incoming(tonic::transport::server::TcpIncoming::from(listener)),
    );
    Ok((address, calls))
}

#[tokio::test]
async fn the_client_follows_leader_redirects_without_the_caller_noticing() -> TestResult {
    let dir = tempfile::tempdir()?;
    let node = ControlNode::open(
        dir.path().join("control.db"),
        ControlOptions::default(),
        SystemClock,
    )
    .await?;
    let leader = serve(node.handle()).await?;
    // Two stale nodes: the first points at the second, which points at the leader.
    let (second, second_calls) = serve_redirect(leader).await?;
    let (first, first_calls) = serve_redirect(second).await?;

    let status = not_leader(&leader.to_string());
    assert_eq!(status.code(), Code::Unavailable);
    let named = status
        .metadata()
        .get(LEADER_METADATA_KEY)
        .ok_or("no leader")?;
    assert_eq!(named.to_str()?, leader.to_string());
    assert_eq!(leader_hint(&status), Some(leader.to_string()));

    let control = client(first);
    let joined = control.join(join_request("a")).await?;
    assert_eq!(joined.membership_epoch, 1);
    assert_eq!(control.leader(), leader.to_string());
    // A call that is never retried follows redirects too, and later calls go straight to the
    // leader.
    control
        .create_database(CreateDatabaseRequest {
            database: "tenant:acme".into(),
            replication_factor: 1,
            engine: Engine::Sqlite.into(),
            acks: Acks::Unspecified.into(),
            min_replicas: 0,
        })
        .await?;
    assert_eq!(first_calls.load(Ordering::SeqCst), 1);
    assert_eq!(second_calls.load(Ordering::SeqCst), 1);

    let fresh = client(first);
    fresh
        .acquire_lease(AcquireLeaseRequest {
            database: "tenant:acme".into(),
            node_id: "a".into(),
        })
        .await?;
    assert_eq!(fresh.leader(), leader.to_string());
    assert_eq!(first_calls.load(Ordering::SeqCst), 2);
    node.shutdown().await;
    Ok(())
}

/// A creation request for `database` with `replication_factor` replicas, 0 for the fleet's
/// default, and every other setting the fleet's default.
fn create_request(database: &str, replication_factor: u32) -> CreateDatabaseRequest {
    CreateDatabaseRequest {
        database: database.into(),
        replication_factor,
        engine: Engine::Sqlite.into(),
        acks: Acks::Unspecified.into(),
        min_replicas: 0,
    }
}

/// The replication factor of every database creation in the log at `path`, by name, in log
/// order.
fn logged_factors(path: &Path) -> Result<Vec<(String, u8)>, Box<dyn Error>> {
    let log = CommandLog::open(path, ControlConfig::new())?;
    Ok(log
        .commands()?
        .into_iter()
        .filter_map(|(_, command)| match command.action {
            Action::CreateDatabase {
                name,
                replication_factor,
                ..
            } => Some((name.to_string(), replication_factor.get())),
            _ => None,
        })
        .collect())
}

/// A creation that names no replication factor gets the control node's default, filled in
/// before the command is logged: the log holds the factor itself, so it replays to the same
/// state whatever the default is later, on any release. An explicit factor wins, and one larger
/// than the fleet is accepted, since nodes may join later (ADR-0039).
#[tokio::test]
async fn a_creation_without_a_replication_factor_logs_the_fleets_default() -> TestResult {
    let dir = tempfile::tempdir()?;
    let cases = [
        // A three-node fleet with the built-in default, 3.
        (None, &["a", "b", "c"][..], 3),
        // A dev node: one member, and a default of 1.
        (Some(fleetring::ReplicationFactor::ONE), &["dev"][..], 1),
    ];
    for (index, (default, members, expected)) in cases.into_iter().enumerate() {
        let path = dir.path().join(format!("control-{index}.db"));
        let node =
            ControlNode::open(&path, ControlOptions::default(), TickClock::default()).await?;
        let mut service = ControlService::new(node.handle());
        if let Some(default) = default {
            service = service.with_default_replication_factor(default);
        }
        let control = client(serve_service(service).await?);
        for id in members {
            control.join(join_request(id)).await?;
        }

        let created = control.create_database(create_request("notes", 0)).await?;
        let placement = created.placement.ok_or("no placement")?;
        assert_eq!(placement.replication_factor, expected);
        assert_eq!(placement.replicas.len(), members.len());
        let pinned = control.create_database(create_request("pinned", 2)).await?;
        assert_eq!(
            pinned.placement.ok_or("no placement")?.replication_factor,
            2
        );
        // Larger than the fleet: accepted, with as many replicas as there are members.
        let wide = control.create_database(create_request("wide", 5)).await?;
        let wide = wide.placement.ok_or("no placement")?;
        assert_eq!(wide.replication_factor, 5);
        assert_eq!(wide.replicas.len(), members.len());

        let published = node.handle().state();
        node.shutdown().await;
        assert_eq!(
            logged_factors(&path)?,
            [
                ("notes".to_owned(), u8::try_from(expected)?),
                ("pinned".to_owned(), 2),
                ("wide".to_owned(), 5),
            ],
            "default {default:?}"
        );
        let (replayed, _) = replay(&CommandLog::open(&path, ControlConfig::new())?)?;
        assert_eq!(replayed, *published);
    }
    Ok(())
}

/// The `[replication] replication_factor` of a control node's configuration is the default it
/// creates databases with.
#[tokio::test]
async fn the_replication_table_sets_the_control_nodes_default_replication_factor() -> TestResult {
    let dir = tempfile::tempdir()?;
    let tls = Tls::create(dir.path())?;
    let text = format!(
        "data_dir = {:?}\nlisten = \"127.0.0.1:0\"\nrole = \"control\"\n\n[replication]\n\
         replication_factor = 2\n\n[node]\nid = \"control-1\"\n{}",
        dir.path().join("data"),
        tls.node_table("control-1")?
    );
    let daemon = Daemon::start(Config::from_toml(&text)?).await?;
    let address = daemon.control_address().ok_or("no control address")?;
    for id in ["a", "b"] {
        node_client(address, &tls, id)?
            .join(join_request(id))
            .await?;
    }
    let created = secure_client(address, &tls)?
        .create_database(create_request("notes", 0))
        .await?;
    assert_eq!(
        created.placement.ok_or("no placement")?.replication_factor,
        2
    );
    let notes: fleetring_control::DatabaseName = "notes".parse()?;
    let state = daemon.control().ok_or("no control handle")?.state();
    let database = state.database(&notes).ok_or("no database")?;
    assert_eq!(database.replication_factor().get(), 2);
    daemon.run_until(std::future::ready(())).await;
    Ok(())
}

/// A clock that moves forward one millisecond every time it is read.
#[derive(Debug, Default)]
struct TickClock(AtomicU64);

impl Clock for TickClock {
    fn now(&self) -> Timestamp {
        Timestamp::from_millis(self.0.fetch_add(1, Ordering::SeqCst) + 1)
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_commands_apply_one_at_a_time_in_log_order() -> TestResult {
    const CALLERS: u64 = 8;
    const COMMANDS: u64 = 20;
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("control.db");
    let options = ControlOptions::default().with_snapshot_every(NonZeroU64::new(16).ok_or("zero")?);
    let node = ControlNode::open(&path, options, TickClock::default()).await?;

    let mut callers = Vec::new();
    for caller in 0..CALLERS {
        let handle = node.handle();
        callers.push(tokio::spawn(async move {
            let id: fleetring::NodeId = format!("node-{caller}").parse()?;
            let mut committed = vec![
                handle
                    .submit(Action::Join {
                        member: Member::new(id.clone()),
                    })
                    .await?,
            ];
            for _ in 1..COMMANDS {
                let action = Action::Heartbeat {
                    id: id.clone(),
                    replicas: Vec::new(),
                    unknown_reports: fleetring_control::UnknownReports::Skip,
                    software: None,
                };
                committed.push(handle.submit(action).await?);
            }
            Ok::<_, Box<dyn Error + Send + Sync>>(committed)
        }));
    }
    let mut committed: Vec<Committed> = Vec::new();
    for caller in callers {
        committed.extend(caller.await?.map_err(|error| error.to_string())?);
    }
    let published = node.handle().state();
    node.shutdown().await;

    // Every command got its own index, and was stamped in the order it was logged.
    committed.sort_by_key(Committed::index);
    let total = CALLERS * COMMANDS;
    assert_eq!(committed.len(), usize::try_from(total)?);
    for (expected, record) in (1..=total).zip(&committed) {
        assert_eq!(record.index().get(), expected);
        assert_eq!(record.command().at.as_millis(), expected, "{record:?}");
        assert_eq!(record.state().now().as_millis(), expected, "{record:?}");
    }

    // The log holds the same commands in the same order, and replaying it reproduces each
    // outcome and the final state.
    let log = CommandLog::open(&path, ControlConfig::new())?;
    let logged = log.commands()?;
    assert_eq!(logged.len(), committed.len());
    let (replayed, outcomes) = replay(&log)?;
    for ((record, (index, command)), outcome) in committed.iter().zip(&logged).zip(&outcomes) {
        assert_eq!(record.index(), *index);
        assert_eq!(record.command(), command);
        assert_eq!(record.outcome(), outcome);
    }
    assert_eq!(replayed, *published);
    let recovered = log.recover()?;
    assert_eq!(recovered.state, replayed);
    assert_eq!(recovered.snapshot.map(|index| index.get()), Some(160));
    assert_eq!(recovered.replayed, 0);
    Ok(())
}

/// How many transactions the SQLite log at `path` has committed to its write-ahead log since
/// it was created: one commit frame ends each.
fn commits(path: &Path) -> Result<usize, Box<dyn Error>> {
    let mut wal = path.as_os_str().to_owned();
    wal.push("-wal");
    Ok(
        fleetring_wal::read_committed_file(wal, None)?.map_or(0, |batch| {
            batch
                .frame_headers()
                .filter(|frame| frame.is_commit())
                .count()
        }),
    )
}

/// Every command that waits for the writer when it picks the next one up is logged with it in
/// one transaction, so a burst costs one sync, not one per command. The commands still get
/// their own indexes and stamps, in order.
///
/// The writer runs inline on this single-threaded runtime, so it can't pick anything up until
/// every submission below has been queued.
#[tokio::test]
async fn commands_queued_together_are_logged_in_one_transaction() -> TestResult {
    const QUEUED: u64 = 40;
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("control.db");
    let options = ControlOptions::default().with_storage_io(StorageIo::Inline);
    let node = ControlNode::open(&path, options, TickClock::default()).await?;
    let before = commits(&path)?;

    let mut submits = tokio::task::JoinSet::new();
    for _ in 0..QUEUED {
        let handle = node.handle();
        submits.spawn(async move { handle.submit(Action::Tick).await });
    }
    let mut committed = Vec::new();
    while let Some(joined) = submits.join_next().await {
        committed.push(joined??);
    }
    committed.sort_by_key(Committed::index);
    let indexes: Vec<u64> = committed
        .iter()
        .map(|record| record.index().get())
        .collect();
    assert_eq!(indexes, (1..=QUEUED).collect::<Vec<_>>());
    let stamps: Vec<Timestamp> = committed.iter().map(|record| record.command().at).collect();
    assert!(
        stamps.windows(2).all(|pair| pair[0] < pair[1]),
        "{stamps:?}"
    );
    assert_eq!(commits(&path)? - before, 1, "one transaction for the group");
    node.shutdown().await;

    let log = CommandLog::open(&path, ControlConfig::new())?;
    let logged = log.commands()?;
    assert_eq!(logged.len(), usize::try_from(QUEUED)?);
    for (record, (index, command)) in committed.iter().zip(&logged) {
        assert_eq!(record.index(), *index);
        assert_eq!(record.command(), command);
    }
    Ok(())
}

/// A group is logged whole or not at all: an index already taken fails the group, and the
/// commands before it in the group aren't logged either.
#[test]
fn a_group_with_a_taken_index_logs_nothing() -> TestResult {
    let dir = tempfile::tempdir()?;
    let mut log = CommandLog::open(dir.path().join("control.db"), ControlConfig::new())?;
    let tick = |at: u64| Command::new(Timestamp::from_millis(at), Action::Tick);
    log.append(LogIndex::new(1), &tick(1))?;
    let group = [tick(2), tick(3)];
    let refused = log
        .append_all([(LogIndex::new(2), &group[0]), (LogIndex::new(1), &group[1])])
        .expect_err("index 1 is taken");
    assert!(matches!(refused, LogError::Sqlite { .. }), "{refused}");
    let logged = log.commands()?;
    assert_eq!(logged, vec![(LogIndex::new(1), tick(1))]);
    log.append_all([(LogIndex::new(2), &group[0]), (LogIndex::new(3), &group[1])])?;
    assert_eq!(log.commands()?.len(), 3);
    Ok(())
}

/// Runs a few commands with frequent snapshots and returns the log's path.
async fn log_with_snapshot(dir: &Path) -> Result<std::path::PathBuf, Box<dyn Error>> {
    let path = dir.join("control.db");
    let options = ControlOptions::default().with_snapshot_every(NonZeroU64::new(3).ok_or("zero")?);
    let node = ControlNode::open(&path, options, TickClock::default()).await?;
    let handle = node.handle();
    for id in ["a", "b", "c", "d"] {
        let member = Member::new(id.parse()?);
        handle.submit(Action::Join { member }).await?;
    }
    node.shutdown().await;
    Ok(path)
}

fn tamper(path: &Path, sql: &str) -> rusqlite::Result<()> {
    let connection = rusqlite::Connection::open(path)?;
    connection.execute_batch(sql)?;
    Ok(())
}

fn tamper_snapshot(
    path: &Path,
    change: impl FnOnce(&mut serde_json::Value),
) -> Result<(), Box<dyn Error>> {
    let connection = rusqlite::Connection::open(path)?;
    let text: String = connection.query_row("SELECT state FROM snapshots", [], |row| row.get(0))?;
    let mut state: serde_json::Value = serde_json::from_str(&text)?;
    change(&mut state);
    connection.execute(
        "UPDATE snapshots SET state = ?1",
        [serde_json::to_string(&state)?],
    )?;
    Ok(())
}

type SnapshotChange = Box<dyn Fn(&mut serde_json::Value)>;

#[tokio::test]
async fn a_corrupt_snapshot_is_an_error() -> TestResult {
    let cases: [(&str, SnapshotChange); 4] = [
        (
            "",
            Box::new(|state| *state = serde_json::Value::String("garbage".into())),
        ),
        (
            "membership epoch 0",
            Box::new(|state| state["membership_epoch"] = 0.into()),
        ),
        (
            "genesis configuration",
            Box::new(|state| state["config"]["lease_duration"] = 1.into()),
        ),
        (
            "heartbeat after the state's time",
            Box::new(|state| state["now"] = 0.into()),
        ),
    ];
    for (reason, change) in cases {
        let dir = tempfile::tempdir()?;
        let path = log_with_snapshot(dir.path()).await?;
        CommandLog::open(&path, ControlConfig::new())?.recover()?;
        tamper_snapshot(&path, change)?;
        let error = CommandLog::open(&path, ControlConfig::new())?
            .recover()
            .expect_err(reason);
        assert!(
            matches!(&error, LogError::CorruptSnapshot { index, .. } if index.get() == 3),
            "{error}"
        );
        assert!(error.to_string().contains(reason), "{reason}: {error}");
        let refused = ControlNode::open(&path, ControlOptions::default(), SystemClock)
            .await
            .expect_err("a node must not start from a corrupt snapshot");
        assert!(
            matches!(refused, OpenError::Log(LogError::CorruptSnapshot { .. })),
            "{refused}"
        );
    }
    Ok(())
}

#[tokio::test]
async fn a_damaged_log_is_an_error() -> TestResult {
    let dir = tempfile::tempdir()?;
    let path = log_with_snapshot(dir.path()).await?;
    tamper(
        &path,
        "DELETE FROM snapshots; DELETE FROM commands WHERE log_index = 2;",
    )?;
    let error = CommandLog::open(&path, ControlConfig::new())?
        .recover()
        .expect_err("gap");
    assert!(matches!(error, LogError::Gap { .. }), "{error}");

    let dir = tempfile::tempdir()?;
    let path = log_with_snapshot(dir.path()).await?;
    tamper(
        &path,
        "UPDATE commands SET command = '{' WHERE log_index = 4;",
    )?;
    let error = CommandLog::open(&path, ControlConfig::new())?
        .recover()
        .expect_err("corrupt command");
    assert!(matches!(error, LogError::CorruptCommand { .. }), "{error}");

    let dir = tempfile::tempdir()?;
    let path = log_with_snapshot(dir.path()).await?;
    tamper(&path, "DELETE FROM commands WHERE log_index >= 3;")?;
    let error = CommandLog::open(&path, ControlConfig::new())?
        .recover()
        .expect_err("snapshot ahead of the log");
    assert!(matches!(error, LogError::CorruptSnapshot { .. }), "{error}");
    Ok(())
}

#[tokio::test]
async fn an_existing_log_keeps_its_genesis_timings() -> TestResult {
    let dir = tempfile::tempdir()?;
    let path = log_with_snapshot(dir.path()).await?;
    let other = ControlConfig::new().with_lease_duration(Millis::new(1));
    let node = ControlNode::open(&path, ControlOptions::new(other), SystemClock).await?;
    assert_eq!(*node.handle().state().config(), ControlConfig::new());
    assert_eq!(node.recovered().snapshot.map(|index| index.get()), Some(3));
    assert_eq!(node.recovered().replayed, 1);
    node.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn durability_settings_travel_through_the_control_service() -> TestResult {
    let dir = tempfile::tempdir()?;
    let tls = Tls::create(dir.path())?;
    let text = format!(
        "data_dir = {:?}\nlisten = \"127.0.0.1:0\"\nrole = \"control\"\n\n[node]\nid = \"control-1\"\n\n\
         [replication]\nacks = \"local\"\n{}",
        dir.path().join("data"),
        tls.node_table("control-1")?
    );
    let config = Config::from_toml(&text)?;
    assert_eq!(config.replication.acks, fleetring_control::Acks::Local);
    assert_eq!(config.replication.min_replicas, None);
    let daemon = Daemon::start(config).await?;
    let address = daemon.control_address().ok_or("no control address")?;
    let control = secure_client(address, &tls)?;
    for id in ["a", "b", "c"] {
        node_client(address, &tls, id)?
            .join(join_request(id))
            .await?;
    }
    let create = |database: &str, acks: Acks, min_replicas: u32| CreateDatabaseRequest {
        database: database.into(),
        replication_factor: 3,
        engine: Engine::Sqlite.into(),
        acks: acks.into(),
        min_replicas,
    };
    // Unspecified settings take the control node's defaults; explicit ones are kept.
    let placement = control
        .create_database(create("defaulted", Acks::Unspecified, 0))
        .await?
        .placement
        .ok_or("no placement")?;
    assert_eq!(
        (placement.acks, placement.min_replicas),
        (i32::from(Acks::Local), 1)
    );
    let placement = control
        .create_database(create("strict", Acks::Quorum, 3))
        .await?
        .placement
        .ok_or("no placement")?;
    assert_eq!(
        (placement.acks, placement.min_replicas),
        (i32::from(Acks::Quorum), 3)
    );
    let refused = control
        .create_database(create("bad", Acks::All, 2))
        .await
        .expect_err("a floor below W");
    assert_eq!(refused.code(), Code::InvalidArgument, "{refused}");
    assert_eq!(
        refused.message(),
        "min_replicas 2 is below the write quorum 3"
    );
    let located = control
        .locate(LocateRequest {
            database: "strict".into(),
        })
        .await?
        .placement
        .ok_or("no placement")?;
    assert_eq!(
        (located.acks, located.min_replicas),
        (i32::from(Acks::Quorum), 3)
    );

    // A change is recorded and answered with what is in effect until the next lease.
    let changed = control
        .change_durability(ChangeDurabilityRequest {
            database: "strict".into(),
            acks: Acks::All.into(),
            min_replicas: 0,
        })
        .await?;
    assert_eq!(
        (changed.acks, changed.min_replicas),
        (i32::from(Acks::All), 3)
    );
    assert_eq!(
        (changed.current_acks, changed.current_min_replicas),
        (i32::from(Acks::Quorum), 3)
    );
    let unknown = control
        .change_durability(ChangeDurabilityRequest {
            database: "nope".into(),
            acks: Acks::All.into(),
            min_replicas: 0,
        })
        .await
        .expect_err("unknown database");
    assert_eq!(unknown.code(), Code::NotFound, "{unknown}");
    let state = daemon.control().ok_or("no control handle")?.state();
    let strict: fleetring_control::DatabaseName = "strict".parse()?;
    let database = state.database(&strict).ok_or("strict exists")?;
    assert_eq!(
        database.durability().acks(),
        fleetring_control::Acks::Quorum
    );
    assert_eq!(
        database.pending_durability().map(|d| d.acks()),
        Some(fleetring_control::Acks::All)
    );
    daemon.run_until(std::future::ready(())).await;
    Ok(())
}

#[tokio::test]
async fn a_heartbeat_naming_an_unknown_database_still_counts() -> TestResult {
    let dir = tempfile::tempdir()?;
    let tls = Tls::create(dir.path())?;
    let text = format!(
        "data_dir = {:?}\nlisten = \"127.0.0.1:0\"\nrole = \"control\"\n\n[node]\nid = \"control-1\"\n{}",
        dir.path().join("data"),
        tls.node_table("control-1")?
    );
    let daemon = Daemon::start(Config::from_toml(&text)?).await?;
    let address = daemon.control_address().ok_or("no control address")?;
    let control = secure_client(address, &tls)?;
    let a = node_client(address, &tls, "a")?;
    a.join(join_request("a")).await?;
    control
        .create_database(CreateDatabaseRequest {
            database: "known".into(),
            replication_factor: 1,
            engine: Engine::Sqlite.into(),
            acks: Acks::Unspecified.into(),
            min_replicas: 0,
        })
        .await?;
    let status = |database: &str| DatabaseStatus {
        database: database.into(),
        role: ReplicaRole::Primary.into(),
        epoch: 1,
        position: vec![0, 7],
    };
    // The known report lands, the unknown one is named, and the node stays live.
    let beat = a
        .heartbeat(HeartbeatRequest {
            node_id: "a".into(),
            databases: vec![status("ghost"), status("known")],
            software: None,
        })
        .await?;
    assert_eq!(beat.unknown_databases, vec!["ghost".to_owned()]);
    let state = daemon.control().ok_or("no control handle")?.state();
    let node: fleetring::NodeId = "a".parse()?;
    let known: fleetring_control::DatabaseName = "known".parse()?;
    let ghost: fleetring_control::DatabaseName = "ghost".parse()?;
    assert!(state.report(&node, &known).is_some());
    assert!(state.report(&node, &ghost).is_none());
    assert_eq!(state.last_heartbeat(&node), Some(state.now()));
    daemon.run_until(std::future::ready(())).await;
    Ok(())
}

#[tokio::test]
async fn a_revocation_reaches_data_nodes_through_their_heartbeats() -> TestResult {
    use fleetring_proto::v1::{Revocation, RevokeTokenRequest, revocation::Revoked as Wire};
    use fleetringd::{Membership, MembershipSettings};

    let dir = tempfile::tempdir()?;
    let tls = Tls::create(dir.path())?;
    let text = format!(
        "data_dir = {:?}\nlisten = \"127.0.0.1:0\"\nrole = \"control\"\n\n[node]\nid = \"control-1\"\n{}",
        dir.path().join("data"),
        tls.node_table("control-1")?
    );
    let daemon = Daemon::start(Config::from_toml(&text)?).await?;
    let address = daemon.control_address().ok_or("no control address")?;
    let control = secure_client(address, &tls)?;
    let revoke = |revoked: Wire, until_ms: u64| RevokeTokenRequest {
        revocation: Some(Revocation {
            revoked: Some(revoked),
            until_ms,
        }),
    };
    let far = u64::MAX;
    // A token's entry is kept no longer than any token lasts; a key's as long as asked.
    let kept = control
        .revoke_token(revoke(Wire::TokenId("tok-1".into()), far))
        .await?;
    let longest = SystemClock.now().as_millis()
        + u64::try_from(fleetringd::auth::MAX_TOKEN_LIFETIME.as_millis())?
        + fleetringd::auth::NOT_BEFORE_LEEWAY_SECS * 1_000;
    assert!(kept.until_ms <= longest, "{} > {longest}", kept.until_ms);
    assert!(kept.until_ms + 60_000 > longest, "{}", kept.until_ms);
    let kept = control
        .revoke_token(revoke(Wire::KeyId("key-1".into()), far))
        .await?;
    assert_eq!(kept.until_ms, far);
    let past = control
        .revoke_token(revoke(Wire::TokenId("tok-2".into()), 1))
        .await
        .expect_err("a revocation that has passed is refused");
    assert_eq!(past.code(), Code::FailedPrecondition);
    let invalid = control
        .revoke_token(revoke(Wire::TokenId("not an id".into()), far))
        .await
        .expect_err("an invalid id is refused");
    assert_eq!(invalid.code(), Code::InvalidArgument);

    let settings = MembershipSettings {
        heartbeat_interval_ms: NonZeroU64::new(50).ok_or("zero")?,
        ..MembershipSettings::default()
    };
    // A node that joins gets the list in the join reply, before its first heartbeat.
    let joined = tls
        .node_network("b")?
        .control_client(address.to_string())
        .join(join_with(
            "b",
            Some(reporting(fleetringd::software::VERSION)),
        ))
        .await?;
    assert_eq!(joined.revocations.len(), 2);

    let node = tls.node_network("a")?.control_client(address.to_string());
    let (membership, tasks) = Membership::start(Member::new("a".parse()?), node, &settings);
    let token = fleetring_control::Revoked::Token("tok-1".parse()?);
    let key = fleetring_control::Revoked::Key("key-1".parse()?);
    // Guards against a hang only; nothing here asserts how long a heartbeat takes.
    tokio::time::timeout(std::time::Duration::from_secs(60), async {
        while !membership.denylist().load().contains(&token) {
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    })
    .await?;
    let list = membership.denylist().load();
    assert!(list.contains(&key));
    assert_eq!(list.len(), 2);
    tasks.stop().await;
    daemon.run_until(std::future::ready(())).await;
    Ok(())
}

/// A control plane that answers more slowly than the heartbeat interval must not starve a node of
/// the denylist: the node waits long enough for an answer to reach it (issue 64).
#[tokio::test]
async fn a_node_hears_the_denylist_from_a_control_plane_slower_than_its_heartbeat() -> TestResult {
    use fleetring_proto::v1::{Revocation, RevokeTokenRequest, revocation::Revoked as Wire};
    use fleetringd::{Membership, MembershipSettings};

    let dir = tempfile::tempdir()?;
    let tls = Tls::create(dir.path())?;
    let text = format!(
        "data_dir = {:?}\nlisten = \"127.0.0.1:0\"\nrole = \"control\"\n\n[node]\nid = \"control-1\"\n{}",
        dir.path().join("data"),
        tls.node_table("control-1")?
    );
    let daemon = Daemon::start(Config::from_toml(&text)?).await?;
    let address = daemon.control_address().ok_or("no control address")?;
    secure_client(address, &tls)?
        .revoke_token(RevokeTokenRequest {
            revocation: Some(Revocation {
                revoked: Some(Wire::KeyId("key-1".into())),
                until_ms: u64::MAX,
            }),
        })
        .await?;

    // Every round trip takes at least 200 ms, four times the 50 ms heartbeat interval.
    let slow = delaying_proxy(address, std::time::Duration::from_millis(100)).await?;
    let settings = MembershipSettings {
        heartbeat_interval_ms: NonZeroU64::new(50).ok_or("zero")?,
        ..MembershipSettings::default()
    };
    let node = tls.node_network("a")?.control_client(slow.to_string());
    let (membership, tasks) = Membership::start(Member::new("a".parse()?), node, &settings);
    let key = fleetring_control::Revoked::Key("key-1".parse()?);
    // Guards against a hang only; nothing here asserts how long a heartbeat takes.
    tokio::time::timeout(std::time::Duration::from_secs(60), async {
        while !membership.denylist().load().contains(&key) {
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    })
    .await?;
    tasks.stop().await;
    daemon.run_until(std::future::ready(())).await;
    Ok(())
}

#[tokio::test]
async fn a_node_certificate_acts_on_its_own_node_only() -> TestResult {
    use fleetring_proto::v1::{Revocation, RevokeTokenRequest, revocation::Revoked as Wire};

    let dir = tempfile::tempdir()?;
    let tls = Tls::create(dir.path())?;
    let text = format!(
        "data_dir = {:?}\nlisten = \"127.0.0.1:0\"\nrole = \"control\"\n\n[node]\nid = \"control-1\"\n{}",
        dir.path().join("data"),
        tls.node_table("control-1")?
    );
    let daemon = Daemon::start(Config::from_toml(&text)?).await?;
    let address = daemon.control_address().ok_or("no control address")?;
    let operator = secure_client(address, &tls)?;
    let nodes: Vec<ControlClient> = ["a", "b", "c", "d"]
        .into_iter()
        .map(|id| node_client(address, &tls, id))
        .collect::<Result<_, _>>()?;
    for (id, node) in ["a", "b", "c", "d"].into_iter().zip(&nodes) {
        node.join(join_request(id)).await?;
    }
    let (a, b) = (&nodes[0], &nodes[1]);
    let revoke = RevokeTokenRequest {
        revocation: Some(Revocation {
            revoked: Some(Wire::TokenId("tok-1".into())),
            until_ms: u64::MAX,
        }),
    };

    // A node certificate may not drain or remove another member, nor revoke a token.
    let drain = a
        .drain(DrainRequest {
            node_id: "b".into(),
        })
        .await
        .expect_err("a node can't drain another");
    assert_eq!(drain.code(), Code::PermissionDenied, "{drain}");
    assert_eq!(
        drain.message(),
        "Drain for node 'b' needs that node's certificate, or the fleet scope"
    );
    let leave = a
        .leave(LeaveRequest {
            node_id: "b".into(),
        })
        .await
        .expect_err("a node can't remove another");
    assert_eq!(leave.code(), Code::PermissionDenied, "{leave}");
    let revoked = a
        .revoke_token(revoke.clone())
        .await
        .expect_err("a node can't revoke tokens");
    assert_eq!(revoked.code(), Code::PermissionDenied, "{revoked}");
    assert_eq!(revoked.message(), "RevokeToken needs the fleet scope");
    // Nor create a database, or hand over a lease it doesn't hold.
    let created = a
        .create_database(CreateDatabaseRequest {
            database: "tenant:acme".into(),
            replication_factor: 1,
            engine: Engine::Sqlite.into(),
            acks: Acks::Unspecified.into(),
            min_replicas: 0,
        })
        .await
        .expect_err("a node can't create");
    assert_eq!(
        created.message(),
        "CreateDatabase needs admin on 'tenant:acme'"
    );
    let transfer = a
        .transfer_lease(TransferLeaseRequest {
            database: "tenant:acme".into(),
            target: String::new(),
        })
        .await
        .expect_err("not its lease");
    assert_eq!(
        transfer.message(),
        "TransferLease refused for node 'a': not the holder of database 'tenant:acme' at epoch 0"
    );
    assert_eq!(refused_binding(&transfer), Some("holder"));

    // It may drain and leave itself, as a daemon does when it shuts down.
    b.drain(DrainRequest {
        node_id: "b".into(),
    })
    .await?;
    b.leave(LeaveRequest {
        node_id: "b".into(),
    })
    .await?;

    // A principal with the fleet scope may do all three to any member.
    operator
        .drain(DrainRequest {
            node_id: "c".into(),
        })
        .await?;
    operator
        .leave(LeaveRequest {
            node_id: "c".into(),
        })
        .await?;
    operator.revoke_token(revoke).await?;
    let state = daemon.control().ok_or("no control handle")?.state();
    let members: Vec<String> = state
        .members()
        .map(|member| member.id().to_string())
        .collect();
    assert_eq!(members, ["a", "d"]);
    daemon.run_until(std::future::ready(())).await;
    Ok(())
}

/// A control daemon with three member nodes, each with its own certificate, and a database at
/// R = 2, so that one member is outside its replica set (FR-050).
///
/// The failover timer never ticks and nobody heartbeats on their own, so the control log grows
/// only by the calls a test makes.
struct Bound {
    daemon: Daemon,
    log: std::path::PathBuf,
    operator: ControlClient,
    nodes: std::collections::BTreeMap<String, ControlClient>,
    database: String,
    /// The replica set, the holder first.
    replicas: Vec<String>,
    /// The member outside the replica set.
    outsider: String,
    _dir: tempfile::TempDir,
}

impl Bound {
    async fn start() -> Result<Self, Box<dyn Error>> {
        let dir = tempfile::tempdir()?;
        let tls = Tls::create(dir.path())?;
        let text = format!(
            "data_dir = {:?}\nlisten = \"127.0.0.1:0\"\nrole = \"control\"\n\n[node]\nid = \"control-1\"\n\n\
             [control]\nheartbeat_timeout_ms = 600000\ntick_interval_ms = 3600000\n{}",
            dir.path().join("data"),
            tls.node_table("control-1")?
        );
        let daemon = Daemon::start(Config::from_toml(&text)?).await?;
        let address = daemon.control_address().ok_or("no control address")?;
        let operator = secure_client(address, &tls)?;
        let mut nodes = std::collections::BTreeMap::new();
        for id in ["a", "b", "c"] {
            let node = node_client(address, &tls, id)?;
            node.join(join_request(id)).await?;
            nodes.insert(id.to_owned(), node);
        }
        let database = "tenant:bound".to_owned();
        let created = operator
            .create_database(CreateDatabaseRequest {
                database: database.clone(),
                replication_factor: 2,
                engine: Engine::Sqlite.into(),
                acks: Acks::Unspecified.into(),
                min_replicas: 0,
            })
            .await?
            .placement
            .ok_or("no placement")?;
        let replicas = created.followers;
        assert_eq!(replicas.len(), 2, "{replicas:?}");
        let outsider = ["a", "b", "c"]
            .into_iter()
            .find(|id| !replicas.iter().any(|replica| replica == id))
            .ok_or("every member is a replica")?
            .to_owned();
        let bound = Self {
            log: dir.path().join("data").join("control.db"),
            daemon,
            operator,
            nodes,
            database,
            replicas,
            outsider,
            _dir: dir,
        };
        bound
            .node(bound.holder())
            .acquire_lease(AcquireLeaseRequest {
                database: bound.database.clone(),
                node_id: bound.holder().to_owned(),
            })
            .await?;
        // The follower reports its copy, so that a transfer has a candidate.
        bound
            .node(bound.follower())
            .heartbeat(HeartbeatRequest {
                node_id: bound.follower().to_owned(),
                databases: vec![DatabaseStatus {
                    database: bound.database.clone(),
                    role: ReplicaRole::Follower.into(),
                    epoch: 1,
                    position: vec![0, 1],
                }],
                software: None,
            })
            .await?;
        Ok(bound)
    }

    fn node(&self, id: &str) -> &ControlClient {
        &self.nodes[id]
    }

    fn holder(&self) -> &str {
        &self.replicas[0]
    }

    fn follower(&self) -> &str {
        &self.replicas[1]
    }

    /// The index of the last command the control log holds.
    fn logged(&self) -> Result<i64, Box<dyn Error>> {
        let log = rusqlite::Connection::open_with_flags(
            &self.log,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        )?;
        Ok(log.query_row(
            "SELECT coalesce(max(log_index), 0) FROM commands",
            [],
            |row| row.get(0),
        )?)
    }

    async fn locate(&self) -> Result<fleetring_proto::v1::Placement, Box<dyn Error>> {
        Ok(self
            .operator
            .locate(LocateRequest {
                database: self.database.clone(),
            })
            .await?
            .placement
            .ok_or("no placement")?)
    }

    /// Drains the holder and has it hand its lease over, as a draining node does, and returns
    /// the promotion's epoch and candidate.
    async fn transfer(&self) -> Result<(u64, String), Box<dyn Error>> {
        let holder = self.node(self.holder());
        holder
            .drain(DrainRequest {
                node_id: self.holder().to_owned(),
            })
            .await?;
        let started = holder
            .transfer_lease(TransferLeaseRequest {
                database: self.database.clone(),
                target: String::new(),
            })
            .await?;
        assert_eq!(started.candidate, self.follower());
        Ok((started.epoch, started.candidate))
    }

    async fn stop(self) {
        self.daemon.run_until(std::future::ready(())).await;
    }
}

/// Asserts that `refused` is a binding refusal naming `binding`, and that the control log still
/// ends at `logged`.
fn assert_refused(
    bound: &Bound,
    refused: &Status,
    binding: &str,
    logged: i64,
) -> Result<(), Box<dyn Error>> {
    assert_eq!(refused.code(), Code::PermissionDenied, "{refused}");
    assert_eq!(refused_binding(refused), Some(binding), "{refused}");
    assert_eq!(bound.logged()?, logged, "a refused call logged nothing");
    Ok(())
}

#[tokio::test]
async fn start_promotion_from_outside_the_replica_set_is_refused() -> TestResult {
    let bound = Bound::start().await?;
    let logged = bound.logged()?;
    let request = || StartPromotionRequest {
        database: bound.database.clone(),
    };
    let refused = bound
        .node(&bound.outsider)
        .start_promotion(request())
        .await
        .expect_err("not a replica");
    assert_refused(&bound, &refused, "replica", logged)?;
    assert_eq!(
        refused.message(),
        format!(
            "StartPromotion refused for node '{}': not a replica of database 'tenant:bound'",
            bound.outsider
        )
    );

    // A replica passes the binding; the lease hasn't expired, so the state machine says no.
    let early = bound
        .node(bound.follower())
        .start_promotion(request())
        .await
        .expect_err("the lease is live");
    assert_eq!(early.code(), Code::FailedPrecondition, "{early}");
    assert_eq!(refused_binding(&early), None);
    bound.stop().await;
    Ok(())
}

#[tokio::test]
async fn record_fenced_from_a_node_other_than_the_candidate_is_refused() -> TestResult {
    let bound = Bound::start().await?;
    let (epoch, candidate) = bound.transfer().await?;
    let fence = |node: &str, epoch: u64| RecordFencedRequest {
        database: bound.database.clone(),
        node_id: node.to_owned(),
        epoch,
        position: vec![0, 1],
    };
    let logged = bound.logged()?;
    for caller in [bound.holder(), bound.outsider.as_str()] {
        let refused = bound
            .node(caller)
            .record_fenced(fence(caller, epoch))
            .await
            .expect_err("not the candidate");
        assert_refused(&bound, &refused, "candidate", logged)?;
        assert_eq!(
            refused.message(),
            format!(
                "RecordFenced refused for node '{caller}': not the candidate of database \
                 'tenant:bound' at epoch {epoch}"
            )
        );
    }
    // No promotion to a later epoch exists yet, so nobody is its candidate, the current one
    // included.
    let ahead = bound
        .node(&candidate)
        .record_fenced(fence(&candidate, epoch + 1))
        .await
        .expect_err("no promotion to that epoch");
    assert_refused(&bound, &ahead, "candidate", logged)?;

    let recorded = bound
        .node(&candidate)
        .record_fenced(fence(&candidate, epoch))
        .await?;
    assert_eq!(recorded.fenced, 1);
    bound.stop().await;
    Ok(())
}

#[tokio::test]
async fn complete_promotion_from_a_node_other_than_the_candidate_is_refused() -> TestResult {
    let bound = Bound::start().await?;
    let (epoch, candidate) = bound.transfer().await?;
    for fenced in [bound.holder(), candidate.as_str()] {
        bound
            .node(&candidate)
            .record_fenced(RecordFencedRequest {
                database: bound.database.clone(),
                node_id: fenced.to_owned(),
                epoch,
                position: vec![0, 1],
            })
            .await?;
    }
    let complete = || CompletePromotionRequest {
        database: bound.database.clone(),
        epoch,
    };
    let logged = bound.logged()?;
    for caller in [bound.holder(), bound.outsider.as_str()] {
        let refused = bound
            .node(caller)
            .complete_promotion(complete())
            .await
            .expect_err("not the candidate");
        assert_refused(&bound, &refused, "candidate", logged)?;
    }
    let completed = bound
        .node(&candidate)
        .complete_promotion(complete())
        .await?;
    assert_eq!(completed.lease.map(|lease| lease.epoch), Some(epoch));
    // Once it completed, a repeat is the state machine's to refuse, as before.
    let again = bound
        .node(&candidate)
        .complete_promotion(complete())
        .await
        .expect_err("no promotion left");
    assert_eq!(again.code(), Code::FailedPrecondition, "{again}");
    assert_eq!(refused_binding(&again), None);
    bound.stop().await;
    Ok(())
}

#[tokio::test]
async fn transfer_lease_from_a_node_that_does_not_hold_the_lease_is_refused() -> TestResult {
    let bound = Bound::start().await?;
    bound
        .node(bound.holder())
        .drain(DrainRequest {
            node_id: bound.holder().to_owned(),
        })
        .await?;
    let transfer = || TransferLeaseRequest {
        database: bound.database.clone(),
        target: String::new(),
    };
    let logged = bound.logged()?;
    for caller in [bound.follower(), bound.outsider.as_str()] {
        let refused = bound
            .node(caller)
            .transfer_lease(transfer())
            .await
            .expect_err("not the holder");
        assert_refused(&bound, &refused, "holder", logged)?;
        assert_eq!(
            refused.message(),
            format!(
                "TransferLease refused for node '{caller}': not the holder of database \
                 'tenant:bound' at epoch 1"
            )
        );
    }
    let started = bound
        .node(bound.holder())
        .transfer_lease(transfer())
        .await?;
    assert_eq!(started.epoch, 2);
    assert_eq!(started.candidate, bound.follower());
    bound.stop().await;
    Ok(())
}

#[tokio::test]
async fn locate_names_the_candidate_while_a_promotion_is_in_progress() -> TestResult {
    let bound = Bound::start().await?;
    let before = bound.locate().await?;
    assert_eq!(before.primary.as_deref(), Some(bound.holder()));
    assert_eq!(before.candidate, None);

    let (epoch, candidate) = bound.transfer().await?;
    let during = bound.locate().await?;
    assert_eq!(during.epoch, epoch);
    assert_eq!(during.primary, None);
    assert_eq!(during.candidate.as_deref(), Some(candidate.as_str()));

    for fenced in [bound.holder(), candidate.as_str()] {
        bound
            .node(&candidate)
            .record_fenced(RecordFencedRequest {
                database: bound.database.clone(),
                node_id: fenced.to_owned(),
                epoch,
                position: vec![0, 1],
            })
            .await?;
    }
    bound
        .node(&candidate)
        .complete_promotion(CompletePromotionRequest {
            database: bound.database.clone(),
            epoch,
        })
        .await?;
    let after = bound.locate().await?;
    assert_eq!(after.primary.as_deref(), Some(candidate.as_str()));
    assert_eq!(after.candidate, None);
    bound.stop().await;
    Ok(())
}

/// A join request from `id` reporting `software`, or nothing, as 0.3.0 nodes send.
fn join_with(id: &str, software: Option<fleetring_proto::v1::Software>) -> JoinRequest {
    JoinRequest {
        software,
        ..join_request(id)
    }
}

/// The software this build reports, with its version replaced.
fn reporting(version: &str) -> fleetring_proto::v1::Software {
    fleetring_proto::v1::Software {
        version: version.to_owned(),
        protocol: Some(PROTOCOL_VERSION),
        control_format: 1,
        features: vec!["undrain".to_owned()],
    }
}

/// `version` without any fleet feature: a member that can run beside this build but can't act on
/// a command a feature gates.
fn featureless(version: &str) -> fleetring_proto::v1::Software {
    fleetring_proto::v1::Software {
        features: Vec::new(),
        ..reporting(version)
    }
}

/// The first release of the line before this build's, which may run beside it (ADR-0038).
fn previous_line() -> fleetring_control::Release {
    let (major, minor) = fleetringd::software::release().line();
    fleetring_control::Release::new(major, minor.saturating_sub(1), 0)
}

/// The first release of the line after this build's: two lines past [`previous_line`].
fn next_line() -> fleetring_control::Release {
    let (major, minor) = fleetringd::software::release().line();
    fleetring_control::Release::new(major, minor + 1, 0)
}

/// ADR-0038 over the wire: the control node records what each member runs, refuses a join two
/// release lines away, refuses `Undrain` while a member doesn't support it without logging it,
/// and once every member does, applies it and refuses members that lack it from then on.
#[tokio::test]
async fn a_mixed_fleet_refuses_a_command_a_member_does_not_support() -> TestResult {
    let dir = tempfile::tempdir()?;
    let tls = Tls::create(dir.path())?;
    let text = format!(
        "data_dir = {:?}\nlisten = \"127.0.0.1:0\"\nrole = \"control\"\n\n[node]\nid = \"control-1\"\n{}",
        dir.path().join("data"),
        tls.node_table("control-1")?,
    );
    let daemon = Daemon::start(Config::from_toml(&text)?).await?;
    let address = daemon.control_address().ok_or("no control address")?;
    let control = secure_client(address, &tls)?;
    let current = fleetringd::software::VERSION;
    let previous = previous_line().to_string();
    let next = next_line().to_string();

    // "old" runs the line before this build's without the feature; "new" runs this build.
    node_client(address, &tls, "old")?
        .join(join_with("old", Some(featureless(&previous))))
        .await?;
    node_client(address, &tls, "new")?
        .join(join_with("new", Some(reporting(current))))
        .await?;
    let update = control
        .watch_membership(WatchMembershipRequest { after_epoch: 0 })
        .await?
        .message()
        .await?
        .ok_or("membership stream ended")?;
    let reported: Vec<_> = update
        .members
        .iter()
        .map(|member| {
            member
                .software
                .as_ref()
                .map(|software| software.version.as_str())
        })
        .collect();
    assert_eq!(
        reported,
        [Some(current), Some(previous.as_str())],
        "new, then old"
    );

    // Two release lines past the baseline is refused, naming both nodes: "old", or "new" when
    // this build is older than the baseline's line.
    let far = node_client(address, &tls, "far")?
        .join(join_with("far", Some(reporting(&next))))
        .await
        .expect_err("two lines past old");
    assert_eq!(far.code(), Code::FailedPrecondition);
    assert!(
        far.message().starts_with(&format!(
            "node 'far' runs fleetringd {next}, which can't run beside node '"
        )),
        "{far:?}"
    );

    control
        .drain(DrainRequest {
            node_id: "new".into(),
        })
        .await?;
    let undrain = || fleetring_proto::v1::UndrainRequest {
        node_id: "new".into(),
    };
    let refused = control
        .undrain(undrain())
        .await
        .expect_err("old doesn't support undrain");
    assert_eq!(refused.code(), Code::FailedPrecondition);
    assert!(
        refused.message().contains(&format!(
            "can't use 'undrain' yet: node 'old' runs fleetringd {previous}"
        )),
        "{refused:?}"
    );
    let handle = daemon.control().ok_or("no control")?;
    assert!(handle.state().is_draining(&"new".parse()?));

    // Once "old" runs this build too, the command applies, and enables the feature for good.
    node_client(address, &tls, "old")?
        .join(join_with("old", Some(reporting(current))))
        .await?;
    control.undrain(undrain()).await?;
    let state = handle.state();
    assert!(!state.is_draining(&"new".parse()?));
    assert!(state.is_enabled(fleetring_control::Feature::Undrain));
    let rolled_back = node_client(address, &tls, "old")?
        .join(join_with("old", Some(featureless(&previous))))
        .await
        .expect_err("a rollback below the feature is refused");
    assert_eq!(rolled_back.code(), Code::FailedPrecondition);
    assert!(
        rolled_back
            .message()
            .contains("lacks the fleet feature 'undrain'"),
        "{rolled_back:?}"
    );

    daemon.run_until(async {}).await;
    // The refused command never reached the log, where a control node of an older release
    // couldn't read it; the accepted one did.
    let log = CommandLog::open(dir.path().join("data/control.db"), ControlConfig::new())?;
    let undrains = log
        .commands()?
        .into_iter()
        .filter(|(_, command)| matches!(command.action, Action::Undrain { .. }))
        .count();
    assert_eq!(undrains, 1);
    Ok(())
}

/// `action` as a control node of a release without software, such as 0.3.0 after a rollback,
/// decodes it from the log: its types have no `software` fields, so they are dropped.
fn without_software(action: &Action) -> Result<Action, Box<dyn Error>> {
    let mut value = serde_json::to_value(action)?;
    if let Some(bodies) = value.as_object_mut() {
        for body in bodies.values_mut().filter_map(|body| body.as_object_mut()) {
            body.remove("software");
            if let Some(member) = body.get_mut("member").and_then(|m| m.as_object_mut()) {
                member.remove("software");
            }
        }
    }
    Ok(serde_json::from_value(value)?)
}

/// A join the control node refuses for its software is refused before it is logged, as a
/// command that needs a feature is (ADR-0038, point 4). A control node rolled back to 0.3.0
/// replays the log without software, and would accept a logged one: a node the fleet refused
/// would become a member there, and the epoch would grow once per retry. So the log holds none,
/// and a replay without software reaches the same members, epoch, and ring.
#[tokio::test]
async fn a_join_refused_for_its_software_is_never_logged() -> TestResult {
    use fleetring_control::{Rejection, Software};
    use fleetringd::control::SubmitError;

    let dir = tempfile::tempdir()?;
    let path = dir.path().join("control.db");
    let node = ControlNode::open(&path, ControlOptions::default(), TickClock::default()).await?;
    let handle = node.handle();
    let join = |id: &str, software: Option<Software>| -> Result<Action, Box<dyn Error>> {
        let member = Member::new(id.parse()?);
        Ok(Action::Join {
            member: match software {
                Some(software) => member.with_software(software),
                None => member,
            },
        })
    };
    let current = fleetringd::software::running();

    // "old" runs the line before this build's without the feature; "new" runs this build.
    let old = Software::new(previous_line(), 1);
    handle.submit(join("old", Some(old.clone()))?).await?;
    handle.submit(join("new", Some(current.clone()))?).await?;
    // A node on another major is refused at every retry, as its heartbeat loop sends them.
    let far = Software::new("9.0.0".parse()?, 1);
    for _ in 0..3 {
        let refused = handle
            .submit(join("far", Some(far.clone()))?)
            .await
            .expect_err("9.0 can't run beside this build's line");
        assert!(
            matches!(
                &refused,
                SubmitError::Refused(Rejection::IncompatibleVersion { id, .. }) if id.as_str() == "far"
            ),
            "{refused:?}"
        );
    }
    handle.submit(join("old", Some(current.clone()))?).await?;
    let before_feature = handle.state();

    // Once a feature is enabled, a node without it is refused, a new one and a rollback alike.
    handle
        .submit(Action::Undrain { id: "new".parse()? })
        .await?;
    let featureless = Software::new(fleetringd::software::release(), 1);
    for refused in [join("late", Some(featureless))?, join("old", Some(old))?] {
        let refused = handle.submit(refused).await.expect_err("lacks undrain");
        assert!(
            matches!(
                &refused,
                SubmitError::Refused(Rejection::MissingFeature { .. })
            ),
            "{refused:?}"
        );
    }
    let published = handle.state();
    node.shutdown().await;

    let log = CommandLog::open(&path, ControlConfig::new())?;
    let logged: Vec<Action> = log
        .commands()?
        .into_iter()
        .map(|(_, command)| command.action)
        .collect();
    let joins: Vec<&str> = logged
        .iter()
        .filter_map(|action| match action {
            Action::Join { member } => Some(member.id().as_str()),
            _ => None,
        })
        .collect();
    assert_eq!(joins, ["old", "new", "old"], "no refused join is logged");
    let (replayed, outcomes) = replay(&log)?;
    assert!(outcomes.iter().all(Result::is_ok), "{outcomes:?}");
    assert_eq!(replayed, *published);

    // A control node rolled back before the feature was first used replays the log so far
    // without software, and reaches the membership this build published.
    let mut old = State::new(log.genesis());
    let unfeatured = logged
        .iter()
        .take_while(|action| !matches!(action, Action::Undrain { .. }));
    for (at, action) in (1..).zip(unfeatured) {
        apply(
            &mut old,
            Command::new(Timestamp::from_millis(at), without_software(action)?),
        )?;
    }
    assert_eq!(old.membership_epoch(), before_feature.membership_epoch());
    let ids = |state: &State| state.members().map(|m| m.id().clone()).collect::<Vec<_>>();
    assert_eq!(ids(&old), ids(&before_feature));
    assert_eq!(
        old.ring()?.fingerprint(),
        before_feature.ring()?.fingerprint()
    );
    // It accepts what this build refused, which is why the refusal must never reach the log.
    apply(
        &mut old,
        Command::new(
            Timestamp::from_millis(100),
            without_software(&join("far", Some(far))?)?,
        ),
    )?;
    assert!(old.member(&"far".parse()?).is_some());
    Ok(())
}

/// The writer screens every command that needs any fleet feature, not only `undrain`: a
/// metadata or expiry command a member can't act on is refused before it is logged, so a
/// control node of the previous line, after a rollback, never meets one it can't decode
/// (ADR-0041, point 3). This build reports `expiry`, so a fleet of it takes an expiry.
#[tokio::test]
async fn a_metadata_command_a_member_lacks_is_never_logged() -> TestResult {
    use fleetring_control::{
        Engine, ExpiryChange, Feature, Labels, MetadataChange, MetadataSpec, Rejection, Software,
        parse_label,
    };
    use fleetringd::control::SubmitError;

    let dir = tempfile::tempdir()?;
    let path = dir.path().join("control.db");
    let node = ControlNode::open(&path, ControlOptions::default(), TickClock::default()).await?;
    let handle = node.handle();
    let join = |id: &str, software: Software| -> Result<Action, Box<dyn Error>> {
        Ok(Action::Join {
            member: Member::new(id.parse()?).with_software(software),
        })
    };
    let create = |name: &str, metadata: Option<MetadataSpec>| -> Result<Action, Box<dyn Error>> {
        Ok(Action::CreateDatabase {
            name: name.parse()?,
            replication_factor: fleetring::ReplicationFactor::ONE,
            engine: Engine::Sqlite,
            durability: None,
            metadata,
        })
    };
    let labelled = MetadataSpec {
        labels: Labels::try_from_pairs([parse_label("purpose=task")?])?,
        ..MetadataSpec::default()
    };
    let label = Action::UpdateDatabase {
        name: "plain".parse()?,
        change: MetadataChange {
            set_labels: Labels::try_from_pairs([parse_label("purpose=memory")?])?,
            ..MetadataChange::default()
        },
        by: None,
        if_version: None,
    };

    let expiring = MetadataSpec {
        expires_at: Some(Timestamp::MAX),
        ..MetadataSpec::default()
    };
    let extend = Action::UpdateDatabase {
        name: "plain".parse()?,
        change: MetadataChange {
            expiry: ExpiryChange::Set(Timestamp::MAX),
            ..MetadataChange::default()
        },
        by: None,
        if_version: None,
    };
    let refused_for = |refused: &SubmitError, feature: Feature| {
        matches!(
            refused,
            SubmitError::Refused(Rejection::FeatureUnsupported {
                feature: lacking,
                id,
                ..
            }) if *lacking == feature && id.as_str() == "old"
        )
    };

    // "old" runs the line before this build's, which knows neither metadata nor expiry. A
    // command with an expiry needs both, and names the first one the member lacks.
    let old = Software::new(previous_line(), 1).with_feature(Feature::Undrain);
    let current = fleetringd::software::running();
    handle.submit(join("old", old)?).await?;
    handle.submit(join("new", current.clone())?).await?;
    handle.submit(create("plain", None)?).await?;
    for refused in [
        create("labelled", Some(labelled.clone()))?,
        label.clone(),
        create("expiring", Some(expiring.clone()))?,
        extend.clone(),
    ] {
        let refused = handle
            .submit(refused)
            .await
            .expect_err("old lacks metadata");
        assert!(refused_for(&refused, Feature::Metadata), "{refused:?}");
    }

    // A member that reports metadata but not expiry, as the release before this phase did:
    // metadata applies, and an expiry is refused for want of `expiry` alone.
    let without_expiry = Software::new(previous_line(), 1)
        .with_feature(Feature::Undrain)
        .with_feature(Feature::Metadata);
    handle.submit(join("old", without_expiry)?).await?;
    handle.submit(create("labelled", Some(labelled))?).await?;
    handle.submit(label).await?;
    for refused in [create("expiring", Some(expiring.clone()))?, extend.clone()] {
        let refused = handle.submit(refused).await.expect_err("old lacks expiry");
        assert!(refused_for(&refused, Feature::Expiry), "{refused:?}");
    }
    assert!(!handle.state().is_enabled(Feature::Expiry));

    // Once every member runs this build, which reports `expiry`, an expiry applies.
    handle.submit(join("old", current)?).await?;
    handle.submit(create("expiring", Some(expiring))?).await?;
    handle.submit(extend).await?;
    let published = handle.state();
    assert!(published.is_enabled(Feature::Metadata));
    assert!(published.is_enabled(Feature::Expiry));
    node.shutdown().await;

    let log = CommandLog::open(&path, ControlConfig::new())?;
    let logged: Vec<Action> = log
        .commands()?
        .into_iter()
        .map(|(_, command)| command.action)
        .collect();
    let gated: Vec<&Action> = logged
        .iter()
        .filter(|action| !action.features().is_empty())
        .collect();
    assert_eq!(
        gated.len(),
        4,
        "only the accepted metadata and expiry commands: {gated:?}"
    );
    let (replayed, outcomes) = replay(&log)?;
    assert!(outcomes.iter().all(Result::is_ok), "{outcomes:?}");
    assert_eq!(replayed, *published);
    Ok(())
}

/// Database metadata over the wire (ADR-0041, FR-070 phase 2).
mod metadata {
    use std::error::Error;
    use std::net::SocketAddr;
    use std::path::Path;

    use fleetring_control::{Action, ControlConfig, Feature};
    use fleetring_proto::v1::update_database_request::Description;
    use fleetring_proto::v1::{
        CreateDatabaseWithMetadataRequest, DatabaseMetadata, Label, ListDatabasesRequest,
        LocateRequest, MetadataSpec, UpdateDatabaseRequest,
    };
    use fleetringd::control::{CommandLog, ControlClient};
    use fleetringd::{Config, Daemon};
    use tonic::Code;

    use super::{
        TestResult, create_request, join_with, node_client, previous_line, reporting, secure_client,
    };
    use crate::support::Tls;

    /// This build's software, as a member running it reports it: `metadata` among its features.
    fn this_build() -> fleetring_proto::v1::Software {
        let running = fleetringd::software::running();
        fleetring_proto::v1::Software {
            features: running
                .features()
                .map(|feature| feature.as_str().to_owned())
                .collect(),
            ..reporting(fleetringd::software::VERSION)
        }
    }

    /// The configuration of a control daemon in `dir`, served over TLS. Paths go through `{:?}`,
    /// which escapes a Windows path's backslashes for TOML.
    fn control_config(dir: &Path, tls: &Tls) -> Result<String, Box<dyn Error>> {
        Ok(format!(
            "data_dir = {:?}\nlisten = \"127.0.0.1:0\"\nrole = \"control\"\n\n[node]\nid = \
             \"control-1\"\n{}",
            dir.join("data"),
            tls.node_table("control-1")?,
        ))
    }

    /// A control daemon in `dir`, served over TLS.
    async fn control_daemon(dir: &Path, tls: &Tls) -> Result<Daemon, Box<dyn Error>> {
        Ok(Daemon::start(Config::from_toml(&control_config(dir, tls)?)?).await?)
    }

    /// A control daemon whose fleet has one member, `data-1`, running this build, and a client
    /// acting as the principal `tester`.
    async fn fleet(dir: &Path) -> Result<(Daemon, ControlClient, Tls), Box<dyn Error>> {
        let tls = Tls::create(dir)?;
        let daemon = control_daemon(dir, &tls).await?;
        let address = daemon.control_address().ok_or("no control address")?;
        join(address, &tls, "data-1", this_build()).await?;
        let control = secure_client(address, &tls)?;
        Ok((daemon, control, tls))
    }

    async fn join(
        address: SocketAddr,
        tls: &Tls,
        id: &str,
        software: fleetring_proto::v1::Software,
    ) -> TestResult {
        node_client(address, tls, id)?
            .join(join_with(id, Some(software)))
            .await?;
        Ok(())
    }

    fn label(key: &str, value: &str) -> Label {
        Label {
            key: key.to_owned(),
            value: value.to_owned(),
        }
    }

    fn with_metadata(
        database: &str,
        description: &str,
        labels: Vec<Label>,
    ) -> CreateDatabaseWithMetadataRequest {
        CreateDatabaseWithMetadataRequest {
            database: Some(create_request(database, 1)),
            metadata: Some(MetadataSpec {
                description: description.to_owned(),
                labels,
                expiry: None,
            }),
        }
    }

    async fn locate(
        control: &ControlClient,
        database: &str,
    ) -> Result<Option<DatabaseMetadata>, Box<dyn Error>> {
        Ok(control
            .locate(LocateRequest {
                database: database.to_owned(),
            })
            .await?
            .placement
            .ok_or("no placement")?
            .metadata)
    }

    fn keys(metadata: &DatabaseMetadata) -> Vec<String> {
        metadata
            .labels
            .iter()
            .map(|label| format!("{}={}", label.key, label.value))
            .collect()
    }

    #[tokio::test]
    async fn metadata_round_trips_through_create_update_and_locate() -> TestResult {
        let dir = tempfile::tempdir()?;
        let (daemon, control, _tls) = fleet(dir.path()).await?;

        let created = control
            .create_database_with_metadata(with_metadata(
                "notes",
                "Memory for agent 42.\nTables: facts.",
                vec![label("purpose", "memory"), label("agent", "42")],
            ))
            .await?
            .placement
            .ok_or("no placement")?
            .metadata
            .ok_or("no metadata")?;
        assert_eq!(created.description, "Memory for agent 42.\nTables: facts.");
        assert_eq!(created.description_by.as_deref(), Some("tester"));
        assert_eq!(created.created_by.as_deref(), Some("tester"));
        assert!(created.created_at_ms.is_some());
        assert_eq!(created.expires_at_ms, None);
        assert_eq!(created.version, 1);
        assert_eq!(
            keys(&created),
            ["agent=42", "purpose=memory"],
            "sorted by key"
        );

        let updated = control
            .update_database(UpdateDatabaseRequest {
                database: "notes".to_owned(),
                description: Some(Description::SetDescription("Version 2.".to_owned())),
                set_labels: vec![label("purpose", "task")],
                remove_labels: vec!["agent".to_owned()],
                if_version: None,
                expiry: None,
            })
            .await?
            .metadata
            .ok_or("no metadata")?;
        assert_eq!(updated.version, 2);
        assert_eq!(updated.description, "Version 2.");
        assert_eq!(keys(&updated), ["purpose=task"]);
        assert_eq!(updated.created_at_ms, created.created_at_ms);
        assert_eq!(locate(&control, "notes").await?, Some(updated.clone()));

        // A listing leaves the text out unless asked, and keeps the rest.
        let listed = |descriptions| ListDatabasesRequest {
            descriptions,
            ..ListDatabasesRequest::default()
        };
        let brief = control.list_databases(listed(false)).await?.databases;
        let brief = brief
            .first()
            .and_then(|placement| placement.metadata.clone())
            .ok_or("no metadata")?;
        assert_eq!(brief.description, "");
        assert_eq!(brief.description_by.as_deref(), Some("tester"));
        assert_eq!(keys(&brief), ["purpose=task"]);
        let full = control.list_databases(listed(true)).await?.databases;
        assert_eq!(
            full.first()
                .and_then(|placement| placement.metadata.as_ref()),
            Some(&updated)
        );

        // Clearing the description clears its author; a restored description has none.
        let cleared = control
            .update_database(UpdateDatabaseRequest {
                database: "notes".to_owned(),
                description: Some(Description::ClearDescription(true)),
                ..UpdateDatabaseRequest::default()
            })
            .await?
            .metadata
            .ok_or("no metadata")?;
        assert_eq!(
            (cleared.description.as_str(), cleared.description_by),
            ("", None)
        );
        let restored = control
            .update_database(UpdateDatabaseRequest {
                database: "notes".to_owned(),
                description: Some(Description::SetDescriptionWithoutAuthor(
                    "From the archive.".to_owned(),
                )),
                ..UpdateDatabaseRequest::default()
            })
            .await?
            .metadata
            .ok_or("no metadata")?;
        assert_eq!(restored.description, "From the archive.");
        assert_eq!(restored.description_by, None);
        daemon.run_until(async {}).await;
        Ok(())
    }

    /// The creator is recorded on a plain creation only once `metadata` is enabled, so the first
    /// ordinary creation after an upgrade doesn't enable it (ADR-0041, point 3).
    #[tokio::test]
    async fn a_plain_creation_records_the_creator_once_metadata_is_enabled() -> TestResult {
        let dir = tempfile::tempdir()?;
        let (daemon, control, _tls) = fleet(dir.path()).await?;
        let handle = daemon.control().ok_or("no control")?;

        control.create_database(create_request("before", 1)).await?;
        assert_eq!(locate(&control, "before").await?, None);
        assert!(!handle.state().is_enabled(Feature::Metadata));

        control
            .create_database_with_metadata(with_metadata("labelled", "", vec![label("a", "b")]))
            .await?;
        assert!(handle.state().is_enabled(Feature::Metadata));
        let labelled = locate(&control, "labelled").await?.ok_or("no metadata")?;
        assert_eq!(labelled.description_by, None, "no description, no author");

        control.create_database(create_request("after", 1)).await?;
        let after = locate(&control, "after").await?.ok_or("no metadata")?;
        assert_eq!(after.created_by.as_deref(), Some("tester"));
        assert_eq!((after.description.as_str(), after.labels.len()), ("", 0));
        assert_eq!(after.version, 1);
        assert_eq!(locate(&control, "before").await?, None, "unchanged");
        daemon.run_until(async {}).await;
        Ok(())
    }

    /// A creation or an update with metadata, in a fleet where a member runs the previous line,
    /// is refused before it is logged, naming the member, and nothing is created or changed; a
    /// plain creation still works (ADR-0041, point 3; ADR-0038, point 4).
    #[tokio::test]
    async fn a_creation_with_metadata_is_refused_in_a_mixed_fleet() -> TestResult {
        let dir = tempfile::tempdir()?;
        let tls = Tls::create(dir.path())?;
        let daemon = control_daemon(dir.path(), &tls).await?;
        let address = daemon.control_address().ok_or("no control address")?;
        let previous = previous_line().to_string();
        join(address, &tls, "old", reporting(&previous)).await?;
        join(address, &tls, "new", this_build()).await?;
        let control = secure_client(address, &tls)?;
        let names_old = |status: &tonic::Status| {
            assert_eq!(status.code(), Code::FailedPrecondition, "{status:?}");
            assert!(
                status.message().contains(&format!(
                    "can't use 'metadata' yet: node 'old' runs fleetringd {previous}"
                )),
                "{status:?}"
            );
        };

        let refused = control
            .create_database_with_metadata(with_metadata("notes", "x", Vec::new()))
            .await
            .expect_err("old lacks metadata");
        names_old(&refused);
        let missing = control
            .locate(LocateRequest {
                database: "notes".to_owned(),
            })
            .await
            .expect_err("never created");
        assert_eq!(missing.code(), Code::NotFound);

        control.create_database(create_request("plain", 1)).await?;
        let refused = control
            .update_database(UpdateDatabaseRequest {
                database: "plain".to_owned(),
                set_labels: vec![label("purpose", "memory")],
                ..UpdateDatabaseRequest::default()
            })
            .await
            .expect_err("old lacks metadata");
        names_old(&refused);
        assert_eq!(locate(&control, "plain").await?, None);
        let handle = daemon.control().ok_or("no control")?;
        assert!(!handle.state().is_enabled(Feature::Metadata));

        daemon.run_until(async {}).await;
        let log = CommandLog::open(dir.path().join("data/control.db"), ControlConfig::new())?;
        let logged: Vec<Action> = log
            .commands()?
            .into_iter()
            .map(|(_, command)| command.action)
            .collect();
        assert!(
            logged.iter().all(|action| action.features().is_empty()),
            "no refused command is logged: {logged:?}"
        );
        assert_eq!(
            logged
                .iter()
                .filter(|action| matches!(action, Action::CreateDatabase { .. }))
                .count(),
            1
        );
        Ok(())
    }

    #[tokio::test]
    async fn an_update_with_a_stale_version_is_aborted() -> TestResult {
        let dir = tempfile::tempdir()?;
        let (daemon, control, _tls) = fleet(dir.path()).await?;
        control
            .create_database_with_metadata(with_metadata("notes", "v1", Vec::new()))
            .await?;
        let rewrite = |text: &str, if_version| UpdateDatabaseRequest {
            database: "notes".to_owned(),
            description: Some(Description::SetDescription(text.to_owned())),
            if_version: Some(if_version),
            ..UpdateDatabaseRequest::default()
        };
        control.update_database(rewrite("v2", 1)).await?;
        let stale = control
            .update_database(rewrite("lost", 1))
            .await
            .expect_err("version 1 is gone");
        assert_eq!(stale.code(), Code::Aborted, "{stale:?}");
        assert_eq!(
            stale.message(),
            "metadata of database 'notes' is at version 2, not 1"
        );
        let now = locate(&control, "notes").await?.ok_or("no metadata")?;
        assert_eq!((now.description.as_str(), now.version), ("v2", 2));
        daemon.run_until(async {}).await;
        Ok(())
    }

    /// The names a listing returns, and the name the next page starts after.
    async fn list(
        control: &ControlClient,
        request: ListDatabasesRequest,
    ) -> Result<(Vec<String>, String), Box<dyn Error>> {
        let response = control.list_databases(request).await?;
        let names = response
            .databases
            .into_iter()
            .map(|placement| placement.database)
            .collect();
        Ok((names, response.next_after))
    }

    fn labelled(labels: &[&str]) -> ListDatabasesRequest {
        ListDatabasesRequest {
            labels: labels.iter().map(|text| (*text).to_owned()).collect(),
            ..ListDatabasesRequest::default()
        }
    }

    #[tokio::test]
    async fn listing_filters_by_label_and_prefix_and_pages() -> TestResult {
        let dir = tempfile::tempdir()?;
        let (daemon, control, _tls) = fleet(dir.path()).await?;
        for (name, purpose) in [
            ("agent-1-a", "memory"),
            ("agent-1-b", "task"),
            ("agent-2-a", "memory"),
            ("other", "cache"),
        ] {
            control
                .create_database_with_metadata(with_metadata(
                    name,
                    "",
                    vec![label("purpose", purpose)],
                ))
                .await?;
        }
        control.create_database(create_request("notes", 1)).await?;

        let (memory, _) = list(&control, labelled(&["purpose=memory"])).await?;
        assert_eq!(memory, ["agent-1-a", "agent-2-a"]);
        let (any_purpose, _) = list(&control, labelled(&["purpose"])).await?;
        assert_eq!(
            any_purpose,
            ["agent-1-a", "agent-1-b", "agent-2-a", "other"]
        );
        let (both, _) = list(&control, labelled(&["purpose=memory", "purpose=task"])).await?;
        assert!(both.is_empty(), "every requirement must hold: {both:?}");
        let (agent_1, _) = list(
            &control,
            ListDatabasesRequest {
                prefix: "agent-1-".to_owned(),
                ..labelled(&[])
            },
        )
        .await?;
        assert_eq!(agent_1, ["agent-1-a", "agent-1-b"]);

        let mut pages = Vec::new();
        let mut after = String::new();
        loop {
            let (page, next) = list(
                &control,
                ListDatabasesRequest {
                    after: after.clone(),
                    limit: 2,
                    ..labelled(&[])
                },
            )
            .await?;
            pages.push(page);
            if next.is_empty() {
                break;
            }
            after = next;
        }
        assert_eq!(
            pages,
            [
                vec!["agent-1-a", "agent-1-b"],
                vec!["agent-2-a", "notes"],
                vec!["other"],
            ]
        );

        let bad = control
            .list_databases(labelled(&["-bad"]))
            .await
            .expect_err("an invalid key");
        assert_eq!(bad.code(), Code::InvalidArgument);
        daemon.run_until(async {}).await;
        Ok(())
    }

    #[tokio::test]
    async fn oversized_descriptions_and_invalid_labels_are_refused() -> TestResult {
        let dir = tempfile::tempdir()?;
        let (daemon, control, _tls) = fleet(dir.path()).await?;
        control
            .create_database_with_metadata(with_metadata("notes", "kept", Vec::new()))
            .await?;
        let before = locate(&control, "notes").await?;

        let too_many: Vec<Label> = (0..33).map(|n| label(&format!("k{n}"), "v")).collect();
        let descriptions = [
            "x".repeat(1_025),
            "ab\x1b[2J".to_owned(),
            "zero\u{200B}width".to_owned(),
            "tag\u{E0041}".to_owned(),
            "line\u{2028}separator".to_owned(),
        ];
        let label_sets = [
            vec![label("-bad", "x")],
            vec![label("fleetring.dev/x", "y")],
            too_many,
        ];
        let mut refusals = Vec::new();
        for description in &descriptions {
            let created = control
                .create_database_with_metadata(with_metadata("new", description, Vec::new()))
                .await;
            refusals.push(created.map(|_| ()));
            let updated = control
                .update_database(UpdateDatabaseRequest {
                    database: "notes".to_owned(),
                    description: Some(Description::SetDescription(description.clone())),
                    ..UpdateDatabaseRequest::default()
                })
                .await;
            refusals.push(updated.map(|_| ()));
        }
        for labels in &label_sets {
            let created = control
                .create_database_with_metadata(with_metadata("new", "", labels.clone()))
                .await;
            refusals.push(created.map(|_| ()));
            let updated = control
                .update_database(UpdateDatabaseRequest {
                    database: "notes".to_owned(),
                    set_labels: labels.clone(),
                    ..UpdateDatabaseRequest::default()
                })
                .await;
            refusals.push(updated.map(|_| ()));
        }
        for refused in refusals {
            let status = refused.expect_err("invalid metadata");
            assert_eq!(status.code(), Code::InvalidArgument, "{status:?}");
        }

        // An update that would leave a database with more than 32 labels.
        let full: Vec<Label> = (0..32).map(|n| label(&format!("k{n}"), "v")).collect();
        control
            .create_database_with_metadata(with_metadata("full", "", full))
            .await?;
        let over = control
            .update_database(UpdateDatabaseRequest {
                database: "full".to_owned(),
                set_labels: vec![label("one", "more")],
                ..UpdateDatabaseRequest::default()
            })
            .await
            .expect_err("33 labels");
        assert_eq!(over.code(), Code::InvalidArgument, "{over:?}");

        let missing = control
            .locate(LocateRequest {
                database: "new".to_owned(),
            })
            .await
            .expect_err("nothing created");
        assert_eq!(missing.code(), Code::NotFound);
        assert_eq!(locate(&control, "notes").await?, before, "nothing changed");
        let full = locate(&control, "full").await?.ok_or("no metadata")?;
        assert_eq!((full.labels.len(), full.version), (32, 1));
        daemon.run_until(async {}).await;
        Ok(())
    }

    /// A new database's name may hold no character a reader can't see: the format characters
    /// descriptions refuse, bidirectional controls and zero-width characters among them, and the
    /// line and paragraph separators. The control service refuses them, through every creation
    /// call; `apply` doesn't, so a log that already holds such a name replays (ADR-0041,
    /// amendment 5).
    #[tokio::test]
    async fn a_new_name_with_invisible_characters_is_refused() -> TestResult {
        let dir = tempfile::tempdir()?;
        let (daemon, control, _tls) = fleet(dir.path()).await?;
        for name in [
            "evil\u{202E}gnp",
            "zero\u{200B}width",
            "tag\u{E0041}",
            "line\u{2028}separator",
            "isolate\u{2066}x",
        ] {
            let plain = control
                .create_database(create_request(name, 1))
                .await
                .expect_err("an invisible character");
            let labelled = control
                .create_database_with_metadata(with_metadata(name, "", vec![label("a", "b")]))
                .await
                .expect_err("an invisible character");
            for refused in [plain, labelled] {
                assert_eq!(
                    refused.code(),
                    Code::InvalidArgument,
                    "{name:?}: {refused:?}"
                );
                assert!(
                    refused
                        .message()
                        .starts_with("invalid database name: it contains"),
                    "{refused:?}"
                );
            }
            let missing = control
                .locate(LocateRequest {
                    database: name.to_owned(),
                })
                .await
                .expect_err("never created");
            assert_eq!(missing.code(), Code::NotFound);
        }
        control
            .create_database(create_request("café-notes", 1))
            .await?;

        // The writer, below the service, still takes such a name, as a log written before the
        // check replays it.
        let handle = daemon.control().ok_or("no control")?;
        handle
            .submit(Action::CreateDatabase {
                name: "old\u{202E}name".parse()?,
                replication_factor: fleetring::ReplicationFactor::ONE,
                engine: fleetring_control::Engine::Sqlite,
                durability: None,
                metadata: None,
            })
            .await?;
        assert!(
            handle
                .state()
                .database(&"old\u{202E}name".parse()?)
                .is_some()
        );
        daemon.run_until(async {}).await;
        Ok(())
    }

    #[tokio::test]
    async fn metadata_survives_a_control_node_restart() -> TestResult {
        let dir = tempfile::tempdir()?;
        let tls = Tls::create(dir.path())?;
        let config = control_config(dir.path(), &tls)?;
        let daemon = Daemon::start(Config::from_toml(&config)?).await?;
        let address = daemon.control_address().ok_or("no control address")?;
        join(address, &tls, "data-1", this_build()).await?;
        let control = secure_client(address, &tls)?;
        control
            .create_database_with_metadata(with_metadata(
                "notes",
                "Memory.",
                vec![label("purpose", "memory")],
            ))
            .await?;
        control
            .update_database(UpdateDatabaseRequest {
                database: "notes".to_owned(),
                set_labels: vec![label("agent", "42")],
                ..UpdateDatabaseRequest::default()
            })
            .await?;
        let before = locate(&control, "notes").await?.ok_or("no metadata")?;
        daemon.run_until(async {}).await;

        let restarted = Daemon::start(Config::from_toml(&config)?).await?;
        let address = restarted.control_address().ok_or("no control address")?;
        let control = secure_client(address, &tls)?;
        assert_eq!(locate(&control, "notes").await?, Some(before));
        restarted.run_until(async {}).await;
        Ok(())
    }
}

/// Database expiry through the control service, its sweep, and its time (ADR-0041, point 4;
/// FR-070 phase 3). Each test runs a control node, its service, and, where it says so, its
/// failover timer and expiry sweep on this test's paused Tokio clock, with the service served
/// in memory, so that a minute passes at once and nothing else moves time.
mod expiry {
    use std::collections::BTreeMap;
    use std::error::Error;
    use std::io;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::time::Duration;

    use fleetring_control::{
        Action, ControlConfig, DatabaseName, Engine, Member, MetadataSpec, Millis, Rejection,
        Timestamp,
    };
    use fleetring_proto::v1::metadata_spec::Expiry as SpecExpiry;
    use fleetring_proto::v1::update_database_request::Expiry as ExpiryUpdate;
    use fleetring_proto::v1::{
        CreateDatabaseWithMetadataRequest, LocateRequest, MetadataSpec as ProtoSpec,
        UpdateDatabaseRequest,
    };
    use fleetringd::Network;
    use fleetringd::auth::{AuthLayer, Authorizer};
    use fleetringd::clock::SimClock;
    use fleetringd::control::{
        CommandLog, ControlClient, ControlHandle, ControlNode, ControlOptions, ControlService,
        Failover, FailoverSettings, MIN_EXPIRY, StorageIo, rejection_status,
    };
    use fleetringd::transport::control_server;
    use tokio_stream::StreamExt as _;
    use tonic::Code;

    use super::{TestResult, create_request};

    /// Where the tests' clocks start: 2026-11-05T00:00:00Z.
    const BASE: Timestamp = Timestamp::from_millis(1_793_836_800_000);

    /// Whether a plane runs its failover timer and expiry sweep.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum Timer {
        Running,
        Stopped,
    }

    /// A control node with one member running this build, its service served in memory, and,
    /// if started, its failover timer and expiry sweep.
    struct Plane {
        node: Option<ControlNode>,
        failover: Option<Failover>,
        client: ControlClient,
        path: PathBuf,
        _dir: tempfile::TempDir,
    }

    impl Plane {
        /// A plane whose control node and sweep read `clock`, and whose service reads
        /// `service_clock`.
        async fn start(
            clock: SimClock,
            service_clock: SimClock,
            timer: Timer,
        ) -> Result<Self, Box<dyn Error>> {
            let dir = tempfile::tempdir()?;
            let path = dir.path().join("control.db");
            let options =
                ControlOptions::new(ControlConfig::new()).with_storage_io(StorageIo::Inline);
            let node = ControlNode::open(&path, options, clock).await?;
            let handle = node.handle();
            handle
                .submit(Action::Join {
                    member: Member::new("data-1".parse()?)
                        .with_software(fleetringd::software::running()),
                })
                .await?;
            // Ticks only keep the state's time moving, which expiry doesn't need: few of them
            // keep the log, which syncs each, short.
            let failover = match timer {
                Timer::Running => Some(Failover::start(
                    handle.clone(),
                    Arc::new(clock),
                    Network::default(),
                    FailoverSettings::new().with_tick_interval(Duration::from_secs(10)),
                )),
                Timer::Stopped => None,
            };
            let service = ControlService::new(handle).with_clock(Arc::new(service_clock));
            Ok(Self {
                node: Some(node),
                failover,
                client: serve_in_memory(service),
                path,
                _dir: dir,
            })
        }

        /// A plane whose every part reads one clock, starting at [`BASE`].
        async fn on_one_clock(timer: Timer) -> Result<Self, Box<dyn Error>> {
            let clock = SimClock::new(BASE);
            Self::start(clock, clock, timer).await
        }

        fn handle(&self) -> Result<ControlHandle, Box<dyn Error>> {
            Ok(self.node.as_ref().ok_or("stopped")?.handle())
        }

        /// Stops the plane and returns every command its log holds, in log order.
        async fn stop(mut self) -> Result<Vec<(Timestamp, Action)>, Box<dyn Error>> {
            if let Some(failover) = self.failover.take() {
                failover.stop().await;
            }
            if let Some(node) = self.node.take() {
                node.shutdown().await;
            }
            let log = CommandLog::open(&self.path, ControlConfig::new())?;
            Ok(log
                .commands()?
                .into_iter()
                .map(|(_, command)| (command.at, command.action))
                .collect())
        }
    }

    /// Serves `service` over in-memory connections, behind a plaintext request boundary, and
    /// returns a client for it: no socket, so nothing waits on the operating system while time
    /// is paused.
    fn serve_in_memory(service: ControlService) -> ControlClient {
        let (connections, incoming) =
            tokio::sync::mpsc::unbounded_channel::<tokio::io::DuplexStream>();
        let incoming =
            tokio_stream::wrappers::UnboundedReceiverStream::new(incoming).map(Ok::<_, io::Error>);
        tokio::spawn(
            tonic::transport::Server::builder()
                .layer(AuthLayer::new(Authorizer::plaintext()))
                .add_service(control_server(service))
                .serve_with_incoming(incoming),
        );
        ControlClient::with_connector("memory:0", move |_| {
            let connections = connections.clone();
            Ok(tonic::transport::Endpoint::from_static("http://memory:0")
                .connect_with_connector_lazy(tower::service_fn(move |_| {
                    let (client, server) = tokio::io::duplex(1 << 16);
                    let sent = connections.send(server);
                    async move {
                        sent.map_err(|_| io::Error::other("the in-memory server stopped"))?;
                        Ok::<_, io::Error>(hyper_util::rt::TokioIo::new(client))
                    }
                })))
        })
    }

    fn expiring(database: &str, expiry: SpecExpiry) -> CreateDatabaseWithMetadataRequest {
        CreateDatabaseWithMetadataRequest {
            database: Some(create_request(database, 1)),
            metadata: Some(ProtoSpec {
                expiry: Some(expiry),
                ..ProtoSpec::default()
            }),
        }
    }

    fn millis(duration: Duration) -> u64 {
        u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
    }

    /// Creates `database` expiring `expires_in` from now through the service, and returns its
    /// expiry.
    async fn create_expiring_in(
        client: &ControlClient,
        database: &str,
        expires_in: Duration,
    ) -> Result<u64, Box<dyn Error>> {
        let created = client
            .create_database_with_metadata(expiring(
                database,
                SpecExpiry::ExpiresInMs(millis(expires_in)),
            ))
            .await?;
        Ok(created
            .placement
            .and_then(|placement| placement.metadata)
            .and_then(|metadata| metadata.expires_at_ms)
            .ok_or("no expiry")?)
    }

    /// Creates `count` databases expiring at `expires_at`, straight through the writer.
    async fn create_expiring_at(
        handle: &ControlHandle,
        count: u64,
        expires_at: Timestamp,
    ) -> TestResult {
        for k in 0..count {
            let created = handle
                .submit(Action::CreateDatabase {
                    name: format!("scratch-{k:03}").parse()?,
                    replication_factor: fleetring::ReplicationFactor::ONE,
                    engine: Engine::Sqlite,
                    durability: None,
                    metadata: Some(MetadataSpec {
                        expires_at: Some(expires_at),
                        ..MetadataSpec::default()
                    }),
                })
                .await?;
            assert!(created.outcome().is_ok(), "{:?}", created.outcome());
        }
        Ok(())
    }

    fn set_expiry(database: &str, expiry: ExpiryUpdate) -> UpdateDatabaseRequest {
        UpdateDatabaseRequest {
            database: database.to_owned(),
            expiry: Some(expiry),
            ..UpdateDatabaseRequest::default()
        }
    }

    /// The database's expiry, `Ok(None)` without one, or the status `Locate` answers.
    async fn expiry_of(
        client: &ControlClient,
        database: &str,
    ) -> Result<Option<u64>, tonic::Status> {
        let placement = client
            .locate(LocateRequest {
                database: database.to_owned(),
            })
            .await?
            .placement;
        Ok(placement
            .and_then(|placement| placement.metadata)
            .and_then(|metadata| metadata.expires_at_ms))
    }

    /// Every `ExpireDatabase` in `commands`, with its stamp.
    fn expirations(commands: &[(Timestamp, Action)]) -> Vec<(Timestamp, String)> {
        commands
            .iter()
            .filter_map(|(at, action)| match action {
                Action::ExpireDatabase { name } => Some((*at, name.to_string())),
                _ => None,
            })
            .collect()
    }

    #[tokio::test(start_paused = true)]
    async fn an_expired_database_is_dropped_by_the_control_node() -> TestResult {
        let plane = Plane::on_one_clock(Timer::Running).await?;
        let expires_at =
            create_expiring_in(&plane.client, "scratch", Duration::from_secs(61)).await?;
        assert_eq!(expires_at, BASE.as_millis() + 61_000);

        tokio::time::sleep(Duration::from_secs(60)).await;
        assert_eq!(
            expiry_of(&plane.client, "scratch").await?,
            Some(expires_at),
            "not yet"
        );
        tokio::time::sleep(Duration::from_secs(2)).await;
        let gone = expiry_of(&plane.client, "scratch")
            .await
            .expect_err("expired");
        assert_eq!(gone.code(), Code::NotFound, "{gone}");

        let expired = expirations(&plane.stop().await?);
        assert_eq!(expired.len(), 1, "{expired:?}");
        let (at, name) = &expired[0];
        assert_eq!(name, "scratch");
        assert!(at.as_millis() >= expires_at, "{at} < {expires_at}");
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn an_expiry_under_a_minute_is_refused() -> TestResult {
        let plane = Plane::on_one_clock(Timer::Stopped).await?;
        let minimum = Duration::from_millis(MIN_EXPIRY.get());
        let short = plane
            .client
            .create_database_with_metadata(expiring(
                "short",
                SpecExpiry::ExpiresInMs(millis(minimum) - 1),
            ))
            .await
            .expect_err("under a minute");
        assert_eq!(short.code(), Code::InvalidArgument, "{short}");
        assert_eq!(short.message(), "expiry must be at least 1m from now");
        let early = plane
            .client
            .create_database_with_metadata(expiring(
                "early",
                SpecExpiry::ExpiresAtMs(BASE.as_millis() + 30_000),
            ))
            .await
            .expect_err("an absolute time under a minute ahead");
        assert_eq!(early.code(), Code::InvalidArgument, "{early}");
        let state = plane.handle()?.state();
        assert!(state.database(&"short".parse()?).is_none());
        assert!(state.database(&"early".parse()?).is_none());

        // A minute exactly is enough, as a duration or as a time.
        let expires_at = create_expiring_in(&plane.client, "minute", minimum).await?;
        assert_eq!(expires_at, BASE.as_millis() + MIN_EXPIRY.get());
        let refused = plane
            .client
            .update_database(set_expiry(
                "minute",
                ExpiryUpdate::ExpiresInMs(millis(minimum) - 1),
            ))
            .await
            .expect_err("an update under a minute");
        assert_eq!(refused.code(), Code::InvalidArgument, "{refused}");
        plane
            .client
            .update_database(set_expiry(
                "minute",
                ExpiryUpdate::ExpiresAtMs(BASE.as_millis() + MIN_EXPIRY.get()),
            ))
            .await?;
        plane.stop().await?;
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn extending_an_expiry_keeps_the_database() -> TestResult {
        let plane = Plane::on_one_clock(Timer::Running).await?;
        create_expiring_in(&plane.client, "task", Duration::from_secs(61)).await?;
        tokio::time::sleep(Duration::from_secs(50)).await;
        let extended = plane
            .client
            .update_database(set_expiry("task", ExpiryUpdate::ExpiresInMs(300_000)))
            .await?
            .metadata
            .ok_or("no metadata")?;
        let new_expiry = BASE.as_millis() + 50_000 + 300_000;
        assert_eq!(extended.expires_at_ms, Some(new_expiry));
        assert_eq!(extended.version, 2);

        // Past the old expiry, it stays; past the new one, it goes.
        tokio::time::sleep(Duration::from_secs(60)).await;
        assert_eq!(expiry_of(&plane.client, "task").await?, Some(new_expiry));
        tokio::time::sleep(Duration::from_secs(250)).await;
        let gone = expiry_of(&plane.client, "task").await.expect_err("expired");
        assert_eq!(gone.code(), Code::NotFound, "{gone}");
        assert_eq!(expirations(&plane.stop().await?).len(), 1);
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn clearing_an_expiry_keeps_the_database() -> TestResult {
        let plane = Plane::on_one_clock(Timer::Running).await?;
        create_expiring_in(&plane.client, "memory", Duration::from_secs(61)).await?;
        let cleared = plane
            .client
            .update_database(set_expiry("memory", ExpiryUpdate::ClearExpiry(true)))
            .await?
            .metadata
            .ok_or("no metadata")?;
        assert_eq!(cleared.expires_at_ms, None);
        tokio::time::sleep(Duration::from_secs(600)).await;
        assert_eq!(expiry_of(&plane.client, "memory").await?, None);
        assert!(expirations(&plane.stop().await?).is_empty());
        Ok(())
    }

    /// An expiry extended through the service after the sweep looked wins: the sweep's
    /// command, judged at its own stamp, finds the database not expired and leaves it.
    #[tokio::test(start_paused = true)]
    async fn an_extension_wins_over_a_stale_sweep() -> TestResult {
        let plane = Plane::on_one_clock(Timer::Stopped).await?;
        let handle = plane.handle()?;
        create_expiring_in(&plane.client, "task", Duration::from_secs(61)).await?;
        tokio::time::sleep(Duration::from_secs(62)).await;
        // What a sweep reads now.
        let task: DatabaseName = "task".parse()?;
        let due = handle
            .state()
            .expired_databases(BASE.saturating_add(Millis::new(62_000)));
        assert_eq!(due, std::slice::from_ref(&task));
        // The agent extends it before the sweep's command is logged.
        plane
            .client
            .update_database(set_expiry("task", ExpiryUpdate::ExpiresInMs(3_600_000)))
            .await?;
        let committed = handle.submit(Action::ExpireDatabase { name: task }).await?;
        assert!(
            matches!(committed.outcome(), Err(Rejection::NotExpired { .. })),
            "{:?}",
            committed.outcome()
        );
        assert!(expiry_of(&plane.client, "task").await?.is_some());
        plane.stop().await?;
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn the_sweep_expires_at_most_64_databases_per_second() -> TestResult {
        const DATABASES: u64 = 150;
        let plane = Plane::on_one_clock(Timer::Running).await?;
        let handle = plane.handle()?;
        create_expiring_at(&handle, DATABASES, BASE.saturating_add(Millis::new(61_000))).await?;
        tokio::time::sleep(Duration::from_secs(70)).await;
        assert_eq!(handle.state().databases().count(), 0);

        // A sweep's commands share its stamp: at most 64 of them, so 150 take three sweeps, a
        // second apart.
        let expired = expirations(&plane.stop().await?);
        assert_eq!(expired.len(), usize::try_from(DATABASES)?);
        let mut per_sweep: BTreeMap<Timestamp, usize> = BTreeMap::new();
        for (at, _) in &expired {
            *per_sweep.entry(*at).or_default() += 1;
        }
        let counts: Vec<usize> = per_sweep.values().copied().collect();
        assert_eq!(counts, [64, 64, 22], "{per_sweep:?}");
        let stamps: Vec<u64> = per_sweep.keys().map(|at| at.as_millis()).collect();
        assert!(
            stamps.windows(2).all(|pair| pair[1] - pair[0] >= 1_000),
            "one sweep a second: {stamps:?}"
        );
        // In the order the state lists them: by expiry, then by name.
        let names: Vec<&String> = expired.iter().map(|(_, name)| name).collect();
        let mut sorted = names.clone();
        sorted.sort();
        assert_eq!(names, sorted);
        Ok(())
    }

    /// After a failover from a control node whose clock ran ahead, the control plane's time is
    /// ahead of this node's clock. A duration resolves against the later of the two, so it
    /// doesn't land behind the time `apply` judges by.
    #[tokio::test(start_paused = true)]
    async fn a_duration_resolves_against_the_control_planes_time_when_it_is_ahead() -> TestResult {
        let ahead = SimClock::new(BASE.saturating_add(Millis::new(300_000)));
        let behind = SimClock::new(BASE);
        let plane = Plane::start(ahead, behind, Timer::Stopped).await?;
        let control_time = plane.handle()?.state().now();
        assert_eq!(control_time.as_millis(), BASE.as_millis() + 300_000);

        let expires_at =
            create_expiring_in(&plane.client, "scratch", Duration::from_secs(120)).await?;
        assert_eq!(expires_at, control_time.as_millis() + 120_000);
        plane.stop().await?;
        Ok(())
    }

    /// The control plane's time moving past an expiry before it is logged is retryable: a retry
    /// resolves the duration against the newer time.
    #[test]
    fn expiry_passed_is_retryable() -> TestResult {
        let status = rejection_status(&Rejection::ExpiryPassed {
            database: "scratch".parse()?,
            expires_at: BASE,
        });
        assert_eq!(status.code(), Code::Unavailable);
        assert_eq!(
            status.message(),
            "the control plane's time moved past the requested expiry; retry"
        );
        Ok(())
    }
}

/// `node list` (ADR-0043): the members with their liveness, drain state and release, as the
/// real binary prints them.
mod node_list {
    use std::error::Error;
    use std::net::SocketAddr;
    use std::process::Command;
    use std::time::Duration;

    use fleetring_proto::v1::{DrainRequest, HeartbeatRequest, JoinRequest, Node};
    use fleetringd::{Config, Daemon};
    use serde_json::json;

    use super::{TestResult, join_request, node_client, reporting, secure_client};
    use crate::support::Tls;

    /// How long a member stays live without a heartbeat in these tests.
    const HEARTBEAT_TIMEOUT_MS: u64 = 400;

    /// A control daemon with three members: `node-a` (live, on 0.3.1, with every attribute),
    /// `node-b` (live, draining, on the 0.3.0 a member that reports no software runs), and
    /// `node-c` (down).
    struct Fleet {
        _dir: tempfile::TempDir,
        tls: Tls,
        daemon: Daemon,
        address: SocketAddr,
        heartbeats: Vec<tokio::task::JoinHandle<()>>,
    }

    impl Fleet {
        async fn start() -> Result<Self, Box<dyn Error>> {
            let dir = tempfile::tempdir()?;
            let tls = Tls::create(dir.path())?;
            let config = format!(
                "data_dir = {:?}\nlisten = \"127.0.0.1:0\"\nrole = \"control\"\n\n[node]\nid = \
                 \"control-1\"\n\n[control]\nheartbeat_timeout_ms = {HEARTBEAT_TIMEOUT_MS}\n{}",
                dir.path().join("data"),
                tls.node_table("control-1")?,
            );
            let daemon = Daemon::start(Config::from_toml(&config)?).await?;
            let address = daemon.control_address().ok_or("no control address")?;
            let control = secure_client(address, &tls)?;

            // node-c joins first and never beats again, so it is down by the time the others
            // have joined.
            node_client(address, &tls, "node-c")?
                .join(join_request("node-c"))
                .await?;
            tokio::time::sleep(Duration::from_millis(HEARTBEAT_TIMEOUT_MS * 2)).await;
            node_client(address, &tls, "node-a")?
                .join(JoinRequest {
                    node: Some(Node {
                        id: "node-a".into(),
                        zone: Some("zone-1".into()),
                        host: Some("host-1".into()),
                        weight: 2,
                        address: Some("10.0.0.1:7400".into()),
                    }),
                    software: Some(reporting("0.3.1")),
                    ..join_request("node-a")
                })
                .await?;
            node_client(address, &tls, "node-b")?
                .join(join_request("node-b"))
                .await?;
            control
                .drain(DrainRequest {
                    node_id: "node-b".into(),
                })
                .await?;

            let heartbeats = ["node-a", "node-b"]
                .into_iter()
                .map(|id| {
                    let client = node_client(address, &tls, id)?;
                    Ok(tokio::spawn(async move {
                        loop {
                            let _ = client
                                .heartbeat(HeartbeatRequest {
                                    node_id: id.into(),
                                    databases: Vec::new(),
                                    software: None,
                                })
                                .await;
                            tokio::time::sleep(Duration::from_millis(50)).await;
                        }
                    }))
                })
                .collect::<Result<Vec<_>, Box<dyn Error>>>()?;
            Ok(Self {
                _dir: dir,
                tls,
                daemon,
                address,
                heartbeats,
            })
        }

        /// Runs `fleetring node list` with `args`, and returns its stdout.
        async fn node_list(&self, args: &[&str]) -> Result<String, Box<dyn Error>> {
            let tls_dir = self.tls.dir().to_owned();
            let control = self.address.to_string();
            let args: Vec<String> = args.iter().map(|arg| (*arg).to_owned()).collect();
            let output = tokio::task::spawn_blocking(move || {
                Command::new(env!("CARGO_BIN_EXE_fleetring"))
                    .env("FLEETRING_TLS_DIR", tls_dir)
                    .args(["node", "list"])
                    .args(&args)
                    .args(["--control", &control])
                    .output()
            })
            .await??;
            let stderr = String::from_utf8_lossy(&output.stderr);
            assert!(output.status.success(), "{stderr}");
            Ok(String::from_utf8(output.stdout)?)
        }

        async fn stop(self) {
            for heartbeat in &self.heartbeats {
                heartbeat.abort();
            }
            self.daemon.run_until(std::future::ready(())).await;
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn node_list_shows_every_member_with_liveness_drain_and_release() -> TestResult {
        let fleet = Fleet::start().await?;
        let printed = fleet.node_list(&[]).await?;
        let rows: Vec<Vec<&str>> = printed
            .lines()
            .map(|line| line.split_whitespace().collect())
            .collect();
        assert_eq!(
            rows,
            [
                [
                    "NODE", "ADDRESS", "ZONE", "HOST", "WEIGHT", "LIVENESS", "DRAINING", "RELEASE"
                ]
                .to_vec(),
                [
                    "node-a",
                    "10.0.0.1:7400",
                    "zone-1",
                    "host-1",
                    "2",
                    "live",
                    "no",
                    "0.3.1"
                ]
                .to_vec(),
                ["node-b", "-", "-", "-", "1", "live", "yes", "0.3.0"].to_vec(),
                ["node-c", "-", "-", "-", "1", "down", "no", "0.3.0"].to_vec(),
            ],
            "{printed}"
        );
        fleet.stop().await;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn node_list_json_is_pinned() -> TestResult {
        let fleet = Fleet::start().await?;
        let printed = fleet.node_list(&["--json"]).await?;
        let parsed: serde_json::Value = serde_json::from_str(&printed)?;
        assert_eq!(
            parsed,
            json!({"nodes": [
                {
                    "node": "node-a",
                    "address": "10.0.0.1:7400",
                    "zone": "zone-1",
                    "host": "host-1",
                    "weight": 2,
                    "liveness": "live",
                    "draining": false,
                    "release": "0.3.1",
                },
                {
                    "node": "node-b",
                    "address": null,
                    "zone": null,
                    "host": null,
                    "weight": 1,
                    "liveness": "live",
                    "draining": true,
                    "release": "0.3.0",
                },
                {
                    "node": "node-c",
                    "address": null,
                    "zone": null,
                    "host": null,
                    "weight": 1,
                    "liveness": "down",
                    "draining": false,
                    "release": "0.3.0",
                },
            ]})
        );
        // `-o json` is the same output.
        assert_eq!(fleet.node_list(&["-o", "json"]).await?, printed);
        fleet.stop().await;
        Ok(())
    }
}
