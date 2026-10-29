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
    CreateDatabaseResponse, DatabaseStatus, DropDatabaseRequest, DropDatabaseResponse, Engine,
    HeartbeatRequest, HeartbeatResponse, JoinRequest, JoinResponse, LeaseResponse, LeaveRequest,
    LeaveResponse, ListDatabasesRequest, ListDatabasesResponse, LocateRequest, LocateResponse,
    MembershipUpdate, Node, PromotionResponse, RecordFencedRequest, RecordFencedResponse,
    RenewLeaseRequest, Role, StartPromotionRequest, WatchMembershipRequest,
};
use fleetringd::clock::{Clock, SystemClock};
use fleetringd::control::{
    CommandLog, Committed, ControlClient, ControlHandle, ControlNode, ControlOptions,
    ControlService, LEADER_METADATA_KEY, LogError, LogIndex, OpenError, StorageIo, leader_hint,
    not_leader,
};
use fleetringd::transport::{TransportConfig, control_server};
use fleetringd::{Config, ConfigError, Daemon, Role as DaemonRole};
use tokio_stream::Stream;
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
    }
}

fn client(address: SocketAddr) -> ControlClient {
    ControlClient::new(address.to_string(), TransportConfig::default())
}

/// Serves `node` on a random local port.
async fn serve(node: ControlHandle) -> Result<SocketAddr, Box<dyn Error>> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(control_server(ControlService::new(node)))
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
    assert_eq!(defaults.role, DaemonRole::Both);

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
    let text = format!(
        "data_dir = {:?}\nlisten = \"127.0.0.1:0\"\nrole = \"control\"\n\n[node]\nid = \"control-1\"\n",
        dir.path().join("data")
    );
    let daemon = Daemon::start(Config::from_toml(&text)?).await?;
    let address = daemon.control_address().ok_or("no control address")?;
    let control = client(address);

    for id in ["a", "b", "c"] {
        let joined = control.join(join_request(id)).await?;
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

    let beat = control
        .heartbeat(HeartbeatRequest {
            node_id: primary.clone(),
            databases: vec![DatabaseStatus {
                database: "tenant:acme".into(),
                role: Role::Follower.into(),
                epoch: 0,
                position: vec![0, 0, 0, 1],
            }],
        })
        .await?;
    assert_eq!(beat.membership_epoch, 3);

    let lease = control
        .acquire_lease(AcquireLeaseRequest {
            database: "tenant:acme".into(),
            node_id: primary.clone(),
        })
        .await?;
    assert_eq!(lease.epoch, 1);
    let renewed = control
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
    let stale = control
        .renew_lease(RenewLeaseRequest {
            database: "tenant:acme".into(),
            node_id: primary.clone(),
            epoch: 7,
        })
        .await
        .expect_err("stale epoch");
    assert_eq!(stale.code(), Code::FailedPrecondition, "{stale}");
    let invalid = control
        .join(join_request("Not A Label"))
        .await
        .expect_err("invalid id");
    assert_eq!(invalid.code(), Code::InvalidArgument, "{invalid}");
    let zero = control
        .create_database(CreateDatabaseRequest {
            database: "tenant:zero".into(),
            replication_factor: 0,
            engine: Engine::Unspecified.into(),
            acks: Acks::Unspecified.into(),
            min_replicas: 0,
        })
        .await
        .expect_err("zero replicas");
    assert_eq!(zero.code(), Code::InvalidArgument, "{zero}");

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
    let located = client(daemon.control_address().ok_or("no control address")?)
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

#[tokio::test]
async fn the_control_node_starts_a_promotion_for_an_expired_lease_and_a_node_leaves() -> TestResult
{
    let dir = tempfile::tempdir()?;
    let text = format!(
        "data_dir = {:?}\nlisten = \"127.0.0.1:0\"\nrole = \"control\"\n\n[node]\nid = \"control-1\"\n\n\
         [control]\nlease_duration_ms = 2000\nfailover_margin_ms = 0\ntick_interval_ms = 20\n",
        dir.path().join("data")
    );
    let daemon = Daemon::start(Config::from_toml(&text)?).await?;
    let control = client(daemon.control_address().ok_or("no control address")?);
    for id in ["a", "b", "c"] {
        control.join(join_request(id)).await?;
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
    control
        .acquire_lease(AcquireLeaseRequest {
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
            role: Role::Follower.into(),
            epoch: 1,
            position: vec![0, position],
        }],
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
    // The timer notices the expiry on its own and starts a promotion: no primary at epoch 2.
    let started = tokio::time::timeout(std::time::Duration::from_secs(60), async {
        loop {
            control.heartbeat(report("b", 2)).await?;
            control.heartbeat(report("c", 1)).await?;
            let placement = locate().await?;
            if placement.epoch == 2 {
                return Ok::<_, Status>(placement);
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    })
    .await
    .map_err(|_| "no promotion started")??;
    assert_eq!(started.primary, None, "no primary while the promotion runs");
    let busy = control
        .start_promotion(StartPromotionRequest {
            database: database.clone(),
        })
        .await
        .expect_err("already in progress");
    assert_eq!(busy.code(), Code::FailedPrecondition, "{busy}");

    let fence = |node: &str, position: u8| RecordFencedRequest {
        database: database.clone(),
        node_id: node.into(),
        epoch: 2,
        position: vec![0, position],
    };
    let first = control.record_fenced(fence("b", 2)).await?;
    assert_eq!((first.fenced, first.needed), (1, 2));
    let early = control
        .complete_promotion(CompletePromotionRequest {
            database: database.clone(),
            epoch: 2,
        })
        .await
        .expect_err("one fenced member is too few");
    assert_eq!(early.code(), Code::FailedPrecondition, "{early}");
    let second = control.record_fenced(fence("c", 1)).await?;
    assert_eq!((second.fenced, second.needed), (2, 2));
    let twice = control
        .record_fenced(fence("c", 1))
        .await
        .expect_err("fenced already");
    assert_eq!(twice.code(), Code::AlreadyExists, "{twice}");
    let completed = control
        .complete_promotion(CompletePromotionRequest {
            database: database.clone(),
            epoch: 2,
        })
        .await?;
    assert_eq!(completed.lease.map(|lease| lease.epoch), Some(2));
    assert_eq!(completed.adopted, vec![0, 2]);
    let promoted = locate().await?;
    assert_eq!(promoted.primary.as_deref(), Some("b"));
    assert_eq!(promoted.epoch, 2);

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
    let text = format!(
        "data_dir = {:?}\nlisten = \"127.0.0.1:0\"\nrole = \"control\"\n\n[node]\nid = \"control-1\"\n\n\
         [replication]\nacks = \"local\"\n",
        dir.path().join("data")
    );
    let config = Config::from_toml(&text)?;
    assert_eq!(config.replication.acks, fleetring_control::Acks::Local);
    assert_eq!(config.replication.min_replicas, None);
    let daemon = Daemon::start(config).await?;
    let control = client(daemon.control_address().ok_or("no control address")?);
    for id in ["a", "b", "c"] {
        control.join(join_request(id)).await?;
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
    let text = format!(
        "data_dir = {:?}\nlisten = \"127.0.0.1:0\"\nrole = \"control\"\n\n[node]\nid = \"control-1\"\n",
        dir.path().join("data")
    );
    let daemon = Daemon::start(Config::from_toml(&text)?).await?;
    let control = client(daemon.control_address().ok_or("no control address")?);
    control.join(join_request("a")).await?;
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
        role: Role::Primary.into(),
        epoch: 1,
        position: vec![0, 7],
    };
    // The known report lands, the unknown one is named, and the node stays live.
    let beat = control
        .heartbeat(HeartbeatRequest {
            node_id: "a".into(),
            databases: vec![status("ghost"), status("known")],
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
