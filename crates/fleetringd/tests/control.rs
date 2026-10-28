//! FR-028: the control node's log, writer, service, and client, over real TCP.

use std::error::Error;
use std::net::SocketAddr;
use std::num::NonZeroU64;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

use fleetring_control::{Action, ControlConfig, Member, Millis, State, Timestamp, apply};
use fleetring_proto::PROTOCOL_VERSION;
use fleetring_proto::v1::control_server::Control;
use fleetring_proto::v1::{
    AcquireLeaseRequest, CreateDatabaseRequest, CreateDatabaseResponse, DatabaseStatus, Engine,
    HeartbeatRequest, HeartbeatResponse, JoinRequest, JoinResponse, LeaseResponse, LocateRequest,
    LocateResponse, MembershipUpdate, Node, RenewLeaseRequest, Role, WatchMembershipRequest,
};
use fleetringd::clock::{Clock, SystemClock};
use fleetringd::control::{
    CommandLog, Committed, ControlClient, ControlHandle, ControlNode, ControlOptions,
    ControlService, LEADER_METADATA_KEY, LogError, OpenError, leader_hint, not_leader,
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

    async fn locate(&self, _: Request<LocateRequest>) -> Result<Response<LocateResponse>, Status> {
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
