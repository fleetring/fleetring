//! FR-028: the control node survives crashes, under turmoil (ADR-0020).
//!
//! The control node runs on a simulated host whose data directory is a real temporary
//! directory, so it survives crashes while the host's memory doesn't. A driver keeps sending
//! commands through the control client while the test crashes and restarts the host.
#![cfg(feature = "simulation")]

use std::error::Error;
use std::future::Future;
use std::num::NonZeroU64;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use fleetring_control::{ControlConfig, DatabaseName, State, Timestamp, apply};
use fleetring_proto::PROTOCOL_VERSION;
use fleetring_proto::v1::{CreateDatabaseRequest, Engine, HeartbeatRequest, JoinRequest, Node};
use fleetringd::clock::SimClock;
use fleetringd::control::{
    CommandLog, ControlClient, ControlNode, ControlOptions, ControlService, StorageIo,
};
use fleetringd::transport::{Backoff, control_server, sim};
use tonic::{Code, Status};

const CONTROL: &str = "control";
const PORT: u16 = 7400;
const NODES: usize = 12;
const DOWN_STEPS: u64 = 25;

type TestResult = Result<(), Box<dyn Error>>;

/// What the control node acknowledged to the driver.
#[derive(Debug, Default)]
struct Acknowledged {
    members: Vec<String>,
    databases: Vec<String>,
    heartbeats: usize,
}

fn control_host(sim: &mut turmoil::Sim<'_>, path: PathBuf) {
    sim.host(CONTROL, move || {
        let path = path.clone();
        async move {
            let elapsed = turmoil::sim_elapsed().unwrap_or_default();
            let base = Timestamp::from_millis(u64::try_from(elapsed.as_millis())?);
            let options = ControlOptions::new(ControlConfig::new())
                .with_snapshot_every(NonZeroU64::new(5).ok_or("zero")?)
                .with_storage_io(StorageIo::Inline);
            let node = ControlNode::open(path, options, SimClock::new(base)).await?;
            let listener = turmoil::net::TcpListener::bind(("0.0.0.0", PORT)).await?;
            tonic::transport::Server::builder()
                .add_service(control_server(ControlService::new(node.handle())))
                .serve_with_incoming(sim::incoming(listener))
                .await?;
            Ok(())
        }
    });
}

/// Repeats `call` until the control node answers. `ALREADY_EXISTS` counts as an answer: an
/// earlier attempt was applied but its reply was lost to a crash.
async fn until_answered<T, F, Fut>(mut call: F) -> Result<Option<T>, Status>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, Status>>,
{
    loop {
        match tokio::time::timeout(Duration::from_millis(500), call()).await {
            Ok(Ok(value)) => return Ok(Some(value)),
            Ok(Err(status)) if status.code() == Code::AlreadyExists => return Ok(None),
            Ok(Err(status))
                if matches!(
                    status.code(),
                    Code::InvalidArgument | Code::NotFound | Code::FailedPrecondition
                ) =>
            {
                return Err(status);
            }
            Ok(Err(_)) | Err(_) => tokio::time::sleep(Duration::from_millis(20)).await,
        }
    }
}

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

fn driver(sim: &mut turmoil::Sim<'_>, acknowledged: Arc<Mutex<Acknowledged>>) {
    sim.client("driver", async move {
        let backoff = Backoff {
            initial: Duration::from_millis(10),
            max: Duration::from_millis(100),
            attempts: 3,
        };
        let control = ControlClient::with_connector(format!("{CONTROL}:{PORT}"), sim::channel_to)
            .with_backoff(backoff);
        for k in 0..NODES {
            let id = format!("node-{k}");
            until_answered(|| control.join(join_request(&id))).await?;
            acknowledged.lock().expect("lock").members.push(id.clone());

            let database = format!("tenant:{k}");
            until_answered(|| {
                control.create_database(CreateDatabaseRequest {
                    database: database.clone(),
                    replication_factor: 1,
                    engine: Engine::Sqlite.into(),
                })
            })
            .await?;
            acknowledged.lock().expect("lock").databases.push(database);

            until_answered(|| {
                control.heartbeat(HeartbeatRequest {
                    node_id: id.clone(),
                    databases: Vec::new(),
                })
            })
            .await?;
            acknowledged.lock().expect("lock").heartbeats += 1;
        }
        Ok(())
    });
}

/// Checks the log on disk: it replays to the state recovery builds, and that state holds every
/// command the node acknowledged. Returns how many commands the log holds.
fn check(path: &Path, acknowledged: &Mutex<Acknowledged>) -> Result<usize, Box<dyn Error>> {
    if !path.exists() {
        return Ok(0);
    }
    let log = CommandLog::open(path, ControlConfig::new())?;
    let recovered = log.recover()?;
    let commands = log.commands()?;
    let mut replayed = State::new(log.genesis());
    for (_, command) in &commands {
        let _outcome = apply(&mut replayed, command.clone());
    }
    assert_eq!(
        recovered.state, replayed,
        "recovery must equal a fresh replay"
    );
    let acknowledged = acknowledged.lock().expect("lock");
    for id in &acknowledged.members {
        assert!(
            recovered.state.member(&id.parse()?).is_some(),
            "acknowledged member {id} is lost"
        );
    }
    for name in &acknowledged.databases {
        let name: DatabaseName = name.parse()?;
        assert!(
            recovered.state.database(&name).is_some(),
            "acknowledged database {name} is lost"
        );
    }
    let logged_heartbeats = commands
        .iter()
        .filter(|(_, command)| {
            matches!(command.action, fleetring_control::Action::Heartbeat { .. })
        })
        .count();
    assert!(logged_heartbeats >= acknowledged.heartbeats);
    Ok(commands.len())
}

#[test]
fn acknowledged_commands_survive_crashes_and_replay_to_the_same_state() -> TestResult {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("control.db");
    let acknowledged = Arc::new(Mutex::new(Acknowledged::default()));
    let mut sim = turmoil::Builder::new()
        .simulation_duration(Duration::from_secs(300))
        .rng_seed(28)
        .build();
    control_host(&mut sim, path.clone());
    driver(&mut sim, Arc::clone(&acknowledged));

    let mut step: u64 = 0;
    let mut crashes: u64 = 0;
    let mut next_crash: u64 = 30;
    let mut restart_at: Option<u64> = None;
    loop {
        if sim.step()? {
            break;
        }
        step += 1;
        match restart_at {
            Some(at) if step >= at => {
                sim.bounce(CONTROL);
                restart_at = None;
                // Vary the uptime so crashes land at different points of the command cycle.
                next_crash = step + 15 + (crashes * 37) % 61;
            }
            None if step >= next_crash => {
                sim.crash(CONTROL);
                crashes += 1;
                check(&path, &acknowledged)?;
                restart_at = Some(step + DOWN_STEPS);
            }
            _ => {}
        }
    }
    drop(sim);

    let logged = check(&path, &acknowledged)?;
    let acknowledged = acknowledged.lock().expect("lock");
    assert_eq!(acknowledged.members.len(), NODES);
    assert_eq!(acknowledged.databases.len(), NODES);
    assert!(crashes >= 10, "only {crashes} crashes");
    assert!(logged >= 3 * NODES, "only {logged} commands logged");
    Ok(())
}
