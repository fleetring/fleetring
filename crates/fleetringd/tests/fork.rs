//! FR-085: forks on real daemons (ADR-0046, points 4, 6, 7, and 11). A control daemon and three
//! data daemons run in process over TLS; a fork's replicas are placed apart from its source's,
//! so its seeder fetches the source over the network. The fault hooks hold a node's fork work
//! where a test crashes the node or changes the fleet around it.

use std::collections::BTreeSet;
use std::error::Error;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use fleetring::{NodeId, ReplicationFactor};
use fleetring_control::fork::OperationId;
use fleetring_control::{DatabaseName, Member, membership_ring};
use fleetring_proto::v1::{self as proto, ForkOperationStatus, SnapshotRequest};
use fleetringd::auth::refused_binding;
use fleetringd::data::{Client, DatabaseSpec, ForkSpec};
use fleetringd::engine::{Statement, Value};
use fleetringd::replication::{FORK_NOT_READY, FaultPoint, ForkFaults};
use fleetringd::transport::{database_client, replication_client};
use fleetringd::{Config, Daemon, Environment, Network};
use tonic::Code;

use crate::support::Tls;

mod support;

type TestResult = Result<(), Box<dyn Error>>;

/// Guards against a hang only; nothing here asserts how long anything takes.
const HANG_GUARD: Duration = Duration::from_secs(90);

const DATA: [&str; 3] = ["data-1", "data-2", "data-3"];

/// Timings short enough for a failover to finish quickly over loopback.
const FAST_LEASES: &str = "lease_duration_ms = 800\nfailover_margin_ms = 100\n\
                           heartbeat_timeout_ms = 500\ntick_interval_ms = 20\n";

/// The free space every data daemon believes its volume has, unless a test says otherwise.
const ROOMY: u64 = 1 << 40;

/// Fork limits short enough for the tests: a seeder that fails is retried at once.
const FORK: &str = "\n[fork]\ntransfer_backoff_ms = 50\n";

/// A control daemon and three data daemons, each data daemon with its fault hooks.
struct Fleet {
    control: Daemon,
    data: Vec<Daemon>,
    faults: Vec<ForkFaults>,
    tls: Tls,
}

/// What a fleet is started with.
#[derive(Default)]
struct Settings {
    /// Appended to every daemon's `[control]` table.
    control: &'static str,
    /// Appended to every data daemon's configuration, after its `[fork]` table.
    data: String,
    /// Each data daemon's `[membership] on_shutdown`: whether it stays a member when stopped.
    stay: bool,
    /// Appended to every data daemon's `[replication]` table.
    replication: &'static str,
}

fn config(
    dir: &Path,
    tls: &Tls,
    id: &str,
    role: &str,
    control: Option<SocketAddr>,
    settings: &Settings,
) -> Result<String, Box<dyn Error>> {
    let control_table = control.map_or_else(String::new, |address| {
        format!("\n[control]\naddress = \"{address}\"\n{}", settings.control)
    });
    let control_table = if role == "control" {
        format!("\n[control]\n{}", settings.control)
    } else {
        control_table
    };
    let replication = if role == "data" {
        format!(
            "\n[replication]\nack_timeout_ms = 20000\n{}",
            settings.replication
        )
    } else {
        String::new()
    };
    let stay = if settings.stay {
        "on_shutdown = \"stay\"\n"
    } else {
        ""
    };
    let extra = if role == "data" {
        settings.data.as_str()
    } else {
        ""
    };
    let tls = tls.node_table(id)?;
    Ok(format!(
        "data_dir = {:?}\nlisten = \"127.0.0.1:0\"\nrole = \"{role}\"\n\n[node]\nid = \"{id}\"\n\
         {replication}\n[membership]\nheartbeat_interval_ms = 100\n{stay}{tls}{control_table}{FORK}{extra}",
        dir.join(id)
    ))
}

/// Prints the daemons' logs when `FORK_TEST_LOG` names a filter, for debugging a test.
fn logs() {
    if let Ok(filter) = std::env::var("FORK_TEST_LOG") {
        let _ = tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::new(filter))
            .with_test_writer()
            .try_init();
    }
}

impl Fleet {
    async fn start(dir: &Path, settings: Settings) -> Result<Self, Box<dyn Error>> {
        logs();
        let tls = Tls::create(dir)?;
        let control_text = config(dir, &tls, "control", "control", None, &settings)?;
        let control = Daemon::start(Config::from_toml(&control_text)?).await?;
        let address = control.control_address().ok_or("no control address")?;
        let mut data = Vec::new();
        let mut faults = Vec::new();
        for id in DATA {
            let text = config(dir, &tls, id, "data", Some(address), &settings)?;
            let hooks = ForkFaults::new();
            // Room for any fork here, whatever the machine running the tests has free: the
            // disk tests set their own.
            hooks.set_disk(ROOMY, 2 * ROOMY);
            let environment = Environment::production().with_fork_faults(hooks.clone());
            data.push(Daemon::start_with(Config::from_toml(&text)?, environment).await?);
            faults.push(hooks);
        }
        let fleet = Self {
            control,
            data,
            faults,
            tls,
        };
        fleet.converge().await?;
        Ok(fleet)
    }

    /// Waits until every data daemon publishes the control plane's ring with every member live.
    async fn converge(&self) -> TestResult {
        let handle = self.control.control().ok_or("no control handle")?;
        let mut states = handle.watch();
        let count = self.data.len();
        let state = tokio::time::timeout(
            HANG_GUARD,
            states.wait_for(|state| {
                state.members().len() == count
                    && state.members().all(|member| {
                        state.liveness(member.id(), state.now())
                            == Some(fleetring_control::Liveness::Live)
                    })
            }),
        )
        .await??
        .clone();
        let fingerprint = state.ring()?.fingerprint();
        for daemon in &self.data {
            let membership = daemon.membership().ok_or("no membership")?;
            tokio::time::timeout(
                HANG_GUARD,
                membership.wait_for(|view| view.fingerprint() == Some(fingerprint)),
            )
            .await?
            .ok_or("membership stopped")?;
        }
        Ok(())
    }

    fn client(&self) -> Result<Client, Box<dyn Error>> {
        let address = self
            .control
            .control_address()
            .ok_or("no control address")?
            .to_string();
        Ok(Client::new(address, client_network(&self.tls)?))
    }

    fn index(&self, id: &NodeId) -> Result<usize, Box<dyn Error>> {
        self.data
            .iter()
            .position(|daemon| daemon.config().node.id == *id)
            .ok_or_else(|| format!("no data daemon {id}").into())
    }

    fn daemon(&self, id: &NodeId) -> Result<&Daemon, Box<dyn Error>> {
        Ok(&self.data[self.index(id)?])
    }

    fn faults(&self, id: &NodeId) -> Result<&ForkFaults, Box<dyn Error>> {
        Ok(&self.faults[self.index(id)?])
    }

    /// The replica set the ring gives `name` at `factor`.
    fn ring_replicas(&self, name: &str, factor: u8) -> Result<Vec<NodeId>, Box<dyn Error>> {
        let ring = self.data[0]
            .membership()
            .ok_or("no membership")?
            .ring()
            .ok_or("no ring")?;
        let factor = ReplicationFactor::new(factor).ok_or("zero")?;
        Ok(ring.replicas_n(name, factor).to_ids())
    }

    /// A name for a fork of `factor` replicas that the ring places apart from `apart`.
    fn name_apart(
        &self,
        prefix: &str,
        factor: u8,
        apart: &NodeId,
    ) -> Result<DatabaseName, Box<dyn Error>> {
        for n in 0..1_000 {
            let candidate = format!("{prefix}-{n}");
            if !self.ring_replicas(&candidate, factor)?.contains(apart) {
                return Ok(candidate.parse()?);
            }
        }
        Err("no name the ring places apart".into())
    }

    /// Stops data daemon `id` as a crash would leave its fleet: it stays a member, hands no
    /// lease over, and its fork work stops wherever it was.
    async fn crash(&mut self, id: &NodeId) -> TestResult {
        let index = self.index(id)?;
        let daemon = self.data.remove(index);
        let hooks = self.faults.remove(index);
        daemon.run_until(async {}).await;
        drop(hooks);
        Ok(())
    }

    async fn stop(self) {
        for daemon in self.data {
            daemon.run_until(async {}).await;
        }
        self.control.run_until(async {}).await;
    }
}

/// The network the tests' clients use, as the principal `tester`.
fn client_network(tls: &Tls) -> Result<Network, Box<dyn Error>> {
    tls.client_network_with(
        "tester",
        fleetringd::transport::TransportConfig {
            request_timeout: Duration::from_secs(60),
            ..fleetringd::transport::TransportConfig::default()
        },
    )
}

fn name(text: &str) -> Result<DatabaseName, Box<dyn Error>> {
    Ok(text.parse()?)
}

fn factor(n: u8) -> Result<ReplicationFactor, Box<dyn Error>> {
    Ok(ReplicationFactor::new(n).ok_or("zero")?)
}

/// A fresh operation ID, its bytes from `seed`.
fn operation(seed: u8) -> OperationId {
    let mut bytes = [seed; 16];
    bytes[0] &= 0x7f;
    OperationId::from_bytes(bytes)
}

async fn wait_until(what: &str, mut condition: impl AsyncFnMut() -> bool) -> TestResult {
    let waited = tokio::time::timeout(HANG_GUARD, async {
        while !condition().await {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await;
    waited.map_err(|_| format!("timed out waiting until {what}").into())
}

/// Waits until operation `id` reaches `state`, and returns its status.
async fn await_state(
    client: &Client,
    id: OperationId,
    state: proto::ForkState,
) -> Result<ForkOperationStatus, Box<dyn Error>> {
    let mut last = None;
    let reached = tokio::time::timeout(HANG_GUARD, async {
        loop {
            if let Ok(status) = client.fork_operation(id).await {
                if status.state() == state {
                    return status;
                }
                last = Some(status);
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await;
    reached.map_err(|_| format!("the fork never reached {state:?}; last {last:?}").into())
}

/// Creates `source` with `replicas` replicas and a table of `rows` rows, and returns its primary.
async fn create_source(
    client: &Client,
    source: &DatabaseName,
    replicas: u8,
    rows: i64,
) -> Result<NodeId, Box<dyn Error>> {
    let location = client.create_database(source, factor(replicas)?).await?;
    client
        .execute(
            source,
            Statement::new("CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)"),
        )
        .await?;
    for n in 0..rows {
        client
            .execute(
                source,
                Statement::with_params(
                    "INSERT INTO notes (body) VALUES (?)",
                    [Value::from(format!("note {n}"))],
                ),
            )
            .await?;
    }
    Ok(location.primary.ok_or("no primary")?)
}

/// Every row of `notes` in `database`.
async fn notes(
    client: &Client,
    database: &DatabaseName,
) -> Result<Vec<Vec<Value>>, Box<dyn Error>> {
    Ok(client
        .query(
            database,
            Statement::new("SELECT id, body FROM notes ORDER BY id"),
        )
        .await?
        .rows)
}

/// A fork of `source` named `target` with `replicas` replicas, under `id`.
fn spec(id: OperationId, replicas: u8) -> Result<ForkSpec, Box<dyn Error>> {
    let mut spec = ForkSpec::new(id);
    spec.database = DatabaseSpec::new(factor(replicas)?);
    Ok(spec)
}

/// The database directory of `database` on data daemon `id`.
fn database_dir(dir: &Path, id: &NodeId, database: &DatabaseName) -> PathBuf {
    dir.join(id.as_str())
        .join("databases")
        .join(database.as_str())
}

/// The staged copies of `database` on data daemon `id`, as their directories.
fn staged_dirs(dir: &Path, id: &NodeId, database: &DatabaseName) -> Vec<PathBuf> {
    let root = dir.join(id.as_str()).join("staged").join(database.as_str());
    let mut found = Vec::new();
    for identity in std::fs::read_dir(&root).into_iter().flatten().flatten() {
        for attempt in std::fs::read_dir(identity.path())
            .into_iter()
            .flatten()
            .flatten()
        {
            found.push(attempt.path());
        }
    }
    found
}

/// What the reserved tables of a copy of a database hold: the change record's rows, the sink
/// marks, and the fork markers, each as `(incarnation, operation)`.
struct Reserved {
    changes: i64,
    marks: i64,
    markers: Vec<(i64, Vec<u8>)>,
}

/// Reads `database`'s reserved tables in a scratch copy of its files on data daemon `id`.
fn reserved(dir: &Path, id: &NodeId, database: &DatabaseName) -> Result<Reserved, Box<dyn Error>> {
    let source = database_dir(dir, id, database);
    let scratch = tempfile::tempdir()?;
    for file in ["data.db", "data.db-wal"] {
        let from = source.join(file);
        if from.try_exists()? {
            std::fs::copy(&from, scratch.path().join(file))?;
        }
    }
    let connection = rusqlite::Connection::open(scratch.path().join("data.db"))?;
    let count = |table: &str, filter: &str| -> Result<i64, Box<dyn Error>> {
        let exists: i64 = connection.query_row(
            "SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name = ?1",
            [table],
            |row| row.get(0),
        )?;
        if exists == 0 {
            return Ok(0);
        }
        Ok(connection.query_row(
            &format!("SELECT count(*) FROM {table} {filter}"),
            [],
            |row| row.get(0),
        )?)
    };
    // The change record's floor says where it starts; it records no change.
    let changes = count("_fleetring_cdc", "WHERE table_name != '_fleetring_floor'")?;
    let marks = count("_fleetring_sinks", "")?;
    let markers = if count("_fleetring_fork", "")? > 0 {
        let mut statement =
            connection.prepare("SELECT fork_incarnation, operation FROM _fleetring_fork")?;
        statement
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
            .collect::<Result<Vec<_>, _>>()?
    } else {
        Vec::new()
    };
    Ok(Reserved {
        changes,
        marks,
        markers,
    })
}

/// A logical sink on `src` that delivers, so the source holds sink marks, and one that can't,
/// so it keeps its change record: what a fork inherits and its reset clears.
fn source_sinks(dir: &Path) -> String {
    format!(
        "\n[[changes.sink]]\nname = \"log\"\nkind = \"file\"\nformat = \"logical\"\n\
         databases = [\"src\"]\npath = {:?}\n\
         \n[[changes.sink]]\nname = \"stuck\"\nkind = \"webhook\"\nformat = \"logical\"\n\
         databases = [\"src\"]\nurl = \"http://127.0.0.1:9/changes\"\nallow_http = true\n",
        dir.join("sink")
    )
}

/// A fork becomes ready: its rows are the source's at the position it was seeded at, the
/// source's chain hash there matches, and its change record and sink marks are empty, its
/// marker naming its identity (ADR-0046, points 4 and 6; #135).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_fork_becomes_ready_with_the_sources_rows_and_nothing_it_inherited() -> TestResult {
    let dir = tempfile::tempdir()?;
    let fleet = Fleet::start(
        dir.path(),
        Settings {
            data: source_sinks(dir.path()),
            ..Settings::default()
        },
    )
    .await?;
    let client = fleet.client()?;
    let source = name("src")?;
    let source_primary = create_source(&client, &source, 1, 20).await?;
    // The source records its changes and delivers some: the fork inherits both.
    wait_until("the source's sink marks it", async || {
        reserved(dir.path(), &source_primary, &source).is_ok_and(|held| held.marks > 0)
    })
    .await?;
    let inherited = reserved(dir.path(), &source_primary, &source)?;
    assert!(
        inherited.changes > 0,
        "the stuck sink keeps the change record"
    );

    let target = fleet.name_apart("dst", 2, &source_primary)?;
    let id = operation(1);
    client
        .fork_database(&source, &target, &spec(id, 2)?)
        .await?;
    let ready = await_state(&client, id, proto::ForkState::Ready).await?;
    let at = ready
        .position
        .clone()
        .ok_or("a ready fork has a position")?;
    let chain_hash = ready
        .chain_hash
        .clone()
        .ok_or("a ready fork has a chain hash")?;

    assert_eq!(
        notes(&client, &target).await?,
        notes(&client, &source).await?
    );
    let source_address = fleet.daemon(&source_primary)?.listen_address().to_string();
    let verified = client
        .verify(
            &source_address,
            &source,
            Some(&fleetringd::engine::Position::from(at)),
        )
        .await?;
    assert_eq!(
        verified.chain_hash.as_bytes().as_slice(),
        chain_hash.as_slice()
    );

    let located = client.locate(&target).await?;
    let fork_primary = located.primary.clone().ok_or("the fork has a primary")?;
    assert_ne!(fork_primary, source_primary, "placed apart");
    let held = reserved(dir.path(), &fork_primary, &target)?;
    assert_eq!(held.changes, 0, "the fork's change record is empty");
    assert_eq!(held.marks, 0, "the fork's sink marks are empty");
    assert_eq!(
        held.markers,
        vec![(ready.incarnation.cast_signed(), id.to_bytes().to_vec())],
        "the marker names the fork"
    );
    // A ready fork takes writes as any database does.
    client
        .execute(
            &target,
            Statement::new("INSERT INTO notes (body) VALUES ('mine')"),
        )
        .await?;
    assert_eq!(
        notes(&client, &source).await?.len() + 1,
        notes(&client, &target).await?.len(),
        "writes to the fork never reach the source"
    );
    fleet.stop().await;
    Ok(())
}

/// Waits until some data daemon's work for operation `id` arrives at `point`, held there, and
/// returns that daemon's node.
async fn await_held(fleet: &Fleet, point: FaultPoint) -> Result<NodeId, Box<dyn Error>> {
    let mut found = None;
    wait_until("a node's fork work reaches the held point", async || {
        found = fleet
            .data
            .iter()
            .zip(&fleet.faults)
            .find(|(_, faults)| faults.arrivals(point) > 0)
            .map(|(daemon, _)| daemon.config().node.id.clone());
        found.is_some()
    })
    .await?;
    Ok(found.ok_or("no node arrived")?)
}

/// Whether `status` refuses a call to a fork that isn't ready: `UNAVAILABLE`, with a
/// retry-after.
fn not_ready(status: &tonic::Status) -> bool {
    status.code() == Code::Unavailable
        && status
            .metadata()
            .get(fleetringd::replication::RETRY_AFTER_KEY)
            .is_some()
        && status.message().contains(FORK_NOT_READY)
}

/// Between `CompleteFork` and the reset's commit, which a fault hook holds, every data call,
/// `WatchChanges`, and a configured sink against the fork are refused or don't run (ADR-0046,
/// point 6.2; #135).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_fork_serves_nothing_until_its_reset_commits() -> TestResult {
    let dir = tempfile::tempdir()?;
    let sink = dir.path().join("sink");
    let fleet = Fleet::start(
        dir.path(),
        Settings {
            data: format!(
                "\n[[changes.sink]]\nname = \"log\"\nkind = \"file\"\nformat = \"logical\"\n\
                 databases = [\"*\"]\npath = {sink:?}\n"
            ),
            ..Settings::default()
        },
    )
    .await?;
    let client = fleet.client()?;
    let source = name("src")?;
    let source_primary = create_source(&client, &source, 1, 5).await?;
    let target = fleet.name_apart("dst", 2, &source_primary)?;
    for faults in &fleet.faults {
        faults.hold(FaultPoint::BeforeReset);
    }
    let id = operation(2);
    client
        .fork_database(&source, &target, &spec(id, 2)?)
        .await?;
    let primary = await_held(&fleet, FaultPoint::BeforeReset).await?;
    let seeded = client.fork_operation(id).await?;
    assert_eq!(seeded.state(), proto::ForkState::Seeded);

    // The primary refuses every data call, and `WatchChanges`, with a retry-after.
    let address = fleet.daemon(&primary)?.listen_address().to_string();
    let mut raw = database_client(client_network(&fleet.tls)?.channel(&address)?);
    let executed = raw
        .execute(proto::ExecuteRequest {
            database: target.to_string(),
            sql: "INSERT INTO notes (body) VALUES ('early')".to_owned(),
            params: Vec::new(),
        })
        .await
        .expect_err("a write to a fork that isn't ready");
    assert!(not_ready(&executed), "{executed:?}");
    let queried = raw
        .query(proto::QueryRequest {
            database: target.to_string(),
            sql: "SELECT count(*) FROM notes".to_owned(),
            ..proto::QueryRequest::default()
        })
        .await
        .expect_err("a read of a fork that isn't ready");
    assert!(not_ready(&queried), "{queried:?}");
    let watched = match raw
        .watch_changes(proto::WatchChangesRequest {
            database: target.to_string(),
            after: None,
        })
        .await
    {
        Err(status) => status,
        Ok(stream) => stream
            .into_inner()
            .message()
            .await
            .expect_err("watching a fork that isn't ready"),
    };
    assert!(not_ready(&watched), "{watched:?}");
    // No change stream runs on it: the configured sink has no pump for it.
    let streams = fleet
        .daemon(&primary)?
        .change_streams()
        .ok_or("no change streams")?;
    assert!(
        streams
            .status()
            .iter()
            .all(|status| status.database != target),
        "no sink runs on a fork that isn't ready"
    );

    for faults in &fleet.faults {
        faults.release(FaultPoint::BeforeReset);
    }
    await_state(&client, id, proto::ForkState::Ready).await?;
    assert_eq!(notes(&client, &target).await?.len(), 5);
    wait_until("the sink runs on the ready fork", async || {
        streams
            .status()
            .iter()
            .any(|status| status.database == target)
    })
    .await?;
    fleet.stop().await;
    Ok(())
}

/// A seeding `Snapshot` request with `fork`'s identity, from `seeder`'s point of view.
fn seeding_request(
    database: &DatabaseName,
    fork: &ForkOperationStatus,
    attempt: u32,
    staged: bool,
) -> SnapshotRequest {
    SnapshotRequest {
        database: database.to_string(),
        epoch: 0,
        fork: Some(fork.database.clone()),
        fork_incarnation: fork.incarnation,
        operation: fork.operation_id.clone(),
        fork_attempt: attempt,
        source_incarnation: fork.source_incarnation,
        staged,
    }
}

/// Sends `request` to the node at `address` as `network`'s identity, and returns how it was
/// answered: its first message, or the status it was refused with.
async fn ask(
    network: &Network,
    address: &str,
    request: SnapshotRequest,
) -> Result<Result<proto::SnapshotChunk, tonic::Status>, Box<dyn Error>> {
    let mut client = replication_client(network.channel(address)?);
    Ok(match client.snapshot(request).await {
        Ok(stream) => match stream.into_inner().message().await {
            Ok(Some(chunk)) => Ok(chunk),
            Ok(None) => Err(tonic::Status::data_loss("an empty stream")),
            Err(status) => Err(status),
        },
        Err(status) => Err(status),
    })
}

/// Whether `answer` is a `seeding` binding refusal.
fn refused_seeding(answer: &Result<proto::SnapshotChunk, tonic::Status>) -> bool {
    matches!(answer, Err(status) if refused_binding(status) == Some("seeding"))
}

/// The seeding binding refuses a request from a non-seeder, for a stale attempt, with a
/// mismatched source or fork incarnation or operation, naming a source that isn't the fork's,
/// to a node that isn't the seeder for a staged copy, and from a client certificate (ADR-0046,
/// points 4.1 and 4.4; #135).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_seeding_binding_refuses_every_mismatch() -> TestResult {
    let dir = tempfile::tempdir()?;
    let fleet = Fleet::start(dir.path(), Settings::default()).await?;
    let client = fleet.client()?;
    let source = name("src")?;
    let source_primary = create_source(&client, &source, 1, 5).await?;
    let other = name("other")?;
    client.create_database(&other, factor(1)?).await?;
    let target = fleet.name_apart("dst", 2, &source_primary)?;
    // Every copy stages but none is reported, so the fork stays seeding attempt 1.
    for faults in &fleet.faults {
        faults.hold(FaultPoint::Staged);
    }
    let id = operation(3);
    client
        .fork_database(&source, &target, &spec(id, 2)?)
        .await?;
    let seeder = await_held(&fleet, FaultPoint::Staged).await?;
    let status = client.fork_operation(id).await?;
    assert_eq!(status.state(), proto::ForkState::Seeding);
    let placement = status.placement.clone().ok_or("no placement")?;
    let peer: NodeId = placement
        .replicas
        .iter()
        .map(|replica| replica.node_id.parse::<NodeId>())
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .find(|node| *node != seeder)
        .ok_or("no peer")?;
    let source_address = fleet.daemon(&source_primary)?.listen_address().to_string();
    let peer_address = fleet.daemon(&peer)?.listen_address().to_string();
    let as_seeder = fleet.tls.node_network(seeder.as_str())?;
    let as_peer = fleet.tls.node_network(peer.as_str())?;
    let attempt = status.attempt;
    let valid = seeding_request(&source, &status, attempt, false);

    // The attempt's seeder, with the whole identity, is served.
    let served = ask(&as_seeder, &source_address, valid.clone()).await?;
    assert!(
        served
            .as_ref()
            .is_ok_and(|chunk| chunk.total_bytes.is_some()),
        "{served:?}"
    );

    let mut cases: Vec<(&str, Network, String, SnapshotRequest)> = vec![
        (
            "a non-seeder",
            as_peer.clone(),
            source_address.clone(),
            valid.clone(),
        ),
        (
            "a stale attempt",
            as_seeder.clone(),
            source_address.clone(),
            seeding_request(&source, &status, attempt + 1, false),
        ),
    ];
    let mut wrong = valid.clone();
    wrong.fork_incarnation ^= 1;
    cases.push((
        "another fork incarnation",
        as_seeder.clone(),
        source_address.clone(),
        wrong,
    ));
    let mut wrong = valid.clone();
    wrong.source_incarnation ^= 1;
    cases.push((
        "another source incarnation",
        as_seeder.clone(),
        source_address.clone(),
        wrong,
    ));
    let mut wrong = valid.clone();
    wrong.operation = operation(99).to_bytes().to_vec();
    cases.push((
        "another operation",
        as_seeder.clone(),
        source_address.clone(),
        wrong,
    ));
    let mut wrong = valid.clone();
    wrong.database = other.to_string();
    cases.push((
        "a source that isn't the fork's",
        as_seeder.clone(),
        source_address.clone(),
        wrong,
    ));
    cases.push((
        "a peer request to a node that isn't the seeder",
        as_seeder.clone(),
        peer_address.clone(),
        seeding_request(&target, &status, attempt, true),
    ));
    for (case, network, address, request) in cases {
        let answer = ask(&network, &address, request).await?;
        assert!(refused_seeding(&answer), "{case}: {answer:?}");
    }
    // A client certificate never gets a seeding stream, however well it names the fork.
    let as_client = client_network(&fleet.tls)?;
    let answer = ask(&as_client, &source_address, valid).await?;
    assert!(
        matches!(&answer, Err(status) if status.code() == Code::PermissionDenied),
        "a client certificate: {answer:?}"
    );
    fleet.stop().await;
    Ok(())
}

/// A pin past `[fork] max_pin_ms` ends its stream with `ABORTED` and is released: the source's
/// next checkpoint succeeds (ADR-0046, point 4.2; #135).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_expired_pin_aborts_its_stream_and_is_released() -> TestResult {
    let dir = tempfile::tempdir()?;
    let fleet = Fleet::start(
        dir.path(),
        Settings {
            data: "max_pin_ms = 1500\n".to_owned(),
            ..Settings::default()
        },
    )
    .await?;
    let client = fleet.client()?;
    let source = name("src")?;
    let source_primary = create_source(&client, &source, 1, 0).await?;
    // Several megabytes, so that the stream outgrows what its channel and its connection
    // buffer.
    for _ in 0..24 {
        client
            .execute(
                &source,
                Statement::new("INSERT INTO notes (body) VALUES (randomblob(400000))"),
            )
            .await?;
    }
    let target = fleet.name_apart("dst", 2, &source_primary)?;
    // The seeder asks for nothing itself: the test asks in its name.
    for faults in &fleet.faults {
        faults.hold(FaultPoint::Fetch);
    }
    let id = operation(4);
    client
        .fork_database(&source, &target, &spec(id, 2)?)
        .await?;
    await_held(&fleet, FaultPoint::Fetch).await?;
    let status = client.fork_operation(id).await?;
    let seeder: NodeId = status
        .placement
        .as_ref()
        .and_then(|placement| placement.fork.as_ref())
        .and_then(|fork| fork.seeder.as_deref())
        .ok_or("no seeder")?
        .parse()?;
    let source_address = fleet.daemon(&source_primary)?.listen_address().to_string();
    let as_seeder = fleet.tls.node_network(seeder.as_str())?;
    let mut seeding = replication_client(as_seeder.channel(&source_address)?);
    let mut stream = tokio::time::timeout(HANG_GUARD, async {
        loop {
            match seeding
                .snapshot(seeding_request(&source, &status, status.attempt, false))
                .await
            {
                // The source asks for a moment while its chain reaches the pinned position.
                Err(status) if status.code() == Code::Unavailable => {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                answer => return answer,
            }
        }
    })
    .await??
    .into_inner();
    let first = stream.message().await?.ok_or("an empty stream")?;
    assert!(first.total_bytes.is_some());
    // The seeder stops reading; the pin expires.
    tokio::time::sleep(Duration::from_millis(2500)).await;
    let ended = loop {
        match stream.message().await {
            Ok(Some(_)) => {}
            Ok(None) => break None,
            Err(status) => break Some(status),
        }
    };
    let ended = ended.ok_or("the stream ended without a status")?;
    assert_eq!(ended.code(), Code::Aborted, "{ended:?}");
    // The pin is released: the source checkpoints.
    let replication = fleet
        .daemon(&source_primary)?
        .replication()
        .ok_or("no replication")?;
    wait_until("the source checkpoints", async || {
        replication.checkpoint(&source).await.is_ok()
    })
    .await?;
    fleet.stop().await;
    Ok(())
}

/// A count from a daemon's metrics: the sample of `family` with `labels`, or 0.
fn metric(daemon: &Daemon, sample: &str) -> u64 {
    daemon
        .metrics()
        .encode()
        .lines()
        .find_map(|line| line.strip_prefix(sample)?.trim().parse::<f64>().ok())
        .map_or(0, |value| {
            #[expect(
                clippy::cast_possible_truncation,
                clippy::cast_sign_loss,
                reason = "a small, whole count"
            )]
            let count = value as u64;
            count
        })
}

/// One replica's disk refusal doesn't fail the fork when the other two admit it (ADR-0046,
/// point 7.2; #135).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn one_replicas_disk_refusal_leaves_the_fork_to_the_others() -> TestResult {
    let dir = tempfile::tempdir()?;
    let fleet = Fleet::start(dir.path(), Settings::default()).await?;
    let client = fleet.client()?;
    let source = name("src")?;
    create_source(&client, &source, 1, 5).await?;
    // The replica the fork's first attempt seeds from: its refusal can't be overtaken.
    let tiny = fleet
        .ring_replicas("dst", 3)?
        .into_iter()
        .next()
        .ok_or("no replica")?;
    // A volume with no room for any copy.
    fleet.faults(&tiny)?.set_disk(1 << 20, 1 << 30);
    let target = name("dst")?;
    let id = operation(5);
    client
        .fork_database(&source, &target, &spec(id, 3)?)
        .await?;
    let mut refused = BTreeSet::new();
    let ready = tokio::time::timeout(HANG_GUARD, async {
        loop {
            let status = client.fork_operation(id).await?;
            if let Some(fork) = status
                .placement
                .as_ref()
                .and_then(|placement| placement.fork.as_ref())
            {
                refused.extend(fork.disk_refused.iter().cloned());
            }
            if status.state() == proto::ForkState::Ready {
                return Ok::<_, tonic::Status>(status);
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await??;
    assert_eq!(ready.state(), proto::ForkState::Ready);
    assert_eq!(refused, BTreeSet::from([tiny.to_string()]));
    assert_eq!(
        metric(
            fleet.daemon(&tiny)?,
            "fleetring_fork_attempts_total{outcome=\"insufficient_disk\"}"
        ),
        1
    );
    assert_eq!(notes(&client, &target).await?.len(), 5);
    fleet.stop().await;
    Ok(())
}

/// Two forks at once to a node with room for one get one admission and one refusal on that
/// node, never two admissions (ADR-0046, point 7.1; #135).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn two_forks_never_count_the_same_free_bytes() -> TestResult {
    let dir = tempfile::tempdir()?;
    let fleet = Fleet::start(dir.path(), Settings::default()).await?;
    let client = fleet.client()?;
    let source = name("src")?;
    let source_primary = create_source(&client, &source, 1, 0).await?;
    for _ in 0..4 {
        client
            .execute(
                &source,
                Statement::new("INSERT INTO notes (body) VALUES (hex(randomblob(250000)))"),
            )
            .await?;
    }
    let replication = fleet
        .daemon(&source_primary)?
        .replication()
        .ok_or("no replication")?;
    wait_until("the source checkpoints", async || {
        replication.checkpoint(&source).await.is_ok()
    })
    .await?;
    let tight: NodeId = DATA
        .iter()
        .map(|id| id.parse::<NodeId>())
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .find(|node| *node != source_primary)
        .ok_or("no other node")?;
    // A probe fork learns how big a copy of the source is: the size its sidecar records.
    let faults = fleet.faults(&tight)?;
    faults.hold(FaultPoint::Staged);
    let probe = operation(9);
    client
        .fork_database(&source, &name("probe")?, &spec(probe, 3)?)
        .await?;
    let mut size = None;
    wait_until("the tight node stages the probe", async || {
        size = staged_dirs(dir.path(), &tight, &name("probe").expect("a name"))
            .iter()
            .find_map(|staged| {
                let sidecar = std::fs::read_to_string(staged.join("sidecar.json")).ok()?;
                serde_json::from_str::<serde_json::Value>(&sidecar).ok()?["size"].as_u64()
            });
        size.is_some()
    })
    .await?;
    let size = size.ok_or("no size")?;
    faults.release(FaultPoint::Staged);
    await_state(&client, probe, proto::ForkState::Ready).await?;
    // Room for one copy beside the headroom, not two.
    let headroom = 1 << 30;
    faults.set_disk(headroom + size * 16 / 10, 2 * headroom);
    let staged_before = metric(
        fleet.daemon(&tight)?,
        "fleetring_fork_attempts_total{outcome=\"staged\"}",
    );
    let (first, second) = (operation(6), operation(7));
    let (a, b) = (name("dst-a")?, name("dst-b")?);
    let (spec_a, spec_b) = (spec(first, 3)?, spec(second, 3)?);
    let (forked_a, forked_b) = tokio::join!(
        client.fork_database(&source, &a, &spec_a),
        client.fork_database(&source, &b, &spec_b)
    );
    forked_a?;
    forked_b?;
    await_state(&client, first, proto::ForkState::Ready).await?;
    await_state(&client, second, proto::ForkState::Ready).await?;
    let daemon = fleet.daemon(&tight)?;
    assert_eq!(
        metric(daemon, "fleetring_fork_attempts_total{outcome=\"staged\"}") - staged_before,
        1,
        "one admission on the tight node"
    );
    assert_eq!(
        metric(
            daemon,
            "fleetring_fork_attempts_total{outcome=\"insufficient_disk\"}"
        ),
        1,
        "one refusal on the tight node"
    );
    fleet.stop().await;
    Ok(())
}

/// A dev node: one daemon that is its own control plane and its only data node, with `extra`
/// appended to its configuration.
async fn dev_node(dir: &Path, extra: &str) -> Result<(Daemon, Client), Box<dyn Error>> {
    let tls = Tls::create(dir)?;
    let text = format!(
        "data_dir = {:?}\nlisten = \"127.0.0.1:0\"\n\n[node]\nid = \"dev\"\n\n[membership]\n\
         heartbeat_interval_ms = 100\n{}{FORK}{extra}",
        dir.join("dev"),
        tls.node_table("dev")?
    );
    let faults = ForkFaults::new();
    faults.set_disk(ROOMY, 2 * ROOMY);
    let daemon = Daemon::start_with(
        Config::from_toml(&text)?,
        Environment::production().with_fork_faults(faults),
    )
    .await?;
    let address = daemon.control_address().ok_or("no control address")?;
    let membership = daemon.membership().ok_or("no membership")?;
    tokio::time::timeout(
        HANG_GUARD,
        membership.wait_for(|view| view.ring().is_some()),
    )
    .await?
    .ok_or("membership stopped")?;
    let client = Client::new(address.to_string(), client_network(&tls)?);
    Ok((daemon, client))
}

/// A dev node, R = 1 and W = 1, forks and its fork becomes ready through the same path
/// (#135).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_dev_node_forks() -> TestResult {
    let dir = tempfile::tempdir()?;
    let (daemon, client) = dev_node(dir.path(), "").await?;
    let source = name("src")?;
    create_source(&client, &source, 1, 3).await?;
    let target = name("dst")?;
    let id = operation(8);
    client
        .fork_database(&source, &target, &spec(id, 1)?)
        .await?;
    await_state(&client, id, proto::ForkState::Ready).await?;
    assert_eq!(
        notes(&client, &target).await?,
        notes(&client, &source).await?
    );
    let held = reserved(dir.path(), &"dev".parse()?, &target)?;
    assert_eq!(held.markers.len(), 1);
    daemon.run_until(async {}).await;
    Ok(())
}

/// A lone primary, R = 1, that checkpointed while it was written serves a chain hash at the
/// position it pins, so a dev node forks a database that has checkpointed (#156).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_dev_node_forks_a_database_that_checkpointed() -> TestResult {
    let dir = tempfile::tempdir()?;
    let (daemon, client) = dev_node(
        dir.path(),
        "\n[replication]\ncheckpoint_threshold_bytes = 4096\n",
    )
    .await?;
    let source = name("src")?;
    create_source(&client, &source, 1, 0).await?;
    let replication = daemon.replication().ok_or("no replication")?;
    // Writes go on while the checkpoints they set off run, so that some land between where the
    // chain was brought up to and where the checkpoint restarts the log.
    for n in 0..200 {
        client
            .execute(
                &source,
                Statement::with_params(
                    "INSERT INTO notes (body) VALUES (?)",
                    [Value::from(format!("{n:0>600}"))],
                ),
            )
            .await?;
    }
    let status = replication
        .primary_status(&source)
        .ok_or("the dev node leads the source")?;
    assert!(status.checkpointed.is_some(), "the source checkpointed");
    let target = name("dst")?;
    let id = operation(10);
    client
        .fork_database(&source, &target, &spec(id, 1)?)
        .await?;
    await_state(&client, id, proto::ForkState::Ready).await?;
    assert_eq!(
        notes(&client, &target).await?,
        notes(&client, &source).await?
    );
    daemon.run_until(async {}).await;
    Ok(())
}

impl Fleet {
    /// Starts data daemon `id` again from its configuration, with fresh fault hooks.
    async fn restart(&mut self, config: Config) -> TestResult {
        let hooks = ForkFaults::new();
        hooks.set_disk(ROOMY, 2 * ROOMY);
        let environment = Environment::production().with_fork_faults(hooks.clone());
        self.data
            .push(Daemon::start_with(config, environment).await?);
        self.faults.push(hooks);
        self.converge().await
    }
}

/// A sink that can't deliver, on every database, so each keeps its change record: a reset that
/// ran again would empty it.
const STUCK_SINK: &str = "\n[[changes.sink]]\nname = \"stuck\"\nkind = \"webhook\"\n\
     format = \"logical\"\ndatabases = [\"*\"]\nurl = \"http://127.0.0.1:9/changes\"\n\
     allow_http = true\n";

/// Crashes a fork's primary where its reset work reaches `point`: the fork still becomes ready,
/// its marker is the fork's, the reset is written only while the marker is absent, and a change
/// a client writes after readiness survives the next failover (ADR-0046, points 6.1 to 6.3;
/// #135).
async fn crash_during_the_reset(point: FaultPoint, seed: u8) -> TestResult {
    let dir = tempfile::tempdir()?;
    let mut fleet = Fleet::start(
        dir.path(),
        Settings {
            control: FAST_LEASES,
            data: STUCK_SINK.to_owned(),
            stay: true,
            ..Settings::default()
        },
    )
    .await?;
    let client = fleet.client()?;
    let source = name("src")?;
    create_source(&client, &source, 1, 5).await?;
    let mut retired: Vec<ForkFaults> = Vec::new();
    let id = operation(seed);
    let target = name("dst")?;
    for faults in &fleet.faults {
        faults.hold_operation(point, id);
    }
    client
        .fork_database(&source, &target, &spec(id, 3)?)
        .await?;
    let primary = await_held(&fleet, point).await?;
    // The primary crashes there; the others let the fork go on.
    let config = fleet.daemon(&primary)?.config().clone();
    let index = fleet.index(&primary)?;
    retired.push(fleet.faults[index].clone());
    fleet.crash(&primary).await?;
    for faults in &fleet.faults {
        faults.release_operation(point, id);
    }
    fleet.restart(config).await?;
    let ready = await_state(&client, id, proto::ForkState::Ready).await?;
    let writes: u64 = fleet
        .faults
        .iter()
        .chain(&retired)
        .map(|faults| faults.passes(FaultPoint::AfterLocalCommit))
        .sum();
    // Before or during the reset, the crashed primary wrote nothing, and the next wrote it
    // once. After its local commit, the next primary writes it again only if the marker didn't
    // survive, so that it is in the surviving history once. After its quorum commit, the next
    // primary finds the marker and doesn't write it.
    match point {
        FaultPoint::AfterLocalCommit => assert!((1..=2).contains(&writes), "{writes}"),
        _ => assert_eq!(writes, 1),
    }
    let fork_primary = client
        .locate(&target)
        .await?
        .primary
        .ok_or("a ready fork has a primary")?;
    let marker = (ready.incarnation.cast_signed(), id.to_bytes().to_vec());
    assert_eq!(
        reserved(dir.path(), &fork_primary, &target)?.markers,
        vec![marker.clone()]
    );

    // A change written after readiness survives the next failover: no reset runs again.
    client
        .execute(
            &target,
            Statement::new("INSERT INTO notes (body) VALUES ('after')"),
        )
        .await?;
    wait_until("the fork's primary records the change", async || {
        reserved(dir.path(), &fork_primary, &target).is_ok_and(|held| held.changes > 0)
    })
    .await?;
    fleet.crash(&fork_primary).await?;
    let moved = tokio::time::timeout(HANG_GUARD, async {
        loop {
            match client
                .execute(
                    &target,
                    Statement::new("INSERT INTO notes (body) VALUES ('later')"),
                )
                .await
            {
                Ok(written) => return Ok(written),
                Err(status) if matches!(status.code(), Code::Unavailable | Code::Aborted) => {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                Err(status) => return Err(status),
            }
        }
    })
    .await??;
    let next = moved.primary.ok_or("no primary named")?;
    assert_ne!(next, fork_primary);
    let held = reserved(dir.path(), &next, &target)?;
    assert!(
        held.changes >= 2,
        "the earlier change survives: {}",
        held.changes
    );
    assert_eq!(held.markers, vec![marker]);
    fleet.stop().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_crash_before_the_reset_leaves_exactly_one() -> TestResult {
    crash_during_the_reset(FaultPoint::BeforeReset, 20).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_crash_during_the_reset_leaves_exactly_one() -> TestResult {
    crash_during_the_reset(FaultPoint::DuringReset, 21).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_crash_after_the_resets_local_commit_leaves_exactly_one() -> TestResult {
    crash_during_the_reset(FaultPoint::AfterLocalCommit, 22).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_crash_after_the_resets_quorum_commit_leaves_exactly_one() -> TestResult {
    crash_during_the_reset(FaultPoint::AfterQuorumCommit, 23).await
}

/// Target fencing on real daemons: a fork's seeder pauses after it stages attempt 1; the fork
/// is dropped and made again under its name, and reaches attempt 1 on the same nodes; the old
/// seeder then resumes. The new fork never counts the old copy, the old identity's requests are
/// refused by the seeding binding, and the old staging directory is deleted on the drop or kept
/// as an orphan, never opened (ADR-0046, points 4 and 12; #135).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_dropped_forks_seeder_never_touches_its_replacement() -> TestResult {
    let dir = tempfile::tempdir()?;
    let fleet = Fleet::start(dir.path(), Settings::default()).await?;
    let client = fleet.client()?;
    let source = name("src")?;
    let source_primary = create_source(&client, &source, 1, 5).await?;
    let target = fleet.name_apart("dst", 2, &source_primary)?;
    let (old_id, new_id) = (operation(30), operation(31));
    for faults in &fleet.faults {
        faults.hold_operation(FaultPoint::Staged, old_id);
    }
    client
        .fork_database(&source, &target, &spec(old_id, 2)?)
        .await?;
    let seeder = await_held(&fleet, FaultPoint::Staged).await?;
    let old = client.fork_operation(old_id).await?;
    assert_eq!(old.state(), proto::ForkState::Seeding);
    let peer: NodeId = old
        .placement
        .as_ref()
        .ok_or("no placement")?
        .replicas
        .iter()
        .map(|replica| replica.node_id.parse::<NodeId>())
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .find(|node| *node != seeder)
        .ok_or("no peer")?;

    client.drop_database(&target).await?;
    await_state(&client, old_id, proto::ForkState::Cancelled).await?;
    client
        .fork_database(&source, &target, &spec(new_id, 2)?)
        .await?;
    let new = await_state(&client, new_id, proto::ForkState::Ready).await?;
    assert_ne!(new.incarnation, old.incarnation);
    // The old seeder resumes.
    for faults in &fleet.faults {
        faults.release_operation(FaultPoint::Staged, old_id);
    }
    tokio::time::sleep(Duration::from_millis(500)).await;
    let again = client.fork_operation(new_id).await?;
    assert_eq!(again.state(), proto::ForkState::Ready);
    assert_eq!(again.incarnation, new.incarnation);
    assert_eq!(
        notes(&client, &target).await?,
        notes(&client, &source).await?
    );

    // The old identity's requests: a peer's to the seeder, and the seeder's to the source.
    let seeder_address = fleet.daemon(&seeder)?.listen_address().to_string();
    let source_address = fleet.daemon(&source_primary)?.listen_address().to_string();
    let as_peer = fleet.tls.node_network(peer.as_str())?;
    let as_seeder = fleet.tls.node_network(seeder.as_str())?;
    let answer = ask(
        &as_peer,
        &seeder_address,
        seeding_request(&target, &old, old.attempt, true),
    )
    .await?;
    assert!(refused_seeding(&answer), "an old peer request: {answer:?}");
    let answer = ask(
        &as_seeder,
        &source_address,
        seeding_request(&source, &old, old.attempt, false),
    )
    .await?;
    assert!(
        refused_seeding(&answer),
        "an old source request: {answer:?}"
    );

    // The old staging directory went with the drop, or stays as an orphan; it was never opened.
    let old_dir = format!(
        "{:016x}-{}",
        old.incarnation,
        OperationId::from_bytes(old.operation_id.as_slice().try_into()?)
    );
    let kept = staged_dirs(dir.path(), &seeder, &target)
        .iter()
        .any(|staged| {
            staged
                .parent()
                .is_some_and(|identity| identity.ends_with(&old_dir))
        });
    if kept {
        let status = client.node_status(&seeder_address).await?;
        assert!(
            status.orphans.iter().any(|orphan| orphan.database == target
                && orphan
                    .staged
                    .is_some_and(|staged| staged.incarnation.get() == old.incarnation)),
            "a kept staged copy of the old fork is an orphan"
        );
    }
    fleet.stop().await;
    Ok(())
}

impl Fleet {
    /// Starts data daemon `id`, a new member, and waits until every data daemon publishes the
    /// ring with it.
    async fn join(&mut self, dir: &Path, id: &str, settings: &Settings) -> TestResult {
        let address = self.control.control_address().ok_or("no control address")?;
        let text = config(dir, &self.tls, id, "data", Some(address), settings)?;
        self.restart(Config::from_toml(&text)?).await
    }

    /// A name for a fork of three replicas that the ring places on `joiner` and not on
    /// `excluded` once `joiner` joins the fleet with the attributes of its first member.
    fn name_moved_on_join(
        &self,
        joiner: &NodeId,
        excluded: &NodeId,
    ) -> Result<DatabaseName, Box<dyn Error>> {
        let handle = self.control.control().ok_or("no control handle")?;
        let state = handle.watch().borrow().clone();
        let template = state.members().next().ok_or("no member")?.clone();
        let mut joined = Member::new(joiner.clone()).with_weight(template.weight());
        if let Some(zone) = template.zone() {
            joined = joined.with_zone(zone.clone());
        }
        if let Some(host) = template.host() {
            joined = joined.with_host(host.clone());
        }
        let mut members: Vec<Member> = state.members().cloned().collect();
        members.push(joined);
        let ring = membership_ring(&members, state.membership_epoch())?;
        for n in 0..1_000 {
            let candidate = format!("moved-{n}");
            let replicas = ring.replicas_n(&candidate, factor(3)?).to_ids();
            if replicas.contains(joiner) && !replicas.contains(excluded) {
                return Ok(candidate.parse()?);
            }
        }
        Err("no name the joined ring moves".into())
    }
}

/// Writes a row to `database` once its primary serves clients, and returns the primary.
async fn write_once_served(
    client: &Client,
    database: &DatabaseName,
    body: &str,
) -> Result<NodeId, Box<dyn Error>> {
    let written = tokio::time::timeout(HANG_GUARD, async {
        loop {
            match client
                .execute(
                    database,
                    Statement::with_params(
                        "INSERT INTO notes (body) VALUES (?)",
                        [Value::from(body.to_owned())],
                    ),
                )
                .await
            {
                Ok(written) => return Ok(written),
                Err(status) if matches!(status.code(), Code::Unavailable | Code::Aborted) => {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                Err(status) => return Err(status),
            }
        }
    })
    .await??;
    Ok(written.primary.ok_or("no primary named")?)
}

/// The followers `database`'s primary on data daemon `id` streams to.
fn followers_of(
    fleet: &Fleet,
    id: &NodeId,
    database: &DatabaseName,
) -> Result<BTreeSet<NodeId>, Box<dyn Error>> {
    let status = fleet
        .daemon(id)?
        .replication()
        .ok_or("no replication")?
        .primary_status(database)
        .ok_or("not the primary")?;
    Ok(status
        .followers
        .into_iter()
        .map(|progress| progress.node)
        .collect())
}

/// A fork of three replicas seeded on the first three data daemons, whose primary serves
/// clients while its followers' reset reports are held, so it stays seeded; then a fourth data
/// daemon joins, and the ring moves the fork onto it and off the second.
struct Moved {
    fleet: Fleet,
    client: Client,
    target: DatabaseName,
    id: OperationId,
    primary: NodeId,
    holders: BTreeSet<NodeId>,
    joiner: NodeId,
}

const JOINER: &str = "data-4";

async fn seeded_then_moved(
    dir: &Path,
    settings: fn() -> Settings,
    seed: u8,
) -> Result<Moved, Box<dyn Error>> {
    let mut fleet = Fleet::start(dir, settings()).await?;
    let client = fleet.client()?;
    let source = name("src")?;
    create_source(&client, &source, 1, 5).await?;
    let joiner: NodeId = JOINER.parse()?;
    let excluded: NodeId = DATA[1].parse()?;
    let target = fleet.name_moved_on_join(&joiner, &excluded)?;
    let id = operation(seed);
    for faults in &fleet.faults {
        faults.hold_operation(FaultPoint::FollowerReset, id);
    }
    client
        .fork_database(&source, &target, &spec(id, 3)?)
        .await?;
    await_state(&client, id, proto::ForkState::Seeded).await?;
    let primary = write_once_served(&client, &target, "served while seeded").await?;
    assert_eq!(primary.as_str(), DATA[0], "the first live holder leads");
    let seeded = client.fork_operation(id).await?;
    assert_eq!(
        seeded.state(),
        proto::ForkState::Seeded,
        "the reports are held"
    );
    let holders: BTreeSet<NodeId> = DATA
        .iter()
        .map(|id| id.parse::<NodeId>())
        .collect::<Result<_, _>>()?;
    fleet.join(dir, JOINER, &settings()).await?;
    let ring: BTreeSet<NodeId> = fleet
        .ring_replicas(target.as_str(), 3)?
        .into_iter()
        .collect();
    assert!(
        ring.contains(&joiner) && !ring.contains(&excluded),
        "the ring moved the fork: {ring:?}"
    );
    Ok(Moved {
        fleet,
        client,
        target,
        id,
        primary,
        holders,
        joiner,
    })
}

/// From the reset's commit to `Ready`, a seeded fork's primary streams to its recorded holders
/// alone and counts only them toward a write's quorum, though it serves clients and the ring
/// has moved the fork; once ready, it follows the ring (ADR-0046, point 3.10).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_seeded_fork_streams_only_to_its_holders_until_it_is_ready() -> TestResult {
    let dir = tempfile::tempdir()?;
    let Moved {
        fleet,
        client,
        target,
        id,
        primary,
        holders,
        joiner,
        ..
    } = seeded_then_moved(dir.path(), Settings::default, 40).await?;
    // A write after the ring moved is acknowledged by the holders alone.
    write_once_served(&client, &target, "after the move").await?;
    tokio::time::sleep(Duration::from_secs(1)).await;
    let expected: BTreeSet<NodeId> = holders
        .iter()
        .filter(|node| **node != primary)
        .cloned()
        .collect();
    assert_eq!(followers_of(&fleet, &primary, &target)?, expected);
    let joined = fleet
        .daemon(&joiner)?
        .replication()
        .ok_or("no replication")?;
    assert!(
        joined.follower_status(&target).is_none(),
        "the joined node never took the seeded fork's stream"
    );
    assert_eq!(
        client.fork_operation(id).await?.state(),
        proto::ForkState::Seeded
    );

    for faults in &fleet.faults {
        faults.release_operation(FaultPoint::FollowerReset, id);
    }
    await_state(&client, id, proto::ForkState::Ready).await?;
    // Ready, the fork follows the ring: the joined node becomes a follower.
    wait_until(
        "the ready fork's primary streams to the joined node",
        async || {
            followers_of(&fleet, &primary, &target)
                .is_ok_and(|followers| followers.contains(&joiner))
        },
    )
    .await?;
    assert_eq!(notes(&client, &target).await?.len(), 7);
    fleet.stop().await;
    Ok(())
}

fn fast_failover() -> Settings {
    Settings {
        control: FAST_LEASES,
        stay: true,
        ..Settings::default()
    }
}

/// A holder promotes a seeded fork whose placement moved: it fences the recorded holders, never
/// the node the ring moved the fork onto, and the fork becomes ready with every acknowledged
/// write (ADR-0046, points 3.9 and 3.10).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_holder_promotes_a_seeded_fork_over_its_holders_after_placement_moved() -> TestResult {
    let dir = tempfile::tempdir()?;
    let Moved {
        mut fleet,
        client,
        target,
        id,
        primary,
        holders,
        joiner,
        ..
    } = seeded_then_moved(dir.path(), fast_failover, 41).await?;
    write_once_served(&client, &target, "after the move").await?;
    // Both followers hold everything, so the holders rank equal and the lowest ID, the holder
    // the ring moved the fork off, is the candidate.
    wait_until("both followers hold every write", async || {
        fleet
            .daemon(&primary)
            .ok()
            .and_then(|daemon| daemon.replication())
            .and_then(|replication| replication.primary_status(&target))
            .is_some_and(|status| {
                status.followers.len() == 2
                    && status.followers.iter().all(|follower| {
                        follower.reported && follower.acknowledged == status.committed
                    })
            })
    })
    .await?;
    fleet.crash(&primary).await?;
    let mut promoted = None;
    wait_until("a holder is promoted", async || {
        promoted = client
            .locate(&target)
            .await
            .ok()
            .and_then(|location| location.primary)
            .filter(|node| *node != primary);
        promoted.is_some()
    })
    .await?;
    let promoted = promoted.ok_or("no primary")?;
    assert!(holders.contains(&promoted), "{promoted} is a holder");
    let joined = fleet
        .daemon(&joiner)?
        .replication()
        .ok_or("no replication")?;
    assert_eq!(
        joined.highest_epoch(&target),
        None,
        "the node the ring moved the fork onto was never fenced"
    );
    assert_eq!(
        client.fork_operation(id).await?.state(),
        proto::ForkState::Seeded
    );
    // The promoted holder serves every acknowledged write once its gate opens.
    wait_until("the promoted holder serves every write", async || {
        notes(&client, &target)
            .await
            .is_ok_and(|rows| rows.len() == 7)
    })
    .await?;
    for faults in &fleet.faults {
        faults.release_operation(FaultPoint::FollowerReset, id);
    }
    await_state(&client, id, proto::ForkState::Ready).await?;
    fleet.stop().await;
    Ok(())
}

/// Every data call against `database` that the data daemon at `address` answers, each with the
/// status it was refused with, by call; an error if one was served.
async fn refusals(
    network: &Network,
    address: &str,
    database: &DatabaseName,
) -> Result<Vec<(&'static str, tonic::Status)>, Box<dyn Error>> {
    let mut raw = database_client(network.channel(address)?);
    let database = database.to_string();
    let served = |call: &str| format!("{call} was served");
    let mut refused = Vec::new();
    let execute = raw
        .execute(proto::ExecuteRequest {
            database: database.clone(),
            sql: "INSERT INTO notes (body) VALUES ('early')".to_owned(),
            params: Vec::new(),
        })
        .await;
    refused.push(("Execute", execute.err().ok_or_else(|| served("Execute"))?));
    let query = raw
        .query(proto::QueryRequest {
            database: database.clone(),
            sql: "SELECT count(*) FROM notes".to_owned(),
            ..proto::QueryRequest::default()
        })
        .await;
    refused.push(("Query", query.err().ok_or_else(|| served("Query"))?));
    let schema = raw
        .schema(proto::SchemaRequest {
            database: database.clone(),
            ..proto::SchemaRequest::default()
        })
        .await;
    refused.push(("Schema", schema.err().ok_or_else(|| served("Schema"))?));
    let verify = raw
        .verify(proto::VerifyRequest {
            database: database.clone(),
            ..proto::VerifyRequest::default()
        })
        .await;
    refused.push(("Verify", verify.err().ok_or_else(|| served("Verify"))?));
    let watched = match raw
        .watch_changes(proto::WatchChangesRequest {
            database: database.clone(),
            after: None,
        })
        .await
    {
        Err(status) => status,
        Ok(stream) => stream
            .into_inner()
            .message()
            .await
            .err()
            .ok_or_else(|| served("WatchChanges"))?,
    };
    refused.push(("WatchChanges", watched));
    let open = proto::SessionRequest {
        request: Some(proto::session_request::Request::Open(proto::SessionOpen {
            database,
        })),
    };
    let session = match raw.session(tokio_stream::iter([open])).await {
        Err(status) => status,
        Ok(stream) => stream
            .into_inner()
            .message()
            .await
            .err()
            .ok_or_else(|| served("Session"))?,
    };
    refused.push(("Session", session));
    Ok(refused)
}

/// Asserts that every data call against `database`, from the data daemon at each of `addresses`
/// and through the client, is refused as `not_ready` at `stage`, or, for a failed fork, as
/// failed.
async fn assert_refused(
    fleet: &Fleet,
    client: &Client,
    addresses: &[String],
    database: &DatabaseName,
    stage: &str,
) -> TestResult {
    let failed = stage == "failed";
    let expected = |status: &tonic::Status| {
        if failed {
            status.code() == Code::FailedPrecondition
                && status.message().contains("failed")
                && status
                    .message()
                    .contains(&format!("fleetring db drop {database}"))
        } else {
            not_ready(status)
        }
    };
    let network = client_network(&fleet.tls)?;
    for address in addresses {
        for (call, status) in refusals(&network, address, database).await? {
            assert!(
                expected(&status),
                "{stage}: {call} at {address}: {status:?}"
            );
        }
    }
    let queried = client
        .query(database, Statement::new("SELECT count(*) FROM notes"))
        .await
        .expect_err("the client's query is refused");
    assert!(
        expected(&queried),
        "{stage}: the client's query: {queried:?}"
    );
    Ok(())
}

/// Every data call against a fork that isn't ready is refused alike, on every node and through
/// the client, while it seeds, once it is seeded with no primary, and while its primary holds
/// the reset: `UNAVAILABLE` "fork not ready" with a retry-after; and a failed fork's calls say
/// it failed and how to remove it (ADR-0046, point 6.2).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_fork_that_isnt_ready_refuses_every_data_call_alike() -> TestResult {
    let dir = tempfile::tempdir()?;
    let fleet = Fleet::start(dir.path(), Settings::default()).await?;
    let client = fleet.client()?;
    let source = name("src")?;
    let source_primary = create_source(&client, &source, 1, 5).await?;
    let target = fleet.name_apart("dst", 2, &source_primary)?;
    let every: Vec<String> = fleet
        .data
        .iter()
        .map(|daemon| daemon.listen_address().to_string())
        .collect();
    let id = operation(50);
    for faults in &fleet.faults {
        for point in [
            FaultPoint::Staged,
            FaultPoint::FirstLease,
            FaultPoint::BeforeReset,
        ] {
            faults.hold_operation(point, id);
        }
    }
    client
        .fork_database(&source, &target, &spec(id, 2)?)
        .await?;
    await_held(&fleet, FaultPoint::Staged).await?;
    assert_eq!(
        client.fork_operation(id).await?.state(),
        proto::ForkState::Seeding
    );
    assert_refused(&fleet, &client, &every, &target, "seeding").await?;

    for faults in &fleet.faults {
        faults.release_operation(FaultPoint::Staged, id);
    }
    await_state(&client, id, proto::ForkState::Seeded).await?;
    await_held(&fleet, FaultPoint::FirstLease).await?;
    assert_eq!(client.locate(&target).await?.primary, None);
    assert_refused(&fleet, &client, &every, &target, "seeded before its lease").await?;

    for faults in &fleet.faults {
        faults.release_operation(FaultPoint::FirstLease, id);
    }
    let primary = await_held(&fleet, FaultPoint::BeforeReset).await?;
    let at_primary = [fleet.daemon(&primary)?.listen_address().to_string()];
    assert_refused(
        &fleet,
        &client,
        &at_primary,
        &target,
        "leased with its reset held",
    )
    .await?;

    for faults in &fleet.faults {
        faults.release_operation(FaultPoint::BeforeReset, id);
    }
    await_state(&client, id, proto::ForkState::Ready).await?;
    assert_eq!(notes(&client, &target).await?.len(), 5);

    // A fork whose source is dropped while it seeds fails, and says so.
    let doomed = name("doomed")?;
    let failing = operation(51);
    for faults in &fleet.faults {
        faults.hold_operation(FaultPoint::Staged, failing);
    }
    client
        .fork_database(&source, &doomed, &spec(failing, 2)?)
        .await?;
    await_held(&fleet, FaultPoint::Staged).await?;
    client.drop_database(&source).await?;
    await_state(&client, failing, proto::ForkState::Failed).await?;
    assert_refused(&fleet, &client, &every, &doomed, "failed").await?;
    for faults in &fleet.faults {
        faults.release_operation(FaultPoint::Staged, failing);
    }
    fleet.stop().await;
    Ok(())
}
