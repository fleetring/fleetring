//! FR-033: the `Database` service, the fleet client, and the command line, over real TCP.
//!
//! Each test starts a control daemon and three data daemons in process, on loopback ports the
//! operating system picks. Writes replicate to their followers through the daemons' own
//! `Replication` services (FR-030).

use std::collections::BTreeMap;
use std::error::Error;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use fleetring::ReplicationFactor;
use fleetring_control::{Acks, DatabaseName, Durability, LeaseEpoch, Liveness, ReplicaRole};
use fleetring_proto::v1::{self as proto, ExecuteRequest};
use fleetringd::data::{Client, DatabaseSpec, LeaseUpdate, LeaseUpdates, Location, primary_hint};
use fleetringd::engine::{Internal, ObjectKind, RowCount, SchemaRequest, Statement, Value};
use fleetringd::replication::{LeaseChange, LeaseEvent};
use fleetringd::transport::database_client;
use fleetringd::{Config, Daemon, Network};
use tonic::Code;

use crate::support::Tls;

mod support;

type TestResult = Result<(), Box<dyn Error>>;

/// Guards against a hang only; nothing here asserts how long anything takes.
const HANG_GUARD: Duration = Duration::from_secs(60);

/// The acknowledgement window the data daemons in these tests run with; see [`config_with`].
const DATA_ACK_TIMEOUT_MS: u64 = 20_000;

const DATA: [&str; 3] = ["data-1", "data-2", "data-3"];

fn config(
    dir: &Path,
    tls: &Tls,
    id: &str,
    role: &str,
    control: Option<SocketAddr>,
) -> Result<String, Box<dyn Error>> {
    config_with(dir, tls, id, role, control, "")
}

/// A configuration with `membership_settings` appended to its `[membership]` table.
fn config_with(
    dir: &Path,
    tls: &Tls,
    id: &str,
    role: &str,
    control: Option<SocketAddr>,
    membership_settings: &str,
) -> Result<String, Box<dyn Error>> {
    let control = control.map_or_else(String::new, |address| {
        format!("\n[control]\naddress = \"{address}\"\n")
    });
    // Data daemons wait longer than the production default for a follower's acknowledgement,
    // which also bounds how long a new database waits for its followers to connect. Every test
    // here runs four daemons, the tests run in parallel, and on a slow CI disk a follower can
    // take more than five seconds to open a new database and acknowledge its first write.
    let replication = if role == "data" {
        format!("\n[replication]\nack_timeout_ms = {DATA_ACK_TIMEOUT_MS}\n")
    } else {
        String::new()
    };
    let tls = tls.node_table(id)?;
    Ok(format!(
        "data_dir = {:?}\nlisten = \"127.0.0.1:0\"\nrole = \"{role}\"\n\n[node]\nid = \"{id}\"\n\
         {replication}\n[membership]\nheartbeat_interval_ms = 100\n{membership_settings}{tls}{control}",
        dir.join(id)
    ))
}

/// A control daemon and three data daemons that have all joined and published one ring.
struct Fleet {
    control: Daemon,
    data: Vec<Daemon>,
    tls: Tls,
}

impl Fleet {
    async fn start(dir: &Path) -> Result<Self, Box<dyn Error>> {
        Self::start_with(dir, "").await
    }

    /// Starts the fleet with `control_settings` appended to every daemon's `[control]` table.
    async fn start_with(dir: &Path, control_settings: &str) -> Result<Self, Box<dyn Error>> {
        Self::start_configured(dir, control_settings, "", "").await
    }

    /// Starts the fleet with `control_settings` appended to every daemon's `[control]` table,
    /// `control_tables` appended to the control daemon's configuration, and
    /// `membership_settings` to every data daemon's `[membership]` table.
    async fn start_configured(
        dir: &Path,
        control_settings: &str,
        control_tables: &str,
        membership_settings: &str,
    ) -> Result<Self, Box<dyn Error>> {
        Self::start_in(
            dir,
            control_settings,
            control_tables,
            membership_settings,
            fleetringd::Environment::production(),
        )
        .await
    }

    /// Starts the fleet as [`Fleet::start_configured`] does, with the control daemon in
    /// `control_environment`, as one whose clock a test moves.
    async fn start_in(
        dir: &Path,
        control_settings: &str,
        control_tables: &str,
        membership_settings: &str,
        control_environment: fleetringd::Environment,
    ) -> Result<Self, Box<dyn Error>> {
        let tls = Tls::create(dir)?;
        let control_text = format!(
            "{}\n[control]\n{control_settings}\n{control_tables}",
            config(dir, &tls, "control", "control", None)?
        );
        let control =
            Daemon::start_with(Config::from_toml(&control_text)?, control_environment).await?;
        let address = control.control_address().ok_or("no control address")?;
        let mut data = Vec::new();
        for id in DATA {
            let text = format!(
                "{}{control_settings}",
                config_with(dir, &tls, id, "data", Some(address), membership_settings)?
            );
            data.push(Daemon::start(Config::from_toml(&text)?).await?);
        }
        let fleet = Self { control, data, tls };
        fleet.converge().await?;
        Ok(fleet)
    }

    /// Waits until every data daemon publishes the control plane's ring with every member live.
    async fn converge(&self) -> TestResult {
        let handle = self.control.control().ok_or("no control handle")?;
        let mut states = handle.watch();
        let state = tokio::time::timeout(
            HANG_GUARD,
            states.wait_for(|state| {
                state.members().len() == DATA.len()
                    && state.members().all(|member| {
                        state.liveness(member.id(), state.now()) == Some(Liveness::Live)
                    })
            }),
        )
        .await??
        .clone();
        let epoch = state.membership_epoch();
        let fingerprint = state.ring()?.fingerprint();
        for daemon in &self.data {
            let membership = daemon.membership().ok_or("no membership")?;
            tokio::time::timeout(
                HANG_GUARD,
                membership.wait_for(|view| {
                    view.epoch() >= epoch
                        && view.fingerprint() == Some(fingerprint)
                        && view.members().all(|(_, live)| live == Liveness::Live)
                }),
            )
            .await?
            .ok_or("membership stopped")?;
        }
        Ok(())
    }

    fn control_address(&self) -> Result<String, Box<dyn Error>> {
        Ok(self
            .control
            .control_address()
            .ok_or("no control address")?
            .to_string())
    }

    fn client(&self) -> Result<Client, Box<dyn Error>> {
        Ok(Client::new(self.control_address()?, network(&self.tls)?))
    }

    fn data_daemon(&self, id: &str) -> Result<&Daemon, Box<dyn Error>> {
        self.data
            .iter()
            .find(|daemon| daemon.config().node.id.as_str() == id)
            .ok_or_else(|| format!("no data daemon {id}").into())
    }

    async fn stop(self) {
        for daemon in self.data {
            daemon.run_until(async {}).await;
        }
        self.control.run_until(async {}).await;
    }
}

/// The network the tests' clients use: real TCP as the principal `tester`, with a request
/// timeout that outlasts a slow runner. Creating a database waits for every follower to connect
/// and bootstrap, which can take several seconds on a loaded machine, and the tests have their
/// own hang guards.
fn network(tls: &Tls) -> Result<Network, Box<dyn Error>> {
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

fn three() -> Result<ReplicationFactor, Box<dyn Error>> {
    Ok(ReplicationFactor::new(3).ok_or("zero")?)
}

/// The address of the first follower of `location`.
fn follower_address(location: &Location) -> Result<String, Box<dyn Error>> {
    Ok(location
        .followers()
        .next()
        .and_then(|follower| follower.address.clone())
        .ok_or("no follower with an address")?)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn create_exec_query_and_locate_work_end_to_end() -> TestResult {
    let dir = tempfile::tempdir()?;
    let fleet = Fleet::start(dir.path()).await?;
    let client = fleet.client()?;
    let acme = name("tenant:acme")?;

    let created = client.create_database(&acme, three()?).await?;
    let primary = created.primary.clone().ok_or("no primary after create")?;
    assert_eq!(created.epoch.get(), 1);
    assert_eq!(created.replicas.len(), 3);
    assert_eq!(created.followers().count(), 2);
    // The primary is the first live member of the replica set in the ring's preference order.
    assert_eq!(created.replicas[0].node, primary);
    let registry = fleet
        .data_daemon(primary.as_str())?
        .registry()
        .ok_or("no registry")?;
    assert!(registry.is_open(&acme), "the primary opened its database");
    for follower in created.followers() {
        let registry = fleet
            .data_daemon(follower.node.as_str())?
            .registry()
            .ok_or("no registry")?;
        assert!(
            !registry.is_open(&acme),
            "followers get copies from replication"
        );
    }

    let table = client
        .execute(
            &acme,
            Statement::new("CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT, score REAL)"),
        )
        .await?;
    assert_eq!(table.rows_affected, 0);
    let written = client
        .execute(
            &acme,
            Statement::with_params(
                "INSERT INTO notes (body, score) VALUES (?, ?), (?, ?)",
                [
                    Value::from("hello"),
                    Value::Real(0.5),
                    Value::from("world"),
                    Value::Null,
                ],
            ),
        )
        .await?;
    assert_eq!(written.rows_affected, 2);
    assert!(
        written.acknowledged_by >= 2,
        "a write quorum of 2 holds it: {written:?}"
    );
    assert_eq!(written.replicas, 3);
    assert!(!written.under_replicated, "every replica is live");

    let rows = client
        .query(
            &acme,
            Statement::with_params(
                "SELECT id, body, score FROM notes WHERE id >= ? ORDER BY id",
                [Value::Integer(1)],
            ),
        )
        .await?;
    assert_eq!(rows.columns, ["id", "body", "score"]);
    assert_eq!(
        rows.rows,
        vec![
            vec![Value::Integer(1), Value::from("hello"), Value::Real(0.5)],
            vec![Value::Integer(2), Value::from("world"), Value::Null],
        ]
    );

    let located = client.locate(&acme).await?;
    assert_eq!(located.primary.as_ref(), Some(&primary));
    assert_eq!(located.epoch.get(), 1);
    let primary_address = fleet
        .data_daemon(primary.as_str())?
        .listen_address()
        .to_string();
    assert_eq!(
        located
            .primary_replica()
            .and_then(|replica| replica.address.as_deref()),
        Some(primary_address.as_str())
    );

    // A database with one replica isn't under-replicated.
    let solo = name("solo")?;
    client
        .create_database(&solo, ReplicationFactor::ONE)
        .await?;
    client
        .execute(&solo, Statement::new("CREATE TABLE t (x)"))
        .await?;
    let written = client
        .execute(&solo, Statement::new("INSERT INTO t VALUES (1)"))
        .await?;
    assert_eq!((written.acknowledged_by, written.replicas), (1, 1));
    assert!(!written.under_replicated);

    fleet.stop().await;
    Ok(())
}

/// The database file of `database` in data daemon `id`'s data directory.
fn database_file(dir: &Path, id: &str, database: &str) -> PathBuf {
    dir.join(id)
        .join("databases")
        .join(database)
        .join("data.db")
}

/// The values of table `notes` in a scratch copy of the SQLite files at `path`, so that the
/// daemon's own files stay untouched.
fn notes_in(path: &Path) -> Result<Vec<i64>, Box<dyn Error>> {
    let scratch = tempfile::tempdir()?;
    let copy = scratch.path().join("data.db");
    std::fs::copy(path, &copy)?;
    let wal = path.with_file_name("data.db-wal");
    if wal.try_exists()? {
        std::fs::copy(&wal, scratch.path().join("data.db-wal"))?;
    }
    let connection = rusqlite::Connection::open(&copy)?;
    let mut statement = connection.prepare("SELECT n FROM notes ORDER BY n")?;
    let values = statement
        .query_map([], |row| row.get::<_, i64>(0))?
        .collect::<Result<Vec<i64>, _>>()?;
    Ok(values)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn writes_replicate_to_every_follower_through_the_daemons() -> TestResult {
    const ROWS: i64 = 40;
    let dir = tempfile::tempdir()?;
    let fleet = Fleet::start(dir.path()).await?;
    let client = fleet.client()?;
    let orders = name("orders")?;

    let created = client.create_database(&orders, three()?).await?;
    let primary = created.primary.clone().ok_or("no primary after create")?;
    client
        .execute(
            &orders,
            Statement::new("CREATE TABLE notes (n INTEGER PRIMARY KEY, body TEXT)"),
        )
        .await?;
    for n in 0..ROWS {
        let written = client
            .execute(
                &orders,
                Statement::with_params(
                    "INSERT INTO notes (n, body) VALUES (?, ?)",
                    [Value::Integer(n), Value::from(format!("note {n}"))],
                ),
            )
            .await?;
        assert!(
            written.acknowledged_by >= 2 && written.acknowledged_by <= 3,
            "write {n} reached a quorum of 2 of 3: {written:?}"
        );
        assert_eq!(written.replicas, 3);
        assert!(!written.under_replicated, "write {n}: {written:?}");
    }

    // Every follower acknowledges everything, then the primary checkpoints and ships the marker.
    let replication = fleet
        .data_daemon(primary.as_str())?
        .replication()
        .ok_or("no replication on the primary")?;
    tokio::time::timeout(HANG_GUARD, async {
        loop {
            let caught_up = replication.primary_status(&orders).is_some_and(|status| {
                status.committed.is_some()
                    && status.followers.len() == 2
                    && status
                        .followers
                        .iter()
                        .all(|follower| follower.acknowledged == status.committed)
            });
            if caught_up {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await?;
    let marker = replication.checkpoint(&orders).await?;
    for follower in created.followers() {
        let replication = fleet
            .data_daemon(follower.node.as_str())?
            .replication()
            .ok_or("no replication on a follower")?;
        tokio::time::timeout(HANG_GUARD, async {
            while !replication
                .follower_status(&orders)
                .is_some_and(|status| status.checkpointed.as_ref() == Some(&marker.position))
            {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await?;
    }

    let expected: Vec<i64> = (0..ROWS).collect();
    let primary_file = database_file(dir.path(), primary.as_str(), "orders");
    let primary_bytes = std::fs::read(&primary_file)?;
    for follower in created.followers() {
        let file = database_file(dir.path(), follower.node.as_str(), "orders");
        assert_eq!(notes_in(&file)?, expected, "{}'s rows", follower.node);
        assert!(
            std::fs::read(&file)? == primary_bytes,
            "{}'s copy differs from the primary's after the checkpoint",
            follower.node
        );
    }

    // Heartbeats carry every replica's role and position to the control plane.
    let located = tokio::time::timeout(HANG_GUARD, async {
        loop {
            let located = client.locate(&orders).await?;
            if located
                .replicas
                .iter()
                .all(|replica| replica.position.is_some())
            {
                return Ok::<_, tonic::Status>(located);
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await??;
    for replica in &located.replicas {
        let role = if replica.node == primary {
            ReplicaRole::Primary
        } else {
            ReplicaRole::Follower
        };
        assert_eq!(replica.reported_role, Some(role), "{}", replica.node);
    }

    fleet.stop().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_request_to_a_follower_is_redirected_to_the_primary() -> TestResult {
    let dir = tempfile::tempdir()?;
    let fleet = Fleet::start(dir.path()).await?;
    let client = fleet.client()?;
    let acme = name("acme")?;
    let location = client.create_database(&acme, three()?).await?;
    let primary = location.primary.clone().ok_or("no primary")?;
    let primary_address = fleet
        .data_daemon(primary.as_str())?
        .listen_address()
        .to_string();
    let follower = follower_address(&location)?;

    // Sent straight to a follower, a write is answered with the primary's address.
    let mut raw = database_client(network(&fleet.tls)?.channel(&follower)?);
    let redirect = raw
        .execute(ExecuteRequest {
            database: acme.to_string(),
            sql: "CREATE TABLE t (x)".into(),
            params: Vec::new(),
        })
        .await
        .expect_err("a follower doesn't run writes");
    assert_eq!(redirect.code(), Code::Unavailable, "{redirect}");
    assert_eq!(primary_hint(&redirect), Some(primary_address));

    // A client that starts at the follower follows the redirect.
    let via_follower = fleet.client()?.with_entry(follower);
    via_follower
        .execute(&acme, Statement::new("CREATE TABLE t (x)"))
        .await?;
    via_follower
        .execute(
            &acme,
            Statement::with_params("INSERT INTO t VALUES (?)", [Value::Integer(7)]),
        )
        .await?;
    let rows = via_follower
        .query(&acme, Statement::new("SELECT x FROM t"))
        .await?;
    assert_eq!(rows.rows, vec![vec![Value::Integer(7)]]);

    fleet.stop().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_schema_is_served_by_the_primary_and_redirected_from_followers() -> TestResult {
    let dir = tempfile::tempdir()?;
    let fleet = Fleet::start(dir.path()).await?;
    let client = fleet.client()?;
    let acme = name("acme")?;
    let location = client.create_database(&acme, three()?).await?;
    let primary = location.primary.clone().ok_or("no primary")?;
    let primary_address = fleet
        .data_daemon(primary.as_str())?
        .listen_address()
        .to_string();
    let follower = follower_address(&location)?;

    // DDL the caller just ran shows at once: the primary answers, not a follower that may lag.
    for sql in [
        "CREATE TABLE notes (id INTEGER PRIMARY KEY, topic TEXT, body TEXT)",
        "CREATE INDEX notes_topic ON notes (topic)",
        "CREATE TRIGGER notes_seen AFTER INSERT ON notes BEGIN SELECT 1; END",
        "CREATE VIEW recent AS SELECT body FROM notes ORDER BY id DESC",
        "INSERT INTO notes (topic, body) VALUES ('a', 'x'), ('b', 'y'), ('c', 'z')",
    ] {
        client.execute(&acme, Statement::new(sql)).await?;
    }
    let request = |count_limit| SchemaRequest {
        count_limit,
        internal: Internal::Hide,
    };

    // Sent straight to a follower, the call is answered with the primary's address.
    let mut raw = database_client(network(&fleet.tls)?.channel(&follower)?);
    let redirect = raw
        .schema(proto::SchemaRequest {
            database: acme.to_string(),
            count_limit: 10,
            include_internal: false,
        })
        .await
        .expect_err("a follower doesn't answer");
    assert_eq!(redirect.code(), Code::Unavailable, "{redirect}");
    assert_eq!(primary_hint(&redirect), Some(primary_address));

    // A client that starts at the follower follows the redirect.
    let via_follower = fleet.client()?.with_entry(follower);
    for asking in [&client, &via_follower] {
        let view = asking.schema(&acme, request(10)).await?;
        assert_eq!(view.primary.as_ref(), Some(&primary));
        assert_eq!(view.epoch, location.epoch);
        assert!(!view.schema.truncated);
        let objects: Vec<(ObjectKind, &str)> = view
            .schema
            .objects
            .iter()
            .map(|object| (object.kind, object.name.as_str()))
            .collect();
        assert_eq!(
            objects,
            [
                (ObjectKind::Table, "notes"),
                (ObjectKind::Index, "notes_topic"),
                (ObjectKind::Trigger, "notes_seen"),
                (ObjectKind::View, "recent"),
            ]
        );
        assert_eq!(view.schema.objects[0].rows, Some(RowCount::Exact(3)));
        assert!(
            view.schema.objects[1]
                .definition
                .as_deref()
                .is_some_and(|sql| sql.starts_with("CREATE INDEX notes_topic")),
            "{:?}",
            view.schema.objects[1]
        );
    }
    // The cap holds through the wire: the count stops at the limit.
    let capped = via_follower.schema(&acme, request(2)).await?;
    assert_eq!(capped.schema.objects[0].rows, Some(RowCount::AtLeast(2)));
    let uncounted = client.schema(&acme, request(0)).await?;
    assert_eq!(uncounted.schema.objects[0].rows, None);

    let missing = client
        .schema(&name("missing")?, request(10))
        .await
        .expect_err("no such database");
    assert_eq!(missing.code(), Code::NotFound, "{missing}");

    fleet.stop().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn errors_map_to_statuses() -> TestResult {
    let dir = tempfile::tempdir()?;
    let fleet = Fleet::start(dir.path()).await?;
    let client = fleet.client()?;
    let acme = name("acme")?;
    client.create_database(&acme, three()?).await?;

    let unknown = client
        .execute(&name("missing")?, Statement::new("SELECT 1"))
        .await
        .expect_err("no such database");
    assert_eq!(unknown.code(), Code::NotFound, "{unknown}");

    let syntax = client
        .execute(&acme, Statement::new("CREATE TABLEX"))
        .await
        .expect_err("bad SQL");
    assert_eq!(syntax.code(), Code::InvalidArgument, "{syntax}");

    let transaction = client
        .execute(&acme, Statement::new("BEGIN"))
        .await
        .expect_err("statements can't control transactions");
    assert_eq!(transaction.code(), Code::InvalidArgument, "{transaction}");

    let twice = client
        .create_database(&acme, three()?)
        .await
        .expect_err("already created");
    assert_eq!(twice.code(), Code::AlreadyExists, "{twice}");

    fleet.stop().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn status_shows_the_ring_every_node_publishes() -> TestResult {
    let dir = tempfile::tempdir()?;
    let fleet = Fleet::start(dir.path()).await?;
    let view = fleet.client()?.status().await?;

    let state = fleet.control.control().ok_or("no control")?.state();
    let fingerprint = state.ring()?.fingerprint();
    assert_eq!(view.fingerprint(), Some(fingerprint));
    assert_eq!(view.epoch(), state.membership_epoch());
    for daemon in &fleet.data {
        let published = daemon.membership().ok_or("no membership")?.view();
        assert_eq!(published.fingerprint(), Some(fingerprint));
    }
    let members: Vec<(String, Option<String>, Liveness)> = view
        .members()
        .map(|(member, live)| {
            (
                member.id().to_string(),
                member.address().map(str::to_owned),
                live,
            )
        })
        .collect();
    let expected: Vec<(String, Option<String>, Liveness)> = fleet
        .data
        .iter()
        .map(|daemon| {
            (
                daemon.config().node.id.to_string(),
                Some(daemon.listen_address().to_string()),
                Liveness::Live,
            )
        })
        .collect();
    assert_eq!(
        members, expected,
        "members advertise their listen addresses"
    );

    fleet.stop().await;
    Ok(())
}

/// A node that stays a member across a quick restart (`on_shutdown = "stay"`) keeps its leases:
/// it doesn't drain, and serves its databases again once it is back.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_restarted_primary_serves_its_database_again() -> TestResult {
    let dir = tempfile::tempdir()?;
    let Fleet {
        control,
        mut data,
        tls,
    } = Fleet::start_configured(dir.path(), "", "", "on_shutdown = \"stay\"\n").await?;
    let fleet_control = control.control_address().ok_or("no control address")?;
    let client = Client::new(fleet_control.to_string(), network(&tls)?);
    let acme = name("acme")?;
    let location = client.create_database(&acme, three()?).await?;
    let primary = location.primary.clone().ok_or("no primary")?;
    client
        .execute(&acme, Statement::new("CREATE TABLE t (x)"))
        .await?;
    client
        .execute(&acme, Statement::new("INSERT INTO t VALUES (1)"))
        .await?;

    // Restart the primary on a new port: it rejoins with its new address.
    let index = data
        .iter()
        .position(|daemon| daemon.config().node.id == primary)
        .ok_or("primary isn't a data daemon")?;
    let old = data.remove(index);
    let config = old.config().clone();
    old.run_until(async {}).await;
    let restarted = Daemon::start(config).await?;
    let new_address = restarted.listen_address().to_string();
    data.push(restarted);
    let fleet = Fleet { control, data, tls };
    fleet.converge().await?;
    tokio::time::timeout(HANG_GUARD, async {
        loop {
            let location = client.locate(&acme).await?;
            if location
                .primary_replica()
                .and_then(|replica| replica.address.as_deref())
                == Some(new_address.as_str())
            {
                return Ok::<_, tonic::Status>(());
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await??;

    client
        .execute(&acme, Statement::new("INSERT INTO t VALUES (2)"))
        .await?;
    let rows = client
        .query(&acme, Statement::new("SELECT x FROM t ORDER BY x"))
        .await?;
    assert_eq!(
        rows.rows,
        vec![vec![Value::Integer(1)], vec![Value::Integer(2)]]
    );

    fleet.stop().await;
    Ok(())
}

/// Timings short enough for a failover to finish quickly over loopback.
const FAST_LEASES: &str = "lease_duration_ms = 800\nfailover_margin_ms = 100\n\
                           heartbeat_timeout_ms = 500\ntick_interval_ms = 20\n";

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn db_exec_keeps_working_through_a_primary_failover_after_a_retry() -> TestResult {
    let dir = tempfile::tempdir()?;
    let fleet = Fleet::start_with(dir.path(), FAST_LEASES).await?;
    let client = fleet.client()?;
    let acme = name("acme")?;
    let location = client.create_database(&acme, three()?).await?;
    let old = location.primary.clone().ok_or("no primary")?;
    client
        .execute(&acme, Statement::new("CREATE TABLE t (x)"))
        .await?;
    let first = client
        .execute(&acme, Statement::new("INSERT INTO t VALUES (1)"))
        .await?;
    assert_eq!(first.primary.as_ref(), Some(&old));

    // The primary goes away; the next write fails until a follower takes over, then succeeds
    // when retried, as `fleetringd db exec` would be.
    let Fleet {
        control,
        mut data,
        tls,
    } = fleet;
    let index = data
        .iter()
        .position(|daemon| daemon.config().node.id == old)
        .ok_or("primary isn't a data daemon")?;
    data.remove(index).run_until(async {}).await;
    let written = tokio::time::timeout(HANG_GUARD, async {
        loop {
            match client
                .execute(&acme, Statement::new("INSERT INTO t VALUES (2)"))
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
    let new = written.primary.clone().ok_or("no primary named")?;
    assert_ne!(new, old);
    // Usually 2. A promotion that stalls past the lease and the margin, as one can on a loaded
    // machine, is replaced by another at the next epoch.
    assert!(written.epoch.get() >= 2, "epoch {}", written.epoch.get());
    let rows = client
        .query(&acme, Statement::new("SELECT DISTINCT x FROM t ORDER BY x"))
        .await?;
    assert_eq!(
        rows.rows,
        vec![vec![Value::Integer(1)], vec![Value::Integer(2)]]
    );
    Fleet { control, data, tls }.stop().await;
    Ok(())
}

/// A database's metadata lives in control state, so a failover, which moves the lease and
/// nothing else, leaves it as it was (ADR-0041).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn metadata_is_unchanged_by_a_failover() -> TestResult {
    use fleetring_control::{Labels, MetadataChange, parse_label};
    use fleetringd::data::NewMetadata;

    let dir = tempfile::tempdir()?;
    let fleet = Fleet::start_with(dir.path(), FAST_LEASES).await?;
    let client = fleet.client()?;
    let acme = name("acme")?;
    let created = client
        .create_database_with_metadata(
            &acme,
            DatabaseSpec::new(three()?),
            &NewMetadata {
                description: Some("Orders for tenant acme.\nTable: orders.".parse()?),
                labels: Labels::try_from_pairs([parse_label("purpose=memory")?])?,
                ..NewMetadata::default()
            },
        )
        .await?;
    let old = created.primary.clone().ok_or("no primary")?;
    client
        .update_database(
            &acme,
            &MetadataChange {
                set_labels: Labels::try_from_pairs([parse_label("tier=gold")?])?,
                ..MetadataChange::default()
            },
            Some(1),
        )
        .await?;
    client
        .execute(&acme, Statement::new("CREATE TABLE t (x)"))
        .await?;
    let before = client.locate(&acme).await?.metadata;
    let labels = before.as_ref().map(|metadata| metadata.labels.clone());
    assert_eq!(
        labels,
        Some(vec![
            ("purpose".to_owned(), "memory".to_owned()),
            ("tier".to_owned(), "gold".to_owned()),
        ])
    );

    let Fleet {
        control,
        mut data,
        tls,
    } = fleet;
    let index = data
        .iter()
        .position(|daemon| daemon.config().node.id == old)
        .ok_or("primary isn't a data daemon")?;
    data.remove(index).run_until(async {}).await;
    let written = tokio::time::timeout(HANG_GUARD, async {
        loop {
            match client
                .execute(&acme, Statement::new("INSERT INTO t VALUES (1)"))
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
    assert_ne!(written.primary.as_ref(), Some(&old));
    let after = client.locate(&acme).await?;
    assert!(after.epoch.get() >= 2, "epoch {}", after.epoch.get());
    assert_eq!(after.metadata, before);
    Fleet { control, data, tls }.stop().await;
    Ok(())
}

/// Waits until `condition` holds, polling every 20 ms, or fails after the hang guard.
async fn wait_until(what: &str, mut condition: impl AsyncFnMut() -> bool) -> TestResult {
    let waited = tokio::time::timeout(HANG_GUARD, async {
        while !condition().await {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
    waited.map_err(|_| format!("timed out waiting until {what}").into())
}

/// The directory of `database`'s copy in data daemon `id`'s data directory.
fn database_dir(dir: &Path, id: &str, database: &str) -> PathBuf {
    dir.join(id).join("databases").join(database)
}

/// Whether every data daemon has a copy of `database` on disk.
fn every_node_has(dir: &Path, database: &str) -> bool {
    DATA.iter()
        .all(|id| database_file(dir, id, database).is_file())
}

/// Whether no data daemon has any trace of `database` on disk: its copy or its epoch record.
fn no_node_has(dir: &Path, database: &str) -> bool {
    DATA.iter().all(|id| {
        !database_dir(dir, id, database).exists()
            && !dir
                .join(id)
                .join("replication")
                .join("epochs")
                .join(database)
                .exists()
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn db_list_shows_every_database_with_its_primary_and_filters_by_node() -> TestResult {
    let dir = tempfile::tempdir()?;
    let fleet = Fleet::start(dir.path()).await?;
    let client = fleet.client()?;
    assert!(client.list_databases().await?.is_empty());

    let names = ["orders", "acme", "notes"];
    let mut created = Vec::new();
    for text in names {
        created.push(client.create_database(&name(text)?, three()?).await?);
    }
    let listed = client.list_databases().await?;
    let mut sorted = names.to_vec();
    sorted.sort_unstable();
    let listed_names: Vec<String> = listed.iter().map(|l| l.database.to_string()).collect();
    assert_eq!(listed_names, sorted, "sorted by name");
    for location in &listed {
        let made = created
            .iter()
            .find(|c| c.database == location.database)
            .ok_or("listed a database that wasn't created")?;
        assert_eq!(location.primary, made.primary, "{}", location.database);
        assert_eq!(location.epoch.get(), 1, "{}", location.database);
        assert_eq!(location.replicas.len(), 3, "{}", location.database);
        let replicas: Vec<_> = location.replicas.iter().map(|r| &r.node).collect();
        let expected: Vec<_> = made.replicas.iter().map(|r| &r.node).collect();
        assert_eq!(replicas, expected, "{}", location.database);
    }

    // `--node` keeps the databases a node is primary for.
    let node = created[0].primary.clone().ok_or("no primary")?;
    let mine: Vec<&Location> = listed
        .iter()
        .filter(|location| location.primary.as_ref() == Some(&node))
        .collect();
    assert!(!mine.is_empty());
    for location in &mine {
        assert_eq!(
            client.locate(&location.database).await?.primary,
            Some(node.clone())
        );
    }

    fleet.stop().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dropping_a_database_removes_it_from_every_node() -> TestResult {
    let dir = tempfile::tempdir()?;
    let fleet = Fleet::start_with(dir.path(), FAST_LEASES).await?;
    let client = fleet.client()?;
    let orders = name("orders")?;
    let kept = name("kept")?;
    let created = client.create_database(&orders, three()?).await?;
    let primary = created.primary.clone().ok_or("no primary")?;
    client.create_database(&kept, three()?).await?;
    for database in [&orders, &kept] {
        client
            .execute(database, Statement::new("CREATE TABLE t (x)"))
            .await?;
        client
            .execute(database, Statement::new("INSERT INTO t VALUES (1)"))
            .await?;
    }
    wait_until("every node holds a copy of orders", async || {
        every_node_has(dir.path(), "orders") && every_node_has(dir.path(), "kept")
    })
    .await?;
    let primary_registry = fleet
        .data_daemon(primary.as_str())?
        .registry()
        .ok_or("no registry")?;
    assert!(primary_registry.is_open(&orders));

    // The drop is a control command; the control plane forgets the database at once.
    let epoch = client.drop_database(&orders).await?;
    assert_eq!(epoch.get(), 1);
    let gone = client.locate(&orders).await.expect_err("dropped");
    assert_eq!(gone.code(), Code::NotFound, "{gone}");
    let listed: Vec<String> = client
        .list_databases()
        .await?
        .iter()
        .map(|location| location.database.to_string())
        .collect();
    assert_eq!(listed, ["kept"]);
    let again = client
        .drop_database(&orders)
        .await
        .expect_err("already dropped");
    assert_eq!(again.code(), Code::NotFound, "{again}");

    // The primary stops at its next renewal, the followers' streams end, and every node deletes
    // its copy and its epoch record.
    wait_until("every node deleted its copy of orders", async || {
        no_node_has(dir.path(), "orders")
    })
    .await?;
    assert!(
        !primary_registry.is_open(&orders),
        "the primary closed it first"
    );
    for id in DATA {
        let replication = fleet
            .data_daemon(id)?
            .replication()
            .ok_or("no replication")?;
        assert_eq!(
            replication.serving(&orders),
            None,
            "{id} still serves orders"
        );
        assert_eq!(
            replication.highest_epoch(&orders),
            None,
            "{id} remembers an epoch"
        );
    }

    // A write to a dropped database finds no database, at the control plane and at its old
    // primary alike; the other database is untouched.
    let refused = client
        .execute(&orders, Statement::new("INSERT INTO t VALUES (2)"))
        .await
        .expect_err("dropped");
    assert_eq!(refused.code(), Code::NotFound, "{refused}");
    let old_primary = fleet
        .data_daemon(primary.as_str())?
        .listen_address()
        .to_string();
    let at_primary = fleet
        .client()?
        .with_entry(old_primary)
        .execute(&orders, Statement::new("INSERT INTO t VALUES (2)"))
        .await
        .expect_err("dropped");
    assert_eq!(at_primary.code(), Code::NotFound, "{at_primary}");
    let rows = client
        .query(&kept, Statement::new("SELECT x FROM t"))
        .await?;
    assert_eq!(rows.rows, vec![vec![Value::Integer(1)]]);
    assert!(every_node_has(dir.path(), "kept"));

    // The name is free again, and the new database starts from epoch 1 on every node.
    let recreated = client.create_database(&orders, three()?).await?;
    assert_eq!(recreated.epoch.get(), 1);
    client
        .execute(&orders, Statement::new("CREATE TABLE t (y)"))
        .await?;
    let written = client
        .execute(&orders, Statement::new("INSERT INTO t VALUES (7)"))
        .await?;
    assert!(written.acknowledged_by >= 2, "{written:?}");
    wait_until("every node holds the new orders", async || {
        every_node_has(dir.path(), "orders")
    })
    .await?;

    fleet.stop().await;
    Ok(())
}

/// The bytes of `database`'s files in data daemon `id`'s directory: the main file and its log.
fn database_bytes(dir: &Path, id: &str, database: &str) -> Result<Vec<u8>, Box<dyn Error>> {
    let file = database_file(dir, id, database);
    let mut bytes = std::fs::read(&file)?;
    let wal = file.with_file_name("data.db-wal");
    if wal.try_exists()? {
        bytes.extend(std::fs::read(wal)?);
    }
    Ok(bytes)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn writes_below_the_replica_floor_are_refused_with_unavailable() -> TestResult {
    let dir = tempfile::tempdir()?;
    let Fleet {
        control,
        mut data,
        tls,
    } = Fleet::start(dir.path()).await?;
    let client = Client::new(
        control
            .control_address()
            .ok_or("no control address")?
            .to_string(),
        network(&tls)?,
    );
    let strict = name("strict")?;
    let spec = DatabaseSpec::new(three()?).with_min_replicas(3);
    let created = client.create_database_with(&strict, spec).await?;
    assert_eq!(
        created.durability,
        Durability::with_min_replicas(Acks::Quorum, 3, three()?)?
    );
    assert_eq!(client.locate(&strict).await?.durability, created.durability);
    let primary = created.primary.clone().ok_or("no primary")?;
    client
        .execute(&strict, Statement::new("CREATE TABLE t (x)"))
        .await?;
    let written = client
        .execute(&strict, Statement::new("INSERT INTO t VALUES (1)"))
        .await?;
    assert!(!written.under_replicated, "{written:?}");

    // One follower stops: a majority is still up, but the floor of 3 isn't met, so the primary
    // refuses writes before running them, and its files don't change.
    let follower = created
        .followers()
        .next()
        .map(|replica| replica.node.clone())
        .ok_or("no follower")?;
    let index = data
        .iter()
        .position(|daemon| daemon.config().node.id == follower)
        .ok_or("follower isn't a data daemon")?;
    data.remove(index).run_until(async {}).await;
    let refused = tokio::time::timeout(HANG_GUARD, async {
        loop {
            match client
                .execute(&strict, Statement::new("INSERT INTO t VALUES (2)"))
                .await
            {
                Err(status) if status.code() == Code::Unavailable => return status,
                // A write that slipped in before the primary noticed the stop.
                Ok(_) | Err(_) => tokio::time::sleep(Duration::from_millis(20)).await,
            }
        }
    })
    .await?;
    assert!(refused.message().contains("nothing committed"), "{refused}");
    let before = database_bytes(dir.path(), primary.as_str(), "strict")?;
    for _ in 0..3 {
        let again = client
            .execute(&strict, Statement::new("INSERT INTO t VALUES (3)"))
            .await
            .expect_err("still below the floor");
        assert_eq!(again.code(), Code::Unavailable, "{again}");
    }
    assert_eq!(
        database_bytes(dir.path(), primary.as_str(), "strict")?,
        before,
        "refused writes changed the primary's files"
    );
    let rows = client
        .query(
            &strict,
            Statement::new("SELECT count(*) FROM t WHERE x > 1"),
        )
        .await?;
    assert_eq!(rows.rows, vec![vec![Value::Integer(0)]]);
    Fleet { control, data, tls }.stop().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn databases_take_the_fleet_default_durability_unless_told_otherwise() -> TestResult {
    let dir = tempfile::tempdir()?;
    let fleet = Fleet::start_configured(
        dir.path(),
        "",
        "[replication]\nacks = \"all\"\nmin_replicas = 9\n",
        "",
    )
    .await?;
    let client = fleet.client()?;
    // The default mode applies, and a default floor past R is clamped to R.
    let everyone = name("everyone")?;
    let created = client.create_database(&everyone, three()?).await?;
    assert_eq!(created.durability, Durability::new(Acks::All, three()?));
    assert_eq!(created.durability.min_replicas(), 3);
    let written = client
        .execute(&everyone, Statement::new("CREATE TABLE t (x)"))
        .await?;
    assert_eq!(written.acknowledged_by, 3, "{written:?}");
    // An explicit mode wins, the default floor still applies, and an explicit floor that
    // doesn't fit is refused.
    let alone = name("alone")?;
    let spec = DatabaseSpec::new(three()?).with_acks(Acks::Local);
    let created = client.create_database_with(&alone, spec).await?;
    assert_eq!(
        created.durability,
        Durability::with_min_replicas(Acks::Local, 3, three()?)?
    );
    let spec = DatabaseSpec::new(three()?)
        .with_acks(Acks::Local)
        .with_min_replicas(1);
    let created = client.create_database_with(&name("lax")?, spec).await?;
    assert_eq!(created.durability, Durability::new(Acks::Local, three()?));
    let bad = DatabaseSpec::new(three()?)
        .with_acks(Acks::All)
        .with_min_replicas(2);
    let refused = client
        .create_database_with(&name("bad")?, bad)
        .await
        .expect_err("a floor below W");
    assert_eq!(refused.code(), Code::InvalidArgument, "{refused}");
    assert!(client.locate(&name("bad")?).await.is_err());
    let listed: Vec<(String, Acks, u8)> = client
        .list_databases()
        .await?
        .iter()
        .map(|location| {
            (
                location.database.to_string(),
                location.durability.acks(),
                location.durability.min_replicas(),
            )
        })
        .collect();
    assert_eq!(
        listed,
        vec![
            ("alone".to_owned(), Acks::Local, 3),
            ("everyone".to_owned(), Acks::All, 3),
            ("lax".to_owned(), Acks::Local, 1)
        ]
    );
    // A change is recorded for the next epoch; the current primary keeps its settings.
    let change = client
        .change_durability(&everyone, Acks::Quorum, None)
        .await?;
    assert_eq!((change.acks, change.min_replicas), (Acks::Quorum, 2));
    assert_eq!(
        (change.current_acks, change.current_min_replicas),
        (Acks::All, 3)
    );
    assert_eq!(
        client.locate(&everyone).await?.durability,
        Durability::new(Acks::All, three()?)
    );
    fleet.stop().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn status_node_shows_what_each_data_node_knows() -> TestResult {
    let dir = tempfile::tempdir()?;
    let fleet = Fleet::start(dir.path()).await?;
    let client = fleet.client()?;
    let acme = name("acme")?;
    let created = client.create_database(&acme, three()?).await?;
    let primary = created.primary.clone().ok_or("no primary")?;
    client
        .execute(&acme, Statement::new("CREATE TABLE t (x)"))
        .await?;
    wait_until("every node holds a copy of acme", async || {
        every_node_has(dir.path(), "acme")
    })
    .await?;

    let state = fleet.control.control().ok_or("no control")?.state();
    let fingerprint = state.ring()?.fingerprint();
    for daemon in &fleet.data {
        let address = daemon.listen_address().to_string();
        let id = daemon.config().node.id.clone();
        let status = client.node_status(&address).await?;
        assert_eq!(status.node, id);
        assert_eq!(status.fingerprint, Some(fingerprint), "{id}");
        assert_eq!(status.membership_epoch, state.membership_epoch(), "{id}");
        // Followers report their copy once a stream has opened it, which may take a moment.
        wait_until("the node reports acme", async || {
            client
                .node_status(&address)
                .await
                .is_ok_and(|status| status.databases.iter().any(|d| d.database == acme))
        })
        .await?;
        let status = client.node_status(&address).await?;
        let served = status
            .databases
            .iter()
            .find(|d| d.database == acme)
            .ok_or("acme not served")?;
        let role = if id == primary {
            ReplicaRole::Primary
        } else {
            ReplicaRole::Follower
        };
        assert_eq!(served.role, role, "{id}");
        assert_eq!(served.epoch.get(), 1, "{id}");
    }

    // The control node hosts no data, so it has no node status to give.
    let control = fleet.control_address()?;
    let refused = client
        .node_status(&control)
        .await
        .expect_err("no data role");
    assert_eq!(refused.code(), Code::Unimplemented, "{refused}");

    fleet.stop().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_control_call_to_a_data_node_names_the_control_node() -> TestResult {
    let dir = tempfile::tempdir()?;
    let fleet = Fleet::start(dir.path()).await?;
    let control = fleet.control_address()?;
    for daemon in &fleet.data {
        let data_node = daemon.listen_address().to_string();
        let wrong = Client::new(data_node, network(&fleet.tls)?);
        let refused = wrong
            .locate(&name("acme")?)
            .await
            .expect_err("a data node runs no control plane");
        assert_eq!(refused.code(), Code::FailedPrecondition, "{refused}");
        assert_eq!(
            refused.message(),
            format!("this node runs no control plane; the control node is {control}")
        );
        let listed = wrong.list_databases().await.expect_err("same for a list");
        assert_eq!(listed.code(), Code::FailedPrecondition, "{listed}");
    }
    fleet.stop().await;
    Ok(())
}

#[cfg(unix)]
mod binary {
    use std::path::Path;
    use std::process::{Command, Output};

    use fleetringd::engine::{Statement, Value};

    use super::{
        FAST_LEASES, Fleet, Location, TestResult, config, database_file, every_node_has,
        follower_address, name, no_node_has, three, wait_until,
    };

    /// Runs `fleetringd` with `args` on a blocking thread, so the daemons in this process keep
    /// serving.
    /// Runs the binary with the client certificate under `tls_dir`, as `FLEETRING_TLS_DIR`
    /// names it.
    pub(super) async fn fleetringd(
        tls_dir: &Path,
        args: Vec<String>,
    ) -> Result<Output, Box<dyn std::error::Error>> {
        let tls_dir = tls_dir.to_owned();
        Ok(tokio::task::spawn_blocking(move || {
            Command::new(env!("CARGO_BIN_EXE_fleetringd"))
                .env("FLEETRING_TLS_DIR", &tls_dir)
                .args(&args)
                .output()
        })
        .await??)
    }

    /// Runs `fleetringd` with `args`, a command that streams until stopped, and returns the
    /// first line it prints, then stops it.
    async fn first_line(
        tls_dir: &Path,
        args: Vec<String>,
    ) -> Result<String, Box<dyn std::error::Error>> {
        use std::io::{BufRead as _, BufReader};
        use std::process::Stdio;

        let tls_dir = tls_dir.to_owned();
        let line = tokio::task::spawn_blocking(move || {
            let mut child = Command::new(env!("CARGO_BIN_EXE_fleetringd"))
                .env("FLEETRING_TLS_DIR", &tls_dir)
                .args(&args)
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()?;
            let mut line = String::new();
            if let Some(stdout) = child.stdout.take() {
                BufReader::new(stdout).read_line(&mut line)?;
            }
            child.kill()?;
            let output = child.wait_with_output()?;
            assert!(
                !line.is_empty(),
                "no output: {}",
                String::from_utf8_lossy(&output.stderr)
            );
            Ok::<_, std::io::Error>(line)
        });
        Ok(tokio::time::timeout(super::HANG_GUARD, line).await???)
    }

    pub(super) fn args(words: &[&str], control: &str) -> Vec<String> {
        words
            .iter()
            .map(|word| (*word).to_owned())
            .chain(["--control".to_owned(), control.to_owned()])
            .collect()
    }

    pub(super) fn stdout(output: &Output) -> Result<String, Box<dyn std::error::Error>> {
        let text = String::from_utf8(output.stdout.clone())?;
        assert!(
            output.status.success(),
            "exit {}: {text}{}",
            output.status,
            String::from_utf8_lossy(&output.stderr)
        );
        Ok(text)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_command_line_creates_writes_reads_locates_and_shows_status() -> TestResult {
        let dir = tempfile::tempdir()?;
        let fleet = Fleet::start(dir.path()).await?;
        let control = fleet.control_address()?;

        let created = stdout(
            &fleetringd(
                fleet.tls.dir(),
                args(
                    &["db", "create", "tenant:acme", "--replication-factor", "3"],
                    &control,
                ),
            )
            .await?,
        )?;
        assert!(created.starts_with("created tenant:acme\n"), "{created}");
        assert!(created.contains("epoch: 1\n"), "{created}");

        let table = stdout(
            &fleetringd(
                fleet.tls.dir(),
                args(
                    &[
                        "db",
                        "exec",
                        "tenant:acme",
                        "CREATE TABLE t (n INTEGER, s TEXT, r REAL, b BLOB, z)",
                    ],
                    &control,
                ),
            )
            .await?,
        )?;
        assert!(
            [2, 3].iter().any(|count| table
                == format!("0 rows affected; acknowledged by {count} of 3 replicas\n")),
            "{table}"
        );
        let insert = stdout(
            &fleetringd(
                fleet.tls.dir(),
                args(
                    &[
                        "db",
                        "exec",
                        "tenant:acme",
                        "INSERT INTO t VALUES (?, ?, ?, ?, ?)",
                        "-4",
                        "text:42",
                        "2.5",
                        "blob:00ff",
                        "null",
                    ],
                    &control,
                ),
            )
            .await?,
        )?;
        assert!(
            [2, 3].iter().any(|count| insert
                == format!("1 row affected; acknowledged by {count} of 3 replicas\n")),
            "{insert}"
        );

        let rows = stdout(
            &fleetringd(
                fleet.tls.dir(),
                args(
                    &["db", "query", "tenant:acme", "SELECT n, s, r, b, z FROM t"],
                    &control,
                ),
            )
            .await?,
        )?;
        assert_eq!(rows, "n\ts\tr\tb\tz\n-4\t42\t2.5\tx'00ff'\tNULL\n");

        // Through a follower, the request is redirected to the primary.
        let location = fleet.client()?.locate(&name("tenant:acme")?).await?;
        let follower = follower_address(&location)?;
        let mut via_follower = args(
            &[
                "db",
                "query",
                "tenant:acme",
                "SELECT count(*) AS rows FROM t",
            ],
            &control,
        );
        let follower_id = location
            .followers()
            .next()
            .ok_or("no follower")?
            .node
            .to_string();
        let mut by_id = via_follower.clone();
        via_follower.extend(["--address".to_owned(), follower.clone()]);
        assert_eq!(
            stdout(&fleetringd(fleet.tls.dir(), via_follower).await?)?,
            "rows\n1\n"
        );
        // `--node` takes the follower's ID, and the control plane's membership its address.
        by_id.extend(["--node".to_owned(), follower_id]);
        assert_eq!(
            stdout(&fleetringd(fleet.tls.dir(), by_id).await?)?,
            "rows\n1\n"
        );
        // An address where a node ID belongs names no member, and the error says what to use.
        let mut by_address = args(&["db", "exec", "tenant:acme", "DELETE FROM t"], &control);
        by_address.extend(["--node".to_owned(), follower.clone()]);
        let refused = fleetringd(fleet.tls.dir(), by_address).await?;
        assert!(!refused.status.success());
        let stderr = String::from_utf8_lossy(&refused.stderr);
        assert!(
            stderr.contains(&format!(
                "no member of the fleet has the node ID '{follower}'"
            )) && stderr.contains(&format!(
                "hint: --node takes a node ID; to dial an address, use --address {follower}"
            )),
            "{stderr}"
        );

        let located = stdout(
            &fleetringd(
                fleet.tls.dir(),
                args(&["db", "locate", "tenant:acme"], &control),
            )
            .await?,
        )?;
        let primary = location.primary.ok_or("no primary")?;
        assert!(
            located.contains(&format!("primary: {primary} (")),
            "{located}"
        );
        assert!(located.contains("engine: sqlite\n"), "{located}");
        assert!(
            located.contains("acks: quorum\nmin replicas: 2\n"),
            "{located}"
        );
        assert!(located.contains("positions:\n"), "{located}");

        let status = stdout(&fleetringd(fleet.tls.dir(), args(&["status"], &control)).await?)?;
        let fingerprint = fleet
            .control
            .control()
            .ok_or("no control")?
            .state()
            .ring()?
            .fingerprint();
        assert!(
            status.contains(&format!("ring fingerprint: {fingerprint}\n")),
            "{status}"
        );
        for daemon in &fleet.data {
            let line = format!(
                "  {} live weight=1 address={} version={}\n",
                daemon.config().node.id,
                daemon.listen_address(),
                fleetringd::software::VERSION
            );
            assert!(status.contains(&line), "{line} in {status}");
        }

        // A daemon's configuration file names the control plane too.
        let file = dir.path().join("data.toml");
        let address = fleet
            .control
            .control_address()
            .ok_or("no control address")?;
        std::fs::write(
            &file,
            config(dir.path(), &fleet.tls, "data-9", "data", Some(address))?,
        )?;
        let by_config = fleetringd(
            fleet.tls.dir(),
            vec![
                "status".to_owned(),
                "--config".to_owned(),
                file.display().to_string(),
            ],
        )
        .await?;
        assert_eq!(stdout(&by_config)?, status);

        let failed = fleetringd(
            fleet.tls.dir(),
            args(&["db", "exec", "tenant:acme", "NOT SQL"], &control),
        )
        .await?;
        assert!(!failed.status.success());
        let stderr = String::from_utf8_lossy(&failed.stderr);
        assert!(
            stderr.contains("Client specified an invalid argument"),
            "{stderr}"
        );

        fleet.stop().await;
        Ok(())
    }

    /// FR-044: `db verify` prints one hash and that every replica agrees, confirms a hash
    /// recorded earlier, and names the replica whose log changed since.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_command_line_verifies_every_replicas_hash_chain() -> TestResult {
        let dir = tempfile::tempdir()?;
        let fleet = Fleet::start(dir.path()).await?;
        let control = fleet.control_address()?;
        let client = fleet.client()?;
        let ledger = name("ledger")?;
        let created = client.create_database(&ledger, three()?).await?;
        client
            .execute(&ledger, Statement::new("CREATE TABLE t (n INTEGER)"))
            .await?;
        for n in 0..5_i64 {
            client
                .execute(
                    &ledger,
                    Statement::with_params("INSERT INTO t VALUES (?)", [Value::Integer(n)]),
                )
                .await?;
        }
        let follower = created.followers().next().ok_or("a follower")?.node.clone();
        // Every replica has caught up to the primary's latest position and reported so.
        let primary_address = created
            .primary_replica()
            .and_then(|replica| replica.address.clone())
            .ok_or("the primary's address")?;
        let latest = client
            .verify(&primary_address, &ledger, None)
            .await?
            .position;
        assert!(latest.is_some());
        wait_until("every replica reporting the latest position", async || {
            let Ok(located) = client.locate(&ledger).await else {
                return false;
            };
            located.replicas.len() == 3
                && located
                    .replicas
                    .iter()
                    .all(|replica| replica.position == latest)
        })
        .await?;

        let verified = stdout(
            &fleetringd(fleet.tls.dir(), args(&["db", "verify", "ledger"], &control)).await?,
        )?;
        assert!(
            verified.starts_with(
                "database: ledger
position: "
            ),
            "{verified}"
        );
        assert!(
            verified.contains(
                "
all 3 replicas agree
"
            ),
            "{verified}"
        );
        let hash = verified
            .lines()
            .find_map(|line| line.strip_prefix("chain hash: "))
            .ok_or("a chain hash line")?
            .to_owned();
        assert_eq!(hash.len(), 64, "{verified}");
        let position = verified
            .lines()
            .find_map(|line| line.strip_prefix("position: "))
            .ok_or("a position line")?
            .to_owned();

        // The same hash through the client, and `--expect` confirms nothing changed.
        let through_client = client.verify(&primary_address, &ledger, None).await?;
        assert_eq!(through_client.chain_hash.to_string(), hash);
        assert_eq!(through_client.epoch.get(), 1);
        let expected = stdout(
            &fleetringd(
                fleet.tls.dir(),
                args(
                    &[
                        "db", "verify", "ledger", "--at", &position, "--expect", &hash,
                    ],
                    &control,
                ),
            )
            .await?,
        )?;
        assert!(
            expected.contains(&format!(
                "nothing changed since {position}
"
            )),
            "{expected}"
        );
        let mismatch = fleetringd(
            fleet.tls.dir(),
            args(
                &["db", "verify", "ledger", "--expect", &"0".repeat(64)],
                &control,
            ),
        )
        .await?;
        assert!(!mismatch.status.success());
        assert!(
            String::from_utf8_lossy(&mismatch.stderr).contains("not the expected"),
            "{}",
            String::from_utf8_lossy(&mismatch.stderr)
        );

        // A byte flipped in a follower's log after it was applied names that follower.
        let wal =
            database_file(dir.path(), follower.as_str(), "ledger").with_file_name("data.db-wal");
        let mut bytes = std::fs::read(&wal)?;
        let last = bytes.len() - 1;
        bytes[last] ^= 0x01;
        std::fs::write(&wal, &bytes)?;
        let tampered =
            fleetringd(fleet.tls.dir(), args(&["db", "verify", "ledger"], &control)).await?;
        assert!(!tampered.status.success());
        let stderr = String::from_utf8_lossy(&tampered.stderr);
        assert!(
            stderr.contains(&format!(
                "replicas of ledger disagree at {position}: {follower} "
            )),
            "{stderr}"
        );
        assert!(stderr.contains("can't be read back"), "{stderr}");

        fleet.stop().await;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_command_line_lists_drops_and_asks_a_node() -> TestResult {
        let dir = tempfile::tempdir()?;
        let fleet = Fleet::start_with(dir.path(), FAST_LEASES).await?;
        let control = fleet.control_address()?;
        let client = fleet.client()?;
        let mut locations = Vec::new();
        for text in ["orders", "acme"] {
            locations.push(client.create_database(&name(text)?, three()?).await?);
        }

        let listed = stdout(&fleetringd(fleet.tls.dir(), args(&["db", "list"], &control)).await?)?;
        let lines: Vec<&str> = listed.lines().collect();
        assert!(lines[0].starts_with("NAME  "), "{listed}");
        assert!(lines[1].starts_with("acme  "), "sorted by name: {listed}");
        assert!(lines[2].starts_with("orders  "), "sorted by name: {listed}");
        for location in &locations {
            let primary = location.primary.clone().ok_or("no primary")?;
            let line = lines
                .iter()
                .find(|line| line.starts_with(location.database.as_str()))
                .ok_or("database not listed")?;
            let replicas: Vec<String> = location
                .replicas
                .iter()
                .map(|replica| replica.node.to_string())
                .collect();
            let cells: Vec<&str> = line.split_whitespace().collect();
            assert_eq!(
                cells,
                [
                    location.database.as_str(),
                    "sqlite",
                    "1",
                    "3",
                    "quorum",
                    "2",
                    primary.as_str(),
                    &replicas.join(",")
                ],
                "{line}"
            );
        }
        assert!(
            lines[3].starts_with("2 databases; primaries per node: "),
            "{listed}"
        );
        for id in super::DATA {
            assert!(lines[3].contains(&format!(" {id}=")), "{listed}");
        }

        let primary = locations[0].primary.clone().ok_or("no primary")?;
        let filtered = stdout(
            &fleetringd(
                fleet.tls.dir(),
                args(&["db", "list", "--node", primary.as_str()], &control),
            )
            .await?,
        )?;
        let expected: Vec<&Location> = locations
            .iter()
            .filter(|location| location.primary.as_ref() == Some(&primary))
            .collect();
        assert_eq!(filtered.lines().count(), expected.len() + 2, "{filtered}");
        for location in expected {
            assert!(filtered.contains(location.database.as_str()), "{filtered}");
        }

        // Each data node reports the control plane's fingerprint on its own.
        let fingerprint = fleet
            .control
            .control()
            .ok_or("no control")?
            .state()
            .ring()?
            .fingerprint();
        for daemon in &fleet.data {
            let address = daemon.listen_address().to_string();
            let id = daemon.config().node.id.to_string();
            // By address, with nothing else; by node ID, through the control plane.
            for words in [
                vec!["status".to_owned(), "--address".to_owned(), address],
                args(&["status", "--node", id.as_str()], &control),
            ] {
                let status = stdout(&fleetringd(fleet.tls.dir(), words).await?)?;
                assert!(status.starts_with(&format!("node: {id}\n")), "{status}");
                assert!(
                    status.contains(&format!("ring fingerprint: {fingerprint}\n")),
                    "{status}"
                );
            }
        }

        // `lease watch` starts with the leases the primary holds, by address or by node ID.
        let primary_address = locations[0]
            .primary_replica()
            .and_then(|replica| replica.address.clone())
            .ok_or("no primary address")?;
        for words in [
            vec![
                "lease".to_owned(),
                "watch".to_owned(),
                "--address".to_owned(),
                primary_address.clone(),
            ],
            args(&["lease", "watch", "--node", primary.as_str()], &control),
        ] {
            let first = first_line(fleet.tls.dir(), words).await?;
            assert!(first.starts_with("held "), "{first}");
        }

        // 0.2's spellings are gone.
        for words in [
            vec![
                "watch-leases".to_owned(),
                "--node".to_owned(),
                primary_address.clone(),
            ],
            vec![
                "cert".to_owned(),
                "node".to_owned(),
                "n".to_owned(),
                "--hosts".to_owned(),
                "127.0.0.1".to_owned(),
                "--dir".to_owned(),
                ".".to_owned(),
            ],
        ] {
            let refused = fleetringd(fleet.tls.dir(), words.clone()).await?;
            assert_eq!(refused.status.code(), Some(2), "{words:?}: a usage error");
        }

        // Durability settings are set at creation, shown by locate and list, and changed for
        // the next epoch.
        let created = stdout(
            &fleetringd(
                fleet.tls.dir(),
                args(
                    &[
                        "db",
                        "create",
                        "everyone",
                        "--acks",
                        "all",
                        "--min-replicas",
                        "3",
                    ],
                    &control,
                ),
            )
            .await?,
        )?;
        assert!(
            created.contains("acks: all\nmin replicas: 3\n"),
            "{created}"
        );
        let located = stdout(
            &fleetringd(
                fleet.tls.dir(),
                args(&["db", "locate", "everyone"], &control),
            )
            .await?,
        )?;
        assert!(
            located.contains("replication factor: 3\nacks: all\nmin replicas: 3\n"),
            "{located}"
        );
        let listed = stdout(&fleetringd(fleet.tls.dir(), args(&["db", "list"], &control)).await?)?;
        let line = listed
            .lines()
            .find(|line| line.starts_with("everyone"))
            .ok_or("everyone not listed")?;
        let cells: Vec<&str> = line.split_whitespace().collect();
        assert_eq!(&cells[1..6], ["sqlite", "1", "3", "all", "3"], "{line}");
        let bad = fleetringd(
            fleet.tls.dir(),
            args(&["db", "create", "bad", "--acks", "sometimes"], &control),
        )
        .await?;
        assert!(!bad.status.success());
        let changed = stdout(
            &fleetringd(
                fleet.tls.dir(),
                args(
                    &["db", "set-durability", "everyone", "--acks", "local"],
                    &control,
                ),
            )
            .await?,
        )?;
        assert_eq!(
            changed,
            "durability of everyone changes to acks=local min_replicas=1 at its next epoch; \
             now acks=all min_replicas=3\n"
        );
        let located = stdout(
            &fleetringd(
                fleet.tls.dir(),
                args(&["db", "locate", "everyone"], &control),
            )
            .await?,
        )?;
        assert!(located.contains("acks: all\n"), "{located}");

        // A control call aimed at a data node names the control node.
        let wrong = fleetringd(
            fleet.tls.dir(),
            args(&["db", "list"], &fleet.data[0].listen_address().to_string()),
        )
        .await?;
        assert!(!wrong.status.success());
        let stderr = String::from_utf8_lossy(&wrong.stderr);
        assert!(
            stderr.contains(&format!(
                "this node runs no control plane; the control node is {control}"
            )),
            "{stderr}"
        );

        // Without a terminal, dropping needs `--yes`.
        wait_until("every node holds a copy of orders", async || {
            every_node_has(dir.path(), "orders")
        })
        .await?;
        let asked = fleetringd(fleet.tls.dir(), args(&["db", "drop", "orders"], &control)).await?;
        assert!(!asked.status.success());
        assert!(
            String::from_utf8_lossy(&asked.stderr).contains("pass --yes"),
            "{}",
            String::from_utf8_lossy(&asked.stderr)
        );
        assert!(client.locate(&name("orders")?).await.is_ok());
        let dropped = stdout(
            &fleetringd(
                fleet.tls.dir(),
                args(&["db", "drop", "orders", "--yes"], &control),
            )
            .await?,
        )?;
        assert_eq!(dropped, "dropped orders at epoch 1\n");
        let gone = fleetringd(fleet.tls.dir(), args(&["db", "locate", "orders"], &control)).await?;
        assert!(!gone.status.success());
        wait_until("every node deleted its copy of orders", async || {
            no_node_has(dir.path(), "orders")
        })
        .await?;
        let listed = stdout(&fleetringd(fleet.tls.dir(), args(&["db", "list"], &control)).await?)?;
        assert!(!listed.contains("orders"), "{listed}");

        fleet.stop().await;
        Ok(())
    }
}

/// How many databases the drain test spreads over the three nodes.
const DRAINED_DATABASES: usize = 50;

/// What a client writing through a drain saw.
#[derive(Debug, Default)]
struct WriterReport {
    /// How many writes succeeded.
    written: u64,
    /// How many writes needed at least one retry.
    retried: u64,
    /// The most retries one write needed.
    max_retries: u32,
    /// A write that failed with a status retrying can't fix, if any.
    rejected: Option<String>,
}

/// Writes one row at a time to each of `databases` in turn until `stop` is set, retrying a
/// write that is refused as `UNAVAILABLE` or `ABORTED`, as `fleetringd db exec` would be.
async fn write_until_stopped(
    client: Client,
    databases: Vec<DatabaseName>,
    stop: std::sync::Arc<std::sync::atomic::AtomicBool>,
) -> WriterReport {
    use std::sync::atomic::Ordering;
    let mut report = WriterReport::default();
    let mut value = 0_i64;
    'writes: while !stop.load(Ordering::SeqCst) {
        for database in &databases {
            value += 1;
            let mut retries = 0_u32;
            loop {
                let written = client
                    .execute(
                        database,
                        Statement::with_params("INSERT INTO t VALUES (?)", [Value::Integer(value)]),
                    )
                    .await;
                match written {
                    Ok(_) => break,
                    Err(status)
                        if matches!(status.code(), Code::Unavailable | Code::Aborted)
                            && !stop.load(Ordering::SeqCst) =>
                    {
                        retries += 1;
                        tokio::time::sleep(Duration::from_millis(20)).await;
                    }
                    Err(status) => {
                        report.rejected = Some(format!("{database}: {status}"));
                        break 'writes;
                    }
                }
            }
            report.written += 1;
            if retries > 0 {
                report.retried += 1;
                report.max_retries = report.max_retries.max(retries);
            }
        }
    }
    report
}

/// Draining a node hands every database it is primary for to a follower, through the same
/// epoch and fencing rules as a failover, while a client keeps writing: it sees `UNAVAILABLE`
/// and redirects for a moment and retries, but nothing is refused for good. The drained node
/// stays a member and a follower; stopping it afterwards costs nothing more, and it leaves.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn draining_a_node_moves_its_primaries_while_a_client_keeps_writing() -> TestResult {
    let dir = tempfile::tempdir()?;
    let fleet = Fleet::start(dir.path()).await?;
    let client = fleet.client()?;
    let mut primaries: std::collections::BTreeMap<String, Vec<DatabaseName>> =
        std::collections::BTreeMap::new();
    for k in 0..DRAINED_DATABASES {
        let database = name(&format!("db-{k}"))?;
        let location = client.create_database(&database, three()?).await?;
        client
            .execute(&database, Statement::new("CREATE TABLE t (x)"))
            .await?;
        let primary = location.primary.ok_or("no primary")?;
        primaries
            .entry(primary.to_string())
            .or_default()
            .push(database);
    }
    // The ring spreads the primaries; drain the node that holds the most.
    let (drained, mine) = primaries
        .iter()
        .max_by_key(|(_, databases)| databases.len())
        .map(|(node, databases)| (node.clone(), databases.clone()))
        .ok_or("no primaries")?;
    assert!(
        primaries.len() == DATA.len() && mine.len() >= DRAINED_DATABASES / 6,
        "primaries per node: {:?}",
        primaries
            .iter()
            .map(|(node, databases)| (node, databases.len()))
            .collect::<Vec<_>>()
    );
    let drained_id: fleetring::NodeId = drained.parse()?;

    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let writer = tokio::spawn(write_until_stopped(
        fleet.client()?,
        mine.clone(),
        std::sync::Arc::clone(&stop),
    ));
    tokio::time::sleep(Duration::from_millis(200)).await;

    let started = tokio::time::Instant::now();
    let mut announced = client.drain_node(&drained_id).await?;
    announced.sort();
    let mut expected = mine.clone();
    expected.sort();
    assert_eq!(
        announced, expected,
        "the drain names every lease the node holds"
    );
    wait_until("every primary has moved", async || {
        for database in &mine {
            let Ok(location) = client.locate(database).await else {
                return false;
            };
            if location.primary.as_ref() == Some(&drained_id)
                || location.primary.is_none()
                || location.epoch.get() != 2
            {
                return false;
            }
        }
        true
    })
    .await?;
    let took = started.elapsed();
    stop.store(true, std::sync::atomic::Ordering::SeqCst);
    let report = writer.await?;
    eprintln!(
        "drained {} primaries of {drained} in {took:?}; the client wrote {} rows, {} with retries (at most {})",
        mine.len(),
        report.written,
        report.retried,
        report.max_retries
    );
    assert_eq!(report.rejected, None, "a write was refused for good");
    assert!(report.written > 0);

    // The drained node is still a member and a follower of what it used to lead.
    for database in &mine {
        let location = client.locate(database).await?;
        assert!(
            location
                .replicas
                .iter()
                .any(|replica| replica.node == drained_id),
            "{database}: {location:?}"
        );
        assert_eq!(location.epoch.get(), 2, "{database}");
    }
    let status = client.status().await?;
    assert_eq!(status.liveness(&drained_id), Some(Liveness::Live));
    // Every row the client wrote is on the new primary.
    for database in mine.iter().take(3) {
        let rows = client
            .query(database, Statement::new("SELECT count(*) FROM t"))
            .await?;
        assert!(matches!(
            rows.rows.first().and_then(|row| row.first()),
            Some(Value::Integer(_))
        ));
    }

    // Stopping the drained node now leaves nothing to hand over; it leaves the fleet.
    let Fleet {
        control,
        mut data,
        tls,
    } = fleet;
    let index = data
        .iter()
        .position(|daemon| daemon.config().node.id == drained_id)
        .ok_or("the drained node isn't a data daemon")?;
    data.remove(index).run_until(async {}).await;
    wait_until("the drained node has left", async || {
        client
            .status()
            .await
            .is_ok_and(|view| view.liveness(&drained_id).is_none())
    })
    .await?;
    for database in &mine {
        let location = client.locate(database).await?;
        assert_ne!(location.primary.as_ref(), Some(&drained_id), "{database}");
        assert_eq!(
            location.epoch.get(),
            2,
            "{database}: a stop after a drain costs no epoch"
        );
    }
    Fleet { control, data, tls }.stop().await;
    Ok(())
}

/// A write the primary admitted before a drain began, and that is still running when the drain
/// would step down, completes: the drain waits for it to reach replication and ships it with the
/// tail. Stepping down under it would leave its batch with no primary to publish to, and fail it
/// as `ABORTED`, "may or may not have taken effect", though the hand-over was planned.
///
/// The write is held in the primary's engine by the test, not by timing: another connection
/// holds the database's SQLite write lock, so the write waits on it (rusqlite's five-second busy
/// timeout) until the test lets go, well after the drain began.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_write_running_when_a_drain_begins_completes() -> TestResult {
    let dir = tempfile::tempdir()?;
    let fleet = Fleet::start(dir.path()).await?;
    let client = fleet.client()?;
    let database = name("held")?;
    let location = client.create_database(&database, three()?).await?;
    client
        .execute(&database, Statement::new("CREATE TABLE t (x)"))
        .await?;
    let primary = location.primary.ok_or("no primary")?;

    let lock = rusqlite::Connection::open(database_file(dir.path(), primary.as_str(), "held"))?;
    lock.execute_batch("BEGIN IMMEDIATE")?;
    let writer = {
        let client = fleet.client()?;
        let database = database.clone();
        tokio::spawn(async move {
            client
                .execute(&database, Statement::new("INSERT INTO t VALUES (1)"))
                .await
        })
    };
    // The write reaches the engine and waits for the lock; then the drain begins, and without
    // waiting for the write would have handed the lease over and stepped down by the time the
    // lock is let go.
    tokio::time::sleep(Duration::from_millis(500)).await;
    client.drain_node(&primary).await?;
    tokio::time::sleep(Duration::from_millis(2_000)).await;
    lock.execute_batch("ROLLBACK")?;
    drop(lock);

    let written = tokio::time::timeout(HANG_GUARD, writer).await??;
    match &written {
        Ok(written) => assert_eq!(written.rows_affected, 1),
        // A write that reached the primary only once the drain refused writes is refused before
        // it runs, which is safe to retry; it never ends as "may or may not have taken effect".
        Err(status) => assert_eq!(status.code(), Code::Unavailable, "{status:?}"),
    }
    wait_until("the lease has moved", async || {
        client.locate(&database).await.is_ok_and(|location| {
            location.epoch.get() == 2
                && location
                    .primary
                    .as_ref()
                    .is_some_and(|primary_now| *primary_now != primary)
        })
    })
    .await?;
    if written.is_ok() {
        let rows = client
            .query(&database, Statement::new("SELECT count(*) FROM t"))
            .await?;
        assert_eq!(
            rows.rows.first().and_then(|row| row.first()),
            Some(&Value::Integer(1)),
            "the acknowledged write is on the new primary"
        );
    }
    fleet.stop().await;
    Ok(())
}

/// The next event on `updates`, failing instead of hanging.
async fn next_lease_event(updates: &mut LeaseUpdates) -> Result<LeaseEvent, Box<dyn Error>> {
    match tokio::time::timeout(HANG_GUARD, updates.next()).await?? {
        Some(LeaseUpdate::Event(event)) => Ok(event),
        other => Err(format!("expected a lease event, got {other:?}").into()),
    }
}

/// The leases a fresh stream on `address` starts with.
async fn held_leases(
    client: &Client,
    address: &str,
) -> Result<(LeaseUpdates, BTreeMap<DatabaseName, LeaseEpoch>), Box<dyn Error>> {
    let mut updates = client.watch_leases(address).await?;
    match tokio::time::timeout(HANG_GUARD, updates.next()).await?? {
        Some(LeaseUpdate::Held { leases, .. }) => Ok((updates, leases)),
        other => Err(format!("expected the held leases first, got {other:?}").into()),
    }
}

/// `WatchLeases` on every data node (FR-042): each stream starts with the leases held, the node that
/// takes a database's first lease reports it gained at epoch 1, a drain reports it lost at epoch
/// 1 on the old primary and gained at epoch 2 on the new one, and a stream opened afterwards
/// starts from the current leases with no stale event. The control node, which hosts no data,
/// answers `UNIMPLEMENTED` as it does every database call.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn watch_leases_streams_the_leases_each_node_gains_and_loses() -> TestResult {
    let dir = tempfile::tempdir()?;
    let fleet = Fleet::start(dir.path()).await?;
    let client = fleet.client()?;
    let addresses: BTreeMap<String, String> = fleet
        .data
        .iter()
        .map(|daemon| {
            (
                daemon.config().node.id.to_string(),
                daemon.listen_address().to_string(),
            )
        })
        .collect();
    let mut watchers = BTreeMap::new();
    for (id, address) in &addresses {
        let mut updates = client.watch_leases(address).await?;
        match tokio::time::timeout(HANG_GUARD, updates.next()).await?? {
            Some(LeaseUpdate::Held { node, leases }) => {
                assert_eq!(node.as_str(), id);
                assert!(leases.is_empty(), "{id} holds nothing yet: {leases:?}");
            }
            other => return Err(format!("expected the held leases first, got {other:?}").into()),
        }
        watchers.insert(id.clone(), updates);
    }
    let refused = client
        .watch_leases(&fleet.control_address()?)
        .await
        .expect_err("the control node hosts no data");
    assert_eq!(refused.code(), Code::Unimplemented, "{refused}");

    let notes = name("notes")?;
    let created = client.create_database(&notes, three()?).await?;
    let primary = created.primary.ok_or("no primary")?.to_string();
    client
        .execute(&notes, Statement::new("CREATE TABLE t (x)"))
        .await?;
    let gained = next_lease_event(watchers.get_mut(&primary).ok_or("no watcher")?).await?;
    assert_eq!(
        gained,
        LeaseEvent::new(notes.clone(), LeaseEpoch::new(1), LeaseChange::Gained)
    );

    let drained: fleetring::NodeId = primary.parse()?;
    client.drain_node(&drained).await?;
    let lost = next_lease_event(watchers.get_mut(&primary).ok_or("no watcher")?).await?;
    assert_eq!(
        lost,
        LeaseEvent::new(notes.clone(), LeaseEpoch::new(1), LeaseChange::Lost)
    );
    wait_until("the lease has moved", async || {
        client
            .locate(&notes)
            .await
            .is_ok_and(|location| location.epoch.get() == 2 && location.primary.is_some())
    })
    .await?;
    let moved_to = client
        .locate(&notes)
        .await?
        .primary
        .ok_or("no new primary")?
        .to_string();
    assert_ne!(moved_to, primary);
    let gained = next_lease_event(watchers.get_mut(&moved_to).ok_or("no watcher")?).await?;
    assert_eq!(
        gained,
        LeaseEvent::new(notes.clone(), LeaseEpoch::new(2), LeaseChange::Gained)
    );
    // The node that answers for the database at epoch 2 reported it before it took a write.
    let written = client
        .execute(
            &notes,
            Statement::with_params("INSERT INTO t VALUES (?)", [Value::Integer(1)]),
        )
        .await?;
    assert_eq!(written.epoch.get(), 2);
    assert_eq!(
        written.primary.map(|id| id.to_string()),
        Some(moved_to.clone())
    );

    // A stream opened now starts from the current leases, with no stale event before them.
    let (mut fresh, held) = held_leases(&client, &addresses[&moved_to]).await?;
    assert_eq!(held, BTreeMap::from([(notes.clone(), LeaseEpoch::new(2))]));
    let (mut old, held) = held_leases(&client, &addresses[&primary]).await?;
    assert!(held.is_empty(), "the drained node holds nothing: {held:?}");
    let quiet = Duration::from_millis(300);
    for stream in [&mut fresh, &mut old] {
        assert!(
            tokio::time::timeout(quiet, stream.next()).await.is_err(),
            "a fresh stream replays nothing"
        );
    }
    fleet.stop().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn statements_are_confined_to_the_database_they_name() -> TestResult {
    let dir = tempfile::tempdir()?;
    let fleet = Fleet::start(dir.path()).await?;
    let client = fleet.client()?;
    let victim = name("victim")?;
    let intruder = name("intruder")?;
    let created = client.create_database(&victim, three()?).await?;
    let primary = created.primary.clone().ok_or("no primary after create")?;
    client.create_database(&intruder, three()?).await?;
    client
        .execute(
            &victim,
            Statement::new("CREATE TABLE notes (n INTEGER PRIMARY KEY, body TEXT)"),
        )
        .await?;
    client
        .execute(
            &victim,
            Statement::new("INSERT INTO notes (n, body) VALUES (1, 'mine')"),
        )
        .await?;

    // Whichever node runs the intruder, every copy of the victim's file is out of reach.
    for id in DATA {
        let path = database_file(dir.path(), id, "victim");
        let attach = format!("ATTACH DATABASE '{}' AS z", path.display());
        let queried = client
            .query(&intruder, Statement::new(attach.clone()))
            .await
            .expect_err("a query can't attach another database");
        assert_eq!(queried.code(), Code::InvalidArgument, "{queried}");
        assert!(queried.message().contains("attach"), "{queried}");
        let executed = client
            .execute(&intruder, Statement::new(attach))
            .await
            .expect_err("a statement can't attach another database");
        assert_eq!(executed.code(), Code::InvalidArgument, "{executed}");
        assert!(executed.message().contains("attach"), "{executed}");
    }
    // Nothing stayed attached on either connection.
    let read = client
        .query(&intruder, Statement::new("SELECT n FROM z.notes"))
        .await
        .expect_err("no database z");
    assert_eq!(read.code(), Code::InvalidArgument, "{read}");
    let write = client
        .execute(
            &intruder,
            Statement::new("INSERT INTO z.notes (n, body) VALUES (2, 'theirs')"),
        )
        .await
        .expect_err("no database z");
    assert_eq!(write.code(), Code::InvalidArgument, "{write}");

    let rows = client
        .query(
            &victim,
            Statement::new("SELECT n, body FROM notes ORDER BY n"),
        )
        .await?;
    assert_eq!(
        rows.rows,
        vec![vec![Value::Integer(1), Value::from("mine")]]
    );
    assert_eq!(
        notes_in(&database_file(dir.path(), primary.as_str(), "victim"))?,
        [1]
    );

    fleet.stop().await;
    Ok(())
}

/// FR-049: archives every data daemon shares, and restores from them into new databases.
mod archives {
    use std::path::Path;

    use std::path::PathBuf;

    use fleetring_control::{DatabaseName, Labels, parse_label};
    use fleetring_proto::v1::RestoreRequest;
    use fleetringd::data::{Client, DatabaseSpec, NewMetadata, restored_metadata};
    use fleetringd::durability::{
        ArchivedMetadata, RestoreTarget, checksum_hex, decode_metadata, encode_metadata,
    };
    use fleetringd::engine::{Position, Statement, Value};
    use fleetringd::transport::database_client;
    use tonic::Code;

    use super::{Fleet, HANG_GUARD, TestResult, every_node_has, name, network, three, wait_until};

    /// The archive's name in every test here.
    pub(super) const ARCHIVE: &str = "backups";

    /// A fleet whose data daemons all archive every database to one directory, segmenting
    /// every 50 ms.
    pub(super) async fn archived_fleet(dir: &Path) -> Result<Fleet, Box<dyn std::error::Error>> {
        let table = format!(
            "\n[[archive]]\nname = \"{ARCHIVE}\"\ndatabases = [\"*\"]\nstore = \"directory\"\n\
             path = {:?}\nsegment_interval_ms = 50\n",
            dir.join("archive")
        );
        Fleet::start_configured(dir, "", "", &table).await
    }

    /// Creates `database` with three replicas and the values `1..=count` in `t`, and waits until
    /// its primary has archived them all.
    pub(super) async fn create_archived(
        fleet: &Fleet,
        client: &Client,
        database: &DatabaseName,
        count: i64,
    ) -> TestResult {
        client.create_database(database, three()?).await?;
        write_archived(fleet, client, database, count).await
    }

    /// Writes `1..=count` to `database`, and waits until its primary has archived them all.
    async fn write_archived(
        fleet: &Fleet,
        client: &Client,
        database: &DatabaseName,
        count: i64,
    ) -> TestResult {
        client
            .execute(
                database,
                Statement::new("CREATE TABLE t (x INTEGER NOT NULL)"),
            )
            .await?;
        for x in 1..=count {
            client
                .execute(
                    database,
                    Statement::with_params("INSERT INTO t (x) VALUES (?1)", [Value::Integer(x)]),
                )
                .await?;
        }
        let primary = client.locate(database).await?.primary.ok_or("a primary")?;
        let streams = fleet
            .data_daemon(primary.as_str())?
            .change_streams()
            .ok_or("change streams")?;
        if !streams.await_caught_up(HANG_GUARD).await {
            return Err("the archive didn't catch up".into());
        }
        Ok(())
    }

    async fn values(
        client: &Client,
        database: &DatabaseName,
    ) -> Result<Vec<i64>, Box<dyn std::error::Error>> {
        let rows = client
            .query(database, Statement::new("SELECT x FROM t ORDER BY x"))
            .await?;
        Ok(rows
            .rows
            .iter()
            .filter_map(|row| match row.first() {
                Some(Value::Integer(x)) => Some(*x),
                _ => None,
            })
            .collect())
    }

    /// The hash every replica of `database` with an address reports at its latest position,
    /// once they all report the same position and hash.
    async fn converged(
        client: &Client,
        database: &DatabaseName,
    ) -> Result<(Option<Position>, fleetringd::replication::ChainHash), Box<dyn std::error::Error>>
    {
        let mut agreed = None;
        wait_until("every replica agreeing", async || {
            let Ok(location) = client.locate(database).await else {
                return false;
            };
            let mut answers = Vec::new();
            for replica in &location.replicas {
                let Some(address) = &replica.address else {
                    return false;
                };
                match client.verify(address, database, None).await {
                    Ok(answer) => answers.push((answer.position, answer.chain_hash)),
                    Err(_) => return false,
                }
            }
            let same = answers.len() == 3 && answers.windows(2).all(|pair| pair[0] == pair[1]);
            if same {
                agreed = answers.pop();
            }
            same
        })
        .await?;
        Ok(agreed.ok_or("an answer")?)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn db_restore_creates_a_new_database_from_the_archive_and_serves_it() -> TestResult {
        let dir = tempfile::tempdir()?;
        let fleet = archived_fleet(dir.path()).await?;
        let client = fleet.client()?;
        let ledger = name("ledger")?;
        create_archived(&fleet, &client, &ledger, 20).await?;
        let (position, hash) = converged(&client, &ledger).await?;

        let restored = name("restored")?;
        let restoration = client
            .restore_database(
                &restored,
                DatabaseSpec::new(three()?),
                ARCHIVE,
                &ledger,
                &RestoreTarget::Latest,
            )
            .await?;
        assert_eq!(restoration.position, position);
        assert_eq!(
            restoration.chain_hash, hash,
            "the chain every replica reports"
        );
        assert!(restoration.location.primary.is_some());
        assert_eq!(
            values(&client, &restored).await?,
            (1..=20).collect::<Vec<_>>()
        );

        // The new database takes writes, and its followers seed from it and converge.
        client
            .execute(
                &restored,
                Statement::with_params("INSERT INTO t (x) VALUES (?1)", [Value::Integer(21)]),
            )
            .await?;
        assert_eq!(
            values(&client, &restored).await?,
            (1..=21).collect::<Vec<_>>()
        );
        converged(&client, &restored).await?;
        assert!(every_node_has(dir.path(), "restored"));
        // The source is untouched.
        assert_eq!(
            values(&client, &ledger).await?,
            (1..=20).collect::<Vec<_>>()
        );
        fleet.stop().await;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn restore_refuses_a_database_that_already_has_a_lease() -> TestResult {
        let dir = tempfile::tempdir()?;
        let fleet = archived_fleet(dir.path()).await?;
        let client = fleet.client()?;
        let ledger = name("ledger")?;
        create_archived(&fleet, &client, &ledger, 3).await?;
        let other = name("other")?;
        create_archived(&fleet, &client, &other, 1).await?;

        let refused = client
            .restore_database(
                &other,
                DatabaseSpec::new(three()?),
                ARCHIVE,
                &ledger,
                &RestoreTarget::Latest,
            )
            .await
            .expect_err("a database with a primary isn't restored over");
        assert_eq!(refused.code(), Code::AlreadyExists, "{refused:?}");

        // Asked directly, the primary refuses too, and the database keeps its rows.
        let location = client.locate(&other).await?;
        let primary = location
            .primary_replica()
            .and_then(|replica| replica.address.clone())
            .ok_or("the primary's address")?;
        let refused = database_client(network(&fleet.tls)?.channel(&primary)?)
            .restore(RestoreRequest {
                database: other.to_string(),
                archive: ARCHIVE.to_owned(),
                source: ledger.to_string(),
                point: None,
            })
            .await
            .expect_err("a leased database isn't restored over");
        assert_eq!(refused.code(), Code::FailedPrecondition, "{refused:?}");
        assert!(
            refused.message().contains("already has a lease"),
            "{refused:?}"
        );
        assert_eq!(values(&client, &other).await?, [1]);

        // An archive the node doesn't have is named.
        let missing = client
            .restore_database(
                &name("fresh")?,
                DatabaseSpec::new(three()?),
                "nope",
                &ledger,
                &RestoreTarget::Latest,
            )
            .await
            .expect_err("no such archive");
        assert_eq!(missing.code(), Code::NotFound, "{missing:?}");
        assert!(missing.message().contains("nope"), "{missing:?}");
        // The failed restore left an unleased database, which a retry with the right archive
        // finishes.
        let retried = client
            .restore_database(
                &name("fresh")?,
                DatabaseSpec::new(three()?),
                ARCHIVE,
                &ledger,
                &RestoreTarget::Latest,
            )
            .await?;
        assert!(retried.location.primary.is_some());
        assert_eq!(values(&client, &name("fresh")?).await?, [1, 2, 3]);

        // A restore may give the new database a description and labels, which its creation
        // records (FR-070 phase 2; phase 5 adds the archive's own).
        let described = name("described")?;
        client
            .restore_database_with_metadata(
                &described,
                DatabaseSpec::new(three()?),
                &fleetringd::data::NewMetadata {
                    description: Some("Restored from ledger.".parse()?),
                    labels: fleetring_control::Labels::try_from_pairs([
                        fleetring_control::parse_label("purpose=restore")?,
                    ])?,
                    ..fleetringd::data::NewMetadata::default()
                },
                ARCHIVE,
                &ledger,
                &RestoreTarget::Latest,
            )
            .await?;
        let metadata = client
            .locate(&described)
            .await?
            .metadata
            .ok_or("no metadata")?;
        assert_eq!(
            metadata.description.as_deref(),
            Some("Restored from ledger.")
        );
        assert_eq!(metadata.label("purpose"), Some("restore"));
        assert_eq!(metadata.created_by.as_deref(), Some("tester"));
        assert_eq!(values(&client, &described).await?, [1, 2, 3]);
        fleet.stop().await;
        Ok(())
    }

    /// The metadata a ledger is created with in the tests below.
    fn ledger_metadata() -> Result<NewMetadata, Box<dyn std::error::Error>> {
        Ok(NewMetadata {
            description: Some("Ledger notes.\nSecond line.".parse()?),
            labels: Labels::try_from_pairs([
                parse_label("purpose=ledger")?,
                parse_label("team=billing")?,
            ])?,
            ..NewMetadata::default()
        })
    }

    /// Creates `database` with three replicas and `metadata`, writes `1..=count` to it, and
    /// waits until its primary has archived them all, its metadata included.
    async fn create_archived_with_metadata(
        fleet: &Fleet,
        client: &Client,
        database: &DatabaseName,
        count: i64,
    ) -> TestResult {
        client
            .create_database_with_metadata(
                database,
                DatabaseSpec::new(three()?),
                &ledger_metadata()?,
            )
            .await?;
        write_archived(fleet, client, database, count).await
    }

    /// The metadata objects the directory archive under `root` holds.
    fn metadata_files(root: &Path) -> Result<Vec<PathBuf>, Box<dyn std::error::Error>> {
        let mut found = Vec::new();
        let mut pending = vec![root.to_owned()];
        while let Some(dir) = pending.pop() {
            for entry in std::fs::read_dir(&dir)? {
                let path = entry?.path();
                if path.is_dir() {
                    pending.push(path);
                } else if path
                    .extension()
                    .is_some_and(|extension| extension == "json")
                    && path
                        .components()
                        .any(|component| component.as_os_str() == ".metadata")
                {
                    found.push(path);
                }
            }
        }
        Ok(found)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn restore_applies_archived_description_and_labels_but_not_the_expiry() -> TestResult {
        let dir = tempfile::tempdir()?;
        let fleet = archived_fleet(dir.path()).await?;
        let client = fleet.client()?;
        let ledger = name("ledger")?;
        create_archived_with_metadata(&fleet, &client, &ledger, 3).await?;

        // An archive isn't signed, so its object may say anything. This one claims that `ops`
        // wrote the description and created the database, and that it expires. The checksum
        // in its name is the forged bytes' own, which is all a checksum can vouch for.
        let files = metadata_files(&dir.path().join("archive"))?;
        assert_eq!(files.len(), 1, "{files:?}");
        let genuine = decode_metadata(&std::fs::read(&files[0])?)?;
        assert_eq!(genuine.description_by.as_deref(), Some("tester"));
        let forged = ArchivedMetadata {
            description_by: Some("ops".to_owned()),
            created_by: Some("ops".to_owned()),
            expires_at_ms: Some(1),
            ..genuine
        };
        let bytes = encode_metadata(&forged);
        let stem = files[0]
            .file_stem()
            .and_then(|stem| stem.to_str())
            .ok_or("a file name")?;
        let id = stem.rsplit_once('_').ok_or("an id and a checksum")?.0;
        std::fs::write(
            files[0].with_file_name(format!("{id}_{}.json", checksum_hex(&bytes))),
            &bytes,
        )?;
        std::fs::remove_file(&files[0])?;

        let restored = name("restored")?;
        let restoration = client
            .restore_database(
                &restored,
                DatabaseSpec::new(three()?),
                ARCHIVE,
                &ledger,
                &RestoreTarget::Latest,
            )
            .await?;
        assert_eq!(restoration.archived_metadata_skipped, None);
        let archived = restoration.archived_metadata.as_ref().ok_or("metadata")?;
        // What the archive claims is reported as its claim.
        assert_eq!(archived.description_by.as_deref(), Some("ops"));
        assert_eq!(archived.created_by.as_deref(), Some("ops"));
        assert!(archived.expires_at.is_some());

        let restore = restored_metadata(archived, &NewMetadata::default());
        assert!(restore.skipped.is_empty(), "{:?}", restore.skipped);
        client
            .update_database(&restored, &restore.change, None)
            .await?;
        let metadata = client
            .locate(&restored)
            .await?
            .metadata
            .ok_or("no metadata")?;
        assert_eq!(
            metadata.description.as_deref(),
            Some("Ledger notes.\nSecond line.")
        );
        assert_eq!(
            metadata.description_by, None,
            "a restored description records no author, so every reader treats it as untrusted"
        );
        assert_eq!(metadata.label("purpose"), Some("ledger"));
        assert_eq!(metadata.label("team"), Some("billing"));
        assert_eq!(
            metadata.created_by.as_deref(),
            Some("tester"),
            "the creator is the principal that restored"
        );
        assert_eq!(metadata.expires_at, None, "an expiry is never restored");
        assert_eq!(values(&client, &restored).await?, [1, 2, 3]);
        fleet.stop().await;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn restore_flags_win_over_the_archive() -> TestResult {
        let dir = tempfile::tempdir()?;
        let fleet = archived_fleet(dir.path()).await?;
        let client = fleet.client()?;
        let ledger = name("ledger")?;
        create_archived_with_metadata(&fleet, &client, &ledger, 3).await?;

        let flags = NewMetadata {
            description: Some("Mine.".parse()?),
            labels: Labels::try_from_pairs([parse_label("purpose=scratch")?])?,
            ..NewMetadata::default()
        };
        let restored = name("restored")?;
        let restoration = client
            .restore_database_with_metadata(
                &restored,
                DatabaseSpec::new(three()?),
                &flags,
                ARCHIVE,
                &ledger,
                &RestoreTarget::Latest,
            )
            .await?;
        let archived = restoration.archived_metadata.as_ref().ok_or("metadata")?;
        let restore = restored_metadata(archived, &flags);
        client
            .update_database(&restored, &restore.change, None)
            .await?;
        let metadata = client
            .locate(&restored)
            .await?
            .metadata
            .ok_or("no metadata")?;
        assert_eq!(metadata.description.as_deref(), Some("Mine."));
        assert_eq!(
            metadata.description_by.as_deref(),
            Some("tester"),
            "a description given to the restore is the caller's own"
        );
        assert_eq!(metadata.label("purpose"), Some("scratch"), "the flag wins");
        assert_eq!(
            metadata.label("team"),
            Some("billing"),
            "the rest is archived's"
        );
        fleet.stop().await;
        Ok(())
    }

    #[cfg(unix)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_command_line_restores_archived_metadata_as_untrusted() -> TestResult {
        use super::binary::{args, fleetringd, stdout};

        let dir = tempfile::tempdir()?;
        let fleet = archived_fleet(dir.path()).await?;
        let client = fleet.client()?;
        let ledger = name("ledger")?;
        create_archived_with_metadata(&fleet, &client, &ledger, 3).await?;
        let control = fleet.control_address()?;
        let run = async |words: &[&str]| -> Result<String, Box<dyn std::error::Error>> {
            stdout(&fleetringd(fleet.tls.dir(), args(words, &control)).await?)
        };

        let restored = run(&[
            "db",
            "restore",
            "restored",
            "--archive",
            ARCHIVE,
            "--from",
            "ledger",
        ])
        .await?;
        assert!(
            restored.contains("archived description:\n  | Ledger notes.\n  | Second line.\n"),
            "{restored}"
        );
        assert!(
            restored
                .contains("archived description by: tester (the archive's claim; not recorded)\n"),
            "{restored}"
        );
        assert!(
            restored.contains("archived creator: tester (the archive's claim; not recorded)\n"),
            "{restored}"
        );
        assert!(
            restored.contains("archived labels: purpose=ledger, team=billing\n"),
            "{restored}"
        );
        let shown: serde_json::Value =
            serde_json::from_str(&run(&["db", "show", "restored", "--json"]).await?)?;
        let metadata = &shown["metadata"];
        assert_eq!(metadata["description"], "Ledger notes.\nSecond line.");
        assert_eq!(metadata["description_by"], serde_json::Value::Null);
        assert_eq!(metadata["labels"]["purpose"], "ledger");
        assert_eq!(metadata["labels"]["team"], "billing");
        assert_eq!(metadata["created_by"], "tester");
        assert_eq!(metadata["expires_at"], serde_json::Value::Null);

        let described = run(&["db", "archive", "show", "ledger", "--archive", ARCHIVE]).await?;
        assert!(described.contains("metadata: yes"), "{described}");

        // Offline, the object lands beside the database file, byte for byte.
        let config = dir.path().join("offline.toml");
        std::fs::write(
            &config,
            format!(
                "data_dir = {:?}\nlisten = \"127.0.0.1:0\"\ninsecure_plaintext = true\n\n\
                 [node]\nid = \"offline\"\n\n[[archive]]\nname = \"{ARCHIVE}\"\n\
                 databases = [\"*\"]\nstore = \"directory\"\npath = {:?}\n",
                dir.path().join("offline"),
                dir.path().join("archive")
            ),
        )?;
        let to = dir.path().join("recovered");
        let words: Vec<String> = [
            "archive",
            "restore",
            "--config",
            config.to_str().ok_or("a UTF-8 path")?,
            "--archive",
            ARCHIVE,
            "--from",
            "ledger",
            "--to",
            to.to_str().ok_or("a UTF-8 path")?,
        ]
        .iter()
        .map(|word| (*word).to_owned())
        .collect();
        let offline = stdout(&fleetringd(fleet.tls.dir(), words).await?)?;
        assert!(offline.contains("metadata.json"), "{offline}");
        // The restored database is archived with its own metadata, so pick the ledger's.
        let files: Vec<PathBuf> = metadata_files(&dir.path().join("archive"))?
            .into_iter()
            .filter(|file| {
                file.parent()
                    .is_some_and(|parent| parent.ends_with("ledger"))
            })
            .collect();
        assert_eq!(files.len(), 1, "{files:?}");
        assert_eq!(
            std::fs::read(to.join("metadata.json"))?,
            std::fs::read(&files[0])?
        );
        assert!(to.join("data.db").is_file());
        fleet.stop().await;
        Ok(())
    }

    #[cfg(unix)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn db_archive_verify_agrees_with_the_live_replicas() -> TestResult {
        use super::binary::{args, fleetringd, stdout};

        let dir = tempfile::tempdir()?;
        let fleet = archived_fleet(dir.path()).await?;
        let client = fleet.client()?;
        let ledger = name("ledger")?;
        create_archived(&fleet, &client, &ledger, 5).await?;
        converged(&client, &ledger).await?;
        let control = fleet.control_address()?;

        let verified = stdout(
            &fleetringd(
                fleet.tls.dir(),
                args(
                    &["db", "archive", "verify", "ledger", "--archive", ARCHIVE],
                    &control,
                ),
            )
            .await?,
        )?;
        assert!(
            verified.contains("archive agrees with every replica\n"),
            "{verified}"
        );
        let described = stdout(
            &fleetringd(
                fleet.tls.dir(),
                args(
                    &["db", "archive", "show", "ledger", "--archive", ARCHIVE],
                    &control,
                ),
            )
            .await?,
        )?;
        assert!(described.contains("database: ledger\n"), "{described}");
        assert!(described.contains("snapshots:\n  "), "{described}");

        // Offline, from a configuration that names the archive and no fleet, a restore leaves a
        // plain SQLite database.
        let config = dir.path().join("offline.toml");
        std::fs::write(
            &config,
            format!(
                "data_dir = {:?}\nlisten = \"127.0.0.1:0\"\ninsecure_plaintext = true\n\n\
                 [node]\nid = \"offline\"\n\n[[archive]]\nname = \"{ARCHIVE}\"\n\
                 databases = [\"*\"]\nstore = \"directory\"\npath = {:?}\n",
                dir.path().join("offline"),
                dir.path().join("archive")
            ),
        )?;
        let to = dir.path().join("recovered");
        let words: Vec<String> = [
            "archive",
            "restore",
            "--config",
            config.to_str().ok_or("a UTF-8 path")?,
            "--archive",
            ARCHIVE,
            "--from",
            "ledger",
            "--to",
            to.to_str().ok_or("a UTF-8 path")?,
        ]
        .iter()
        .map(|word| (*word).to_owned())
        .collect();
        let restored = stdout(&fleetringd(fleet.tls.dir(), words).await?)?;
        assert!(restored.contains("plain SQLite database"), "{restored}");
        let conn = rusqlite::Connection::open(to.join("data.db"))?;
        let rows = conn
            .prepare("SELECT x FROM t ORDER BY x")?
            .query_map([], |row| row.get(0))?
            .collect::<Result<Vec<i64>, _>>()?;
        assert_eq!(rows, (1..=5).collect::<Vec<_>>());
        fleet.stop().await;
        Ok(())
    }
}

/// FR-070 phase 3: expired databases leave the data nodes, the last writes of a dropped database
/// reach its archive, and a restore's expiry is set only after the restore (ADR-0041, points 4
/// and 10).
mod expiry {
    use std::path::Path;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::Duration;

    use fleetring_control::{Action, ControlConfig, DatabaseName, ExpiryChange, Millis, Timestamp};
    use fleetringd::clock::{Clock, SystemClock};
    use fleetringd::control::CommandLog;
    use fleetringd::data::{Client, DatabaseSpec, Expiry, NewMetadata, RestoredExpiry};
    use fleetringd::durability::RestoreTarget;
    use fleetringd::engine::{Statement, Value};
    use tonic::Code;

    use super::archives::{ARCHIVE, archived_fleet, create_archived};
    use super::{
        DATA, Fleet, HANG_GUARD, TestResult, every_node_has, name, no_node_has, three, wait_until,
    };

    /// The system clock moved forward by however far the test has advanced it: a control node
    /// whose clock jumps ahead, which expires at once every database due within the jump.
    #[derive(Debug, Clone, Default)]
    struct JumpingClock(Arc<AtomicU64>);

    impl JumpingClock {
        fn jump(&self, by: Duration) {
            let by = u64::try_from(by.as_millis()).unwrap_or(u64::MAX);
            self.0.fetch_add(by, Ordering::SeqCst);
        }
    }

    impl Clock for JumpingClock {
        fn now(&self) -> Timestamp {
            SystemClock
                .now()
                .saturating_add(Millis::new(self.0.load(Ordering::SeqCst)))
        }
    }

    fn expiring_in(expires_in: Duration) -> NewMetadata {
        NewMetadata {
            expiry: Some(Expiry::In(expires_in)),
            ..NewMetadata::default()
        }
    }

    async fn insert(client: &Client, database: &DatabaseName, x: i64) -> TestResult {
        client
            .execute(
                database,
                Statement::with_params("INSERT INTO t (x) VALUES (?1)", [Value::Integer(x)]),
            )
            .await?;
        Ok(())
    }

    async fn values(
        client: &Client,
        database: &DatabaseName,
    ) -> Result<Vec<i64>, Box<dyn std::error::Error>> {
        let rows = client
            .query(database, Statement::new("SELECT x FROM t ORDER BY x"))
            .await?;
        Ok(rows
            .rows
            .iter()
            .filter_map(|row| match row.first() {
                Some(Value::Integer(x)) => Some(*x),
                _ => None,
            })
            .collect())
    }

    /// Every action the control log in `dir` holds about `database`, in log order.
    fn logged_about(
        dir: &Path,
        database: &DatabaseName,
    ) -> Result<Vec<Action>, Box<dyn std::error::Error>> {
        let log = CommandLog::open(dir.join("control").join("control.db"), ControlConfig::new())?;
        Ok(log
            .commands()?
            .into_iter()
            .map(|(_, command)| command.action)
            .filter(|action| match action {
                Action::CreateDatabase { name, .. }
                | Action::UpdateDatabase { name, .. }
                | Action::ExpireDatabase { name } => name == database,
                Action::AcquireLease {
                    database: leased, ..
                } => leased == database,
                _ => false,
            })
            .collect())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_expired_databases_replicas_delete_their_copies() -> TestResult {
        let dir = tempfile::tempdir()?;
        let clock = JumpingClock::default();
        let environment = fleetringd::Environment::production().with_clock(clock.clone());
        let fleet = Fleet::start_in(dir.path(), "", "", "", environment).await?;
        let client = fleet.client()?;
        let scratch = name("scratch")?;
        let kept = name("kept")?;
        let spec = DatabaseSpec::new(three()?);
        client
            .create_database_with_metadata(&scratch, spec, &expiring_in(Duration::from_secs(60)))
            .await?;
        client
            .create_database_with_metadata(&kept, spec, &expiring_in(Duration::from_secs(3_600)))
            .await?;
        for database in [&scratch, &kept] {
            client
                .execute(
                    database,
                    Statement::new("CREATE TABLE t (x INTEGER NOT NULL)"),
                )
                .await?;
            insert(&client, database, 1).await?;
        }
        wait_until("every node holds both databases", async || {
            every_node_has(dir.path(), "scratch") && every_node_has(dir.path(), "kept")
        })
        .await?;

        // The control node's clock passes the first expiry: its sweep drops the database, and
        // every data node deletes its copy within a renewal interval or a stream's end.
        clock.jump(Duration::from_secs(61));
        wait_until("no node holds scratch", async || {
            no_node_has(dir.path(), "scratch")
        })
        .await?;
        let gone = client.locate(&scratch).await.expect_err("expired");
        assert_eq!(gone.code(), Code::NotFound, "{gone}");
        for id in DATA {
            let replication = fleet
                .data_daemon(id)?
                .replication()
                .ok_or("no replication")?;
            assert_eq!(replication.serving(&scratch), None, "{id} serves scratch");
        }
        assert!(every_node_has(dir.path(), "kept"));
        let kept_until = client
            .locate(&kept)
            .await?
            .metadata
            .and_then(|metadata| metadata.expires_at)
            .ok_or("kept has no expiry")?;
        assert!(kept_until > clock.now(), "kept expires later");
        fleet.stop().await;

        let expired: Vec<Action> = logged_about(dir.path(), &scratch)?
            .into_iter()
            .filter(|action| matches!(action, Action::ExpireDatabase { .. }))
            .collect();
        assert_eq!(expired.len(), 1, "{expired:?}");
        Ok(())
    }

    /// A dropped database's primary flushes its archive before it deletes its copy, so writes
    /// still waiting for an hourly segment reach the archive, and a restore brings them back.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_dropped_databases_last_writes_reach_its_archive() -> TestResult {
        let dir = tempfile::tempdir()?;
        let table = format!(
            "\n[[archive]]\nname = \"{ARCHIVE}\"\ndatabases = [\"*\"]\nstore = \"directory\"\n\
             path = {:?}\nsegment_interval_ms = 3600000\n",
            dir.path().join("archive")
        );
        let fleet = Fleet::start_configured(dir.path(), "", "", &table).await?;
        let client = fleet.client()?;
        let ledger = name("ledger")?;
        let location = client.create_database(&ledger, three()?).await?;
        client
            .execute(
                &ledger,
                Statement::new("CREATE TABLE t (x INTEGER NOT NULL)"),
            )
            .await?;
        let primary = location.primary.ok_or("no primary")?;
        let streams = fleet
            .data_daemon(primary.as_str())?
            .change_streams()
            .ok_or("no change streams")?;
        // The first delivery puts the base snapshot; the next segment would wait an hour.
        wait_until("the base snapshot", async || {
            streams
                .status()
                .iter()
                .any(|status| status.database == ledger && status.position.is_some())
        })
        .await?;
        for x in 1..=5 {
            insert(&client, &ledger, x).await?;
        }

        client.drop_database(&ledger).await?;
        wait_until("no node holds ledger", async || {
            no_node_has(dir.path(), "ledger")
        })
        .await?;
        let revived = name("revived")?;
        client
            .restore_database(
                &revived,
                DatabaseSpec::new(three()?),
                ARCHIVE,
                &ledger,
                &RestoreTarget::Latest,
            )
            .await?;
        assert_eq!(values(&client, &revived).await?, [1, 2, 3, 4, 5]);
        fleet.stop().await;
        Ok(())
    }

    /// A restore's expiry is set with an update once `Restore` has returned, never with the
    /// creation, so a short one can't drop the database while it is being restored.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_short_expiry_on_restore_is_set_only_after_the_restore() -> TestResult {
        let dir = tempfile::tempdir()?;
        let fleet = archived_fleet(dir.path()).await?;
        let client = fleet.client()?;
        let ledger = name("ledger")?;
        create_archived(&fleet, &client, &ledger, 3).await?;

        let copy = name("copy")?;
        let asked = SystemClock.now();
        let restored = tokio::time::timeout(
            HANG_GUARD,
            client.restore_database_with_metadata(
                &copy,
                DatabaseSpec::new(three()?),
                &expiring_in(Duration::from_secs(60)),
                ARCHIVE,
                &ledger,
                &RestoreTarget::Latest,
            ),
        )
        .await??;
        assert!(restored.location.primary.is_some());
        // The restore itself set no expiry, and it is now set as `db restore` sets it, after
        // the archive's description and labels.
        assert_eq!(
            client
                .locate(&copy)
                .await?
                .metadata
                .and_then(|m| m.expires_at),
            None
        );
        let expiry = client
            .set_restored_expiry(&copy, Some(Expiry::In(Duration::from_secs(60))))
            .await;
        let RestoredExpiry::Set(expires_at) = expiry else {
            return Err(format!("no expiry was set: {expiry:?}").into());
        };
        assert!(
            expires_at >= asked.saturating_add(Millis::new(60_000)),
            "{expires_at} is a minute from when the restore returned"
        );
        assert_eq!(values(&client, &copy).await?, [1, 2, 3]);
        fleet.stop().await;

        // The creation carried no expiry; the update that set it came after the restoring node
        // took the new database's first lease, which ends the restore.
        let logged = logged_about(dir.path(), &copy)?;
        let created = logged
            .iter()
            .position(|action| {
                matches!(action, Action::CreateDatabase { metadata, .. }
                    if metadata.as_ref().is_none_or(|spec| spec.expires_at.is_none()))
            })
            .ok_or("no creation without an expiry")?;
        let leased = logged
            .iter()
            .position(|action| matches!(action, Action::AcquireLease { .. }))
            .ok_or("no lease")?;
        let expiry_set = logged
            .iter()
            .position(|action| {
                matches!(action, Action::UpdateDatabase { change, .. }
                    if matches!(change.expiry, ExpiryChange::Set(_)))
            })
            .ok_or("no update setting the expiry")?;
        assert!(created < leased && leased < expiry_set, "{logged:?}");
        Ok(())
    }
}
