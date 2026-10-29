//! FR-033: the `Database` service, the cluster client, and the command line, over real TCP.
//!
//! Each test starts a control daemon and three data daemons in process, on loopback ports the
//! operating system picks. Writes replicate to their followers through the daemons' own
//! `Replication` services (FR-030).

use std::error::Error;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use fleetring::ReplicationFactor;
use fleetring_control::{DatabaseName, Liveness, Role};
use fleetring_proto::v1::ExecuteRequest;
use fleetringd::data::{Client, Location, primary_hint};
use fleetringd::engine::{Statement, Value};
use fleetringd::transport::database_client;
use fleetringd::{Config, Daemon, Network};
use tonic::Code;
use tonic::transport::Endpoint;

type TestResult = Result<(), Box<dyn Error>>;

/// Guards against a hang only; nothing here asserts how long anything takes.
const HANG_GUARD: Duration = Duration::from_secs(60);

const DATA: [&str; 3] = ["data-1", "data-2", "data-3"];

fn config(dir: &Path, id: &str, role: &str, control: Option<SocketAddr>) -> String {
    let control = control.map_or_else(String::new, |address| {
        format!("\n[control]\naddress = \"{address}\"\n")
    });
    format!(
        "data_dir = {:?}\nlisten = \"127.0.0.1:0\"\nrole = \"{role}\"\n\n[node]\nid = \"{id}\"\n\
         \n[membership]\nheartbeat_interval_ms = 100\n{control}",
        dir.join(id)
    )
}

/// A control daemon and three data daemons that have all joined and published one ring.
struct Cluster {
    control: Daemon,
    data: Vec<Daemon>,
}

impl Cluster {
    async fn start(dir: &Path) -> Result<Self, Box<dyn Error>> {
        Self::start_with(dir, "").await
    }

    /// Starts the cluster with `control_settings` appended to every daemon's `[control]` table.
    async fn start_with(dir: &Path, control_settings: &str) -> Result<Self, Box<dyn Error>> {
        let control_text = format!(
            "{}\n[control]\n{control_settings}",
            config(dir, "control", "control", None)
        );
        let control = Daemon::start(Config::from_toml(&control_text)?).await?;
        let address = control.control_address().ok_or("no control address")?;
        let mut data = Vec::new();
        for id in DATA {
            let text = format!(
                "{}{control_settings}",
                config(dir, id, "data", Some(address))
            );
            data.push(Daemon::start(Config::from_toml(&text)?).await?);
        }
        let cluster = Self { control, data };
        cluster.converge().await?;
        Ok(cluster)
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
        Ok(Client::new(self.control_address()?, Network::default()))
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
    let cluster = Cluster::start(dir.path()).await?;
    let client = cluster.client()?;
    let acme = name("tenant:acme")?;

    let created = client.create_database(&acme, three()?).await?;
    let primary = created.primary.clone().ok_or("no primary after create")?;
    assert_eq!(created.epoch.get(), 1);
    assert_eq!(created.replicas.len(), 3);
    assert_eq!(created.followers().count(), 2);
    // The primary is the first live member of the replica set in the ring's preference order.
    assert_eq!(created.replicas[0].node, primary);
    let registry = cluster
        .data_daemon(primary.as_str())?
        .registry()
        .ok_or("no registry")?;
    assert!(registry.is_open(&acme), "the primary opened its database");
    for follower in created.followers() {
        let registry = cluster
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
    let primary_address = cluster
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

    cluster.stop().await;
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
    let cluster = Cluster::start(dir.path()).await?;
    let client = cluster.client()?;
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
    let replication = cluster
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
        let replication = cluster
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
            Role::Primary
        } else {
            Role::Follower
        };
        assert_eq!(replica.reported_role, Some(role), "{}", replica.node);
    }

    cluster.stop().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_request_to_a_follower_is_redirected_to_the_primary() -> TestResult {
    let dir = tempfile::tempdir()?;
    let cluster = Cluster::start(dir.path()).await?;
    let client = cluster.client()?;
    let acme = name("acme")?;
    let location = client.create_database(&acme, three()?).await?;
    let primary = location.primary.clone().ok_or("no primary")?;
    let primary_address = cluster
        .data_daemon(primary.as_str())?
        .listen_address()
        .to_string();
    let follower = follower_address(&location)?;

    // Sent straight to a follower, a write is answered with the primary's address.
    let mut raw = database_client(
        Endpoint::from_shared(format!("http://{follower}"))?
            .connect()
            .await?,
    );
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
    let via_follower = cluster.client()?.with_entry(follower);
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

    cluster.stop().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn errors_map_to_statuses() -> TestResult {
    let dir = tempfile::tempdir()?;
    let cluster = Cluster::start(dir.path()).await?;
    let client = cluster.client()?;
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

    cluster.stop().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn status_shows_the_ring_every_node_publishes() -> TestResult {
    let dir = tempfile::tempdir()?;
    let cluster = Cluster::start(dir.path()).await?;
    let view = cluster.client()?.status().await?;

    let state = cluster.control.control().ok_or("no control")?.state();
    let fingerprint = state.ring()?.fingerprint();
    assert_eq!(view.fingerprint(), Some(fingerprint));
    assert_eq!(view.epoch(), state.membership_epoch());
    for daemon in &cluster.data {
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
    let expected: Vec<(String, Option<String>, Liveness)> = cluster
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

    cluster.stop().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_restarted_primary_serves_its_database_again() -> TestResult {
    let dir = tempfile::tempdir()?;
    let Cluster { control, mut data } = Cluster::start(dir.path()).await?;
    let cluster_control = control.control_address().ok_or("no control address")?;
    let client = Client::new(cluster_control.to_string(), Network::default());
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
    let cluster = Cluster { control, data };
    cluster.converge().await?;
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

    cluster.stop().await;
    Ok(())
}

/// Timings short enough for a failover to finish quickly over loopback.
const FAST_LEASES: &str = "lease_duration_ms = 800\nfailover_margin_ms = 100\n\
                           heartbeat_timeout_ms = 500\ntick_interval_ms = 20\n";

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn db_exec_keeps_working_through_a_primary_failover_after_a_retry() -> TestResult {
    let dir = tempfile::tempdir()?;
    let cluster = Cluster::start_with(dir.path(), FAST_LEASES).await?;
    let client = cluster.client()?;
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
    let Cluster { control, mut data } = cluster;
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
    Cluster { control, data }.stop().await;
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
    let cluster = Cluster::start(dir.path()).await?;
    let client = cluster.client()?;
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

    cluster.stop().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dropping_a_database_removes_it_from_every_node() -> TestResult {
    let dir = tempfile::tempdir()?;
    let cluster = Cluster::start_with(dir.path(), FAST_LEASES).await?;
    let client = cluster.client()?;
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
    let primary_registry = cluster
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
        let replication = cluster
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
    let old_primary = cluster
        .data_daemon(primary.as_str())?
        .listen_address()
        .to_string();
    let at_primary = cluster
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

    cluster.stop().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn status_node_shows_what_each_data_node_knows() -> TestResult {
    let dir = tempfile::tempdir()?;
    let cluster = Cluster::start(dir.path()).await?;
    let client = cluster.client()?;
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

    let state = cluster.control.control().ok_or("no control")?.state();
    let fingerprint = state.ring()?.fingerprint();
    for daemon in &cluster.data {
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
            Role::Primary
        } else {
            Role::Follower
        };
        assert_eq!(served.role, role, "{id}");
        assert_eq!(served.epoch.get(), 1, "{id}");
    }

    // The control node hosts no data, so it has no node status to give.
    let control = cluster.control_address()?;
    let refused = client
        .node_status(&control)
        .await
        .expect_err("no data role");
    assert_eq!(refused.code(), Code::Unimplemented, "{refused}");

    cluster.stop().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_control_call_to_a_data_node_names_the_control_node() -> TestResult {
    let dir = tempfile::tempdir()?;
    let cluster = Cluster::start(dir.path()).await?;
    let control = cluster.control_address()?;
    for daemon in &cluster.data {
        let data_node = daemon.listen_address().to_string();
        let wrong = Client::new(data_node, Network::default());
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
    cluster.stop().await;
    Ok(())
}

#[cfg(unix)]
mod binary {
    use std::process::{Command, Output};

    use super::{
        Cluster, FAST_LEASES, Location, TestResult, config, every_node_has, follower_address, name,
        no_node_has, three, wait_until,
    };

    /// Runs `fleetringd` with `args` on a blocking thread, so the daemons in this process keep
    /// serving.
    async fn fleetringd(args: Vec<String>) -> Result<Output, Box<dyn std::error::Error>> {
        Ok(tokio::task::spawn_blocking(move || {
            Command::new(env!("CARGO_BIN_EXE_fleetringd"))
                .args(&args)
                .output()
        })
        .await??)
    }

    fn args(words: &[&str], control: &str) -> Vec<String> {
        words
            .iter()
            .map(|word| (*word).to_owned())
            .chain(["--control".to_owned(), control.to_owned()])
            .collect()
    }

    fn stdout(output: &Output) -> Result<String, Box<dyn std::error::Error>> {
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
        let cluster = Cluster::start(dir.path()).await?;
        let control = cluster.control_address()?;

        let created = stdout(
            &fleetringd(args(
                &["db", "create", "tenant:acme", "--replication-factor", "3"],
                &control,
            ))
            .await?,
        )?;
        assert!(created.starts_with("created tenant:acme\n"), "{created}");
        assert!(created.contains("epoch: 1\n"), "{created}");

        let table = stdout(
            &fleetringd(args(
                &[
                    "db",
                    "exec",
                    "tenant:acme",
                    "CREATE TABLE t (n INTEGER, s TEXT, r REAL, b BLOB, z)",
                ],
                &control,
            ))
            .await?,
        )?;
        assert!(
            [2, 3].iter().any(|count| table
                == format!("0 rows affected; acknowledged by {count} of 3 replicas\n")),
            "{table}"
        );
        let insert = stdout(
            &fleetringd(args(
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
            ))
            .await?,
        )?;
        assert!(
            [2, 3].iter().any(|count| insert
                == format!("1 row affected; acknowledged by {count} of 3 replicas\n")),
            "{insert}"
        );

        let rows = stdout(
            &fleetringd(args(
                &["db", "query", "tenant:acme", "SELECT n, s, r, b, z FROM t"],
                &control,
            ))
            .await?,
        )?;
        assert_eq!(rows, "n\ts\tr\tb\tz\n-4\t42\t2.5\tx'00ff'\tNULL\n");

        // Through a follower, the request is redirected to the primary.
        let location = cluster.client()?.locate(&name("tenant:acme")?).await?;
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
        via_follower.extend(["--node".to_owned(), follower]);
        assert_eq!(stdout(&fleetringd(via_follower).await?)?, "rows\n1\n");

        let located = stdout(&fleetringd(args(&["db", "locate", "tenant:acme"], &control)).await?)?;
        let primary = location.primary.ok_or("no primary")?;
        assert!(
            located.contains(&format!("primary: {primary} (")),
            "{located}"
        );
        assert!(located.contains("engine: sqlite\n"), "{located}");
        assert!(located.contains("positions:\n"), "{located}");

        let status = stdout(&fleetringd(args(&["status"], &control)).await?)?;
        let fingerprint = cluster
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
        for daemon in &cluster.data {
            let line = format!(
                "  {} live weight=1 address={}\n",
                daemon.config().node.id,
                daemon.listen_address()
            );
            assert!(status.contains(&line), "{line} in {status}");
        }

        // A daemon's configuration file names the control plane too.
        let file = dir.path().join("data.toml");
        let address = cluster
            .control
            .control_address()
            .ok_or("no control address")?;
        std::fs::write(&file, config(dir.path(), "data-9", "data", Some(address)))?;
        let by_config = fleetringd(vec![
            "status".to_owned(),
            "--config".to_owned(),
            file.display().to_string(),
        ])
        .await?;
        assert_eq!(stdout(&by_config)?, status);

        let failed = fleetringd(args(&["db", "exec", "tenant:acme", "NOT SQL"], &control)).await?;
        assert!(!failed.status.success());
        let stderr = String::from_utf8_lossy(&failed.stderr);
        assert!(
            stderr.contains("Client specified an invalid argument"),
            "{stderr}"
        );

        cluster.stop().await;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_command_line_lists_drops_and_asks_a_node() -> TestResult {
        let dir = tempfile::tempdir()?;
        let cluster = Cluster::start_with(dir.path(), FAST_LEASES).await?;
        let control = cluster.control_address()?;
        let client = cluster.client()?;
        let mut locations = Vec::new();
        for text in ["orders", "acme"] {
            locations.push(client.create_database(&name(text)?, three()?).await?);
        }

        let listed = stdout(&fleetringd(args(&["db", "list"], &control)).await?)?;
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
            &fleetringd(args(&["db", "list", "--node", primary.as_str()], &control)).await?,
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
        let fingerprint = cluster
            .control
            .control()
            .ok_or("no control")?
            .state()
            .ring()?
            .fingerprint();
        for daemon in &cluster.data {
            let address = daemon.listen_address().to_string();
            let status = stdout(
                &fleetringd(vec!["status".to_owned(), "--node".to_owned(), address]).await?,
            )?;
            assert!(
                status.starts_with(&format!("node: {}\n", daemon.config().node.id)),
                "{status}"
            );
            assert!(
                status.contains(&format!("ring fingerprint: {fingerprint}\n")),
                "{status}"
            );
        }

        // A control call aimed at a data node names the control node.
        let wrong = fleetringd(args(
            &["db", "list"],
            &cluster.data[0].listen_address().to_string(),
        ))
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
        let asked = fleetringd(args(&["db", "drop", "orders"], &control)).await?;
        assert!(!asked.status.success());
        assert!(
            String::from_utf8_lossy(&asked.stderr).contains("pass --yes"),
            "{}",
            String::from_utf8_lossy(&asked.stderr)
        );
        assert!(client.locate(&name("orders")?).await.is_ok());
        let dropped =
            stdout(&fleetringd(args(&["db", "drop", "orders", "--yes"], &control)).await?)?;
        assert_eq!(dropped, "dropped orders at epoch 1\n");
        let gone = fleetringd(args(&["db", "locate", "orders"], &control)).await?;
        assert!(!gone.status.success());
        wait_until("every node deleted its copy of orders", async || {
            no_node_has(dir.path(), "orders")
        })
        .await?;
        let listed = stdout(&fleetringd(args(&["db", "list"], &control)).await?)?;
        assert!(!listed.contains("orders"), "{listed}");

        cluster.stop().await;
        Ok(())
    }
}
