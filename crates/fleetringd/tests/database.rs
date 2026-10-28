//! FR-033: the `Database` service, the cluster client, and the command line, over real TCP.
//!
//! Each test starts a control daemon and three data daemons in process, on loopback ports the
//! operating system picks.

use std::error::Error;
use std::net::SocketAddr;
use std::path::Path;
use std::time::Duration;

use fleetring::ReplicationFactor;
use fleetring_control::{DatabaseName, Liveness};
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
        let control =
            Daemon::start(Config::from_toml(&config(dir, "control", "control", None))?).await?;
        let address = control.control_address().ok_or("no control address")?;
        let mut data = Vec::new();
        for id in DATA {
            let text = config(dir, id, "data", Some(address));
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
    assert_eq!(written.acknowledged_by, 1, "no followers until replication");
    assert_eq!(written.replicas, 3);
    assert!(written.under_replicated);

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

#[cfg(unix)]
mod binary {
    use std::process::{Command, Output};

    use super::{Cluster, TestResult, config, follower_address, name};

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
        assert_eq!(
            table,
            "0 rows affected; acknowledged by 1 of 3 replicas; under-replicated\n"
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
        assert_eq!(
            insert,
            "1 row affected; acknowledged by 1 of 3 replicas; under-replicated\n"
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
}
