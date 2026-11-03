//! FR-046: mutual TLS between nodes and clients, over real TCP, with certificates from the
//! built-in authority.

use std::error::Error;
use std::net::SocketAddr;
use std::path::Path;
use std::time::Duration;

use fleetring::ReplicationFactor;
use fleetring_control::Liveness;
use fleetring_proto::PROTOCOL_VERSION;
use fleetring_proto::v1::{
    DrainRequest, FenceRequest, HeartbeatRequest, JoinRequest, LeaveRequest, LocateRequest, Node,
};
use fleetringd::data::Client;
use fleetringd::engine::Statement;
use fleetringd::tls::{Authority, Identity, Security};
use fleetringd::transport::{Backoff, replication_client};
use fleetringd::{Config, ConfigError, Daemon, Network, StartError};
use tonic::Code;

use crate::support::Tls;

mod support;

type TestResult = Result<(), Box<dyn Error>>;

/// Guards against a hang only; nothing here asserts how long anything takes.
const HANG_GUARD: Duration = Duration::from_secs(60);

/// One attempt, so a refused connection fails at once instead of after every retry.
const ONE_ATTEMPT: Backoff = Backoff {
    initial: Duration::from_millis(10),
    max: Duration::from_millis(10),
    attempts: 1,
};

fn config(dir: &Path, id: &str, role: &str, control: Option<SocketAddr>, tls: &str) -> String {
    let control = control.map_or_else(String::new, |address| {
        format!("\n[control]\naddress = \"{address}\"\n")
    });
    format!(
        "data_dir = {:?}\nlisten = \"127.0.0.1:0\"\nrole = \"{role}\"\n\n[node]\nid = \"{id}\"\n\
         \n[membership]\nheartbeat_interval_ms = 100\n{tls}{control}",
        dir.join(id)
    )
}

async fn control_daemon(dir: &Path, tls: &Tls) -> Result<Daemon, Box<dyn Error>> {
    let table = tls.node_table("control")?;
    Ok(Daemon::start(Config::from_toml(&config(
        dir, "control", "control", None, &table,
    ))?)
    .await?)
}

/// A refused connection never reaches a service: the failure is the transport's, with the
/// underlying error as its source. Which code tonic picks depends on how far the handshake got:
/// a connection error is `UNAVAILABLE`, a stream the server closed mid-handshake is `UNKNOWN`,
/// and a TLS alert read as HTTP/2 frames is `INTERNAL`. None is a status a handler produced.
fn assert_transport_failure(status: &tonic::Status) {
    assert!(
        matches!(
            status.code(),
            Code::Unavailable | Code::Unknown | Code::Internal
        ),
        "{status}"
    );
    assert!(status.source().is_some(), "{status}");
}

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

#[tokio::test]
async fn a_daemon_without_an_identity_refuses_to_start_unless_plaintext_is_allowed() -> TestResult {
    let dir = tempfile::tempdir()?;
    let text = config(dir.path(), "solo", "both", None, "");
    let refused = Daemon::start(Config::from_toml(&text)?)
        .await
        .expect_err("no [tls] table");
    assert!(
        matches!(
            refused,
            StartError::Config(ConfigError::Missing { field: "tls", .. })
        ),
        "{refused}"
    );
    assert!(refused.to_string().contains("--insecure-plaintext"));

    let allowed = format!("insecure_plaintext = true\n{text}");
    let daemon = Daemon::start(Config::from_toml(&allowed)?).await?;
    let client = Client::new(daemon.listen_address().to_string(), Network::default());
    let status = tokio::time::timeout(HANG_GUARD, client.status()).await??;
    assert_eq!(status.members().len(), 1);
    daemon.run_until(async {}).await;
    Ok(())
}

#[tokio::test]
async fn clients_without_a_trusted_certificate_are_refused() -> TestResult {
    let dir = tempfile::tempdir()?;
    let tls = Tls::create(dir.path())?;
    let daemon = control_daemon(dir.path(), &tls).await?;
    let address = daemon.control_address().ok_or("no control address")?;
    let locate = LocateRequest {
        database: "acme".into(),
    };

    // No certificate at all: a plaintext client.
    let plaintext = Network::default()
        .control_client(address.to_string())
        .with_backoff(ONE_ATTEMPT);
    let refused = tokio::time::timeout(HANG_GUARD, plaintext.locate(locate.clone()))
        .await?
        .expect_err("plaintext is refused");
    assert_transport_failure(&refused);

    // A certificate from another authority.
    let other = Authority::create("test".parse()?, Tls::validity())?;
    let stranger = Identity::from_issued(
        other.certificate_pem(),
        &other.issue_client(&"stranger".parse()?, Tls::validity())?,
    )?;
    let stranger = Network::default()
        .with_security(Security::Tls(stranger))
        .control_client(address.to_string())
        .with_backoff(ONE_ATTEMPT);
    let refused = tokio::time::timeout(HANG_GUARD, stranger.locate(locate.clone()))
        .await?
        .expect_err("another authority is refused");
    assert_transport_failure(&refused);

    // A certificate from the right authority that has expired.
    let expired = Identity::from_issued(
        tls.authority().certificate_pem(),
        &tls.authority()
            .issue_client(&"late".parse()?, Tls::expired())?,
    )?;
    let expired = Network::default()
        .with_security(Security::Tls(expired))
        .control_client(address.to_string())
        .with_backoff(ONE_ATTEMPT);
    let refused = tokio::time::timeout(HANG_GUARD, expired.locate(locate.clone()))
        .await?
        .expect_err("an expired certificate is refused");
    assert_transport_failure(&refused);

    // A valid one is answered: the database doesn't exist, which is the control plane talking.
    let tester = tls
        .client_network("tester")?
        .control_client(address.to_string());
    let answered = tokio::time::timeout(HANG_GUARD, tester.locate(locate))
        .await?
        .expect_err("no such database");
    assert_eq!(answered.code(), Code::NotFound, "{answered}");
    daemon.run_until(async {}).await;
    Ok(())
}

#[tokio::test]
async fn a_node_certificate_for_another_id_cannot_join_heartbeat_leave_or_drain() -> TestResult {
    let dir = tempfile::tempdir()?;
    let tls = Tls::create(dir.path())?;
    let daemon = control_daemon(dir.path(), &tls).await?;
    let address = daemon.control_address().ok_or("no control address")?;
    let other = tls
        .node_network("other")?
        .control_client(address.to_string());

    let refused = other.join(join_request("b")).await.expect_err("wrong id");
    assert_eq!(refused.code(), Code::PermissionDenied, "{refused}");
    assert_eq!(
        refused.message(),
        "Join for node 'b' needs that node's certificate"
    );
    let refused = other
        .heartbeat(HeartbeatRequest {
            node_id: "b".into(),
            databases: Vec::new(),
        })
        .await
        .expect_err("wrong id");
    assert_eq!(refused.code(), Code::PermissionDenied, "{refused}");
    let refused = other
        .drain(DrainRequest {
            node_id: "b".into(),
        })
        .await
        .expect_err("wrong id");
    assert_eq!(refused.code(), Code::PermissionDenied, "{refused}");
    let refused = other
        .leave(LeaveRequest {
            node_id: "b".into(),
        })
        .await
        .expect_err("wrong id");
    assert_eq!(refused.code(), Code::PermissionDenied, "{refused}");
    let state = daemon.control().ok_or("no control handle")?.state();
    assert_eq!(state.members().len(), 0, "nothing joined");

    // As itself, the node is welcome.
    let joined = other.join(join_request("other")).await?;
    assert_eq!(joined.membership_epoch, 1);
    other
        .heartbeat(HeartbeatRequest {
            node_id: "other".into(),
            databases: Vec::new(),
        })
        .await?;
    other
        .leave(LeaveRequest {
            node_id: "other".into(),
        })
        .await?;
    daemon.run_until(async {}).await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_principal_cannot_make_node_to_node_calls() -> TestResult {
    let dir = tempfile::tempdir()?;
    let tls = Tls::create(dir.path())?;
    let table = tls.node_table("solo")?;
    let daemon = Daemon::start(Config::from_toml(&config(
        dir.path(),
        "solo",
        "both",
        None,
        &table,
    ))?)
    .await?;
    let address = daemon.listen_address().to_string();
    let principal = tls.client_network("tester")?;
    let mut replication = replication_client(principal.channel(&address)?);
    let refused = replication
        .fence(FenceRequest {
            database: "acme".into(),
            epoch: 1,
            candidate: "solo".into(),
        })
        .await
        .expect_err("a principal can't fence");
    assert_eq!(refused.code(), Code::PermissionDenied, "{refused}");
    assert_eq!(refused.message(), "Fence needs a node certificate");

    // A node passes the boundary and gets the real answer: its binding is checked against
    // control state, which knows no such database (FR-050).
    let node = tls.node_network("peer")?;
    let mut replication = replication_client(node.channel(&address)?);
    let answered = replication
        .fence(FenceRequest {
            database: "acme".into(),
            epoch: 1,
            candidate: "peer".into(),
        })
        .await
        .expect_err("control state knows no promotion of acme");
    assert_eq!(answered.code(), Code::NotFound, "{answered}");
    daemon.run_until(async {}).await;
    Ok(())
}

/// Replaces `path` atomically, so a reload never reads half a file.
fn replace_file(path: &Path, contents: &str) -> Result<(), Box<dyn Error>> {
    let staged = path.with_extension("staged");
    std::fs::write(&staged, contents)?;
    std::fs::rename(&staged, path)?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rotated_certificates_are_picked_up_without_a_restart_while_a_stream_stays_open()
-> TestResult {
    let dir = tempfile::tempdir()?;
    let tls = Tls::create(dir.path())?;
    let control = control_daemon(dir.path(), &tls).await?;
    let address = control.control_address().ok_or("no control address")?;
    let mut data = Vec::new();
    for id in ["data-1", "data-2"] {
        let table = tls.node_table(id)?;
        let text = config(dir.path(), id, "data", Some(address), &table);
        data.push(Daemon::start(Config::from_toml(&text)?).await?);
    }
    let handle = control.control().ok_or("no control handle")?;
    let mut states = handle.watch();
    tokio::time::timeout(
        HANG_GUARD,
        states.wait_for(|state| {
            state.members().len() == 2
                && state
                    .members()
                    .all(|member| state.liveness(member.id(), state.now()) == Some(Liveness::Live))
        }),
    )
    .await??;
    let client = Client::new(address.to_string(), tls.client_network("tester")?);
    let acme: fleetring_control::DatabaseName = "acme".parse()?;
    let two = ReplicationFactor::new(2).ok_or("zero")?;
    let location = tokio::time::timeout(HANG_GUARD, client.create_database(&acme, two)).await??;
    let primary = location.primary.clone().ok_or("no primary")?;
    let written = client
        .execute(&acme, Statement::new("CREATE TABLE t (x)"))
        .await?;
    assert_eq!(
        written.acknowledged_by, 2,
        "the stream to the follower is open"
    );

    // A second authority joins the trusted set on every node, and the primary's certificate
    // is reissued by it.
    let successor = Authority::create("test".parse()?, Tls::validity())?;
    let bundle = format!(
        "{}{}",
        tls.authority().certificate_pem(),
        successor.certificate_pem()
    );
    replace_file(&tls.dir().join("ca.crt"), &bundle)?;
    let reissued = successor.issue_node(&primary, &["127.0.0.1".to_owned()], Tls::validity())?;
    let stem = format!("node.{primary}");
    replace_file(
        &tls.dir().join(format!("{stem}.crt")),
        reissued.certificate_pem(),
    )?;
    replace_file(&tls.dir().join(format!("{stem}.key")), reissued.key_pem())?;

    // A client that trusts only the successor reaches the primary once it has reloaded.
    let primary_address = data
        .iter()
        .find(|daemon| daemon.config().node.id == primary)
        .ok_or("primary isn't a data daemon")?
        .listen_address()
        .to_string();
    let only_successor = Identity::from_issued(
        successor.certificate_pem(),
        &successor.issue_client(&"tester".parse()?, Tls::validity())?,
    )?;
    let fresh = Client::new(
        address.to_string(),
        Network::default().with_security(Security::Tls(only_successor)),
    );
    tokio::time::timeout(HANG_GUARD, async {
        loop {
            match fresh.node_status(&primary_address).await {
                Ok(status) => break Ok::<_, Box<dyn Error>>(status),
                Err(status) if status.code() == Code::Unavailable => {
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
                Err(status) => break Err(status.into()),
            }
        }
    })
    .await??;

    // The replication stream opened under the old certificate carries on: a write is still
    // acknowledged by both replicas, and the primary keeps its lease.
    let written = client
        .execute(&acme, Statement::new("INSERT INTO t VALUES (1)"))
        .await?;
    assert_eq!(
        written.acknowledged_by, 2,
        "the stream survived the rotation"
    );
    assert_eq!(written.primary.as_ref(), Some(&primary));
    assert_eq!(written.epoch, location.epoch, "no failover happened");
    for daemon in data {
        daemon.run_until(async {}).await;
    }
    control.run_until(async {}).await;
    Ok(())
}

/// Certificates `fleetringd` 0.2.0 issued, when the fleet was called a cluster: an authority for
/// `legacy`, the node `node-1` at 127.0.0.1, and the principal `tester`, each valid for a
/// century. The authority's private key isn't kept.
const LEGACY: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/tls-0.2.0");

#[tokio::test]
async fn certificates_from_0_2_still_connect() -> TestResult {
    let fixtures = Path::new(LEGACY);
    let dir = tempfile::tempdir()?;
    let table = format!(
        "\n[tls]\nca = {:?}\ncert = {:?}\nkey = {:?}\n\
         \n[[principal]]\nname = \"tester\"\nscopes = [\"admin:*\", \"fleet\"]\n",
        fixtures.join("ca.crt"),
        fixtures.join("node.node-1.crt"),
        fixtures.join("node.node-1.key"),
    );
    let daemon = Daemon::start(Config::from_toml(&config(
        dir.path(),
        "node-1",
        "both",
        None,
        &table,
    ))?)
    .await?;
    let identity = Identity::load(
        &fixtures.join("ca.crt"),
        &fixtures.join("client.tester.crt"),
        &fixtures.join("client.tester.key"),
    )?;
    assert_eq!(identity.fleet().as_str(), "legacy");
    let address = daemon.control_address().ok_or("no control address")?;
    let client = Client::new(
        address.to_string(),
        Network::default().with_security(Security::Tls(identity)),
    );
    let status = tokio::time::timeout(HANG_GUARD, client.status()).await??;
    assert_eq!(status.members().len(), 1);
    daemon.run_until(async {}).await;
    Ok(())
}

#[test]
fn cert_init_names_the_fleet_and_refuses_the_old_flag() -> TestResult {
    let dir = tempfile::tempdir()?;
    let run = |flag: &str| {
        std::process::Command::new(env!("CARGO_BIN_EXE_fleetringd"))
            .args(["cert", "init", flag, "home", "--dir"])
            .arg(dir.path())
            .output()
    };
    let created = run("--fleet")?;
    assert!(created.status.success(), "{created:?}");
    assert!(created.stderr.is_empty(), "{created:?}");
    assert!(String::from_utf8(created.stdout)?.contains("for fleet home"));
    assert_eq!(Authority::load(dir.path())?.fleet().as_str(), "home");

    let node = std::process::Command::new(env!("CARGO_BIN_EXE_fleetringd"))
        .args([
            "cert",
            "node",
            "node-1",
            "--addresses",
            "127.0.0.1,node-1.lan",
        ])
        .arg("--dir")
        .arg(dir.path())
        .output()?;
    assert!(node.status.success(), "{node:?}");
    assert!(
        String::from_utf8(node.stdout)?
            .contains("issued fleetring://home/node/node-1 for 127.0.0.1, node-1.lan")
    );
    let identity = Identity::load(
        &dir.path().join("ca.crt"),
        &dir.path().join("node.node-1.crt"),
        &dir.path().join("node.node-1.key"),
    )?;
    assert_eq!(identity.fleet().as_str(), "home");

    let refused = run("--cluster")?;
    assert_eq!(refused.status.code(), Some(2), "a usage error: {refused:?}");
    assert!(String::from_utf8(refused.stderr)?.contains("unexpected argument '--cluster'"));
    Ok(())
}
