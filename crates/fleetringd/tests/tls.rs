//! FR-046: mutual TLS between nodes and clients, over real TCP, with certificates from the
//! built-in authority.

use std::error::Error;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use fleetring::ReplicationFactor;
use fleetring_control::{Liveness, Timestamp};
use fleetring_proto::PROTOCOL_VERSION;
use fleetring_proto::v1::{
    DrainRequest, FenceRequest, HeartbeatRequest, JoinRequest, LeaveRequest, LocateRequest, Node,
};
use fleetringd::clock::{Clock as _, SystemClock};
use fleetringd::data::Client;
use fleetringd::engine::Statement;
use fleetringd::tls::{
    Authority, Identity, IdentityError, Issued, IssuerCertificate, MAX_ISSUER_LIFETIME, Raw,
    Security, Validity,
};
use fleetringd::transport::{Backoff, replication_client};
use fleetringd::{Config, ConfigError, Daemon, Network, StartError};
use rustls_pki_types::pem::PemObject as _;
use rustls_pki_types::{CertificateDer, PrivateKeyDer};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tonic::Code;

use crate::support::{ADMIN, Tls};

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
         \n[membership]\nheartbeat_interval_ms = 100\n{tls}{control}{ADMIN}",
        dir.join(id)
    )
}

async fn control_daemon(dir: &Path, tls: &Tls) -> Result<Daemon, Box<dyn Error>> {
    let table = tls.node_tls_table("control")?;
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
        software: None,
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
            software: None,
            ..HeartbeatRequest::default()
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
            software: None,
            ..HeartbeatRequest::default()
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
    let table = tls.node_tls_table("solo")?;
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
        let table = tls.node_tls_table(id)?;
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

/// Credentials the `fleetringd-v0.7.0` tag's own binary issued (FR-096), with the authority's
/// private key dropped: an authority for `legacy` valid until 2036; the node `node-1` at
/// 127.0.0.1 and the principal `tester`, each for a century (`cert node`, `cert client`); a
/// token for `tester` with `admin:*` and `fleet` (`token issue`); the context `legacy`
/// (`context add`), its TLS directory written as `TLS_DIR`; and agent `42` on that context
/// (`agent add 42 --authority`), a version 1 record with a 30-day token.
const FROM_0_7: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures/credentials-0.7.0"
);

#[tokio::test]
async fn certificates_from_0_2_still_connect() -> TestResult {
    node_and_principal_certificates_connect(Path::new(LEGACY)).await
}

#[tokio::test]
async fn certificates_from_0_7_still_connect() -> TestResult {
    node_and_principal_certificates_connect(Path::new(FROM_0_7)).await
}

/// ADR-0047, point 12.1: a token 0.7 signed (its `iat` equal to its `nbf`, and none of the
/// claims 0.8 added) verifies as it did, for its whole life and no longer, and its ID still
/// revokes it. The fixture's tokens expire, so they are checked at fixed times.
#[test]
fn tokens_from_0_7_verify_as_they_did() -> TestResult {
    use base64::Engine as _;
    use fleetring_control::{Revocations, Revoked};
    use fleetringd::auth::{BearerToken, TokenError};

    let fixtures = Path::new(FROM_0_7);
    let identity = Identity::load(
        &fixtures.join("ca.crt"),
        &fixtures.join("client.tester.crt"),
        &fixtures.join("client.tester.key"),
    )?;
    let verifier = identity.token_verifier();
    for (file, principal, scopes) in [
        ("token.tester", "tester", &["admin:*", "fleet"][..]),
        ("agents/42/token", "agent-42", &["admin:agent-42-*"][..]),
    ] {
        let token = BearerToken::new(std::fs::read_to_string(fixtures.join(file))?.trim());
        let payload = token.expose().split('.').nth(1).ok_or("no claims")?;
        let claims: serde_json::Value = serde_json::from_slice(
            &base64::engine::general_purpose::URL_SAFE_NO_PAD.decode(payload)?,
        )?;
        // Exactly 0.7's claims (named in sorted order), with `iat` equal to `nbf`.
        let names: Vec<&str> = claims
            .as_object()
            .ok_or("the claims aren't an object")?
            .keys()
            .map(String::as_str)
            .collect();
        assert_eq!(
            names,
            ["exp", "fleetring.scopes", "iat", "iss", "jti", "nbf", "sub"],
            "{file}"
        );
        assert_eq!(claims["iat"], claims["nbf"], "{file}");
        let seconds = |claim: &str| claims[claim].as_u64().ok_or("not a number");
        let not_before = Timestamp::from_millis(seconds("nbf")? * 1_000);
        let expires_at = Timestamp::from_millis(seconds("exp")? * 1_000);

        assert_eq!(
            verifier
                .verify(
                    &token,
                    Timestamp::from_millis(not_before.as_millis() - 60_000),
                    &Revocations::new()
                )
                .map(drop),
            Err(TokenError::NotYetValid),
            "{file}"
        );
        for now in [
            not_before,
            Timestamp::from_millis(expires_at.as_millis() - 1),
        ] {
            let verified = verifier.verify(&token, now, &Revocations::new())?;
            assert_eq!(verified.principal.as_str(), principal, "{file}");
            assert_eq!(verified.claims.issued_at, Some(not_before), "{file}");
            assert_eq!(verified.claims.kind, None, "{file}");
            assert_eq!(verified.claims.owner, None, "{file}");
            assert_eq!(verified.claims.parent, None, "{file}");
            assert!(verified.claims.chain.is_empty(), "{file}");
            let granted: Vec<String> = verified
                .claims
                .scopes
                .as_slice()
                .iter()
                .map(ToString::to_string)
                .collect();
            assert_eq!(granted, scopes, "{file}");
            let revoked: Revocations =
                [(Revoked::Token(verified.claims.id.clone()), Timestamp::MAX)]
                    .into_iter()
                    .collect();
            assert_eq!(
                verifier.verify(&token, now, &revoked).map(drop),
                Err(TokenError::Revoked),
                "{file}"
            );
        }
        assert_eq!(
            verifier
                .verify(&token, expires_at, &Revocations::new())
                .map(drop),
            Err(TokenError::Expired),
            "{file}"
        );
    }
    Ok(())
}

/// ADR-0047, point 12.1, and ADR-0043, point 4: the context and the version 1 agent identity
/// 0.7 wrote are read as they are.
#[test]
fn a_context_and_an_agent_identity_from_0_7_still_read() -> TestResult {
    let fixtures = Path::new(FROM_0_7);
    let home = tempfile::tempdir()?;
    let config_dir = home.path().join("config");
    let data_dir = home.path().join("data");
    let contexts = config_dir.join("fleetring");
    let agent = data_dir
        .join("fleetring")
        .join("agents")
        .join("legacy")
        .join("42");
    std::fs::create_dir_all(&contexts)?;
    std::fs::create_dir_all(&agent)?;
    let tls_dir = fixtures.to_str().ok_or("not UTF-8")?.replace('\\', "/");
    std::fs::write(
        contexts.join("contexts.toml"),
        std::fs::read_to_string(fixtures.join("contexts.toml"))?.replace("TLS_DIR", &tls_dir),
    )?;
    for entry in std::fs::read_dir(fixtures.join("agents").join("42"))? {
        let entry = entry?;
        std::fs::copy(entry.path(), agent.join(entry.file_name()))?;
    }
    let record = std::fs::read_to_string(agent.join("identity.toml"))?;
    assert!(record.contains("version = 1\n"), "{record}");
    let run = |args: &[&str]| -> Result<String, Box<dyn Error>> {
        let output = crate::support::fleetring()
            .args(args)
            .env("HOME", home.path())
            .env("USERPROFILE", home.path())
            .env("XDG_CONFIG_HOME", &config_dir)
            .env("APPDATA", &config_dir)
            .env("XDG_DATA_HOME", &data_dir)
            .env("LOCALAPPDATA", &data_dir)
            .output()?;
        let stdout = String::from_utf8(output.stdout)?;
        assert!(
            output.status.success(),
            "{args:?}: {stdout}{}",
            String::from_utf8_lossy(&output.stderr)
        );
        Ok(stdout)
    };
    let listed = run(&["context", "list"])?;
    assert!(
        listed.contains("*        legacy  legacy  127.0.0.1:7400  tester"),
        "{listed}"
    );
    let shown = run(&["agent", "show", "42"])?;
    for line in [
        "principal     agent-42",
        "fleet         legacy",
        "scopes        admin:agent-42-*",
        "cert expires  2126-10-21T02:11:47Z",
    ] {
        assert!(shown.contains(line), "{line}: {shown}");
    }
    let listed = run(&["agent", "list"])?;
    assert!(listed.contains("42  agent-42"), "{listed}");
    // Reading it changed nothing: the record is still the one 0.7 wrote.
    assert_eq!(
        std::fs::read_to_string(agent.join("identity.toml"))?,
        record
    );
    Ok(())
}

/// Starts a node on the certificates in `fixtures` and asks it for the fleet's status as the
/// principal `tester`, on that principal's certificate from the same directory.
async fn node_and_principal_certificates_connect(fixtures: &Path) -> TestResult {
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
        crate::support::fleetring()
            .args(["cert", "init", flag, "home", "--dir"])
            .arg(dir.path())
            .output()
    };
    let created = run("--fleet")?;
    assert!(created.status.success(), "{created:?}");
    assert!(created.stderr.is_empty(), "{created:?}");
    assert!(String::from_utf8(created.stdout)?.contains("for fleet home"));
    assert_eq!(Authority::load(dir.path())?.fleet().as_str(), "home");

    let node = crate::support::fleetring()
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

/// The issuer certificate in `path`, checked as a control node checks it at load.
fn issuer_in(path: &Path, tls: &Tls) -> Result<IssuerCertificate, Box<dyn Error>> {
    let der = CertificateDer::from_pem_file(path)?;
    let trusted = [tls.authority().token_signer()?.verifying_key()];
    Ok(IssuerCertificate::check(
        der.as_ref(),
        tls.authority().fleet(),
        &trusted,
    )?)
}

#[test]
fn cert_issuer_writes_0600_key_and_refuses_over_90_days() -> TestResult {
    let root = tempfile::tempdir()?;
    let tls = Tls::create(root.path())?;
    let issuer = |extra: &[&str]| {
        crate::support::fleetring()
            .args(["cert", "issuer", "--name", "control-1", "--dir"])
            .arg(tls.dir())
            .args(extra)
            .output()
    };
    let written = issuer(&[])?;
    assert!(written.status.success(), "{written:?}");
    let stdout = String::from_utf8(written.stdout)?;
    let cert = tls.dir().join("issuer.control-1.crt");
    let key = tls.dir().join("issuer.control-1.key");
    assert!(
        stdout.contains("issued fleetring://test/issuer/control-1"),
        "{stdout}"
    );
    assert!(stdout.contains("[control]"), "{stdout}");
    assert!(
        stdout.contains(&format!("issuer_cert = \"{}\"", cert.display())),
        "{stdout}"
    );
    assert!(
        stdout.contains(&format!("issuer_key = \"{}\"", key.display())),
        "{stdout}"
    );
    assert!(!stdout.contains("PRIVATE KEY"), "the key is never printed");
    let checked = issuer_in(&cert, &tls)?;
    assert_eq!(checked.name().as_str(), "control-1");
    assert_eq!(
        checked.not_after().as_millis() - checked.not_before().as_millis(),
        30 * 24 * 60 * 60 * 1_000,
        "30 days by default"
    );
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        assert_eq!(std::fs::metadata(&key)?.permissions().mode() & 0o777, 0o600);
    }

    // Running it again renews in place: a new key and certificate over the old pair, up to 90
    // days, the key still 0600 (ADR-0047, point 1.1).
    let again = issuer(&["--ttl", "90d"])?;
    assert!(again.status.success(), "{again:?}");
    let renewed = issuer_in(&cert, &tls)?;
    assert_ne!(renewed.key_id(), checked.key_id(), "a new key");
    assert_eq!(
        renewed.not_after().as_millis() - renewed.not_before().as_millis(),
        u64::try_from(MAX_ISSUER_LIFETIME.as_millis())?
    );
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        assert_eq!(std::fs::metadata(&key)?.permissions().mode() & 0o777, 0o600);
    }
    let leftovers: Vec<_> = std::fs::read_dir(tls.dir())?
        .filter_map(Result::ok)
        .filter(|entry| entry.file_name().to_string_lossy().ends_with(".tmp"))
        .collect();
    assert!(leftovers.is_empty(), "{leftovers:?}");
    let out = root.path().join("out");
    let longest = crate::support::fleetring()
        .args(["cert", "issuer", "--name", "c2", "--ttl", "90d", "--dir"])
        .arg(tls.dir())
        .arg("--out")
        .arg(&out)
        .output()?;
    assert!(longest.status.success(), "{longest:?}");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        assert_eq!(std::fs::metadata(&out)?.permissions().mode() & 0o777, 0o700);
    }
    let checked = issuer_in(&out.join("issuer.c2.crt"), &tls)?;
    assert_eq!(
        checked.not_after().as_millis() - checked.not_before().as_millis(),
        u64::try_from(MAX_ISSUER_LIFETIME.as_millis())?
    );
    for ttl in ["91d", "2161h"] {
        let refused = crate::support::fleetring()
            .args(["cert", "issuer", "--name", "c3", "--ttl", ttl, "--dir"])
            .arg(tls.dir())
            .output()?;
        assert!(!refused.status.success(), "{ttl}: {refused:?}");
        let stderr = String::from_utf8(refused.stderr)?;
        assert!(stderr.contains("at most 90 days"), "{ttl}: {stderr}");
        assert!(!tls.dir().join("issuer.c3.key").exists(), "{ttl}");
    }
    Ok(())
}

#[tokio::test]
async fn control_node_with_mismatched_issuer_pair_refuses_to_start_naming_both_files() -> TestResult
{
    let dir = tempfile::tempdir()?;
    let tls = Tls::create(dir.path())?;
    let validity = Validity::new(SystemClock.now(), MAX_ISSUER_LIFETIME);
    let name = "control".parse()?;
    tls.authority()
        .issue_issuer(&name, validity)?
        .write(tls.dir(), "issuer.control")?;
    let cert = tls.dir().join("issuer.control.crt");
    let key = tls.dir().join("issuer.control.key");
    let issuer_table = format!("\n[control]\nissuer_cert = {cert:?}\nissuer_key = {key:?}\n");
    let table = tls.node_tls_table("control")?;
    let text = format!(
        "{}{issuer_table}",
        config(dir.path(), "control", "control", None, &table)
    );

    // A matching pair starts, and the daemon holds it.
    let daemon = Daemon::start(Config::from_toml(&text)?).await?;
    let loaded = daemon.issuer().ok_or("no issuer pair")?.certificate();
    assert_eq!(loaded.key_id(), issuer_in(&cert, &tls)?.key_id());
    daemon.run_until(async {}).await;

    // Another pair's key: refused before anything starts, naming both files.
    let other = tls.authority().issue_issuer(&name, validity)?;
    replace_file(&key, other.key_pem())?;
    let refused = Daemon::start(Config::from_toml(&text)?)
        .await
        .expect_err("a mismatched pair");
    assert!(matches!(refused, StartError::Issuer(_)), "{refused}");
    let mut chain = refused.to_string();
    let mut source = refused.source();
    while let Some(cause) = source {
        chain.push_str(": ");
        chain.push_str(&cause.to_string());
        source = cause.source();
    }
    assert!(chain.contains(&cert.display().to_string()), "{chain}");
    assert!(chain.contains(&key.display().to_string()), "{chain}");
    assert!(
        chain.contains("the key doesn't match the certificate"),
        "{chain}"
    );
    assert!(!chain.contains("PRIVATE KEY"), "{chain}");

    // A certificate from another authority is refused the same way.
    let stranger = Authority::create("test".parse()?, Tls::validity())?;
    let foreign = stranger.issue_issuer(&name, validity)?;
    replace_file(&cert, foreign.certificate_pem())?;
    replace_file(&key, foreign.key_pem())?;
    let refused = Daemon::start(Config::from_toml(&text)?)
        .await
        .expect_err("another authority's issuer");
    let cause = refused
        .source()
        .map(ToString::to_string)
        .unwrap_or_default();
    assert!(cause.contains("issuer.control.crt"), "{cause}");
    Ok(())
}

/// How a connection opened by hand with a chosen client certificate ended.
#[derive(Debug)]
enum Outcome {
    /// The server answered the request.
    Served,
    /// The server closed the connection after the handshake without a TLS alert: what an
    /// accepting listener does when `peer_of` refuses the certificate.
    Closed,
    /// The server sent a TLS alert, as rustls does when its verifier refuses a certificate.
    Alert(
        #[expect(
            dead_code,
            reason = "read through Debug, in a failed assertion's message"
        )]
        String,
    ),
}

/// The HTTP/2 client preface followed by an empty SETTINGS frame: enough for a gRPC listener to
/// answer with its own SETTINGS frame.
const H2_PREFACE: &[u8] = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n\x00\x00\x00\x04\x00\x00\x00\x00\x00";

/// A request the admin listener answers whoever sends it.
const ADMIN_REQUEST: &[u8] = b"GET /healthz HTTP/1.1\r\nhost: node\r\nconnection: close\r\n\r\n";

/// A rustls client that trusts `authority_pem`, speaks `alpn`, and presents `client`'s
/// certificate, or none. It is built by hand because an [`Identity`] refuses an issuer
/// certificate before any connection is made.
fn hand_built_client(
    authority_pem: &str,
    client: Option<&Issued>,
    alpn: &[u8],
) -> Result<Arc<rustls::ClientConfig>, Box<dyn Error>> {
    let mut roots = rustls::RootCertStore::empty();
    roots.add(CertificateDer::from_pem_slice(authority_pem.as_bytes())?)?;
    let builder = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_protocol_versions(&[&rustls::version::TLS13])?
    .with_root_certificates(roots);
    let mut config = match client {
        Some(issued) => builder.with_client_auth_cert(
            vec![CertificateDer::from_pem_slice(
                issued.certificate_pem().as_bytes(),
            )?],
            PrivateKeyDer::from_pem_slice(issued.key_pem().as_bytes())?,
        )?,
        None => builder.with_no_client_auth(),
    };
    config.alpn_protocols = vec![alpn.to_vec()];
    Ok(Arc::new(config))
}

/// Connects to `address` with `config`, sends `request`, and reads what comes back.
async fn dial(
    address: SocketAddr,
    config: Arc<rustls::ClientConfig>,
    request: &[u8],
) -> Result<Outcome, Box<dyn Error>> {
    let tcp = tokio::net::TcpStream::connect(address).await?;
    let name = rustls_pki_types::ServerName::IpAddress(address.ip().into());
    let mut stream = match tokio_rustls::TlsConnector::from(config)
        .connect(name, tcp)
        .await
    {
        Ok(stream) => stream,
        Err(error) => return Ok(Outcome::Alert(error.to_string())),
    };
    // The write can race the server's close; the read says how the connection ended.
    let _written = stream.write_all(request).await;
    let mut answer = [0_u8; 64];
    match tokio::time::timeout(HANG_GUARD, stream.read(&mut answer)).await? {
        Ok(0) => Ok(Outcome::Closed),
        Ok(_) => Ok(Outcome::Served),
        Err(error) => match error
            .get_ref()
            .and_then(|inner| inner.downcast_ref::<rustls::Error>())
        {
            Some(tls) => Ok(Outcome::Alert(tls.to_string())),
            None => Ok(Outcome::Closed),
        },
    }
}

/// ADR-0047 point 1.5: an issuer certificate never authenticates a TLS peer, on any listener
/// and as either end, although it chains to the fleet's authority and has no extended key usage
/// (which rustls-webpki reads as any use).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn issuer_certificate_cannot_open_a_connection() -> TestResult {
    let dir = tempfile::tempdir()?;
    let tls = Tls::create(dir.path())?;
    let authority_pem = tls.authority().certificate_pem();
    // Valid by the wall clock, which rustls checks: from a day ago, for the longest allowed.
    let start = SystemClock
        .now()
        .as_millis()
        .checked_sub(24 * 60 * 60 * 1_000)
        .ok_or("the clock is before 1970")?;
    let issuer = tls.authority().issue_issuer(
        &"control".parse()?,
        Validity::new(Timestamp::from_millis(start), MAX_ISSUER_LIFETIME),
    )?;

    // It can't be a node's or a client's identity, so no daemon or client presents it.
    let refused = Identity::from_issued(authority_pem, &issuer).expect_err("not an identity");
    assert!(matches!(refused, IdentityError::Issuer), "{refused}");
    issuer.write(tls.dir(), "issuer.control")?;
    let as_node = Daemon::start(Config::from_toml(&config(
        dir.path(),
        "control",
        "both",
        None,
        &tls.tls_table("issuer.control"),
    ))?)
    .await
    .expect_err("a node can't present an issuer certificate");
    assert!(
        as_node.to_string().contains("delegation issuer")
            || std::error::Error::source(&as_node)
                .is_some_and(|source| source.to_string().contains("delegation issuer")),
        "{as_node}"
    );

    // The admin listener serves HTTPS with the node's certificate, asking for a client
    // certificate without requiring one.
    let table = tls.node_tls_table("solo")?;
    let text = config(dir.path(), "solo", "both", None, &table)
        .replace(ADMIN, "\n[admin]\nlisten = \"127.0.0.1:0\"\ntls = true\n");
    let daemon = Daemon::start(Config::from_toml(&text)?).await?;
    let listen = daemon.listen_address();
    let admin = daemon.admin_address().ok_or("no admin listener")?;
    let tester = tls
        .authority()
        .issue_client(&"tester".parse()?, Tls::validity())?;
    let peer = tls.issue_node("peer")?;

    // The gRPC listener, which clients and other nodes dial alike: a principal's and a node's
    // certificates are answered, an issuer's is accepted by the TLS verifier and then refused
    // by its subject kind, so the connection closes without an alert.
    for (client, what) in [
        (&tester, "a client certificate"),
        (&peer, "a node certificate"),
    ] {
        let config = hand_built_client(authority_pem, Some(client), b"h2")?;
        let outcome = dial(listen, config, H2_PREFACE).await?;
        assert!(matches!(outcome, Outcome::Served), "{what}: {outcome:?}");
    }
    let config = hand_built_client(authority_pem, Some(&issuer), b"h2")?;
    let outcome = dial(listen, config, H2_PREFACE).await?;
    assert!(
        matches!(outcome, Outcome::Closed),
        "an issuer certificate on the gRPC listener: {outcome:?}"
    );

    // The admin listener, which allows no certificate at all, refuses an issuer's.
    for client in [None, Some(&tester)] {
        let config = hand_built_client(authority_pem, client, b"http/1.1")?;
        let outcome = dial(admin, config, ADMIN_REQUEST).await?;
        assert!(
            matches!(outcome, Outcome::Served),
            "{client:?}: {outcome:?}"
        );
    }
    let config = hand_built_client(authority_pem, Some(&issuer), b"http/1.1")?;
    let outcome = dial(admin, config, ADMIN_REQUEST).await?;
    assert!(
        matches!(outcome, Outcome::Closed),
        "an issuer certificate on the admin listener: {outcome:?}"
    );
    daemon.run_until(async {}).await;

    // As a server certificate, dialed by a node's client configuration. rustls-webpki would
    // accept it for server authentication; what refuses it is the server-name check, since its
    // only subject alternative name is a URI, which matches no IP address or DNS name. Each
    // name a node could dial is tried, the issuer's own name included.
    let node = Security::Tls(tls.node_identity("dialer")?);
    let issuer_server = serve_tls(&issuer).await?;
    for name in ["127.0.0.1", "localhost", "control"] {
        let tcp = tokio::net::TcpStream::connect(issuer_server).await?;
        let dialed = format!("{name}:{}", issuer_server.port());
        let refused = node
            .connect(&dialed, Raw::Tcp(tcp))
            .await
            .expect_err("an issuer certificate can't serve");
        let cause = refused
            .get_ref()
            .and_then(|inner| inner.downcast_ref::<rustls::Error>());
        assert!(
            matches!(
                cause,
                Some(rustls::Error::InvalidCertificate(
                    rustls::CertificateError::NotValidForName
                        | rustls::CertificateError::NotValidForNameContext { .. }
                ))
            ),
            "dialing {dialed}: {refused}"
        );
    }
    // The same dial reaches a node certificate for the address.
    let node_server = serve_tls(&tls.issue_node("server")?).await?;
    let tcp = tokio::net::TcpStream::connect(node_server).await?;
    node.connect(&node_server.to_string(), Raw::Tcp(tcp))
        .await?;
    Ok(())
}

/// Serves TLS with `issued`'s certificate on a new local port, finishing handshakes and nothing
/// more, and returns the port's address.
async fn serve_tls(issued: &Issued) -> Result<SocketAddr, Box<dyn Error>> {
    let config = rustls::ServerConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_protocol_versions(&[&rustls::version::TLS13])?
    .with_no_client_auth()
    .with_single_cert(
        vec![CertificateDer::from_pem_slice(
            issued.certificate_pem().as_bytes(),
        )?],
        PrivateKeyDer::from_pem_slice(issued.key_pem().as_bytes())?,
    )?;
    let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(config));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    tokio::spawn(async move {
        while let Ok((tcp, _)) = listener.accept().await {
            let acceptor = acceptor.clone();
            tokio::spawn(async move {
                // A refused handshake is the point; the stream is dropped either way.
                let _handshake = acceptor.accept(tcp).await;
            });
        }
    });
    Ok(address)
}
