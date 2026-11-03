//! FR-047: principals and scopes, enforced at every RPC, through client certificates and
//! bearer tokens, over real TCP with mutual TLS.
//!
//! One control daemon and one data-only daemon. The test principal `tester` administers;
//! `reader` may read `tenant:acme/*` with its certificate alone; `app` has no scopes of its own
//! and acts through tokens; `router` forwards its callers' tokens.

use std::error::Error;
use std::net::SocketAddr;
use std::path::Path;
use std::time::Duration;

use fleetring::ReplicationFactor;
use fleetring_control::{Acks, DatabaseName, Millis};
use fleetring_proto::v1::revocation::Revoked;
use fleetring_proto::v1::{CreateRequest, Revocation, RevokeTokenRequest, TransferLeaseRequest};
use fleetringd::auth::{BearerToken, Grant, TokenSigner};
use fleetringd::clock::{Clock as _, SystemClock};
use fleetringd::data::{Client, DatabaseSpec, LeaseUpdate};
use fleetringd::engine::{Statement, Value};
use fleetringd::transport::{Credentials, database_client_with};
use fleetringd::{Config, Daemon, Network};
use tonic::Code;

use crate::support::Tls;

mod support;

type TestResult = Result<(), Box<dyn Error>>;

/// Guards against a hang only.
const HANG_GUARD: Duration = Duration::from_secs(60);

/// How often the data daemon sends heartbeats, which bounds how long a revocation takes to reach
/// it.
const HEARTBEAT_MS: u64 = 100;

/// The control plane's heartbeat timeout, which is how old the data daemon's denylist may get
/// before it refuses tokens (ADR-0026): fifteen heartbeats, so that a heartbeat or two delayed on
/// a loaded runner never trips it, and short enough that the test waits for it to.
const HEARTBEAT_TIMEOUT_MS: u64 = 1_500;

/// The principals every daemon in this test knows, besides `tester`.
const PRINCIPALS: &str = "\n[[principal]]\nname = \"reader\"\nscopes = [\"read:tenant:acme/*\"]\n\
     \n[[principal]]\nname = \"app\"\n\
     \n[[principal]]\nname = \"router\"\nforward_tokens = true\n\
     \n[[principal]]\nname = \"streamer\"\nscopes = [\"stream:tenant:acme/*\"]\n\
     \n[[principal]]\nname = \"writer\"\nscopes = [\"write:tenant:acme/*\"]\n";

fn config(
    dir: &Path,
    tls: &Tls,
    id: &str,
    role: &str,
    control: Option<SocketAddr>,
) -> Result<String, Box<dyn Error>> {
    let address = control.map_or_else(String::new, |address| format!("address = \"{address}\"\n"));
    let control = format!("\n[control]\n{address}heartbeat_timeout_ms = {HEARTBEAT_TIMEOUT_MS}\n");
    // Data nodes feed every database to a file sink, so node status has sinks to filter.
    let sink = if role == "data" {
        format!(
            "\n[[changes.sink]]\nname = \"archive\"\nkind = \"file\"\ndatabases = [\"*\"]\npath = {:?}\n",
            dir.join(format!("{id}-sink"))
        )
    } else {
        String::new()
    };
    Ok(format!(
        "data_dir = {:?}\nlisten = \"127.0.0.1:0\"\nrole = \"{role}\"\n\n[node]\nid = \"{id}\"\n\
         \n[membership]\nheartbeat_interval_ms = {HEARTBEAT_MS}\n{}{control}{PRINCIPALS}{sink}",
        dir.join(id),
        tls.node_table(id)?,
    ))
}

fn network(tls: &Tls, principal: &str) -> Result<Network, Box<dyn Error>> {
    tls.client_network(principal)
}

fn with_token(network: Network, token: &BearerToken) -> Result<Network, Box<dyn Error>> {
    Ok(network.with_credentials(Credentials::bearer(token)?))
}

fn token(
    signer: &TokenSigner,
    principal: &str,
    scopes: &[&str],
    id: &str,
    lifetime: Duration,
) -> Result<BearerToken, Box<dyn Error>> {
    let now = SystemClock.now();
    let lifetime = Millis::new(u64::try_from(lifetime.as_millis())?);
    Ok(signer.issue(&Grant {
        principal: principal.parse()?,
        scopes: scopes
            .iter()
            .map(|scope| scope.parse())
            .collect::<Result<_, _>>()?,
        id: id.parse()?,
        not_before: now,
        expires_at: now.saturating_add(lifetime),
    })?)
}

fn read_one() -> Statement {
    Statement::new("SELECT count(*) FROM t")
}

fn write_one() -> Statement {
    Statement::with_params("INSERT INTO t VALUES (?)", [Value::Integer(1)])
}

/// Checks what a principal limited to `read:tenant:acme/*` may do through `client`.
async fn reads_acme_and_nothing_else(client: &Client, who: &str) -> TestResult {
    let acme: DatabaseName = "tenant:acme/orders".parse()?;
    let zed: DatabaseName = "tenant:zed/orders".parse()?;

    let rows = client.query(&acme, read_one()).await?;
    assert_eq!(rows.rows.len(), 1, "{who} reads its database");
    client.locate(&acme).await?;
    let listed: Vec<String> = client
        .list_databases()
        .await?
        .iter()
        .map(|location| location.database.to_string())
        .collect();
    assert_eq!(
        listed,
        ["tenant:acme/orders"],
        "{who} lists what it may read"
    );

    let denied = |status: tonic::Status, what: &str| {
        assert_eq!(
            status.code(),
            Code::PermissionDenied,
            "{who} {what}: {status}"
        );
        status.message().to_owned()
    };
    let write = denied(
        client.execute(&acme, write_one()).await.expect_err("write"),
        "write",
    );
    assert_eq!(write, "Execute needs write on 'tenant:acme/orders'");
    denied(
        client
            .query(&zed, read_one())
            .await
            .expect_err("other prefix"),
        "read another prefix",
    );
    denied(
        client.locate(&zed).await.expect_err("other prefix"),
        "locate another prefix",
    );
    let created = denied(
        client
            .create_database(&"tenant:acme/new".parse()?, ReplicationFactor::ONE)
            .await
            .expect_err("create"),
        "create",
    );
    assert!(created.contains("admin on 'tenant:acme/new'"), "{created}");
    denied(client.drop_database(&acme).await.expect_err("drop"), "drop");
    denied(
        client
            .change_durability(&acme, Acks::All, None)
            .await
            .expect_err("durability"),
        "change durability",
    );
    let drain = denied(
        client
            .drain_node(&"data-1".parse()?)
            .await
            .expect_err("drain"),
        "drain",
    );
    assert_eq!(drain, "Drain needs the fleet scope");
    denied(
        client.status().await.expect_err("membership"),
        "watch membership",
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scopes_hold_through_certificates_and_tokens() -> TestResult {
    let dir = tempfile::tempdir()?;
    let tls = Tls::create(dir.path())?;
    let control = Daemon::start(Config::from_toml(&config(
        dir.path(),
        &tls,
        "control",
        "control",
        None,
    )?)?)
    .await?;
    let address = control.control_address().ok_or("no control address")?;
    let data = Daemon::start(Config::from_toml(&config(
        dir.path(),
        &tls,
        "data-1",
        "data",
        Some(address),
    )?)?)
    .await?;
    let control_target = address.to_string();
    let data_target = data.listen_address().to_string();

    // The operator creates two tenants' databases.
    let admin = Client::new(control_target.clone(), network(&tls, "tester")?);
    tokio::time::timeout(HANG_GUARD, async {
        loop {
            match admin.status().await {
                Ok(view) if view.members().len() == 1 => return Ok::<_, tonic::Status>(()),
                _ => tokio::time::sleep(Duration::from_millis(50)).await,
            }
        }
    })
    .await??;
    for database in ["tenant:acme/orders", "tenant:zed/orders"] {
        let database: DatabaseName = database.parse()?;
        admin
            .create_database_with(&database, DatabaseSpec::new(ReplicationFactor::ONE))
            .await?;
        admin
            .execute(&database, Statement::new("CREATE TABLE t (v INTEGER)"))
            .await?;
        admin.execute(&database, write_one()).await?;
    }

    // A client certificate whose principal is limited to one tenant.
    let reader = Client::new(control_target.clone(), network(&tls, "reader")?);
    reads_acme_and_nothing_else(&reader, "reader's certificate").await?;

    // The same limit carried by a token: `app` has no scopes of its own.
    let signer = tls.authority().token_signer()?;
    let app_cert_only = Client::new(control_target.clone(), network(&tls, "app")?);
    let refused = app_cert_only
        .query(&"tenant:acme/orders".parse()?, read_one())
        .await
        .expect_err("a certificate without scopes does nothing");
    assert_eq!(refused.code(), Code::PermissionDenied);
    let app_token = token(
        &signer,
        "app",
        &["read:tenant:acme/*"],
        "app-1",
        Duration::from_secs(3600),
    )?;
    let app = Client::new(
        control_target.clone(),
        with_token(network(&tls, "app")?, &app_token)?,
    );
    reads_acme_and_nothing_else(&app, "app's token").await?;

    // A token naming another principal than the certificate is refused...
    let alice = token(
        &signer,
        "alice",
        &["read:tenant:acme/*"],
        "alice-1",
        Duration::from_secs(3600),
    )?;
    let stolen = Client::new(
        control_target.clone(),
        with_token(network(&tls, "app")?, &alice)?,
    );
    let refused = stolen
        .query(&"tenant:acme/orders".parse()?, read_one())
        .await
        .expect_err("another principal's token");
    assert_eq!(refused.code(), Code::PermissionDenied, "{refused}");
    // ...unless the certificate's principal forwards tokens, as a router does: the request then
    // acts as the token's principal, with its scopes alone.
    let routed = Client::new(
        control_target.clone(),
        with_token(network(&tls, "router")?, &alice)?,
    );
    reads_acme_and_nothing_else(&routed, "a forwarded token").await?;
    // Without a token, the router has no scopes of its own.
    let router_alone = Client::new(control_target.clone(), network(&tls, "router")?);
    let status = router_alone
        .list_databases()
        .await
        .expect_err("the router alone reads nothing");
    assert_eq!(status.code(), Code::PermissionDenied, "{status}");

    // Expired and tampered tokens are unauthenticated.
    let expired = token(
        &signer,
        "app",
        &["read:tenant:acme/*"],
        "app-2",
        Duration::from_secs(1),
    )?;
    tokio::time::sleep(Duration::from_millis(2_100)).await;
    let late = Client::new(
        control_target.clone(),
        with_token(network(&tls, "app")?, &expired)?,
    );
    let status = late
        .query(&"tenant:acme/orders".parse()?, read_one())
        .await
        .expect_err("expired");
    assert_eq!(status.code(), Code::Unauthenticated, "{status}");
    assert_eq!(status.message(), "the token has expired");
    let tampered = BearerToken::new(format!("{}A", app_token.expose()));
    let forged = Client::new(
        control_target.clone(),
        with_token(network(&tls, "app")?, &tampered)?,
    );
    let status = forged
        .query(&"tenant:acme/orders".parse()?, read_one())
        .await
        .expect_err("tampered");
    assert_eq!(status.code(), Code::Unauthenticated, "{status}");

    data_node_calls_are_scoped(&tls, &control_target, &data_target).await?;
    statements_stay_in_their_database(&tls, &control_target, &dir.path().join("data-1")).await?;

    // An open stream ends once its token is revoked.
    let mut leases = app.watch_leases(&data_target).await?;
    assert!(matches!(
        leases.next().await?,
        Some(LeaseUpdate::Held { .. })
    ));

    // A revoked token is refused by the control node at once, and by the data-only node within
    // one heartbeat interval. Verification needs neither: before the revocation the data node
    // answers with no call to the control plane.
    app.node_status(&data_target).await?;
    admin
        .control()
        .revoke_token(RevokeTokenRequest {
            revocation: Some(Revocation {
                revoked: Some(Revoked::TokenId("app-1".into())),
                until_ms: SystemClock
                    .now()
                    .saturating_add(Millis::new(3_600_000))
                    .as_millis(),
            }),
        })
        .await?;
    let status = app
        .locate(&"tenant:acme/orders".parse()?)
        .await
        .expect_err("revoked at the control node");
    assert_eq!(status.message(), "the token is revoked");
    let revoked_at = tokio::time::Instant::now();
    tokio::time::timeout(HANG_GUARD, async {
        while app.node_status(&data_target).await.is_ok() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await?;
    let waited = revoked_at.elapsed();
    let status = app
        .node_status(&data_target)
        .await
        .expect_err("revoked at the data node");
    assert_eq!(status.code(), Code::Unauthenticated, "{status}");
    // The revocation arrives with a heartbeat, so well before the list could go stale.
    assert!(
        waited < Duration::from_millis(HEARTBEAT_TIMEOUT_MS),
        "the data node refused the token after {waited:?}"
    );
    // The key's other tokens still work.
    reads_acme_and_nothing_else(&routed, "a token that wasn't revoked").await?;
    let ended = tokio::time::timeout(HANG_GUARD, async {
        loop {
            match leases.next().await {
                Ok(Some(_)) => {}
                Ok(None) => return Err::<tonic::Status, _>("the stream ended without a refusal"),
                Err(status) => return Ok(status),
            }
        }
    })
    .await??;
    assert_eq!(ended.code(), Code::Unauthenticated, "{ended}");
    assert_eq!(ended.message(), "the token is revoked");

    // Cut off from the control plane, the data node stops trusting tokens once its denylist is
    // as old as the control plane's heartbeat timeout, and keeps serving certificates.
    let reader = Client::new(control_target.clone(), network(&tls, "reader")?);
    control.run_until(async {}).await;
    let stale = tokio::time::timeout(HANG_GUARD, async {
        loop {
            match routed.node_status(&data_target).await {
                Err(status) if status.code() == Code::Unavailable => return status,
                _ => tokio::time::sleep(Duration::from_millis(20)).await,
            }
        }
    })
    .await?;
    assert_eq!(
        stale.message(),
        "this node's token denylist is stale; retry"
    );
    reader.node_status(&data_target).await?;

    // A node that starts and can't reach the control plane has no denylist, so it trusts no
    // token until its first join or heartbeat reply arrives.
    let late = Daemon::start(Config::from_toml(&config(
        dir.path(),
        &tls,
        "data-2",
        "data",
        Some(address),
    )?)?)
    .await?;
    let late_target = late.listen_address().to_string();
    let status = routed
        .node_status(&late_target)
        .await
        .expect_err("no denylist yet");
    assert_eq!(status.code(), Code::Unavailable, "{status}");
    assert_eq!(
        status.message(),
        "this node hasn't received the token denylist yet; retry"
    );
    reader.node_status(&late_target).await?;

    late.run_until(async {}).await;
    data.run_until(async {}).await;
    Ok(())
}

/// Every file under `dir`, recursively.
fn files_under(dir: &Path) -> Vec<std::path::PathBuf> {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return Vec::new();
    };
    entries
        .filter_map(Result::ok)
        .flat_map(|entry| {
            let path = entry.path();
            if path.is_dir() {
                files_under(&path)
            } else {
                vec![path]
            }
        })
        .collect()
}

/// A principal that may read or write `tenant:acme/*` can't reach another tenant's database
/// file from a statement: the engine confines every statement to the database it names, which
/// the scope model depends on (ADR-0026). `ATTACH` through `Query` or `Execute`, and
/// `VACUUM INTO`, are refused before they run.
async fn statements_stay_in_their_database(
    tls: &Tls,
    control: &str,
    data_dir: &Path,
) -> TestResult {
    let acme: DatabaseName = "tenant:acme/orders".parse()?;
    let zed: DatabaseName = "tenant:zed/orders".parse()?;
    // Another tenant's database file on the same node, as an attacker would guess it.
    let files = files_under(data_dir);
    let target = files
        .iter()
        .find(|path| {
            let text = path.to_string_lossy();
            text.contains("zed") && !text.ends_with("-wal") && !text.ends_with("-shm")
        })
        .or_else(|| files.first())
        .ok_or("no database files on the data node")?
        .to_string_lossy()
        .replace('\'', "''");
    let attach = format!("ATTACH DATABASE '{target}' AS other");
    let reader = Client::new(control.to_owned(), network(tls, "reader")?);
    let writer = Client::new(control.to_owned(), network(tls, "writer")?);
    let refused = |status: tonic::Status, what: &str| {
        assert_eq!(status.code(), Code::InvalidArgument, "{what}: {status}");
    };
    refused(
        reader
            .query(&acme, Statement::new(attach.clone()))
            .await
            .expect_err("attach through query"),
        "attach through query",
    );
    refused(
        writer
            .execute(&acme, Statement::new(attach.clone()))
            .await
            .expect_err("attach through execute"),
        "attach through execute",
    );
    refused(
        writer
            .execute(&acme, Statement::new(format!("VACUUM INTO '{target}'")))
            .await
            .expect_err("vacuum into another file"),
        "vacuum into",
    );
    refused(
        writer
            .execute(&acme, Statement::new("DETACH DATABASE main"))
            .await
            .expect_err("detach"),
        "detach",
    );
    // The writer still writes its own tenant, and the other tenant is untouched.
    writer.execute(&acme, write_one()).await?;
    let admin = Client::new(control.to_owned(), network(tls, "tester")?);
    let rows = admin.query(&zed, read_one()).await?;
    assert_eq!(
        rows.rows,
        [vec![Value::Integer(1)]],
        "tenant:zed/orders changed"
    );
    Ok(())
}

/// What a principal limited to `tenant:acme/*`, and one that may only stream it, may ask the
/// data node directly.
async fn data_node_calls_are_scoped(tls: &Tls, control: &str, data: &str) -> TestResult {
    let acme: DatabaseName = "tenant:acme/orders".parse()?;
    let zed: DatabaseName = "tenant:zed/orders".parse()?;
    let admin = Client::new(control.to_owned(), network(tls, "tester")?);
    let reader = Client::new(control.to_owned(), network(tls, "reader")?);

    // Leases: the held leases list only what the caller may read.
    let held = |client: Client| async move {
        let mut updates = client.watch_leases(data).await?;
        match updates.next().await? {
            Some(LeaseUpdate::Held { leases, .. }) => {
                Ok::<_, Box<dyn Error>>(leases.keys().map(ToString::to_string).collect::<Vec<_>>())
            }
            other => Err(format!("expected a snapshot, got {other:?}").into()),
        }
    };
    assert_eq!(
        held(admin.clone()).await?,
        ["tenant:acme/orders", "tenant:zed/orders"]
    );
    assert_eq!(held(reader.clone()).await?, ["tenant:acme/orders"]);

    // Node status: databases by read; the reader sees no sink, which needs stream.
    let status = reader.node_status(data).await?;
    let served: Vec<String> = status
        .databases
        .iter()
        .map(|served| served.database.to_string())
        .collect();
    assert_eq!(served, ["tenant:acme/orders"]);
    assert!(status.sinks.is_empty(), "{:?}", status.sinks);

    // A principal that may only stream sees its sinks' progress and nothing else.
    let streamer = Client::new(control.to_owned(), network(tls, "streamer")?);
    let sinks = tokio::time::timeout(HANG_GUARD, async {
        loop {
            let status = streamer.node_status(data).await?;
            assert!(status.databases.is_empty(), "{:?}", status.databases);
            if !status.sinks.is_empty() {
                return Ok::<_, tonic::Status>(status.sinks);
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await??;
    assert!(sinks.iter().all(|sink| sink.database == acme), "{sinks:?}");
    let leases = streamer
        .watch_leases(data)
        .await
        .expect_err("leases need read");
    assert_eq!(leases.code(), Code::PermissionDenied, "{leases}");
    let query = streamer
        .query(&acme, read_one())
        .await
        .expect_err("stream doesn't read");
    assert_eq!(query.code(), Code::PermissionDenied, "{query}");

    // Verify: read on the key.
    reader.verify(data, &acme, None).await?;
    let refused = reader
        .verify(data, &zed, None)
        .await
        .expect_err("another prefix");
    assert_eq!(refused.code(), Code::PermissionDenied, "{refused}");
    assert_eq!(
        refused.message(),
        "Verify needs read on 'tenant:zed/orders'"
    );

    // Handing a lease over and creating on a data node are admin.
    let transfer = reader
        .control()
        .transfer_lease(TransferLeaseRequest {
            database: acme.to_string(),
            target: String::new(),
        })
        .await
        .expect_err("transfer needs admin");
    assert_eq!(
        transfer.message(),
        "TransferLease needs admin on 'tenant:acme/orders'"
    );
    let network = network(tls, "reader")?;
    let mut direct = database_client_with(network.channel(data)?, &Credentials::none());
    let create = direct
        .create(CreateRequest {
            database: "tenant:acme/new".into(),
        })
        .await
        .expect_err("create needs admin");
    assert_eq!(create.code(), Code::PermissionDenied, "{create}");
    assert_eq!(create.message(), "Create needs admin on 'tenant:acme/new'");
    Ok(())
}

/// Runs the `fleetringd` binary with `args`, with `FLEETRING_TLS_DIR` at `tls_dir` and `env` set.
async fn fleetringd(
    tls_dir: &Path,
    env: &[(&str, &str)],
    args: &[&str],
) -> Result<std::process::Output, Box<dyn Error>> {
    let tls_dir = tls_dir.to_owned();
    let env: Vec<(String, String)> = env
        .iter()
        .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
        .collect();
    let args: Vec<String> = args.iter().map(|arg| (*arg).to_owned()).collect();
    Ok(tokio::task::spawn_blocking(move || {
        std::process::Command::new(env!("CARGO_BIN_EXE_fleetringd"))
            .env("FLEETRING_TLS_DIR", &tls_dir)
            .env_remove("FLEETRING_TOKEN")
            .envs(env)
            .args(&args)
            .output()
    })
    .await??)
}

fn succeeded(output: &std::process::Output, what: &str) -> Result<String, Box<dyn Error>> {
    let text = String::from_utf8(output.stdout.clone())?;
    assert!(
        output.status.success(),
        "{what}: exit {}: {text}{}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(text)
}

fn failed_with(output: &std::process::Output, what: &str, needle: &str) {
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(!output.status.success(), "{what} should fail");
    assert!(stderr.contains(needle), "{what}: {stderr}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_command_line_acts_as_its_certificate_or_its_token() -> TestResult {
    let dir = tempfile::tempdir()?;
    let tls = Tls::create(dir.path())?;
    let node = Daemon::start(Config::from_toml(&config(
        dir.path(),
        &tls,
        "node-1",
        "both",
        None,
    )?)?)
    .await?;
    let control = node
        .control_address()
        .ok_or("no control address")?
        .to_string();
    let tls_dir = tls.dir().to_owned();
    let tls_text = tls_dir.to_str().ok_or("a non-UTF-8 path")?;
    let target = ["--control", control.as_str()];

    // The operator, `tester`, creates a database.
    tokio::time::timeout(HANG_GUARD, async {
        loop {
            let output = fleetringd(&tls_dir, &[], &[&["status"][..], &target].concat()).await?;
            if output.status.success() && String::from_utf8_lossy(&output.stdout).contains("node-1")
            {
                return Ok::<_, Box<dyn Error>>(());
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await??;
    for words in [
        &[
            "db",
            "create",
            "tenant:acme/orders",
            "--replication-factor",
            "1",
        ][..],
        &[
            "db",
            "exec",
            "tenant:acme/orders",
            "CREATE TABLE t (v INTEGER)",
        ],
        &[
            "db",
            "exec",
            "tenant:acme/orders",
            "INSERT INTO t VALUES (1)",
        ],
    ] {
        succeeded(
            &fleetringd(&tls_dir, &[], &[words, &target].concat()).await?,
            "operator",
        )?;
    }

    // `reader` acts through its certificate, with the scopes `[[principal]]` gives it.
    succeeded(
        &fleetringd(
            &tls_dir,
            &[],
            &["cert", "client", "reader", "--dir", tls_text],
        )
        .await?,
        "cert client",
    )?;
    let reader_cert = tls_dir.join("client.reader.crt");
    let reader_key = tls_dir.join("client.reader.key");
    let as_reader = [
        "--cert",
        reader_cert.to_str().ok_or("a non-UTF-8 path")?,
        "--key",
        reader_key.to_str().ok_or("a non-UTF-8 path")?,
    ];
    let query = [
        "db",
        "query",
        "tenant:acme/orders",
        "SELECT count(*) FROM t",
    ];
    let write = [
        "db",
        "exec",
        "tenant:acme/orders",
        "INSERT INTO t VALUES (2)",
    ];
    succeeded(
        &fleetringd(&tls_dir, &[], &[&query[..], &target, &as_reader].concat()).await?,
        "reader queries",
    )?;
    failed_with(
        &fleetringd(&tls_dir, &[], &[&write[..], &target, &as_reader].concat()).await?,
        "reader writes",
        "Execute needs write on 'tenant:acme/orders'",
    );

    // `app` has no scopes of its own and acts through a token `token issue` signed.
    succeeded(
        &fleetringd(&tls_dir, &[], &["cert", "client", "app", "--dir", tls_text]).await?,
        "cert client",
    )?;
    let issued = fleetringd(
        &tls_dir,
        &[],
        &[
            "token",
            "issue",
            "--principal",
            "app",
            "--scope",
            "read:tenant:acme/*",
            "--ttl",
            "1h",
            "--dir",
            tls_text,
        ],
    )
    .await?;
    let token = succeeded(&issued, "token issue")?.trim().to_owned();
    let stderr = String::from_utf8_lossy(&issued.stderr);
    let token_id = stderr
        .split_whitespace()
        .nth(2)
        .ok_or("token issue printed no id")?
        .to_owned();
    let app_cert = tls_dir.join("client.app.crt");
    let app_key = tls_dir.join("client.app.key");
    let as_app = [
        "--cert",
        app_cert.to_str().ok_or("a non-UTF-8 path")?,
        "--key",
        app_key.to_str().ok_or("a non-UTF-8 path")?,
    ];
    failed_with(
        &fleetringd(&tls_dir, &[], &[&query[..], &target, &as_app].concat()).await?,
        "app without its token",
        "needs read on 'tenant:acme/orders'",
    );
    let with_token = [("FLEETRING_TOKEN", token.as_str())];
    succeeded(
        &fleetringd(
            &tls_dir,
            &with_token,
            &[&query[..], &target, &as_app].concat(),
        )
        .await?,
        "app queries with its token",
    )?;
    failed_with(
        &fleetringd(
            &tls_dir,
            &with_token,
            &[&write[..], &target, &as_app].concat(),
        )
        .await?,
        "app writes with its token",
        "Execute needs write",
    );

    // The operator revokes it, and the next query is refused.
    succeeded(
        &fleetringd(
            &tls_dir,
            &[],
            &[
                &["token", "revoke", "--token-id", token_id.as_str()][..],
                &target,
            ]
            .concat(),
        )
        .await?,
        "token revoke",
    )?;
    failed_with(
        &fleetringd(
            &tls_dir,
            &with_token,
            &[&query[..], &target, &as_app].concat(),
        )
        .await?,
        "app with a revoked token",
        "the token is revoked",
    );

    node.run_until(async {}).await;
    Ok(())
}
