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
use fleetringd::auth::{BearerToken, TokenSigner, TokenSpec};
use fleetringd::clock::{Clock as _, SystemClock};
use fleetringd::data::{Client, DatabaseSpec, LeaseUpdate};
use fleetringd::engine::{Internal, RowCount, SchemaRequest, Statement, Value};
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
    Ok(signer.issue(&TokenSpec {
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

/// A database's schema needs `read` on it: a principal whose scopes cover another prefix, or
/// only `stream`, or none, is refused, at the data node itself and not only where the client
/// asks the control plane for the primary.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_schema_needs_read_on_the_database() -> TestResult {
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
    let data_target = data.listen_address().to_string();

    let admin = Client::new(address.to_string(), network(&tls, "tester")?);
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
    }
    let acme: DatabaseName = "tenant:acme/orders".parse()?;
    let zed: DatabaseName = "tenant:zed/orders".parse()?;
    let request = SchemaRequest {
        count_limit: 100,
        internal: Internal::Hide,
    };

    // Each principal asks the data node directly, so the refusal is its own.
    let asking = |principal: &str| -> Result<Client, Box<dyn Error>> {
        Ok(Client::new(address.to_string(), network(&tls, principal)?)
            .with_entry(data_target.clone()))
    };
    let tables = |view: fleetringd::data::SchemaView| {
        view.schema
            .objects
            .into_iter()
            .map(|object| object.name)
            .collect::<Vec<_>>()
    };
    assert_eq!(
        tables(asking("tester")?.schema(&acme, request).await?),
        ["t"]
    );
    let reader = asking("reader")?;
    assert_eq!(tables(reader.schema(&acme, request).await?), ["t"]);
    let refused = reader
        .schema(&zed, request)
        .await
        .expect_err("another prefix");
    assert_eq!(refused.code(), Code::PermissionDenied, "{refused}");
    assert_eq!(
        refused.message(),
        "Schema needs read on 'tenant:zed/orders'"
    );
    // `write` implies `read`, `stream` doesn't, and a principal with no scope has nothing.
    assert_eq!(
        tables(asking("writer")?.schema(&acme, request).await?),
        ["t"]
    );
    for principal in ["streamer", "app"] {
        let refused = asking(principal)?
            .schema(&acme, request)
            .await
            .expect_err("without read");
        assert_eq!(refused.code(), Code::PermissionDenied, "{principal}");
        assert_eq!(
            refused.message(),
            "Schema needs read on 'tenant:acme/orders'",
            "{principal}"
        );
    }

    data.run_until(async {}).await;
    control.run_until(async {}).await;
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
        crate::support::fleetring()
            .env("FLEETRING_TLS_DIR", &tls_dir)
            .env_remove("FLEETRING_TOKEN")
            .env_remove("FLEETRING_AS")
            .env_remove("FLEETRING_CA")
            .env_remove("FLEETRING_CERT")
            .env_remove("FLEETRING_KEY")
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

/// FR-055: the identity a command acts as comes from the first of its flag, its environment
/// variable, `--as` (or `FLEETRING_AS`), and the `client.crt` and `client.key` in
/// `FLEETRING_TLS_DIR`, at each step of the precedence (ADR-0043, point 4.4).
///
/// `reader` may read `tenant:acme/*` and `app` may do nothing, so which of them a command acted
/// as shows in whether its query was allowed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_command_line_picks_its_identity_by_flag_variable_or_principal() -> TestResult {
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
    let tls_text = tls_dir.to_str().ok_or("a non-UTF-8 path")?.to_owned();
    let target = ["--control", control.as_str()];

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
    ] {
        succeeded(
            &fleetringd(&tls_dir, &[], &[words, &target].concat()).await?,
            "operator",
        )?;
    }
    for principal in ["reader", "app"] {
        succeeded(
            &fleetringd(
                &tls_dir,
                &[],
                &["cert", "client", principal, "--dir", &tls_text],
            )
            .await?,
            "cert client",
        )?;
    }
    let file = |name: &str| tls_dir.join(name).to_str().map(str::to_owned);
    let reader_cert = file("client.reader.crt").ok_or("a non-UTF-8 path")?;
    let reader_key = file("client.reader.key").ok_or("a non-UTF-8 path")?;
    let app_cert = file("client.app.crt").ok_or("a non-UTF-8 path")?;
    let app_key = file("client.app.key").ok_or("a non-UTF-8 path")?;
    let ca = file("ca.crt").ok_or("a non-UTF-8 path")?;
    let query = [
        "db",
        "query",
        "tenant:acme/orders",
        "SELECT count(*) FROM t",
        "--control",
        control.as_str(),
    ];
    fn with<'a>(query: &[&'a str], extra: &[&'a str]) -> Vec<&'a str> {
        [query, extra].concat()
    }
    let denied = "needs read on 'tenant:acme/orders'";

    // Without any of them, the command acts as `tester`, whose files are the defaults.
    succeeded(
        &fleetringd(&tls_dir, &[], &query).await?,
        "the default identity",
    )?;

    // `--as` and `FLEETRING_AS` pick `client.<principal>.crt` and `.key` from the directory.
    succeeded(
        &fleetringd(&tls_dir, &[], &with(&query, &["--as", "reader"])).await?,
        "--as reader",
    )?;
    failed_with(
        &fleetringd(&tls_dir, &[], &with(&query, &["--as", "app"])).await?,
        "--as app",
        denied,
    );
    succeeded(
        &fleetringd(&tls_dir, &[("FLEETRING_AS", "reader")], &query).await?,
        "FLEETRING_AS=reader",
    )?;
    failed_with(
        &fleetringd(&tls_dir, &[("FLEETRING_AS", "app")], &query).await?,
        "FLEETRING_AS=app",
        denied,
    );

    // The flag beats its environment variable, and `--as` beats `FLEETRING_AS`.
    let as_app = [
        ("FLEETRING_CERT", app_cert.as_str()),
        ("FLEETRING_KEY", app_key.as_str()),
    ];
    succeeded(
        &fleetringd(
            &tls_dir,
            &as_app,
            &with(&query, &["--cert", &reader_cert, "--key", &reader_key]),
        )
        .await?,
        "--cert and --key over FLEETRING_CERT and FLEETRING_KEY",
    )?;
    succeeded(
        &fleetringd(
            &tls_dir,
            &[("FLEETRING_AS", "app")],
            &with(&query, &["--as", "reader"]),
        )
        .await?,
        "--as over FLEETRING_AS",
    )?;

    // A flag outranks every variable (ADR-0043, point 4.4), so `--as` beats FLEETRING_CERT and
    // FLEETRING_KEY. Within the variables' own step, an explicit path beats `FLEETRING_AS`.
    succeeded(
        &fleetringd(&tls_dir, &as_app, &with(&query, &["--as", "reader"])).await?,
        "--as over FLEETRING_CERT and FLEETRING_KEY",
    )?;
    let over_as = [
        ("FLEETRING_CERT", app_cert.as_str()),
        ("FLEETRING_KEY", app_key.as_str()),
        ("FLEETRING_AS", "reader"),
    ];
    failed_with(
        &fleetringd(&tls_dir, &over_as, &query).await?,
        "FLEETRING_CERT and FLEETRING_KEY over FLEETRING_AS",
        denied,
    );

    // FLEETRING_CA, FLEETRING_CERT, and FLEETRING_KEY alone stand in for the three flags, with
    // no usable directory.
    let empty = dir.path().join("empty");
    std::fs::create_dir(&empty)?;
    let all = [
        ("FLEETRING_CA", ca.as_str()),
        ("FLEETRING_CERT", reader_cert.as_str()),
        ("FLEETRING_KEY", reader_key.as_str()),
    ];
    succeeded(
        &fleetringd(&empty, &all, &query).await?,
        "the three variables",
    )?;
    let flags = with(
        &query,
        &["--ca", &ca, "--cert", &reader_cert, "--key", &reader_key],
    );
    succeeded(&fleetringd(&empty, &[], &flags).await?, "the three flags")?;

    // An unknown principal is an error that names the files it looked for, and how to make them.
    let nobody_cert = tls_dir.join("client.nobody.crt");
    let nobody_key = tls_dir.join("client.nobody.key");
    for (what, env, flags) in [
        ("--as", &[][..], &["--as", "nobody"][..]),
        ("FLEETRING_AS", &[("FLEETRING_AS", "nobody")][..], &[][..]),
    ] {
        let output = fleetringd(&tls_dir, env, &with(&query, flags)).await?;
        assert!(!output.status.success(), "{what} nobody should fail");
        let stderr = String::from_utf8_lossy(&output.stderr);
        for needle in [
            "fleetring: no client identity for principal 'nobody'",
            nobody_cert.to_str().ok_or("a non-UTF-8 path")?,
            nobody_key.to_str().ok_or("a non-UTF-8 path")?,
            "hint: create it with `fleetring cert client nobody --dir",
        ] {
            assert!(stderr.contains(needle), "{what}: {needle} not in {stderr}");
        }
    }

    // `--as` without a directory says what it needs.
    let output = tokio::task::spawn_blocking(move || {
        crate::support::fleetring()
            .env_remove("FLEETRING_TLS_DIR")
            .env_remove("FLEETRING_AS")
            .args(["status", "--control", "127.0.0.1:1", "--as", "reader"])
            .output()
    })
    .await??;
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("--as reader needs FLEETRING_TLS_DIR to find client.reader.crt"),
        "{stderr}"
    );

    node.run_until(async {}).await;
    Ok(())
}

/// How many requests `daemon` refused for a scope or a binding: `PERMISSION_DENIED`, whatever
/// the requirement was, `fleet` included.
fn scope_refusals(daemon: &Daemon) -> Result<u64, Box<dyn Error>> {
    let text = daemon.metrics().encode();
    let line = text
        .lines()
        .find(|line| {
            line.starts_with("fleetring_auth_refusals_total{") && line.contains("reason=\"scope\"")
        })
        .ok_or("no count of scope refusals")?;
    let (_, value) = line.rsplit_once(' ').ok_or("a sample without a value")?;
    Ok(value.parse()?)
}

/// ADR-0041 point 5, over the real gRPC path: a principal whose only scope is
/// `admin:agent-42-*`, and no `fleet`, creates, describes, labels, uses, finds, shows, and drops
/// its own databases, and is refused everything else. No call it makes for that needs `fleet`:
/// neither daemon refuses anything during it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_agent_token_manages_its_own_prefix_and_nothing_else() -> TestResult {
    use fleetring_control::{Labels, MetadataChange, parse_label};
    use fleetringd::data::{ListFilter, NewMetadata};

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

    // The operator creates a database outside the agent's prefix, with the label the agent
    // will look for.
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
    let memory = || Labels::try_from_pairs([parse_label("purpose=memory")?]);
    let notes: DatabaseName = "notes".parse()?;
    admin
        .create_database_with_metadata(
            &notes,
            DatabaseSpec::new(ReplicationFactor::ONE),
            &NewMetadata {
                description: Some("The operator's notes.".parse()?),
                labels: memory()?,
                ..NewMetadata::default()
            },
        )
        .await?;

    let signer = tls.authority().token_signer()?;
    let token = token(
        &signer,
        "app",
        &["admin:agent-42-*"],
        "agent-42",
        Duration::from_secs(3600),
    )?;
    let agent = Client::new(
        control_target.clone(),
        with_token(network(&tls, "app")?, &token)?,
    );
    let before = (scope_refusals(&control)?, scope_refusals(&data)?);

    // Everything the agent does with its own databases.
    let own: DatabaseName = "agent-42-notes".parse()?;
    let created = agent
        .create_database_with_metadata(
            &own,
            DatabaseSpec::new(ReplicationFactor::ONE),
            &NewMetadata {
                description: Some("Memory for agent 42.".parse()?),
                labels: memory()?,
                ..NewMetadata::default()
            },
        )
        .await?;
    let metadata = created.metadata.ok_or("no metadata")?;
    assert_eq!(metadata.created_by.as_deref(), Some("app"));
    assert_eq!(metadata.description_by.as_deref(), Some("app"));
    agent
        .execute(&own, Statement::new("CREATE TABLE t (v INTEGER)"))
        .await?;
    agent.execute(&own, write_one()).await?;
    assert_eq!(agent.query(&own, read_one()).await?.rows.len(), 1);
    let schema = agent
        .schema(
            &own,
            SchemaRequest {
                count_limit: 100,
                internal: Internal::Hide,
            },
        )
        .await?
        .schema;
    assert_eq!(schema.objects.len(), 1, "{schema:?}");
    assert_eq!(schema.objects[0].name, "t");
    assert_eq!(schema.objects[0].rows, Some(RowCount::Exact(1)));
    let updated = agent
        .update_database(
            &own,
            &MetadataChange {
                set_labels: Labels::try_from_pairs([parse_label("task=7")?])?,
                ..MetadataChange::default()
            },
            Some(1),
        )
        .await?;
    assert_eq!(updated.version, 2);
    let shown = agent.locate(&own).await?.metadata.ok_or("no metadata")?;
    assert_eq!(shown, updated);
    assert_eq!(shown.label("task"), Some("7"));
    let listed = |filter: ListFilter| {
        let agent = agent.clone();
        async move {
            Ok::<_, Box<dyn Error>>(
                agent
                    .list_databases_where(&filter)
                    .await?
                    .databases
                    .into_iter()
                    .map(|location| location.database.to_string())
                    .collect::<Vec<_>>(),
            )
        }
    };
    let by_label = listed(ListFilter {
        labels: vec!["purpose=memory".parse()?],
        ..ListFilter::default()
    })
    .await?;
    assert_eq!(by_label, ["agent-42-notes"], "notes is outside the scope");
    assert_eq!(listed(ListFilter::default()).await?, ["agent-42-notes"]);
    agent.drop_database(&own).await?;
    assert_eq!(
        (scope_refusals(&control)?, scope_refusals(&data)?),
        before,
        "the agent's own work needs no scope it lacks"
    );

    // Everything else is refused, before it touches any state.
    let denied = |status: tonic::Status, what: &str| {
        assert_eq!(status.code(), Code::PermissionDenied, "{what}: {status}");
        status.message().to_owned()
    };
    for other in ["agent-420-x", "agent-43-x", "agent-42"] {
        let refused = agent
            .create_database_with_metadata(
                &other.parse()?,
                DatabaseSpec::new(ReplicationFactor::ONE),
                &NewMetadata {
                    labels: memory()?,
                    ..NewMetadata::default()
                },
            )
            .await
            .expect_err("outside the prefix");
        assert_eq!(
            denied(refused, other),
            format!("CreateDatabaseWithMetadata needs admin on '{other}'")
        );
    }
    let relabel = MetadataChange {
        set_labels: Labels::try_from_pairs([parse_label("owner=agent-42")?])?,
        ..MetadataChange::default()
    };
    let refused = agent
        .update_database(&notes, &relabel, None)
        .await
        .expect_err("not the agent's");
    assert_eq!(
        denied(refused, "update"),
        "UpdateDatabase needs admin on 'notes'"
    );
    denied(
        agent
            .drop_database(&notes)
            .await
            .expect_err("not the agent's"),
        "drop",
    );
    denied(
        agent.locate(&notes).await.expect_err("not the agent's"),
        "locate",
    );
    // The schema of a database outside the scope is refused wherever it is asked: by the
    // control plane, which the client asks where the primary is, and by the data node itself.
    let everything = SchemaRequest {
        count_limit: 100,
        internal: Internal::Show,
    };
    assert_eq!(
        denied(
            agent
                .schema(&notes, everything)
                .await
                .expect_err("not the agent's"),
            "schema",
        ),
        "Locate needs read on 'notes'"
    );
    let direct = agent.clone().with_entry(data.listen_address().to_string());
    assert_eq!(
        denied(
            direct
                .schema(&notes, everything)
                .await
                .expect_err("not the agent's"),
            "schema, direct",
        ),
        "Schema needs read on 'notes'"
    );
    let refusals = (before.0 + 7, before.1 + 1);
    tokio::time::timeout(HANG_GUARD, async {
        while scope_refusals(&control)? < refusals.0 || scope_refusals(&data)? < refusals.1 {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        Ok::<_, Box<dyn Error>>(())
    })
    .await??;
    assert_eq!(
        (scope_refusals(&control)?, scope_refusals(&data)?),
        refusals
    );
    let untouched = admin.locate(&notes).await?.metadata.ok_or("no metadata")?;
    assert_eq!(untouched.version, 1);

    data.run_until(async {}).await;
    control.run_until(async {}).await;
    Ok(())
}

/// The same agent through the command line: `db create --description --label`, `db show`,
/// `db edit`, `db list --label`, and `db drop`, with a token whose
/// only scope is `admin:agent-42-*`; and `token issue` warns about a prefix without a separator.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_command_line_manages_an_agent_prefix_with_a_token() -> TestResult {
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
    // The operator's database, with the label the agent looks for.
    succeeded(
        &fleetringd(
            &tls_dir,
            &[],
            &[
                &[
                    "db",
                    "create",
                    "notes",
                    "--replication-factor",
                    "1",
                    "--label",
                    "purpose=memory",
                ][..],
                &target,
            ]
            .concat(),
        )
        .await?,
        "operator creates notes",
    )?;

    succeeded(
        &fleetringd(&tls_dir, &[], &["cert", "client", "app", "--dir", tls_text]).await?,
        "cert client",
    )?;
    let issue = |scope: &'static str| {
        let tls_dir = tls_dir.clone();
        async move {
            fleetringd(
                &tls_dir,
                &[],
                &[
                    "token",
                    "issue",
                    "--principal",
                    "app",
                    "--scope",
                    scope,
                    "--ttl",
                    "1h",
                    "--dir",
                    tls_dir.to_str().ok_or("a non-UTF-8 path")?,
                ],
            )
            .await
        }
    };
    let loose = issue("admin:agent-42*").await?;
    succeeded(&loose, "token issue")?;
    let warned = String::from_utf8_lossy(&loose.stderr);
    assert!(
        warned.contains(
            "warning: the scope admin:agent-42* also covers databases such as agent-420…; end \
             the prefix with a separator, such as admin:agent-42-*"
        ),
        "{warned}"
    );
    let issued = issue("admin:agent-42-*").await?;
    let token = succeeded(&issued, "token issue")?.trim().to_owned();
    let quiet = String::from_utf8_lossy(&issued.stderr);
    assert!(!quiet.contains("warning"), "{quiet}");

    let app_cert = tls_dir.join("client.app.crt");
    let app_key = tls_dir.join("client.app.key");
    let as_app = [
        "--cert",
        app_cert.to_str().ok_or("a non-UTF-8 path")?,
        "--key",
        app_key.to_str().ok_or("a non-UTF-8 path")?,
    ];
    let agent = |words: Vec<&str>| {
        let tls_dir = tls_dir.clone();
        let args: Vec<String> = [&words[..], &target, &as_app]
            .concat()
            .into_iter()
            .map(str::to_owned)
            .collect();
        let token = token.clone();
        async move {
            let args: Vec<&str> = args.iter().map(String::as_str).collect();
            fleetringd(&tls_dir, &[("FLEETRING_TOKEN", token.as_str())], &args).await
        }
    };

    let created = agent(vec![
        "db",
        "create",
        "agent-42-notes",
        "--replication-factor",
        "1",
        "--description",
        "Memory for agent 42.\nTables: facts.",
        "--label",
        "purpose=memory",
        "--label",
        "agent=42",
    ])
    .await?;
    let created = succeeded(&created, "agent creates its database")?;
    assert!(
        created.contains("labels: agent=42, purpose=memory"),
        "{created}"
    );
    assert!(created.contains("creator: app"), "{created}");
    for sql in [
        "CREATE TABLE facts (id INTEGER PRIMARY KEY, body TEXT)",
        "CREATE INDEX facts_body ON facts (body)",
        "INSERT INTO facts (body) VALUES ('one'), ('two')",
    ] {
        succeeded(
            &agent(vec!["db", "exec", "agent-42-notes", sql]).await?,
            "agent writes its database",
        )?;
    }

    let shown = succeeded(
        &agent(vec!["db", "show", "agent-42-notes", "--json"]).await?,
        "db show --json",
    )?;
    let shown: serde_json::Value = serde_json::from_str(&shown)?;
    assert_eq!(shown["database"], "agent-42-notes");
    assert_eq!(shown["placement"]["replication_factor"], 1);
    let metadata = &shown["metadata"];
    assert_eq!(
        metadata["description"],
        "Memory for agent 42.\nTables: facts."
    );
    assert_eq!(metadata["description_by"], "app");
    assert_eq!(metadata["created_by"], "app");
    assert_eq!(metadata["labels"]["purpose"], "memory");
    assert_eq!(metadata["labels"]["agent"], "42");
    assert!(metadata["expires_at"].is_null());
    assert_eq!(metadata["version"], 1);
    assert!(shown["schema_unavailable"].is_null());
    assert_eq!(shown["schema"]["truncated"], false);
    let objects = shown["schema"]["objects"]
        .as_array()
        .ok_or("no schema objects")?;
    let keys: Vec<&str> = objects[0]
        .as_object()
        .ok_or("an object")?
        .keys()
        .map(String::as_str)
        .collect();
    assert_eq!(
        keys,
        [
            "definition",
            "kind",
            "name",
            "rows",
            "rows_at_least",
            "table"
        ]
    );
    let described: Vec<(&str, &str, Option<u64>)> = objects
        .iter()
        .map(|object| {
            (
                object["kind"].as_str().unwrap_or_default(),
                object["name"].as_str().unwrap_or_default(),
                object["rows"].as_u64(),
            )
        })
        .collect();
    assert_eq!(
        described,
        [("table", "facts", Some(2)), ("index", "facts_body", None)]
    );
    assert_eq!(
        objects[0]["definition"],
        "CREATE TABLE facts (id INTEGER PRIMARY KEY, body TEXT)"
    );
    assert_eq!(objects[1]["table"], "facts");
    let text = succeeded(
        &agent(vec!["db", "show", "agent-42-notes"]).await?,
        "db show",
    )?;
    assert!(
        text.contains(
            "description:\n  | Memory for agent 42.\n  | Tables: facts.\ndescription by: app (you)\n"
        ),
        "{text}"
    );
    assert!(
        text.ends_with(
            "schema:\n  table facts: 2 rows\n    | CREATE TABLE facts (id INTEGER PRIMARY KEY, body TEXT)\n    index facts_body\n      | CREATE INDEX facts_body ON facts (body)\n"
        ),
        "{text}"
    );
    let capped = succeeded(
        &agent(vec!["db", "show", "agent-42-notes", "--count-limit", "1"]).await?,
        "db show --count-limit",
    )?;
    assert!(
        capped.contains("table facts: more than 1 row\n"),
        "{capped}"
    );
    let skipped = succeeded(
        &agent(vec![
            "db",
            "show",
            "agent-42-notes",
            "--no-schema",
            "--json",
        ])
        .await?,
        "db show --no-schema",
    )?;
    let skipped: serde_json::Value = serde_json::from_str(&skipped)?;
    assert!(skipped["schema"].is_null() && skipped["schema_unavailable"].is_null());

    let relabelled = succeeded(
        &agent(vec![
            "db",
            "edit",
            "agent-42-notes",
            "--label",
            "task=7",
            "--remove-label",
            "agent",
            "--if-version",
            "1",
        ])
        .await?,
        "db edit --label",
    )?;
    assert!(
        relabelled.starts_with("metadata: updated\n"),
        "{relabelled}"
    );
    assert!(
        relabelled.contains("\nlabels: purpose=memory, task=7\n"),
        "{relabelled}"
    );
    assert!(relabelled.ends_with("version: 2\n"), "{relabelled}");
    let stale = agent(vec![
        "db",
        "edit",
        "agent-42-notes",
        "--description",
        "lost",
        "--if-version",
        "1",
    ])
    .await?;
    failed_with(
        &stale,
        "a stale --if-version",
        "the database's metadata changed since version 1; read it again with fleetring db show",
    );
    succeeded(
        &agent(vec![
            "db",
            "edit",
            "agent-42-notes",
            "--description",
            "Memory, version 2.",
            "--if-version",
            "2",
        ])
        .await?,
        "db edit --description",
    )?;

    // Every setting alone, then several together.
    let described = succeeded(
        &agent(vec!["db", "edit", "agent-42-notes", "--clear-description"]).await?,
        "db edit --clear-description",
    )?;
    assert!(described.contains("description: none\n"), "{described}");
    assert!(described.ends_with("version: 4\n"), "{described}");
    let expiring = succeeded(
        &agent(vec!["db", "edit", "agent-42-notes", "--expires-in", "48h"]).await?,
        "db edit --expires-in",
    )?;
    assert!(expiring.contains("\nexpires: 20"), "{expiring}");
    assert!(expiring.contains(" (in "), "{expiring}");
    let dated = succeeded(
        &agent(vec![
            "db",
            "edit",
            "agent-42-notes",
            "--expires-at",
            "2099-01-01T00:00:00Z",
        ])
        .await?,
        "db edit --expires-at",
    )?;
    assert!(
        dated.contains("\nexpires: 2099-01-01T00:00:00Z (in "),
        "{dated}"
    );
    let never = succeeded(
        &agent(vec!["db", "edit", "agent-42-notes", "--clear-expiry"]).await?,
        "db edit --clear-expiry",
    )?;
    assert!(never.contains("\nexpires: never\n"), "{never}");
    let durable = succeeded(
        &agent(vec!["db", "edit", "agent-42-notes", "--acks", "all"]).await?,
        "db edit --acks",
    )?;
    assert_eq!(
        durable,
        "durability: acks=all min_replicas=1 at its next epoch; now acks=quorum min_replicas=1\n"
    );
    let combined = succeeded(
        &agent(vec![
            "db",
            "edit",
            "agent-42-notes",
            "--description",
            "Combined.",
            "--label",
            "stage=test",
            "--expires-in",
            "24h",
            "--acks",
            "quorum",
            "--min-replicas",
            "1",
            "--json",
        ])
        .await?,
        "db edit with every kind of setting",
    )?;
    let combined: serde_json::Value = serde_json::from_str(&combined)?;
    assert_eq!(combined["database"], "agent-42-notes");
    assert_eq!(combined["durability"]["acks"], "quorum");
    assert_eq!(combined["metadata"]["description"], "Combined.");
    assert_eq!(combined["metadata"]["labels"]["stage"], "test");
    assert!(combined["metadata"]["expires_at"].is_string());
    assert!(combined["metadata_error"].is_null());

    // An empty edit changes nothing, and says so.
    for words in [
        vec!["db", "edit", "agent-42-notes"],
        vec!["db", "edit", "agent-42-notes", "--if-version", "1"],
    ] {
        failed_with(
            &agent(words).await?,
            "an empty edit",
            "nothing to edit: pass at least one of --description",
        );
    }

    // Durability goes first; a metadata update refused after it says so, and the exit is not 0.
    let partial = agent(vec![
        "db",
        "edit",
        "agent-42-notes",
        "--acks",
        "local",
        "--description",
        "Lost.",
        "--if-version",
        "1",
    ])
    .await?;
    assert!(!partial.status.success());
    let out = String::from_utf8_lossy(&partial.stdout);
    assert!(
        out.contains("durability: acks=local min_replicas=1 at its next epoch; now acks=")
            && out.ends_with("metadata: not changed\n"),
        "{out}"
    );
    let err = String::from_utf8_lossy(&partial.stderr);
    assert!(
        err.contains(
            "the durability change of agent-42-notes was applied, but the metadata update \
             failed: the database's metadata changed since version 1"
        ),
        "{err}"
    );
    let shown = succeeded(
        &agent(vec![
            "db",
            "show",
            "agent-42-notes",
            "--json",
            "--no-schema",
        ])
        .await?,
        "db show after a partial edit",
    )?;
    let shown: serde_json::Value = serde_json::from_str(&shown)?;
    assert_eq!(shown["metadata"]["description"], "Combined.");

    // The removed verbs fail at parse, each with a hint naming `db edit`.
    for (verb, flags) in [
        ("set-description", "--description"),
        ("set-labels", "--label"),
        ("set-expiry", "--expires-in"),
        ("set-durability", "--acks"),
    ] {
        let removed = agent(vec!["db", verb, "agent-42-notes"]).await?;
        assert_eq!(removed.status.code(), Some(2), "{verb}");
        let err = String::from_utf8_lossy(&removed.stderr);
        assert!(
            err.contains(&format!("unrecognized subcommand '{verb}'"))
                && err.contains("hint: use 'fleetring db edit <db> ")
                && err.contains(flags),
            "{verb}: {err}"
        );
    }

    // `db show` carries the placement `db locate` prints, in text and in JSON.
    let located = succeeded(
        &agent(vec!["db", "locate", "agent-42-notes"]).await?,
        "db locate",
    )?;
    let shown_text = succeeded(
        &agent(vec!["db", "show", "agent-42-notes", "--no-schema"]).await?,
        "db show",
    )?;
    for line in located.lines() {
        assert!(
            shown_text.contains(&format!("{line}\n")),
            "{line}\n{shown_text}"
        );
    }
    assert!(
        shown_text.contains("primary: node-1 (") && shown_text.contains("\nfollowers: none\n"),
        "{shown_text}"
    );
    let shown = succeeded(
        &agent(vec![
            "db",
            "show",
            "agent-42-notes",
            "--no-schema",
            "--json",
        ])
        .await?,
        "db show --json",
    )?;
    let shown: serde_json::Value = serde_json::from_str(&shown)?;
    let placement = &shown["placement"];
    assert_eq!(placement["primary"], "node-1");
    assert_eq!(placement["followers"], serde_json::json!([]));
    assert!(
        placement["lease_epoch"]
            .as_u64()
            .is_some_and(|epoch| epoch >= 1)
    );
    assert_eq!(placement["acks"], "quorum");
    assert_eq!(placement["replicas"][0]["node"], "node-1");
    assert!(placement["replicas"][0]["position"].is_string());

    let listed = succeeded(
        &agent(vec!["db", "list", "--label", "purpose=memory"]).await?,
        "db list --label",
    )?;
    let names: Vec<&str> = listed
        .lines()
        .skip(1)
        .filter_map(|line| line.split_whitespace().next())
        .filter(|name| !name.ends_with(';') && name.parse::<u64>().is_err())
        .collect();
    assert_eq!(names, ["agent-42-notes"], "{listed}");

    failed_with(
        &agent(vec![
            "db",
            "create",
            "agent-420",
            "--label",
            "purpose=memory",
        ])
        .await?,
        "outside the prefix",
        "CreateDatabaseWithMetadata needs admin on 'agent-420'",
    );
    succeeded(
        &agent(vec!["db", "drop", "agent-42-notes", "--yes"]).await?,
        "db drop",
    )?;

    node.run_until(async {}).await;
    Ok(())
}

/// FR-049: restoring from and describing archives.
mod archives {
    use super::*;
    use fleetringd::durability::RestoreTarget;

    /// A control daemon, and a data daemon that archives every database to a directory, with
    /// `tenant:acme` created, written, and archived.
    struct Archived {
        control: Daemon,
        data: Daemon,
        tls: Tls,
        control_target: String,
        data_target: String,
    }

    impl Archived {
        async fn start(dir: &Path) -> Result<Self, Box<dyn Error>> {
            let tls = Tls::create(dir)?;
            let control = Daemon::start(Config::from_toml(&config(
                dir, &tls, "control", "control", None,
            )?)?)
            .await?;
            let address = control.control_address().ok_or("no control address")?;
            let archive = format!(
                "\n[[archive]]\nname = \"backups\"\ndatabases = [\"*\"]\nstore = \"directory\"\n\
                 path = {:?}\nsegment_interval_ms = 50\n",
                dir.join("archive")
            );
            let text = format!(
                "{}{archive}",
                config(dir, &tls, "data-1", "data", Some(address))?
            );
            let data = Daemon::start(Config::from_toml(&text)?).await?;
            let archived = Self {
                control_target: address.to_string(),
                data_target: data.listen_address().to_string(),
                control,
                data,
                tls,
            };
            let admin = archived.client("tester")?;
            tokio::time::timeout(HANG_GUARD, async {
                loop {
                    match admin.status().await {
                        Ok(view) if view.members().len() == 1 => {
                            return Ok::<_, tonic::Status>(());
                        }
                        _ => tokio::time::sleep(Duration::from_millis(50)).await,
                    }
                }
            })
            .await??;
            let acme: DatabaseName = "tenant:acme".parse()?;
            admin
                .create_database_with(&acme, DatabaseSpec::new(ReplicationFactor::ONE))
                .await?;
            admin
                .execute(&acme, Statement::new("CREATE TABLE t (v INTEGER)"))
                .await?;
            admin.execute(&acme, write_one()).await?;
            let streams = archived.data.change_streams().ok_or("change streams")?;
            if !streams.await_caught_up(HANG_GUARD).await {
                return Err("the archive didn't catch up".into());
            }
            Ok(archived)
        }

        fn client(&self, principal: &str) -> Result<Client, Box<dyn Error>> {
            Ok(Client::new(
                self.control_target.clone(),
                network(&self.tls, principal)?,
            ))
        }

        /// A client acting as `app` with a token carrying `scopes`.
        fn app(&self, scopes: &[&str], id: &str) -> Result<Client, Box<dyn Error>> {
            let signer = self.tls.authority().token_signer()?;
            let token = token(&signer, "app", scopes, id, Duration::from_secs(3600))?;
            Ok(Client::new(
                self.control_target.clone(),
                with_token(network(&self.tls, "app")?, &token)?,
            ))
        }

        async fn stop(self) {
            self.data.run_until(async {}).await;
            self.control.run_until(async {}).await;
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn restore_needs_admin_on_the_target_and_the_source() -> TestResult {
        let dir = tempfile::tempdir()?;
        let archived = Archived::start(dir.path()).await?;
        let acme: DatabaseName = "tenant:acme".parse()?;
        let target: DatabaseName = "restored/x".parse()?;
        let spec = DatabaseSpec::new(ReplicationFactor::ONE);

        // Admin on the new database and read on the source aren't enough: a restore copies the
        // source's whole history into a database its caller controls.
        let reader = archived.app(&["admin:restored/*", "read:tenant:*"], "restore-1")?;
        let refused = reader
            .restore_database(&target, spec, "backups", &acme, &RestoreTarget::Latest)
            .await
            .expect_err("no admin on the source");
        assert_eq!(refused.code(), Code::PermissionDenied, "{refused}");
        assert_eq!(refused.message(), "Restore needs admin on 'tenant:acme'");

        // Admin on the source but not on the new database is refused at creation.
        let source_admin = archived.app(&["admin:tenant:*"], "restore-2")?;
        let refused = source_admin
            .restore_database(
                &"other/y".parse()?,
                spec,
                "backups",
                &acme,
                &RestoreTarget::Latest,
            )
            .await
            .expect_err("no admin on the target");
        assert_eq!(refused.code(), Code::PermissionDenied, "{refused}");

        // Admin on both restores.
        let both = archived.app(&["admin:restored/*", "admin:tenant:*"], "restore-3")?;
        let restored = both
            .restore_database(&target, spec, "backups", &acme, &RestoreTarget::Latest)
            .await?;
        assert!(restored.location.primary.is_some());
        let rows = both.query(&target, read_one()).await?;
        assert_eq!(rows.rows, vec![vec![Value::Integer(1)]]);
        archived.stop().await;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn describing_an_archive_needs_read_on_the_key_and_verifying_it_admin() -> TestResult {
        let dir = tempfile::tempdir()?;
        let archived = Archived::start(dir.path()).await?;
        let acme: DatabaseName = "tenant:acme".parse()?;
        let data = archived.data_target.clone();

        let other = archived.app(&["read:tenant:zed*"], "describe-1")?;
        let refused = other
            .describe_archive(&data, "backups", &acme)
            .await
            .expect_err("no read on the database");
        assert_eq!(refused.code(), Code::PermissionDenied, "{refused}");
        assert_eq!(
            refused.message(),
            "DescribeArchive needs read on 'tenant:acme'"
        );
        let refused = other
            .verify_archive(&data, "backups", &acme, &RestoreTarget::Latest)
            .await
            .expect_err("no read on the database");
        assert_eq!(refused.code(), Code::PermissionDenied, "{refused}");

        // Read, or the fleet scope, shows the archive, and verifying it, which builds a whole
        // copy on the node's disk, needs admin.
        for (scopes, id) in [
            (["read:tenant:acme"], "describe-2"),
            (["fleet"], "describe-3"),
        ] {
            let allowed = archived.app(&scopes, id)?;
            let described = allowed.describe_archive(&data, "backups", &acme).await?;
            assert_eq!(described.snapshots.len(), 1, "{scopes:?}");
            let refused = allowed
                .verify_archive(&data, "backups", &acme, &RestoreTarget::Latest)
                .await
                .expect_err("verifying needs admin");
            assert_eq!(
                refused.code(),
                Code::PermissionDenied,
                "{scopes:?}: {refused}"
            );
            assert_eq!(
                refused.message(),
                "VerifyArchive needs admin on 'tenant:acme'"
            );
        }
        let admin = archived.app(&["admin:tenant:acme"], "verify-1")?;
        let described = admin.describe_archive(&data, "backups", &acme).await?;
        let verified = admin
            .verify_archive(&data, "backups", &acme, &RestoreTarget::Latest)
            .await?;
        assert_eq!(verified.position, described.latest);
        archived.stop().await;
        Ok(())
    }
}
