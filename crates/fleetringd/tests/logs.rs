//! FR-063: what the daemon's JSON logs say about a request: the `rpc` span on every line it
//! causes, and its request ID on the calls a node makes for it (ADR-0034, point 8).
//!
//! These tests read the lines of the daemon's own subscriber (`observe::log_subscriber`, JSON,
//! `fleetringd` at debug), installed once as the process's global default. A subscriber set with
//! `set_default` covers only its own thread, and tracing caches whether a callsite is enabled
//! from whichever thread reaches it first, so in a binary whose other tests run daemons with no
//! subscriber it would miss spans depending on the order the tests run in. This binary runs only
//! tests that read the global subscriber's lines, each picking out its own by request ID or
//! node ID.

use std::collections::BTreeSet;
use std::error::Error;
use std::path::Path;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use fleetring::ReplicationFactor;
use fleetring_control::DatabaseName;
use fleetring_proto::v1::{Acks, CreateDatabaseRequest, CreateRequest, Engine, ExecuteRequest};
use fleetringd::data::Client;
use fleetringd::observe::log_subscriber;
use fleetringd::transport::database_client;
use fleetringd::{Config, Daemon, LogFormat};

use crate::support::Tls;

mod support;

type TestResult = Result<(), Box<dyn Error>>;

/// Guards against a hang only; nothing here asserts how long anything takes.
const HANG_GUARD: Duration = Duration::from_secs(60);

/// How often a test looks again at what it waits for.
const POLL: Duration = Duration::from_millis(50);

/// A writer that keeps every log line in memory.
#[derive(Debug, Clone, Default)]
struct Lines(Arc<Mutex<Vec<u8>>>);

impl std::io::Write for Lines {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.lock().expect("lines lock").extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'writer> tracing_subscriber::fmt::MakeWriter<'writer> for Lines {
    type Writer = Self;

    fn make_writer(&'writer self) -> Self::Writer {
        self.clone()
    }
}

/// The lines the process's global subscriber has written, installing it on first use.
fn lines() -> &'static Lines {
    static LINES: OnceLock<Lines> = OnceLock::new();
    LINES.get_or_init(|| {
        let lines = Lines::default();
        let filter = tracing_subscriber::EnvFilter::new("info,fleetringd=debug");
        tracing::subscriber::set_global_default(log_subscriber(
            LogFormat::Json,
            filter,
            lines.clone(),
        ))
        .expect("no other global subscriber in this binary");
        lines
    })
}

/// Every line written so far, each parsed as one JSON object.
fn objects() -> Result<Vec<serde_json::Value>, Box<dyn Error>> {
    let text = String::from_utf8(lines().0.lock().expect("lines lock").clone())?;
    text.lines()
        .map(|line| {
            let object: serde_json::Value = serde_json::from_str(line)
                .map_err(|error| format!("{error}: not one JSON object: {line}"))?;
            if object.is_object() {
                Ok(object)
            } else {
                Err(format!("not an object: {line}").into())
            }
        })
        .collect()
}

/// The `rpc finished` lines of RPCs whose request ID `matches`: each with its span.
fn finished_rpcs(matches: impl Fn(&str) -> bool) -> Result<Vec<serde_json::Value>, Box<dyn Error>> {
    Ok(objects()?
        .into_iter()
        .filter(|object| {
            object["fields"]["message"] == serde_json::json!("rpc finished")
                && object["span"]["name"] == serde_json::json!("rpc")
                && object["span"]["request_id"].as_str().is_some_and(&matches)
        })
        .collect())
}

/// A daemon running both roles as node `id`, once it has joined and published a ring.
async fn both(dir: &Path, tls: &Tls, id: &str) -> Result<Daemon, Box<dyn Error>> {
    let text = format!(
        "data_dir = {:?}\nlisten = \"127.0.0.1:0\"\nrole = \"both\"\n\n[node]\nid = \"{id}\"\n\n\
         [membership]\nheartbeat_interval_ms = 100\n{}",
        dir.join(id),
        tls.node_table(id)?
    );
    let daemon = Daemon::start(Config::from_toml(&text)?).await?;
    let membership = daemon.membership().ok_or("no membership")?;
    tokio::time::timeout(
        HANG_GUARD,
        membership.wait_for(|view| view.ring().is_some()),
    )
    .await?
    .ok_or("membership stopped")?;
    Ok(daemon)
}

fn client(daemon: &Daemon, tls: &Tls) -> Result<Client, Box<dyn Error>> {
    let control = daemon.control_address().ok_or("no control address")?;
    Ok(Client::new(
        control.to_string(),
        tls.client_network("tester")?,
    ))
}

/// Polls `check` until it holds.
async fn eventually(
    what: &str,
    mut check: impl FnMut() -> Result<bool, Box<dyn Error>>,
) -> TestResult {
    let waited = tokio::time::timeout(HANG_GUARD, async {
        loop {
            if check()? {
                return Ok::<(), Box<dyn Error>>(());
            }
            tokio::time::sleep(POLL).await;
        }
    })
    .await;
    match waited {
        Ok(result) => result,
        Err(_) => Err(format!("gave up waiting for {what}").into()),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_request_id_reaches_the_control_plane() -> TestResult {
    lines();
    let dir = tempfile::tempdir()?;
    let tls = Tls::create(dir.path())?;
    let daemon = both(dir.path(), &tls, "carrier").await?;
    let address = daemon.listen_address().to_string();
    // The control plane places the database; the data node's `Create` takes its first lease.
    client(&daemon, &tls)?
        .control()
        .create_database(CreateDatabaseRequest {
            database: "ledger".to_owned(),
            replication_factor: 1,
            engine: Engine::Sqlite.into(),
            acks: Acks::Unspecified.into(),
            min_replicas: 0,
        })
        .await?;
    let mut request = tonic::Request::new(CreateRequest {
        database: "ledger".to_owned(),
    });
    request
        .metadata_mut()
        .insert("x-request-id", "req-42".parse()?);
    let network = tls.client_network("tester")?;
    tokio::time::timeout(
        HANG_GUARD,
        database_client(network.channel(&address)?).create(request),
    )
    .await??;
    // The span's line is written once the response's status has passed, which may be after the
    // client has read it.
    eventually("the request's spans", || {
        let methods: BTreeSet<String> = finished_rpcs(|id| id == "req-42")?
            .iter()
            .filter_map(|object| object["span"]["method"].as_str().map(str::to_owned))
            .collect();
        Ok(methods.contains("Create") && methods.contains("AcquireLease"))
    })
    .await?;
    // Calls made for no request get IDs of their own, from the node's counter.
    eventually("a heartbeat with the node's own ID", || {
        Ok(finished_rpcs(|id| id.starts_with("carrier-"))?
            .iter()
            .any(|object| object["span"]["method"] == serde_json::json!("Heartbeat")))
    })
    .await?;
    daemon.run_until(async {}).await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn json_logs_carry_the_rpc_span() -> TestResult {
    lines();
    let dir = tempfile::tempdir()?;
    let tls = Tls::create(dir.path())?;
    let daemon = both(dir.path(), &tls, "writer").await?;
    let ledger: DatabaseName = "ledger".parse()?;
    client(&daemon, &tls)?
        .create_database(&ledger, ReplicationFactor::ONE)
        .await?;
    let address = daemon.listen_address().to_string();
    let mut request = tonic::Request::new(ExecuteRequest {
        database: ledger.to_string(),
        sql: "CREATE TABLE t (x)".to_owned(),
        params: Vec::new(),
    });
    request
        .metadata_mut()
        .insert("x-request-id", "req-7".parse()?);
    tokio::time::timeout(
        HANG_GUARD,
        database_client(tls.client_network("tester")?.channel(&address)?).execute(request),
    )
    .await??;
    eventually("the request's span", || {
        Ok(!finished_rpcs(|id| id == "req-7")?.is_empty())
    })
    .await?;
    daemon.run_until(async {}).await;

    // Every line parses as one JSON object.
    let rpc = finished_rpcs(|id| id == "req-7")?
        .into_iter()
        .next()
        .ok_or("no line of the request's span")?;
    assert_eq!(rpc["span"]["method"], serde_json::json!("Execute"), "{rpc}");
    assert_eq!(
        rpc["span"]["principal"],
        serde_json::json!("tester"),
        "{rpc}"
    );
    assert_eq!(
        rpc["span"]["database"],
        serde_json::json!("ledger"),
        "{rpc}"
    );
    assert_eq!(rpc["span"]["code"], serde_json::json!("ok"), "{rpc}");
    assert!(rpc["spans"].is_array(), "{rpc}");
    Ok(())
}
