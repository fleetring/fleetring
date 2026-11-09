//! FR-063: metrics, health and readiness on the admin listener, refusals, request IDs, and JSON
//! logs, against daemons over real TCP (ADR-0034).
//!
//! Nothing here asserts how long anything takes: each test waits for the state it checks, with a
//! guard against a hang only, and counts that scheduling could change are checked as lower bounds.

use std::collections::BTreeSet;
use std::error::Error;
use std::future::Future;
use std::io::Write as _;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use fleetring::ReplicationFactor;
use fleetring_control::{DatabaseName, Liveness};
use fleetring_proto::v1::{ExecuteRequest, FenceRequest};
use fleetringd::data::Client;
use fleetringd::engine::Statement;
use fleetringd::observe::{
    ALLOWED_LABELS, FAMILIES, Kind, LogColour, closed_values, log_subscriber,
};
use fleetringd::tls::Issued;
use fleetringd::transport::{database_client, replication_client};
use fleetringd::{Config, ConfigError, Daemon, LogFormat, StartError};
use http_body_util::{BodyExt as _, Empty};
use hyper::body::Bytes;
use rustls_pki_types::pem::PemObject as _;
use rustls_pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use tokio::io::{AsyncRead, AsyncReadExt as _, AsyncWrite, AsyncWriteExt as _};
use tokio::net::TcpStream;

use crate::support::Tls;

mod support;

type TestResult = Result<(), Box<dyn Error>>;

/// Guards against a hang only; nothing here asserts how long anything takes.
const HANG_GUARD: Duration = Duration::from_secs(60);

/// How often a test looks again at what it waits for.
const POLL: Duration = Duration::from_millis(50);

/// The content type of the exposition.
const OPENMETRICS: &str = "application/openmetrics-text; version=1.0.0; charset=utf-8";

// ---------------------------------------------------------------------------------------------
// Daemons.

/// A daemon's configuration: `[tls]` from `tls`, its admin listener on a port of its own and
/// sampling every 100 ms, and `admin` appended to the `[admin]` table (or tables after it).
fn config(
    dir: &Path,
    tls: &Tls,
    id: &str,
    role: &str,
    control: Option<SocketAddr>,
    admin: &str,
) -> Result<String, Box<dyn Error>> {
    let control = control.map_or_else(String::new, |address| {
        format!("\n[control]\naddress = \"{address}\"\n")
    });
    let tls = tls.node_tls_table(id)?;
    Ok(format!(
        "data_dir = {:?}\nlisten = \"127.0.0.1:0\"\nrole = \"{role}\"\n\n[node]\nid = \"{id}\"\n\n\
         [membership]\nheartbeat_interval_ms = 100\n{control}{tls}\n\
         [admin]\nlisten = \"127.0.0.1:0\"\nsample_interval_ms = 100\n{admin}",
        dir.join(id)
    ))
}

async fn start(text: &str) -> Result<Daemon, Box<dyn Error>> {
    Ok(Daemon::start(Config::from_toml(text)?).await?)
}

/// One daemon running both roles, once it has joined and published a ring, so that databases
/// can be created on it.
async fn both(dir: &Path, tls: &Tls, id: &str, admin: &str) -> Result<Daemon, Box<dyn Error>> {
    let daemon = start(&config(dir, tls, id, "both", None, admin)?).await?;
    joined(&daemon).await?;
    Ok(daemon)
}

/// Waits until a daemon that hosts data has joined and published a ring.
async fn joined(daemon: &Daemon) -> TestResult {
    let membership = daemon.membership().ok_or("no membership")?;
    tokio::time::timeout(
        HANG_GUARD,
        membership.wait_for(|view| view.ring().is_some()),
    )
    .await?
    .ok_or("membership stopped")?;
    Ok(())
}

fn admin_address(daemon: &Daemon) -> Result<SocketAddr, Box<dyn Error>> {
    Ok(daemon.admin_address().ok_or("no admin listener")?)
}

fn client(daemon: &Daemon, tls: &Tls) -> Result<Client, Box<dyn Error>> {
    let control = daemon.control_address().ok_or("no control address")?;
    Ok(Client::new(
        control.to_string(),
        tls.client_network("tester")?,
    ))
}

fn name(text: &str) -> Result<DatabaseName, Box<dyn Error>> {
    Ok(text.parse()?)
}

/// A control daemon and `n` data daemons that have joined and published one ring.
struct Fleet {
    control: Daemon,
    data: Vec<Daemon>,
}

impl Fleet {
    async fn start(dir: &Path, tls: &Tls, n: usize) -> Result<Self, Box<dyn Error>> {
        let control = start(&config(dir, tls, "control", "control", None, "")?).await?;
        let address = control.control_address().ok_or("no control address")?;
        let mut data = Vec::new();
        for k in 1..=n {
            let id = format!("data-{k}");
            data.push(start(&config(dir, tls, &id, "data", Some(address), "")?).await?);
        }
        let fleet = Self { control, data };
        fleet.converge(n).await?;
        Ok(fleet)
    }

    /// Waits until every data daemon publishes a ring with every member live.
    async fn converge(&self, n: usize) -> TestResult {
        for daemon in &self.data {
            let membership = daemon.membership().ok_or("no membership")?;
            tokio::time::timeout(
                HANG_GUARD,
                membership.wait_for(|view| {
                    view.ring().is_some()
                        && view.members().len() == n
                        && view.members().all(|(_, live)| live == Liveness::Live)
                }),
            )
            .await?
            .ok_or("membership stopped")?;
        }
        Ok(())
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

// ---------------------------------------------------------------------------------------------
// HTTP.

/// One HTTP answer.
#[derive(Debug)]
struct Reply {
    status: u16,
    content_type: Option<String>,
    allow: Option<String>,
    body: String,
}

/// Sends one HTTP/1.1 request on `stream`.
async fn send<S>(stream: S, method: &str, path: &str) -> Result<Reply, Box<dyn Error>>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let (mut sender, connection) =
        hyper::client::conn::http1::handshake(hyper_util::rt::TokioIo::new(stream)).await?;
    let driver = tokio::spawn(connection);
    let request = hyper::Request::builder()
        .method(method)
        .uri(path)
        .header("host", "127.0.0.1")
        .body(Empty::<Bytes>::new())?;
    let response = sender.send_request(request).await?;
    let header = |name: &str| {
        response
            .headers()
            .get(name)
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned)
    };
    let (status, content_type, allow) = (
        response.status().as_u16(),
        header("content-type"),
        header("allow"),
    );
    let body = response.into_body().collect().await?.to_bytes();
    driver.abort();
    Ok(Reply {
        status,
        content_type,
        allow,
        body: String::from_utf8(body.to_vec())?,
    })
}

async fn http(address: SocketAddr, method: &str, path: &str) -> Result<Reply, Box<dyn Error>> {
    let stream = tokio::time::timeout(HANG_GUARD, TcpStream::connect(address)).await??;
    tokio::time::timeout(HANG_GUARD, send(stream, method, path)).await?
}

async fn get(address: SocketAddr, path: &str) -> Result<Reply, Box<dyn Error>> {
    http(address, "GET", path).await
}

/// A client TLS configuration trusting `tls`'s authority, presenting `certificate` if given, and
/// asking for HTTP/1.1.
fn https_client(
    tls: &Tls,
    certificate: Option<&Issued>,
) -> Result<Arc<rustls::ClientConfig>, Box<dyn Error>> {
    let mut roots = rustls::RootCertStore::empty();
    for authority in CertificateDer::pem_slice_iter(tls.authority().certificate_pem().as_bytes()) {
        roots.add(authority?)?;
    }
    let builder = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_protocol_versions(&[&rustls::version::TLS13])?
    .with_root_certificates(roots);
    let mut config = match certificate {
        None => builder.with_no_client_auth(),
        Some(issued) => {
            let chain = CertificateDer::pem_slice_iter(issued.certificate_pem().as_bytes())
                .collect::<Result<Vec<_>, _>>()?;
            let key = PrivateKeyDer::from_pem_slice(issued.key_pem().as_bytes())?;
            builder.with_client_auth_cert(chain, key)?
        }
    };
    config.alpn_protocols = vec![b"http/1.1".to_vec()];
    Ok(Arc::new(config))
}

async fn https(
    address: SocketAddr,
    client: &Arc<rustls::ClientConfig>,
    path: &str,
) -> Result<Reply, Box<dyn Error>> {
    let connector = tokio_rustls::TlsConnector::from(Arc::clone(client));
    let stream = tokio::time::timeout(HANG_GUARD, TcpStream::connect(address)).await??;
    let server = ServerName::try_from(support::HOST)?;
    let stream = tokio::time::timeout(HANG_GUARD, connector.connect(server, stream)).await??;
    tokio::time::timeout(HANG_GUARD, send(stream, "GET", path)).await?
}

// ---------------------------------------------------------------------------------------------
// The exposition.

/// One sample of the exposition: its name, labels, and value.
#[derive(Debug, Clone, PartialEq)]
struct Sample {
    name: String,
    labels: Vec<(String, String)>,
    value: f64,
}

/// Parses every sample line up to `# EOF`.
fn samples(text: &str) -> Result<Vec<Sample>, Box<dyn Error>> {
    let mut samples = Vec::new();
    for line in text.lines() {
        if line == "# EOF" {
            return Ok(samples);
        }
        if line.starts_with('#') {
            continue;
        }
        let (series, value) = line.rsplit_once(' ').ok_or(format!("no value: {line}"))?;
        let value = match value {
            "+Inf" | "inf" => f64::INFINITY,
            other => other.parse()?,
        };
        let (name, labels) = match series.split_once('{') {
            None => (series, Vec::new()),
            Some((name, rest)) => (name, labels(rest.strip_suffix('}').ok_or("no '}'")?)?),
        };
        samples.push(Sample {
            name: name.to_owned(),
            labels,
            value,
        });
    }
    Err("no # EOF".into())
}

/// Parses `a="x",b="y"`, with `\"` and `\\` escapes in values.
fn labels(text: &str) -> Result<Vec<(String, String)>, Box<dyn Error>> {
    let mut labels = Vec::new();
    let mut rest = text;
    while !rest.is_empty() {
        let (name, after) = rest
            .split_once("=\"")
            .ok_or(format!("bad labels: {text}"))?;
        let mut value = String::new();
        let mut chars = after.char_indices();
        let end = loop {
            match chars.next() {
                Some((_, '\\')) => {
                    if let Some((_, escaped)) = chars.next() {
                        value.push(escaped);
                    }
                }
                Some((at, '"')) => break at,
                Some((_, c)) => value.push(c),
                None => return Err(format!("unterminated value: {text}").into()),
            }
        };
        labels.push((name.to_owned(), value));
        rest = after[end + 1..]
            .strip_prefix(',')
            .unwrap_or(&after[end + 1..]);
    }
    Ok(labels)
}

/// The value of the sample `name` whose labels include every pair of `labels`, summed over the
/// samples that match.
fn value(text: &str, name: &str, labels: &[(&str, &str)]) -> Result<Option<f64>, Box<dyn Error>> {
    let matching: Vec<f64> = samples(text)?
        .into_iter()
        .filter(|sample| sample.name == name)
        .filter(|sample| {
            labels.iter().all(|(key, wanted)| {
                sample
                    .labels
                    .iter()
                    .any(|(label, value)| label == key && value == wanted)
            })
        })
        .map(|sample| sample.value)
        .collect();
    Ok((!matching.is_empty()).then(|| matching.iter().sum()))
}

/// Polls `check` until it holds.
async fn eventually<F, Fut>(what: &str, mut check: F) -> TestResult
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<bool, Box<dyn Error>>>,
{
    let waited = tokio::time::timeout(HANG_GUARD, async {
        loop {
            if check().await? {
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

/// Waits until the daemon's exposition has `name{labels}` at `at_least` or more.
async fn eventually_at_least(
    daemon: &Daemon,
    name: &str,
    labels: &[(&str, &str)],
    at_least: f64,
) -> TestResult {
    let what = format!("{name}{labels:?} >= {at_least}");
    eventually(&what, || async {
        let text = daemon.metrics().encode();
        Ok(value(&text, name, labels)?.is_some_and(|value| value >= at_least))
    })
    .await
}

fn readiness(reply: &Reply) -> Result<serde_json::Value, Box<dyn Error>> {
    Ok(serde_json::from_str(&reply.body)?)
}

fn failing(readiness: &serde_json::Value) -> Vec<String> {
    readiness["checks"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|check| check["ok"] == serde_json::json!(false))
        .filter_map(|check| check["name"].as_str().map(str::to_owned))
        .collect()
}

// ---------------------------------------------------------------------------------------------
// The admin listener.

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn metrics_health_and_readiness_on_the_admin_listener() -> TestResult {
    let dir = tempfile::tempdir()?;
    let tls = Tls::create(dir.path())?;
    let daemon = both(dir.path(), &tls, "solo", "").await?;
    let admin = admin_address(&daemon)?;
    assert_ne!(admin.port(), daemon.listen_address().port());

    let health = get(admin, "/healthz").await?;
    assert_eq!((health.status, health.body.as_str()), (200, "ok\n"));

    let metrics = get(admin, "/metrics").await?;
    assert_eq!(metrics.status, 200);
    assert_eq!(metrics.content_type.as_deref(), Some(OPENMETRICS));
    for family in FAMILIES {
        assert!(
            metrics.body.contains(&format!("# TYPE {} ", family.name)),
            "{} is missing:\n{}",
            family.name,
            metrics.body
        );
    }
    assert!(metrics.body.ends_with("# EOF\n"));

    let head = http(admin, "HEAD", "/metrics").await?;
    assert_eq!(head.status, 200);
    assert_eq!(head.body, "");
    let post = http(admin, "POST", "/metrics").await?;
    assert_eq!(post.status, 405);
    assert_eq!(post.allow.as_deref(), Some("GET, HEAD"));
    assert_eq!(get(admin, "/nope").await?.status, 404);
    assert_eq!(get(admin, "/metrics?name=x").await?.status, 200);

    daemon.run_until(async {}).await;
    // The listener stops with the daemon.
    assert!(TcpStream::connect(admin).await.is_err() || get(admin, "/healthz").await.is_err());
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn readiness_lists_every_check_of_both_roles() -> TestResult {
    let dir = tempfile::tempdir()?;
    let tls = Tls::create(dir.path())?;
    let daemon = both(dir.path(), &tls, "solo", "").await?;
    let admin = admin_address(&daemon)?;
    eventually("readiness", || async {
        Ok(get(admin, "/readyz").await?.status == 200)
    })
    .await?;
    let ready = get(admin, "/readyz").await?;
    assert_eq!(ready.content_type.as_deref(), Some("application/json"));
    let body = readiness(&ready)?;
    assert_eq!(body["ready"], serde_json::json!(true));
    let names: Vec<&str> = body["checks"]
        .as_array()
        .ok_or("no checks")?
        .iter()
        .filter_map(|check| check["name"].as_str())
        .collect();
    assert_eq!(
        names,
        [
            "joined",
            "ring",
            "denylist",
            "draining",
            "control_log",
            "leader"
        ]
    );
    assert!(failing(&body).is_empty(), "{body}");
    daemon.run_until(async {}).await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_node_is_not_ready_until_it_joins() -> TestResult {
    let dir = tempfile::tempdir()?;
    let tls = Tls::create(dir.path())?;
    // A port nothing listens on: the node never reaches a control plane.
    let closed = std::net::TcpListener::bind("127.0.0.1:0")?.local_addr()?;
    let daemon = start(&config(
        dir.path(),
        &tls,
        "data-1",
        "data",
        Some(closed),
        "",
    )?)
    .await?;
    let admin = admin_address(&daemon)?;
    assert_eq!(get(admin, "/healthz").await?.status, 200);
    let unready = get(admin, "/readyz").await?;
    assert_eq!(unready.status, 503);
    let body = readiness(&unready)?;
    assert_eq!(body["ready"], serde_json::json!(false));
    assert_eq!(failing(&body), ["joined", "ring", "denylist"]);
    let joined = &body["checks"][0];
    assert_eq!(
        joined["detail"],
        serde_json::json!("not joined to the fleet yet")
    );
    daemon.run_until(async {}).await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_draining_node_is_not_ready() -> TestResult {
    let dir = tempfile::tempdir()?;
    let tls = Tls::create(dir.path())?;
    let fleet = Fleet::start(dir.path(), &tls, 2).await?;
    let staying = admin_address(fleet.data_daemon("data-1")?)?;
    let draining = admin_address(fleet.data_daemon("data-2")?)?;
    let client = client(&fleet.control, &tls)?;
    eventually("both nodes ready", || async {
        Ok(get(staying, "/readyz").await?.status == 200
            && get(draining, "/readyz").await?.status == 200)
    })
    .await?;
    client.drain_node(&"data-2".parse()?).await?;
    eventually("the drained node unready", || async {
        let reply = get(draining, "/readyz").await?;
        Ok(reply.status == 503 && failing(&readiness(&reply)?) == ["draining"])
    })
    .await?;
    let reply = get(draining, "/readyz").await?;
    let body = readiness(&reply)?;
    let check = body["checks"]
        .as_array()
        .ok_or("no checks")?
        .iter()
        .find(|check| check["name"] == serde_json::json!("draining"))
        .ok_or("no draining check")?;
    assert_eq!(check["detail"], serde_json::json!("the node is draining"));
    eventually("the other node ready", || async {
        Ok(get(staying, "/readyz").await?.status == 200)
    })
    .await?;
    assert_eq!(get(draining, "/healthz").await?.status, 200);
    fleet.stop().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sampled_gauges_count_the_databases() -> TestResult {
    let dir = tempfile::tempdir()?;
    let tls = Tls::create(dir.path())?;
    let daemon = both(dir.path(), &tls, "solo", "").await?;
    let client = client(&daemon, &tls)?;
    for k in 1..=3 {
        client
            .create_database(&name(&format!("db-{k}"))?, ReplicationFactor::ONE)
            .await?;
    }
    let admin = admin_address(&daemon)?;
    eventually("three databases sampled", || async {
        let text = get(admin, "/metrics").await?.body;
        Ok(
            value(&text, "fleetring_databases", &[("replica_role", "primary")])? == Some(3.0)
                && value(&text, "fleetring_leases_held", &[])? == Some(3.0)
                && value(&text, "fleetring_control_databases", &[])? == Some(3.0)
                && value(&text, "fleetring_databases_open", &[])?.is_some_and(|open| open >= 3.0),
        )
    })
    .await?;
    let text = daemon.metrics().encode();
    assert_eq!(
        value(
            &text,
            "fleetring_databases",
            &[("replica_role", "follower")]
        )?,
        Some(0.0)
    );
    assert_eq!(
        value(&text, "fleetring_members", &[("liveness", "live")])?,
        Some(1.0)
    );
    assert_eq!(value(&text, "fleetring_control_leader", &[])?, Some(1.0));
    assert!(value(&text, "fleetring_membership_epoch", &[])?.is_some_and(|epoch| epoch >= 1.0));
    daemon.run_until(async {}).await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn admin_tls_requires_a_listed_scraper() -> TestResult {
    let dir = tempfile::tempdir()?;
    let tls = Tls::create(dir.path())?;
    let daemon = both(
        dir.path(),
        &tls,
        "solo",
        "tls = true\nscrapers = [\"prometheus\"]\n",
    )
    .await?;
    let admin = admin_address(&daemon)?;
    let validity = Tls::validity();
    let anonymous = https_client(&tls, None)?;
    let ops = https_client(
        &tls,
        Some(&tls.authority().issue_client(&"ops".parse()?, validity)?),
    )?;
    let node = https_client(&tls, Some(&tls.issue_node("solo")?))?;
    let prometheus = https_client(
        &tls,
        Some(
            &tls.authority()
                .issue_client(&"prometheus".parse()?, validity)?,
        ),
    )?;

    // The probes answer anyone.
    let health = https(admin, &anonymous, "/healthz").await?;
    assert_eq!((health.status, health.body.as_str()), (200, "ok\n"));
    eventually("readiness over TLS", || async {
        Ok(https(admin, &anonymous, "/readyz").await?.status == 200)
    })
    .await?;

    // `/metrics` answers only a listed principal.
    let unauthenticated = https(admin, &anonymous, "/metrics").await?;
    assert_eq!(unauthenticated.status, 401);
    assert_eq!(unauthenticated.body, "a client certificate is required\n");
    let forbidden = https(admin, &ops, "/metrics").await?;
    assert_eq!(forbidden.status, 403);
    assert_eq!(forbidden.body, "'ops' may not read metrics\n");
    let node_refused = https(admin, &node, "/metrics").await?;
    assert_eq!(node_refused.status, 403);
    assert_eq!(node_refused.body, "'node:solo' may not read metrics\n");
    let served = https(admin, &prometheus, "/metrics").await?;
    assert_eq!(served.status, 200);
    assert_eq!(served.content_type.as_deref(), Some(OPENMETRICS));
    assert!(served.body.contains("# TYPE fleetring_build_info gauge"));
    assert!(served.body.ends_with("# EOF\n"));

    // A plaintext request gets no HTTP answer: the handshake fails.
    let mut plain = TcpStream::connect(admin).await?;
    plain
        .write_all(b"GET /metrics HTTP/1.1\r\nhost: 127.0.0.1\r\n\r\n")
        .await?;
    let mut answer = Vec::new();
    let _read = tokio::time::timeout(HANG_GUARD, plain.read_to_end(&mut answer)).await?;
    assert!(
        !answer.starts_with(b"HTTP/"),
        "{:?}",
        String::from_utf8_lossy(&answer)
    );

    daemon.run_until(async {}).await;
    Ok(())
}

/// How many connections the admin listener serves at once (`observe::admin::MAX_CONNECTIONS`).
const ADMIN_CONNECTIONS: usize = 64;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_admin_listener_closes_connections_over_its_limit() -> TestResult {
    let dir = tempfile::tempdir()?;
    let tls = Tls::create(dir.path())?;
    let daemon = both(dir.path(), &tls, "solo", "").await?;
    let admin = admin_address(&daemon)?;
    // Idle connections that send nothing hold every slot. The listener accepts in the order
    // clients connect, so each of these takes a slot before the next connection is accepted.
    let mut idle = Vec::with_capacity(ADMIN_CONNECTIONS);
    for _ in 0..ADMIN_CONNECTIONS {
        idle.push(TcpStream::connect(admin).await?);
    }
    // One more is closed as soon as it is accepted: it gets no HTTP answer.
    assert!(
        get(admin, "/healthz").await.is_err(),
        "a connection over the limit is closed"
    );
    // Once a slot frees, a probe is served again.
    drop(idle.pop());
    eventually("a probe within the limit", || async {
        Ok(get(admin, "/healthz")
            .await
            .is_ok_and(|reply| reply.status == 200))
    })
    .await?;
    drop(idle);
    daemon.run_until(async {}).await;
    Ok(())
}

#[tokio::test]
async fn admin_tls_needs_the_node_s_certificate() -> TestResult {
    let dir = tempfile::tempdir()?;
    let text = format!(
        "data_dir = {:?}\nlisten = \"127.0.0.1:0\"\ninsecure_plaintext = true\n\n[node]\nid = \"solo\"\n\n\
         [admin]\nlisten = \"127.0.0.1:0\"\ntls = true\n",
        dir.path().join("solo")
    );
    let refused = Daemon::start(Config::from_toml(&text)?)
        .await
        .expect_err("admin.tls needs [tls]");
    assert!(
        matches!(
            refused,
            StartError::Config(ConfigError::Missing { field: "tls", .. })
        ),
        "{refused}"
    );
    assert!(refused.to_string().contains("admin.tls"), "{refused}");
    Ok(())
}

#[tokio::test]
async fn a_non_loopback_admin_address_fails_validation() -> TestResult {
    let dir = tempfile::tempdir()?;
    let text = format!(
        "data_dir = {:?}\nlisten = \"127.0.0.1:0\"\ninsecure_plaintext = true\n\n[node]\nid = \"solo\"\n\n\
         [admin]\nlisten = \"0.0.0.0:7401\"\n",
        dir.path().join("solo")
    );
    let refused = Config::from_toml(&text).expect_err("not loopback");
    let message = refused.to_string();
    assert!(message.contains("admin.tls"), "{message}");
    assert!(message.contains("admin.allow_plaintext"), "{message}");
    Ok(())
}

#[tokio::test]
async fn a_disabled_admin_listener_still_keeps_metrics() -> TestResult {
    let dir = tempfile::tempdir()?;
    let tls = Tls::create(dir.path())?;
    let daemon = both(dir.path(), &tls, "solo", "enabled = false\n").await?;
    assert_eq!(daemon.admin_address(), None);
    assert!(
        daemon
            .metrics()
            .encode()
            .contains("# TYPE fleetring_writes counter")
    );
    daemon.run_until(async {}).await;
    Ok(())
}

// ---------------------------------------------------------------------------------------------
// RPCs, refusals, and request IDs.

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rpcs_and_refusals_are_counted() -> TestResult {
    let dir = tempfile::tempdir()?;
    let tls = Tls::create(dir.path())?;
    let daemon = both(dir.path(), &tls, "solo", "").await?;
    let address = daemon.listen_address().to_string();
    let missing = client(&daemon, &tls)?.locate(&name("missing")?).await;
    assert_eq!(
        missing.expect_err("no such database").code(),
        tonic::Code::NotFound
    );

    // A principal with no scope may not write, nor make a node's call.
    let nobody = tls.client_network("nobody")?;
    let refused = database_client(nobody.channel(&address)?)
        .execute(ExecuteRequest {
            database: "any".to_owned(),
            sql: "SELECT 1".to_owned(),
            params: Vec::new(),
        })
        .await
        .expect_err("no scope");
    assert_eq!(refused.code(), tonic::Code::PermissionDenied);
    let fenced = replication_client(nobody.channel(&address)?)
        .fence(FenceRequest {
            database: "any".to_owned(),
            epoch: 1,
            candidate: String::new(),
        })
        .await
        .expect_err("a principal can't fence");
    assert_eq!(fenced.code(), tonic::Code::PermissionDenied);

    let metrics = daemon.metrics();
    eventually_at_least(
        &daemon,
        "fleetring_rpc_requests_total",
        &[
            ("service", "control"),
            ("method", "Locate"),
            ("code", "not_found"),
        ],
        1.0,
    )
    .await?;
    eventually_at_least(
        &daemon,
        "fleetring_auth_refusals_total",
        &[("reason", "scope")],
        2.0,
    )
    .await?;
    eventually_at_least(
        &daemon,
        "fleetring_rpc_requests_total",
        &[
            ("service", "replication"),
            ("method", "Fence"),
            ("code", "permission_denied"),
        ],
        1.0,
    )
    .await?;
    eventually("nothing in flight", || async {
        let text = metrics.encode();
        let mut idle = true;
        for (service, method) in [
            ("control", "Locate"),
            ("database", "Execute"),
            ("replication", "Fence"),
        ] {
            let in_flight = value(
                &text,
                "fleetring_rpc_in_flight",
                &[("service", service), ("method", method)],
            )?;
            idle &= in_flight == Some(0.0);
        }
        Ok(idle)
    })
    .await?;
    let text = metrics.encode();
    assert!(
        value(
            &text,
            "fleetring_rpc_duration_seconds_count",
            &[("service", "control"), ("method", "Locate")]
        )?
        .is_some_and(|count| count >= 1.0)
    );
    daemon.run_until(async {}).await;
    Ok(())
}

// ---------------------------------------------------------------------------------------------
// Writes, queries, replication, leases, promotions, heartbeats, and change streams.

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn writes_and_queries_are_counted() -> TestResult {
    let dir = tempfile::tempdir()?;
    let tls = Tls::create(dir.path())?;
    let fleet = Fleet::start(dir.path(), &tls, 2).await?;
    let client = client(&fleet.control, &tls)?;
    let ledger = name("ledger")?;
    let location = client
        .create_database(&ledger, ReplicationFactor::new(2).ok_or("two")?)
        .await?;
    let primary = location.primary.clone().ok_or("no primary")?;
    let follower = location.followers().next().ok_or("no follower")?.clone();
    client
        .execute(&ledger, Statement::new("CREATE TABLE t (x)"))
        .await?;
    client
        .execute(&ledger, Statement::new("INSERT INTO t VALUES (1)"))
        .await?;
    client
        .execute(&ledger, Statement::new("INSERT INTO t VALUES (2)"))
        .await?;
    client
        .query(&ledger, Statement::new("SELECT x FROM t"))
        .await?;
    let redirect = database_client(
        tls.client_network("tester")?
            .channel(follower.address.as_deref().ok_or("no follower address")?)?,
    )
    .execute(ExecuteRequest {
        database: ledger.to_string(),
        sql: "INSERT INTO t VALUES (3)".to_owned(),
        params: Vec::new(),
    })
    .await
    .expect_err("a follower doesn't take writes");
    assert_eq!(redirect.code(), tonic::Code::Unavailable);

    let on_primary = fleet.data_daemon(primary.as_str())?;
    let on_follower = fleet.data_daemon(follower.node.as_str())?;
    eventually_at_least(
        on_primary,
        "fleetring_writes_total",
        &[("outcome", "acknowledged")],
        3.0,
    )
    .await?;
    eventually_at_least(
        on_primary,
        "fleetring_queries_total",
        &[("outcome", "ok")],
        1.0,
    )
    .await?;
    eventually_at_least(
        on_primary,
        "fleetring_replication_batches_sent_total",
        &[],
        3.0,
    )
    .await?;
    eventually_at_least(
        on_primary,
        "fleetring_replication_sent_bytes_total",
        &[],
        1.0,
    )
    .await?;
    eventually_at_least(
        on_follower,
        "fleetring_writes_total",
        &[("outcome", "redirected")],
        1.0,
    )
    .await?;
    eventually_at_least(
        on_follower,
        "fleetring_replication_batches_applied_total",
        &[],
        3.0,
    )
    .await?;
    let text = on_primary.metrics().encode();
    assert!(
        value(&text, "fleetring_write_duration_seconds_count", &[])?
            .is_some_and(|count| count >= 3.0)
    );
    eventually_at_least(
        on_primary,
        "fleetring_lease_renewals_total",
        &[("outcome", "renewed")],
        1.0,
    )
    .await?;
    fleet.stop().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_drain_counts_a_transfer_and_a_promotion() -> TestResult {
    let dir = tempfile::tempdir()?;
    let tls = Tls::create(dir.path())?;
    let fleet = Fleet::start(dir.path(), &tls, 2).await?;
    let client = client(&fleet.control, &tls)?;
    let ledger = name("ledger")?;
    let location = client
        .create_database(&ledger, ReplicationFactor::new(2).ok_or("two")?)
        .await?;
    let primary = location.primary.clone().ok_or("no primary")?;
    let follower = location
        .followers()
        .next()
        .ok_or("no follower")?
        .node
        .clone();
    client
        .execute(&ledger, Statement::new("CREATE TABLE t (x)"))
        .await?;
    client.drain_node(&primary).await?;
    let drained = fleet.data_daemon(primary.as_str())?;
    let promoted = fleet.data_daemon(follower.as_str())?;
    eventually_at_least(
        drained,
        "fleetring_lease_transfers_total",
        &[("outcome", "completed")],
        1.0,
    )
    .await?;
    eventually_at_least(
        promoted,
        "fleetring_promotions_total",
        &[("outcome", "completed")],
        1.0,
    )
    .await?;
    eventually_at_least(
        promoted,
        "fleetring_promotion_duration_seconds_count",
        &[],
        1.0,
    )
    .await?;
    fleet.stop().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn heartbeats_and_sink_deliveries_are_counted() -> TestResult {
    let dir = tempfile::tempdir()?;
    let tls = Tls::create(dir.path())?;
    let sink = format!(
        "\n[[changes.sink]]\nname = \"audit\"\nkind = \"file\"\ndatabases = [\"*\"]\npath = {:?}\n",
        dir.path().join("audit")
    );
    let daemon = both(dir.path(), &tls, "solo", &sink).await?;
    let text = daemon.metrics().encode();
    for outcome in ["delivered", "failed"] {
        assert_eq!(
            value(
                &text,
                "fleetring_change_deliveries_total",
                &[("sink", "audit"), ("outcome", outcome)]
            )?,
            Some(0.0),
            "every series of a configured sink starts at 0"
        );
    }
    let client = client(&daemon, &tls)?;
    let ledger = name("ledger")?;
    client
        .create_database(&ledger, ReplicationFactor::ONE)
        .await?;
    client
        .execute(&ledger, Statement::new("CREATE TABLE t (x)"))
        .await?;
    eventually_at_least(
        &daemon,
        "fleetring_heartbeats_total",
        &[("outcome", "recorded")],
        2.0,
    )
    .await?;
    eventually_at_least(
        &daemon,
        "fleetring_change_deliveries_total",
        &[("sink", "audit"), ("outcome", "delivered")],
        1.0,
    )
    .await?;
    eventually_at_least(
        &daemon,
        "fleetring_heartbeat_duration_seconds_count",
        &[],
        2.0,
    )
    .await?;
    eventually_at_least(
        &daemon,
        "fleetring_control_commands_total",
        &[("action", "create_database"), ("outcome", "applied")],
        1.0,
    )
    .await?;
    eventually_at_least(&daemon, "fleetring_control_log_index", &[], 1.0).await?;
    eventually("the sink's backlog sampled", || async {
        let text = daemon.metrics().encode();
        Ok(value(
            &text,
            "fleetring_change_backlog_batches",
            &[("sink", "audit")],
        )?
        .is_some()
            && value(
                &text,
                "fleetring_change_backlog_batches_max",
                &[("sink", "audit")],
            )?
            .is_some())
    })
    .await?;
    daemon.run_until(async {}).await;
    Ok(())
}

// ---------------------------------------------------------------------------------------------
// Cardinality, separate registries, and the README.

/// Creates `names` eight at a time and writes once to each.
async fn create_and_write(client: &Client, names: &[DatabaseName]) -> TestResult {
    for chunk in names.chunks(8) {
        let mut tasks = tokio::task::JoinSet::new();
        for name in chunk {
            let client = client.clone();
            let name = name.clone();
            tasks.spawn(async move {
                client
                    .create_database(&name, ReplicationFactor::ONE)
                    .await?;
                client
                    .execute(&name, Statement::new("CREATE TABLE t (x)"))
                    .await?;
                Ok::<(), tonic::Status>(())
            });
        }
        while let Some(joined) = tasks.join_next().await {
            joined??;
        }
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_hundred_databases_stay_within_the_series_budget() -> TestResult {
    let dir = tempfile::tempdir()?;
    let tls = Tls::create(dir.path())?;
    let daemon = both(dir.path(), &tls, "solo", "").await?;
    let client = client(&daemon, &tls)?;
    let names = (1..=200)
        .map(|k| name(&format!("tenant:t{k}/db")))
        .collect::<Result<Vec<_>, _>>()?;
    create_and_write(&client, &names).await?;
    eventually_at_least(
        &daemon,
        "fleetring_databases",
        &[("replica_role", "primary")],
        200.0,
    )
    .await?;

    let text = get(admin_address(&daemon)?, "/metrics").await?.body;
    let samples = samples(&text)?;
    assert!(samples.len() < 1_500, "{} series", samples.len());
    let database_names: BTreeSet<String> = names.iter().map(ToString::to_string).collect();
    let families: Vec<(String, &str)> = FAMILIES
        .iter()
        .map(|family| (family.exposed_name(), family.name))
        .collect();
    for sample in &samples {
        // A histogram's samples carry suffixes; a counter's is its exposed name.
        let family = families
            .iter()
            .find(|(exposed, _)| {
                sample.name == *exposed
                    || ["_bucket", "_sum", "_count"]
                        .iter()
                        .any(|suffix| sample.name == format!("{exposed}{suffix}"))
            })
            .map(|(_, registered)| *registered)
            .ok_or(format!("{} is in no family", sample.name))?;
        for (label, value) in &sample.labels {
            assert!(
                database_names.iter().all(|name| name != value) && !value.contains("tenant:"),
                "{} has a database name as a label value: {value}",
                sample.name
            );
            if label == "le" {
                continue;
            }
            assert!(
                ALLOWED_LABELS.contains(&label.as_str()),
                "{} uses label {label}",
                sample.name
            );
            if let Some(closed) = closed_values(family, label) {
                assert!(
                    closed.contains(&value.as_str()),
                    "{} {label}={value} is outside its closed set",
                    sample.name
                );
            }
        }
    }
    daemon.run_until(async {}).await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn two_daemons_in_one_process_keep_separate_metrics() -> TestResult {
    let dir = tempfile::tempdir()?;
    let tls = Tls::create(dir.path())?;
    let first = both(&dir.path().join("first"), &tls, "first", "").await?;
    let second = both(&dir.path().join("second"), &tls, "second", "").await?;
    let client = client(&first, &tls)?;
    let ledger = name("ledger")?;
    client
        .create_database(&ledger, ReplicationFactor::ONE)
        .await?;
    for statement in [
        "CREATE TABLE t (x)",
        "INSERT INTO t VALUES (1)",
        "INSERT INTO t VALUES (2)",
    ] {
        client.execute(&ledger, Statement::new(statement)).await?;
    }
    eventually_at_least(
        &first,
        "fleetring_writes_total",
        &[("outcome", "acknowledged")],
        3.0,
    )
    .await?;
    let text = get(admin_address(&second)?, "/metrics").await?.body;
    assert_eq!(
        value(
            &text,
            "fleetring_writes_total",
            &[("outcome", "acknowledged")]
        )?,
        Some(0.0)
    );
    assert!(text.contains("node_id=\"second\""), "{text}");
    first.run_until(async {}).await;
    second.run_until(async {}).await;
    Ok(())
}

#[test]
fn the_readme_lists_every_family() -> TestResult {
    let readme = std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/README.md"))?;
    for family in FAMILIES {
        let exposed = family.exposed_name();
        assert!(
            readme.contains(&format!("`{exposed}`")),
            "the README doesn't list {exposed}"
        );
        if family.kind == Kind::Histogram {
            assert!(
                exposed.ends_with("_seconds") || exposed.ends_with("_bytes"),
                "{exposed}"
            );
        }
    }
    Ok(())
}

#[test]
fn the_daemon_writes_json_lines_with_log_format_json() -> TestResult {
    let dir = tempfile::tempdir()?;
    let tls = Tls::create(dir.path())?;
    let path = dir.path().join("node.toml");
    std::fs::File::create(&path)?
        .write_all(config(dir.path(), &tls, "solo", "both", None, "")?.as_bytes())?;
    for (flag, variable) in [(Some("json"), None), (None, Some("json"))] {
        let mut command = crate::support::fleetring();
        command
            .args(["serve", "--config"])
            .arg(&path)
            .env("RUST_LOG", "info")
            .env_remove("FLEETRING_LOG_FORMAT")
            .stderr(std::process::Stdio::piped());
        if let Some(flag) = flag {
            command.args(["--log-format", flag]);
        }
        if let Some(variable) = variable {
            command.env("FLEETRING_LOG_FORMAT", variable);
        }
        let mut daemon = KillOnDrop(command.spawn()?);
        let stderr = daemon.0.stderr.take().ok_or("no stderr")?;
        let mut started = None;
        for line in std::io::BufRead::lines(std::io::BufReader::new(stderr)) {
            let line = line?;
            let object: serde_json::Value = serde_json::from_str(&line)
                .map_err(|error| format!("{error}: not one JSON object: {line}"))?;
            if object["fields"]["message"] == serde_json::json!("fleetringd started") {
                started = Some(object);
                break;
            }
        }
        let started = started.ok_or("the daemon never logged its start")?;
        assert_eq!(started["fields"]["node"], serde_json::json!("solo"));
        let admin = started["fields"]["admin"]
            .as_str()
            .ok_or("no admin address")?;
        assert!(admin.parse::<SocketAddr>().is_ok(), "{started}");
        drop(daemon);
    }
    let refused = crate::support::fleetring()
        .args(["serve", "--config"])
        .arg(&path)
        .env("FLEETRING_LOG_FORMAT", "xml")
        .output()?;
    assert_eq!(refused.status.code(), Some(2));
    let stderr = String::from_utf8_lossy(&refused.stderr);
    assert!(
        stderr.contains("invalid FLEETRING_LOG_FORMAT 'xml'; use text or json"),
        "{stderr}"
    );
    Ok(())
}

/// A child process killed when dropped, so a failed test leaves none behind.
struct KillOnDrop(std::process::Child);

impl Drop for KillOnDrop {
    fn drop(&mut self) {
        let _killed = self.0.kill();
        let _reaped = self.0.wait();
    }
}

// ---------------------------------------------------------------------------------------------
// A measurement of the request path, for docs/perf.md.

/// Calls per measured case, split across its callers.
const CALLS: usize = 4_000;

fn percentile(sorted: &[Duration], per_mille: usize) -> Duration {
    let index = (sorted.len() * per_mille / 1_000).min(sorted.len().saturating_sub(1));
    sorted.get(index).copied().unwrap_or_default()
}

/// The latency and throughput of `Database.Execute` (one small replicated-to-itself write, with
/// its fsync) and `Control.Locate` (no storage: the request path alone) through a running daemon
/// over TLS, with the daemon's info-level log subscriber installed, as in production.
///
/// ```sh
/// cargo test -p fleetringd --release --test observe -- --ignored --nocapture rpc_latency
/// ```
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "a measurement, not a check; run it in release mode"]
async fn rpc_latency_through_the_daemon() -> TestResult {
    let _installed = tracing::subscriber::set_global_default(log_subscriber(
        LogFormat::Text,
        LogColour::Plain,
        tracing_subscriber::EnvFilter::new("info"),
        std::io::sink,
    ));
    let dir = tempfile::tempdir()?;
    let tls = Tls::create(dir.path())?;
    let text = format!(
        "data_dir = {:?}\nlisten = \"127.0.0.1:0\"\n\n[node]\nid = \"solo\"\n{}\n[admin]\nlisten = \"127.0.0.1:0\"\n",
        dir.path().join("solo"),
        tls.node_tls_table("solo")?
    );
    let daemon = Daemon::start(Config::from_toml(&text)?).await?;
    let membership = daemon.membership().ok_or("no membership")?;
    tokio::time::timeout(
        HANG_GUARD,
        membership.wait_for(|view| view.ring().is_some()),
    )
    .await?
    .ok_or("membership stopped")?;
    let control = daemon
        .control_address()
        .ok_or("no control address")?
        .to_string();
    let network = tls.client_network("tester")?;
    let client = Client::new(control.clone(), network.clone());
    let ledger: DatabaseName = "ledger".parse()?;
    client
        .create_database(&ledger, ReplicationFactor::ONE)
        .await?;
    client
        .execute(
            &ledger,
            Statement::new("CREATE TABLE kv (k INTEGER PRIMARY KEY, v BLOB)"),
        )
        .await?;
    let channel = network.channel(&daemon.listen_address().to_string())?;
    println!("| RPC | Callers | Calls | Throughput (calls/s) | p50 | p99 |");
    println!("|---|---|---|---|---|---|");
    for rpc in ["Locate", "Execute"] {
        for callers in [1_usize, 16] {
            let per_caller = CALLS / callers;
            let started = tokio::time::Instant::now();
            let mut tasks = tokio::task::JoinSet::new();
            for caller in 0..callers {
                let database = database_client(channel.clone());
                let control = fleetringd::transport::control_client(channel.clone());
                let ledger = ledger.to_string();
                tasks.spawn(async move {
                    let (mut database, mut control) = (database, control);
                    let mut latencies = Vec::with_capacity(per_caller);
                    for i in 0..per_caller {
                        let begun = tokio::time::Instant::now();
                        if rpc == "Locate" {
                            control
                                .locate(fleetring_proto::v1::LocateRequest {
                                    database: ledger.clone(),
                                })
                                .await?;
                        } else {
                            database
                                .execute(ExecuteRequest {
                                    database: ledger.clone(),
                                    sql: format!(
                                        "INSERT OR REPLACE INTO kv VALUES ({}, zeroblob(64))",
                                        caller * 1_000_000 + i
                                    ),
                                    params: Vec::new(),
                                })
                                .await?;
                        }
                        latencies.push(begun.elapsed());
                    }
                    Ok::<_, tonic::Status>(latencies)
                });
            }
            let mut latencies = Vec::new();
            while let Some(done) = tasks.join_next().await {
                latencies.extend(done??);
            }
            let elapsed = started.elapsed();
            latencies.sort();
            let count = latencies.len();
            let rate = u128::try_from(count)? * 1_000_000 / elapsed.as_micros().max(1);
            println!(
                "| {rpc} | {callers} | {count} | {rate} | {:.0?} | {:.0?} |",
                percentile(&latencies, 500),
                percentile(&latencies, 990),
            );
        }
    }
    daemon.run_until(async {}).await;
    Ok(())
}
