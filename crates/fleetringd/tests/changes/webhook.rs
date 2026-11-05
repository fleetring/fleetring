//! The webhook sink delivers logical row changes, with the database name, position, and epoch,
//! to an HTTP or HTTPS server, and retries on 5xx and on a TLS failure (FR-073).

use std::convert::Infallible;
use std::error::Error;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use fleetringd::changes::{
    ChangeRecord, ClientCertificate, ConfiguredSink, Format, RecordContent, TokenSource,
    WebhookConfigError, WebhookSecurity, WebhookSettings, WebhookSink,
};
use fleetringd::durability::HttpPolicy;
use fleetringd::engine::{RowOperation, Value};
use http_body_util::{BodyExt as _, Full};
use hyper::body::{Bytes, Incoming};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use tokio::net::TcpListener;

use crate::common::{Harness, Node, TestResult, eventually, every_database, insert, name};
use crate::file::inserted;
use crate::receiver::{ClientAuth, Receiver, Seen};
use crate::support::Tls;

/// The security of a webhook to a plain `http://` test server.
fn plaintext() -> WebhookSecurity {
    WebhookSecurity {
        http: HttpPolicy::AllowHttp,
        ..WebhookSecurity::default()
    }
}

/// What the test server saw: every request's body with the status it answered.
#[derive(Debug, Default)]
struct Received {
    requests: Vec<(StatusCode, String)>,
}

/// Serves on a loopback port, answering 500 to the first `failures` requests and 200 after.
async fn serve(
    failures: usize,
) -> Result<(String, Arc<Mutex<Received>>), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let url = format!("http://{}/changes", listener.local_addr()?);
    let received = Arc::new(Mutex::new(Received::default()));
    let seen = Arc::clone(&received);
    tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                return;
            };
            let seen = Arc::clone(&seen);
            let service = service_fn(move |request: Request<Incoming>| {
                let seen = Arc::clone(&seen);
                async move {
                    let content_type = request
                        .headers()
                        .get("content-type")
                        .and_then(|value| value.to_str().ok())
                        .unwrap_or_default()
                        .to_owned();
                    let body = request
                        .into_body()
                        .collect()
                        .await
                        .map(|collected| {
                            String::from_utf8_lossy(&collected.to_bytes()).into_owned()
                        })
                        .unwrap_or_default();
                    let status = {
                        let mut seen = seen.lock().expect("server lock");
                        let status = if seen.requests.len() < failures
                            || content_type != "application/json"
                        {
                            StatusCode::INTERNAL_SERVER_ERROR
                        } else {
                            StatusCode::OK
                        };
                        seen.requests.push((status, body));
                        status
                    };
                    Ok::<_, Infallible>(
                        Response::builder()
                            .status(status)
                            .body(Full::new(Bytes::new()))
                            .expect("a response"),
                    )
                }
            });
            tokio::spawn(async move {
                let _served = http1::Builder::new()
                    .serve_connection(TokioIo::new(stream), service)
                    .await;
            });
        }
    });
    Ok((url, received))
}

#[tokio::test(flavor = "multi_thread")]
async fn the_webhook_sink_posts_logical_changes_and_retries_on_5xx() -> TestResult {
    let harness = Harness::start().await?;
    let (url, received) = serve(2).await?;
    let sink = WebhookSink::new(
        "hooks",
        &url,
        WebhookSettings {
            timeout: Duration::from_secs(5),
            retries: 5,
        },
        &plaintext(),
    )?;
    let node = harness
        .node(vec![ConfiguredSink::new(
            sink,
            Format::Logical,
            every_database(),
        )])
        .await?;
    let ledger = name("ledger");
    node.create(&ledger, "CREATE TABLE t (value INTEGER NOT NULL)")
        .await?;
    node.await_caught_up().await?;
    for value in 1..=3 {
        node.execute(&ledger, insert(value)).await?;
        node.await_caught_up().await?;
    }
    eventually("the receiver taking every change", || {
        let seen = received.lock().expect("server lock");
        seen.requests
            .iter()
            .filter(|(status, _)| status.is_success())
            .count()
            >= 4
    })
    .await?;
    let requests = received.lock().expect("server lock").requests.clone();
    let (failed, ok): (Vec<_>, Vec<_>) = requests
        .iter()
        .partition(|(status, _)| status.is_server_error());
    assert_eq!(
        failed.len(),
        2,
        "the first two requests failed and were retried"
    );
    assert_eq!(
        failed[0].1, ok[0].1,
        "the retried request carried the same change"
    );
    let records: Vec<ChangeRecord> = ok
        .iter()
        .map(|(_, body)| serde_json::from_str(body))
        .collect::<Result<_, _>>()?;
    let mut values = Vec::new();
    for record in &records {
        assert_eq!(record.database, "ledger");
        assert_eq!(record.epoch, 1);
        assert!(!record.position.is_empty());
        let RecordContent::Logical { rows } = &record.content else {
            panic!("a logical sink: {record:?}");
        };
        for row in rows {
            assert_eq!(
                (row.table.as_str(), row.operation),
                ("t", RowOperation::Insert)
            );
            values.push(row.values()[0].value.clone());
        }
    }
    assert_eq!(
        values,
        vec![Value::Integer(1), Value::Integer(2), Value::Integer(3)]
    );
    for pair in records.windows(2) {
        assert_eq!(pair[1].start.as_deref(), Some(pair[0].position.as_str()));
    }
    node.kill().await;
    Ok(())
}

#[test]
fn a_webhook_needs_an_http_or_https_url_with_a_host() {
    let allowed = plaintext();
    for url in [
        "ftp://example.com/x",
        "/relative",
        "http:///nohost",
        "not a url",
    ] {
        for security in [&allowed, &WebhookSecurity::default()] {
            assert!(
                WebhookSink::new("hooks", url, WebhookSettings::new(), security).is_err(),
                "{url}"
            );
        }
    }
    assert!(
        WebhookSink::new(
            "hooks",
            "http://127.0.0.1:1/x",
            WebhookSettings::new(),
            &allowed
        )
        .is_ok()
    );
    assert!(
        WebhookSink::new(
            "hooks",
            "https://example.com/x",
            WebhookSettings::new(),
            &WebhookSecurity::default()
        )
        .is_ok()
    );
}

#[test]
fn plain_http_needs_allow_http() {
    let error = WebhookSink::new(
        "hooks",
        "http://127.0.0.1:1/x",
        WebhookSettings::new(),
        &WebhookSecurity::default(),
    )
    .expect_err("http without allow_http");
    assert!(
        matches!(error, WebhookConfigError::HttpNotAllowed { .. }),
        "{error}"
    );
    assert!(error.to_string().contains("allow_http"), "{error}");
}

/// A test authority and the directory its files are in.
struct Pki {
    _dir: tempfile::TempDir,
    tls: Tls,
}

impl Pki {
    fn new() -> Result<Self, Box<dyn Error>> {
        let dir = tempfile::tempdir()?;
        let tls = Tls::create(dir.path())?;
        Ok(Self { _dir: dir, tls })
    }

    /// The authority's certificate, as a webhook's `ca`.
    fn ca(&self) -> PathBuf {
        self.tls.dir().join("ca.crt")
    }

    /// A receiver presenting a certificate this authority issued for loopback.
    async fn receiver(&self, client_auth: ClientAuth) -> Result<Receiver, Box<dyn Error>> {
        Receiver::start(
            &self.tls.issue_node("receiver")?,
            self.tls.authority().certificate_pem(),
            client_auth,
        )
        .await
    }

    /// A client certificate for `principal`: its certificate and key files.
    fn client(&self, principal: &str) -> Result<ClientCertificate, Box<dyn Error>> {
        let issued = self
            .tls
            .authority()
            .issue_client(&principal.parse()?, Tls::validity())?;
        issued.write(self.tls.dir(), principal)?;
        Ok(ClientCertificate {
            certificate: self.tls.dir().join(format!("{principal}.crt")),
            key: self.tls.dir().join(format!("{principal}.key")),
        })
    }

    /// A file only its owner may read, holding `text`.
    fn secret(&self, name: &str, text: &str) -> Result<PathBuf, Box<dyn Error>> {
        write_private(&self.tls.dir().join(name), text)
    }
}

fn write_private(path: &Path, text: &str) -> Result<PathBuf, Box<dyn Error>> {
    use std::io::Write as _;
    #[cfg(unix)]
    use std::os::unix::fs::OpenOptionsExt as _;
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    options.mode(0o600);
    options.open(path)?.write_all(text.as_bytes())?;
    Ok(path.to_owned())
}

/// A node streaming every database to `url` through a sink named `hooks`.
async fn node_posting_to(
    harness: &Harness,
    url: &str,
    retries: u32,
    security: &WebhookSecurity,
) -> Result<Node, Box<dyn Error>> {
    let sink = WebhookSink::new(
        "hooks",
        url,
        WebhookSettings {
            timeout: Duration::from_secs(5),
            retries,
        },
        security,
    )?;
    harness
        .node(vec![ConfiguredSink::new(
            sink,
            Format::Logical,
            every_database(),
        )])
        .await
}

/// Creates a database through `node` and writes 1, 2, and 3 to it, without waiting for the sink.
async fn write_three(node: &Node) -> TestResult {
    let ledger = name("ledger");
    node.create(&ledger, "CREATE TABLE t (value INTEGER NOT NULL)")
        .await?;
    for value in 1..=3 {
        node.execute(&ledger, insert(value)).await?;
    }
    Ok(())
}

/// The values the receiver was sent, in order.
fn delivered(seen: &[Seen]) -> Result<Vec<i64>, Box<dyn Error>> {
    let records: Vec<ChangeRecord> = seen
        .iter()
        .map(|request| serde_json::from_str(&request.body))
        .collect::<Result<_, _>>()?;
    Ok(inserted(&records))
}

#[tokio::test(flavor = "multi_thread")]
async fn an_https_webhook_delivers_to_a_receiver_signed_by_its_ca() -> TestResult {
    let harness = Harness::start().await?;
    let pki = Pki::new()?;
    let receiver = pki.receiver(ClientAuth::None).await?;
    let node = node_posting_to(
        &harness,
        receiver.url(),
        5,
        &WebhookSecurity {
            ca: Some(pki.ca()),
            ..WebhookSecurity::default()
        },
    )
    .await?;
    write_three(&node).await?;
    node.await_caught_up().await?;
    let seen = receiver.seen();
    assert_eq!(delivered(&seen)?, vec![1, 2, 3]);
    assert!(
        seen.iter()
            .all(|request| request.authorization.is_none() && request.client.is_none()),
        "no token and no client certificate were configured: {seen:?}"
    );
    node.kill().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn an_untrusted_certificate_is_refused_and_the_batch_is_retried_not_dropped() -> TestResult {
    let harness = Harness::start().await?;
    let trusted = Pki::new()?;
    let stranger = Pki::new()?;
    // The receiver presents a certificate from an authority the sink doesn't trust.
    let receiver = stranger.receiver(ClientAuth::None).await?;
    let node = node_posting_to(
        &harness,
        receiver.url(),
        1,
        &WebhookSecurity {
            ca: Some(trusted.ca()),
            ..WebhookSecurity::default()
        },
    )
    .await?;
    write_three(&node).await?;
    eventually("the sink being refused again and again", || {
        receiver.refused() >= 3
    })
    .await?;
    assert!(
        receiver.seen().is_empty(),
        "nothing is delivered over an untrusted connection"
    );
    let statuses = node.streams.status();
    assert_eq!(statuses.len(), 1, "{statuses:?}");
    assert_eq!(
        statuses[0].position, None,
        "the sink's position didn't advance: {statuses:?}"
    );

    // The receiver now presents a certificate the sink trusts: the same batches arrive, from the
    // first one.
    receiver.present(&trusted.tls.issue_node("receiver")?)?;
    node.await_caught_up().await?;
    assert_eq!(delivered(&receiver.seen())?, vec![1, 2, 3]);
    let statuses = node.streams.status();
    assert!(
        statuses[0].position.is_some() && statuses[0].position == statuses[0].acknowledged,
        "{statuses:?}"
    );
    node.kill().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_client_certificate_authenticates_the_webhook_to_a_receiver_that_requires_one()
-> TestResult {
    let harness = Harness::start().await?;
    let pki = Pki::new()?;
    let receiver = pki.receiver(ClientAuth::Required).await?;
    let node = node_posting_to(
        &harness,
        receiver.url(),
        5,
        &WebhookSecurity {
            ca: Some(pki.ca()),
            client_certificate: Some(pki.client("hooker")?),
            ..WebhookSecurity::default()
        },
    )
    .await?;
    write_three(&node).await?;
    node.await_caught_up().await?;
    let seen = receiver.seen();
    assert_eq!(delivered(&seen)?, vec![1, 2, 3]);
    let subject = format!("fleetring://{}/principal/hooker", crate::support::FLEET);
    assert!(
        seen.iter()
            .all(|request| request.client.as_deref() == Some(subject.as_str())),
        "the receiver saw the client certificate: {seen:?}"
    );
    node.kill().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_receiver_that_requires_a_client_certificate_refuses_a_webhook_without_one() -> TestResult
{
    let harness = Harness::start().await?;
    let pki = Pki::new()?;
    let receiver = pki.receiver(ClientAuth::Required).await?;
    let node = node_posting_to(
        &harness,
        receiver.url(),
        0,
        &WebhookSecurity {
            ca: Some(pki.ca()),
            ..WebhookSecurity::default()
        },
    )
    .await?;
    write_three(&node).await?;
    eventually("the receiver refusing the sink", || receiver.refused() >= 2).await?;
    assert!(receiver.seen().is_empty(), "{:?}", receiver.seen());
    node.kill().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_bearer_token_from_a_file_is_sent_with_every_request() -> TestResult {
    let harness = Harness::start().await?;
    let pki = Pki::new()?;
    let receiver = pki.receiver(ClientAuth::None).await?;
    // A trailing newline, as editors and `echo` leave, is not part of the token.
    let token_file = pki.secret("hooks.token", "file-token-4711\n")?;
    let node = node_posting_to(
        &harness,
        receiver.url(),
        5,
        &WebhookSecurity {
            ca: Some(pki.ca()),
            token: Some(TokenSource::File(token_file)),
            ..WebhookSecurity::default()
        },
    )
    .await?;
    write_three(&node).await?;
    node.await_caught_up().await?;
    let seen = receiver.seen();
    assert_eq!(delivered(&seen)?, vec![1, 2, 3]);
    assert!(
        seen.iter()
            .all(|request| request.authorization.as_deref() == Some("Bearer file-token-4711")),
        "{seen:?}"
    );
    node.kill().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_bearer_token_from_the_environment_is_sent_with_every_request() -> TestResult {
    // Cargo sets this in the environment of every test it runs. A test can't set a variable
    // itself, since the process is multi-threaded.
    const VARIABLE: &str = "CARGO_PKG_NAME";
    let expected = std::env::var(VARIABLE).map_err(|_| format!("run under cargo: {VARIABLE}"))?;
    let harness = Harness::start().await?;
    let pki = Pki::new()?;
    let receiver = pki.receiver(ClientAuth::None).await?;
    let node = node_posting_to(
        &harness,
        receiver.url(),
        5,
        &WebhookSecurity {
            ca: Some(pki.ca()),
            token: Some(TokenSource::Environment(VARIABLE.to_owned())),
            ..WebhookSecurity::default()
        },
    )
    .await?;
    write_three(&node).await?;
    node.await_caught_up().await?;
    let seen = receiver.seen();
    assert_eq!(delivered(&seen)?, vec![1, 2, 3]);
    let want = format!("Bearer {expected}");
    assert!(
        seen.iter()
            .all(|request| request.authorization.as_deref() == Some(want.as_str())),
        "{seen:?}"
    );
    node.kill().await;
    Ok(())
}

#[test]
fn a_token_that_is_missing_empty_or_shared_is_refused() -> TestResult {
    let pki = Pki::new()?;
    let build = |token| {
        WebhookSink::new(
            "hooks",
            "https://127.0.0.1:1/x",
            WebhookSettings::new(),
            &WebhookSecurity {
                token: Some(token),
                ..WebhookSecurity::default()
            },
        )
    };
    let error = build(TokenSource::Environment(
        "FLEETRING_NO_SUCH_TOKEN".to_owned(),
    ))
    .expect_err("an unset variable");
    assert!(
        matches!(error, WebhookConfigError::Token { .. })
            && error.to_string().contains("FLEETRING_NO_SUCH_TOKEN"),
        "{error}"
    );
    let error =
        build(TokenSource::File(pki.tls.dir().join("missing.token"))).expect_err("a missing file");
    assert!(matches!(error, WebhookConfigError::Token { .. }), "{error}");
    let empty = pki.secret("empty.token", " \n")?;
    let error = build(TokenSource::File(empty)).expect_err("an empty file");
    assert!(error.to_string().contains("empty"), "{error}");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        let shared = pki.secret("shared.token", "token")?;
        std::fs::set_permissions(&shared, std::fs::Permissions::from_mode(0o644))?;
        let error = build(TokenSource::File(shared)).expect_err("a world-readable file");
        assert!(
            matches!(error, WebhookConfigError::TokenShared { .. }),
            "{error}"
        );
    }
    Ok(())
}

#[test]
fn a_client_certificate_that_cannot_be_read_is_refused() -> TestResult {
    let pki = Pki::new()?;
    let mut client = pki.client("hooker")?;
    let build = |client: &ClientCertificate| {
        WebhookSink::new(
            "hooks",
            "https://127.0.0.1:1/x",
            WebhookSettings::new(),
            &WebhookSecurity {
                client_certificate: Some(client.clone()),
                ..WebhookSecurity::default()
            },
        )
    };
    assert!(build(&client).is_ok());
    std::mem::swap(&mut client.key, &mut client.certificate);
    let error = build(&client).expect_err("a certificate where the key goes");
    assert!(matches!(error, WebhookConfigError::Tls { .. }), "{error}");
    client.key = pki.tls.dir().join("missing.key");
    assert!(build(&client).is_err());
    Ok(())
}

#[test]
fn the_token_and_the_key_stay_out_of_debug_output() -> TestResult {
    let pki = Pki::new()?;
    let token = "debug-token-9d1f";
    let token_file = pki.secret("hooks.token", token)?;
    let client = pki.client("hooker")?;
    let key = std::fs::read_to_string(&client.key)?;
    let key_body: String = key
        .lines()
        .filter(|line| !line.starts_with("-----"))
        .collect();
    let security = WebhookSecurity {
        ca: Some(pki.ca()),
        client_certificate: Some(client),
        token: Some(TokenSource::File(token_file.clone())),
        ..WebhookSecurity::default()
    };
    let sink = WebhookSink::new(
        "hooks",
        "https://127.0.0.1:1/x",
        WebhookSettings::new(),
        &security,
    )?;
    let config = fleetringd::Config::from_toml(&format!(
        "data_dir = \"/tmp/x\"\nlisten = \"127.0.0.1:7400\"\n[node]\nid = \"n\"\n\
         [[changes.sink]]\nname = \"hooks\"\nkind = \"webhook\"\ndatabases = [\"*\"]\n\
         url = \"https://127.0.0.1:1/x\"\ntoken_file = {token_file:?}\n"
    ))?;
    let text = format!("{sink:?} {security:?} {config:?}");
    for secret in [token, key_body.as_str(), "Bearer"] {
        assert!(!text.contains(secret), "{secret} appears in {text}");
    }
    Ok(())
}
