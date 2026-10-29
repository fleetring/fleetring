//! The webhook sink delivers logical row changes, with the database name, position, and epoch,
//! to an HTTP server, and retries on 5xx.

use std::convert::Infallible;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use fleetringd::changes::{
    ChangeRecord, ConfiguredSink, Format, RecordContent, WebhookSettings, WebhookSink,
};
use fleetringd::engine::{RowOperation, Value};
use http_body_util::{BodyExt as _, Full};
use hyper::body::{Bytes, Incoming};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use tokio::net::TcpListener;

use crate::common::{Harness, TestResult, eventually, every_database, insert, name};

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
            values.push(row.values[0].value.clone());
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
fn a_webhook_needs_an_http_url_with_a_host() {
    for url in [
        "https://example.com/x",
        "/relative",
        "http:///nohost",
        "not a url",
    ] {
        assert!(
            WebhookSink::new("hooks", url, WebhookSettings::new()).is_err(),
            "{url}"
        );
    }
    assert!(WebhookSink::new("hooks", "http://127.0.0.1:1/x", WebhookSettings::new()).is_ok());
}
