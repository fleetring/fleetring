//! Hrana over HTTP through the router: pipelines, transactions, batches, cursors, the version
//! probes, authentication, batons, failover by drain, and the bounds on idle streams and open
//! transactions.

use std::time::Duration;

use fleetringd::engine::{Statement, Value as SqlValue};
use fleetringd::transport::Credentials;
use http_body_util::Full;
use hyper::body::Bytes;
use hyper::{Method, StatusCode};
use hyper_util::client::legacy::Client as HttpClient;
use hyper_util::rt::TokioExecutor;
use serde_json::{Value, json};
use tonic::Code;

use crate::fleet::{
    Fleet, Options, TestResult, close, execute, integer, issue, pipeline, send, start_router, text,
};
use crate::support::Tls;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_pipeline_runs_and_closes() -> TestResult {
    let fleet = Fleet::start(Options::default()).await?;
    fleet.create("notes", 3).await?;
    let token = fleet.token("app", &["write:notes"], "t-app")?;
    let answer = fleet
        .post(
            "/db/notes/v3/pipeline",
            Some(&token),
            &pipeline(
                None,
                json!([
                    execute(
                        "INSERT INTO t (v) VALUES (?) RETURNING id",
                        json!([text("a")])
                    ),
                    execute("SELECT id, v FROM t", json!([])),
                    close(),
                ]),
            ),
        )
        .await?;
    assert_eq!(answer.status, StatusCode::OK, "{}", answer.text);
    let inserted = &answer.result(0)["response"]["result"];
    assert_eq!(inserted["rows"], json!([[integer(1)]]), "{}", answer.text);
    assert_eq!(inserted["affected_row_count"], 1);
    assert_eq!(inserted["last_insert_rowid"], "1");
    let selected = &answer.result(1)["response"]["result"];
    assert_eq!(selected["cols"][1]["name"], "v");
    assert_eq!(selected["rows"], json!([[integer(1), text("a")]]));
    assert_eq!(answer.result(2)["response"]["type"], "close");
    assert_eq!(answer.baton(), None, "a closed stream has no baton");
    let primary = fleet.primary("notes").await?;
    assert_eq!(
        answer
            .headers
            .get("fleetring-primary")
            .and_then(|value| value.to_str().ok()),
        Some(primary.as_str()),
        "the commit's headers name the primary"
    );
    assert_eq!(
        answer
            .headers
            .get("fleetring-replicas")
            .and_then(|value| value.to_str().ok()),
        Some("3")
    );
    let acknowledged: u32 = answer
        .headers
        .get("fleetring-acknowledged-by")
        .ok_or("no acknowledgement header")?
        .to_str()?
        .parse()?;
    assert!(
        acknowledged >= 2,
        "a write quorum of three holds it: {acknowledged}"
    );
    fleet.stop().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_transaction_spans_requests_and_rolls_back_or_commits() -> TestResult {
    let fleet = Fleet::start(Options::default()).await?;
    fleet.create("ledger", 3).await?;
    let token = fleet.token("app", &["write:ledger"], "t-app")?;
    let path = "/db/ledger/v3/pipeline";
    // A transaction that rolls back leaves nothing.
    let first = fleet
        .post(
            path,
            Some(&token),
            &pipeline(
                None,
                json!([
                    execute("BEGIN", json!([])),
                    execute("INSERT INTO t (v) VALUES ('gone')", json!([]))
                ]),
            ),
        )
        .await?;
    assert_eq!(first.status, StatusCode::OK, "{}", first.text);
    let baton = first.baton().ok_or("a baton")?;
    let rolled_back = fleet
        .post(
            path,
            Some(&token),
            &pipeline(
                Some(&baton),
                json!([execute("ROLLBACK", json!([])), {"type": "get_autocommit"}]),
            ),
        )
        .await?;
    assert_eq!(
        rolled_back.result(1)["response"]["is_autocommit"],
        true,
        "{}",
        rolled_back.text
    );
    let baton = rolled_back.baton().ok_or("a baton")?;
    // Then one that commits, across two requests.
    let began = fleet
        .post(
            path,
            Some(&token),
            &pipeline(
                Some(&baton),
                json!([
                    execute("BEGIN", json!([])),
                    execute("INSERT INTO t (v) VALUES ('a')", json!([]))
                ]),
            ),
        )
        .await?;
    assert_eq!(began.result(1)["type"], "ok", "{}", began.text);
    assert!(
        fleet.values("ledger").await?.is_empty(),
        "nothing committed yet"
    );
    let baton = began.baton().ok_or("a baton")?;
    let committed = fleet
        .post(
            path,
            Some(&token),
            &pipeline(
                Some(&baton),
                json!([
                    execute("INSERT INTO t (v) VALUES ('b')", json!([])),
                    execute("COMMIT", json!([])),
                    close(),
                ]),
            ),
        )
        .await?;
    assert_eq!(committed.result(1)["type"], "ok", "{}", committed.text);
    assert!(committed.headers.contains_key("fleetring-epoch"));
    assert_eq!(
        fleet.values("ledger").await?,
        [SqlValue::from("a"), SqlValue::from("b")]
    );
    // The baton was used: sending it again is refused.
    let replayed = fleet
        .post(
            path,
            Some(&token),
            &pipeline(Some(&baton), json!([close()])),
        )
        .await?;
    assert_eq!(
        replayed.status,
        StatusCode::BAD_REQUEST,
        "{}",
        replayed.text
    );
    fleet.stop().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_batch_rolls_back_on_error_and_the_cursor_streams_rows() -> TestResult {
    let fleet = Fleet::start(Options::default()).await?;
    fleet.create("orders", 3).await?;
    let token = fleet.token("app", &["write:orders"], "t-app")?;
    // The shape `@libsql/client` sends for `batch(…, "write")`.
    let step = |sql: &str, condition: Value| json!({"condition": condition, "stmt": {"sql": sql}});
    let ok = |index: u32| json!({"type": "ok", "step": index});
    let batch = json!({"steps": [
        step("BEGIN IMMEDIATE", Value::Null),
        step("INSERT INTO t (id, v) VALUES (1, 'x')", ok(0)),
        step("INSERT INTO t (id, v) VALUES (1, 'dup')", ok(1)),
        step("COMMIT", ok(2)),
        step("ROLLBACK", json!({"type": "not", "cond": ok(3)})),
    ]});
    let answer = fleet
        .post(
            "/db/orders/v3/pipeline",
            Some(&token),
            &pipeline(None, json!([{"type": "batch", "batch": batch}, close()])),
        )
        .await?;
    assert_eq!(answer.status, StatusCode::OK, "{}", answer.text);
    let result = &answer.result(0)["response"]["result"];
    assert_eq!(
        result["step_errors"][2]["code"], "SQLITE_CONSTRAINT_PRIMARYKEY",
        "{}",
        answer.text
    );
    assert_eq!(
        result["step_results"][3],
        Value::Null,
        "the commit was skipped"
    );
    assert_eq!(
        result["step_results"][4]["affected_row_count"], 0,
        "the rollback ran"
    );
    assert!(
        fleet.values("orders").await?.is_empty(),
        "nothing committed"
    );

    // A cursor streams each step's rows between its begin and end.
    let cursor = fleet
        .post(
            "/db/orders/v3/cursor",
            Some(&token),
            &json!({"baton": null, "batch": {"steps": [
                step("INSERT INTO t (v) VALUES ('a'), ('b')", Value::Null),
                step("SELECT v FROM t ORDER BY id", Value::Null),
                step("SELECT * FROM missing", Value::Null),
            ]}}),
        )
        .await?;
    assert_eq!(cursor.status, StatusCode::OK, "{}", cursor.text);
    let lines: Vec<Value> = cursor
        .text
        .lines()
        .map(serde_json::from_str)
        .collect::<Result<_, _>>()?;
    let kinds: Vec<&str> = lines[1..]
        .iter()
        .map(|line| line["type"].as_str().unwrap_or_default())
        .collect();
    assert_eq!(
        kinds,
        [
            "step_begin",
            "step_end",
            "step_begin",
            "row",
            "row",
            "step_end",
            "step_error"
        ],
        "{}",
        cursor.text
    );
    assert!(
        lines[0]["baton"].is_string(),
        "the header line carries the baton"
    );
    assert_eq!(lines[4]["row"], json!([text("a")]));
    assert_eq!(lines[7]["step"], 2);
    fleet.stop().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_read_only_token_reads_and_cant_write_through_the_router_or_directly() -> TestResult {
    let fleet = Fleet::start(Options::default()).await?;
    fleet.create("reports", 3).await?;
    let reader = fleet.token("app", &["read:reports"], "t-reader")?;
    let answer = fleet
        .post(
            "/db/reports/v3/pipeline",
            Some(&reader),
            &pipeline(
                None,
                json!([
                    execute("SELECT count(*) FROM t", json!([])),
                    execute("INSERT INTO t (v) VALUES ('no')", json!([])),
                    execute("BEGIN", json!([])),
                    close(),
                ]),
            ),
        )
        .await?;
    assert_eq!(answer.status, StatusCode::OK, "{}", answer.text);
    assert_eq!(answer.result(0)["type"], "ok", "{}", answer.text);
    assert_eq!(answer.error_code(1), "PERMISSION_DENIED");
    let message = answer.result(1)["error"]["message"]
        .as_str()
        .unwrap_or_default();
    assert!(message.contains("write"), "{message}");
    assert_eq!(answer.error_code(2), "PERMISSION_DENIED");
    assert!(fleet.values("reports").await?.is_empty());
    // The data node refuses the same write with the same token, called directly.
    let primary = fleet.admin.locate(&"reports".parse()?).await?;
    let address = primary
        .replicas
        .iter()
        .find(|replica| Some(&replica.node) == primary.primary.as_ref())
        .and_then(|replica| replica.address.clone())
        .ok_or("no primary address")?;
    let network = fleet
        .tls
        .client_network("router")?
        .with_credentials(Credentials::bearer(&fleetringd::auth::BearerToken::new(
            reader,
        ))?);
    let direct = fleetringd::data::Client::new(fleet.control_address.to_string(), network)
        .with_entry(address);
    let refused = direct
        .execute(
            &"reports".parse()?,
            Statement::new("INSERT INTO t (v) VALUES ('no')"),
        )
        .await
        .expect_err("a read-only token can't write");
    assert_eq!(refused.code(), Code::PermissionDenied, "{refused}");
    fleet.stop().await;
    Ok(())
}

/// Authentication comes first: a router whose control plane can't be reached still refuses a
/// request without a token with `401`, and one whose token can't read the database with `403`,
/// so neither reached the network. The version probes need neither.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn requests_are_refused_before_any_node_is_called() -> TestResult {
    let dir = tempfile::tempdir()?;
    let tls = Tls::create(dir.path())?;
    // Nothing listens at the control address.
    let unreachable = std::net::TcpListener::bind("127.0.0.1:0")?.local_addr()?;
    let router = start_router(dir.path(), &tls, unreachable, "").await?;
    let http = HttpClient::builder(TokioExecutor::new()).build_http::<Full<Bytes>>();
    let address = router.listen_address();
    let body = pipeline(None, json!([execute("SELECT 1", json!([]))]));
    let post = |token: Option<String>, path: &'static str| {
        let http = http.clone();
        let body = body.clone();
        async move {
            send(
                &http,
                address,
                Method::POST,
                path,
                token.as_deref(),
                None,
                &body,
            )
            .await
        }
    };
    let anonymous = post(None, "/db/notes/v3/pipeline").await?;
    assert_eq!(
        anonymous.status,
        StatusCode::UNAUTHORIZED,
        "{}",
        anonymous.text
    );
    assert_eq!(anonymous.body["code"], "UNAUTHENTICATED");
    let forged = post(Some("not-a-token".to_owned()), "/db/notes/v3/pipeline").await?;
    assert_eq!(forged.status, StatusCode::UNAUTHORIZED);
    let signer = tls.authority().token_signer()?;
    let other = issue(&signer, "app", &["write:other"], "t-other")?;
    let denied = post(Some(other), "/db/notes/v3/pipeline").await?;
    assert_eq!(denied.status, StatusCode::FORBIDDEN, "{}", denied.text);
    assert_eq!(denied.body["code"], "PERMISSION_DENIED");
    assert_eq!(
        denied.body["message"],
        "this token can't read database 'notes'"
    );
    // An unknown endpoint is `404`, naming both forms of the path.
    let lost = post(None, "/db/notes/v4/pipeline").await?;
    assert_eq!(lost.status, StatusCode::NOT_FOUND, "{}", lost.text);
    assert!(
        lost.text.contains("/db/<percent-encoded name>/"),
        "{}",
        lost.text
    );
    for probe in ["/db/notes/v2", "/db/notes/v3"] {
        let answer = send(&http, address, Method::GET, probe, None, None, &Value::Null).await?;
        assert_eq!(answer.status, StatusCode::OK, "{probe}");
    }
    router.stop().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn v2_refuses_v3_only_requests() -> TestResult {
    let fleet = Fleet::start(Options {
        data_nodes: 1,
        ..Options::default()
    })
    .await?;
    fleet.create("old", 1).await?;
    let token = fleet.token("app", &["write:old"], "t-app")?;
    let answer = fleet
        .post(
            "/db/old/v2/pipeline",
            Some(&token),
            &pipeline(
                None,
                json!([
                    {"type": "get_autocommit"},
                    {"type": "batch", "batch": {"steps": [
                        {"condition": {"type": "is_autocommit"}, "stmt": {"sql": "SELECT 1"}}
                    ]}},
                    execute("SELECT 1", json!([])),
                    close(),
                ]),
            ),
        )
        .await?;
    assert_eq!(answer.status, StatusCode::OK, "{}", answer.text);
    assert_eq!(answer.error_code(0), "PROTOCOL_ERROR");
    assert_eq!(answer.error_code(1), "PROTOCOL_ERROR");
    assert_eq!(answer.result(2)["type"], "ok");
    fleet.stop().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn batons_belong_to_their_token_and_expire_when_idle() -> TestResult {
    let fleet = Fleet::start(Options {
        data_nodes: 1,
        router_tables: "\n[streams]\nidle_timeout_ms = 300\n".to_owned(),
        ..Options::default()
    })
    .await?;
    fleet.create("shared", 1).await?;
    let owner = fleet.token("app", &["write:shared"], "t-owner")?;
    let intruder = fleet.token("intruder", &["write:shared"], "t-intruder")?;
    let same_principal = fleet.token("app", &["write:shared"], "t-second")?;
    let path = "/db/shared/v3/pipeline";
    let opened = fleet
        .post(
            path,
            Some(&owner),
            &pipeline(None, json!([execute("SELECT 1", json!([]))])),
        )
        .await?;
    let baton = opened.baton().ok_or("a baton")?;
    let stolen = fleet
        .post(
            path,
            Some(&intruder),
            &pipeline(Some(&baton), json!([close()])),
        )
        .await?;
    assert_eq!(stolen.status, StatusCode::FORBIDDEN, "{}", stolen.text);
    assert_eq!(
        stolen.body["message"],
        "this stream belongs to another principal"
    );
    let other_token = fleet
        .post(
            path,
            Some(&same_principal),
            &pipeline(Some(&baton), json!([close()])),
        )
        .await?;
    assert_eq!(
        other_token.status,
        StatusCode::FORBIDDEN,
        "{}",
        other_token.text
    );
    // A forged baton reads as an expired one.
    let mut forged = baton.clone().into_bytes();
    forged[12] = if forged[12] == b'A' { b'B' } else { b'A' };
    let forged = String::from_utf8(forged)?;
    let refused = fleet
        .post(
            path,
            Some(&owner),
            &pipeline(Some(&forged), json!([close()])),
        )
        .await?;
    assert_eq!(refused.body["code"], "STREAM_EXPIRED", "{}", refused.text);
    // The owner still holds the stream, until it sits idle past the timeout.
    tokio::time::sleep(Duration::from_millis(1_600)).await;
    let expired = fleet
        .post(
            path,
            Some(&owner),
            &pipeline(Some(&baton), json!([close()])),
        )
        .await?;
    assert_eq!(expired.status, StatusCode::BAD_REQUEST, "{}", expired.text);
    assert_eq!(expired.body["code"], "STREAM_EXPIRED");
    fleet.stop().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_open_transaction_past_its_limit_rolls_back_and_writers_go_on() -> TestResult {
    let fleet = Fleet::start(Options {
        data_nodes: 1,
        transaction_timeout_ms: 500,
        ..Options::default()
    })
    .await?;
    fleet.create("held", 1).await?;
    let token = fleet.token("app", &["write:held"], "t-app")?;
    let path = "/db/held/v3/pipeline";
    let began = fleet
        .post(
            path,
            Some(&token),
            &pipeline(
                None,
                json!([
                    execute("BEGIN", json!([])),
                    execute("INSERT INTO t (v) VALUES ('held')", json!([]))
                ]),
            ),
        )
        .await?;
    assert_eq!(began.result(1)["type"], "ok", "{}", began.text);
    let baton = began.baton().ok_or("a baton")?;
    // Another client's write waits for the transaction, then goes on once it is rolled back.
    let other = fleet
        .post(
            path,
            Some(&token),
            &pipeline(
                None,
                json!([
                    execute("INSERT INTO t (v) VALUES ('other')", json!([])),
                    close()
                ]),
            ),
        )
        .await?;
    assert_eq!(other.result(0)["type"], "ok", "{}", other.text);
    tokio::time::sleep(Duration::from_millis(300)).await;
    let late = fleet
        .post(
            path,
            Some(&token),
            &pipeline(Some(&baton), json!([execute("COMMIT", json!([]))])),
        )
        .await?;
    assert_eq!(
        late.error_code(0),
        "FLEETRING_TRANSACTION_ABORTED",
        "{}",
        late.text
    );
    // The stream stays broken.
    let baton = late.baton().ok_or("a baton")?;
    let again = fleet
        .post(
            path,
            Some(&token),
            &pipeline(
                Some(&baton),
                json!([execute("SELECT 1", json!([])), close()]),
            ),
        )
        .await?;
    assert_eq!(again.error_code(0), "FLEETRING_TRANSACTION_ABORTED");
    assert_eq!(fleet.values("held").await?, [SqlValue::from("other")]);
    fleet.stop().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_drain_redirects_the_next_autocommit_request() -> TestResult {
    let fleet = Fleet::start(Options::default()).await?;
    fleet.create("moving", 3).await?;
    let token = fleet.token("app", &["write:moving"], "t-app")?;
    let path = "/db/moving/v3/pipeline";
    let first = fleet
        .post(
            path,
            Some(&token),
            &pipeline(
                None,
                json!([execute("INSERT INTO t (v) VALUES ('before')", json!([]))]),
            ),
        )
        .await?;
    assert_eq!(first.result(0)["type"], "ok", "{}", first.text);
    let old = fleet.primary("moving").await?;
    let baton = first.baton().ok_or("a baton")?;
    fleet.admin.drain_node(&old.parse()?).await?;
    let new = tokio::time::timeout(crate::fleet::HANG_GUARD, async {
        loop {
            match fleet.primary("moving").await {
                Ok(primary) if primary != old => return primary,
                _ => tokio::time::sleep(Duration::from_millis(50)).await,
            }
        }
    })
    .await?;
    let after = fleet
        .post(
            path,
            Some(&token),
            &pipeline(
                Some(&baton),
                json!([
                    execute("INSERT INTO t (v) VALUES ('after')", json!([])),
                    close()
                ]),
            ),
        )
        .await?;
    assert_eq!(
        after.result(0)["type"],
        "ok",
        "no client-visible error: {}",
        after.text
    );
    assert_eq!(
        after
            .headers
            .get("fleetring-primary")
            .and_then(|value| value.to_str().ok()),
        Some(new.as_str()),
        "the write reached the new primary"
    );
    assert_eq!(
        fleet.values("moving").await?,
        [SqlValue::from("before"), SqlValue::from("after")]
    );
    fleet.stop().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_host_form_and_percent_encoded_names_route() -> TestResult {
    let fleet = Fleet::start(Options {
        data_nodes: 1,
        router_tables: "\n[routing]\nhost_suffix = \"db.example.com\"\n\n[http]\nadvertise_url = \"http://router-1.example.com:8080\"\n".to_owned(),
        ..Options::default()
    })
    .await?;
    fleet.create("notes", 1).await?;
    fleet.create("tenant:acme/notes", 1).await?;
    let token = fleet.token("app", &["write:*"], "t-app")?;
    let request = pipeline(
        None,
        json!([execute("INSERT INTO t (v) VALUES ('x')", json!([]))]),
    );
    let by_host = fleet
        .post_to_host(
            "notes.db.example.com:8080",
            "/v3/pipeline",
            Some(&token),
            &request,
        )
        .await?;
    assert_eq!(by_host.result(0)["type"], "ok", "{}", by_host.text);
    assert_eq!(
        by_host.body["base_url"],
        Value::Null,
        "the host form keeps its own URL"
    );
    let by_path = fleet
        .post(
            "/db/tenant%3Aacme%2Fnotes/v3/pipeline",
            Some(&token),
            &request,
        )
        .await?;
    assert_eq!(by_path.result(0)["type"], "ok", "{}", by_path.text);
    assert_eq!(
        by_path.body["base_url"],
        "http://router-1.example.com:8080/db/tenant%3Aacme%2Fnotes/"
    );
    assert_eq!(fleet.values("notes").await?, [SqlValue::from("x")]);
    assert_eq!(
        fleet.values("tenant:acme/notes").await?,
        [SqlValue::from("x")]
    );
    fleet.stop().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn one_principal_hits_its_stream_cap_while_another_still_opens() -> TestResult {
    let fleet = Fleet::start(Options {
        data_nodes: 1,
        router_tables: "\n[streams]\nmax_streams_per_principal = 2\n".to_owned(),
        ..Options::default()
    })
    .await?;
    fleet.create("capped", 1).await?;
    let path = "/db/capped/v3/pipeline";
    let greedy = fleet.token("greedy", &["read:capped"], "t-greedy")?;
    // A second token of the same principal shares its streams' places.
    let greedy_again = fleet.token("greedy", &["read:capped"], "t-greedy-2")?;
    let open = |token: String| {
        let fleet = &fleet;
        async move {
            fleet
                .post(
                    path,
                    Some(&token),
                    &pipeline(None, json!([execute("SELECT 1", json!([]))])),
                )
                .await
        }
    };
    let first = open(greedy.clone()).await?;
    assert_eq!(first.status, StatusCode::OK, "{}", first.text);
    let second = open(greedy_again.clone()).await?;
    assert_eq!(second.status, StatusCode::OK, "{}", second.text);
    let refused = open(greedy.clone()).await?;
    assert_eq!(
        refused.status,
        StatusCode::TOO_MANY_REQUESTS,
        "{}",
        refused.text
    );
    assert_eq!(
        refused.body["code"], "RESOURCE_EXHAUSTED",
        "{}",
        refused.text
    );
    let message = refused.body["message"].as_str().unwrap_or_default();
    assert!(message.contains("max_streams_per_principal"), "{message}");
    // Another principal still opens one.
    let other = fleet.token("other", &["read:capped"], "t-other")?;
    let theirs = open(other).await?;
    assert_eq!(theirs.status, StatusCode::OK, "{}", theirs.text);
    // A closed stream gives its place back.
    let baton = first.baton().ok_or("a baton")?;
    let closed = fleet
        .post(
            path,
            Some(&greedy),
            &pipeline(Some(&baton), json!([close()])),
        )
        .await?;
    assert_eq!(closed.status, StatusCode::OK, "{}", closed.text);
    let again = open(greedy).await?;
    assert_eq!(again.status, StatusCode::OK, "{}", again.text);
    // Stream IDs are random: two batons share no leading stream ID.
    let (one, two) = (
        second.baton().ok_or("a baton")?,
        again.baton().ok_or("a baton")?,
    );
    assert_ne!(one.get(..10), two.get(..10), "{one} {two}");
    fleet.stop().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_result_too_large_leaves_the_transaction_open() -> TestResult {
    let fleet = Fleet::start(Options {
        data_nodes: 1,
        router_tables: "\n[streams]\nmax_response_bytes = 1000\n".to_owned(),
        ..Options::default()
    })
    .await?;
    fleet.create("large", 1).await?;
    let token = fleet.token("app", &["write:large"], "t-app")?;
    let path = "/db/large/v3/pipeline";
    // One call that begins a transaction, writes, and then reads rows that don't fit the
    // response: it ran, and left its session in the transaction.
    let step = |sql: &str| json!({"stmt": {"sql": sql}});
    let batch = json!({"steps": [
        step("BEGIN"),
        step("INSERT INTO t (v) VALUES ('kept')"),
        step("SELECT zeroblob(4000)"),
    ]});
    let began = fleet
        .post(
            path,
            Some(&token),
            &pipeline(
                None,
                json!([{"type": "batch", "batch": batch}, {"type": "get_autocommit"}]),
            ),
        )
        .await?;
    assert_eq!(began.status, StatusCode::OK, "{}", began.text);
    assert_eq!(began.error_code(0), "FLEETRING_RESULT_TOO_LARGE");
    // The stream knows it is in the transaction.
    assert_eq!(
        began.result(1)["response"]["is_autocommit"],
        false,
        "{}",
        began.text
    );
    let baton = began.baton().ok_or("a baton")?;
    // And the transaction goes on to commit what it wrote.
    let committed = fleet
        .post(
            path,
            Some(&token),
            &pipeline(
                Some(&baton),
                json!([execute("COMMIT", json!([])), {"type": "get_autocommit"}]),
            ),
        )
        .await?;
    assert_eq!(committed.result(0)["type"], "ok", "{}", committed.text);
    assert_eq!(
        committed.result(1)["response"]["is_autocommit"],
        true,
        "{}",
        committed.text
    );
    assert_eq!(fleet.values("large").await?, [SqlValue::from("kept")]);
    fleet.stop().await;
    Ok(())
}
