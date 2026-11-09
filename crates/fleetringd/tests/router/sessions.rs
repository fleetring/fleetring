//! `Database.Session` on the data nodes themselves (ADR-0037, points 11 to 17): transactions
//! across calls, acknowledged commits, redirects, read-only sessions, and what a lease that moves
//! or a caller that leaves does to an open transaction.

use std::error::Error;
use std::time::Duration;

use fleetring_client::redirect::primary_hint;
use fleetring_proto::v1::database_client::DatabaseClient;
use fleetring_proto::v1::{
    self as proto, session_call::Call, session_request::Request as Message,
    session_response::Response, session_result::Result as Answer,
};
use fleetringd::auth::BearerToken;
use fleetringd::engine::Value as SqlValue;
use fleetringd::transport::{CredentialedChannel, Credentials, database_client_with};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Code, Status, Streaming};

use crate::fleet::{Fleet, HANG_GUARD, Options, TestResult};

/// A session opened directly on a node, as the router opens one.
struct Session {
    calls: mpsc::Sender<proto::SessionRequest>,
    responses: Streaming<proto::SessionResponse>,
    opened: proto::SessionOpened,
    next: u64,
}

impl Session {
    /// Opens a session on `database` at the node at `address`, as `principal` with `token`, if
    /// one is given.
    async fn open(
        fleet: &Fleet,
        address: &str,
        principal: &str,
        token: Option<&str>,
        database: &str,
    ) -> Result<Self, Status> {
        let network = fleet
            .tls
            .client_network(principal)
            .map_err(|error| Status::internal(error.to_string()))?;
        let channel = network
            .channel(address)
            .map_err(|error| Status::internal(error.to_string()))?;
        let credentials = match token {
            Some(token) => Credentials::bearer(&BearerToken::new(token))
                .map_err(|error| Status::internal(error.to_string()))?,
            None => Credentials::none(),
        };
        let mut client: DatabaseClient<CredentialedChannel> =
            database_client_with(channel, &credentials);
        let (calls, requests) = mpsc::channel(4);
        calls
            .send(proto::SessionRequest {
                request: Some(Message::Open(proto::SessionOpen {
                    database: database.to_owned(),
                })),
            })
            .await
            .map_err(|_| Status::internal("unsent"))?;
        let mut responses = client
            .session(ReceiverStream::new(requests))
            .await?
            .into_inner();
        let Some(proto::SessionResponse {
            response: Some(Response::Opened(opened)),
        }) = responses.message().await?
        else {
            return Err(Status::internal("no opened"));
        };
        Ok(Self {
            calls,
            responses,
            opened,
            next: 0,
        })
    }

    /// Runs `sql` and returns its result.
    async fn execute(&mut self, sql: &str) -> Result<proto::SessionResult, Status> {
        self.next += 1;
        self.calls
            .send(proto::SessionRequest {
                request: Some(Message::Call(proto::SessionCall {
                    call_id: self.next,
                    call: Some(Call::Execute(proto::SessionStatement {
                        sql: sql.to_owned(),
                        ..proto::SessionStatement::default()
                    })),
                })),
            })
            .await
            .map_err(|_| Status::internal("unsent"))?;
        let message = tokio::time::timeout(HANG_GUARD, self.responses.message())
            .await
            .map_err(|_| Status::deadline_exceeded("no answer"))??;
        match message.and_then(|message| message.response) {
            Some(Response::Result(result)) => {
                assert_eq!(result.call_id, self.next);
                Ok(result)
            }
            other => Err(Status::internal(format!("unexpected: {other:?}"))),
        }
    }

    /// How the session ends, waiting for it.
    async fn ended(&mut self) -> Status {
        match tokio::time::timeout(HANG_GUARD, self.responses.message()).await {
            Ok(Err(status)) => status,
            Ok(Ok(other)) => Status::internal(format!("not ended: {other:?}")),
            Err(_) => Status::deadline_exceeded("never ended"),
        }
    }
}

/// The error code of a result, which must have failed.
fn error_code(result: &proto::SessionResult) -> &str {
    match &result.result {
        Some(Answer::Error(error)) => &error.code,
        other => panic!("not an error: {other:?}"),
    }
}

/// The address of `database`'s primary, and of one of its followers.
async fn addresses(fleet: &Fleet, database: &str) -> Result<(String, String), Box<dyn Error>> {
    let location = fleet.admin.locate(&database.parse()?).await?;
    let primary = location.primary.ok_or("no primary")?;
    let address = |primary_wanted: bool| {
        location
            .replicas
            .iter()
            .find(|replica| (replica.node == primary) == primary_wanted)
            .and_then(|replica| replica.address.clone())
    };
    Ok((
        address(true).ok_or("no primary address")?,
        address(false).ok_or("no follower address")?,
    ))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_session_runs_a_transaction_across_calls_and_waits_for_its_quorum() -> TestResult {
    let fleet = Fleet::start(Options::default()).await?;
    fleet.create("orders", 3).await?;
    let (primary, follower) = addresses(&fleet, "orders").await?;
    // A follower redirects to the primary.
    let redirected = Session::open(&fleet, &follower, "tester", None, "orders")
        .await
        .err()
        .ok_or("a follower opened a session")?;
    assert_eq!(redirected.code(), Code::Unavailable, "{redirected}");
    assert_eq!(primary_hint(&redirected).as_deref(), Some(primary.as_str()));

    let mut session = Session::open(&fleet, &primary, "tester", None, "orders").await?;
    assert!(!session.opened.read_only);
    let began = session.execute("BEGIN").await?;
    assert!(!began.autocommit);
    for value in ["a", "b"] {
        let inserted = session
            .execute(&format!("INSERT INTO t (v) VALUES ('{value}')"))
            .await?;
        assert!(inserted.commit.is_none(), "nothing committed yet");
    }
    assert!(fleet.values("orders").await?.is_empty());
    let committed = session.execute("COMMIT").await?;
    assert!(committed.autocommit);
    let commit = committed.commit.ok_or("the commit is acknowledged")?;
    assert!(commit.acknowledged_by >= 2, "{commit:?}");
    assert_eq!(commit.replicas, 3);
    assert_eq!(commit.epoch, session.opened.epoch);
    assert_eq!(
        fleet.values("orders").await?,
        [SqlValue::from("a"), SqlValue::from("b")]
    );
    fleet.stop().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_read_only_caller_gets_a_read_only_session_and_no_read_gets_none() -> TestResult {
    let fleet = Fleet::start(Options {
        data_nodes: 1,
        ..Options::default()
    })
    .await?;
    fleet.create("reports", 1).await?;
    let (primary, _) = match addresses(&fleet, "reports").await {
        Ok(addresses) => addresses,
        Err(_) => {
            let location = fleet.admin.locate(&"reports".parse()?).await?;
            let address = location.replicas[0].address.clone().ok_or("an address")?;
            (address.clone(), address)
        }
    };
    let reader = fleet.token("app", &["read:reports"], "t-reader")?;
    let mut session = Session::open(&fleet, &primary, "router", Some(&reader), "reports").await?;
    assert!(session.opened.read_only);
    let read = session.execute("SELECT count(*) FROM t").await?;
    assert!(
        matches!(read.result, Some(Answer::Statement(_))),
        "{read:?}"
    );
    let refused = session.execute("INSERT INTO t (v) VALUES ('no')").await?;
    assert_eq!(error_code(&refused), "PERMISSION_DENIED");
    let refused = session.execute("BEGIN").await?;
    assert_eq!(error_code(&refused), "PERMISSION_DENIED");
    assert!(fleet.values("reports").await?.is_empty());
    // A certificate principal with read alone gets a read-only session too.
    let mut by_certificate = Session::open(&fleet, &primary, "reader", None, "reports").await?;
    assert!(by_certificate.opened.read_only);
    let refused = by_certificate.execute("DELETE FROM t").await?;
    assert_eq!(error_code(&refused), "PERMISSION_DENIED");
    // Without read, no session at all.
    let stranger = fleet.token("app", &["read:other"], "t-stranger")?;
    let denied = Session::open(&fleet, &primary, "router", Some(&stranger), "reports")
        .await
        .err()
        .ok_or("a session without read")?;
    assert_eq!(denied.code(), Code::PermissionDenied, "{denied}");
    assert_eq!(denied.message(), "Session needs read on 'reports'");
    // The router's own certificate, without a token, may do nothing.
    let bare = Session::open(&fleet, &primary, "router", None, "reports")
        .await
        .err()
        .ok_or("the router alone opened a session")?;
    assert_eq!(bare.code(), Code::PermissionDenied, "{bare}");
    fleet.stop().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_session_that_ends_mid_transaction_rolls_back() -> TestResult {
    let fleet = Fleet::start(Options {
        data_nodes: 1,
        ..Options::default()
    })
    .await?;
    fleet.create("left", 1).await?;
    let location = fleet.admin.locate(&"left".parse()?).await?;
    let address = location.replicas[0].address.clone().ok_or("an address")?;
    let mut session = Session::open(&fleet, &address, "tester", None, "left").await?;
    session.execute("BEGIN").await?;
    session.execute("INSERT INTO t (v) VALUES ('gone')").await?;
    drop(session);
    // The writer is free at once, and the row never committed.
    tokio::time::timeout(Duration::from_secs(10), async {
        fleet
            .admin
            .execute(
                &"left".parse()?,
                fleetringd::engine::Statement::new("INSERT INTO t (v) VALUES ('kept')"),
            )
            .await?;
        Ok::<_, Box<dyn Error>>(())
    })
    .await??;
    assert_eq!(fleet.values("left").await?, [SqlValue::from("kept")]);
    fleet.stop().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn losing_the_lease_ends_the_session_and_rolls_back() -> TestResult {
    let fleet = Fleet::start(Options::default()).await?;
    fleet.create("moving", 3).await?;
    let (primary, _) = addresses(&fleet, "moving").await?;
    let old = fleet.primary("moving").await?;
    let mut session = Session::open(&fleet, &primary, "tester", None, "moving").await?;
    session.execute("BEGIN").await?;
    session
        .execute("INSERT INTO t (v) VALUES ('uncommitted')")
        .await?;
    fleet.admin.drain_node(&old.parse()?).await?;
    let ended = session.ended().await;
    assert_eq!(ended.code(), Code::Unavailable, "{ended}");
    tokio::time::timeout(HANG_GUARD, async {
        loop {
            match fleet.primary("moving").await {
                Ok(primary) if primary != old => return,
                _ => tokio::time::sleep(Duration::from_millis(50)).await,
            }
        }
    })
    .await?;
    assert!(
        fleet.values("moving").await?.is_empty(),
        "nothing the session didn't commit survives"
    );
    fleet.stop().await;
    Ok(())
}
