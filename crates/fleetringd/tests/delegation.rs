//! FR-090: which connections carry a delegated agent's token, and how revoking an ancestor ends
//! its requests and streams, over real TCP with mutual TLS and through the router (ADR-0047,
//! point 4.1 and invariants I5 and I9).
//!
//! One control daemon, one data daemon, and a router. The tokens are signed in the test: the
//! root agent `agent-42`'s by the authority, and its delegated agents' by two delegation issuers
//! the authority certified, `A` and `B`, as a key rotation or a leader change leaves a fleet.
//! Nodes need nothing to verify them but the authority they already trust (ADR-0047, point 1.4).
//!
//! The decision table of the connection rule, the refusal of a token without a client
//! certificate (which no TLS listener can produce), and the control freshness hook are unit
//! tests in `auth/boundary.rs`.

use std::error::Error;
use std::net::SocketAddr;
use std::path::Path;
use std::time::Duration;

use fleetring::ReplicationFactor;
use fleetring_control::delegation::{ChainLink, DelegationId, TokenClaims, TokenKind};
use fleetring_control::{DatabaseName, Millis, Timestamp, TokenId};
use fleetring_proto::v1::database_client::DatabaseClient;
use fleetring_proto::v1::revocation::Revoked;
use fleetring_proto::v1::{
    self as proto, Revocation, RevokeTokenRequest, session_request::Request as Message,
    session_response::Response,
};
use fleetringd::auth::{
    BearerToken, DelegationSigner, STREAM_RECHECK, Scopes, TokenSigner, TokenSpec,
};
use fleetringd::clock::{Clock as _, SystemClock};
use fleetringd::data::{Client, DatabaseSpec};
use fleetringd::engine::Statement;
use fleetringd::router::{Router, RouterConfig};
use fleetringd::tls::Validity;
use fleetringd::transport::{CredentialedChannel, Credentials, database_client_with};
use fleetringd::{Config, Daemon, Network};
use http_body_util::{BodyExt as _, Full};
use hyper::body::Bytes;
use hyper::{Request, StatusCode};
use hyper_util::client::legacy::Client as HttpClient;
use hyper_util::rt::TokioExecutor;
use rustls_pki_types::pem::PemObject as _;
use rustls_pki_types::{CertificateDer, PrivateKeyDer};
use serde_json::{Value, json};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Code, Status, Streaming};

use crate::support::Tls;

mod support;

#[path = "delegation/ancestor.rs"]
mod ancestor;

type TestResult = Result<(), Box<dyn Error>>;

/// Guards against a hang only.
const HANG_GUARD: Duration = Duration::from_secs(60);

/// How often the data daemon sends heartbeats, which bounds how long a revocation takes to
/// reach it.
const HEARTBEAT_MS: u64 = 100;

/// The control plane's heartbeat timeout, how old the data daemon's denylist may get before it
/// refuses every token: fifteen heartbeats, so a delayed heartbeat on a loaded runner never
/// trips it.
const HEARTBEAT_TIMEOUT_MS: u64 = 1_500;

/// Room for a loaded runner on top of [`STREAM_RECHECK`] between the data node refusing a token
/// and the stream it opened ending: the recheck runs every second, and the test sees the refusal
/// only after a poll.
const RECHECK_SLACK: Duration = Duration::from_secs(1);

/// The root token's ID.
const ROOT: &str = "root-42";

/// The principals every node knows besides `tester`: the router forwards its callers' tokens
/// and may do nothing itself. `agent-42`, `agent-43` and the rest have no entry, as agents
/// don't (ADR-0043, point 8.7): their certificates carry tokens and grant nothing.
const PRINCIPALS: &str = "\n[[principal]]\nname = \"router\"\nscopes = []\nforward_tokens = true\n";

/// The databases the tests read: one each for `agent-42.c7`, `agent-42.c7.r1`, and
/// `agent-42.c7.r2`.
const C7_DB: &str = "agent-42.c7-notes";
const R1_DB: &str = "agent-42.c7.r1-notes";
const R2_DB: &str = "agent-42.c7.r2-notes";

/// A control node, a data node, a router, an administrator, and the signers of a lineage.
struct Fleet {
    _dir: tempfile::TempDir,
    tls: Tls,
    control: Daemon,
    data: Daemon,
    router: Router,
    admin: Client,
    control_target: String,
    data_target: String,
    http: HttpClient<hyper_util::client::legacy::connect::HttpConnector, Full<Bytes>>,
    authority: TokenSigner,
    a: DelegationSigner,
    b: DelegationSigner,
    /// When every token here starts, in whole seconds, as a delegation's times are.
    issued: Timestamp,
}

fn node_config(
    dir: &Path,
    tls: &Tls,
    id: &str,
    role: &str,
    control: Option<SocketAddr>,
) -> Result<String, Box<dyn Error>> {
    let address = control.map_or_else(String::new, |address| format!("address = \"{address}\"\n"));
    Ok(format!(
        "data_dir = {:?}\nlisten = \"127.0.0.1:0\"\nrole = \"{role}\"\n\n[node]\nid = \"{id}\"\n\
         \n[membership]\nheartbeat_interval_ms = {HEARTBEAT_MS}\n\
         \n[control]\n{address}heartbeat_timeout_ms = {HEARTBEAT_TIMEOUT_MS}\n{}{PRINCIPALS}",
        dir.join(id),
        tls.node_table(id)?,
    ))
}

fn issuer(tls: &Tls, name: &str, issued: Timestamp) -> Result<DelegationSigner, Box<dyn Error>> {
    let start = Timestamp::from_millis(issued.as_millis().saturating_sub(3_600_000));
    let pair = tls.authority().issue_issuer(
        &name.parse()?,
        Validity::new(start, Duration::from_secs(30 * 24 * 3600)),
    )?;
    let cert = CertificateDer::from_pem_slice(pair.certificate_pem().as_bytes())?;
    let PrivateKeyDer::Pkcs8(key) = PrivateKeyDer::from_pem_slice(pair.key_pem().as_bytes())?
    else {
        return Err("an issuer key is PKCS#8".into());
    };
    let authorities = [tls.authority().token_signer()?.verifying_key()];
    Ok(DelegationSigner::new(
        cert.as_ref(),
        key.secret_pkcs8_der(),
        tls.authority().fleet(),
        &authorities,
    )?)
}

impl Fleet {
    async fn start() -> Result<Self, Box<dyn Error>> {
        let dir = tempfile::tempdir()?;
        let tls = Tls::create(dir.path())?;
        let control = Daemon::start(Config::from_toml(&node_config(
            dir.path(),
            &tls,
            "control",
            "control",
            None,
        )?)?)
        .await?;
        let address = control.control_address().ok_or("no control address")?;
        let data = Daemon::start(Config::from_toml(&node_config(
            dir.path(),
            &tls,
            "data-1",
            "data",
            Some(address),
        )?)?)
        .await?;
        let admin = Client::new(address.to_string(), tls.client_network("tester")?);
        tokio::time::timeout(HANG_GUARD, async {
            loop {
                match admin.status().await {
                    Ok(view) if view.members().len() == 1 => return Ok::<_, Status>(()),
                    _ => tokio::time::sleep(Duration::from_millis(50)).await,
                }
            }
        })
        .await??;
        for database in [C7_DB, R1_DB, R2_DB] {
            let name: DatabaseName = database.parse()?;
            admin
                .create_database_with(&name, DatabaseSpec::new(ReplicationFactor::ONE))
                .await?;
            admin
                .execute(
                    &name,
                    Statement::new("CREATE TABLE t (id INTEGER PRIMARY KEY, v)"),
                )
                .await?;
        }
        let router = start_router(&tls, address).await?;
        let now = SystemClock.now().as_millis();
        let issued = Timestamp::from_millis(now - now % 1_000);
        Ok(Self {
            authority: tls.authority().token_signer()?,
            a: issuer(&tls, "control-a", issued)?,
            b: issuer(&tls, "control-b", issued)?,
            issued,
            control_target: address.to_string(),
            data_target: data.listen_address().to_string(),
            _dir: dir,
            tls,
            control,
            data,
            router,
            admin,
            http: HttpClient::builder(TokioExecutor::new()).build_http(),
        })
    }

    async fn stop(self) {
        self.router.stop().await;
        self.data.run_until(async {}).await;
        self.control.run_until(async {}).await;
    }

    fn expires(&self) -> Timestamp {
        self.issued.saturating_add(Millis::new(3_600_000))
    }

    /// `agent-42`'s root token, which the authority signs.
    fn root(&self) -> Result<BearerToken, Box<dyn Error>> {
        Ok(self.authority.issue(
            &TokenSpec::new(
                "agent-42".parse()?,
                scopes(&["admin:agent-42-*", "admin:agent-42.*", "grant:agent-42.*"])?,
                ROOT.parse()?,
                self.issued,
                self.expires(),
            )
            .agent("alice".parse()?),
        )?)
    }

    /// An authority-signed token for `principal`, which isn't an agent.
    fn service(&self, principal: &str, id: &str) -> Result<BearerToken, Box<dyn Error>> {
        Ok(self.authority.issue(&TokenSpec::new(
            principal.parse()?,
            scopes(&["read:svc-*"])?,
            id.parse()?,
            self.issued,
            self.expires(),
        ))?)
    }

    fn root_link(&self) -> Result<ChainLink, Box<dyn Error>> {
        Ok(ChainLink {
            id: ROOT.parse()?,
            kid: self.authority.key_id().clone(),
        })
    }

    /// `agent-42.c7`'s claims, delegated from the root token.
    fn c7(&self) -> Result<TokenClaims, Box<dyn Error>> {
        self.claims(
            "agent-42.c7",
            c7_id(),
            "agent-42",
            &[
                "admin:agent-42.c7-*",
                "stream:agent-42.c7-*",
                "grant:agent-42.c7.*",
            ],
            vec![self.root_link()?],
        )
    }

    /// The grandchild `agent-42.c7.<segment>`, delegated from the copy of `c7` that `copy`
    /// signed.
    fn grandchild(
        &self,
        segment: &str,
        id: [u8; 16],
        copy: &DelegationSigner,
    ) -> Result<TokenClaims, Box<dyn Error>> {
        let principal = format!("agent-42.c7.{segment}");
        self.claims(
            &principal,
            DelegationId::from_bytes(id).into(),
            "agent-42.c7",
            &[
                &format!("admin:{principal}-*"),
                &format!("stream:{principal}-*"),
            ],
            vec![
                self.root_link()?,
                ChainLink {
                    id: c7_id(),
                    kid: copy.key_id().clone(),
                },
            ],
        )
    }

    fn claims(
        &self,
        principal: &str,
        id: TokenId,
        parent: &str,
        scope_texts: &[&str],
        chain: Vec<ChainLink>,
    ) -> Result<TokenClaims, Box<dyn Error>> {
        Ok(TokenClaims {
            fleet: self.tls.authority().fleet().as_str().into(),
            principal: principal.parse()?,
            id,
            issued_at: Some(self.issued),
            not_before: self.issued,
            expires_at: self.expires(),
            scopes: scopes(scope_texts)?,
            kind: Some(TokenKind::Agent),
            owner: Some("alice".parse()?),
            parent: Some(parent.parse()?),
            chain,
        })
    }

    /// A client whose certificate names `principal`, carrying `token`.
    fn client(&self, principal: &str, token: &BearerToken) -> Result<Client, Box<dyn Error>> {
        Ok(Client::new(
            self.control_target.clone(),
            with_token(self.tls.client_network(principal)?, token)?,
        ))
    }

    /// Revokes `revoked` fleet-wide, as an operator does.
    async fn revoke(&self, revoked: Revoked) -> TestResult {
        self.admin
            .control()
            .revoke_token(RevokeTokenRequest {
                revocation: Some(Revocation {
                    revoked: Some(revoked),
                    until_ms: self
                        .expires()
                        .saturating_add(Millis::new(60_000))
                        .as_millis(),
                }),
                principal: String::new(),
            })
            .await?;
        Ok(())
    }

    /// Waits until the data node refuses `client`'s token, and returns how long that took.
    async fn refused_at_the_data_node(&self, client: &Client) -> Result<Duration, Box<dyn Error>> {
        let started = tokio::time::Instant::now();
        tokio::time::timeout(HANG_GUARD, async {
            while client.node_status(&self.data_target).await.is_ok() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await?;
        let waited = started.elapsed();
        let status = client
            .node_status(&self.data_target)
            .await
            .expect_err("refused at the data node");
        assert_eq!(status.code(), Code::Unauthenticated, "{status}");
        assert_eq!(status.message(), "the token is revoked");
        Ok(waited)
    }

    /// Posts a Hrana pipeline to the router for `database`, carrying `token`.
    async fn pipeline(
        &self,
        database: &str,
        token: &BearerToken,
        baton: Option<&str>,
        requests: Value,
    ) -> Result<(StatusCode, Value), Box<dyn Error>> {
        let body = json!({ "baton": baton, "requests": requests });
        let request = Request::builder()
            .method("POST")
            .uri(format!(
                "http://{}/db/{database}/v3/pipeline",
                self.router.listen_address()
            ))
            .header("content-type", "application/json")
            .header("authorization", format!("Bearer {}", token.expose()))
            .body(Full::new(Bytes::from(serde_json::to_vec(&body)?)))?;
        let response = tokio::time::timeout(HANG_GUARD, self.http.request(request)).await??;
        let status = response.status();
        let bytes = response.into_body().collect().await?.to_bytes();
        Ok((
            status,
            serde_json::from_slice(&bytes).unwrap_or(Value::Null),
        ))
    }
}

fn c7_id() -> TokenId {
    DelegationId::from_bytes([7; 16]).into()
}

fn scopes(texts: &[&str]) -> Result<Scopes, Box<dyn Error>> {
    Ok(texts
        .iter()
        .map(|text| text.parse())
        .collect::<Result<_, _>>()?)
}

fn with_token(network: Network, token: &BearerToken) -> Result<Network, Box<dyn Error>> {
    Ok(network.with_credentials(Credentials::bearer(token)?))
}

fn read_one() -> Statement {
    Statement::new("SELECT count(*) FROM t")
}

fn execute(sql: &str) -> Value {
    json!({ "type": "execute", "stmt": { "sql": sql } })
}

async fn start_router(tls: &Tls, control: SocketAddr) -> Result<Router, Box<dyn Error>> {
    let stem = "client.router";
    tls.authority()
        .issue_client(&"router".parse()?, Tls::validity())?
        .write(tls.dir(), stem)?;
    let text = format!(
        "listen = \"127.0.0.1:0\"\n\n[tls]\nca = {:?}\ncert = {:?}\nkey = {:?}\n\n[control]\naddress = \"{control}\"\n",
        tls.dir().join("ca.crt"),
        tls.dir().join(format!("{stem}.crt")),
        tls.dir().join(format!("{stem}.key")),
    );
    Ok(Router::start(RouterConfig::from_toml(&text)?).await?)
}

/// Reads `updates` until the stream ends with a refusal, and returns it.
async fn refusal_of(
    updates: &mut fleetringd::data::LeaseUpdates,
) -> Result<Status, Box<dyn Error>> {
    tokio::time::timeout(HANG_GUARD, async {
        loop {
            match updates.next().await {
                Ok(Some(_)) => {}
                Ok(None) => return Err("ended without a refusal".into()),
                Err(status) => return Ok(status),
            }
        }
    })
    .await?
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn child_and_grandchild_tokens_ride_the_root_certificate() -> TestResult {
    let fleet = Fleet::start().await?;
    let c7 = fleet.a.sign(&fleet.c7()?)?;
    let r1 = fleet.b.sign(&fleet.grandchild("r1", [1; 16], &fleet.a)?)?;

    // `agent-42`'s certificate carries its child's and its grandchild's tokens, at the control
    // node and at the data node, each acting with its own scopes.
    let child = fleet.client("agent-42", &c7)?;
    child.query(&C7_DB.parse()?, read_one()).await?;
    child.node_status(&fleet.data_target).await?;
    let grandchild = fleet.client("agent-42", &r1)?;
    grandchild.query(&R1_DB.parse()?, read_one()).await?;
    grandchild.node_status(&fleet.data_target).await?;
    let denied = child
        .query(&R1_DB.parse()?, read_one())
        .await
        .expect_err("c7's scopes don't reach r1's databases");
    assert_eq!(denied.code(), Code::PermissionDenied, "{denied}");

    // The root's own token rides its own certificate, as in 0.7.
    let root = fleet.client("agent-42", &fleet.root()?)?;
    root.query(&C7_DB.parse()?, read_one()).await?;
    fleet.stop().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn child_token_on_agent_43_agent_420_agent_4_or_node_certificate_is_refused() -> TestResult {
    let fleet = Fleet::start().await?;
    let c7 = fleet.a.sign(&fleet.c7()?)?;
    // A copied child token is useless without an ancestor's certificate key: another root's,
    // a longer or a shorter name sharing its digits, or a node's.
    for principal in ["agent-43", "agent-420", "agent-4"] {
        let copied = fleet.client(principal, &c7)?;
        let at_control = copied
            .query(&C7_DB.parse()?, read_one())
            .await
            .expect_err("refused at the control node");
        assert_eq!(
            at_control.code(),
            Code::PermissionDenied,
            "{principal}: {at_control}"
        );
        assert_eq!(
            at_control.message(),
            "the delegated token names neither the client certificate's principal nor a \
             principal below it",
            "{principal}"
        );
        let at_data = copied
            .node_status(&fleet.data_target)
            .await
            .expect_err("refused at the data node");
        assert_eq!(
            at_data.code(),
            Code::PermissionDenied,
            "{principal}: {at_data}"
        );
    }
    let node = Client::new(
        fleet.control_target.clone(),
        with_token(fleet.tls.node_network("data-9")?, &c7)?,
    );
    let refused = node
        .node_status(&fleet.data_target)
        .await
        .expect_err("a node's connection carries no token");
    assert_eq!(refused.code(), Code::PermissionDenied, "{refused}");
    fleet.stop().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn root_or_service_token_never_rides_an_ancestor_certificate() -> TestResult {
    let fleet = Fleet::start().await?;
    // `workload.prod`'s token, which the authority signed, rides only `workload.prod`'s
    // certificate: the ancestor match is for delegated tokens alone.
    let service = fleet.service("workload.prod", "svc-1")?;
    let on_parent = fleet.client("workload", &service)?;
    let refused = on_parent
        .list_databases()
        .await
        .expect_err("workload's certificate can't carry it");
    assert_eq!(refused.code(), Code::PermissionDenied, "{refused}");
    assert_eq!(
        refused.message(),
        "the token names another principal than the client certificate"
    );
    let refused = on_parent
        .node_status(&fleet.data_target)
        .await
        .expect_err("nor at the data node");
    assert_eq!(refused.code(), Code::PermissionDenied, "{refused}");
    fleet
        .client("workload.prod", &service)?
        .list_databases()
        .await?;

    // Nor does a root agent's token ride another name's certificate.
    let root = fleet.root()?;
    let refused = fleet
        .client("agent-4", &root)?
        .list_databases()
        .await
        .expect_err("agent-4's certificate can't carry agent-42's token");
    assert_eq!(refused.code(), Code::PermissionDenied, "{refused}");
    fleet.stop().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn child_token_is_relay_carried_through_a_forward_tokens_principal_and_the_router()
-> TestResult {
    let fleet = Fleet::start().await?;
    let c7 = fleet.a.sign(&fleet.c7()?)?;
    let r1 = fleet.b.sign(&fleet.grandchild("r1", [1; 16], &fleet.a)?)?;

    // A principal that forwards tokens carries a delegated token as its caller's.
    let relayed = fleet.client("router", &c7)?;
    relayed.query(&C7_DB.parse()?, read_one()).await?;
    relayed.node_status(&fleet.data_target).await?;

    // So does the router, whose HTTP clients present no certificate (ADR-0037): it verifies
    // the token, then calls the data node on its own certificate.
    let (status, body) = fleet
        .pipeline(C7_DB, &c7, None, json!([execute("SELECT count(*) FROM t")]))
        .await?;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["results"][0]["type"], "ok", "{body}");
    let (status, body) = fleet
        .pipeline(R1_DB, &r1, None, json!([execute("SELECT count(*) FROM t")]))
        .await?;
    assert_eq!(status, StatusCode::OK, "{body}");
    // Each acts with its own scopes.
    let (status, body) = fleet
        .pipeline(C7_DB, &r1, None, json!([execute("SELECT count(*) FROM t")]))
        .await?;
    assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
    fleet.stop().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn revoking_the_root_jti_ends_the_child_within_a_heartbeat_and_its_watch_within_recheck()
-> TestResult {
    let fleet = Fleet::start().await?;
    let c7 = fleet.a.sign(&fleet.c7()?)?;
    let child = fleet.client("agent-42", &c7)?;
    let mut watch = child.watch_leases(&fleet.data_target).await?;
    child.node_status(&fleet.data_target).await?;

    fleet.revoke(Revoked::TokenId(ROOT.into())).await?;
    let refused = child
        .query(&C7_DB.parse()?, read_one())
        .await
        .expect_err("refused by the control node at once");
    assert_eq!(refused.message(), "the token is revoked");
    let waited = fleet.refused_at_the_data_node(&child).await?;
    assert!(
        waited < Duration::from_millis(HEARTBEAT_TIMEOUT_MS),
        "the data node refused the child after {waited:?}"
    );
    let refused_at = tokio::time::Instant::now();
    let ended = refusal_of(&mut watch).await?;
    assert_eq!(ended.code(), Code::Unauthenticated, "{ended}");
    assert_eq!(ended.message(), "the token is revoked");
    assert!(
        refused_at.elapsed() < STREAM_RECHECK + RECHECK_SLACK,
        "the watch ended {:?} after the data node refused the token",
        refused_at.elapsed()
    );
    fleet.stop().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn revoked_key_of_a_chain_copy_ends_the_grandchild() -> TestResult {
    let fleet = Fleet::start().await?;
    let c7_claims = fleet.c7()?;
    let c7_a = fleet.client("agent-42", &fleet.a.sign(&c7_claims)?)?;
    let c7_b = fleet.client("agent-42", &fleet.b.sign(&c7_claims)?)?;
    // After a rotation to B: r1 was delegated from c7's A copy, r2 from its B copy.
    let r1 = fleet.client(
        "agent-42",
        &fleet.b.sign(&fleet.grandchild("r1", [1; 16], &fleet.a)?)?,
    )?;
    let r2 = fleet.client(
        "agent-42",
        &fleet.b.sign(&fleet.grandchild("r2", [2; 16], &fleet.b)?)?,
    )?;
    let mut r1_watch = r1.watch_leases(&fleet.data_target).await?;

    fleet
        .revoke(Revoked::KeyId(fleet.a.key_id().to_string()))
        .await?;
    fleet.refused_at_the_data_node(&r1).await?;
    fleet.refused_at_the_data_node(&c7_a).await?;
    let ended = refusal_of(&mut r1_watch).await?;
    assert_eq!(ended.message(), "the token is revoked");
    // What A never signed, at any level, survives.
    c7_b.node_status(&fleet.data_target).await?;
    r2.node_status(&fleet.data_target).await?;
    r2.query(&R2_DB.parse()?, read_one()).await?;
    fleet.stop().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn revoked_chain_id_ends_every_copy_of_the_grandchild() -> TestResult {
    let fleet = Fleet::start().await?;
    let r1_claims = fleet.grandchild("r1", [1; 16], &fleet.a)?;
    let copies = [
        fleet.client("agent-42", &fleet.a.sign(&r1_claims)?)?,
        fleet.client("agent-42", &fleet.b.sign(&r1_claims)?)?,
    ];
    let r2 = fleet.client(
        "agent-42",
        &fleet.b.sign(&fleet.grandchild("r2", [2; 16], &fleet.b)?)?,
    )?;
    let root = fleet.client("agent-42", &fleet.root()?)?;
    for copy in &copies {
        copy.node_status(&fleet.data_target).await?;
    }

    // Revoking c7's delegation ID ends every token below it, whichever issuer signed which copy.
    fleet.revoke(Revoked::TokenId(c7_id().to_string())).await?;
    for copy in &copies {
        fleet.refused_at_the_data_node(copy).await?;
    }
    fleet.refused_at_the_data_node(&r2).await?;
    root.node_status(&fleet.data_target).await?;
    fleet.stop().await;
    Ok(())
}

/// A `Database.Session` opened directly on a node, as the router opens one.
struct Session {
    _calls: mpsc::Sender<proto::SessionRequest>,
    responses: Streaming<proto::SessionResponse>,
}

impl Session {
    async fn open(
        fleet: &Fleet,
        principal: &str,
        token: &BearerToken,
        database: &str,
    ) -> Result<Self, Box<dyn Error>> {
        let address = fleet.admin.locate(&database.parse()?).await?;
        let primary = address
            .replicas
            .iter()
            .find(|replica| Some(&replica.node) == address.primary.as_ref())
            .and_then(|replica| replica.address.clone())
            .ok_or("no primary address")?;
        let channel = fleet.tls.client_network(principal)?.channel(&primary)?;
        let mut client: DatabaseClient<CredentialedChannel> =
            database_client_with(channel, &Credentials::bearer(token)?);
        let (calls, requests) = mpsc::channel(4);
        calls
            .send(proto::SessionRequest {
                request: Some(Message::Open(proto::SessionOpen {
                    database: database.to_owned(),
                })),
            })
            .await?;
        let mut responses = client
            .session(ReceiverStream::new(requests))
            .await?
            .into_inner();
        match responses.message().await? {
            Some(proto::SessionResponse {
                response: Some(Response::Opened(_)),
            }) => Ok(Self {
                _calls: calls,
                responses,
            }),
            other => Err(format!("not opened: {other:?}").into()),
        }
    }

    /// How the session ends, with nothing sent on it.
    async fn ended(&mut self) -> Status {
        match tokio::time::timeout(HANG_GUARD, self.responses.message()).await {
            Ok(Err(status)) => status,
            Ok(Ok(other)) => Status::internal(format!("not ended: {other:?}")),
            Err(_) => Status::deadline_exceeded("never ended"),
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn open_router_session_ends_on_an_ancestors_revocation() -> TestResult {
    let fleet = Fleet::start().await?;
    let r1 = fleet.b.sign(&fleet.grandchild("r1", [1; 16], &fleet.a)?)?;

    // A transaction held open through the router, and a session the way the router holds one:
    // on the router's certificate, relay-carried.
    let (status, body) = fleet
        .pipeline(
            R1_DB,
            &r1,
            None,
            json!([
                execute("BEGIN"),
                execute("INSERT INTO t (v) VALUES ('kept')")
            ]),
        )
        .await?;
    assert_eq!(status, StatusCode::OK, "{body}");
    let baton = body["baton"].as_str().ok_or("a baton")?.to_owned();
    let mut session = Session::open(&fleet, "router", &r1, R1_DB).await?;
    let relayed = fleet.client("router", &r1)?;

    // The parent's delegation ID is revoked on the data node.
    fleet.revoke(Revoked::TokenId(c7_id().to_string())).await?;
    fleet.refused_at_the_data_node(&relayed).await?;
    let refused_at = tokio::time::Instant::now();

    // The idle session ends on its own at the next recheck.
    let ended = session.ended().await;
    assert_eq!(ended.code(), Code::Unauthenticated, "{ended}");
    assert_eq!(ended.message(), "the token is revoked");
    assert!(
        refused_at.elapsed() < STREAM_RECHECK + RECHECK_SLACK,
        "the session ended {:?} after the data node refused the token",
        refused_at.elapsed()
    );

    // The router's transaction is gone with its session: nothing more runs on it, and nothing
    // it wrote was committed.
    let (status, body) = fleet
        .pipeline(R1_DB, &r1, Some(&baton), json!([execute("COMMIT")]))
        .await?;
    let committed = status == StatusCode::OK && body["results"][0]["type"] == "ok";
    assert!(
        !committed,
        "the transaction outlived its session: {status} {body}"
    );
    let rows = fleet
        .admin
        .query(&R1_DB.parse()?, Statement::new("SELECT count(*) FROM t"))
        .await?;
    assert_eq!(
        format!("{:?}", rows.rows),
        format!("{:?}", [[fleetringd::engine::Value::Integer(0)]]),
        "nothing was committed"
    );
    fleet.stop().await;
    Ok(())
}
