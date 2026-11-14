//! The release blocker of ADR-0047 (invariant I5): a revocation ends the whole subtree below it,
//! across issuer rotation and leader changes, on a fleet of three real daemons over TCP with
//! mutual TLS (the plan for FR-090 to FR-093, section 14).
//!
//! In 0.8 a leader change is a control node restart on new issuer files, and a rotation a reload
//! of new files without one. Every case runs on a fresh fleet, once each way:
//!
//! 1. `agent-42` delegates `42.c7` under issuer key A, with explicit scopes that include
//!    `grant:agent-42.c7.*`;
//! 2. the control node moves to issuer key B;
//! 3. `42.c7` delegates `42.c7.r1` from its A-signed copy;
//! 4. `agent-42` resends step 1's operation and receives a B-signed copy of `c7`, the same
//!    delegation;
//! 5. `42.c7` delegates `42.c7.r2` from the B copy;
//! 6. each of `c7`'s two copies, `r1` and `r2` opens a `WatchChanges` stream on its database.
//!
//! Each case then revokes, and checks exactly which tokens end, on the data nodes within the
//! heartbeat timeout and on their open streams within [`STREAM_RECHECK`], which survive, and that
//! no ended token can delegate (I4).

use std::error::Error;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU16, Ordering};
use std::time::Duration;

use fleetring::ReplicationFactor;
use fleetring_control::{DatabaseName, Millis, Timestamp};
use fleetring_proto::v1::revocation::Revoked;
use fleetring_proto::v1::{
    DelegateTokenRequest, DelegateTokenResponse, Revocation, RevokeTokenRequest,
};
use fleetringd::auth::{BearerToken, STREAM_RECHECK, Scopes, TokenSpec};
use fleetringd::clock::{Clock as _, SystemClock};
use fleetringd::control::ControlClient;
use fleetringd::data::{ChangeUpdates, Client, DatabaseSpec};
use fleetringd::engine::Statement;
use fleetringd::tls::Validity;
use fleetringd::transport::Credentials;
use fleetringd::{Config, Daemon, Network};
use tonic::{Code, Status};

use crate::support::Tls;

mod support;

type TestResult = Result<(), Box<dyn Error>>;

/// Guards against a hang only.
const HANG_GUARD: Duration = Duration::from_secs(60);

/// How often data nodes send heartbeats, which bounds how long a revocation takes to reach one.
const HEARTBEAT_MS: u64 = 100;

/// The control plane's heartbeat timeout: fifteen heartbeats, so a delayed one on a loaded
/// runner never trips it. A revocation reaches every live data node well within it.
const HEARTBEAT_TIMEOUT_MS: u64 = 1_500;

/// Room for a loaded runner on top of [`STREAM_RECHECK`] between a data node refusing a token
/// and the stream it opened ending.
const RECHECK_SLACK: Duration = Duration::from_secs(1);

/// `agent-42`'s root token's ID.
const ROOT_ID: &str = "root-42";

/// `agent-42`'s root scopes, as `agent add` mints them once every member reports `delegation`.
const ROOT_SCOPES: [&str; 6] = [
    "admin:agent-42-*",
    "admin:agent-42.*",
    "stream:agent-42-*",
    "stream:agent-42.*",
    "grant:agent-42-*",
    "grant:agent-42.*",
];

/// `agent-42.c7`'s explicit scopes, `grant` included, so it may delegate.
const C7_SCOPES: [&str; 5] = [
    "admin:agent-42.c7-*",
    "stream:agent-42.c7-*",
    "admin:agent-42.c7.*",
    "stream:agent-42.c7.*",
    "grant:agent-42.c7.*",
];

/// The databases the lineage reads and watches, which the operator creates.
const DATABASES: [&str; 5] = [
    "agent-42-notes",
    "agent-42.c7-notes",
    "agent-42.c7.r1-notes",
    "agent-42.c7.r2-notes",
    "agent-42.c7.r1-again",
];

/// How the fleet moves from issuer key A to B.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Change {
    /// New files, reloaded by the running control node.
    Reload,
    /// New files, and the control node restarted on them: 0.8's leader change.
    Restart,
}

/// A control node, two data nodes, the authority, and an operator.
struct Fleet {
    dir: tempfile::TempDir,
    tls: Tls,
    control: Option<Daemon>,
    control_config: String,
    data: Vec<Daemon>,
    address: String,
    operator: Client,
    issuer_cert: PathBuf,
    issuer_key: PathBuf,
    /// When the fleet started, in whole seconds.
    start: Timestamp,
}

/// The next port [`free_port`] tries.
static NEXT_PORT: AtomicU16 = AtomicU16::new(0);

/// A port free now, for the control node, whose address the data nodes keep across a restart.
/// It comes from a range of this binary's own below the ephemeral ports: while the control node
/// is down, a data node redialling an ephemeral port could be given that same port as its own
/// end and connect to itself, holding it.
fn free_port() -> Result<u16, Box<dyn Error>> {
    let start = 24_000 + u16::try_from(std::process::id() % 200)? * 20;
    let _ = NEXT_PORT.compare_exchange(0, start, Ordering::SeqCst, Ordering::SeqCst);
    for _ in 0..1_000 {
        let port = NEXT_PORT.fetch_add(1, Ordering::SeqCst);
        if TcpListener::bind(("127.0.0.1", port)).is_ok() {
            return Ok(port);
        }
    }
    Err("no free port".into())
}

fn node_config(
    dir: &Path,
    tls: &Tls,
    id: &str,
    role: &str,
    listen: &str,
    control: Option<&str>,
    issuer: Option<(&Path, &Path)>,
) -> Result<String, Box<dyn Error>> {
    let address = control.map_or_else(String::new, |address| format!("address = \"{address}\"\n"));
    let issuer = issuer.map_or_else(String::new, |(cert, key)| {
        format!("issuer_cert = {cert:?}\nissuer_key = {key:?}\n")
    });
    // Data nodes record row changes for every database, so `WatchChanges` serves them.
    let sink = if role == "data" {
        format!(
            "\n[[changes.sink]]\nname = \"log\"\nkind = \"file\"\nformat = \"logical\"\n\
             databases = [\"*\"]\npath = {:?}\n",
            dir.join(format!("{id}-sink"))
        )
    } else {
        String::new()
    };
    Ok(format!(
        "data_dir = {:?}\nlisten = \"{listen}\"\nrole = \"{role}\"\n\n[node]\nid = \
         \"{id}\"\n\n[membership]\nheartbeat_interval_ms = {HEARTBEAT_MS}\n\n[control]\n\
         {address}{issuer}heartbeat_timeout_ms = {HEARTBEAT_TIMEOUT_MS}\n{sink}{}",
        dir.join(id),
        tls.node_table(id)?,
    ))
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

/// A request revoking the token with ID `id`.
fn token_request(id: &str) -> RevokeTokenRequest {
    RevokeTokenRequest {
        revocation: Some(Revocation {
            revoked: Some(Revoked::TokenId(id.to_owned())),
            until_ms: u64::MAX,
        }),
        principal: String::new(),
    }
}

/// A request revoking `principal`, and the token with ID `id` after it if given.
fn principal_request(principal: &str, id: Option<&str>) -> RevokeTokenRequest {
    RevokeTokenRequest {
        principal: principal.to_owned(),
        ..id.map_or_else(RevokeTokenRequest::default, token_request)
    }
}

impl Fleet {
    async fn start() -> Result<Self, Box<dyn Error>> {
        let dir = tempfile::tempdir()?;
        let tls = Tls::create(dir.path())?;
        let now = SystemClock.now().as_millis();
        let start = Timestamp::from_millis(now - now % 1_000);
        let issuer_cert = dir.path().join("issuer.crt");
        let issuer_key = dir.path().join("issuer.key");
        let fleet_tls = &tls;
        write_issuer(fleet_tls, "control-a", start, &issuer_cert, &issuer_key)?;
        let address = format!("127.0.0.1:{}", free_port()?);
        let control_config = node_config(
            dir.path(),
            &tls,
            "control",
            "control",
            &address,
            None,
            Some((&issuer_cert, &issuer_key)),
        )?;
        let control = Daemon::start(Config::from_toml(&control_config)?).await?;
        let mut data = Vec::new();
        for id in ["data-1", "data-2"] {
            data.push(
                Daemon::start(Config::from_toml(&node_config(
                    dir.path(),
                    &tls,
                    id,
                    "data",
                    "127.0.0.1:0",
                    Some(&address),
                    None,
                )?)?)
                .await?,
            );
        }
        let operator = Client::new(address.clone(), tls.client_network("tester")?);
        let fleet = Self {
            dir,
            tls,
            control: Some(control),
            control_config,
            data,
            address,
            operator,
            issuer_cert,
            issuer_key,
            start,
        };
        fleet.members(2).await?;
        for database in DATABASES {
            let name: DatabaseName = database.parse()?;
            fleet
                .operator
                .create_database_with(&name, DatabaseSpec::new(ReplicationFactor::ONE))
                .await?;
            fleet
                .operator
                .execute(
                    &name,
                    Statement::new("CREATE TABLE t (id INTEGER PRIMARY KEY)"),
                )
                .await?;
        }
        Ok(fleet)
    }

    /// Waits until the control node answers with `count` members.
    async fn members(&self, count: usize) -> TestResult {
        tokio::time::timeout(HANG_GUARD, async {
            loop {
                match self.operator.status().await {
                    Ok(view) if view.members().len() == count => return,
                    _ => tokio::time::sleep(Duration::from_millis(50)).await,
                }
            }
        })
        .await?;
        Ok(())
    }

    async fn stop(mut self) {
        for data in self.data.drain(..) {
            data.run_until(async {}).await;
        }
        if let Some(control) = self.control.take() {
            control.run_until(async {}).await;
        }
        drop(self.dir);
    }

    /// Moves the control node to issuer key B, as `change` says.
    async fn rotate_to_b(&mut self, change: Change) -> TestResult {
        match change {
            Change::Reload => write_issuer(
                &self.tls,
                "control-b",
                self.start,
                &self.issuer_cert,
                &self.issuer_key,
            ),
            Change::Restart => {
                if let Some(control) = self.control.take() {
                    control.run_until(async {}).await;
                }
                write_issuer(
                    &self.tls,
                    "control-b",
                    self.start,
                    &self.issuer_cert,
                    &self.issuer_key,
                )?;
                // On the same port, which the data nodes keep dialling.
                self.control = Some(Daemon::start(Config::from_toml(&self.control_config)?).await?);
                self.members(2).await
            }
        }
    }

    /// `agent-42`'s root token, signed now by the authority, as `agent add` signs it.
    fn root(&self, id: &str) -> Result<BearerToken, Box<dyn Error>> {
        let now = SystemClock.now();
        Ok(self.tls.authority().token_signer()?.issue(
            &TokenSpec::new(
                "agent-42".parse()?,
                scopes(&ROOT_SCOPES)?,
                id.parse()?,
                self.start,
                self.start.saturating_add(Millis::new(3 * 3_600_000)),
            )
            .signed_at(now)
            .agent("alice".parse()?),
        )?)
    }

    /// The control client of a connection with `agent-42`'s certificate, carrying `token`.
    fn control(&self, token: &BearerToken) -> Result<ControlClient, Box<dyn Error>> {
        Ok(with_token(self.tls.client_network("agent-42")?, token)?
            .control_client(self.address.clone()))
    }

    /// A data client with `agent-42`'s certificate, carrying `token`.
    fn client(&self, token: &BearerToken) -> Result<Client, Box<dyn Error>> {
        Ok(Client::new(
            self.address.clone(),
            with_token(self.tls.client_network("agent-42")?, token)?,
        ))
    }

    /// Delegates `segment` from `parent` under operation `operation`.
    async fn delegate(
        &self,
        parent: &BearerToken,
        operation: u8,
        segment: &str,
        scopes: &[&str],
    ) -> Result<DelegateTokenResponse, Status> {
        self.control(parent)
            .map_err(|error| Status::internal(error.to_string()))?
            .delegate_token(DelegateTokenRequest {
                operation_id: vec![operation; 16],
                segment: segment.to_owned(),
                scopes: scopes.iter().map(|scope| (*scope).to_owned()).collect(),
                ..DelegateTokenRequest::default()
            })
            .await
    }

    /// Revokes with `fleet`, as an operator does.
    async fn revoke_as_operator(&self, request: RevokeTokenRequest) -> TestResult {
        self.operator.control().revoke_token(request).await?;
        Ok(())
    }

    /// Revokes as `token`, under the ancestor rule, and returns the principal entry's `before`
    /// (milliseconds) when the request named a principal.
    async fn revoke_as(
        &self,
        token: &BearerToken,
        request: RevokeTokenRequest,
    ) -> Result<Option<u64>, Box<dyn Error>> {
        let answer = self.control(token)?.revoke_token(request.clone()).await?;
        if !request.principal.is_empty() {
            assert!(answer.before_ms.is_some(), "{answer:?}");
        }
        Ok(answer.before_ms)
    }
}

/// Writes a new issuer pair named `name`, valid for 60 days from an hour before `start`, over
/// `cert` and `key`: the key first, so a reload between the two finds a mismatch and keeps its
/// pair until the next check.
fn write_issuer(tls: &Tls, name: &str, start: Timestamp, cert: &Path, key: &Path) -> TestResult {
    let from = Timestamp::from_millis(start.as_millis().saturating_sub(3_600_000));
    let pair = tls.authority().issue_issuer(
        &name.parse()?,
        Validity::new(from, Duration::from_secs(60 * 24 * 3600)),
    )?;
    std::fs::write(key, pair.key_pem())?;
    std::fs::write(cert, pair.certificate_pem())?;
    Ok(())
}

/// A token of the lineage, the database it reads, and its open stream, if it has one.
struct Holder {
    name: &'static str,
    token: BearerToken,
    database: &'static str,
    stream: Option<ChangeUpdates>,
}

impl Holder {
    fn new(name: &'static str, token: BearerToken, database: &'static str) -> Self {
        Self {
            name,
            token,
            database,
            stream: None,
        }
    }

    /// Opens a `WatchChanges` stream on its database, with its own token.
    async fn watch(mut self, fleet: &Fleet) -> Result<Self, Box<dyn Error>> {
        let client = fleet.client(&self.token)?;
        let stream = client.watch_changes(&self.database.parse()?, None).await?;
        self.stream = Some(stream);
        Ok(self)
    }

    async fn read(&self, fleet: &Fleet) -> Result<(), Status> {
        let client = fleet
            .client(&self.token)
            .map_err(|error| Status::internal(error.to_string()))?;
        let name: DatabaseName = self
            .database
            .parse()
            .map_err(|_| Status::internal("a database name"))?;
        client
            .query(&name, Statement::new("SELECT count(*) FROM t"))
            .await
            .map(|_| ())
    }
}

/// Section 14's lineage.
struct Lineage {
    root: Holder,
    c7_a: Holder,
    c7_b: Holder,
    r1: Holder,
    r2: Holder,
    c7_id: String,
    a_key: String,
}

/// Steps 1 to 6 of section 14, moving to issuer key B by `change` after step 1.
async fn lineage(fleet: &mut Fleet, change: Change) -> Result<Lineage, Box<dyn Error>> {
    let root = fleet.root(ROOT_ID)?;
    // 1. Under A.
    let c7_a = fleet.delegate(&root, 1, "c7", &C7_SCOPES).await?;
    let a_key = c7_a.issuer_key_id.clone();
    // 2. To B.
    fleet.rotate_to_b(change).await?;
    // 3. `r1` from c7's A copy, signed once the node holds B: a resend under the same operation
    //    is signed again with the pair the node holds.
    let c7_a_token = BearerToken::new(c7_a.token.clone());
    let r1 = tokio::time::timeout(HANG_GUARD, async {
        loop {
            let r1 = fleet.delegate(&c7_a_token, 2, "r1", &[]).await?;
            if r1.issuer_key_id != a_key {
                return Ok::<_, Status>(r1);
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    })
    .await??;
    // 4. The requester of step 1 resends it: the same delegation, signed by B.
    let c7_b = fleet.delegate(&root, 1, "c7", &C7_SCOPES).await?;
    assert_eq!(c7_b.delegation_id, c7_a.delegation_id);
    assert_eq!(c7_b.issuer_key_id, r1.issuer_key_id);
    assert_ne!(c7_b.token, c7_a.token);
    // 5. `r2` from the B copy.
    let c7_b_token = BearerToken::new(c7_b.token.clone());
    let r2 = fleet.delegate(&c7_b_token, 3, "r2", &[]).await?;
    // The chains: r1's ends with c7's A copy, r2's with its B copy.
    let state = fleet
        .control
        .as_ref()
        .and_then(Daemon::control)
        .ok_or("no control")?
        .state();
    let last_key = |id: &str| {
        state
            .delegations()
            .find(|(recorded, _)| recorded.to_string() == id)
            .and_then(|(_, record)| record.claims.chain.last().map(|link| link.kid.to_string()))
    };
    assert_eq!(last_key(&r1.delegation_id), Some(a_key.clone()));
    assert_eq!(
        last_key(&r2.delegation_id),
        Some(c7_b.issuer_key_id.clone())
    );
    // 6. Streams.
    Ok(Lineage {
        root: Holder::new("agent-42", root, "agent-42-notes"),
        c7_a: Holder::new("c7 (A)", c7_a_token, "agent-42.c7-notes")
            .watch(fleet)
            .await?,
        c7_b: Holder::new("c7 (B)", c7_b_token, "agent-42.c7-notes")
            .watch(fleet)
            .await?,
        r1: Holder::new("r1", BearerToken::new(r1.token), "agent-42.c7.r1-notes")
            .watch(fleet)
            .await?,
        r2: Holder::new("r2", BearerToken::new(r2.token), "agent-42.c7.r2-notes")
            .watch(fleet)
            .await?,
        c7_id: c7_a.delegation_id,
        a_key,
    })
}

/// Waits until `holder`'s token is refused on its database's data node as revoked, and returns
/// how long that took.
async fn refused(fleet: &Fleet, holder: &Holder) -> Result<Duration, Box<dyn Error>> {
    let started = tokio::time::Instant::now();
    let status = tokio::time::timeout(HANG_GUARD, async {
        loop {
            match holder.read(fleet).await {
                Ok(()) => tokio::time::sleep(Duration::from_millis(10)).await,
                Err(status) => return status,
            }
        }
    })
    .await?;
    assert_eq!(
        status.code(),
        Code::Unauthenticated,
        "{}: {status}",
        holder.name
    );
    assert_eq!(status.message(), "the token is revoked", "{}", holder.name);
    Ok(started.elapsed())
}

/// Checks what a case ends and what survives: each ended token is refused on its data node
/// within the heartbeat timeout, its stream ends within [`STREAM_RECHECK`], and it delegates
/// nothing; each survivor still reads, and its stream stays open.
async fn check(
    fleet: &Fleet,
    case: &str,
    ended: &mut [&mut Holder],
    survive: &mut [&mut Holder],
) -> TestResult {
    for holder in ended.iter() {
        let waited = refused(fleet, holder).await?;
        assert!(
            waited < Duration::from_millis(HEARTBEAT_TIMEOUT_MS),
            "{case}: {} was refused after {waited:?}",
            holder.name
        );
    }
    let refused_at = tokio::time::Instant::now();
    for holder in ended.iter_mut() {
        if let Some(stream) = holder.stream.as_mut() {
            let ended = tokio::time::timeout(HANG_GUARD, async {
                loop {
                    match stream.next().await {
                        Ok(Some(_)) => {}
                        Ok(None) => return Status::cancelled("ended without a refusal"),
                        Err(status) => return status,
                    }
                }
            })
            .await?;
            assert_eq!(
                ended.code(),
                Code::Unauthenticated,
                "{case}: {}: {ended}",
                holder.name
            );
            assert!(
                refused_at.elapsed() < STREAM_RECHECK + RECHECK_SLACK,
                "{case}: {}'s stream ended {:?} after its token was refused",
                holder.name,
                refused_at.elapsed()
            );
        }
        // I4: an ended token can't delegate.
        let late = fleet.delegate(&holder.token, 99, "late", &[]).await;
        assert!(late.is_err(), "{case}: {} delegated: {late:?}", holder.name);
    }
    for holder in survive.iter_mut() {
        holder
            .read(fleet)
            .await
            .map_err(|status| format!("{case}: {} must survive: {status}", holder.name))?;
        if let Some(stream) = holder.stream.as_mut() {
            match tokio::time::timeout(STREAM_RECHECK + RECHECK_SLACK, stream.next()).await {
                Err(_) | Ok(Ok(Some(_))) => {}
                Ok(other) => panic!("{case}: {}'s stream ended: {other:?}", holder.name),
            }
        }
    }
    Ok(())
}

// Case 1: `Revoked::Key(A)` ends c7's A copy and r1, minted from it; c7's B copy and r2 survive.
async fn key_a(change: Change) -> TestResult {
    let mut fleet = Fleet::start().await?;
    let mut l = lineage(&mut fleet, change).await?;
    fleet
        .revoke_as_operator(RevokeTokenRequest {
            revocation: Some(Revocation {
                revoked: Some(Revoked::KeyId(l.a_key.clone())),
                until_ms: u64::MAX,
            }),
            principal: String::new(),
        })
        .await?;
    check(
        &fleet,
        "key A",
        &mut [&mut l.c7_a, &mut l.r1],
        &mut [&mut l.c7_b, &mut l.r2, &mut l.root],
    )
    .await?;
    fleet.stop().await;
    Ok(())
}

// Case 2: `Revoked::Token(c7)`, by the operator or by `agent-42`, ends both copies of c7, r1 and
// r2; agent-42 survives.
async fn c7_token(change: Change, by: By) -> TestResult {
    let mut fleet = Fleet::start().await?;
    let mut l = lineage(&mut fleet, change).await?;
    match by {
        By::Operator => fleet.revoke_as_operator(token_request(&l.c7_id)).await?,
        By::Ancestor => {
            fleet
                .revoke_as(&l.root.token, token_request(&l.c7_id))
                .await?;
        }
    }
    check(
        &fleet,
        "c7's token",
        &mut [&mut l.c7_a, &mut l.c7_b, &mut l.r1, &mut l.r2],
        &mut [&mut l.root],
    )
    .await?;
    fleet.stop().await;
    Ok(())
}

/// Who revokes in case 2.
#[derive(Debug, Clone, Copy)]
enum By {
    Operator,
    Ancestor,
}

// Case 3: the operator revokes the root token's ID, which ends everything below it.
async fn root_token(change: Change) -> TestResult {
    let mut fleet = Fleet::start().await?;
    let mut l = lineage(&mut fleet, change).await?;
    fleet.revoke_as_operator(token_request(ROOT_ID)).await?;
    check(
        &fleet,
        "the root token",
        &mut [&mut l.root, &mut l.c7_a, &mut l.c7_b, &mut l.r1, &mut l.r2],
        &mut [],
    )
    .await?;
    fleet.stop().await;
    Ok(())
}

// Case 4: `agent-42` revokes `agent-42.c7` as a principal: c7, r1 and r2 end; agent-42 survives.
async fn c7_principal(change: Change) -> TestResult {
    let mut fleet = Fleet::start().await?;
    let mut l = lineage(&mut fleet, change).await?;
    fleet
        .revoke_as(&l.root.token, principal_request("agent-42.c7", None))
        .await?;
    check(
        &fleet,
        "c7's principal",
        &mut [&mut l.c7_a, &mut l.c7_b, &mut l.r1, &mut l.r2],
        &mut [&mut l.root],
    )
    .await?;
    fleet.stop().await;
    Ok(())
}

// Case 5: `agent remove 42`, `agent-42`'s principal and its token in one request, by itself:
// everything at or below it ends, and a new root token minted more than 30 s later works.
async fn remove_root(change: Change) -> TestResult {
    let mut fleet = Fleet::start().await?;
    let mut l = lineage(&mut fleet, change).await?;
    let before = fleet
        .revoke_as(&l.root.token, principal_request("agent-42", Some(ROOT_ID)))
        .await?
        .ok_or("the principal revocation answered no before")?;
    check(
        &fleet,
        "agent remove 42",
        &mut [&mut l.root, &mut l.c7_a, &mut l.c7_b, &mut l.r1, &mut l.r2],
        &mut [],
    )
    .await?;
    // From the entry's own `before`, not from when the call was sent: under load the control
    // plane can apply the revocation a little later. Nodes compare `iat` in whole seconds with
    // 30 s of leeway, so the first token that works is signed in the first whole second after
    // `before` + 30 s, the time `agent remove` prints. Read by the wall clock the signer reads,
    // which may be stepped while a sleep runs.
    let after = Timestamp::from_millis((before.saturating_add(30_000) / 1_000 + 1) * 1_000);
    while SystemClock.now() < after {
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    let renewed = Holder::new(
        "a new agent-42 token",
        fleet.root("root-42-new")?,
        "agent-42-notes",
    );
    renewed.read(&fleet).await?;
    fleet.stop().await;
    Ok(())
}

// Case 6, a reused segment: `agent-42` revokes c7 by ID, delegates `42.c7` again (c7′), which
// delegates r1′; then `agent-42` revokes `agent-42.c7` as a principal. r1, r2, c7′ and r1′ end,
// and no entry the principal's replaced revives r1 or r2; agent-42 survives.
async fn reused_segment(change: Change) -> TestResult {
    let mut fleet = Fleet::start().await?;
    let mut l = lineage(&mut fleet, change).await?;
    fleet
        .revoke_as(&l.root.token, token_request(&l.c7_id))
        .await?;
    let c7_again = fleet.delegate(&l.root.token, 4, "c7", &C7_SCOPES).await?;
    assert_ne!(c7_again.delegation_id, l.c7_id);
    let c7_again = BearerToken::new(c7_again.token);
    let r1_again = fleet.delegate(&c7_again, 5, "r1", &[]).await?;
    let mut c7_again = Holder::new("c7′", c7_again, "agent-42.c7-notes")
        .watch(&fleet)
        .await?;
    let mut r1_again = Holder::new(
        "r1′",
        BearerToken::new(r1_again.token),
        "agent-42.c7.r1-again",
    )
    .watch(&fleet)
    .await?;
    c7_again.read(&fleet).await?;
    r1_again.read(&fleet).await?;
    fleet
        .revoke_as(&l.root.token, principal_request("agent-42.c7", None))
        .await?;
    check(
        &fleet,
        "a reused segment",
        &mut [
            &mut l.c7_a,
            &mut l.c7_b,
            &mut l.r1,
            &mut l.r2,
            &mut c7_again,
            &mut r1_again,
        ],
        &mut [&mut l.root],
    )
    .await?;
    fleet.stop().await;
    Ok(())
}

macro_rules! both_ways {
    ($($reload:ident, $restart:ident => $case:expr;)+) => {
        $(
            #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
            async fn $reload() -> TestResult {
                let case = $case;
                case(Change::Reload).await
            }

            #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
            async fn $restart() -> TestResult {
                let case = $case;
                case(Change::Restart).await
            }
        )+
    };
}

both_ways! {
    key_a_ends_the_a_copy_and_r1_after_a_reload,
    key_a_ends_the_a_copy_and_r1_after_a_restart => key_a;
    c7_token_by_the_operator_ends_c7_r1_r2_after_a_reload,
    c7_token_by_the_operator_ends_c7_r1_r2_after_a_restart => |change| c7_token(change, By::Operator);
    c7_token_by_agent_42_ends_c7_r1_r2_after_a_reload,
    c7_token_by_agent_42_ends_c7_r1_r2_after_a_restart => |change| c7_token(change, By::Ancestor);
    root_token_ends_everything_below_after_a_reload,
    root_token_ends_everything_below_after_a_restart => root_token;
    c7_principal_ends_c7_r1_r2_after_a_reload,
    c7_principal_ends_c7_r1_r2_after_a_restart => c7_principal;
    agent_remove_42_ends_everything_and_a_later_root_token_works_after_a_reload,
    agent_remove_42_ends_everything_and_a_later_root_token_works_after_a_restart => remove_root;
    reused_segment_revives_nothing_after_a_reload,
    reused_segment_revives_nothing_after_a_restart => reused_segment;
}
