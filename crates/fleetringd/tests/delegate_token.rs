//! FR-091: `Control.DelegateToken` and `Control.ListDelegations` against one dev node, over real
//! TCP with mutual TLS (ADR-0047, points 5 and 6.4).
//!
//! Its root agents' tokens are signed in the test by the dev authority, as `agent add` signs
//! them, with `fleetring.kind`, `fleetring.owner`, and `grant`; the dev node's own
//! delegation issuer, which dev mode creates, signs every child.
//!
//! Each test names the invariant of ADR-0047 point 13 it checks. Cases that need a clock the
//! test controls, or an issuer pair dev mode would replace, run in the simulation
//! (`tests/sim/delegation.rs`); the rules' edges are unit tests in `control/delegation.rs`.

use std::error::Error;
use std::io::Write as _;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use fleetring::ReplicationFactor;
use fleetring_control::delegation::{DelegationId, TokenClaims};
use fleetring_control::{Action, ControlConfig, DatabaseName, Feature, Millis, Timestamp};
use fleetring_proto::v1::revocation::Revoked;
use fleetring_proto::v1::{
    DelegateTokenRequest, DelegateTokenResponse, ListDelegationsRequest, Revocation,
    RevokeTokenRequest, WatchMembershipRequest,
};
use fleetringd::auth::{BearerToken, Scopes, TokenSigner, TokenSpec};
use fleetringd::clock::{Clock as _, SystemClock};
use fleetringd::control::{CommandLog, ControlClient};
use fleetringd::data::{Client, DatabaseSpec};
use fleetringd::tls::{IssuerFiles, IssuerPair, Validity};
use fleetringd::transport::Credentials;
use fleetringd::{Config, Daemon, Network};
use tokio_stream::StreamExt as _;
use tonic::{Code, Status};

use crate::support::Tls;

mod support;

type TestResult = Result<(), Box<dyn Error>>;

/// Guards against a hang only.
const HANG_GUARD: Duration = Duration::from_secs(60);

const HOUR: u64 = 3_600_000;

/// The `[[principal]]` entry of a relay: no scopes of its own, its callers' tokens forwarded.
const RELAY: &str = "\n[[principal]]\nname = \"router\"\nscopes = []\nforward_tokens = true\n";

/// One dev node, its authority, and an operator.
struct Dev {
    _dir: tempfile::TempDir,
    tls: Tls,
    daemon: Daemon,
    address: String,
    authority: TokenSigner,
    /// The time the test's tokens start, in whole seconds.
    now: Timestamp,
    /// The next operation ID's distinguishing byte.
    next_operation: Mutex<u8>,
}

impl Dev {
    async fn start() -> Result<Self, Box<dyn Error>> {
        Self::start_as(Mode::Dev).await
    }

    async fn start_as(mode: Mode) -> Result<Self, Box<dyn Error>> {
        let dir = tempfile::tempdir()?;
        let tls = Tls::create(dir.path())?;
        let dev = match mode {
            Mode::Dev => "dev = true\n",
            Mode::InlineNotDev => "",
        };
        let engine = match mode {
            Mode::Dev => "",
            Mode::InlineNotDev => "\n[engine]\nexecution = \"inline\"\n",
        };
        let config = format!(
            "{dev}data_dir = {:?}\nlisten = \"127.0.0.1:0\"\nrole = \"both\"\n\n[node]\nid = \"dev\"\n\
             \n[membership]\nheartbeat_interval_ms = 100\n\n[replication]\nreplication_factor = 1\n\
             {engine}{}{RELAY}",
            dir.path().join("data"),
            tls.node_table("dev")?,
        );
        let daemon = Daemon::start(Config::from_toml(&config)?).await?;
        let address = daemon
            .control_address()
            .ok_or("no control address")?
            .to_string();
        let operator = Client::new(address.clone(), tls.client_network("tester")?);
        tokio::time::timeout(HANG_GUARD, async {
            loop {
                match operator.status().await {
                    Ok(view) if view.members().len() == 1 => return Ok::<_, Status>(()),
                    _ => tokio::time::sleep(Duration::from_millis(50)).await,
                }
            }
        })
        .await??;
        let now = SystemClock.now().as_millis();
        Ok(Self {
            authority: tls.authority().token_signer()?,
            now: Timestamp::from_millis(now - now % 1_000),
            address,
            daemon,
            tls,
            _dir: dir,
            next_operation: Mutex::new(1),
        })
    }

    async fn stop(self) {
        self.daemon.run_until(async {}).await;
    }

    /// Stops the node and returns its directory, which outlives it, for a look at its files.
    async fn stop_keeping_files(self) -> tempfile::TempDir {
        self.daemon.run_until(async {}).await;
        self._dir
    }

    fn at(&self, offset: u64) -> Timestamp {
        self.now.saturating_add(Millis::new(offset))
    }

    /// A root agent's token, as `agent add` signs it once every member reports `delegation`.
    fn root(
        &self,
        agent: &str,
        id: &str,
        scope_texts: &[&str],
        expires_at: Timestamp,
    ) -> Result<BearerToken, Box<dyn Error>> {
        Ok(self.authority.issue(
            &TokenSpec::new(
                agent.parse()?,
                scopes(scope_texts)?,
                id.parse()?,
                self.now,
                expires_at,
            )
            .agent("alice".parse()?),
        )?)
    }

    /// `agent-42`'s read-write root token, valid an hour: it may delegate within its namespace.
    fn root_42(&self) -> Result<BearerToken, Box<dyn Error>> {
        self.root(
            "agent-42",
            "root-42",
            &[
                "admin:agent-42-*",
                "admin:agent-42.*",
                "stream:agent-42-*",
                "stream:agent-42.*",
                "grant:agent-42.*",
            ],
            self.at(HOUR),
        )
    }

    /// The control client of a connection whose certificate names `certificate`, carrying
    /// `token`.
    fn control(
        &self,
        certificate: &str,
        token: &BearerToken,
    ) -> Result<ControlClient, Box<dyn Error>> {
        Ok(with_token(self.tls.client_network(certificate)?, token)?
            .control_client(self.address.clone()))
    }

    /// A data client of a connection whose certificate names `certificate`, carrying `token`.
    fn client(&self, certificate: &str, token: &BearerToken) -> Result<Client, Box<dyn Error>> {
        Ok(Client::new(
            self.address.clone(),
            with_token(self.tls.client_network(certificate)?, token)?,
        ))
    }

    /// The operator: `tester`'s certificate, with `fleet`.
    fn operator(&self) -> Result<ControlClient, Box<dyn Error>> {
        Ok(self
            .tls
            .client_network("tester")?
            .control_client(self.address.clone()))
    }

    /// A request for the child `segment` with a fresh operation ID and the defaults.
    fn request(&self, segment: &str) -> DelegateTokenRequest {
        let mut next = self.next_operation.lock().expect("lock");
        let operation = vec![*next; 16];
        *next = next.wrapping_add(1);
        DelegateTokenRequest {
            operation_id: operation,
            segment: segment.to_owned(),
            ..DelegateTokenRequest::default()
        }
    }

    /// Delegates as `agent-42`, from its root token, on its own certificate.
    async fn delegate_42(
        &self,
        request: DelegateTokenRequest,
    ) -> Result<DelegateTokenResponse, Status> {
        let control = self
            .control("agent-42", &self.root_42().map_err(internal)?)
            .map_err(internal)?;
        control.delegate_token(request).await
    }

    /// The claims control state recorded for `delegation`.
    fn recorded(&self, delegation: &str) -> Result<TokenClaims, Box<dyn Error>> {
        let id: DelegationId = delegation.parse()?;
        let state = self.daemon.control().ok_or("no control")?.state();
        let claims = state
            .delegations()
            .find(|(recorded, _)| **recorded == id)
            .map(|(_, record)| record.claims.clone())
            .ok_or("not recorded")?;
        Ok(claims)
    }

    async fn revoke(&self, revoked: Revoked) -> TestResult {
        self.operator()?
            .revoke_token(RevokeTokenRequest {
                revocation: Some(Revocation {
                    revoked: Some(revoked),
                    until_ms: self.at(2 * HOUR).as_millis(),
                }),
                principal: String::new(),
            })
            .await?;
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    Dev,
    InlineNotDev,
}

fn internal(error: Box<dyn Error>) -> Status {
    Status::internal(error.to_string())
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

fn strings(texts: &[&str]) -> Vec<String> {
    texts.iter().map(|text| (*text).to_owned()).collect()
}

fn assert_refused(result: Result<DelegateTokenResponse, Status>, code: Code, says: &str) {
    match result {
        Ok(response) => panic!(
            "delegated {} where {code:?} was expected",
            response.principal
        ),
        Err(status) => {
            assert_eq!(status.code(), code, "{status:?}");
            assert!(
                status.message().contains(says),
                "{:?} doesn't say {says:?}",
                status.message()
            );
        }
    }
}

/// Creates `database` with `client`, as a database's own agent does.
async fn create(client: &Client, database: &str) -> Result<(), Status> {
    let name: DatabaseName = database
        .parse()
        .map_err(|_| Status::invalid_argument("name"))?;
    client
        .create_database_with(&name, DatabaseSpec::new(ReplicationFactor::ONE))
        .await
        .map(|_| ())
}

// I1: what a child gets.

#[tokio::test]
async fn root_default_child_gets_admin_and_stream_on_its_prefix_capped_and_said() -> TestResult {
    let dev = Dev::start().await?;
    let granted = dev.delegate_42(dev.request("c7")).await?;
    assert_eq!(granted.principal, "agent-42.c7");
    assert_eq!(
        granted.scopes,
        strings(&["admin:agent-42.c7-*", "stream:agent-42.c7-*"])
    );
    // The root token expires within the default 2 hours, so the child's ends with it.
    assert!(granted.capped, "{granted:?}");
    assert_eq!(granted.expires_at_ms, dev.at(HOUR).as_millis());
    let issuer = dev.daemon.issuer().ok_or("no issuer")?;
    assert_eq!(
        granted.issuer_key_id,
        issuer.certificate().key_id().to_string()
    );
    // The token works, carried by the root's certificate, within its prefix and nowhere else.
    let child = dev.client("agent-42", &BearerToken::new(granted.token.clone()))?;
    create(&child, "agent-42.c7-scratch").await?;
    for outside in ["agent-42-x", "agent-43-x", "agent-42.c70-x"] {
        let refused = create(&child, outside).await.expect_err(outside);
        assert_eq!(
            refused.code(),
            Code::PermissionDenied,
            "{outside}: {refused:?}"
        );
    }
    // A root token lasting longer gives the full 2 hours, not capped.
    let long = dev.root(
        "agent-42",
        "root-42-long",
        &["admin:agent-42.*", "grant:agent-42.*"],
        dev.at(10 * HOUR),
    )?;
    let full = dev
        .control("agent-42", &long)?
        .delegate_token(dev.request("c8"))
        .await?;
    assert!(!full.capped);
    assert_eq!(full.expires_at_ms - full.not_before_ms, 2 * HOUR);
    dev.stop().await;
    Ok(())
}

#[tokio::test]
async fn read_only_root_default_child_gets_read_and_stream() -> TestResult {
    let dev = Dev::start().await?;
    let root = dev.root(
        "agent-42",
        "root-42-ro",
        &[
            "read:agent-42-*",
            "read:agent-42.*",
            "stream:agent-42.*",
            "grant:agent-42.*",
        ],
        dev.at(HOUR),
    )?;
    let granted = dev
        .control("agent-42", &root)?
        .delegate_token(dev.request("c7"))
        .await?;
    assert_eq!(
        granted.scopes,
        strings(&["read:agent-42.c7-*", "stream:agent-42.c7-*"])
    );
    // I1: admin asked of a read-only root isn't covered.
    let mut admin = dev.request("c8");
    admin.scopes = strings(&["admin:agent-42.c8-*"]);
    assert_refused(
        dev.control("agent-42", &root)?.delegate_token(admin).await,
        Code::PermissionDenied,
        "admin:agent-42.c8-*",
    );
    dev.stop().await;
    Ok(())
}

#[tokio::test]
async fn scopes_outside_the_grant_or_the_tier_are_refused() -> TestResult {
    let dev = Dev::start().await?;
    // no_grant_is_refused
    let no_grant = dev.root(
        "agent-42",
        "root-42-ng",
        &["admin:agent-42-*", "admin:agent-42.*"],
        dev.at(HOUR),
    )?;
    assert_refused(
        dev.control("agent-42", &no_grant)?
            .delegate_token(dev.request("c7"))
            .await,
        Code::PermissionDenied,
        "your token holds no grant over agent-42.c7-",
    );
    // write_agent_42_memory_outside_the_grant_is_refused
    let mut memory = dev.request("c7");
    memory.scopes = strings(&["write:agent-42-memory"]);
    assert_refused(
        dev.delegate_42(memory).await,
        Code::PermissionDenied,
        "write:agent-42-memory",
    );
    // fleet_requested_is_refused
    let mut fleet = dev.request("c7");
    fleet.scopes = strings(&["fleet"]);
    assert_refused(
        dev.delegate_42(fleet).await,
        Code::PermissionDenied,
        "fleet",
    );
    // grant_agent_42_star_for_a_child_is_refused_by_the_tier
    let mut wide_grant = dev.request("c7");
    wide_grant.scopes = strings(&["grant:agent-42.*"]);
    assert_refused(
        dev.delegate_42(wide_grant).await,
        Code::InvalidArgument,
        "grant:agent-42.*",
    );
    // Nothing was recorded by any refusal.
    let state = dev.daemon.control().ok_or("no control")?.state();
    assert_eq!(state.delegations().len(), 0);
    dev.stop().await;
    Ok(())
}

#[tokio::test]
async fn lifetime_remaining_plus_one_second_25h_and_59s_are_refused() -> TestResult {
    let dev = Dev::start().await?;
    for (lifetime, says) in [
        (HOUR + 1_000, "would outlast your token"),
        (25 * HOUR, "at most 24 h"),
        (59_000, "at least 60 s"),
    ] {
        let mut request = dev.request("c7");
        request.lifetime_ms = Some(lifetime);
        assert_refused(dev.delegate_42(request).await, Code::InvalidArgument, says);
    }
    let mut minute = dev.request("c7");
    minute.lifetime_ms = Some(60_000);
    let granted = dev.delegate_42(minute).await?;
    assert_eq!(granted.expires_at_ms - granted.not_before_ms, 60_000);
    assert!(!granted.capped);
    dev.stop().await;
    Ok(())
}

#[tokio::test]
async fn not_before_31s_past_24h_and_1s_ahead_or_at_parent_exp_is_refused() -> TestResult {
    let dev = Dev::start().await?;
    let now = SystemClock.now().as_millis();
    let long = dev.root(
        "agent-42",
        "root-42-2d",
        &["admin:agent-42.*", "grant:agent-42.*"],
        dev.at(48 * HOUR),
    )?;
    for (not_before, root, says) in [
        (now - 31_000, &long, "30 s in the past"),
        (now + 24 * HOUR + 1_000, &long, "24 h ahead"),
        (
            dev.at(HOUR).as_millis(),
            &dev.root_42()?,
            "before your token expires",
        ),
    ] {
        let mut request = dev.request("c7");
        request.not_before_ms = Some(not_before);
        assert_refused(
            dev.control("agent-42", root)?.delegate_token(request).await,
            Code::InvalidArgument,
            says,
        );
    }
    let mut ahead = dev.request("c7");
    ahead.not_before_ms = Some(now + HOUR);
    let granted = dev
        .control("agent-42", &long)?
        .delegate_token(ahead)
        .await?;
    assert_eq!(granted.not_before_ms, (now + HOUR) - (now + HOUR) % 1_000);
    dev.stop().await;
    Ok(())
}

#[tokio::test]
async fn bad_segments_long_names_and_depth_nine_are_refused() -> TestResult {
    let dev = Dev::start().await?;
    // segments_with_dot_dash_or_uppercase_are_refused
    for bad in ["c.7", "c-7", "C7", ""] {
        assert_refused(
            dev.delegate_42(dev.request(bad)).await,
            Code::InvalidArgument,
            "segment",
        );
    }
    // child_name_over_64_bytes_is_refused
    let long_root = format!("agent-{}", "a".repeat(30));
    let root = dev.root(
        &long_root,
        "root-long",
        &[
            &format!("admin:{long_root}.*"),
            &format!("grant:{long_root}.*"),
        ],
        dev.at(HOUR),
    )?;
    assert_refused(
        dev.control(&long_root, &root)?
            .delegate_token(dev.request(&"b".repeat(30)))
            .await,
        Code::InvalidArgument,
        "isn't an agent name",
    );
    // depth_nine_is_refused: each level delegates the next with a grant over its children.
    let mut token = dev.root(
        "agent-1",
        "root-1",
        &["admin:agent-1.*", "grant:agent-1.*"],
        dev.at(HOUR),
    )?;
    let mut name = "agent-1".to_owned();
    for segment in ["a", "b", "c", "d", "e", "f", "g", "h"] {
        let child = format!("{name}.{segment}");
        let mut request = dev.request(segment);
        request.scopes = vec![format!("admin:{child}.*"), format!("grant:{child}.*")];
        let granted = dev
            .control("agent-1", &token)?
            .delegate_token(request)
            .await?;
        token = BearerToken::new(granted.token);
        name = child;
    }
    assert_refused(
        dev.control("agent-1", &token)?
            .delegate_token(dev.request("i"))
            .await,
        Code::ResourceExhausted,
        "at most 8 levels below the root",
    );
    dev.stop().await;
    Ok(())
}

#[tokio::test]
async fn delegation_whose_token_would_exceed_8_kib_is_refused_before_recording() -> TestResult {
    let dev = Dev::start().await?;
    let mut request = dev.request("c7");
    request.scopes = (0..200)
        .map(|n| format!("admin:agent-42.c7-database-with-a-long-name-{n:03}"))
        .collect();
    assert_refused(
        dev.delegate_42(request).await,
        Code::InvalidArgument,
        "the child's token would exceed 8 KiB; ask for fewer scopes",
    );
    let state = dev.daemon.control().ok_or("no control")?.state();
    assert_eq!(state.delegations().len(), 0, "nothing is recorded");
    assert_eq!(state.delegation_operations().len(), 0);
    dev.stop().await;
    Ok(())
}

// The caller (ADR-0047, points 4.5 and 5.2) and the issuer (point 1.6).

#[tokio::test]
async fn token_without_kind_or_service_token_cannot_delegate() -> TestResult {
    let dev = Dev::start().await?;
    // A root agent's name, but no kind: as 0.7's `agent add` signed it.
    let kindless = dev.authority.issue(&TokenSpec::new(
        "agent-42".parse()?,
        scopes(&["admin:agent-42.*", "grant:agent-42.*"])?,
        "root-42-old".parse()?,
        dev.now,
        dev.at(HOUR),
    ))?;
    assert_refused(
        dev.control("agent-42", &kindless)?
            .delegate_token(dev.request("c7"))
            .await,
        Code::PermissionDenied,
        "only an agent's token can delegate",
    );
    let service = dev.authority.issue(&TokenSpec::new(
        "workload".parse()?,
        scopes(&["admin:workload-*", "grant:workload-*"])?,
        "svc".parse()?,
        dev.now,
        dev.at(HOUR),
    ))?;
    assert_refused(
        dev.control("workload", &service)?
            .delegate_token(dev.request("c7"))
            .await,
        Code::PermissionDenied,
        "only an agent's token can delegate",
    );
    dev.stop().await;
    Ok(())
}

#[tokio::test]
async fn certificate_alone_cannot_delegate() -> TestResult {
    let dev = Dev::start().await?;
    let refused = dev
        .operator()?
        .delegate_token(dev.request("c7"))
        .await
        .expect_err("a certificate alone");
    assert_eq!(refused.code(), Code::PermissionDenied, "{refused:?}");
    assert!(refused.message().contains("bearer token"), "{refused:?}");
    dev.stop().await;
    Ok(())
}

/// I9: a relay-carried token never delegates, root or delegated.
#[tokio::test]
async fn relay_carried_token_cannot_delegate() -> TestResult {
    let dev = Dev::start().await?;
    let root = dev.root_42()?;
    assert_refused(
        dev.control("router", &root)?
            .delegate_token(dev.request("c7"))
            .await,
        Code::PermissionDenied,
        "delegation needs a direct connection with the certificate of the caller or an ancestor",
    );
    let mut with_grant = dev.request("c7");
    with_grant.scopes = strings(&["admin:agent-42.c7.*", "grant:agent-42.c7.*"]);
    let c7 = BearerToken::new(dev.delegate_42(with_grant).await?.token);
    assert_refused(
        dev.control("router", &c7)?
            .delegate_token(dev.request("r1"))
            .await,
        Code::PermissionDenied,
        "direct connection",
    );
    // The same child token on the root's own certificate delegates.
    dev.control("agent-42", &c7)?
        .delegate_token(dev.request("r1"))
        .await?;
    dev.stop().await;
    Ok(())
}

#[tokio::test]
async fn revoked_issuer_is_failed_precondition() -> TestResult {
    let dev = Dev::start().await?;
    let issuer = dev.daemon.issuer().ok_or("no issuer")?;
    let key = issuer.certificate().key_id().to_string();
    dev.revoke(Revoked::KeyId(key)).await?;
    assert_refused(
        dev.delegate_42(dev.request("c7")).await,
        Code::FailedPrecondition,
        "has a revoked key; issue a new pair with `fleetring cert issuer`",
    );
    dev.stop().await;
    Ok(())
}

// Grandchildren.

#[tokio::test]
async fn child_with_grant_delegates_within_its_subtree_and_never_outlives_its_parent() -> TestResult
{
    let dev = Dev::start().await?;
    let mut request = dev.request("c7");
    request.scopes = strings(&[
        "admin:agent-42.c7-*",
        "admin:agent-42.c7.*",
        "stream:agent-42.c7.*",
        "grant:agent-42.c7.*",
    ]);
    request.lifetime_ms = Some(10 * 60_000);
    let c7 = dev.delegate_42(request).await?;
    let c7_token = BearerToken::new(c7.token.clone());
    let r1 = dev
        .control("agent-42", &c7_token)?
        .delegate_token(dev.request("r1"))
        .await?;
    assert_eq!(r1.principal, "agent-42.c7.r1");
    assert_eq!(
        r1.scopes,
        strings(&["admin:agent-42.c7.r1-*", "stream:agent-42.c7.r1-*"])
    );
    assert!(r1.capped);
    assert_eq!(r1.expires_at_ms, c7.expires_at_ms);
    let recorded = dev.recorded(&r1.delegation_id)?;
    assert_eq!(recorded.chain.len(), 2);
    assert_eq!(recorded.chain[1].id.to_string(), c7.delegation_id);
    assert_eq!(recorded.chain[1].kid.to_string(), c7.issuer_key_id);
    let r1_client = dev.client("agent-42", &BearerToken::new(r1.token))?;
    create(&r1_client, "agent-42.c7.r1-notes").await?;
    // Outside its own subtree, a child's grant covers nothing.
    let mut sibling = dev.request("r2");
    sibling.scopes = strings(&["admin:agent-42.d1-*"]);
    assert_refused(
        dev.control("agent-42", &c7_token)?
            .delegate_token(sibling)
            .await,
        Code::PermissionDenied,
        "admin:agent-42.d1-*",
    );
    dev.stop().await;
    Ok(())
}

// I7: retries.

#[tokio::test]
async fn retries_return_the_same_delegation_and_reveal_nothing_to_others() -> TestResult {
    let dev = Dev::start().await?;
    let mut request = dev.request("c7");
    request.scopes = strings(&["stream:agent-42.c7-*", "admin:agent-42.c7-*"]);
    let first = dev.delegate_42(request.clone()).await?;
    // retry_same_key_returns_identical_bytes
    let again = dev.delegate_42(request.clone()).await?;
    assert_eq!(again, first, "the same key signs the same bytes");
    // retry_with_scopes_reordered_returns_the_same_delegation
    let mut reordered = request.clone();
    reordered.scopes.reverse();
    assert_eq!(
        dev.delegate_42(reordered).await?.delegation_id,
        first.delegation_id
    );
    // retry_from_another_principal_reveals_nothing
    let other = dev.root(
        "agent-43",
        "root-43",
        &["admin:agent-43.*", "grant:agent-43.*"],
        dev.at(HOUR),
    )?;
    let refused = dev
        .control("agent-43", &other)?
        .delegate_token(request.clone())
        .await
        .expect_err("another principal");
    assert_eq!(refused.code(), Code::PermissionDenied);
    assert!(refused.message().ends_with("isn't yours"), "{refused:?}");
    assert!(!refused.message().contains("agent-42"), "{refused:?}");
    // retry_with_another_field_is_already_exists
    let mut longer = request.clone();
    longer.lifetime_ms = Some(HOUR / 2);
    assert_refused(
        dev.delegate_42(longer).await,
        Code::AlreadyExists,
        "lifetime",
    );
    let mut renamed = request.clone();
    renamed.segment = "c8".to_owned();
    assert_refused(
        dev.delegate_42(renamed).await,
        Code::AlreadyExists,
        "segment",
    );
    let state = dev.daemon.control().ok_or("no control")?.state();
    assert_eq!(state.delegations().len(), 1, "recorded once");
    // retry_of_a_revoked_delegation_is_refused_never_reminted
    dev.revoke(Revoked::TokenId(first.delegation_id.clone()))
        .await?;
    assert_refused(
        dev.delegate_42(request).await,
        Code::FailedPrecondition,
        "this operation's delegation is revoked",
    );
    dev.stop().await;
    Ok(())
}

/// I7 across a rotation: a resend after the issuer pair changed returns the same delegation,
/// signed by the new key, and both copies work.
#[tokio::test]
async fn retry_after_rotation_returns_same_id_and_claims_signed_by_b_and_both_verify() -> TestResult
{
    let dev = Dev::start().await?;
    let request = dev.request("c7");
    let a = dev.delegate_42(request.clone()).await?;
    // Rotate: a new pair from the dev authority, in place of the files the node loaded.
    let issuer = dev.daemon.issuer().ok_or("no issuer")?;
    let files = issuer.files().clone();
    let start = Timestamp::from_millis(dev.now.as_millis().saturating_sub(HOUR));
    let pair = dev.tls.authority().issue_issuer(
        &"dev".parse()?,
        Validity::new(start, Duration::from_secs(60 * 24 * 3600)),
    )?;
    std::fs::File::create(&files.key)?.write_all(pair.key_pem().as_bytes())?;
    std::fs::File::create(&files.cert)?.write_all(pair.certificate_pem().as_bytes())?;
    let b = tokio::time::timeout(HANG_GUARD, async {
        loop {
            let resent = dev.delegate_42(request.clone()).await?;
            if resent.issuer_key_id != a.issuer_key_id {
                return Ok::<_, Status>(resent);
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    })
    .await??;
    assert_eq!(b.delegation_id, a.delegation_id);
    assert_eq!(
        (
            b.principal.as_str(),
            &b.scopes,
            b.not_before_ms,
            b.expires_at_ms
        ),
        (
            a.principal.as_str(),
            &a.scopes,
            a.not_before_ms,
            a.expires_at_ms
        )
    );
    assert_ne!(b.token, a.token);
    let state = dev.daemon.control().ok_or("no control")?.state();
    assert_eq!(state.delegations().len(), 1, "one delegation, two copies");
    for (database, copy) in [("agent-42.c7-a", &a), ("agent-42.c7-b", &b)] {
        let client = dev.client("agent-42", &BearerToken::new(copy.token.clone()))?;
        create(&client, database).await?;
    }
    dev.stop().await;
    Ok(())
}

// Limits.

/// A request `apply` refuses for a limit costs the control log nothing: the writer answers the
/// refusal without logging the command (ADR-0047, points 5.10 and 6.2).
#[tokio::test]
async fn rate_limited_requests_log_nothing() -> TestResult {
    let dev = Dev::start().await?;
    for n in 0..30 {
        dev.delegate_42(dev.request(&format!("c{n}"))).await?;
    }
    for n in 30..50 {
        assert_refused(
            dev.delegate_42(dev.request(&format!("c{n}"))).await,
            Code::ResourceExhausted,
            "",
        );
    }
    let dir = dev.stop_keeping_files().await;
    let log = CommandLog::open(dir.path().join("data/control.db"), ControlConfig::new())?;
    let recorded = log
        .commands()?
        .into_iter()
        .filter(|(_, command)| matches!(command.action, Action::RecordDelegation { .. }))
        .count();
    assert_eq!(recorded, 30, "only the accepted delegations are logged");
    Ok(())
}

// I6: forged lineage.

#[tokio::test]
async fn forged_token_with_unrecorded_id_cannot_delegate() -> TestResult {
    let dev = Dev::start().await?;
    let mut request = dev.request("c7");
    request.scopes = strings(&["admin:agent-42.c7.*", "grant:agent-42.c7.*"]);
    let c7 = dev.delegate_42(request).await?;
    let mut forged = dev.recorded(&c7.delegation_id)?;
    forged.id = DelegationId::from_bytes([0xee; 16]).into();
    let signer = dev.daemon.issuer().ok_or("no issuer")?.signer()?;
    let token = signer.sign(&forged)?;
    assert_refused(
        dev.control("agent-42", &token)?
            .delegate_token(dev.request("r1"))
            .await,
        Code::PermissionDenied,
        "the caller's lineage is forged: its delegation isn't recorded",
    );
    dev.stop().await;
    Ok(())
}

#[tokio::test]
async fn issuer_key_resigned_real_claims_with_wider_scopes_cannot_delegate() -> TestResult {
    let dev = Dev::start().await?;
    let mut request = dev.request("c7");
    request.scopes = strings(&["admin:agent-42.c7.*", "grant:agent-42.c7.*"]);
    let c7 = dev.delegate_42(request).await?;
    let mut widened = dev.recorded(&c7.delegation_id)?;
    widened.scopes = scopes(&["admin:agent-42.*", "grant:agent-42.c7.*"])?.normalized();
    let signer = dev.daemon.issuer().ok_or("no issuer")?.signer()?;
    let token = signer.sign(&widened)?;
    assert_refused(
        dev.control("agent-42", &token)?
            .delegate_token(dev.request("r1"))
            .await,
        Code::PermissionDenied,
        "its fleetring.scopes differs from its record",
    );
    dev.stop().await;
    Ok(())
}

/// A widened forgery of `c7` resending the operation under which `c7` delegated `r1` is
/// refused before the resend is answered, so it never receives `r1`: the handler checks the
/// caller's lineage, which a resend's `apply` never sees (ADR-0047, point 5.9).
#[tokio::test]
async fn widened_forgery_cannot_resend_its_originals_delegation() -> TestResult {
    let dev = Dev::start().await?;
    let mut request = dev.request("c7");
    request.scopes = strings(&["admin:agent-42.c7.*", "grant:agent-42.c7.*"]);
    let c7 = dev.delegate_42(request).await?;
    let c7_token = BearerToken::new(c7.token.clone());
    let operation = dev.request("r1");
    let r1 = dev
        .control("agent-42", &c7_token)?
        .delegate_token(operation.clone())
        .await?;
    let mut widened = dev.recorded(&c7.delegation_id)?;
    widened.scopes = scopes(&["admin:agent-42.*", "grant:agent-42.c7.*"])?.normalized();
    let signer = dev.daemon.issuer().ok_or("no issuer")?.signer()?;
    let forged = signer.sign(&widened)?;
    assert_refused(
        dev.control("agent-42", &forged)?
            .delegate_token(operation.clone())
            .await,
        Code::PermissionDenied,
        "its fleetring.scopes differs from its record",
    );
    // The genuine copy still receives it.
    let again = dev
        .control("agent-42", &c7_token)?
        .delegate_token(operation)
        .await?;
    assert_eq!(again, r1);
    dev.stop().await;
    Ok(())
}

/// A copy of `c7`'s exact claims that another issuer key signed, as a stolen key could, can't
/// resend the operation under which the original copy delegated `r1`, whose chain names the
/// original's key; a new delegation from it records a child whose chain names the other key
/// (ADR-0047, point 8, amendment of 2026-11-13).
#[tokio::test]
async fn copy_signed_by_another_issuer_key_cannot_resend_and_its_children_name_that_key()
-> TestResult {
    let dev = Dev::start().await?;
    let mut request = dev.request("c7");
    request.scopes = strings(&["admin:agent-42.c7.*", "grant:agent-42.c7.*"]);
    let c7 = dev.delegate_42(request).await?;
    let c7_token = BearerToken::new(c7.token.clone());
    let operation = dev.request("r1");
    let r1 = dev
        .control("agent-42", &c7_token)?
        .delegate_token(operation.clone())
        .await?;

    // Another issuer pair the dev authority certified, which the node doesn't use.
    let dir = tempfile::tempdir()?;
    let files = IssuerFiles {
        cert: dir.path().join("issuer.pem"),
        key: dir.path().join("issuer.key"),
    };
    let start = Timestamp::from_millis(dev.now.as_millis().saturating_sub(HOUR));
    let pair = dev.tls.authority().issue_issuer(
        &"dev".parse()?,
        Validity::new(start, Duration::from_secs(60 * 24 * 3600)),
    )?;
    std::fs::write(&files.cert, pair.certificate_pem())?;
    std::fs::write(&files.key, pair.key_pem())?;
    let other = IssuerPair::load(
        &files,
        &dev.tls.node_identity("dev")?,
        Arc::new(SystemClock),
    )?
    .signer()?;
    assert_ne!(other.key_id().to_string(), c7.issuer_key_id);
    let copy = other.sign(&dev.recorded(&c7.delegation_id)?)?;

    assert_refused(
        dev.control("agent-42", &copy)?
            .delegate_token(operation.clone())
            .await,
        Code::AlreadyExists,
        "signing key",
    );
    let again = dev
        .control("agent-42", &c7_token)?
        .delegate_token(operation)
        .await?;
    assert_eq!(again, r1);
    let r2 = dev
        .control("agent-42", &copy)?
        .delegate_token(dev.request("r2"))
        .await?;
    let chain = dev.recorded(&r2.delegation_id)?.chain;
    assert_eq!(
        chain.last().map(|link| link.kid.to_string()),
        Some(other.key_id().to_string())
    );
    dev.stop().await;
    Ok(())
}

// Reporting the feature (ADR-0047, points 6.6 and 12; ADR-0038).

#[tokio::test]
async fn non_dev_daemon_reports_delegation_which_enables_nothing_until_first_use() -> TestResult {
    // Neither dev mode nor the simulation: every node of this release reports `delegation`.
    let dev = Dev::start_as(Mode::InlineNotDev).await?;
    let mut updates = dev
        .operator()?
        .watch_membership(WatchMembershipRequest { after_epoch: 0 })
        .await?;
    let update = tokio::time::timeout(HANG_GUARD, updates.next())
        .await?
        .ok_or("no membership update")??;
    let software = update.members[0].software.as_ref().ok_or("no software")?;
    assert!(
        software
            .features
            .iter()
            .any(|feature| feature == "delegation"),
        "{:?}",
        software.features
    );
    // Reporting it enables nothing: a refused delegation, here for want of an issuer, leaves
    // the fleet able to roll back to 0.7.
    assert_refused(
        dev.delegate_42(dev.request("c7")).await,
        Code::FailedPrecondition,
        "issuer",
    );
    let state = dev.daemon.control().ok_or("no control")?.state();
    assert!(!state.is_enabled(Feature::Delegation));
    dev.stop().await;
    Ok(())
}

// `ListDelegations`.

#[tokio::test]
async fn list_delegations_needs_fleet_and_never_returns_tokens() -> TestResult {
    let dev = Dev::start().await?;
    let c7 = dev.delegate_42(dev.request("c7")).await?;
    let listed = dev
        .operator()?
        .list_delegations(ListDelegationsRequest::default())
        .await?;
    assert_eq!(listed.delegations.len(), 1);
    let record = &listed.delegations[0];
    assert_eq!(record.principal, "agent-42.c7");
    assert_eq!(record.delegation_id, c7.delegation_id);
    assert_eq!(record.parent, "agent-42");
    assert_eq!(record.chain.len(), 1);
    assert_eq!(record.chain[0].id, "root-42");
    assert_eq!(record.recorded_by, "dev");
    assert_eq!(record.expires_at_ms, c7.expires_at_ms);
    assert!(
        !format!("{listed:?}").contains(&c7.token),
        "a listing holds no token"
    );
    let refused = dev
        .control("agent-42", &dev.root_42()?)?
        .list_delegations(ListDelegationsRequest::default())
        .await
        .expect_err("needs fleet");
    assert_eq!(refused.code(), Code::PermissionDenied);
    dev.stop().await;
    Ok(())
}

#[tokio::test]
async fn list_delegations_filters_by_recorded_by_and_window() -> TestResult {
    let dev = Dev::start().await?;
    let c7 = dev.delegate_42(dev.request("c7")).await?;
    let recorded_at = dev
        .operator()?
        .list_delegations(ListDelegationsRequest::default())
        .await?
        .delegations[0]
        .recorded_at_ms;
    let operator = dev.operator()?;
    let list = |recorded_by: Option<&str>, from: Option<u64>, to: Option<u64>| {
        let operator = operator.clone();
        let request = ListDelegationsRequest {
            recorded_by: recorded_by.map(str::to_owned),
            recorded_from_ms: from,
            recorded_to_ms: to,
        };
        async move { operator.list_delegations(request).await }
    };
    assert_eq!(list(Some("dev"), None, None).await?.delegations.len(), 1);
    assert_eq!(list(Some("other"), None, None).await?.delegations.len(), 0);
    let window = list(None, Some(recorded_at), Some(recorded_at)).await?;
    assert_eq!(window.delegations[0].delegation_id, c7.delegation_id);
    assert!(
        list(None, Some(recorded_at + 1), None)
            .await?
            .delegations
            .is_empty()
    );
    assert!(
        list(None, None, Some(recorded_at - 1))
            .await?
            .delegations
            .is_empty()
    );
    let refused = list(Some("not a node id!"), None, None)
        .await
        .expect_err("invalid");
    assert_eq!(refused.code(), Code::InvalidArgument);
    dev.stop().await;
    Ok(())
}

// I8: no token bytes in a log.

/// Collects every log line the test's thread writes.
#[derive(Clone, Default)]
struct Captured(Arc<Mutex<Vec<u8>>>);

impl std::io::Write for Captured {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.lock().expect("lock").extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for Captured {
    type Writer = Self;

    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

/// A delegation, a retry, and five refusals, with every log line of the node's request handling
/// captured at the most verbose level: no token, the parent's or the child's, appears in any.
#[tokio::test]
async fn delegation_never_logs_the_token() -> TestResult {
    let captured = Captured::default();
    let subscriber = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::TRACE)
        .with_writer(captured.clone())
        .finish();
    // The process-wide default, not a thread's: tracing caches each log site's interest across
    // threads, so a site another test reached first without a subscriber would stay silent for a
    // thread-local one. Only this test sets it; other tests' lines land here too, harmlessly.
    tracing::subscriber::set_global_default(subscriber)?;
    let dev = Dev::start().await?;
    let root = dev.root_42()?;
    let request = dev.request("c7");
    let granted = dev.delegate_42(request.clone()).await?;
    let resent = dev.delegate_42(request).await?;
    assert_eq!(resent.token, granted.token);
    let mut fleet = dev.request("c8");
    fleet.scopes = strings(&["fleet"]);
    let _refused = dev.delegate_42(fleet).await.expect_err("fleet");
    let mut long = dev.request("c8");
    long.lifetime_ms = Some(25 * HOUR);
    let _refused = dev.delegate_42(long).await.expect_err("too long");
    let _refused = dev
        .delegate_42(dev.request("C8"))
        .await
        .expect_err("segment");
    let _refused = dev
        .control("router", &root)?
        .delegate_token(dev.request("c8"))
        .await
        .expect_err("relayed");
    let child = BearerToken::new(granted.token.clone());
    let _refused = dev
        .control("agent-42", &child)?
        .delegate_token(dev.request("r1"))
        .await
        .expect_err("a default child has no grant");
    dev.stop().await;
    let logs = String::from_utf8(captured.0.lock().expect("lock").clone())?;
    assert!(
        logs.contains("delegated a token"),
        "the audit line is logged"
    );
    assert!(logs.contains("refused a delegation"), "refusals are logged");
    assert!(
        logs.contains(&granted.delegation_id),
        "the audit line names the ID"
    );
    for token in [root.expose(), granted.token.as_str()] {
        let signature = token.rsplit('.').next().ok_or("a JWS")?;
        assert!(!logs.contains(token), "a token reached the log");
        assert!(
            !logs.contains(signature),
            "a token's signature reached the log"
        );
    }
    Ok(())
}
