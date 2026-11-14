//! FR-093: `Control.RevokeToken` under the ancestor rule and for principals, against one dev
//! node over real TCP with mutual TLS (ADR-0047, points 4.5 and 11, invariants I5, I9, and
//! I10).
//!
//! Root agents' tokens are signed in the test by the dev authority, as `agent add` signs
//! them; the dev node's own delegation issuer signs every child, through `DelegateToken`. The
//! ancestor rule's reach itself is `apply`'s, tested in `fleetring-control`; these tests pin what
//! the handler adds: the caller it builds, the relay's own-ID exception, keys, the status of each
//! refusal, the reply, and that refusals are never logged.

use std::error::Error;
use std::sync::Mutex;
use std::time::Duration;

use fleetring_control::{Action, ControlConfig, Millis, Timestamp};
use fleetring_proto::PROTOCOL_VERSION;
use fleetring_proto::v1::revocation::Revoked;
use fleetring_proto::v1::{
    DelegateTokenRequest, DelegateTokenResponse, JoinRequest, Node, PrincipalRevocation,
    Revocation, RevokeTokenRequest, RevokeTokenResponse, Software,
};
use fleetringd::auth::{BearerToken, STREAM_RECHECK, Scopes, TokenSigner, TokenSpec};
use fleetringd::clock::{Clock as _, SystemClock};
use fleetringd::control::{CommandLog, ControlClient};
use fleetringd::data::Client;
use fleetringd::transport::{Credentials, control_client_with};
use fleetringd::{Config, Daemon, Network};
use tonic::{Code, Status};

use crate::support::Tls;

type TestResult = Result<(), Box<dyn Error>>;

/// Guards against a hang only.
const HANG_GUARD: Duration = Duration::from_secs(60);

const HOUR: u64 = 3_600_000;

/// Room for a loaded runner on top of [`STREAM_RECHECK`] between a revocation landing and the
/// stream it ends reporting so.
const RECHECK_SLACK: Duration = Duration::from_secs(1);

/// The `[[principal]]` entries: a relay, which forwards its callers' tokens, and a service.
const PRINCIPALS: &str = "\n[[principal]]\nname = \"router\"\nscopes = []\nforward_tokens = true\n\
     \n[[principal]]\nname = \"workload\"\nscopes = []\n";

/// What the ancestor rule's refusal of a relay-carried token says.
const RELAYED: &str = "a relay-carried token may revoke only its own token id";

/// One dev node, its authority, and an operator.
struct Dev {
    dir: tempfile::TempDir,
    tls: Tls,
    daemon: Daemon,
    address: String,
    authority: TokenSigner,
    /// The time the test's tokens start, in whole seconds.
    now: Timestamp,
    next_operation: Mutex<u8>,
}

impl Dev {
    async fn start() -> Result<Self, Box<dyn Error>> {
        let dir = tempfile::tempdir()?;
        let tls = Tls::create(dir.path())?;
        let config = format!(
            "dev = true\ndata_dir = {:?}\nlisten = \"127.0.0.1:0\"\nrole = \"both\"\n\n[node]\n\
             id = \"dev\"\n\n[membership]\nheartbeat_interval_ms = 100\n\n[replication]\n\
             replication_factor = 1\n{}{PRINCIPALS}",
            dir.path().join("data"),
            tls.node_table("dev")?,
        );
        let daemon = Daemon::start(Config::from_toml(&config)?).await?;
        let address = daemon
            .control_address()
            .ok_or("no control address")?
            .to_string();
        let dev = Self {
            authority: tls.authority().token_signer()?,
            now: Timestamp::ZERO,
            address,
            daemon,
            tls,
            dir,
            next_operation: Mutex::new(1),
        };
        dev.members(1).await?;
        let now = SystemClock.now().as_millis();
        Ok(Self {
            now: Timestamp::from_millis(now - now % 1_000),
            ..dev
        })
    }

    /// Waits until the fleet has `count` members.
    async fn members(&self, count: usize) -> TestResult {
        let operator = Client::new(self.address.clone(), self.tls.client_network("tester")?);
        tokio::time::timeout(HANG_GUARD, async {
            loop {
                match operator.status().await {
                    Ok(view) if view.members().len() == count => return Ok::<_, Status>(()),
                    _ => tokio::time::sleep(Duration::from_millis(50)).await,
                }
            }
        })
        .await??;
        Ok(())
    }

    async fn stop(self) {
        self.daemon.run_until(async {}).await;
    }

    /// Stops the node and returns its directory, for a look at its log.
    async fn stop_keeping_files(self) -> tempfile::TempDir {
        self.daemon.run_until(async {}).await;
        self.dir
    }

    fn at(&self, offset: u64) -> Timestamp {
        self.now.saturating_add(Millis::new(offset))
    }

    /// A root agent's token, as `agent add` signs it once every member reports `delegation`.
    fn root(&self, agent: &str, id: &str) -> Result<BearerToken, Box<dyn Error>> {
        let scopes: Scopes = [
            format!("admin:{agent}-*"),
            format!("admin:{agent}.*"),
            format!("stream:{agent}-*"),
            format!("stream:{agent}.*"),
            format!("grant:{agent}.*"),
        ]
        .iter()
        .map(|text| text.parse())
        .collect::<Result<_, _>>()?;
        Ok(self.authority.issue(
            &TokenSpec::new(agent.parse()?, scopes, id.parse()?, self.now, self.at(HOUR))
                .agent("alice".parse()?),
        )?)
    }

    fn root_42(&self) -> Result<BearerToken, Box<dyn Error>> {
        self.root("agent-42", "root-42")
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

    /// A request for the child `segment` with a fresh operation ID, and `scopes` or the
    /// defaults.
    fn request(&self, segment: &str, scopes: &[&str]) -> DelegateTokenRequest {
        let mut next = self.next_operation.lock().expect("lock");
        let operation = vec![*next; 16];
        *next = next.wrapping_add(1);
        DelegateTokenRequest {
            operation_id: operation,
            segment: segment.to_owned(),
            scopes: scopes.iter().map(|scope| (*scope).to_owned()).collect(),
            ..DelegateTokenRequest::default()
        }
    }

    /// Delegates `segment` from `parent`'s token, carried by `agent-42`'s certificate.
    async fn delegate(
        &self,
        parent: &BearerToken,
        segment: &str,
        scopes: &[&str],
    ) -> Result<DelegateTokenResponse, Box<dyn Error>> {
        Ok(self
            .control("agent-42", parent)?
            .delegate_token(self.request(segment, scopes))
            .await?)
    }

    /// Revokes as `token` on the connection of `certificate`.
    async fn revoke_as(
        &self,
        certificate: &str,
        token: &BearerToken,
        request: RevokeTokenRequest,
    ) -> Result<RevokeTokenResponse, Status> {
        self.control(certificate, token)
            .map_err(|error| Status::internal(error.to_string()))?
            .revoke_token(request)
            .await
    }
}

fn with_token(network: Network, token: &BearerToken) -> Result<Network, Box<dyn Error>> {
    Ok(network.with_credentials(Credentials::bearer(token)?))
}

/// A request revoking the token with ID `id`.
fn token(id: &str) -> RevokeTokenRequest {
    RevokeTokenRequest {
        revocation: Some(Revocation {
            revoked: Some(Revoked::TokenId(id.to_owned())),
            until_ms: u64::MAX,
        }),
        principal: String::new(),
    }
}

/// A request revoking `principal`.
fn principal(principal: &str) -> RevokeTokenRequest {
    RevokeTokenRequest {
        revocation: None,
        principal: principal.to_owned(),
    }
}

/// A request revoking `principal` and the token with ID `id` at once.
fn both(principal: &str, id: &str) -> RevokeTokenRequest {
    RevokeTokenRequest {
        principal: principal.to_owned(),
        ..token(id)
    }
}

fn assert_refused(result: Result<RevokeTokenResponse, Status>, code: Code, says: &str) {
    match result {
        Ok(response) => panic!("revoked ({response:?}) where {code:?} was expected"),
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

/// Whether `client`'s token still holds: a call that needs it.
async fn holds(client: &Client) -> Result<(), Status> {
    client.list_databases().await.map(|_| ())
}

async fn assert_revoked(client: &Client, who: &str) {
    let refused = holds(client).await.expect_err(who);
    assert_eq!(refused.code(), Code::Unauthenticated, "{who}: {refused:?}");
    assert_eq!(refused.message(), "the token is revoked", "{who}");
}

/// How many commands the log holds that revoke under the ancestor rule or a principal.
fn logged_revocations(dir: &tempfile::TempDir) -> Result<usize, Box<dyn Error>> {
    let log = CommandLog::open(dir.path().join("data/control.db"), ControlConfig::new())?;
    Ok(log
        .commands()?
        .into_iter()
        .filter(|(_, command)| {
            matches!(
                command.action,
                Action::RevokeAsAncestor { .. } | Action::RevokePrincipal { .. }
            )
        })
        .count())
}

/// I5 over the RPC: revoking `agent-42.c7` by principal, with `fleet` or by `agent-42` under the
/// ancestor rule, ends `c7`'s token on its next request and its open stream within
/// [`STREAM_RECHECK`], while `agent-42` keeps working. The reply states `before`, which is
/// control time, and the entry's `until`.
#[tokio::test]
async fn principal_revocation_ends_tokens_and_open_streams_within_recheck() -> TestResult {
    let dev = Dev::start().await?;
    let root = dev.root_42()?;
    let c7 = BearerToken::new(dev.delegate(&root, "c7", &[]).await?.token);
    let d1 = BearerToken::new(dev.delegate(&root, "d1", &[]).await?.token);
    let (c7_client, d1_client) = (dev.client("agent-42", &c7)?, dev.client("agent-42", &d1)?);
    holds(&c7_client).await?;
    holds(&d1_client).await?;
    let mut c7_leases = c7_client.watch_leases(&dev.address).await?;
    let mut d1_leases = d1_client.watch_leases(&dev.address).await?;

    // With `fleet`.
    let asked = SystemClock.now();
    let revoked = dev
        .operator()?
        .revoke_token(principal("agent-42.c7"))
        .await?;
    let before = revoked.before_ms.ok_or("no before")?;
    assert!(before >= asked.as_millis(), "{revoked:?}");
    assert_eq!(revoked.principal_until_ms, Some(revoked.until_ms));
    assert!(revoked.until_ms > before + 30 * 24 * HOUR, "{revoked:?}");
    let landed = tokio::time::Instant::now();
    assert_revoked(&c7_client, "c7").await;
    let ended = tokio::time::timeout(HANG_GUARD, async {
        loop {
            match c7_leases.next().await {
                Ok(Some(_)) => {}
                Ok(None) => return Err(Status::internal("ended without a refusal")),
                Err(status) => return Ok(status),
            }
        }
    })
    .await??;
    assert_eq!(ended.message(), "the token is revoked");
    assert!(
        landed.elapsed() <= STREAM_RECHECK + RECHECK_SLACK,
        "ended after {:?}",
        landed.elapsed()
    );

    // By `agent-42`, under the ancestor rule.
    let revoked = dev
        .revoke_as("agent-42", &root, principal("agent-42.d1"))
        .await?;
    assert!(revoked.before_ms.is_some(), "{revoked:?}");
    assert_revoked(&d1_client, "d1").await;
    let ended = tokio::time::timeout(HANG_GUARD, async {
        loop {
            match d1_leases.next().await {
                Ok(Some(_)) => {}
                Ok(None) => return Err(Status::internal("ended without a refusal")),
                Err(status) => return Ok(status),
            }
        }
    })
    .await??;
    assert_eq!(ended.message(), "the token is revoked");

    holds(&dev.client("agent-42", &root)?).await?;
    dev.stop().await;
    Ok(())
}

/// I10, what the rule allows: an agent revokes a delegation recorded below it by its ID or by
/// its principal (a grandchild included), the token it presented, and itself; a root removes
/// itself by principal and token ID in one request, as `agent remove <root>` will.
#[tokio::test]
async fn ancestor_rule_allows_self_own_token_and_recorded_delegations_below() -> TestResult {
    let dev = Dev::start().await?;
    let root = dev.root_42()?;
    let c7 = dev.delegate(&root, "c7", &[]).await?;
    let d1 = dev.delegate(&root, "d1", &[]).await?;
    let e7 = BearerToken::new(
        dev.delegate(
            &root,
            "e7",
            &[
                "admin:agent-42.e7-*",
                "admin:agent-42.e7.*",
                "grant:agent-42.e7.*",
            ],
        )
        .await?
        .token,
    );
    let r1 = BearerToken::new(dev.delegate(&e7, "r1", &[]).await?.token);
    let f1 = dev.delegate(&root, "f1", &[]).await?;

    // A child by its delegation ID: no `before`, and its token ends.
    let revoked = dev
        .revoke_as("agent-42", &root, token(&c7.delegation_id))
        .await?;
    assert_eq!(revoked.before_ms, None);
    assert_revoked(&dev.client("agent-42", &BearerToken::new(c7.token))?, "c7").await;
    // A grandchild by its principal, from the root.
    dev.revoke_as("agent-42", &root, principal("agent-42.e7.r1"))
        .await?;
    assert_revoked(&dev.client("agent-42", &r1)?, "r1").await;
    holds(&dev.client("agent-42", &e7)?).await?;
    // A child revokes the token it presented.
    let d1_token = BearerToken::new(d1.token);
    dev.revoke_as("agent-42", &d1_token, token(&d1.delegation_id))
        .await?;
    assert_revoked(&dev.client("agent-42", &d1_token)?, "d1").await;
    // A child revokes itself by principal and its own token ID at once: the principal covers
    // the token, so the request succeeds.
    let f1_token = BearerToken::new(f1.token);
    let revoked = dev
        .revoke_as(
            "agent-42",
            &f1_token,
            both("agent-42.f1", &f1.delegation_id),
        )
        .await?;
    assert!(revoked.before_ms.is_some(), "{revoked:?}");
    assert_revoked(&dev.client("agent-42", &f1_token)?, "f1").await;

    // The root removes itself: its principal and its own token ID, in one request.
    let revoked = dev
        .revoke_as("agent-42", &root, both("agent-42", "root-42"))
        .await?;
    let before = revoked.before_ms.ok_or("no before")?;
    let principal_until = revoked.principal_until_ms.ok_or("no principal until")?;
    assert!(principal_until > before + 30 * 24 * HOUR, "{revoked:?}");
    assert!(revoked.until_ms > before, "{revoked:?}");
    assert_ne!(revoked.until_ms, principal_until, "the token's own entry");
    assert_revoked(&dev.client("agent-42", &root)?, "agent-42").await;
    assert_revoked(&dev.client("agent-42", &e7)?, "e7").await;
    dev.stop().await;
    Ok(())
}

/// Point 11.7 under a full rate window: after 29 revocations in the minute, and again after 30,
/// `agent remove <root>`'s request lands both its principal and its own token ID, as one
/// command, so the root token's clock-free handle is never lost to the caller's rate.
#[tokio::test]
async fn a_root_removes_itself_in_one_command_whatever_its_rate() -> TestResult {
    for spent in [29_usize, 30] {
        let dev = Dev::start().await?;
        let root = dev.root_42()?;
        for n in 0..spent {
            let child = dev.delegate(&root, &format!("l{n}"), &[]).await?;
            dev.revoke_as("agent-42", &root, token(&child.delegation_id))
                .await?;
        }
        let revoked = dev
            .revoke_as("agent-42", &root, both("agent-42", "root-42"))
            .await?;
        let principal_until = revoked.principal_until_ms.ok_or("no principal until")?;
        assert_ne!(revoked.until_ms, principal_until, "the token's own entry");
        assert_revoked(&dev.client("agent-42", &root)?, "agent-42").await;
        let dir = dev.stop_keeping_files().await;
        assert_eq!(logged_revocations(&dir)?, spent + 1, "after {spent}");
    }
    Ok(())
}

/// A principal and a token the principal covers, named together by any caller, succeed: the
/// token, a delegation recorded below the principal, gets no entry of its own, and is never
/// reported unrevoked.
#[tokio::test]
async fn a_principal_and_a_token_it_covers_succeed_for_any_caller() -> TestResult {
    let dev = Dev::start().await?;
    let root = dev.root_42()?;
    let e7 = BearerToken::new(
        dev.delegate(
            &root,
            "e7",
            &[
                "admin:agent-42.e7-*",
                "admin:agent-42.e7.*",
                "grant:agent-42.e7.*",
            ],
        )
        .await?
        .token,
    );
    let r1 = dev.delegate(&e7, "r1", &[]).await?;
    let revoked = dev
        .revoke_as("agent-42", &root, both("agent-42.e7", &r1.delegation_id))
        .await?;
    let principal_until = revoked.principal_until_ms.ok_or("no principal until")?;
    assert_eq!(revoked.until_ms, principal_until, "no entry of its own");
    assert_revoked(&dev.client("agent-42", &e7)?, "e7").await;
    assert_revoked(&dev.client("agent-42", &BearerToken::new(r1.token))?, "r1").await;
    holds(&dev.client("agent-42", &root)?).await?;
    dev.stop().await;
    Ok(())
}

/// I10, what the rule refuses: other roots, their agents, names that merely start with the
/// caller's (`agent-420`), and names the caller is below (`agent-4` isn't an ancestor of
/// `agent-42`). Nothing refused is logged.
#[tokio::test]
async fn agent_42_cannot_revoke_agent_43_agent_43_x_agent_420_or_agent_4() -> TestResult {
    let dev = Dev::start().await?;
    let root = dev.root_42()?;
    let root_43 = dev.root("agent-43", "root-43")?;
    let x = dev
        .control("agent-43", &root_43)?
        .delegate_token(dev.request("x", &[]))
        .await?;
    for name in ["agent-43", "agent-43.x", "agent-420", "agent-4"] {
        assert_refused(
            dev.revoke_as("agent-42", &root, principal(name)).await,
            Code::PermissionDenied,
            "an agent revokes only itself or a principal it or its descendants delegated to",
        );
    }
    for id in [x.delegation_id.as_str(), "root-43"] {
        assert_refused(
            dev.revoke_as("agent-42", &root, token(id)).await,
            Code::NotFound,
            "no live delegation with that token id",
        );
    }
    holds(&dev.client("agent-43", &BearerToken::new(x.token))?).await?;
    holds(&dev.client("agent-43", &root_43)?).await?;
    let dir = dev.stop_keeping_files().await;
    assert_eq!(logged_revocations(&dir)?, 0, "refusals are never logged");
    Ok(())
}

/// ADR-0047, point 11.8: a child can't revoke its parent or a sibling, by principal or by ID.
#[tokio::test]
async fn child_cannot_revoke_parent_or_sibling() -> TestResult {
    let dev = Dev::start().await?;
    let root = dev.root_42()?;
    let c7 = BearerToken::new(dev.delegate(&root, "c7", &[]).await?.token);
    let d1 = dev.delegate(&root, "d1", &[]).await?;
    for name in ["agent-42", "agent-42.d1"] {
        assert_refused(
            dev.revoke_as("agent-42", &c7, principal(name)).await,
            Code::PermissionDenied,
            "the ancestor rule refuses it",
        );
    }
    for id in [d1.delegation_id.as_str(), "root-42"] {
        assert_refused(
            dev.revoke_as("agent-42", &c7, token(id)).await,
            Code::NotFound,
            "no live delegation with that token id",
        );
    }
    holds(&dev.client("agent-42", &root)?).await?;
    holds(&dev.client("agent-42", &BearerToken::new(d1.token))?).await?;
    dev.stop().await;
    Ok(())
}

/// An unknown token ID and another root's token ID answer alike, so nobody can probe for them.
#[tokio::test]
async fn unknown_token_id_is_not_found_revealing_nothing() -> TestResult {
    let dev = Dev::start().await?;
    let root = dev.root_42()?;
    // `agent-43`'s root token exists and is in use; `agent-42` can't tell.
    let root_43 = dev.root("agent-43", "root-43")?;
    holds(&dev.client("agent-43", &root_43)?).await?;
    let mut messages = Vec::new();
    for id in ["no-such-token", "root-43", "AAAAAAAAAAAAAAAAAAAAAA"] {
        let refused = dev
            .revoke_as("agent-42", &root, token(id))
            .await
            .expect_err(id);
        assert_eq!(refused.code(), Code::NotFound, "{id}: {refused:?}");
        messages.push(refused.message().to_owned());
    }
    assert!(
        messages
            .iter()
            .all(|message| message == "no live delegation with that token id"),
        "{messages:?}"
    );
    holds(&dev.client("agent-43", &root_43)?).await?;
    dev.stop().await;
    Ok(())
}

/// Keys stay `fleet`'s: no agent revokes one, not even the issuer key that signed its own child.
#[tokio::test]
async fn ancestor_cannot_revoke_a_key() -> TestResult {
    let dev = Dev::start().await?;
    let root = dev.root_42()?;
    let c7 = dev.delegate(&root, "c7", &[]).await?;
    let request = RevokeTokenRequest {
        revocation: Some(Revocation {
            revoked: Some(Revoked::KeyId(c7.issuer_key_id.clone())),
            until_ms: u64::MAX,
        }),
        principal: String::new(),
    };
    assert_refused(
        dev.revoke_as("agent-42", &root, request).await,
        Code::PermissionDenied,
        "the ancestor rule revokes no key; revoking a key needs the fleet scope",
    );
    holds(&dev.client("agent-42", &BearerToken::new(c7.token))?).await?;
    dev.stop().await;
    Ok(())
}

/// I9 and point 4.5: a relay-carried token, root or delegated, revokes nothing under the
/// ancestor rule but its own ID.
#[tokio::test]
async fn ancestor_revocation_relay_carried_is_refused() -> TestResult {
    let dev = Dev::start().await?;
    let root = dev.root_42()?;
    let c7 = dev.delegate(&root, "c7", &[]).await?;
    let c7_token = BearerToken::new(c7.token.clone());
    for (who, bearer, request) in [
        ("root, a child's principal", &root, principal("agent-42.c7")),
        ("root, a child's ID", &root, token(&c7.delegation_id)),
        ("root, itself", &root, principal("agent-42")),
        (
            "root, itself and its ID",
            &root,
            both("agent-42", "root-42"),
        ),
        ("c7, itself", &c7_token, principal("agent-42.c7")),
    ] {
        let refused = dev
            .revoke_as("router", bearer, request)
            .await
            .expect_err(who);
        assert_eq!(refused.code(), Code::PermissionDenied, "{who}: {refused:?}");
        assert!(refused.message().contains(RELAYED), "{who}: {refused:?}");
    }
    holds(&dev.client("agent-42", &root)?).await?;
    holds(&dev.client("agent-42", &c7_token)?).await?;
    dev.stop().await;
    Ok(())
}

/// Point 4.5's one exception: a relay-carried token revokes its own ID, so `agent renew` behind
/// a gateway can revoke the token it replaces, a root's or a delegated agent's.
#[tokio::test]
async fn relay_carried_token_revokes_only_its_own_jti() -> TestResult {
    let dev = Dev::start().await?;
    let root = dev.root_42()?;
    let c7 = dev.delegate(&root, "c7", &[]).await?;
    let c7_token = BearerToken::new(c7.token.clone());
    let d1 = dev.delegate(&root, "d1", &[]).await?;
    // Another token's ID, even one below it, is refused through the relay.
    assert_refused(
        dev.revoke_as("router", &root, token(&d1.delegation_id))
            .await,
        Code::PermissionDenied,
        RELAYED,
    );
    dev.revoke_as("router", &c7_token, token(&c7.delegation_id))
        .await?;
    assert_revoked(&dev.client("agent-42", &c7_token)?, "c7").await;
    dev.revoke_as("router", &root, token("root-42")).await?;
    assert_revoked(&dev.client("agent-42", &root)?, "agent-42").await;
    dev.stop().await;
    Ok(())
}

/// Nobody sends `before`: a request whose `revocation` holds a principal's is refused, from an
/// operator or an agent, and a request naming nothing is too.
#[tokio::test]
async fn request_cannot_carry_before() -> TestResult {
    let dev = Dev::start().await?;
    let root = dev.root_42()?;
    let carrying = RevokeTokenRequest {
        revocation: Some(Revocation {
            revoked: Some(Revoked::Principal(PrincipalRevocation {
                fleet: None,
                principal: "agent-42".to_owned(),
                before_ms: dev.at(HOUR).as_millis(),
            })),
            until_ms: u64::MAX,
        }),
        principal: String::new(),
    };
    assert_refused(
        dev.operator()?.revoke_token(carrying.clone()).await,
        Code::InvalidArgument,
        "the control plane sets a principal's revocation time itself",
    );
    assert_refused(
        dev.revoke_as("agent-42", &root, carrying).await,
        Code::InvalidArgument,
        "the control plane sets a principal's revocation time itself",
    );
    assert_refused(
        dev.operator()?
            .revoke_token(RevokeTokenRequest::default())
            .await,
        Code::InvalidArgument,
        "missing revocation",
    );
    holds(&dev.client("agent-42", &root)?).await?;
    dev.stop().await;
    Ok(())
}

/// Without `fleet`, only an agent's token revokes: a service's token is refused by the handler,
/// and a certificate without `fleet` at the boundary.
#[tokio::test]
async fn services_and_certificates_cannot_use_the_ancestor_rule() -> TestResult {
    let dev = Dev::start().await?;
    let read: Scopes = ["read:svc-*"]
        .iter()
        .map(|text| text.parse())
        .collect::<Result<_, _>>()?;
    let service = dev.authority.issue(&TokenSpec::new(
        "workload".parse()?,
        read,
        "svc-1".parse()?,
        dev.now,
        dev.at(HOUR),
    ))?;
    assert_refused(
        dev.revoke_as("workload", &service, token("svc-1")).await,
        Code::PermissionDenied,
        "only an agent revokes without the fleet scope",
    );
    let certificate = dev
        .tls
        .client_network("agent-42")?
        .control_client(dev.address.clone());
    assert_refused(
        certificate.revoke_token(principal("agent-42")).await,
        Code::PermissionDenied,
        "RevokeToken needs the fleet scope, or an agent's bearer token for the ancestor rule",
    );
    dev.stop().await;
    Ok(())
}

/// I4 and point 11.5: re-delegating a name revoked in the same second is refused as retryable,
/// since every node would refuse the token at once, and a retry a second later works.
#[tokio::test]
async fn same_second_redelegation_answers_unavailable() -> TestResult {
    let dev = Dev::start().await?;
    let root = dev.root_42()?;
    // `ControlClient` resends a delegation that answered `UNAVAILABLE`, as the CLI does; this
    // client sends once, so the test sees the answer itself.
    let channel = dev.tls.client_network("agent-42")?.channel(&dev.address)?;
    let mut once = control_client_with(channel, &Credentials::bearer(&root)?);
    let operator = dev.operator()?;
    // Both connections are open before the race, so neither call waits on a handshake.
    once.delegate_token(dev.request("warm", &[])).await?;
    operator.revoke_token(token("unused-1")).await?;
    // A second boundary between the revocation and the delegation lets the delegation through,
    // with a token issued after `before`; start each attempt early in a second.
    for attempt in 0..10 {
        let segment = format!("c{attempt}");
        let name = format!("agent-42.{segment}");
        while SystemClock.now().as_millis() % 1_000 > 200 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let revoked = operator.revoke_token(principal(&name)).await?;
        let before = revoked.before_ms.ok_or("no before")?;
        let request = dev.request(&segment, &[]);
        match once.delegate_token(request.clone()).await {
            Ok(granted) => {
                // Never a token every node would refuse at once.
                let granted = granted.into_inner();
                assert!(
                    granted.not_before_ms > before,
                    "{granted:?} within {before}"
                );
            }
            Err(status) => {
                assert_eq!(status.code(), Code::Unavailable, "{status:?}");
                assert_eq!(
                    status.message(),
                    format!("{name} was revoked less than a second ago; retry")
                );
                // The CLI's resend, a second later, records a token that works.
                tokio::time::sleep(Duration::from_millis(1_100)).await;
                let granted = once.delegate_token(request).await?.into_inner();
                assert_eq!(granted.principal, name);
                holds(&dev.client("agent-42", &BearerToken::new(granted.token))?).await?;
                dev.stop().await;
                return Ok(());
            }
        }
    }
    Err("every attempt crossed a second boundary".into())
}

/// The feature gate: while a member lacks `delegation`, principals and the ancestor rule are
/// refused naming it, and nothing is logged.
#[tokio::test]
async fn member_without_delegation_refuses_ancestor_revocations_naming_it() -> TestResult {
    let dev = Dev::start().await?;
    // A member that runs 0.7, which doesn't report `delegation`, joins on its own certificate.
    dev.tls
        .node_network("data-1")?
        .control_client(dev.address.clone())
        .join(JoinRequest {
            protocol: Some(PROTOCOL_VERSION),
            node: Some(Node {
                id: "data-1".into(),
                zone: None,
                host: None,
                weight: 1,
                address: None,
            }),
            software: Some(Software {
                version: "0.7.2".to_owned(),
                protocol: Some(PROTOCOL_VERSION),
                control_format: 1,
                features: vec!["undrain".to_owned(), "metadata".to_owned()],
            }),
        })
        .await?;
    dev.members(2).await?;
    let root = dev.root_42()?;
    for (who, result) in [
        (
            "ancestor, principal",
            dev.revoke_as("agent-42", &root, principal("agent-42"))
                .await,
        ),
        (
            "ancestor, own token",
            dev.revoke_as("agent-42", &root, token("root-42")).await,
        ),
        (
            "fleet, principal",
            dev.operator()?.revoke_token(principal("agent-42")).await,
        ),
    ] {
        let refused = result.expect_err(who);
        assert_eq!(
            refused.code(),
            Code::FailedPrecondition,
            "{who}: {refused:?}"
        );
        assert!(
            refused.message().contains("node 'data-1'"),
            "{who}: {refused:?}"
        );
    }
    // A token's revocation with `fleet` needs no feature.
    dev.operator()?.revoke_token(token("unused-1")).await?;
    holds(&dev.client("agent-42", &root)?).await?;
    let dir = dev.stop_keeping_files().await;
    assert_eq!(logged_revocations(&dir)?, 0);
    Ok(())
}
