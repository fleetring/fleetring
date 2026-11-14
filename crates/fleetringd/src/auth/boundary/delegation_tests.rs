//! The connection rule by signer, the stream recheck through a token's lineage, audit lines, and
//! the control freshness hook (ADR-0047, points 3 and 4.1, invariants I5, I8 and I9).
//!
//! The end-to-end cases, over real TCP with mutual TLS and through the router, are in
//! `tests/delegation.rs`.

use std::time::Duration;

use fleetring_control::delegation::{ChainLink, DelegationId, TokenClaims, TokenKind};
use fleetring_control::{Action as ControlAction, Entry, Revocations, Revoked, Timestamp, TokenId};
use tonic::Code;

use super::*;
use crate::auth::{
    DelegationSigner, LastAcknowledged, Scopes, TokenSigner, TokenSpec, UnreadableEntry,
};
use crate::clock::SimClock;
use crate::config::{PrincipalConfig, TokenForwarding};
use crate::tls::{Authority, FleetName, Identity, Validity};

const HOUR_MS: u64 = 3_600_000;

const DAY: Duration = Duration::from_secs(24 * 60 * 60);

/// How long the denylist stays fresh in tests that aren't about its age.
const LONG_AGE: Duration = Duration::from_secs(24 * 60 * 60);

/// A fleet's two authorities, a node identity trusting both, and two delegation issuers, `A`
/// and `B`, as a rotation or a leader change leaves them. `authority` signs the root token and
/// `certifier` certified both issuers, so the root's chain entry never names the key that
/// certified an issuer.
struct Lineages {
    identity: Identity,
    authority: TokenSigner,
    certifier: TokenSigner,
    a: DelegationSigner,
    b: DelegationSigner,
    fleet: FleetName,
}

fn der_of(pem: &str) -> Vec<u8> {
    use rustls_pki_types::CertificateDer;
    use rustls_pki_types::pem::PemObject as _;

    CertificateDer::from_pem_slice(pem.as_bytes())
        .expect("a certificate")
        .to_vec()
}

fn pkcs8_of(pem: &str) -> Vec<u8> {
    use rustls_pki_types::PrivateKeyDer;
    use rustls_pki_types::pem::PemObject as _;

    let PrivateKeyDer::Pkcs8(key) = PrivateKeyDer::from_pem_slice(pem.as_bytes()).expect("a key")
    else {
        panic!("an issuer key is PKCS#8");
    };
    key.secret_pkcs8_der().to_vec()
}

fn issuer(authority: &Authority, name: &str) -> DelegationSigner {
    let issued = authority
        .issue_issuer(
            &name.parse().expect("a node id"),
            Validity::new(Timestamp::ZERO, 30 * DAY),
        )
        .expect("an issuer pair");
    let authorities = [authority
        .token_signer()
        .expect("an Ed25519 authority")
        .verifying_key()];
    DelegationSigner::new(
        &der_of(issued.certificate_pem()),
        &pkcs8_of(issued.key_pem()),
        authority.fleet(),
        &authorities,
    )
    .expect("a delegation signer")
}

fn lineages() -> Lineages {
    let fleet: FleetName = "test".parse().expect("a fleet");
    let validity = Validity::new(Timestamp::ZERO, 100 * 365 * DAY);
    let authority = Authority::create(fleet.clone(), validity).expect("an authority");
    let certifier = Authority::create(fleet.clone(), validity).expect("a second authority");
    let bundle = format!(
        "{}{}",
        authority.certificate_pem(),
        certifier.certificate_pem()
    );
    let node = authority
        .issue_node(
            &"n1".parse().expect("a node id"),
            &["127.0.0.1".to_owned()],
            validity,
        )
        .expect("a node certificate");
    Lineages {
        identity: Identity::from_issued(&bundle, &node).expect("an identity"),
        a: issuer(&certifier, "control-a"),
        b: issuer(&certifier, "control-b"),
        authority: authority.token_signer().expect("a signer"),
        certifier: certifier.token_signer().expect("a signer"),
        fleet,
    }
}

fn scopes(texts: &[&str]) -> Scopes {
    texts
        .iter()
        .map(|text| text.parse().expect("a scope"))
        .collect()
}

/// The root token's ID.
const ROOT: &str = "root-42";

impl Lineages {
    /// `agent-42`'s root token, which the authority signs, valid from 0 for an hour.
    fn root(&self) -> BearerToken {
        self.authority
            .issue(
                &TokenSpec::new(
                    "agent-42".parse().expect("a principal"),
                    scopes(&["admin:agent-42-*", "admin:agent-42.*", "grant:agent-42.*"]),
                    ROOT.parse().expect("a token id"),
                    Timestamp::ZERO,
                    Timestamp::from_millis(HOUR_MS),
                )
                .agent("alice".parse().expect("a principal")),
            )
            .expect("a root token")
    }

    /// An authority-signed token for `principal`, which isn't an agent.
    fn service(&self, principal: &str, id: &str) -> BearerToken {
        self.authority
            .issue(&TokenSpec::new(
                principal.parse().expect("a principal"),
                scopes(&["read:svc-*"]),
                id.parse().expect("a token id"),
                Timestamp::ZERO,
                Timestamp::from_millis(HOUR_MS),
            ))
            .expect("a token")
    }

    fn root_link(&self) -> ChainLink {
        ChainLink {
            id: ROOT.parse().expect("a token id"),
            kid: self.authority.key_id().clone(),
        }
    }

    /// `agent-42.c7`'s claims, delegated from the root token.
    fn c7(&self) -> TokenClaims {
        claims(
            &self.fleet,
            "agent-42.c7",
            c7_id(),
            "agent-42",
            &["admin:agent-42.c7-*", "grant:agent-42.c7.*"],
            vec![self.root_link()],
        )
    }

    /// A grandchild below `c7`, delegated from the copy of `c7` that `issuer` signed.
    fn grandchild(&self, segment: &str, id: [u8; 16], issuer: &DelegationSigner) -> TokenClaims {
        let principal = format!("agent-42.c7.{segment}");
        claims(
            &self.fleet,
            &principal,
            DelegationId::from_bytes(id).into(),
            "agent-42.c7",
            &[&format!("admin:{principal}-*")],
            vec![
                self.root_link(),
                ChainLink {
                    id: c7_id(),
                    kid: issuer.key_id().clone(),
                },
            ],
        )
    }
}

fn c7_id() -> TokenId {
    DelegationId::from_bytes([7; 16]).into()
}

fn claims(
    fleet: &FleetName,
    principal: &str,
    id: TokenId,
    parent: &str,
    scope_texts: &[&str],
    chain: Vec<ChainLink>,
) -> TokenClaims {
    TokenClaims {
        fleet: fleet.as_str().into(),
        principal: principal.parse().expect("a principal"),
        id,
        issued_at: Some(Timestamp::ZERO),
        not_before: Timestamp::ZERO,
        expires_at: Timestamp::from_millis(HOUR_MS),
        scopes: scopes(scope_texts),
        kind: Some(TokenKind::Agent),
        owner: Some("alice".parse().expect("a principal")),
        parent: Some(parent.parse().expect("a principal")),
        chain,
    }
}

fn principals(entries: &[(&str, TokenForwarding)]) -> Vec<PrincipalConfig> {
    entries
        .iter()
        .map(|(name, forward_tokens)| PrincipalConfig {
            name: name.parse().expect("a principal"),
            scopes: Scopes::none(),
            forward_tokens: *forward_tokens,
        })
        .collect()
}

/// The principals most tests run with: `router` forwards tokens, `workload` doesn't.
const STANDARD: &[(&str, TokenForwarding)] = &[
    ("router", TokenForwarding::Allowed),
    ("workload", TokenForwarding::Refused),
];

fn authorizer(
    lineages: &Lineages,
    entries: &[(&str, TokenForwarding)],
    denylist: Denylist,
) -> Authorizer {
    Authorizer::new(
        &principals(entries),
        &Security::Tls(lineages.identity.clone()),
        Arc::new(SimClock::new(Timestamp::from_millis(1_000))),
    )
    .with_denylist(denylist, LONG_AGE)
}

fn fresh() -> Denylist {
    let list = Denylist::new();
    list.replace(Revocations::new());
    list
}

fn listing(entries: impl IntoIterator<Item = Revoked>) -> Revocations {
    entries
        .into_iter()
        .map(|revoked| (revoked, Timestamp::MAX))
        .collect()
}

fn peer(name: &str) -> Peer {
    Peer::Principal(name.parse().expect("a principal"))
}

/// What the boundary decided for a token.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Outcome {
    Accepted(Carriage),
    Refused(Code),
}

fn outcome(result: Result<Caller, Status>) -> Outcome {
    match result {
        Ok(caller) => Outcome::Accepted(caller.carriage().expect("a token's caller has one")),
        Err(status) => Outcome::Refused(status.code()),
    }
}

#[tokio::test]
async fn the_connection_rule_by_signer_and_connection() {
    use Outcome::{Accepted, Refused};

    let lineages = lineages();
    let standard = authorizer(&lineages, STANDARD, fresh());
    let workload_forwards = authorizer(
        &lineages,
        &[("workload", TokenForwarding::Allowed)],
        fresh(),
    );
    let root = lineages.root();
    let c7 = lineages.a.sign(&lineages.c7()).expect("c7");
    let r1 = lineages
        .b
        .sign(&lineages.grandchild("r1", [1; 16], &lineages.a))
        .expect("r1");
    let service = lineages.service("workload.prod", "svc-1");
    let node = Peer::Node("n2".parse().expect("a node id"));

    let table: [(&str, &Authorizer, &BearerToken, Peer, Outcome); 20] = [
        // An authority-signed token: only a certificate naming its subject carries it.
        (
            "root on its own certificate",
            &standard,
            &root,
            peer("agent-42"),
            Accepted(Carriage::Direct),
        ),
        (
            "root on a forwarding relay",
            &standard,
            &root,
            peer("router"),
            Accepted(Carriage::Relayed),
        ),
        (
            "root on another agent",
            &standard,
            &root,
            peer("agent-43"),
            Refused(Code::PermissionDenied),
        ),
        (
            "root on a node",
            &standard,
            &root,
            node.clone(),
            Refused(Code::PermissionDenied),
        ),
        (
            "root without a certificate",
            &standard,
            &root,
            Peer::Plaintext,
            Refused(Code::Unauthenticated),
        ),
        (
            "service on its own certificate",
            &standard,
            &service,
            peer("workload.prod"),
            Accepted(Carriage::Direct),
        ),
        (
            "service on its parent name",
            &standard,
            &service,
            peer("workload"),
            Refused(Code::PermissionDenied),
        ),
        (
            "service on a forwarding parent name",
            &workload_forwards,
            &service,
            peer("workload"),
            Accepted(Carriage::Relayed),
        ),
        // An issuer-signed token: its subject or an ancestor carries it directly.
        (
            "child on the root's certificate",
            &standard,
            &c7,
            peer("agent-42"),
            Accepted(Carriage::Direct),
        ),
        (
            "grandchild on the root's certificate",
            &standard,
            &r1,
            peer("agent-42"),
            Accepted(Carriage::Direct),
        ),
        (
            "child on a forwarding relay",
            &standard,
            &c7,
            peer("router"),
            Accepted(Carriage::Relayed),
        ),
        (
            "grandchild on a forwarding relay",
            &standard,
            &r1,
            peer("router"),
            Accepted(Carriage::Relayed),
        ),
        (
            "child on another root",
            &standard,
            &c7,
            peer("agent-43"),
            Refused(Code::PermissionDenied),
        ),
        (
            "child on a longer root",
            &standard,
            &c7,
            peer("agent-420"),
            Refused(Code::PermissionDenied),
        ),
        (
            "child on a shorter root",
            &standard,
            &c7,
            peer("agent-4"),
            Refused(Code::PermissionDenied),
        ),
        (
            "child on a non-forwarding service",
            &standard,
            &c7,
            peer("workload"),
            Refused(Code::PermissionDenied),
        ),
        (
            "child on a node",
            &standard,
            &c7,
            node.clone(),
            Refused(Code::PermissionDenied),
        ),
        (
            "child without a certificate",
            &standard,
            &c7,
            Peer::Plaintext,
            Refused(Code::Unauthenticated),
        ),
        // No certificate names a delegated agent, so neither the child nor its own child rides one.
        (
            "grandchild on the child's name",
            &standard,
            &r1,
            peer("agent-42.c7"),
            Refused(Code::Unauthenticated),
        ),
        (
            "child on its own name",
            &standard,
            &c7,
            peer("agent-42.c7"),
            Refused(Code::Unauthenticated),
        ),
    ];
    for (case, auth, token, peer, expected) in table {
        assert_eq!(
            outcome(auth.resolve(&peer, Some(token))),
            expected,
            "{case}"
        );
    }
}

#[tokio::test]
async fn token_on_a_connection_without_a_client_certificate_is_refused() {
    let lineages = lineages();
    let auth = authorizer(&lineages, STANDARD, fresh());
    let c7 = lineages.a.sign(&lineages.c7()).expect("c7");
    for (who, token) in [("root", lineages.root()), ("delegated", c7)] {
        let refused = auth
            .resolve(&Peer::Plaintext, Some(&token))
            .expect_err("no client certificate");
        assert_eq!(refused.code(), Code::Unauthenticated, "{who}");
        assert_eq!(
            refused.message(),
            "a bearer token needs a client certificate",
            "{who}"
        );
    }
    // It is refused before verification: even a token that would never verify gets the same
    // answer, and a missing denylist isn't consulted.
    let unchecked = Authorizer::new(
        &principals(STANDARD),
        &Security::Tls(lineages.identity.clone()),
        Arc::new(SimClock::new(Timestamp::from_millis(1_000))),
    );
    let refused = unchecked
        .resolve(&Peer::Plaintext, Some(&BearerToken::new("x.y.z")))
        .expect_err("no client certificate");
    assert_eq!(
        refused.message(),
        "a bearer token needs a client certificate"
    );
    // Without a token, a plaintext connection to a TLS daemon is still anonymous here; the
    // listener never produces one.
    assert_eq!(
        auth.resolve(&Peer::Plaintext, None).ok(),
        Some(Caller::Anonymous)
    );
}

#[tokio::test]
async fn the_caller_carries_the_signer_the_carriage_and_the_lineage() {
    let lineages = lineages();
    let auth = authorizer(&lineages, STANDARD, fresh());
    let claims = lineages.grandchild("r1", [1; 16], &lineages.a);
    let r1 = lineages.b.sign(&claims).expect("r1");
    let caller = auth.resolve(&peer("router"), Some(&r1)).expect("relayed");
    let Caller::Principal {
        name,
        credential:
            Credential::Token {
                id,
                key,
                signer,
                carriage,
                claims: carried,
                ..
            },
        ..
    } = &caller
    else {
        panic!("a token's caller: {caller:?}");
    };
    assert_eq!(name.as_str(), "agent-42.c7.r1");
    assert_eq!(id, &claims.id);
    assert_eq!(key, lineages.b.key_id());
    assert!(matches!(signer, Signer::Issuer { name, .. } if name.as_str() == "control-b"));
    assert_eq!(*carriage, Carriage::Relayed);
    assert_eq!(**carried, claims);
    assert_eq!(caller.carriage(), Some(Carriage::Relayed));
    let certificate = auth
        .resolve(&peer("agent-42"), None)
        .expect("a certificate");
    assert_eq!(certificate.carriage(), None);
}

/// The decision for a `WatchLeases` request from `peer` carrying `bearer`.
fn watch(auth: &Authorizer, peer: Peer, bearer: &BearerToken) -> Authorized {
    let mut request = http::Request::builder()
        .uri("/fleetring.v1.Database/WatchLeases")
        .header(AUTHORIZATION, format!("Bearer {}", bearer.expose()))
        .body(())
        .expect("a request");
    request.extensions_mut().insert(peer);
    auth.admit(&request).expect("admitted")
}

fn silent() -> BoxStream<u32> {
    Box::pin(tokio_stream::pending())
}

/// How a guarded stream ends within a few rechecks, or `None` if it is still open.
async fn ended(stream: &mut BoxStream<u32>) -> Option<Status> {
    match tokio::time::timeout(STREAM_RECHECK * 3, stream.next()).await {
        Err(_) => None,
        Ok(Some(Err(status))) => Some(status),
        Ok(other) => panic!("a silent stream yielded {other:?}"),
    }
}

/// The release blocker's node half (the plan's section 14): with `c7` delegated under issuer A,
/// re-signed under B, and grandchildren `r1` (from A's copy) and `r2` (from B's) signed by B,
/// each revocation refuses exactly the tokens below what it names, at verification and on an
/// open stream, within [`STREAM_RECHECK`].
#[tokio::test(start_paused = true)]
async fn subtree_revocation_across_issuers_on_requests_and_streams() {
    let lineages = lineages();
    let c7_claims = lineages.c7();
    let tokens = [
        ("root", lineages.root()),
        ("c7 (A)", lineages.a.sign(&c7_claims).expect("c7 under A")),
        ("c7 (B)", lineages.b.sign(&c7_claims).expect("c7 under B")),
        (
            "r1",
            lineages
                .b
                .sign(&lineages.grandchild("r1", [1; 16], &lineages.a))
                .expect("r1"),
        ),
        (
            "r2",
            lineages
                .b
                .sign(&lineages.grandchild("r2", [2; 16], &lineages.b))
                .expect("r2"),
        ),
    ];
    let cases: [(&str, Revoked, &[&str]); 6] = [
        (
            "issuer key A",
            Revoked::Key(lineages.a.key_id().clone()),
            &["c7 (A)", "r1"],
        ),
        (
            "issuer key B",
            Revoked::Key(lineages.b.key_id().clone()),
            &["c7 (B)", "r1", "r2"],
        ),
        (
            "c7's delegation ID",
            Revoked::Token(c7_id()),
            &["c7 (A)", "c7 (B)", "r1", "r2"],
        ),
        (
            "the root token",
            Revoked::Token(ROOT.parse().expect("a token id")),
            &["root", "c7 (A)", "c7 (B)", "r1", "r2"],
        ),
        (
            "the authority key that signed the root token",
            Revoked::Key(lineages.authority.key_id().clone()),
            &["root", "c7 (A)", "c7 (B)", "r1", "r2"],
        ),
        (
            "the authority key that certified both issuers, and signed no copy in a chain",
            Revoked::Key(lineages.certifier.key_id().clone()),
            &["c7 (A)", "c7 (B)", "r1", "r2"],
        ),
    ];
    for (case, revoked, ends) in cases {
        let denylist = fresh();
        let auth = authorizer(&lineages, STANDARD, denylist.clone());
        let mut streams: Vec<(&str, BoxStream<u32>)> = tokens
            .iter()
            .map(|(name, token)| (*name, watch(&auth, peer("agent-42"), token).guard(silent())))
            .collect();
        denylist.replace(listing([revoked]));
        for (name, stream) in &mut streams {
            let token = &tokens
                .iter()
                .find(|(token_name, _)| token_name == name)
                .expect("a token")
                .1;
            let request = auth.resolve(&peer("agent-42"), Some(token));
            let stream_end = ended(stream).await;
            if ends.contains(name) {
                let refused = request.expect_err("refused at verification");
                assert_eq!(refused.message(), "the token is revoked", "{case}: {name}");
                let status =
                    stream_end.unwrap_or_else(|| panic!("{case}: {name}'s stream is still open"));
                assert_eq!(status.code(), Code::Unauthenticated, "{case}: {name}");
                assert_eq!(status.message(), "the token is revoked", "{case}: {name}");
            } else {
                assert!(request.is_ok(), "{case}: {name} survives: {request:?}");
                assert!(
                    stream_end.is_none(),
                    "{case}: {name}'s stream survives: {stream_end:?}"
                );
            }
        }
    }
}

#[tokio::test(start_paused = true)]
async fn a_relay_carried_stream_ends_on_an_ancestors_revocation_within_a_recheck() {
    let lineages = lineages();
    let denylist = fresh();
    let auth = authorizer(&lineages, STANDARD, denylist.clone());
    let r1 = lineages
        .b
        .sign(&lineages.grandchild("r1", [1; 16], &lineages.a))
        .expect("r1");
    let mut stream = watch(&auth, peer("router"), &r1).guard(silent());
    assert!(
        ended(&mut stream).await.is_none(),
        "open while nothing is revoked"
    );
    denylist.replace(listing([Revoked::Token(c7_id())]));
    let started = tokio::time::Instant::now();
    let status = tokio::time::timeout(STREAM_RECHECK * 3, stream.next())
        .await
        .expect("the stream ends")
        .expect("with an item")
        .expect_err("a refusal");
    assert!(
        started.elapsed() <= STREAM_RECHECK,
        "ended after {:?}",
        started.elapsed()
    );
    assert_eq!(status.message(), "the token is revoked");
}

async fn control_node(dir: &std::path::Path) -> crate::control::ControlNode {
    let options = crate::control::ControlOptions::default()
        .with_storage_io(crate::control::StorageIo::Inline);
    crate::control::ControlNode::open(
        dir.join("control.db"),
        options,
        SimClock::new(Timestamp::ZERO),
    )
    .await
    .expect("a control node")
}

#[tokio::test(start_paused = true)]
async fn stale_acknowledged_control_state_refuses_every_token_and_certificates_still_work() {
    let lineages = lineages();
    let dir = tempfile::tempdir().expect("a directory");
    let control = control_node(dir.path()).await;
    let last = LastAcknowledged::new();
    let max_age = Duration::from_secs(5);
    let auth = Authorizer::new(
        &principals(STANDARD),
        &Security::Tls(lineages.identity.clone()),
        Arc::new(SimClock::new(Timestamp::from_millis(1_000))),
    )
    .with_control_freshness(
        control.handle(),
        ControlFreshness::Acknowledged {
            last: last.clone(),
            max_age,
        },
    );
    let root = lineages.root();
    let c7 = lineages.a.sign(&lineages.c7()).expect("c7");
    let refused_all = |expected: &str| {
        for (who, token) in [("root", &root), ("delegated", &c7)] {
            let refused = auth
                .resolve(&peer("agent-42"), Some(token))
                .expect_err("not current");
            assert_eq!(refused.code(), Code::Unavailable, "{who}");
            assert_eq!(refused.message(), expected, "{who}");
        }
        auth.resolve(&peer("agent-42"), None)
            .expect("a certificate still works");
    };

    // Never acknowledged since it started: no token is trusted.
    refused_all("this control node hasn't been acknowledged by its group yet; retry");

    last.record();
    for token in [&root, &c7] {
        auth.resolve(&peer("agent-42"), Some(token))
            .expect("current");
    }
    let mut stream = watch(&auth, peer("agent-42"), &c7).guard(silent());

    // Cut off from its group for longer than the bound: every token is refused, open streams
    // end, and certificates keep working.
    tokio::time::advance(max_age + Duration::from_millis(1)).await;
    refused_all("this control node's state is stale; retry");
    let status = ended(&mut stream).await.expect("the stream ends");
    assert_eq!(status.code(), Code::Unavailable);

    // Acknowledged again: tokens are trusted again.
    last.record();
    auth.resolve(&peer("agent-42"), Some(&c7))
        .expect("current again");
}

#[tokio::test(start_paused = true)]
async fn sole_writer_state_is_always_current() {
    let lineages = lineages();
    let dir = tempfile::tempdir().expect("a directory");
    let control = control_node(dir.path()).await;
    let handle = control.handle();
    let auth = Authorizer::new(
        &principals(STANDARD),
        &Security::Tls(lineages.identity.clone()),
        Arc::new(SimClock::new(Timestamp::from_millis(1_000))),
    )
    .with_control(handle.clone());
    let root = lineages.root();
    let r1 = lineages
        .b
        .sign(&lineages.grandchild("r1", [1; 16], &lineages.a))
        .expect("r1");

    // However long nothing is applied, the only writer's state is the fleet's: half an hour,
    // hundreds of heartbeat timeouts, within the tokens' hour.
    tokio::time::advance(Duration::from_secs(30 * 60)).await;
    auth.resolve(&peer("agent-42"), Some(&root))
        .expect("current");
    auth.resolve(&peer("agent-42"), Some(&r1)).expect("current");

    // And it is the state tokens are checked against, lineage included.
    let mut stream = watch(&auth, peer("agent-42"), &r1).guard(silent());
    handle
        .submit(ControlAction::RevokeToken {
            revoked: Revoked::Token(c7_id()),
            until: Timestamp::MAX,
        })
        .await
        .expect("committed");
    let refused = auth
        .resolve(&peer("agent-42"), Some(&r1))
        .expect_err("below a revoked delegation");
    assert_eq!(refused.message(), "the token is revoked");
    let status = ended(&mut stream).await.expect("the stream ends");
    assert_eq!(status.message(), "the token is revoked");
    auth.resolve(&peer("agent-42"), Some(&root))
        .expect("the root is untouched");
}

/// A list holding an entry this node can't read is no list (ADR-0047, point 3, I5): every
/// token is refused with `UNAVAILABLE`, root and delegated alike, open streams end within a
/// recheck, certificates keep working, and a readable list makes tokens usable again.
#[tokio::test(start_paused = true)]
async fn an_unreadable_entry_refuses_every_token_until_a_readable_list_arrives() {
    let lineages = lineages();
    let denylist = fresh();
    let auth = authorizer(&lineages, STANDARD, denylist.clone());
    let root = lineages.root();
    let c7 = lineages.a.sign(&lineages.c7()).expect("c7");
    let mut stream = watch(&auth, peer("agent-42"), &c7).guard(silent());

    denylist.receive(Err(UnreadableEntry::new(
        0,
        "a revocation names a token, a key, or a principal",
    )));
    for (who, token) in [("root", &root), ("delegated", &c7)] {
        let refused = auth
            .resolve(&peer("agent-42"), Some(token))
            .expect_err("unusable list");
        assert_eq!(refused.code(), Code::Unavailable, "{who}");
        assert_eq!(
            refused.message(),
            "this node's token denylist holds an entry it can't read; retry",
            "{who}"
        );
    }
    auth.resolve(&peer("agent-42"), None)
        .expect("a certificate still works");
    let status = ended(&mut stream).await.expect("the stream ends");
    assert_eq!(status.code(), Code::Unavailable);

    denylist.replace(Revocations::new());
    for token in [&root, &c7] {
        auth.resolve(&peer("agent-42"), Some(token))
            .expect("usable again");
    }
}

/// A principal's denylist entry as nodes read it: added with `fleet`, refusing tokens issued at
/// or before `before`.
fn principal_listing(principal: &str, before: Timestamp) -> Revocations {
    [(
        Revoked::principal(principal.parse().expect("a principal")),
        Entry::fleet_principal(Timestamp::MAX, before),
    )]
    .into_iter()
    .collect()
}

/// I5 for `Revoked::Principal` on a node: revoking `agent-42.c7` by principal refuses `c7`'s
/// every copy and every token below it, on requests and on open streams within
/// [`STREAM_RECHECK`], whatever issuer signed which copy, and never `agent-42` or a name that
/// merely starts with `agent-42.c7` (`agent-42.c70`).
#[tokio::test(start_paused = true)]
async fn principal_revocation_ends_tokens_and_open_streams_within_recheck() {
    let lineages = lineages();
    let c7_claims = lineages.c7();
    let c70 = claims(
        &lineages.fleet,
        "agent-42.c70",
        DelegationId::from_bytes([70; 16]).into(),
        "agent-42",
        &["admin:agent-42.c70-*"],
        vec![lineages.root_link()],
    );
    let tokens = [
        ("root", lineages.root(), false),
        (
            "c7 (A)",
            lineages.a.sign(&c7_claims).expect("c7 under A"),
            true,
        ),
        (
            "c7 (B)",
            lineages.b.sign(&c7_claims).expect("c7 under B"),
            true,
        ),
        (
            "r1",
            lineages
                .b
                .sign(&lineages.grandchild("r1", [1; 16], &lineages.a))
                .expect("r1"),
            true,
        ),
        ("c70", lineages.a.sign(&c70).expect("c70"), false),
    ];
    let denylist = fresh();
    let auth = authorizer(&lineages, STANDARD, denylist.clone());
    let mut streams: Vec<BoxStream<u32>> = tokens
        .iter()
        .map(|(_, token, _)| watch(&auth, peer("agent-42"), token).guard(silent()))
        .collect();
    // Every token here was issued at 0, at or before the entry's `before`.
    denylist.replace(principal_listing("agent-42.c7", Timestamp::ZERO));
    let started = tokio::time::Instant::now();
    for ((name, token, ends), stream) in tokens.iter().zip(&mut streams) {
        let request = auth.resolve(&peer("agent-42"), Some(token));
        if *ends {
            let refused = request.expect_err("refused at verification");
            assert_eq!(refused.message(), "the token is revoked", "{name}");
            let status = ended(stream)
                .await
                .unwrap_or_else(|| panic!("{name}'s stream is still open"));
            assert_eq!(status.message(), "the token is revoked", "{name}");
            assert!(
                started.elapsed() <= STREAM_RECHECK,
                "{name}'s stream ended after {:?}",
                started.elapsed()
            );
        } else {
            assert!(request.is_ok(), "{name} survives: {request:?}");
        }
    }
    for ((name, _, ends), stream) in tokens.iter().zip(&mut streams) {
        if !*ends {
            assert!(ended(stream).await.is_none(), "{name}'s stream survives");
        }
    }
}

/// Point 3's comparison at its boundaries, by signer: a root token is refused while its `iat` is
/// at most 30 seconds after the entry's `before` (its signer's clock may run that far ahead of
/// control time), a delegated token only while its `iat` is at or before `before`, since its
/// `iat` is control time. So a root token signed 29 or 30 seconds "ahead" is refused, one signed
/// 31 seconds after is not, and a child minted a second after the revocation works.
#[tokio::test]
async fn root_token_signed_29s_ahead_is_refused_and_delegated_token_minted_after_is_not() {
    let lineages = lineages();
    let before = Timestamp::from_millis(40_000);
    let denylist = fresh();
    denylist.replace(principal_listing("agent-42", before));
    let auth = Authorizer::new(
        &principals(STANDARD),
        &Security::Tls(lineages.identity.clone()),
        Arc::new(SimClock::new(Timestamp::from_millis(100_000))),
    )
    .with_denylist(denylist, LONG_AGE);
    let root_issued_at = |secs: u64| {
        let at = Timestamp::from_millis(secs * 1_000);
        lineages
            .authority
            .issue(&TokenSpec {
                issued_at: at,
                ..TokenSpec::new(
                    "agent-42".parse().expect("a principal"),
                    scopes(&["admin:agent-42-*"]),
                    format!("root-{secs}").parse().expect("a token id"),
                    at,
                    Timestamp::from_millis(HOUR_MS),
                )
                .agent("alice".parse().expect("a principal"))
            })
            .expect("a root token")
    };
    let child_issued_at = |secs: u64| {
        let at = Timestamp::from_millis(secs * 1_000);
        let c7 = TokenClaims {
            issued_at: Some(at),
            not_before: at,
            ..lineages.c7()
        };
        lineages.a.sign(&c7).expect("c7")
    };
    let cases = [
        ("root issued at before", root_issued_at(40), true),
        ("root issued 29 s after before", root_issued_at(69), true),
        ("root issued 30 s after before", root_issued_at(70), true),
        ("root issued 31 s after before", root_issued_at(71), false),
        ("child issued at before", child_issued_at(40), true),
        ("child issued 1 s after before", child_issued_at(41), false),
    ];
    for (case, token, refused) in cases {
        let result = auth.resolve(&peer("agent-42"), Some(&token));
        if refused {
            let status = result.expect_err(case);
            assert_eq!(status.message(), "the token is revoked", "{case}");
        } else {
            assert!(result.is_ok(), "{case}: {result:?}");
        }
    }
}

/// Admits a call to `path` from `peer` carrying `bearer`, and returns the audit lines logged.
fn audit_lines(auth: &Authorizer, peer: Peer, path: &str, bearer: &BearerToken) -> Vec<String> {
    super::tests::install_audit_subscriber();
    super::tests::AUDIT_LINES.with(|lines| lines.borrow_mut().clear());
    let mut request = http::Request::builder()
        .uri(path)
        .header(AUTHORIZATION, format!("Bearer {}", bearer.expose()))
        .body(())
        .expect("a request");
    request.extensions_mut().insert(peer);
    let _decision = auth.admit(&request);
    let bytes = super::tests::AUDIT_LINES.with(|lines| std::mem::take(&mut *lines.borrow_mut()));
    String::from_utf8_lossy(&bytes)
        .lines()
        .map(str::to_owned)
        .collect()
}

/// The value of `field=` in `line`, quotes included, or `None` if the line has none.
fn field<'line>(line: &'line str, name: &str) -> Option<&'line str> {
    let prefix = format!("{name}=");
    line.split_whitespace()
        .find_map(|word| word.strip_prefix(prefix.as_str()))
}

#[test]
fn audit_line_names_lineage_and_never_token_bytes() {
    let lineages = lineages();
    let denylist = fresh();
    let auth = authorizer(&lineages, STANDARD, denylist.clone());
    let claims = lineages.grandchild("r1", [1; 16], &lineages.a);
    let r1 = lineages.b.sign(&claims).expect("r1");
    let expected_chain = format!("\"{ROOT},{}\"", c7_id());
    let expected_credential = format!("\"token:{}\"", claims.id);
    let expected_key = format!("\"{}\"", lineages.b.key_id());
    let never_logged: Vec<&str> = r1.expose().split('.').collect();
    let assert_no_token = |lines: &[String]| {
        for line in lines {
            for part in &never_logged {
                assert!(!line.contains(part), "a token part is logged: {line}");
            }
        }
    };

    // A delegated agent's request denied by policy: a node-only call.
    let lines = audit_lines(&auth, peer("agent-42"), "/fleetring.v1.Control/Join", &r1);
    let denied = lines
        .iter()
        .find(|line| line.contains("denied"))
        .unwrap_or_else(|| panic!("no denial in {lines:?}"));
    assert_eq!(
        field(denied, "principal"),
        Some("\"agent-42.c7.r1\""),
        "{denied}"
    );
    assert_eq!(
        field(denied, "credential"),
        Some(expected_credential.as_str()),
        "{denied}"
    );
    assert_eq!(field(denied, "signer"), Some("\"issuer\""), "{denied}");
    assert_eq!(field(denied, "carriage"), Some("\"direct\""), "{denied}");
    assert_eq!(field(denied, "parent"), Some("\"agent-42.c7\""), "{denied}");
    assert_eq!(
        field(denied, "chain"),
        Some(expected_chain.as_str()),
        "{denied}"
    );
    assert_eq!(
        field(denied, "issuer_key"),
        Some(expected_key.as_str()),
        "{denied}"
    );
    assert_no_token(&lines);

    // A copied token on another root's certificate, refused by the connection rule.
    let lines = audit_lines(
        &auth,
        peer("agent-43"),
        "/fleetring.v1.Database/WatchLeases",
        &r1,
    );
    let refused = lines
        .iter()
        .find(|line| line.contains("refused a token"))
        .unwrap_or_else(|| panic!("no refusal in {lines:?}"));
    assert_eq!(
        field(refused, "principal"),
        Some("\"agent-43\""),
        "{refused}"
    );
    assert_eq!(
        field(refused, "credential"),
        Some(expected_credential.as_str()),
        "{refused}"
    );
    assert_eq!(field(refused, "signer"), Some("\"issuer\""), "{refused}");
    assert_eq!(
        field(refused, "carriage"),
        None,
        "it never arrived: {refused}"
    );
    assert_eq!(
        field(refused, "chain"),
        Some(expected_chain.as_str()),
        "{refused}"
    );
    assert_eq!(
        field(refused, "issuer_key"),
        Some(expected_key.as_str()),
        "{refused}"
    );
    assert_no_token(&lines);

    // A revoked one is named by its ID alone, as before.
    denylist.replace(listing([Revoked::Token(c7_id())]));
    let lines = audit_lines(
        &auth,
        peer("agent-42"),
        "/fleetring.v1.Database/WatchLeases",
        &r1,
    );
    let refused = lines
        .iter()
        .find(|line| line.contains("refused a token"))
        .unwrap_or_else(|| panic!("no refusal in {lines:?}"));
    assert_eq!(
        field(refused, "credential"),
        Some(expected_credential.as_str()),
        "{refused}"
    );
    assert_no_token(&lines);

    // An authority-signed token names its signer, and no lineage.
    denylist.replace(Revocations::new());
    let root = lineages.root();
    let lines = audit_lines(&auth, peer("router"), "/fleetring.v1.Control/Join", &root);
    let denied = lines
        .iter()
        .find(|line| line.contains("denied"))
        .unwrap_or_else(|| panic!("no denial in {lines:?}"));
    assert_eq!(field(denied, "signer"), Some("\"authority\""), "{denied}");
    assert_eq!(field(denied, "carriage"), Some("\"relayed\""), "{denied}");
    assert_eq!(field(denied, "parent"), None, "{denied}");
    assert_eq!(field(denied, "chain"), None, "{denied}");
    assert_eq!(field(denied, "issuer_key"), None, "{denied}");
}

#[test]
fn a_stream_end_names_the_lineage_and_the_entry_that_ended_it() {
    let lineages = lineages();
    let claims = lineages.grandchild("r1", [1; 16], &lineages.a);
    let credential = Credential::Token {
        id: claims.id.clone(),
        key: lineages.b.key_id().clone(),
        expires_at: claims.expires_at,
        signer: Signer::Authority,
        carriage: Carriage::Relayed,
        claims: Arc::new(claims.clone()),
    };
    let authority = TokenAudit::of_credential(&credential);
    assert_eq!(authority.signer, Some("authority"));
    assert_eq!(
        authority.chain, None,
        "only an issuer's token names a lineage"
    );
    let r1 = lineages.b.sign(&claims).expect("r1");
    let auth = authorizer(&lineages, STANDARD, fresh());
    let Caller::Principal { credential, .. } =
        auth.resolve(&peer("agent-42"), Some(&r1)).expect("direct")
    else {
        panic!("a principal");
    };
    let audit = TokenAudit::of_credential(&credential);
    assert_eq!(audit.credential, format!("token:{}", claims.id));
    assert_eq!(audit.signer, Some("issuer"));
    assert_eq!(audit.carriage, Some("direct"));
    assert_eq!(audit.parent.as_deref(), Some("agent-42.c7"));
    assert_eq!(audit.chain, Some(format!("{ROOT},{}", c7_id())));
    assert_eq!(audit.issuer_key, Some(lineages.b.key_id().to_string()));
    let revoked = Denylist::new();
    revoked.replace(listing([Revoked::Key(lineages.a.key_id().clone())]));
    let Err(lapsed) = authorizer(&lineages, STANDARD, revoked).token_holds(&credential) else {
        panic!("its chain names a revoked key");
    };
    assert_eq!(
        lapsed.entry,
        Some(Revoked::Key(lineages.a.key_id().clone()))
    );
}

/// ADR-0026 point 7 and ADR-0047 I5 promise that an open stream ends within a second of its
/// node hearing of a revocation; the security analysis and the stream tests here count on it.
#[test]
fn streams_are_rechecked_every_second() {
    assert_eq!(STREAM_RECHECK, Duration::from_secs(1));
}
