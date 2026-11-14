//! FR-090: delegated tokens verified on a node's identity (ADR-0047, points 1.4 and 3), and the
//! `x5c` cache that lives with each loaded identity generation (the plan's P11).
//!
//! The refusals of a token, one rule at a time, are unit tests in `auth/token.rs`; these tests
//! verify through the node's own [`Identity`], as the gRPC boundary and the router do.

use std::error::Error;
use std::fs;
use std::path::Path;
use std::time::Duration;

use fleetring_control::delegation::{ChainLink, DelegationId, TokenClaims, TokenKind};
use fleetring_control::{Revocations, Revoked, Timestamp};
use fleetringd::auth::{DelegationSigner, Signer, TokenError};
use fleetringd::tls::{Authority, FleetName, Identity, IssuerCertError, Validity};
use rustls_pki_types::pem::PemObject as _;
use rustls_pki_types::{CertificateDer, PrivateKeyDer};

type TestResult = Result<(), Box<dyn Error>>;

/// When the issuer certificates here start: 2027-01-15.
const ISSUED: Timestamp = Timestamp::from_millis(1_800_000_000_000);

const DAY: Duration = Duration::from_secs(24 * 60 * 60);

/// The reload check's interval is one second; this waits it out with room.
const RELOAD_WAIT: Duration = Duration::from_millis(1_200);

fn after(at: Timestamp, span: Duration) -> Timestamp {
    Timestamp::from_millis(at.as_millis() + u64::try_from(span.as_millis()).expect("fits"))
}

fn authority(fleet: &FleetName) -> Result<Authority, Box<dyn Error>> {
    Ok(Authority::create(
        fleet.clone(),
        Validity::new(Timestamp::ZERO, 36_500 * DAY),
    )?)
}

/// A delegation signer for `name`, certified by `authority` from [`ISSUED`] for 30 days.
fn delegation_signer(
    authority: &Authority,
    name: &str,
) -> Result<DelegationSigner, Box<dyn Error>> {
    let issued = authority.issue_issuer(&name.parse()?, Validity::new(ISSUED, 30 * DAY))?;
    let cert = CertificateDer::from_pem_slice(issued.certificate_pem().as_bytes())?;
    let PrivateKeyDer::Pkcs8(key) = PrivateKeyDer::from_pem_slice(issued.key_pem().as_bytes())?
    else {
        return Err("an issuer key is PKCS#8".into());
    };
    let authorities = [authority.token_signer()?.verifying_key()];
    Ok(DelegationSigner::new(
        cert.as_ref(),
        key.secret_pkcs8_der(),
        authority.fleet(),
        &authorities,
    )?)
}

/// `agent-42.c7`'s claims, recorded a day after [`ISSUED`], with the scopes the plan's accepted
/// case names, delegated from a root token that `root`, a trusted authority, signed.
fn child_claims(fleet: &FleetName, root: &Authority) -> Result<TokenClaims, Box<dyn Error>> {
    let recorded = after(ISSUED, DAY);
    Ok(TokenClaims {
        fleet: fleet.as_str().into(),
        principal: "agent-42.c7".parse()?,
        id: DelegationId::from_bytes([7; 16]).into(),
        issued_at: Some(recorded),
        not_before: recorded,
        expires_at: after(recorded, Duration::from_secs(60 * 60)),
        scopes: ["admin:agent-42.c7-*", "fork:agent-42-memory"]
            .into_iter()
            .map(str::parse)
            .collect::<Result<_, _>>()?,
        kind: Some(TokenKind::Agent),
        owner: Some("alice".parse()?),
        parent: Some("agent-42".parse()?),
        chain: vec![ChainLink {
            id: "root-1".parse()?,
            kid: root.token_signer()?.key_id().clone(),
        }],
    })
}

/// When the tokens here are checked: a minute into their validity.
fn checked_at() -> Timestamp {
    after(ISSUED, DAY + Duration::from_secs(60))
}

#[test]
fn a_delegated_token_verifies_on_a_node_identity() -> TestResult {
    let fleet: FleetName = "lab".parse()?;
    let authority = authority(&fleet)?;
    let node = authority.issue_node(
        &"n1".parse()?,
        &["127.0.0.1".to_owned()],
        Validity::new(ISSUED, 30 * DAY),
    )?;
    let identity = Identity::from_issued(authority.certificate_pem(), &node)?;
    let signer = delegation_signer(&authority, "control-1")?;
    let claims = child_claims(&fleet, &authority)?;
    let token = signer.sign(&claims)?;

    let verified = identity
        .token_verifier()
        .verify(&token, checked_at(), &Revocations::new())?;
    assert_eq!(verified.claims, claims);
    assert_eq!(verified.principal.as_str(), "agent-42.c7");
    assert_eq!(&verified.key, signer.key_id());
    let Signer::Issuer {
        name,
        authority: signed_by,
    } = &verified.signer
    else {
        panic!("an issuer signed it: {:?}", verified.signer);
    };
    assert_eq!(name.as_str(), "control-1");
    assert_eq!(signed_by, authority.token_signer()?.verifying_key().id());

    // Revoking the authority key that certified the issuer ends its tokens at once, before the
    // `[tls] ca` bundle changes (ADR-0047, point 1.4.8's dated note).
    let revoked: Revocations = [(Revoked::Key(signed_by.clone()), Timestamp::MAX)]
        .into_iter()
        .collect();
    assert_eq!(
        identity
            .token_verifier()
            .verify(&token, checked_at(), &revoked),
        Err(TokenError::Revoked)
    );
    Ok(())
}

/// Writes `pems`, joined, to `path`.
fn write_bundle(path: &Path, pems: &[&str]) -> TestResult {
    fs::write(path, pems.concat())?;
    Ok(())
}

#[test]
fn x5c_cache_is_dropped_when_the_authority_bundle_changes() -> TestResult {
    let fleet: FleetName = "lab".parse()?;
    // Authority A certifies the issuer; authority B certifies the node, so the identity still
    // loads once A leaves the bundle.
    let a = authority(&fleet)?;
    let b = authority(&fleet)?;
    let node = b.issue_node(
        &"n1".parse()?,
        &["127.0.0.1".to_owned()],
        Validity::new(ISSUED, 30 * DAY),
    )?;
    let dir = tempfile::tempdir()?;
    let (ca, cert, key) = (
        dir.path().join("ca.crt"),
        dir.path().join("node.crt"),
        dir.path().join("node.key"),
    );
    write_bundle(&ca, &[a.certificate_pem(), b.certificate_pem()])?;
    fs::write(&cert, node.certificate_pem())?;
    fs::write(&key, node.key_pem())?;
    let identity = Identity::load(&ca, &cert, &key)?;

    // The root token is B's, so only the issuer certificate depends on A.
    let token = delegation_signer(&a, "control-1")?.sign(&child_claims(&fleet, &b)?)?;
    let none = Revocations::new();
    // Twice through verifiers of the same generation: the second is served from its cache.
    for _ in 0..2 {
        identity
            .token_verifier()
            .verify(&token, checked_at(), &none)?;
    }

    // A leaves the bundle; the next check after the reload interval loads a new generation.
    write_bundle(&ca, &[b.certificate_pem()])?;
    std::thread::sleep(RELOAD_WAIT);
    let verifier = identity.token_verifier();
    assert_eq!(verifier.keys().len(), 1, "the bundle was reloaded");
    assert_eq!(
        verifier.verify(&token, checked_at(), &none),
        Err(TokenError::IssuerCertificate(
            IssuerCertError::UnknownAuthority
        ))
    );

    // A returns: a new generation checks the certificate afresh and accepts it again.
    write_bundle(&ca, &[b.certificate_pem(), a.certificate_pem()])?;
    std::thread::sleep(RELOAD_WAIT);
    identity
        .token_verifier()
        .verify(&token, checked_at(), &none)?;
    Ok(())
}
