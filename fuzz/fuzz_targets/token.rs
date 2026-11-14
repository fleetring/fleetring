//! Untrusted bearer tokens and issuer certificates never panic a node (FR-090, ADR-0047 point
//! 1.4). The same bytes are tried as a whole token, as the DER of an issuer certificate, as the
//! certificate in the `x5c` of an otherwise valid delegated token, through a verifier whose
//! cache already holds the fixture's issuer certificate, and as the claims JSON of a token the
//! fixture's issuer key signed, so that what only a signer's claims reach is fuzzed too: the
//! claims' parsing, `check_issuer_signed`, the agent tier, and the checks after them. A stolen
//! issuer key can sign any claims, so they are as untrusted as the rest. `fuzz/seeds/token` holds
//! claims that pass and claims each check refuses.

#![no_main]

use std::sync::OnceLock;
use std::time::Duration;

use base64::Engine as _;
use base64::engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD};
use fleetring_control::delegation::{ChainLink, DelegationId, TokenClaims, TokenKind};
use fleetring_control::{Revocations, Timestamp};
use fleetringd::auth::{BearerToken, DelegationSigner, TokenVerifier, VerifyingKey};
use fleetringd::tls::{Authority, FleetName, IssuerCertificate, Validity};
use libfuzzer_sys::fuzz_target;
use ring::signature::Ed25519KeyPair;
use rustls_pki_types::pem::PemObject as _;
use rustls_pki_types::{CertificateDer, PrivateKeyDer};

/// When the fixture's issuer certificate starts: 2027-01-15.
const ISSUED: Timestamp = Timestamp::from_millis(1_800_000_000_000);

const DAY: Duration = Duration::from_secs(24 * 60 * 60);

/// What every input is checked against: one authority of the fleet `lab`, a verifier trusting
/// it, and a delegated token its issuer signed.
struct Fixture {
    fleet: FleetName,
    authorities: Vec<VerifyingKey>,
    verifier: TokenVerifier,
    /// The issuer's key, which signs the fuzzed claims.
    issuer_key: Ed25519KeyPair,
    /// The valid token's header part, as the issuer signs it.
    header: String,
    /// The valid token's key ID, claims part, and signature part.
    kid: String,
    claims: String,
    signature: String,
    now: Timestamp,
}

fn after(at: Timestamp, span: Duration) -> Timestamp {
    Timestamp::from_millis(at.as_millis() + u64::try_from(span.as_millis()).expect("fits"))
}

fn fixture() -> &'static Fixture {
    static FIXTURE: OnceLock<Fixture> = OnceLock::new();
    FIXTURE.get_or_init(|| {
        let fleet: FleetName = "lab".parse().expect("a fleet");
        let authority =
            Authority::create(fleet.clone(), Validity::new(Timestamp::ZERO, 36_500 * DAY))
                .expect("an authority");
        let authorities = vec![authority.token_signer().expect("Ed25519").verifying_key()];
        let issued = authority
            .issue_issuer(
                &"control-1".parse().expect("a node id"),
                Validity::new(ISSUED, 30 * DAY),
            )
            .expect("an issuer pair");
        let cert = CertificateDer::from_pem_slice(issued.certificate_pem().as_bytes())
            .expect("a certificate");
        let PrivateKeyDer::Pkcs8(key) =
            PrivateKeyDer::from_pem_slice(issued.key_pem().as_bytes()).expect("a key")
        else {
            panic!("an issuer key is PKCS#8");
        };
        let signer =
            DelegationSigner::new(cert.as_ref(), key.secret_pkcs8_der(), &fleet, &authorities)
                .expect("a delegation signer");
        let recorded = after(ISSUED, DAY);
        let claims = TokenClaims {
            fleet: "lab".into(),
            principal: "agent-42.c7".parse().expect("a principal"),
            id: DelegationId::from_bytes([7; 16]).into(),
            issued_at: Some(recorded),
            not_before: recorded,
            expires_at: after(recorded, DAY),
            scopes: ["admin:agent-42.c7-*"]
                .into_iter()
                .map(|scope| scope.parse().expect("a scope"))
                .collect(),
            kind: Some(TokenKind::Agent),
            owner: Some("alice".parse().expect("a principal")),
            parent: Some("agent-42".parse().expect("a principal")),
            chain: vec![ChainLink {
                id: "root-1".parse().expect("a token id"),
                kid: authorities[0].id().clone(),
            }],
        };
        let token = signer.sign(&claims).expect("signs");
        let verifier = TokenVerifier::new("lab", authorities.clone());
        let now = after(recorded, Duration::from_secs(60));
        // Verified once, so the issuer certificate is in the verifier's cache.
        verifier
            .verify(&token, now, &Revocations::new())
            .expect("the fixture's token verifies");
        let issuer_key = Ed25519KeyPair::from_pkcs8_maybe_unchecked(key.secret_pkcs8_der())
            .expect("an Ed25519 key");
        let mut parts = token.expose().split('.');
        let header = parts.next().expect("a header").to_owned();
        Fixture {
            fleet,
            authorities,
            verifier,
            issuer_key,
            header,
            kid: signer.key_id().to_string(),
            claims: parts.next().expect("claims").to_owned(),
            signature: parts.next().expect("a signature").to_owned(),
            now,
        }
    })
}

fuzz_target!(|data: &[u8]| {
    let fixture = fixture();
    let none = Revocations::new();

    // The bytes as a whole token.
    let text = String::from_utf8_lossy(data).into_owned();
    let _ = fixture
        .verifier
        .verify(&BearerToken::new(text), fixture.now, &none);

    // The bytes as an issuer certificate's DER.
    let _ = IssuerCertificate::check(data, &fixture.fleet, &fixture.authorities);

    // The bytes as the certificate in a delegated token's `x5c`, through the cache.
    let header = serde_json::json!({
        "alg": "EdDSA",
        "typ": "JWT",
        "kid": fixture.kid,
        "x5c": [STANDARD.encode(data)],
    });
    let header = URL_SAFE_NO_PAD.encode(serde_json::to_vec(&header).expect("serializes"));
    let token = format!("{header}.{}.{}", fixture.claims, fixture.signature);
    let _ = fixture
        .verifier
        .verify(&BearerToken::new(token), fixture.now, &none);

    // The bytes as the claims JSON of a token the issuer signed, with a valid signature.
    let signed = format!("{}.{}", fixture.header, URL_SAFE_NO_PAD.encode(data));
    let signature = fixture.issuer_key.sign(signed.as_bytes());
    let token = format!("{signed}.{}", URL_SAFE_NO_PAD.encode(signature.as_ref()));
    let _ = fixture
        .verifier
        .verify(&BearerToken::new(token), fixture.now, &none);
});
