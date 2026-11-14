//! The signed-token vectors of token spec v1 (`docs/spec/tokens-v1.md`, section 6): byte-exact
//! headers, claims, and signatures from fixed Ed25519 keys.
//!
//! The vectors live in `fleetring-control`'s `tests/vectors/tokens-v1.json`, whose own tests
//! check every other section and freeze the file. This test checks `signed_tokens`: for each
//! case, the authority's [`TokenSigner`] or a delegation issuer's [`DelegationSigner`], built
//! from the case's seed, must sign the case's claims into exactly the case's token, which a
//! verifier trusting the authority then accepts with exactly those claims. It also checks
//! `refused_tokens`: each token is refused for the case's reason. The tokens of both sections
//! were signed by an implementation independent of this one, from the spec alone.

use base64::Engine as _;
use base64::engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD};
use fleetring_control::delegation::TokenClaims;
use fleetring_control::{Revocations, Timestamp};
use fleetringd::auth::{
    BearerToken, DelegationSigner, Signer, TokenError, TokenSigner, TokenSpec, TokenVerifier,
    VerifyingKey, X5cError,
};
use fleetringd::tls::{FleetName, IssuerCertError, Principal};
use ring::signature::{Ed25519KeyPair, KeyPair as _};
use serde_json::Value;

/// The vector file, which `fleetring-control`'s tests freeze.
const VECTORS: &str = include_str!("../../fleetring-control/tests/vectors/tokens-v1.json");

fn text(value: &Value) -> &str {
    value
        .as_str()
        .unwrap_or_else(|| panic!("{value} is a string"))
}

/// The 32 bytes of a seed in hex.
fn seed(value: &Value) -> [u8; 32] {
    let hex = text(value);
    assert_eq!(hex.len(), 64, "a seed is 32 bytes");
    std::array::from_fn(|i| u8::from_str_radix(&hex[2 * i..2 * i + 2], 16).expect("hex"))
}

/// Wraps a seed in a PKCS#8 v1 Ed25519 private key (RFC 8410).
fn pkcs8(seed: &[u8; 32]) -> Vec<u8> {
    let mut der = vec![
        0x30, 0x2e, 0x02, 0x01, 0x00, 0x30, 0x05, 0x06, 0x03, 0x2b, 0x65, 0x70, 0x04, 0x22, 0x04,
        0x20,
    ];
    der.extend_from_slice(seed);
    der
}

/// The key that verifies the seed's signatures.
fn verifying_key(seed: &[u8; 32]) -> VerifyingKey {
    let pair = Ed25519KeyPair::from_seed_unchecked(seed).expect("a 32-byte seed");
    VerifyingKey::from_raw(pair.public_key().as_ref()).expect("a 32-byte public key")
}

/// The JSON a part of the compact JWS encodes, as text.
fn decoded(part: &str) -> String {
    String::from_utf8(URL_SAFE_NO_PAD.decode(part).expect("base64url")).expect("UTF-8")
}

#[test]
fn every_signed_token_is_signed_byte_for_byte() {
    let vectors: Value = serde_json::from_str(VECTORS).expect("tokens-v1.json is valid JSON");
    let cases = vectors["signed_tokens"].as_array().expect("signed_tokens");
    assert!(!cases.is_empty());
    for case in cases {
        let name = text(&case["name"]);
        let claims: TokenClaims =
            serde_json::from_value(case["claims"].clone()).expect("the claims");
        let key = seed(&case["seed"]);
        let fleet: FleetName = claims.fleet.parse().expect("a fleet name");
        assert_eq!(
            verifying_key(&key).id().as_str(),
            text(&case["kid"]),
            "{name}: kid"
        );

        let token = match text(&case["signer"]) {
            "authority" => {
                let signer =
                    TokenSigner::from_pkcs8(&pkcs8(&key), fleet.as_str()).expect("a signing key");
                assert_eq!(signer.key_id().as_str(), text(&case["kid"]), "{name}");
                let mut spec = TokenSpec::new(
                    claims.principal.as_str().parse().expect("a principal"),
                    claims.scopes.clone(),
                    claims.id.clone(),
                    claims.not_before,
                    claims.expires_at,
                );
                spec.issued_at = claims.issued_at.expect("an iat");
                if let Some(owner) = &claims.owner {
                    let owner: Principal = owner.as_str().parse().expect("an owner");
                    spec = spec.agent(owner);
                }
                let token = signer.issue(&spec).expect("signs");

                // A verifier trusting the authority alone accepts it as signed.
                let verifier = TokenVerifier::new(fleet.as_str(), [signer.verifying_key()]);
                let verified = verifier
                    .verify(&token, claims.not_before, &Revocations::new())
                    .expect("verifies");
                // It carries exactly the vector's claims, `iat` included, under the authority's
                // key.
                assert_eq!(verified.claims, claims, "{name}: claims");
                assert_eq!(
                    verified.principal.as_str(),
                    claims.principal.as_str(),
                    "{name}"
                );
                assert_eq!(verified.key, signer.key_id().clone(), "{name}: key");
                token
            }
            "issuer" => {
                let authority = seed(&case["authority_seed"]);
                assert_eq!(
                    verifying_key(&authority).id().as_str(),
                    text(&case["authority_kid"]),
                    "{name}: the authority's kid"
                );
                let certificate = STANDARD
                    .decode(text(&case["issuer_certificate"]))
                    .expect("standard base64");
                let signer = DelegationSigner::new(
                    &certificate,
                    &pkcs8(&key),
                    &fleet,
                    &[verifying_key(&authority)],
                )
                .expect("an issuer pair");
                assert_eq!(signer.key_id().as_str(), text(&case["kid"]), "{name}");
                assert!(claims.expires_at <= signer.expires_at(), "{name}");
                let token = signer.sign(&claims).expect("signs");

                // A verifier trusting the authority alone accepts it through its issuer
                // certificate, with exactly the vector's claims, under the issuer's key.
                let verifier = TokenVerifier::new(fleet.as_str(), [verifying_key(&authority)]);
                let verified = verifier
                    .verify(&token, claims.not_before, &Revocations::new())
                    .expect("verifies");
                assert_eq!(verified.claims, claims, "{name}: claims");
                assert_eq!(verified.key, signer.key_id().clone(), "{name}: key");
                assert!(
                    matches!(
                        &verified.signer,
                        Signer::Issuer { authority, .. }
                            if authority.as_str() == text(&case["authority_kid"])
                    ),
                    "{name}: {:?}",
                    verified.signer
                );
                token
            }
            other => panic!("unknown signer {other}"),
        };

        let compact = token.expose();
        let parts: Vec<&str> = compact.split('.').collect();
        let [header, payload, _signature] = parts[..] else {
            panic!("{name}: a compact JWS has three parts");
        };
        assert_eq!(decoded(header), text(&case["header"]), "{name}: header");
        assert_eq!(decoded(payload), text(&case["payload"]), "{name}: payload");
        assert_eq!(compact, text(&case["token"]), "{name}: token");
        if case["signer"] == "issuer" {
            let header: Value = serde_json::from_str(text(&case["header"])).expect("JSON");
            assert_eq!(header["x5c"][0], case["issuer_certificate"], "{name}: x5c");
        }
        // Whole seconds, as the claims are.
        for at in [claims.not_before, claims.expires_at] {
            assert_eq!(at, Timestamp::from_millis(at.as_millis() / 1_000 * 1_000));
        }
    }
}

/// The name `refused_tokens` gives a refusal.
fn refusal(error: &TokenError) -> &'static str {
    match error {
        TokenError::Malformed => "malformed",
        TokenError::X5c {
            reason: X5cError::Encoding,
        } => "x5c_encoding",
        TokenError::KidMismatch => "kid_mismatch",
        TokenError::IssuerCertificate(IssuerCertError::Expired) => "issuer_certificate_expired",
        TokenError::OutlivesCertificate => "outlives_certificate",
        TokenError::UntrustedRoot => "untrusted_root",
        other => panic!("no vector names the refusal {other:?}"),
    }
}

#[test]
fn every_refused_token_is_refused_for_its_reason() {
    let vectors: Value = serde_json::from_str(VECTORS).expect("tokens-v1.json is valid JSON");
    let cases = vectors["refused_tokens"]
        .as_array()
        .expect("refused_tokens");
    assert!(!cases.is_empty());
    for case in cases {
        let name = text(&case["name"]);
        let verifier = TokenVerifier::new(
            text(&case["fleet"]),
            [verifying_key(&seed(&case["authority_seed"]))],
        );
        let now = Timestamp::from_millis(case["now"].as_u64().expect("now is milliseconds"));
        let token = BearerToken::new(text(&case["token"]));
        let error = verifier
            .verify(&token, now, &Revocations::new())
            .expect_err(name);
        assert_eq!(refusal(&error), text(&case["refused"]), "{name}: {error}");
    }
}
