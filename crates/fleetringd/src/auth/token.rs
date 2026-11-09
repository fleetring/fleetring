//! Bearer tokens: Ed25519-signed JWTs that name a principal and carry its scopes (ADR-0025).
//!
//! A token is a compact JWS, `<header>.<claims>.<signature>`, each part base64url without
//! padding. The header is `{"alg":"EdDSA","typ":"JWT","kid":"<key id>"}`; the claims are the
//! registered `iss` (the fleet), `sub` (the principal), `jti` (the token's ID), `iat`, `nbf`,
//! and `exp`, plus one FleetRing claim, `fleetring.scopes`, the scopes as strings. Standard JWT
//! libraries can inspect and mint them.
//!
//! The authority signs tokens off the nodes with its Ed25519 key ([`TokenSigner`]). Nodes verify
//! them with the authority's public key alone ([`TokenVerifier`]): no network, no control round
//! trip, and no state but the denylist the control plane publishes. Verification accepts only
//! `EdDSA` from a trusted key ID, so there is no algorithm to confuse, and refuses headers or
//! claims it doesn't know rather than ignore a constraint it can't enforce.

use std::fmt;
use std::time::Duration;

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use fleetring_control::{KeyId, Revocations, Timestamp, TokenId};
use ring::signature::{ED25519, Ed25519KeyPair, KeyPair as _, UnparsedPublicKey};
use serde::{Deserialize, Serialize};

use crate::auth::{MAX_SCOPES, ParseScopeError, Scope, Scopes};
use crate::tls::Principal;

/// The longest token accepted, in bytes. A token carries a few dozen scopes at most.
pub const MAX_TOKEN_LEN: usize = 8 * 1024;

/// How long `fleetringd token issue` makes a token last unless told otherwise.
pub const DEFAULT_TOKEN_LIFETIME: Duration = Duration::from_secs(60 * 60);

/// The longest a token may last, from `nbf` to `exp`. Signers refuse longer tokens and nodes
/// refuse them however they were minted, so a token revocation kept this long outlives every
/// token, and the denylist stays small.
pub const MAX_TOKEN_LIFETIME: Duration = Duration::from_secs(30 * 24 * 60 * 60);

/// How far a node's clock may run behind the issuer's before a fresh token is refused as not yet
/// valid. Expiry gets no such allowance: an expired token is always refused.
pub const NOT_BEFORE_LEEWAY_SECS: u64 = 30;

/// The only signature algorithm tokens use.
const ALGORITHM: &str = "EdDSA";

/// The token type in the header.
const TYPE: &str = "JWT";

/// How many bytes of the public key's SHA-256 digest make up a key ID.
const KEY_ID_DIGEST_BYTES: usize = 12;

/// The length of an Ed25519 public key, in bytes.
const ED25519_PUBLIC_KEY_LEN: usize = 32;

const MILLIS_PER_SEC: u64 = 1_000;

/// A signed token, as sent in `authorization: Bearer <token>`.
///
/// `Debug` hides the token: it is a credential, and it must never reach a log.
#[derive(Clone, PartialEq, Eq)]
pub struct BearerToken(String);

impl BearerToken {
    /// Wraps the text of a token, as received.
    #[must_use]
    pub fn new(text: impl Into<String>) -> Self {
        Self(text.into())
    }

    /// Returns the token's text, to send. Keep it out of logs.
    #[must_use]
    pub fn expose(&self) -> &str {
        &self.0
    }

    /// The principal the token's `sub` claim names, read without verifying the token: who a
    /// client holding it acts as, for its own display. Nodes verify every token they are sent;
    /// nothing may be authorized by this. `None` for text that isn't a token.
    #[must_use]
    pub fn claimed_principal(&self) -> Option<Principal> {
        let payload = self.0.split('.').nth(1)?;
        let claims: Claims = decode_json(payload).ok()?;
        claims.sub.parse().ok()
    }

    /// The token's ID (`jti`) and the moment it expires (`exp`), read without verifying the
    /// token, as [`claimed_principal`](Self::claimed_principal) reads `sub`: for revoking a token
    /// its holder issued, until the token itself would stop being valid. `None` for text that
    /// isn't a token.
    #[must_use]
    pub fn claimed_id_and_expiry(&self) -> Option<(TokenId, Timestamp)> {
        let payload = self.0.split('.').nth(1)?;
        let claims: Claims = decode_json(payload).ok()?;
        Some((claims.jti.parse().ok()?, from_seconds(claims.exp)))
    }
}

impl fmt::Debug for BearerToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("BearerToken(..)")
    }
}

/// The ID tokens name a signing key by: the first 12 bytes of the SHA-256 digest of its raw
/// Ed25519 public key, base64url without padding.
fn key_id_of(public_key: &[u8]) -> KeyId {
    let digest = ring::digest::digest(&ring::digest::SHA256, public_key);
    let prefix = digest
        .as_ref()
        .get(..KEY_ID_DIGEST_BYTES)
        .unwrap_or_default();
    URL_SAFE_NO_PAD
        .encode(prefix)
        .parse()
        .expect("16 base64url characters form a valid key id")
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Header {
    alg: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    typ: Option<String>,
    kid: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Claims {
    iss: String,
    sub: String,
    jti: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    iat: Option<u64>,
    nbf: u64,
    exp: u64,
    #[serde(rename = "fleetring.scopes")]
    scopes: Vec<String>,
}

/// What a token grants: who, what, and when. The issuer is the signer's fleet.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TokenSpec {
    /// The principal the token names.
    pub principal: Principal,
    /// What the principal may do.
    pub scopes: Scopes,
    /// The token's ID, by which it can be revoked.
    pub id: TokenId,
    /// The first moment the token is valid.
    pub not_before: Timestamp,
    /// The moment the token stops being valid.
    pub expires_at: Timestamp,
}

/// The error returned when a token can't be issued.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum IssueError {
    /// The key isn't an Ed25519 private key in PKCS#8.
    #[error("the signing key isn't an Ed25519 PKCS#8 key")]
    Key,
    /// The token would expire before, or when, it becomes valid.
    #[error("a token must expire after it becomes valid")]
    Lifetime,
    /// The token would last longer than [`MAX_TOKEN_LIFETIME`].
    #[error("a token lasts at most {} days", MAX_TOKEN_LIFETIME.as_secs() / (24 * 60 * 60))]
    TooLong,
    /// The token would carry no scope.
    #[error("a token needs at least one scope")]
    NoScopes,
    /// The token would carry more than [`MAX_SCOPES`] scopes, more than its holder could
    /// narrow for a child.
    #[error("a token carries at most {limit} scopes")]
    TooManyScopes {
        /// The most a token may carry, [`MAX_SCOPES`].
        limit: usize,
    },
}

/// The authority's signing key, which issues tokens off the nodes.
///
/// `Debug` shows the key ID and the issuer, never the key.
pub struct TokenSigner {
    key: Ed25519KeyPair,
    id: KeyId,
    issuer: String,
}

impl fmt::Debug for TokenSigner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TokenSigner")
            .field("id", &self.id)
            .field("issuer", &self.issuer)
            .finish_non_exhaustive()
    }
}

impl TokenSigner {
    /// A signer for the fleet `issuer` from an Ed25519 private key in PKCS#8 DER, as the
    /// fleet authority keeps it.
    ///
    /// # Errors
    ///
    /// [`IssueError::Key`] if the bytes aren't an Ed25519 PKCS#8 key.
    pub fn from_pkcs8(der: &[u8], issuer: impl Into<String>) -> Result<Self, IssueError> {
        let key = Ed25519KeyPair::from_pkcs8_maybe_unchecked(der).map_err(|_| IssueError::Key)?;
        let id = key_id_of(key.public_key().as_ref());
        Ok(Self {
            key,
            id,
            issuer: issuer.into(),
        })
    }

    /// Returns the ID tokens name this key by.
    #[must_use]
    pub fn key_id(&self) -> &KeyId {
        &self.id
    }

    /// Returns the key nodes verify this signer's tokens with.
    #[must_use]
    pub fn verifying_key(&self) -> VerifyingKey {
        VerifyingKey::from_raw(self.key.public_key().as_ref())
            .expect("an Ed25519 key pair has a 32-byte public key")
    }

    /// Signs the token `spec` describes. Times are whole seconds; milliseconds are dropped.
    ///
    /// # Errors
    ///
    /// [`IssueError::NoScopes`] for a spec without scopes, [`IssueError::TooManyScopes`] for one
    /// with more than [`MAX_SCOPES`], [`IssueError::Lifetime`] for one that expires before it
    /// starts, and [`IssueError::TooLong`] for one that lasts longer than
    /// [`MAX_TOKEN_LIFETIME`].
    pub fn issue(&self, spec: &TokenSpec) -> Result<BearerToken, IssueError> {
        if spec.scopes.is_empty() {
            return Err(IssueError::NoScopes);
        }
        if spec.scopes.len() > MAX_SCOPES {
            return Err(IssueError::TooManyScopes { limit: MAX_SCOPES });
        }
        let nbf = seconds(spec.not_before);
        let exp = seconds(spec.expires_at);
        if exp <= nbf {
            return Err(IssueError::Lifetime);
        }
        if exp - nbf > MAX_TOKEN_LIFETIME.as_secs() {
            return Err(IssueError::TooLong);
        }
        let header = Header {
            alg: ALGORITHM.to_owned(),
            typ: Some(TYPE.to_owned()),
            kid: self.id.to_string(),
        };
        let claims = Claims {
            iss: self.issuer.clone(),
            sub: spec.principal.to_string(),
            jti: spec.id.to_string(),
            iat: Some(nbf),
            nbf,
            exp,
            scopes: spec
                .scopes
                .as_slice()
                .iter()
                .map(Scope::to_string)
                .collect(),
        };
        let mut text = encode_json(&header);
        text.push('.');
        text.push_str(&encode_json(&claims));
        let signature = self.key.sign(text.as_bytes());
        text.push('.');
        text.push_str(&URL_SAFE_NO_PAD.encode(signature.as_ref()));
        Ok(BearerToken(text))
    }
}

fn encode_json<T: Serialize>(value: &T) -> String {
    let json = serde_json::to_vec(value).expect("token headers and claims serialize");
    URL_SAFE_NO_PAD.encode(json)
}

fn seconds(at: Timestamp) -> u64 {
    at.as_millis() / MILLIS_PER_SEC
}

fn from_seconds(secs: u64) -> Timestamp {
    Timestamp::from_millis(secs.saturating_mul(MILLIS_PER_SEC))
}

/// An Ed25519 public key that signs tokens nodes accept.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifyingKey {
    id: KeyId,
    key: [u8; ED25519_PUBLIC_KEY_LEN],
}

impl VerifyingKey {
    /// The key from its raw 32 bytes, or `None` if `raw` isn't 32 bytes long.
    #[must_use]
    pub fn from_raw(raw: &[u8]) -> Option<Self> {
        let key: [u8; ED25519_PUBLIC_KEY_LEN] = raw.try_into().ok()?;
        Some(Self {
            id: key_id_of(&key),
            key,
        })
    }

    /// Returns the ID tokens name this key by.
    #[must_use]
    pub fn id(&self) -> &KeyId {
        &self.id
    }
}

/// The error returned for a token a node refuses. The messages never quote the token.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum TokenError {
    /// The token isn't a compact JWS with the header and claims tokens carry.
    #[error("the token is malformed")]
    Malformed,
    /// The header names an algorithm other than `EdDSA`.
    #[error("the token's algorithm isn't EdDSA")]
    Algorithm,
    /// No trusted key has the token's key ID.
    #[error("the token's signing key isn't trusted")]
    UnknownKey,
    /// The signature doesn't verify.
    #[error("the token's signature is invalid")]
    Signature,
    /// The token was issued for another fleet.
    #[error("the token was issued for another fleet")]
    Issuer,
    /// The token isn't valid yet.
    #[error("the token isn't valid yet")]
    NotYetValid,
    /// The token has expired.
    #[error("the token has expired")]
    Expired,
    /// The token lasts longer than [`MAX_TOKEN_LIFETIME`], so a revocation might not outlive it.
    #[error("the token lasts longer than tokens may")]
    Lifetime,
    /// The token, or the key that signed it, is revoked.
    #[error("the token is revoked")]
    Revoked,
    /// A scope in the token doesn't parse.
    #[error("the token carries an invalid scope")]
    Scope(#[source] ParseScopeError),
    /// The token's subject is a reserved dotted agent name, which only a delegation may create
    /// (ADR-0044, point 1.8).
    #[error(
        "the token names a principal reserved for delegated agents (agent-<id> with '.'); \
         reissue it under another name"
    )]
    ReservedPrincipal,
}

/// A refused token's error, with the token's ID when the signature verified, so that the ID is
/// the issuer's and not whatever a sender wrote into a forged token.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Refusal {
    error: TokenError,
    token: Option<TokenId>,
}

impl Refusal {
    fn of(error: TokenError) -> Self {
        Self { error, token: None }
    }

    fn of_token(error: TokenError, token: Option<TokenId>) -> Self {
        Self { error, token }
    }

    /// Why the token is refused.
    pub(crate) fn into_error(self) -> TokenError {
        self.error
    }

    /// The refused token's ID, if its signature verified and its claims carry one.
    pub(crate) fn token(&self) -> Option<&TokenId> {
        self.token.as_ref()
    }
}

impl From<TokenError> for Refusal {
    fn from(error: TokenError) -> Self {
        Self::of(error)
    }
}

/// What a node checks tokens against: the fleet they must be issued for and the keys trusted
/// to sign them.
///
/// More than one key may be trusted, so a rotation overlaps, and a later delegated issuer is one
/// more key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TokenVerifier {
    issuer: String,
    keys: Vec<VerifyingKey>,
}

impl TokenVerifier {
    /// A verifier that accepts tokens issued for `issuer` and signed by any of `keys`.
    #[must_use]
    pub fn new(issuer: impl Into<String>, keys: impl IntoIterator<Item = VerifyingKey>) -> Self {
        Self {
            issuer: issuer.into(),
            keys: keys.into_iter().collect(),
        }
    }

    /// Returns the trusted keys.
    #[must_use]
    pub fn keys(&self) -> &[VerifyingKey] {
        &self.keys
    }

    /// Verifies `token` at `now` against the trusted keys and `revoked`, and returns what it
    /// grants with the key that signed it.
    ///
    /// The checks run in an order that does little work for an untrusted sender: the size, the
    /// header, a trusted key, the signature, and only then the claims, the validity window, and
    /// the revocation of the token or its key.
    ///
    /// # Errors
    ///
    /// A [`TokenError`] saying why the token is refused.
    pub fn verify(
        &self,
        token: &BearerToken,
        now: Timestamp,
        revoked: &Revocations,
    ) -> Result<Verified, TokenError> {
        self.verify_identified(token, now, revoked)
            .map_err(Refusal::into_error)
    }

    /// Verifies `token` as [`verify`](Self::verify) does, and names the token in a refusal once
    /// its signature has verified: an expired, revoked, or otherwise refused token can still be
    /// told apart from the others by its ID, which an audit line records.
    pub(crate) fn verify_identified(
        &self,
        token: &BearerToken,
        now: Timestamp,
        revoked: &Revocations,
    ) -> Result<Verified, Refusal> {
        let text = token.expose();
        if text.len() > MAX_TOKEN_LEN {
            return Err(TokenError::Malformed.into());
        }
        let mut parts = text.split('.');
        let (Some(header), Some(claims), Some(signature), None) =
            (parts.next(), parts.next(), parts.next(), parts.next())
        else {
            return Err(TokenError::Malformed.into());
        };
        let signed = text
            .get(..header.len() + 1 + claims.len())
            .ok_or(TokenError::Malformed)?;

        let header: Header = decode_json(header)?;
        if header.alg != ALGORITHM {
            return Err(TokenError::Algorithm.into());
        }
        if header.typ.as_deref().is_some_and(|typ| typ != TYPE) {
            return Err(TokenError::Malformed.into());
        }
        let key_id: KeyId = header.kid.parse().map_err(|_| TokenError::Malformed)?;
        let key = self
            .keys
            .iter()
            .find(|key| key.id == key_id)
            .ok_or(TokenError::UnknownKey)?;
        let signature = URL_SAFE_NO_PAD
            .decode(signature)
            .map_err(|_| TokenError::Malformed)?;
        UnparsedPublicKey::new(&ED25519, &key.key)
            .verify(signed.as_bytes(), &signature)
            .map_err(|_| TokenError::Signature)?;

        // The signature holds, so the claims are the issuer's and so is the ID they carry.
        let claims: Claims = decode_json(claims)?;
        let id: Option<TokenId> = claims.jti.parse().ok();
        let refuse = |error| Refusal::of_token(error, id.clone());
        if revoked.contains(&fleetring_control::Revoked::Key(key_id.clone())) {
            return Err(refuse(TokenError::Revoked));
        }
        if claims.iss != self.issuer {
            return Err(refuse(TokenError::Issuer));
        }
        if claims.exp.saturating_sub(claims.nbf) > MAX_TOKEN_LIFETIME.as_secs() {
            return Err(refuse(TokenError::Lifetime));
        }
        let now_secs = seconds(now);
        if now_secs.saturating_add(NOT_BEFORE_LEEWAY_SECS) < claims.nbf {
            return Err(refuse(TokenError::NotYetValid));
        }
        if now_secs >= claims.exp {
            return Err(refuse(TokenError::Expired));
        }
        let id = id.clone().ok_or(TokenError::Malformed)?;
        let principal: Principal = claims
            .sub
            .parse()
            .map_err(|_| refuse(TokenError::Malformed))?;
        if principal.is_reserved_agent_name() {
            return Err(refuse(TokenError::ReservedPrincipal));
        }
        if revoked.refuses(&id, &key_id) {
            return Err(refuse(TokenError::Revoked));
        }
        let scopes = claims
            .scopes
            .iter()
            .map(|scope| scope.parse())
            .collect::<Result<Scopes, _>>()
            .map_err(|error| refuse(TokenError::Scope(error)))?;
        Ok(Verified {
            spec: TokenSpec {
                principal,
                scopes,
                id,
                not_before: from_seconds(claims.nbf),
                expires_at: from_seconds(claims.exp),
            },
            key: key_id,
        })
    }
}

/// A token that verified: what it grants, and the key that signed it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Verified {
    /// What the token grants.
    pub spec: TokenSpec,
    /// The key that signed it, by which every token it signed can be revoked.
    pub key: KeyId,
}

fn decode_json<T: for<'de> Deserialize<'de>>(part: &str) -> Result<T, TokenError> {
    let json = URL_SAFE_NO_PAD
        .decode(part)
        .map_err(|_| TokenError::Malformed)?;
    serde_json::from_slice(&json).map_err(|_| TokenError::Malformed)
}

#[cfg(test)]
mod tests {
    use super::*;

    use fleetring_control::Revoked;

    /// A fixed Ed25519 PKCS#8 key, so the tests need no randomness.
    fn key(seed: u8, issuer: &str) -> TokenSigner {
        let key = Ed25519KeyPair::from_seed_unchecked(&[seed; 32]).expect("a 32-byte seed");
        let der = pkcs8_v1(&[seed; 32]);
        let signer = TokenSigner::from_pkcs8(&der, issuer).expect("a PKCS#8 key");
        assert_eq!(signer.key.public_key().as_ref(), key.public_key().as_ref());
        signer
    }

    /// Wraps a seed in a PKCS#8 v1 Ed25519 private key (RFC 8410).
    fn pkcs8_v1(seed: &[u8; 32]) -> Vec<u8> {
        let mut der = vec![
            0x30, 0x2e, 0x02, 0x01, 0x00, 0x30, 0x05, 0x06, 0x03, 0x2b, 0x65, 0x70, 0x04, 0x22,
            0x04, 0x20,
        ];
        der.extend_from_slice(seed);
        der
    }

    fn spec(scopes: &[&str]) -> TokenSpec {
        TokenSpec {
            principal: "app".parse().expect("a principal"),
            scopes: scopes
                .iter()
                .map(|scope| scope.parse().expect("a scope"))
                .collect(),
            id: "tok-1".parse().expect("a token id"),
            not_before: Timestamp::from_millis(1_000_000),
            expires_at: Timestamp::from_millis(4_600_000),
        }
    }

    fn at_secs(secs: u64) -> Timestamp {
        Timestamp::from_millis(secs * 1_000)
    }

    #[test]
    fn a_token_round_trips_and_names_its_key() {
        let signer = key(7, "prod");
        let verifier = TokenVerifier::new("prod", [signer.verifying_key()]);
        let issued = signer
            .issue(&spec(&["read:tenant:acme/*"]))
            .expect("issues");
        let got = verifier
            .verify(&issued, at_secs(2_000), &Revocations::new())
            .expect("verifies");
        assert_eq!(got.spec, spec(&["read:tenant:acme/*"]));
        assert_eq!(&got.key, signer.key_id());

        let header = issued.expose().split('.').next().expect("a header");
        let header: Header = decode_json(header).expect("a header decodes");
        assert_eq!(header.kid, signer.key_id().as_str());
        assert_eq!(header.alg, "EdDSA");
        assert_eq!(signer.key_id().as_str().len(), 16);
        assert_eq!(
            issued.claimed_principal().map(|p| p.to_string()).as_deref(),
            Some("app")
        );
        assert_eq!(BearerToken::new("not.a token").claimed_principal(), None);
        assert_eq!(
            issued.claimed_id_and_expiry(),
            Some((
                "tok-1".parse().expect("an id"),
                Timestamp::from_millis(4_600_000)
            ))
        );
        assert_eq!(
            BearerToken::new("not.a token").claimed_id_and_expiry(),
            None
        );
    }

    #[test]
    fn the_validity_window_is_enforced() {
        let signer = key(7, "prod");
        let verifier = TokenVerifier::new("prod", [signer.verifying_key()]);
        let issued = signer.issue(&spec(&["read:x"])).expect("issues");
        let none = Revocations::new();
        assert_eq!(
            verifier.verify(&issued, at_secs(1_000 - NOT_BEFORE_LEEWAY_SECS - 1), &none),
            Err(TokenError::NotYetValid)
        );
        assert!(
            verifier
                .verify(&issued, at_secs(1_000 - NOT_BEFORE_LEEWAY_SECS), &none)
                .is_ok()
        );
        assert!(
            verifier
                .verify(&issued, Timestamp::from_millis(4_599_999), &none)
                .is_ok()
        );
        assert_eq!(
            verifier.verify(&issued, at_secs(4_600), &none),
            Err(TokenError::Expired)
        );
    }

    #[test]
    fn a_tampered_token_is_refused() {
        let signer = key(7, "prod");
        let verifier = TokenVerifier::new("prod", [signer.verifying_key()]);
        let issued = signer.issue(&spec(&["read:x"])).expect("issues");
        let parts: Vec<&str> = issued.expose().split('.').collect();
        let now = at_secs(2_000);
        let none = Revocations::new();

        // Widen the scopes and keep the signature.
        let mut claims: Claims = decode_json(parts[1]).expect("claims decode");
        claims.scopes = vec!["admin:*".to_owned()];
        let forged = format!("{}.{}.{}", parts[0], encode_json(&claims), parts[2]);
        assert_eq!(
            verifier.verify(&BearerToken::new(forged), now, &none),
            Err(TokenError::Signature)
        );

        // Flip one bit of the signature.
        let mut signature = URL_SAFE_NO_PAD.decode(parts[2]).expect("base64");
        signature[0] ^= 1;
        let flipped = format!(
            "{}.{}.{}",
            parts[0],
            parts[1],
            URL_SAFE_NO_PAD.encode(&signature)
        );
        assert_eq!(
            verifier.verify(&BearerToken::new(flipped), now, &none),
            Err(TokenError::Signature)
        );

        // Signed by a key nobody trusts, under the trusted key's ID.
        let rogue = key(9, "prod").issue(&spec(&["admin:*"])).expect("issues");
        let rogue_parts: Vec<&str> = rogue.expose().split('.').collect();
        let spliced = format!("{}.{}.{}", parts[0], rogue_parts[1], rogue_parts[2]);
        assert_eq!(
            verifier.verify(&BearerToken::new(spliced), now, &none),
            Err(TokenError::Signature)
        );
        assert_eq!(
            verifier.verify(&rogue, now, &none),
            Err(TokenError::UnknownKey)
        );
    }

    #[test]
    fn other_algorithms_unknown_fields_and_garbage_are_refused() {
        let signer = key(7, "prod");
        let verifier = TokenVerifier::new("prod", [signer.verifying_key()]);
        let issued = signer.issue(&spec(&["read:x"])).expect("issues");
        let parts: Vec<&str> = issued.expose().split('.').collect();
        let now = at_secs(2_000);
        let none = Revocations::new();
        let with_header = |header: &str| {
            BearerToken::new(format!(
                "{}.{}.{}",
                URL_SAFE_NO_PAD.encode(header),
                parts[1],
                parts[2]
            ))
        };
        let kid = signer.key_id().as_str();
        for (header, want) in [
            (
                format!(r#"{{"alg":"none","kid":"{kid}"}}"#),
                TokenError::Algorithm,
            ),
            (
                format!(r#"{{"alg":"HS256","kid":"{kid}"}}"#),
                TokenError::Algorithm,
            ),
            (
                format!(r#"{{"alg":"EdDSA","kid":"{kid}","crit":["x"]}}"#),
                TokenError::Malformed,
            ),
            (r#"{"alg":"EdDSA"}"#.to_owned(), TokenError::Malformed),
        ] {
            assert_eq!(
                verifier.verify(&with_header(&header), now, &none),
                Err(want),
                "{header}"
            );
        }
        for garbage in [
            "",
            "a.b",
            "a.b.c.d",
            "!!.!!.!!",
            &"a".repeat(MAX_TOKEN_LEN + 1),
        ] {
            assert_eq!(
                verifier.verify(&BearerToken::new(garbage), now, &none),
                Err(TokenError::Malformed),
            );
        }
    }

    #[test]
    fn a_token_longer_than_the_maximum_is_refused_however_it_was_minted() {
        let signer = key(7, "prod");
        let verifier = TokenVerifier::new("prod", [signer.verifying_key()]);
        let max = MAX_TOKEN_LIFETIME.as_secs();
        let mut long = spec(&["read:x"]);
        long.expires_at = Timestamp::from_millis(long.not_before.as_millis() + (max + 1) * 1_000);
        assert_eq!(signer.issue(&long), Err(IssueError::TooLong));
        let mut longest = spec(&["read:x"]);
        longest.expires_at = Timestamp::from_millis(longest.not_before.as_millis() + max * 1_000);
        assert!(signer.issue(&longest).is_ok());

        // Another library signs whatever it is given: the node still refuses it.
        let header = Header {
            alg: ALGORITHM.to_owned(),
            typ: Some(TYPE.to_owned()),
            kid: signer.key_id().to_string(),
        };
        let claims = Claims {
            iss: "prod".to_owned(),
            sub: "app".to_owned(),
            jti: "long".to_owned(),
            iat: None,
            nbf: 1_000,
            exp: 1_000 + max + 1,
            scopes: vec!["read:x".to_owned()],
        };
        let mut text = encode_json(&header);
        text.push('.');
        text.push_str(&encode_json(&claims));
        let signature = signer.key.sign(text.as_bytes());
        text.push('.');
        text.push_str(&URL_SAFE_NO_PAD.encode(signature.as_ref()));
        assert_eq!(
            verifier.verify(&BearerToken::new(text), at_secs(2_000), &Revocations::new()),
            Err(TokenError::Lifetime)
        );
    }

    #[test]
    fn fork_and_grant_travel_in_a_token_and_grant_authorizes_nothing() {
        use crate::auth::{Action, Target};

        let signer = key(7, "prod");
        let verifier = TokenVerifier::new("prod", [signer.verifying_key()]);
        let none = Revocations::new();
        let issued = signer
            .issue(&spec(&["fork:src-*", "grant:agent-42.*"]))
            .expect("issues");
        let claims = issued.expose().split('.').nth(1).expect("a claims part");
        let claims: Claims = decode_json(claims).expect("claims decode");
        assert_eq!(claims.scopes, ["fork:src-*", "grant:agent-42.*"]);
        let verified = verifier
            .verify(&issued, at_secs(2_000), &none)
            .expect("verifies");
        let scopes = &verified.spec.scopes;
        let source = "src-1".parse().expect("a name");
        assert!(scopes.allows(Target::Database(Action::Fork, &source)));
        assert!(scopes.allows(Target::Database(Action::Read, &source)));
        assert!(!scopes.allows(Target::Database(Action::Write, &source)));
        let child = "agent-42.c7-notes".parse().expect("a name");
        for action in Action::ALL {
            assert!(
                !scopes.allows(Target::Database(action, &child)),
                "grant allowed {action}"
            );
        }
    }

    #[test]
    fn the_fleet_scope_is_written_and_read_as_fleet_and_cluster_is_refused() {
        use crate::auth::Target;

        let signer = key(7, "prod");
        let verifier = TokenVerifier::new("prod", [signer.verifying_key()]);
        let none = Revocations::new();
        let now = at_secs(2_000);

        let issued = signer.issue(&spec(&["fleet"])).expect("issues");
        let claims = issued.expose().split('.').nth(1).expect("a claims part");
        let claims: Claims = decode_json(claims).expect("claims decode");
        assert_eq!(claims.scopes, ["fleet"]);
        let verified = verifier.verify(&issued, now, &none).expect("verifies");
        assert!(verified.spec.scopes.allows(Target::Fleet));

        // A token minted by 0.2 carries the scope's old spelling, which 0.3 doesn't accept.
        let header = Header {
            alg: ALGORITHM.to_owned(),
            typ: Some(TYPE.to_owned()),
            kid: signer.key_id().to_string(),
        };
        let claims = Claims {
            iss: "prod".to_owned(),
            sub: "ops".to_owned(),
            jti: "old".to_owned(),
            iat: None,
            nbf: 1_000,
            exp: 4_600,
            scopes: vec!["cluster".to_owned()],
        };
        let mut text = encode_json(&header);
        text.push('.');
        text.push_str(&encode_json(&claims));
        let signature = signer.key.sign(text.as_bytes());
        text.push('.');
        text.push_str(&URL_SAFE_NO_PAD.encode(signature.as_ref()));
        assert!(matches!(
            verifier.verify(&BearerToken::new(text), now, &none),
            Err(TokenError::Scope(ParseScopeError::Shape { scope })) if scope == "cluster"
        ));
    }

    #[test]
    fn a_token_for_another_fleet_is_refused() {
        let signer = key(7, "staging");
        let verifier = TokenVerifier::new("prod", [signer.verifying_key()]);
        let issued = signer.issue(&spec(&["read:x"])).expect("issues");
        assert_eq!(
            verifier.verify(&issued, at_secs(2_000), &Revocations::new()),
            Err(TokenError::Issuer)
        );
    }

    #[test]
    fn a_token_naming_a_reserved_dotted_agent_is_refused() {
        let signer = key(7, "prod");
        let verifier = TokenVerifier::new("prod", [signer.verifying_key()]);
        let dotted = TokenSpec {
            principal: "agent-42.c7".parse().expect("a principal"),
            ..spec(&["read:agent-42.c7-*"])
        };
        let issued = signer.issue(&dotted).expect("issues");
        assert_eq!(
            verifier.verify(&issued, at_secs(2_000), &Revocations::new()),
            Err(TokenError::ReservedPrincipal)
        );
        // A dotted name that isn't an agent's keeps working, as a service.
        let service = TokenSpec {
            principal: "workload.prod".parse().expect("a principal"),
            ..spec(&["read:x"])
        };
        let issued = signer.issue(&service).expect("issues");
        assert!(
            verifier
                .verify(&issued, at_secs(2_000), &Revocations::new())
                .is_ok()
        );
    }

    #[test]
    fn a_revoked_token_or_key_is_refused() {
        let signer = key(7, "prod");
        let verifier = TokenVerifier::new("prod", [signer.verifying_key()]);
        let issued = signer.issue(&spec(&["read:x"])).expect("issues");
        let now = at_secs(2_000);
        let token: Revocations = [(Revoked::Token("tok-1".parse().expect("id")), Timestamp::MAX)]
            .into_iter()
            .collect();
        assert_eq!(
            verifier.verify(&issued, now, &token),
            Err(TokenError::Revoked)
        );
        let key: Revocations = [(Revoked::Key(signer.key_id().clone()), Timestamp::MAX)]
            .into_iter()
            .collect();
        assert_eq!(
            verifier.verify(&issued, now, &key),
            Err(TokenError::Revoked)
        );
        let other: Revocations = [(Revoked::Token("tok-2".parse().expect("id")), Timestamp::MAX)]
            .into_iter()
            .collect();
        assert!(verifier.verify(&issued, now, &other).is_ok());
    }

    #[test]
    fn a_rotation_trusts_both_keys() {
        let old = key(7, "prod");
        let new = key(8, "prod");
        let verifier = TokenVerifier::new("prod", [old.verifying_key(), new.verifying_key()]);
        for signer in [&old, &new] {
            let issued = signer.issue(&spec(&["read:x"])).expect("issues");
            assert!(
                verifier
                    .verify(&issued, at_secs(2_000), &Revocations::new())
                    .is_ok()
            );
        }
    }

    #[test]
    fn issuing_refuses_empty_or_oversized_specs_and_backwards_lifetimes() {
        let signer = key(7, "prod");
        assert_eq!(signer.issue(&spec(&[])), Err(IssueError::NoScopes));
        let many: Vec<String> = (0..=MAX_SCOPES).map(|i| format!("read:x-{i}")).collect();
        let many: Vec<&str> = many.iter().map(String::as_str).collect();
        let refused = signer.issue(&spec(&many));
        assert_eq!(
            refused,
            Err(IssueError::TooManyScopes { limit: MAX_SCOPES })
        );
        assert_eq!(
            refused.map(|_| ()).expect_err("refused").to_string(),
            "a token carries at most 256 scopes"
        );
        let most = signer
            .issue(&spec(&many[..MAX_SCOPES]))
            .expect("the limit issues");
        let verifier = TokenVerifier::new("prod", [signer.verifying_key()]);
        let verified = verifier
            .verify(&most, at_secs(2_000), &Revocations::new())
            .expect("verifies");
        assert_eq!(verified.spec.scopes.len(), MAX_SCOPES);
        let mut backwards = spec(&["read:x"]);
        backwards.expires_at = backwards.not_before;
        assert_eq!(signer.issue(&backwards), Err(IssueError::Lifetime));
        assert_eq!(
            TokenSigner::from_pkcs8(b"not a key", "prod").map(|_| ()),
            Err(IssueError::Key)
        );
    }

    #[test]
    fn debug_never_shows_the_token_or_the_key() {
        let signer = key(7, "prod");
        let issued = signer.issue(&spec(&["read:x"])).expect("issues");
        assert_eq!(format!("{issued:?}"), "BearerToken(..)");
        let shown = format!("{signer:?}");
        assert!(shown.contains(signer.key_id().as_str()), "{shown}");
        assert!(!shown.contains("key:"), "{shown}");
    }
}
