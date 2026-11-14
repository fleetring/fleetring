//! Bearer tokens: Ed25519-signed JWTs that name a principal and carry its scopes (ADR-0025).
//!
//! A token is a compact JWS, `<header>.<claims>.<signature>`, each part base64url without
//! padding. The header is `{"alg":"EdDSA","typ":"JWT","kid":"<key id>"}`; the claims are the
//! registered `iss` (the fleet), `sub` (the principal), `jti` (the token's ID), `iat`, `nbf`,
//! and `exp`, plus the FleetRing claim `fleetring.scopes`, the scopes as strings. Standard JWT
//! libraries can inspect and mint them.
//!
//! An agent's token also carries `fleetring.kind`, `"agent"`, and `fleetring.owner`, the
//! principal the agent answers to (ADR-0047, point 2.6). A delegated agent's token carries its
//! lineage as well: `fleetring.parent`, the principal that delegated to it, and
//! `fleetring.chain`, one `{id, kid}` per ancestor, root first (point 2.2). Its header adds
//! `x5c`, an array of exactly one certificate, the delegation issuer's, in standard base64 with
//! padding (RFC 7515, section 4.1.6), and its `kid` is that certificate's key.
//!
//! The authority signs tokens off the nodes with its Ed25519 key ([`TokenSigner`]); a control
//! node's delegation issuer signs delegated agents' tokens ([`DelegationSigner`]). Nodes verify
//! them with the authority's public key alone ([`TokenVerifier`]): no network, no control round
//! trip, and no state but the denylist the control plane publishes. Verification accepts only
//! `EdDSA` from a trusted key ID, so there is no algorithm to confuse, and refuses headers or
//! claims it doesn't know rather than ignore a constraint it can't enforce.
//!
//! A delegated token is trusted through its `x5c`: the issuer certificate must be one a trusted
//! authority signed for this fleet, valid now, and naming the token's `kid` (ADR-0047, point
//! 1.4), and the token must be inside the agent tier ([`TokenClaims::check_issuer_signed`],
//! point 3). The denylist refuses it by its own ID and key, by every ancestor's ID and key, and
//! by the authority key that signed its issuer certificate.

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use base64::Engine as _;
use base64::engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD};
use fleetring::NodeId;
use fleetring_control::delegation::{
    AgentName, ChainLink, ClaimsViolation, SignerKind, TokenClaims, TokenKind,
};
use fleetring_control::{KeyId, PrincipalName, Revocations, Revoked, Timestamp, TokenId, TokenRef};
use ring::signature::{ED25519, Ed25519KeyPair, KeyPair as _, UnparsedPublicKey};
use serde::{Deserialize, Serialize};

use crate::auth::{MAX_SCOPES, ParseScopeError, Scope, Scopes};
use crate::tls::{FleetName, IssuerCache, IssuerCertError, IssuerCertificate, Principal};

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
    #[serde(
        default,
        deserialize_with = "present",
        skip_serializing_if = "Option::is_none"
    )]
    typ: Option<String>,
    kid: String,
    /// A delegated token's issuer certificate, standard base64 DER; the authority's tokens
    /// carry none.
    #[serde(
        default,
        deserialize_with = "present",
        skip_serializing_if = "Option::is_none"
    )]
    x5c: Option<Vec<String>>,
}

/// Reads an optional header field or claim that is present: its value, never `null`, which
/// a signer never writes for an absent field and a verifier refuses rather than reading as
/// absent (token spec v1, section 6.3). An absent field is `None` through `#[serde(default)]`.
fn present<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    T::deserialize(deserializer).map(Some)
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Claims {
    iss: String,
    sub: String,
    jti: String,
    #[serde(
        default,
        deserialize_with = "present",
        skip_serializing_if = "Option::is_none"
    )]
    iat: Option<u64>,
    nbf: u64,
    exp: u64,
    #[serde(rename = "fleetring.scopes")]
    scopes: Vec<String>,
    #[serde(
        rename = "fleetring.kind",
        default,
        deserialize_with = "present",
        skip_serializing_if = "Option::is_none"
    )]
    kind: Option<String>,
    #[serde(
        rename = "fleetring.owner",
        default,
        deserialize_with = "present",
        skip_serializing_if = "Option::is_none"
    )]
    owner: Option<String>,
    #[serde(
        rename = "fleetring.parent",
        default,
        deserialize_with = "present",
        skip_serializing_if = "Option::is_none"
    )]
    parent: Option<String>,
    /// `None` when the claim is absent, so that an empty array still counts as carrying it.
    #[serde(
        rename = "fleetring.chain",
        default,
        deserialize_with = "present",
        skip_serializing_if = "Option::is_none"
    )]
    chain: Option<Vec<ChainEntry>>,
}

/// One entry of `fleetring.chain`, as the JSON holds it.
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ChainEntry {
    id: String,
    kid: String,
}

impl From<&ChainLink> for ChainEntry {
    fn from(link: &ChainLink) -> Self {
        Self {
            id: link.id.to_string(),
            kid: link.kid.to_string(),
        }
    }
}

/// The text of `fleetring.kind` for `kind`.
fn kind_text(kind: TokenKind) -> String {
    match kind {
        TokenKind::Agent => "agent".to_owned(),
    }
}

/// The kind `fleetring.kind`'s text names, or `None` for text naming no kind.
fn kind_of(text: &str) -> Option<TokenKind> {
    [TokenKind::Agent]
        .into_iter()
        .find(|&kind| kind_text(kind) == text)
}

/// What a token grants: who, what, and when, and for an agent, whom it answers to. The issuer is
/// the signer's fleet.
///
/// Build one with [`TokenSpec::new`], and mark a root agent's with [`TokenSpec::agent`].
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct TokenSpec {
    /// The principal the token names.
    pub principal: Principal,
    /// What the principal may do.
    pub scopes: Scopes,
    /// The token's ID, by which it can be revoked.
    pub id: TokenId,
    /// When the token was signed, its `iat` (ADR-0047, point 2.7).
    pub issued_at: Timestamp,
    /// The first moment the token is valid.
    pub not_before: Timestamp,
    /// The moment the token stops being valid.
    pub expires_at: Timestamp,
    /// What kind of principal the token names, `fleetring.kind`, if it says. Only an agent's
    /// token can delegate (ADR-0047, point 2.6).
    pub kind: Option<TokenKind>,
    /// The principal the agent answers to, `fleetring.owner`, if the token says.
    pub owner: Option<Principal>,
    /// A delegated agent's lineage, which only a delegation issuer signs, never the authority.
    pub lineage: Option<Lineage>,
}

impl TokenSpec {
    /// A token for `principal` granting `scopes`, with the ID `id`, valid from `not_before`
    /// until `expires_at`. It carries no kind, owner, or lineage. Its `iat` is `not_before`, as
    /// 0.7's signer wrote it, until [`signed_at`](Self::signed_at) sets the signing time.
    #[must_use]
    pub fn new(
        principal: Principal,
        scopes: Scopes,
        id: TokenId,
        not_before: Timestamp,
        expires_at: Timestamp,
    ) -> Self {
        Self {
            principal,
            scopes,
            id,
            issued_at: not_before,
            not_before,
            expires_at,
            kind: None,
            owner: None,
            lineage: None,
        }
    }

    /// Sets when the token is signed, its `iat`, apart from `not_before` (ADR-0047, point 2.7).
    /// Every signer passes its clock's reading at signing time, so a token with a later
    /// `not_before` never claims to be issued after a revocation it predates.
    #[must_use]
    pub fn signed_at(self, at: Timestamp) -> Self {
        Self {
            issued_at: at,
            ..self
        }
    }

    /// Marks the token as a root agent's that answers to `owner`: it carries
    /// `fleetring.kind = "agent"` and `fleetring.owner` (ADR-0047, point 2.6). The authority
    /// signs it only for a root agent's name, `agent-<id>`.
    #[must_use]
    pub fn agent(self, owner: Principal) -> Self {
        Self {
            kind: Some(TokenKind::Agent),
            owner: Some(owner),
            ..self
        }
    }
}

/// A delegated agent's lineage, as its token carries it (ADR-0047, point 2.2).
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct Lineage {
    /// The principal that delegated to the agent, `fleetring.parent`.
    pub parent: Principal,
    /// One link per ancestor, root first, ending with the parent, `fleetring.chain`.
    pub chain: Vec<ChainLink>,
}

impl Lineage {
    /// The lineage of an agent whose parent is `parent` and whose ancestors are `chain`.
    #[must_use]
    pub fn new(parent: Principal, chain: Vec<ChainLink>) -> Self {
        Self { parent, chain }
    }
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
    /// The authority was asked to sign a lineage, which only a delegation issuer signs
    /// (ADR-0047, point 2.6).
    #[error("the authority never signs a token with fleetring.parent or fleetring.chain")]
    Lineage,
    /// The authority was asked to sign an agent's token for a name that isn't a root agent's
    /// (ADR-0047, point 2.6).
    #[error("an agent's token from the authority must name a root agent, agent-<id>")]
    NotRootAgent,
    /// The claims aren't a delegated token's (ADR-0047, points 3.1 to 3.8 and 5.9).
    #[error(transparent)]
    Claims(ClaimsViolation),
    /// The claims name a fleet other than the delegation issuer's (ADR-0047, point 3.9).
    #[error("a delegated token must be issued for its issuer's fleet")]
    Fleet,
    /// A time in the claims isn't a whole second, which a JWT can't carry exactly.
    #[error("a delegated token's times must be whole seconds")]
    NotWholeSeconds,
    /// The token would outlive the issuer certificate it carries, and stop being accepted
    /// before it expires (ADR-0047, point 1.6).
    #[error("a delegated token can't outlive its issuer certificate")]
    OutlivesCertificate,
    /// The token would be longer than [`MAX_TOKEN_LEN`], which every node refuses.
    #[error("the token would exceed {limit} bytes; ask for fewer scopes")]
    TooLarge {
        /// The longest token accepted, [`MAX_TOKEN_LEN`].
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

    /// Signs the token `spec` describes, with its [`issued_at`](TokenSpec::issued_at) as
    /// `iat`. Times are whole seconds; milliseconds are dropped.
    ///
    /// # Errors
    ///
    /// [`IssueError::NoScopes`] for a spec without scopes, [`IssueError::TooManyScopes`] for one
    /// with more than [`MAX_SCOPES`], [`IssueError::Lifetime`] for one that expires before it
    /// starts, [`IssueError::TooLong`] for one that lasts longer than [`MAX_TOKEN_LIFETIME`],
    /// [`IssueError::Lineage`] for one with a lineage, and [`IssueError::NotRootAgent`] for an
    /// agent's that doesn't name a root agent.
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
        if spec.lineage.is_some() {
            return Err(IssueError::Lineage);
        }
        if !authority_may_sign_kind(spec.kind, &spec.principal) {
            return Err(IssueError::NotRootAgent);
        }
        let header = Header {
            alg: ALGORITHM.to_owned(),
            typ: Some(TYPE.to_owned()),
            kid: self.id.to_string(),
            x5c: None,
        };
        let claims = Claims {
            iss: self.issuer.clone(),
            sub: spec.principal.to_string(),
            jti: spec.id.to_string(),
            iat: Some(seconds(spec.issued_at)),
            nbf,
            exp,
            scopes: scope_texts(&spec.scopes),
            kind: spec.kind.map(kind_text),
            owner: spec.owner.as_ref().map(Principal::to_string),
            parent: None,
            chain: None,
        };
        Ok(BearerToken(signed_text(&header, &claims, &self.key)))
    }
}

/// Whether the authority may sign a token of `kind` naming `principal`: any principal without a
/// kind, and only a root agent's name, `agent-<id>`, as an agent (ADR-0047, point 2.6).
fn authority_may_sign_kind(kind: Option<TokenKind>, principal: &Principal) -> bool {
    match kind {
        None => true,
        Some(TokenKind::Agent) => principal
            .as_str()
            .parse::<AgentName>()
            .is_ok_and(|agent| !agent.is_delegated()),
    }
}

fn scope_texts(scopes: &Scopes) -> Vec<String> {
    scopes.as_slice().iter().map(Scope::to_string).collect()
}

/// The compact JWS of `header` and `claims`, signed with `key`.
fn signed_text(header: &Header, claims: &Claims, key: &Ed25519KeyPair) -> String {
    let mut text = encode_json(header);
    text.push('.');
    text.push_str(&encode_json(claims));
    let signature = key.sign(text.as_bytes());
    text.push('.');
    text.push_str(&URL_SAFE_NO_PAD.encode(signature.as_ref()));
    text
}

/// The error returned when a [`DelegationSigner`] can't be built from an issuer pair. The
/// messages never quote the key.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum IssuerError {
    /// The certificate fails a check of ADR-0047 point 1.4 that depends on neither a token nor
    /// the time.
    #[error(transparent)]
    Certificate(#[from] IssuerCertError),
    /// The key isn't an Ed25519 private key in PKCS#8.
    #[error("the issuer key isn't an Ed25519 PKCS#8 key")]
    Key,
    /// The key isn't the certificate's.
    #[error("the issuer key doesn't match the issuer certificate")]
    Mismatch,
}

/// A control node's delegation issuer, which signs delegated agents' tokens (ADR-0047, point 1):
/// its Ed25519 key, the issuer certificate the authority gave that key, and the fleet.
///
/// Each token it signs carries the certificate in `x5c`, so nodes need nothing new to verify it.
///
/// `Debug` shows the key ID and the issuer's name, never the key.
pub struct DelegationSigner {
    key: Ed25519KeyPair,
    certificate: IssuerCertificate,
    /// The certificate as `x5c` carries it: standard base64 with padding.
    x5c: String,
    fleet: FleetName,
}

impl fmt::Debug for DelegationSigner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DelegationSigner")
            .field("key_id", self.certificate.key_id())
            .field("issuer", self.certificate.name())
            .finish_non_exhaustive()
    }
}

impl DelegationSigner {
    /// A signer from the issuer certificate `cert_der` (DER) and its Ed25519 private key
    /// `key_pkcs8` (PKCS#8 DER), for `fleet`, whose trusted authority keys are `authorities`.
    ///
    /// # Errors
    ///
    /// [`IssuerError::Certificate`] if the certificate fails [`IssuerCertificate::check`],
    /// [`IssuerError::Key`] if the key isn't an Ed25519 PKCS#8 key, and
    /// [`IssuerError::Mismatch`] if it isn't the certificate's.
    pub fn new(
        cert_der: &[u8],
        key_pkcs8: &[u8],
        fleet: &FleetName,
        authorities: &[VerifyingKey],
    ) -> Result<Self, IssuerError> {
        let certificate = IssuerCertificate::check(cert_der, fleet, authorities)?;
        Self::from_checked(certificate, key_pkcs8, fleet.clone())
    }

    /// A signer from a certificate that already passed [`IssuerCertificate::check`] for
    /// `fleet`, and its key.
    pub(crate) fn from_checked(
        certificate: IssuerCertificate,
        key_pkcs8: &[u8],
        fleet: FleetName,
    ) -> Result<Self, IssuerError> {
        let key =
            Ed25519KeyPair::from_pkcs8_maybe_unchecked(key_pkcs8).map_err(|_| IssuerError::Key)?;
        if key.public_key().as_ref() != certificate.verifying_key().key.as_slice() {
            return Err(IssuerError::Mismatch);
        }
        let x5c = STANDARD.encode(certificate.der());
        Ok(Self {
            key,
            certificate,
            x5c,
            fleet,
        })
    }

    /// Returns the ID of the issuer's key: the `kid` of every token it signs.
    #[must_use]
    pub fn key_id(&self) -> &KeyId {
        self.certificate.key_id()
    }

    /// Returns the last moment the issuer certificate is valid. Nodes refuse its tokens after
    /// it.
    #[must_use]
    pub fn expires_at(&self) -> Timestamp {
        self.certificate.not_after()
    }

    /// Returns the issuer certificate its tokens carry.
    #[must_use]
    pub fn certificate(&self) -> &IssuerCertificate {
        &self.certificate
    }

    /// Signs exactly `claims`, a recorded delegation's, as a delegated agent's token: the header
    /// is `{alg, typ, kid, x5c}`, with this issuer's key ID and certificate, and the claims are
    /// `iss`, `sub`, `jti`, `iat`, `nbf`, `exp`, `fleetring.scopes`, `fleetring.kind`,
    /// `fleetring.owner`, `fleetring.parent`, and `fleetring.chain`, as `claims` holds them.
    ///
    /// It refuses claims that [`TokenClaims::check_recordable`] refuses, the check
    /// [`apply`](fleetring_control::apply) runs before it records a delegation, and a token that
    /// every node would refuse or stop accepting before it expires. It reads no clock: whether
    /// the certificate is valid now is the caller's to check (ADR-0047, point 1.6).
    ///
    /// # Errors
    ///
    /// [`IssueError::Claims`] for claims [`TokenClaims::check_recordable`] refuses;
    /// [`IssueError::Fleet`] for claims of another fleet; [`IssueError::NoScopes`] or
    /// [`IssueError::TooManyScopes`] for no scopes or more than [`MAX_SCOPES`];
    /// [`IssueError::NotWholeSeconds`] for a time that isn't a whole second;
    /// [`IssueError::OutlivesCertificate`] for a token that expires after the certificate; and
    /// [`IssueError::TooLarge`] for a token longer than [`MAX_TOKEN_LEN`].
    pub fn sign(&self, claims: &TokenClaims) -> Result<BearerToken, IssueError> {
        claims.check_recordable().map_err(IssueError::Claims)?;
        let Some(issued_at) = claims.issued_at else {
            return Err(IssueError::Claims(ClaimsViolation::NoIssuedAt));
        };
        if *claims.fleet != *self.fleet.as_str() {
            return Err(IssueError::Fleet);
        }
        if claims.scopes.is_empty() {
            return Err(IssueError::NoScopes);
        }
        if claims.scopes.len() > MAX_SCOPES {
            return Err(IssueError::TooManyScopes { limit: MAX_SCOPES });
        }
        let times = [issued_at, claims.not_before, claims.expires_at];
        if times.iter().any(|at| at.as_millis() % MILLIS_PER_SEC != 0) {
            return Err(IssueError::NotWholeSeconds);
        }
        if claims.expires_at > self.certificate.not_after() {
            return Err(IssueError::OutlivesCertificate);
        }
        let header = Header {
            alg: ALGORITHM.to_owned(),
            typ: Some(TYPE.to_owned()),
            kid: self.key_id().to_string(),
            x5c: Some(vec![self.x5c.clone()]),
        };
        let body = Claims {
            iss: claims.fleet.to_string(),
            sub: claims.principal.to_string(),
            jti: claims.id.to_string(),
            iat: Some(seconds(issued_at)),
            nbf: seconds(claims.not_before),
            exp: seconds(claims.expires_at),
            scopes: scope_texts(&claims.scopes),
            kind: claims.kind.map(kind_text),
            owner: claims.owner.as_ref().map(ToString::to_string),
            parent: claims.parent.as_ref().map(ToString::to_string),
            chain: Some(claims.chain.iter().map(ChainEntry::from).collect()),
        };
        let text = signed_text(&header, &body, &self.key);
        if text.len() > MAX_TOKEN_LEN {
            return Err(IssueError::TooLarge {
                limit: MAX_TOKEN_LEN,
            });
        }
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

    /// Whether `signature` is this key's Ed25519 signature of `message`: for the issuer
    /// certificates the authority signs (ADR-0047, point 1.4.2).
    pub(crate) fn verifies(&self, message: &[u8], signature: &[u8]) -> bool {
        UnparsedPublicKey::new(&ED25519, &self.key)
            .verify(message, signature)
            .is_ok()
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
    /// The token, the key that signed it, an ancestor in its chain or the key that signed that
    /// ancestor's copy, or the authority key that certified its issuer, is revoked.
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
    /// The token's `x5c` isn't exactly one certificate in standard base64 (ADR-0047, point
    /// 1.4.1).
    #[error("the token's x5c {reason}")]
    X5c {
        /// What is wrong with it.
        reason: X5cError,
    },
    /// The issuer certificate in the token's `x5c` is refused (ADR-0047, points 1.4.2 to
    /// 1.4.5).
    #[error(transparent)]
    IssuerCertificate(IssuerCertError),
    /// The token's `kid` isn't the ID of its issuer certificate's key (ADR-0047, point 1.4.6).
    #[error("the token's key id isn't its issuer certificate's key")]
    KidMismatch,
    /// An issuer-signed token's claims are outside what an issuer may sign (ADR-0047, points 3.1
    /// to 3.8), the agent tier's refusals among them.
    #[error(transparent)]
    Claims(ClaimsViolation),
    /// A token without `x5c`, which the authority signed, carries `fleetring.parent` or
    /// `fleetring.chain`, which only a delegation issuer signs (ADR-0047, point 3).
    #[error("a token without x5c can't carry fleetring.parent or fleetring.chain")]
    LineageMismatch,
    /// A token without `x5c` says it is an agent's, yet names no root agent (ADR-0047,
    /// point 2.6).
    #[error("an agent's token without x5c must name a root agent, agent-<id>")]
    NotRootAgent,
    /// The token's `iat` is more than 30 seconds after the node's time (ADR-0047, point 2.7).
    #[error("the token says it was issued in the future")]
    IssuedInTheFuture,
    /// An issuer-signed token expires after its issuer certificate, which no issuer signs
    /// (ADR-0047, point 1.6), so it would outlive the certificate on an open stream.
    #[error("the token expires after its issuer certificate")]
    OutlivesCertificate,
    /// An issuer-signed token's lineage starts from a root token whose signing key, the first
    /// chain entry's `kid`, isn't a trusted authority key, so an authority removed from the
    /// `[tls] ca` bundle ends its root tokens' descendants as well as the root tokens.
    #[error("the token's lineage doesn't start from a trusted authority key")]
    UntrustedRoot,
}

/// What is wrong with a token's `x5c` header (ADR-0047, point 1.4.1).
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum X5cError {
    /// It holds no certificate, or more than one.
    #[error("must hold exactly one certificate")]
    Count,
    /// Its certificate isn't standard base64 with padding (RFC 7515, section 4.1.6); base64url
    /// is refused.
    #[error("isn't standard base64 with padding")]
    Encoding,
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

/// What a node checks tokens against: the fleet they must be issued for and the authority keys
/// trusted to sign them, directly or through an issuer certificate.
///
/// More than one authority key may be trusted, so a rotation overlaps. A delegated token is
/// signed by a control node's issuer key, which the verifier trusts only through the issuer
/// certificate in the token's `x5c`, issued by one of the authority keys (ADR-0047, point 1.4).
/// Verifiers built from one [`Identity`](crate::tls::Identity) generation share a cache of the
/// issuer certificates they have checked.
#[derive(Debug, Clone)]
pub struct TokenVerifier {
    issuer: String,
    /// The fleet issuer certificates must name: `issuer`, if it is a fleet name.
    fleet: Option<FleetName>,
    keys: Vec<VerifyingKey>,
    certificates: Arc<IssuerCache>,
}

impl TokenVerifier {
    /// A verifier that accepts tokens issued for `issuer` and signed by any of `keys`, or by an
    /// issuer key one of `keys` certified for the fleet `issuer`.
    #[must_use]
    pub fn new(issuer: impl Into<String>, keys: impl IntoIterator<Item = VerifyingKey>) -> Self {
        let issuer = issuer.into();
        Self {
            fleet: issuer.parse().ok(),
            issuer,
            keys: keys.into_iter().collect(),
            certificates: Arc::new(IssuerCache::new()),
        }
    }

    /// A verifier for `fleet` and `keys`, as [`new`](Self::new) builds, that keeps the issuer
    /// certificates it checks in `certificates`, which other verifiers of the same identity
    /// generation share.
    pub(crate) fn with_cache(
        fleet: &FleetName,
        keys: impl IntoIterator<Item = VerifyingKey>,
        certificates: Arc<IssuerCache>,
    ) -> Self {
        Self {
            issuer: fleet.as_str().to_owned(),
            fleet: Some(fleet.clone()),
            keys: keys.into_iter().collect(),
            certificates,
        }
    }

    /// Returns the trusted authority keys.
    #[must_use]
    pub fn keys(&self) -> &[VerifyingKey] {
        &self.keys
    }

    /// Verifies `token` at `now` against the trusted keys and `revoked`, and returns what it
    /// grants with the key that signed it.
    ///
    /// The checks run in an order that does little work for an untrusted sender: the size, the
    /// header, a trusted key (for a delegated token, its issuer certificate), the signature, and
    /// only then the claims, the validity window, the agent tier, and the revocation of the
    /// token, its key, or its lineage.
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
        // 1. The size, and the three parts.
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

        // 2 to 4. The header, its algorithm and type, and its key ID.
        let header: Header = decode_json(header)?;
        if header.alg != ALGORITHM {
            return Err(TokenError::Algorithm.into());
        }
        if header.typ.as_deref().is_some_and(|typ| typ != TYPE) {
            return Err(TokenError::Malformed.into());
        }
        let key_id: KeyId = header.kid.parse().map_err(|_| TokenError::Malformed)?;

        // 5. The signing key: an authority's, or for a token with `x5c`, the key of an issuer
        // certificate an authority signed, valid now, whose key the token's `kid` names
        // (ADR-0047, points 1.4.1 to 1.4.6).
        let certificate = header
            .x5c
            .as_deref()
            .map(|x5c| self.issuer_certificate(x5c, now))
            .transpose()?;
        let key = match &certificate {
            Some(certificate) => {
                certificate
                    .matches_kid(&key_id)
                    .map_err(|_| TokenError::KidMismatch)?;
                certificate.verifying_key()
            }
            None => self
                .keys
                .iter()
                .find(|key| key.id == key_id)
                .ok_or(TokenError::UnknownKey)?,
        };

        // 6. The signature (point 1.4.7).
        let signature = URL_SAFE_NO_PAD
            .decode(signature)
            .map_err(|_| TokenError::Malformed)?;
        if !key.verifies(signed.as_bytes(), &signature) {
            return Err(TokenError::Signature.into());
        }

        // 7. The signature holds, so the claims are the signer's and so is the ID they carry.
        let claims: Claims = decode_json(claims)?;
        let id: Option<TokenId> = claims.jti.parse().ok();
        let refuse = |error| Refusal::of_token(error, id.clone());

        // 8. The signing key, and the authority key that certified an issuer's (point 1.4.8 and
        // its dated note), so revoking an authority key ends the issuers it certified.
        if revoked.contains(&Revoked::Key(key_id.clone())) {
            return Err(refuse(TokenError::Revoked));
        }
        if let Some(certificate) = &certificate
            && revoked.contains(&Revoked::Key(certificate.authority_key_id().clone()))
        {
            return Err(refuse(TokenError::Revoked));
        }

        // 9. The fleet (point 3.9).
        if claims.iss != self.issuer {
            return Err(refuse(TokenError::Issuer));
        }

        // 10. The longest any token lasts; an issuer's lasts at most 24 hours, which point 3.8
        // checks below.
        if claims.exp.saturating_sub(claims.nbf) > MAX_TOKEN_LIFETIME.as_secs() {
            return Err(refuse(TokenError::Lifetime));
        }

        // 11. The validity window, with 30 seconds of leeway on its start.
        let now_secs = seconds(now);
        let latest = now_secs.saturating_add(NOT_BEFORE_LEEWAY_SECS);
        if latest < claims.nbf {
            return Err(refuse(TokenError::NotYetValid));
        }
        if now_secs >= claims.exp {
            return Err(refuse(TokenError::Expired));
        }

        // 12. `iat`, for every token, with the leeway `nbf` has (point 2.7). An authority's token
        // may lack it, or have it after `nbf`, as tokens other JWT libraries mint do. It comes
        // after the window so a 0.7 token, whose `iat` is its `nbf`, presented early is still
        // refused as not yet valid.
        if claims.iat.is_some_and(|iat| iat > latest) {
            return Err(refuse(TokenError::IssuedInTheFuture));
        }

        // 13. The subject, and the rest of the claims.
        if id.is_none() {
            return Err(TokenError::Malformed.into());
        }
        let principal: Principal = claims
            .sub
            .parse()
            .map_err(|_| refuse(TokenError::Malformed))?;
        let lineage = Presence::of(claims.parent.is_some() || claims.chain.is_some());
        let token_claims = token_claims(claims).map_err(refuse)?;

        // 14. What the signer may sign: the agent tier for an issuer (point 3.1 to 3.8), and for
        // the authority, no delegated agent, no lineage, and an agent's kind only on a root
        // agent's name (point 3's converse and point 2.6).
        let signer = match &certificate {
            Some(certificate) => {
                token_claims
                    .check_issuer_signed()
                    .map_err(|violation| refuse(TokenError::Claims(violation)))?;
                // No issuer signs a token that outlives its certificate (point 1.6), and the
                // stream recheck ends a token at its `exp`, so none outlives it on a stream.
                if token_claims.expires_at > certificate.not_after() {
                    return Err(refuse(TokenError::OutlivesCertificate));
                }
                // The root of its lineage was signed by an authority this node still trusts.
                // `check_issuer_signed` holds, so the chain has an entry.
                let rooted = token_claims
                    .chain
                    .first()
                    .is_some_and(|root| self.keys.iter().any(|key| key.id == root.kid));
                if !rooted {
                    return Err(refuse(TokenError::UntrustedRoot));
                }
                Signer::Issuer {
                    name: certificate.name().clone(),
                    authority: certificate.authority_key_id().clone(),
                }
            }
            None => {
                if principal.is_reserved_agent_name() {
                    return Err(refuse(TokenError::ReservedPrincipal));
                }
                if lineage == Presence::Present {
                    return Err(refuse(TokenError::LineageMismatch));
                }
                if !authority_may_sign_kind(token_claims.kind, &principal) {
                    return Err(refuse(TokenError::NotRootAgent));
                }
                Signer::Authority
            }
        };

        // 15. The denylist: the token's own ID and key, and every ancestor's (point 3), by the
        // rule the stream recheck applies too.
        if signer.refusal_in(revoked, &token_claims, &key_id).is_some() {
            return Err(refuse(TokenError::Revoked));
        }

        Ok(Verified {
            principal,
            claims: token_claims,
            key: key_id,
            signer,
        })
    }

    /// The issuer certificate a token's `x5c` holds, checked for this fleet against the trusted
    /// authority keys, through the cache, and valid at `now` (ADR-0047, points 1.4.1 to 1.4.5).
    fn issuer_certificate(
        &self,
        x5c: &[String],
        now: Timestamp,
    ) -> Result<Arc<IssuerCertificate>, TokenError> {
        let [encoded] = x5c else {
            return Err(TokenError::X5c {
                reason: X5cError::Count,
            });
        };
        let der = STANDARD.decode(encoded).map_err(|_| TokenError::X5c {
            reason: X5cError::Encoding,
        })?;
        // A verifier whose issuer isn't a fleet name has no fleet an issuer could name.
        let fleet = self
            .fleet
            .as_ref()
            .ok_or(TokenError::IssuerCertificate(IssuerCertError::Subject))?;
        let certificate = self
            .certificates
            .check(&der, fleet, &self.keys, now)
            .map_err(TokenError::IssuerCertificate)?;
        certificate
            .valid_at(now)
            .map_err(TokenError::IssuerCertificate)?;
        Ok(certificate)
    }
}

/// Whether a token carries something.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Presence {
    Absent,
    Present,
}

impl Presence {
    fn of(present: bool) -> Self {
        if present { Self::Present } else { Self::Absent }
    }
}

/// The claims a verified token's JSON holds, as [`TokenClaims`]: every field read by its own
/// grammar, and nothing yet checked of how they fit together.
fn token_claims(claims: Claims) -> Result<TokenClaims, TokenError> {
    let principal = |text: &str| {
        text.parse::<PrincipalName>()
            .map_err(|_| TokenError::Malformed)
    };
    let kind = claims
        .kind
        .as_deref()
        .map(|text| kind_of(text).ok_or(TokenError::Malformed))
        .transpose()?;
    let chain = claims
        .chain
        .unwrap_or_default()
        .into_iter()
        .map(|entry| {
            Ok(ChainLink {
                id: entry.id.parse().map_err(|_| TokenError::Malformed)?,
                kid: entry.kid.parse().map_err(|_| TokenError::Malformed)?,
            })
        })
        .collect::<Result<Vec<_>, TokenError>>()?;
    let scopes = claims
        .scopes
        .iter()
        .map(|scope| scope.parse())
        .collect::<Result<Scopes, _>>()
        .map_err(TokenError::Scope)?;
    Ok(TokenClaims {
        principal: principal(&claims.sub)?,
        id: claims.jti.parse().map_err(|_| TokenError::Malformed)?,
        issued_at: claims.iat.map(from_seconds),
        not_before: from_seconds(claims.nbf),
        expires_at: from_seconds(claims.exp),
        scopes,
        kind,
        owner: claims.owner.as_deref().map(principal).transpose()?,
        parent: claims.parent.as_deref().map(principal).transpose()?,
        chain,
        fleet: claims.iss.into_boxed_str(),
    })
}

/// Who signed a verified token.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Signer {
    /// A trusted authority key: the token carried no `x5c`.
    Authority,
    /// A control node's delegation issuer key, certified by the issuer certificate in the
    /// token's `x5c` (ADR-0047, point 1).
    Issuer {
        /// The issuer's name, from its certificate's `fleetring://<fleet>/issuer/<name>`.
        name: NodeId,
        /// The ID of the trusted authority key that signed the issuer certificate, whose
        /// revocation ends the token too.
        authority: KeyId,
    },
}

impl Signer {
    /// Which kind of signer this is, which decides how a principal revocation judges the
    /// token's `iat` (ADR-0047, point 3).
    #[must_use]
    pub fn kind(&self) -> SignerKind {
        match self {
            Self::Authority => SignerKind::Authority,
            Self::Issuer { .. } => SignerKind::Issuer,
        }
    }

    /// The denylist entry in `revoked` that refuses a token this signer signed with `key`,
    /// carrying `claims`, or `None` if none does: its own ID and key, for an issuer's token the
    /// authority key that certified the issuer (point 1.4.8's dated note), every ancestor's ID
    /// and key in its chain, and a principal entry for its subject or a name it is below, judged
    /// by its `iat` with this signer's leeway: 30 seconds for the authority's tokens, none for an
    /// issuer's, and a token without `iat` refused ([`Revocations::refuses_token`], point 3).
    ///
    /// The verifier and the stream recheck both decide with it, so a stream ends on exactly
    /// the entries that would refuse its token anew.
    pub(crate) fn refusal_in(
        &self,
        revoked: &Revocations,
        claims: &TokenClaims,
        key: &KeyId,
    ) -> Option<Revoked> {
        let certified_by = match self {
            Self::Authority => None,
            Self::Issuer { authority, .. } => Some(Revoked::Key(authority.clone())),
        };
        certified_by
            .filter(|entry| revoked.contains(entry))
            .or_else(|| revoked.refuses_token(&claims.token_ref(key, self.kind())))
    }
}

/// A token that verified: who it names, its claims, and who signed it with which key.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct Verified {
    /// The principal the token names, its `sub`.
    pub principal: Principal,
    /// Every claim the token carries, as it carries them: `iat` is absent when the token says
    /// none.
    pub claims: TokenClaims,
    /// The key that signed it, its `kid`, by which every token it signed can be revoked.
    pub key: KeyId,
    /// Who signed it.
    pub signer: Signer,
}

impl Verified {
    /// What [`Revocations::refuses_token`] needs of the token, to check it again later.
    #[must_use]
    pub fn token_ref(&self) -> TokenRef<'_> {
        self.claims.token_ref(&self.key, self.signer.kind())
    }
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

    use fleetring_control::delegation::DelegationId;
    use fleetring_control::{Millis, Revoked};

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
        TokenSpec::new(
            "app".parse().expect("a principal"),
            scopes
                .iter()
                .map(|scope| scope.parse().expect("a scope"))
                .collect(),
            "tok-1".parse().expect("a token id"),
            Timestamp::from_millis(1_000_000),
            Timestamp::from_millis(4_600_000),
        )
    }

    fn at_secs(secs: u64) -> Timestamp {
        Timestamp::from_millis(secs * 1_000)
    }

    /// The claims the verifier reports for a token the authority of `fleet` signed from `spec`.
    fn claims_of(spec: &TokenSpec, fleet: &str) -> TokenClaims {
        let name = |principal: &Principal| principal.as_str().parse().expect("a principal name");
        TokenClaims {
            fleet: fleet.into(),
            principal: name(&spec.principal),
            id: spec.id.clone(),
            issued_at: Some(spec.issued_at),
            not_before: spec.not_before,
            expires_at: spec.expires_at,
            scopes: spec.scopes.clone(),
            kind: spec.kind,
            owner: spec.owner.as_ref().map(name),
            parent: None,
            chain: Vec::new(),
        }
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
        assert_eq!(
            got.claims,
            claims_of(&spec(&["read:tenant:acme/*"]), "prod")
        );
        assert_eq!(
            (got.principal.as_str(), &got.signer),
            ("app", &Signer::Authority)
        );
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
            x5c: None,
        };
        let claims = Claims {
            iss: "prod".to_owned(),
            sub: "app".to_owned(),
            jti: "long".to_owned(),
            iat: None,
            nbf: 1_000,
            exp: 1_000 + max + 1,
            scopes: vec!["read:x".to_owned()],
            kind: None,
            owner: None,
            parent: None,
            chain: None,
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
        let scopes = &verified.claims.scopes;
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
        assert!(verified.claims.scopes.allows(Target::Fleet));

        // A token minted by 0.2 carries the scope's old spelling, which 0.3 doesn't accept.
        let header = Header {
            alg: ALGORITHM.to_owned(),
            typ: Some(TYPE.to_owned()),
            kid: signer.key_id().to_string(),
            x5c: None,
        };
        let claims = Claims {
            iss: "prod".to_owned(),
            sub: "ops".to_owned(),
            jti: "old".to_owned(),
            iat: None,
            nbf: 1_000,
            exp: 4_600,
            scopes: vec!["cluster".to_owned()],
            kind: None,
            owner: None,
            parent: None,
            chain: None,
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
        assert_eq!(verified.claims.scopes.len(), MAX_SCOPES);
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

    /// A token from 0.7's signer for [`spec`] with these scopes, signed by `key(7, "prod")`:
    /// `TokenSpec::new` without [`TokenSpec::signed_at`] keeps writing it byte for byte
    /// (`iat = nbf`).
    const TOKEN_0_7: &str = "eyJhbGciOiJFZERTQSIsInR5cCI6IkpXVCIsImtpZCI6Il9vRXNFdk9yVE9hc1hiYWEifQ.\
        eyJpc3MiOiJwcm9kIiwic3ViIjoiYXBwIiwianRpIjoidG9rLTEiLCJpYXQiOjEwMDAsIm5iZiI6MTAwMCwiZXhw\
        Ijo0NjAwLCJmbGVldHJpbmcuc2NvcGVzIjpbInJlYWQ6dGVuYW50OmFjbWUvKiIsImZvcms6c3JjLSoiXX0.\
        vUJF7Ojd-46Udz4N5j7y2JoHhdKlENtaCsCV2WaqA9K9X_pVBB7tZ3zzIPhEgt0nusVAI9fWWa0RzNXkQdgfDw";

    #[test]
    fn token_iat_is_signing_time_apart_from_nbf() {
        let signer = key(7, "prod");
        let verifier = TokenVerifier::new("prod", [signer.verifying_key()]);
        let backdated = spec(&["read:x"]).signed_at(Timestamp::from_millis(1_200_999));
        let issued = signer.issue(&backdated).expect("issues");
        let claims: Claims =
            decode_json(issued.expose().split('.').nth(1).expect("claims")).expect("decodes");
        assert_eq!((claims.iat, claims.nbf), (Some(1_200), 1_000));
        let verified = verifier
            .verify(&issued, at_secs(2_000), &Revocations::new())
            .expect("verifies");
        assert_eq!(verified.claims.issued_at, Some(at_secs(1_200)));
        assert_eq!(verified.claims.not_before, at_secs(1_000));
    }

    #[test]
    fn future_nbf_token_claims_signing_time_iat() {
        // Signed at 1,000 s for use from 3,000 s: its `iat` says when it was signed, so a
        // principal revocation at 2,000 s refuses it, which `iat = nbf` would have let through.
        let signer = key(7, "prod");
        let verifier = TokenVerifier::new("prod", [signer.verifying_key()]);
        let later = TokenSpec::new(
            "agent-42".parse().expect("a principal"),
            ["read:agent-42-*".parse().expect("a scope")]
                .into_iter()
                .collect(),
            "tok-2".parse().expect("a token id"),
            at_secs(3_000),
            at_secs(6_000),
        )
        .signed_at(at_secs(1_000));
        let issued = signer.issue(&later).expect("issues");
        let claims: Claims =
            decode_json(issued.expose().split('.').nth(1).expect("claims")).expect("decodes");
        assert_eq!((claims.iat, claims.nbf), (Some(1_000), 3_000));
        let revoked: Revocations = [(
            Revoked::principal("agent-42".parse().expect("a principal")),
            fleetring_control::Entry::fleet_principal(Timestamp::MAX, at_secs(2_000)),
        )]
        .into_iter()
        .collect();
        assert!(
            verifier
                .verify(&issued, at_secs(3_500), &Revocations::new())
                .is_ok()
        );
        assert_eq!(
            verifier.verify(&issued, at_secs(3_500), &revoked),
            Err(TokenError::Revoked)
        );
        // The same token as 0.7 signed it, `iat = nbf`, claimed to be issued after.
        let as_0_7 = TokenSpec {
            issued_at: later.not_before,
            ..later
        };
        let issued = signer.issue(&as_0_7).expect("issues");
        assert!(verifier.verify(&issued, at_secs(3_500), &revoked).is_ok());
    }

    #[test]
    fn token_spec_new_keeps_0_7_bytes() {
        let spec = spec(&["read:tenant:acme/*", "fork:src-*"]);
        assert_eq!(spec.issued_at, spec.not_before);
        let issued = key(7, "prod").issue(&spec).expect("issues");
        assert_eq!(issued.expose(), TOKEN_0_7);
    }

    #[test]
    fn tokens_0_7_with_iat_equal_nbf_still_verify() {
        let signer = key(7, "prod");
        let verifier = TokenVerifier::new("prod", [signer.verifying_key()]);
        let token = BearerToken::new(TOKEN_0_7);
        let none = Revocations::new();
        // Valid from 30 s before its `nbf`, the leeway its `iat` shares, to its `exp`.
        for now in [
            at_secs(1_000 - NOT_BEFORE_LEEWAY_SECS),
            at_secs(1_000),
            at_secs(4_599),
        ] {
            let verified = verifier.verify(&token, now, &none).expect("verifies");
            assert_eq!(verified.claims.issued_at, Some(at_secs(1_000)));
            assert_eq!(verified.signer, Signer::Authority);
        }
    }

    #[test]
    fn an_agent_token_carries_kind_and_owner_and_names_a_root_agent() {
        let signer = key(7, "prod");
        let verifier = TokenVerifier::new("prod", [signer.verifying_key()]);
        let owner: Principal = "alice".parse().expect("a principal");
        let mut root = spec(&["admin:agent-42-*", "grant:agent-42.*"]).agent(owner.clone());
        root.principal = "agent-42".parse().expect("a principal");
        assert_eq!(
            (root.kind, root.owner.as_ref()),
            (Some(TokenKind::Agent), Some(&owner))
        );
        let issued = signer.issue(&root).expect("issues");
        let claims: Claims =
            decode_json(issued.expose().split('.').nth(1).expect("claims")).expect("decodes");
        assert_eq!(claims.kind.as_deref(), Some("agent"));
        assert_eq!(claims.owner.as_deref(), Some("alice"));
        assert!(claims.parent.is_none() && claims.chain.is_none());
        let verified = verifier
            .verify(&issued, at_secs(2_000), &Revocations::new())
            .expect("verifies");
        assert_eq!(verified.claims, claims_of(&root, "prod"));

        for name in ["agent-42.c7", "workload", "workload.prod"] {
            let mut other = root.clone();
            other.principal = name.parse().expect("a principal");
            assert_eq!(
                signer.issue(&other),
                Err(IssueError::NotRootAgent),
                "{name}"
            );
        }
        let mut lineage = spec(&["read:x"]);
        lineage.lineage = Some(Lineage::new(
            "agent-42".parse().expect("a principal"),
            Vec::new(),
        ));
        assert_eq!(signer.issue(&lineage), Err(IssueError::Lineage));
    }

    /// Signs `claims` with `signer`'s key as the authority would never sign them.
    fn forge(signer: &TokenSigner, header: &Header, claims: &Claims) -> BearerToken {
        BearerToken::new(signed_text(header, claims, &signer.key))
    }

    fn authority_header(signer: &TokenSigner) -> Header {
        Header {
            alg: ALGORITHM.to_owned(),
            typ: Some(TYPE.to_owned()),
            kid: signer.key_id().to_string(),
            x5c: None,
        }
    }

    fn authority_claims(sub: &str) -> Claims {
        Claims {
            iss: "prod".to_owned(),
            sub: sub.to_owned(),
            jti: "tok-1".to_owned(),
            iat: Some(1_000),
            nbf: 1_000,
            exp: 4_600,
            scopes: vec!["read:x".to_owned()],
            kind: None,
            owner: None,
            parent: None,
            chain: None,
        }
    }

    #[test]
    fn the_authority_path_refuses_x5c_a_lineage_and_an_agent_kind_off_a_root_agent() {
        let signer = key(7, "prod");
        let verifier = TokenVerifier::new("prod", [signer.verifying_key()]);
        let none = Revocations::new();
        let now = at_secs(2_000);
        let header = authority_header(&signer);
        assert!(
            verifier
                .verify(
                    &forge(&signer, &header, &authority_claims("app")),
                    now,
                    &none
                )
                .is_ok()
        );

        // An `x5c` in the header makes the token an issuer's, whose certificate must check,
        // whatever key ID it names: three zero bytes aren't a certificate.
        let with_x5c = Header {
            x5c: Some(vec!["AAAA".to_owned()]),
            ..authority_header(&signer)
        };
        assert_eq!(
            verifier.verify(
                &forge(&signer, &with_x5c, &authority_claims("app")),
                now,
                &none
            ),
            Err(TokenError::IssuerCertificate(IssuerCertError::Parse))
        );

        // The converse of ADR-0047 point 3, and point 2.6.
        let entry = || ChainEntry {
            id: "tok-0".to_owned(),
            kid: signer.key_id().to_string(),
        };
        let refused = [
            (
                authority_claims("agent-42.c7"),
                TokenError::ReservedPrincipal,
            ),
            (
                Claims {
                    parent: Some("agent-42".to_owned()),
                    ..authority_claims("agent-42")
                },
                TokenError::LineageMismatch,
            ),
            (
                Claims {
                    chain: Some(vec![entry()]),
                    ..authority_claims("agent-42")
                },
                TokenError::LineageMismatch,
            ),
            (
                Claims {
                    chain: Some(Vec::new()),
                    ..authority_claims("workload")
                },
                TokenError::LineageMismatch,
            ),
            (
                Claims {
                    kind: Some("agent".to_owned()),
                    ..authority_claims("workload")
                },
                TokenError::NotRootAgent,
            ),
            (
                Claims {
                    kind: Some("agent".to_owned()),
                    ..authority_claims("workload.prod")
                },
                TokenError::NotRootAgent,
            ),
            (
                Claims {
                    kind: Some("robot".to_owned()),
                    ..authority_claims("agent-42")
                },
                TokenError::Malformed,
            ),
            (
                Claims {
                    owner: Some("Not A Principal".to_owned()),
                    ..authority_claims("agent-42")
                },
                TokenError::Malformed,
            ),
        ];
        for (claims, want) in refused {
            let shown = format!("{claims:?}");
            assert_eq!(
                verifier.verify(&forge(&signer, &header, &claims), now, &none),
                Err(want),
                "{shown}"
            );
        }
        // A service's dotted name, and a root agent's token with its kind, still verify.
        for claims in [
            authority_claims("workload.prod"),
            Claims {
                kind: Some("agent".to_owned()),
                owner: Some("alice".to_owned()),
                ..authority_claims("agent-42")
            },
        ] {
            let shown = format!("{claims:?}");
            let verified = verifier
                .verify(&forge(&signer, &header, &claims), now, &none)
                .expect(&shown);
            assert_eq!(verified.signer, Signer::Authority, "{shown}");
        }
    }

    #[test]
    fn any_token_with_iat_over_30s_ahead_is_refused() {
        let signer = key(7, "prod");
        let verifier = TokenVerifier::new("prod", [signer.verifying_key()]);
        let none = Revocations::new();
        let header = authority_header(&signer);
        let now = at_secs(2_000);
        let at = |iat| {
            forge(
                &signer,
                &header,
                &Claims {
                    iat: Some(iat),
                    ..authority_claims("app")
                },
            )
        };
        assert!(verifier.verify(&at(2_030), now, &none).is_ok());
        assert_eq!(
            verifier.verify(&at(2_031), now, &none),
            Err(TokenError::IssuedInTheFuture)
        );
        assert_eq!(
            verifier.verify(&at(u64::MAX), now, &none),
            Err(TokenError::IssuedInTheFuture)
        );
    }

    #[test]
    fn authority_token_with_backdated_nbf_and_later_iat_still_verifies() {
        let signer = key(7, "prod");
        let verifier = TokenVerifier::new("prod", [signer.verifying_key()]);
        // Another JWT library signs at 1,500 a token valid from 1,000.
        let claims = Claims {
            iat: Some(1_500),
            ..authority_claims("app")
        };
        let verified = verifier
            .verify(
                &forge(&signer, &authority_header(&signer), &claims),
                at_secs(2_000),
                &Revocations::new(),
            )
            .expect("verifies");
        assert_eq!(verified.claims.issued_at, Some(at_secs(1_500)));
        assert_eq!(verified.claims.not_before, at_secs(1_000));
    }

    #[test]
    fn authority_token_without_iat_still_verifies() {
        let signer = key(7, "prod");
        let verifier = TokenVerifier::new("prod", [signer.verifying_key()]);
        let claims = Claims {
            iat: None,
            ..authority_claims("app")
        };
        let verified = verifier
            .verify(
                &forge(&signer, &authority_header(&signer), &claims),
                at_secs(2_000),
                &Revocations::new(),
            )
            .expect("verifies");
        assert_eq!(
            verified.claims.issued_at, None,
            "no iat is reported as none"
        );
    }

    /// A token that doesn't say when it was issued counts as issued before every principal
    /// revocation, so any principal entry covering its subject refuses it (ADR-0047, point 3).
    #[test]
    fn principal_entry_refuses_a_token_without_iat() {
        let signer = key(7, "prod");
        let verifier = TokenVerifier::new("prod", [signer.verifying_key()]);
        let token = forge(
            &signer,
            &authority_header(&signer),
            &Claims {
                iat: None,
                ..authority_claims("app")
            },
        );
        let principal = |name: &str| -> Revocations {
            [(
                Revoked::principal(name.parse().expect("a principal")),
                fleetring_control::Entry::fleet_principal(Timestamp::MAX, Timestamp::ZERO),
            )]
            .into_iter()
            .collect()
        };
        assert_eq!(
            verifier.verify(&token, at_secs(2_000), &principal("app")),
            Err(TokenError::Revoked)
        );
        assert!(
            verifier
                .verify(&token, at_secs(2_000), &principal("other"))
                .is_ok()
        );
    }

    /// The time the issuer certificates here start at.
    const ISSUED: Timestamp = Timestamp::from_millis(1_800_000_000_000);

    const DAY: Duration = Duration::from_secs(24 * 60 * 60);

    fn after(at: Timestamp, span: Duration) -> Timestamp {
        Timestamp::from_millis(at.as_millis() + u64::try_from(span.as_millis()).expect("fits"))
    }

    /// A delegation issuer of `fleet`, named `name`, certified from [`ISSUED`] for 30 days by a
    /// new authority: its certificate and key, and the authority's keys.
    struct Issuer {
        fleet: FleetName,
        cert: Vec<u8>,
        key: Vec<u8>,
        authorities: Vec<VerifyingKey>,
        authority: Arc<crate::tls::Authority>,
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

        let PrivateKeyDer::Pkcs8(key) =
            PrivateKeyDer::from_pem_slice(pem.as_bytes()).expect("a key")
        else {
            panic!("an issuer key is PKCS#8");
        };
        key.secret_pkcs8_der().to_vec()
    }

    impl Issuer {
        fn new(fleet: &str, name: &str) -> Self {
            use crate::tls::{Authority, Validity};

            let fleet: FleetName = fleet.parse().expect("a fleet");
            let authority =
                Authority::create(fleet.clone(), Validity::new(Timestamp::ZERO, 36_500 * DAY))
                    .expect("an authority");
            let issued = authority
                .issue_issuer(
                    &name.parse().expect("a node id"),
                    Validity::new(ISSUED, 30 * DAY),
                )
                .expect("an issuer pair");
            let authorities = vec![
                authority
                    .token_signer()
                    .expect("an Ed25519 authority")
                    .verifying_key(),
            ];
            Self {
                fleet,
                cert: der_of(issued.certificate_pem()),
                key: pkcs8_of(issued.key_pem()),
                authorities,
                authority: Arc::new(authority),
            }
        }

        /// Another issuer certified by the same authority, named `name`.
        fn sibling(&self, name: &str) -> Self {
            use crate::tls::Validity;

            let issued = self
                .authority
                .issue_issuer(
                    &name.parse().expect("a node id"),
                    Validity::new(ISSUED, 30 * DAY),
                )
                .expect("an issuer pair");
            Self {
                fleet: self.fleet.clone(),
                cert: der_of(issued.certificate_pem()),
                key: pkcs8_of(issued.key_pem()),
                authorities: self.authorities.clone(),
                authority: Arc::clone(&self.authority),
            }
        }

        fn signer(&self) -> DelegationSigner {
            DelegationSigner::new(&self.cert, &self.key, &self.fleet, &self.authorities)
                .expect("a delegation signer")
        }

        /// A verifier of this issuer's fleet that trusts its authority.
        fn verifier(&self) -> TokenVerifier {
            TokenVerifier::new(self.fleet.as_str(), self.authorities.clone())
        }

        fn key_pair(&self) -> Ed25519KeyPair {
            Ed25519KeyPair::from_pkcs8_maybe_unchecked(&self.key).expect("an Ed25519 key")
        }

        /// The header the issuer's tokens carry.
        fn header(&self) -> Header {
            Header {
                alg: ALGORITHM.to_owned(),
                typ: Some(TYPE.to_owned()),
                kid: self.signer().key_id().to_string(),
                x5c: Some(vec![STANDARD.encode(&self.cert)]),
            }
        }

        /// Signs `claims` under `header` with the issuer's key, whatever they hold, as a stolen
        /// issuer key could.
        fn forge(&self, header: &Header, claims: &Claims) -> BearerToken {
            BearerToken::new(signed_text(header, claims, &self.key_pair()))
        }

        /// Signs claims given as JSON, for claims the [`Claims`] type can't hold.
        fn forge_json(&self, header: &Header, claims: &serde_json::Value) -> BearerToken {
            let mut text = encode_json(header);
            text.push('.');
            text.push_str(&encode_json(claims));
            let signature = self.key_pair().sign(text.as_bytes());
            text.push('.');
            text.push_str(&URL_SAFE_NO_PAD.encode(signature.as_ref()));
            BearerToken::new(text)
        }
    }

    /// `claims` as a token's JSON carries them, as [`DelegationSigner::sign`] writes them.
    fn wire(claims: &TokenClaims) -> Claims {
        Claims {
            iss: claims.fleet.to_string(),
            sub: claims.principal.to_string(),
            jti: claims.id.to_string(),
            iat: claims.issued_at.map(seconds),
            nbf: seconds(claims.not_before),
            exp: seconds(claims.expires_at),
            scopes: scope_texts(&claims.scopes),
            kind: claims.kind.map(kind_text),
            owner: claims.owner.as_ref().map(ToString::to_string),
            parent: claims.parent.as_ref().map(ToString::to_string),
            chain: Some(claims.chain.iter().map(ChainEntry::from).collect()),
        }
    }

    fn scopes_of(scopes: &[&str]) -> Scopes {
        scopes
            .iter()
            .map(|scope| scope.parse().expect("a scope"))
            .collect()
    }

    fn kid(text: &str) -> KeyId {
        text.parse().expect("a key id")
    }

    /// `agent-42.c7`'s claims for `issuer`'s fleet, recorded at [`ISSUED`] plus a day, a child
    /// of root `agent-42`, whose token `issuer`'s authority signed.
    fn child_claims(issuer: &Issuer) -> TokenClaims {
        let recorded = after(ISSUED, DAY);
        TokenClaims {
            fleet: issuer.fleet.as_str().into(),
            principal: "agent-42.c7".parse().expect("a principal"),
            id: DelegationId::from_bytes([7; 16]).into(),
            issued_at: Some(recorded),
            not_before: after(recorded, Duration::from_secs(60)),
            expires_at: after(recorded, Duration::from_secs(2 * 60 * 60)),
            scopes: scopes_of(&["admin:agent-42.c7-*", "stream:agent-42.c7-*"]),
            kind: Some(TokenKind::Agent),
            owner: Some("alice".parse().expect("a principal")),
            parent: Some("agent-42".parse().expect("a principal")),
            chain: vec![ChainLink {
                id: "root-1".parse().expect("a token id"),
                kid: issuer.authorities[0].id().clone(),
            }],
        }
    }

    #[test]
    fn a_delegated_token_carries_its_certificate_in_x5c_and_exactly_its_claims() {
        let issuer = Issuer::new("lab", "control-1");
        let signer = issuer.signer();
        let claims = child_claims(&issuer);
        let token = signer.sign(&claims).expect("signs");
        let parts: Vec<&str> = token.expose().split('.').collect();
        let [header, body, signature] = parts.as_slice() else {
            panic!("a compact JWS has three parts");
        };

        let header: Header = decode_json(header).expect("a header");
        assert_eq!(header.alg, "EdDSA");
        assert_eq!(header.typ.as_deref(), Some("JWT"));
        assert_eq!(header.kid, signer.key_id().as_str());
        let x5c = header.x5c.expect("x5c");
        assert_eq!(x5c, [STANDARD.encode(&issuer.cert)], "one certificate");
        assert_eq!(
            STANDARD.decode(&x5c[0]).expect("standard base64"),
            issuer.cert
        );

        let body: Claims = decode_json(body).expect("claims");
        let recorded = after(ISSUED, DAY).as_millis() / 1_000;
        assert_eq!(body.iss, "lab");
        assert_eq!(body.sub, "agent-42.c7");
        assert_eq!(body.jti, DelegationId::from_bytes([7; 16]).as_str());
        assert_eq!(body.iat, Some(recorded), "iat is the recorded time");
        assert_eq!(body.nbf, recorded + 60, "apart from nbf");
        assert_eq!(body.exp, recorded + 2 * 60 * 60);
        assert_eq!(body.scopes, ["admin:agent-42.c7-*", "stream:agent-42.c7-*"]);
        assert_eq!(body.kind.as_deref(), Some("agent"));
        assert_eq!(body.owner.as_deref(), Some("alice"));
        assert_eq!(body.parent.as_deref(), Some("agent-42"));
        let chain = body.chain.expect("a chain");
        assert_eq!(
            chain
                .iter()
                .map(|entry| (entry.id.as_str(), entry.kid.as_str()))
                .collect::<Vec<_>>(),
            [("root-1", issuer.authorities[0].id().as_str())]
        );

        let signed = &token.expose()[..token.expose().len() - signature.len() - 1];
        let signature = URL_SAFE_NO_PAD.decode(signature).expect("base64url");
        assert!(
            signer
                .certificate()
                .verifying_key()
                .verifies(signed.as_bytes(), &signature),
            "the issuer key signed it"
        );
        assert_eq!(signer.expires_at(), after(ISSUED, 30 * DAY));
    }

    #[test]
    fn sign_refuses_what_check_recordable_refuses() {
        let issuer = Issuer::new("lab", "control-1");
        let signer = issuer.signer();
        let base = child_claims(&issuer);
        let late = after(
            base.issued_at.expect("iat"),
            Duration::from_secs(25 * 60 * 60),
        );
        let cases = [
            TokenClaims {
                principal: "agent-42".parse().expect("a principal"),
                parent: None,
                chain: Vec::new(),
                ..base.clone()
            },
            TokenClaims {
                kind: None,
                ..base.clone()
            },
            TokenClaims {
                parent: Some("agent-43".parse().expect("a principal")),
                ..base.clone()
            },
            TokenClaims {
                chain: Vec::new(),
                ..base.clone()
            },
            TokenClaims {
                scopes: scopes_of(&["fleet"]),
                ..base.clone()
            },
            TokenClaims {
                scopes: scopes_of(&["admin:agent-43-*"]),
                ..base.clone()
            },
            TokenClaims {
                scopes: scopes_of(&["admin:agent-42.c7x*"]),
                ..base.clone()
            },
            TokenClaims {
                issued_at: None,
                ..base.clone()
            },
            TokenClaims {
                issued_at: Some(after(base.not_before, Duration::from_secs(1))),
                ..base.clone()
            },
            TokenClaims {
                expires_at: base.not_before,
                ..base.clone()
            },
            TokenClaims {
                expires_at: after(base.not_before, Duration::from_secs(24 * 60 * 60 + 1)),
                ..base.clone()
            },
            TokenClaims {
                not_before: late,
                expires_at: after(late, Duration::from_secs(60)),
                ..base.clone()
            },
        ];
        for claims in cases {
            let violation = claims
                .check_recordable()
                .expect_err("check_recordable refuses the case");
            assert_eq!(
                signer.sign(&claims),
                Err(IssueError::Claims(violation)),
                "{claims:?}"
            );
        }
        assert!(base.check_recordable().is_ok());
    }

    #[test]
    fn sign_refuses_another_fleet_no_scopes_split_seconds_and_outliving_its_certificate() {
        let issuer = Issuer::new("lab", "control-1");
        let signer = issuer.signer();
        let base = child_claims(&issuer);
        let prod = TokenClaims {
            fleet: "prod".into(),
            ..base.clone()
        };
        assert_eq!(signer.sign(&prod), Err(IssueError::Fleet));
        assert_eq!(
            signer.sign(&TokenClaims {
                scopes: Scopes::none(),
                ..base.clone()
            }),
            Err(IssueError::NoScopes)
        );
        for claims in [
            TokenClaims {
                issued_at: base.issued_at.map(|at| at.saturating_add(Millis::new(1))),
                ..base.clone()
            },
            TokenClaims {
                not_before: base.not_before.saturating_add(Millis::new(1)),
                ..base.clone()
            },
            TokenClaims {
                expires_at: base.expires_at.saturating_add(Millis::new(1)),
                ..base.clone()
            },
        ] {
            assert_eq!(
                signer.sign(&claims),
                Err(IssueError::NotWholeSeconds),
                "{claims:?}"
            );
        }

        // The certificate lasts 30 days from `ISSUED`; its last second is the latest `exp`.
        let end = signer.expires_at();
        let start = Timestamp::from_millis(end.as_millis() - 3_600_000);
        let last = TokenClaims {
            issued_at: Some(start),
            not_before: start,
            expires_at: end,
            ..base.clone()
        };
        assert!(signer.sign(&last).is_ok());
        let beyond = TokenClaims {
            expires_at: after(end, Duration::from_secs(1)),
            ..last
        };
        assert_eq!(signer.sign(&beyond), Err(IssueError::OutlivesCertificate));
    }

    #[test]
    fn a_depth_eight_chain_with_the_certificate_fits_in_8_kib() {
        // Every name at its longest: a 64-byte fleet, issuer name, owner, and root token ID, and
        // a 64-byte subject eight levels below its root.
        let fleet = "f".repeat(64);
        let issuer = Issuer::new(&fleet, &"i".repeat(64));
        let signer = issuer.signer();
        let segments = [
            "aaaaaa", "bbbbbb", "cccccc", "dddddd", "eeeeee", "ffffff", "ggggg", "hhhhh",
        ];
        let mut name: AgentName = "agent-4242".parse().expect("a root");
        let mut chain = vec![ChainLink {
            id: "r".repeat(64).parse().expect("a token id"),
            kid: issuer.authorities[0].id().clone(),
        }];
        for (level, segment) in segments.iter().enumerate() {
            name = name.child(segment).expect("a child");
            if level + 1 < segments.len() {
                let id: TokenId =
                    DelegationId::from_bytes([u8::try_from(level).expect("small"); 16]).into();
                chain.push(ChainLink {
                    id,
                    kid: signer.key_id().clone(),
                });
            }
        }
        assert_eq!((name.as_str().len(), name.depth()), (64, 8));
        let recorded = after(ISSUED, DAY);
        let claims = TokenClaims {
            fleet: fleet.as_str().into(),
            principal: name.as_principal().clone(),
            id: DelegationId::from_bytes([0xff; 16]).into(),
            issued_at: Some(recorded),
            not_before: recorded,
            expires_at: after(recorded, DAY),
            scopes: scopes_of(&[
                &format!("admin:{name}-*"),
                &format!("stream:{name}-*"),
                &format!("grant:{name}.*"),
            ]),
            kind: Some(TokenKind::Agent),
            owner: Some("o".repeat(64).parse().expect("a principal")),
            parent: name.parent().map(|parent| parent.as_principal().clone()),
            chain,
        };
        let token = signer.sign(&claims).expect("signs");
        let margin = MAX_TOKEN_LEN - token.expose().len();
        // The token takes about 3 KiB, which leaves about 5 KiB: room for some 40 more scopes of
        // this length.
        assert!(margin >= 4 * 1024, "only {margin} bytes left");
    }

    #[test]
    fn a_token_with_many_scopes_over_8_kib_is_refused_by_sign() {
        let issuer = Issuer::new("lab", "control-1");
        let signer = issuer.signer();
        let many: Vec<String> = (0..120)
            .map(|i| format!("admin:agent-42.c7-a-rather-long-database-name-{i:03}-*"))
            .collect();
        let many: Vec<&str> = many.iter().map(String::as_str).collect();
        let claims = TokenClaims {
            scopes: scopes_of(&many),
            ..child_claims(&issuer)
        };
        assert!(claims.scopes.len() <= MAX_SCOPES);
        assert!(claims.check_recordable().is_ok());
        assert_eq!(
            signer.sign(&claims),
            Err(IssueError::TooLarge {
                limit: MAX_TOKEN_LEN
            })
        );
        let fewer = TokenClaims {
            scopes: scopes_of(&many[..20]),
            ..child_claims(&issuer)
        };
        assert!(signer.sign(&fewer).is_ok());
    }

    #[test]
    fn a_delegation_signer_needs_a_checked_certificate_and_its_own_key() {
        let issuer = Issuer::new("lab", "control-1");
        let other = Issuer::new("lab", "control-2");
        assert_eq!(
            DelegationSigner::new(&issuer.cert, &other.key, &issuer.fleet, &issuer.authorities)
                .map(|_| ()),
            Err(IssuerError::Mismatch)
        );
        assert_eq!(
            DelegationSigner::new(
                &issuer.cert,
                b"not a key",
                &issuer.fleet,
                &issuer.authorities
            )
            .map(|_| ()),
            Err(IssuerError::Key)
        );
        assert_eq!(
            DelegationSigner::new(&issuer.cert, &issuer.key, &issuer.fleet, &other.authorities)
                .map(|_| ()),
            Err(IssuerError::Certificate(IssuerCertError::UnknownAuthority))
        );
        assert_eq!(
            DelegationSigner::new(
                &issuer.cert,
                &issuer.key,
                &"prod".parse().expect("a fleet"),
                &issuer.authorities
            )
            .map(|_| ()),
            Err(IssuerError::Certificate(IssuerCertError::Subject))
        );
    }

    #[test]
    fn delegation_signer_debug_never_shows_the_key() {
        let issuer = Issuer::new("lab", "control-1");
        let signer = issuer.signer();
        let shown = format!("{signer:?}");
        assert!(shown.contains(signer.key_id().as_str()), "{shown}");
        assert!(shown.contains("control-1"), "{shown}");
        assert!(!shown.contains("key:"), "{shown}");
        let seed = issuer.key.get(issuer.key.len() - 32..).expect("a seed");
        for encoded in [
            STANDARD.encode(seed),
            URL_SAFE_NO_PAD.encode(seed),
            format!("{seed:?}"),
        ] {
            assert!(!shown.contains(&encoded), "{shown}");
        }
    }

    /// When the delegated tokens here are checked: two minutes after [`child_claims`] were
    /// recorded, a minute into their validity.
    fn checked_at() -> Timestamp {
        after(ISSUED, DAY + Duration::from_secs(120))
    }

    fn principal_name(text: &str) -> PrincipalName {
        text.parse().expect("a principal name")
    }

    #[test]
    fn a_delegated_token_verifies_through_its_issuer_certificate_and_only_through_it() {
        let issuer = Issuer::new("lab", "control-1");
        let signer = issuer.signer();
        let claims = TokenClaims {
            scopes: scopes_of(&["admin:agent-42.c7-*", "fork:agent-42-memory"]),
            ..child_claims(&issuer)
        };
        let token = signer.sign(&claims).expect("signs");
        let none = Revocations::new();
        let verified = issuer
            .verifier()
            .verify(&token, checked_at(), &none)
            .expect("verifies");
        assert_eq!(verified.claims, claims, "every claim, iat included");
        assert_eq!(verified.principal.as_str(), "agent-42.c7");
        assert_eq!(&verified.key, signer.key_id());
        assert_eq!(
            verified.signer,
            Signer::Issuer {
                name: "control-1".parse().expect("a node id"),
                authority: issuer.authorities[0].id().clone(),
            }
        );
        assert_eq!(
            verified.token_ref(),
            claims.token_ref(signer.key_id(), SignerKind::Issuer)
        );

        // The issuer's key trusted as if it were an authority: its certificate is refused.
        let issuer_as_authority =
            TokenVerifier::new("lab", [signer.certificate().verifying_key().clone()]);
        assert_eq!(
            issuer_as_authority.verify(&token, checked_at(), &none),
            Err(TokenError::IssuerCertificate(IssuerCertError::AuthorityKey))
        );
        // Another authority of the same fleet, and this authority for another fleet.
        let other = Issuer::new("lab", "control-1");
        assert_eq!(
            other.verifier().verify(&token, checked_at(), &none),
            Err(TokenError::IssuerCertificate(
                IssuerCertError::UnknownAuthority
            ))
        );
        let prod = TokenVerifier::new("prod", issuer.authorities.clone());
        assert_eq!(
            prod.verify(&token, checked_at(), &none),
            Err(TokenError::IssuerCertificate(IssuerCertError::Subject))
        );
        // A verifier whose issuer isn't a fleet name trusts no issuer certificate.
        let unnamed = TokenVerifier::new("Not A Fleet", issuer.authorities.clone());
        assert_eq!(
            unnamed.verify(&token, checked_at(), &none),
            Err(TokenError::IssuerCertificate(IssuerCertError::Subject))
        );
    }

    /// ADR-0047 invariant I2, and the compromise of an issuer key (ADR §8): whatever a stolen
    /// issuer key signs, every node refuses a token outside the agent tier.
    #[test]
    fn i2_an_issuer_key_signs_nothing_outside_the_agent_tier() {
        use fleetring_control::delegation::tier::TierViolation;

        let issuer = Issuer::new("lab", "control-1");
        let verifier = issuer.verifier();
        let header = issuer.header();
        let now = checked_at();
        let none = Revocations::new();
        let base = child_claims(&issuer);
        let root = base.chain[0].clone();
        let soon = after(now, Duration::from_secs(10));
        let scope = |text: &str| -> Scope { text.parse().expect("a scope") };
        let tier = |violation| TokenError::Claims(ClaimsViolation::Tier(violation));
        let with_scopes = |scopes: &[&str]| TokenClaims {
            scopes: scopes_of(scopes),
            ..base.clone()
        };
        let cases = [
            (
                TokenClaims {
                    principal: principal_name("agent-42"),
                    parent: None,
                    chain: Vec::new(),
                    ..base.clone()
                },
                TokenError::Claims(ClaimsViolation::NotDelegated),
            ),
            (
                TokenClaims {
                    kind: None,
                    ..base.clone()
                },
                TokenError::Claims(ClaimsViolation::NotAgent),
            ),
            (
                TokenClaims {
                    parent: Some(principal_name("agent-43")),
                    ..base.clone()
                },
                TokenError::Claims(ClaimsViolation::Parent),
            ),
            (
                TokenClaims {
                    chain: vec![root.clone(), root.clone()],
                    ..base.clone()
                },
                TokenError::Claims(ClaimsViolation::ChainLength { links: 2, depth: 1 }),
            ),
            (
                TokenClaims {
                    chain: Vec::new(),
                    ..base.clone()
                },
                TokenError::Claims(ClaimsViolation::ChainLength { links: 0, depth: 1 }),
            ),
            (with_scopes(&["fleet"]), tier(TierViolation::Fleet)),
            (
                with_scopes(&["admin:agent-43-*"]),
                tier(TierViolation::OutsideRoot {
                    scope: scope("admin:agent-43-*"),
                }),
            ),
            (
                with_scopes(&["read:pub-*"]),
                tier(TierViolation::OutsideRoot {
                    scope: scope("read:pub-*"),
                }),
            ),
            (
                with_scopes(&["admin:agent-420-*"]),
                tier(TierViolation::OutsideRoot {
                    scope: scope("admin:agent-420-*"),
                }),
            ),
            (
                with_scopes(&["grant:agent-42.d1.*"]),
                tier(TierViolation::GrantOutsideChild {
                    scope: scope("grant:agent-42.d1.*"),
                }),
            ),
            (
                with_scopes(&["admin:agent-42.c7x*"]),
                tier(TierViolation::NoSeparator {
                    scope: scope("admin:agent-42.c7x*"),
                }),
            ),
            (
                TokenClaims {
                    expires_at: after(base.not_before, Duration::from_secs(25 * 60 * 60)),
                    ..base.clone()
                },
                TokenError::Claims(ClaimsViolation::TooLong),
            ),
            (
                TokenClaims {
                    issued_at: None,
                    ..base.clone()
                },
                TokenError::Claims(ClaimsViolation::NoIssuedAt),
            ),
            (
                TokenClaims {
                    issued_at: Some(after(base.not_before, Duration::from_secs(1))),
                    ..base.clone()
                },
                TokenError::Claims(ClaimsViolation::IssuedAfterStart),
            ),
            (
                // Within the window `nbf`'s leeway allows, so only point 3.8 refuses it.
                TokenClaims {
                    not_before: soon,
                    expires_at: soon,
                    ..base.clone()
                },
                TokenError::Claims(ClaimsViolation::EndsBeforeStart),
            ),
            (
                TokenClaims {
                    not_before: soon,
                    expires_at: after(now, Duration::from_secs(5)),
                    ..base.clone()
                },
                TokenError::Claims(ClaimsViolation::EndsBeforeStart),
            ),
            (
                TokenClaims {
                    fleet: "prod".into(),
                    ..base.clone()
                },
                TokenError::Issuer,
            ),
        ];
        for (claims, want) in cases {
            let shown = format!("{claims:?}");
            assert_eq!(
                verifier.verify(&issuer.forge(&header, &wire(&claims)), now, &none),
                Err(want),
                "{shown}"
            );
        }

        // A chain entry that isn't `{id, kid}` with valid IDs.
        let json = serde_json::to_value(wire(&base)).expect("claims serialize");
        let with_chain = |chain: serde_json::Value| {
            let mut claims = json.clone();
            claims["fleetring.chain"] = chain;
            claims
        };
        for claims in [
            with_chain(serde_json::json!([{ "id": "root-1" }])),
            with_chain(serde_json::json!([{ "id": "root-1", "kid": "not a key id!" }])),
            with_chain(serde_json::json!([{ "id": "", "kid": "rootrootrootroot" }])),
            with_chain(serde_json::json!([{
                "id": "root-1", "kid": "rootrootrootroot", "extra": 1
            }])),
        ] {
            assert_eq!(
                verifier.verify(&issuer.forge_json(&header, &claims), now, &none),
                Err(TokenError::Malformed),
                "{claims}"
            );
        }
        assert!(
            verifier
                .verify(&issuer.forge(&header, &wire(&base)), now, &none)
                .is_ok()
        );
    }

    /// ADR-0047 point 2.5 and point 3.3: at most 8 levels below the root.
    #[test]
    fn a_delegated_token_is_at_most_eight_levels_deep() {
        let issuer = Issuer::new("lab", "control-1");
        let verifier = issuer.verifier();
        let header = issuer.header();
        let now = checked_at();
        let none = Revocations::new();
        let link = |level: u8| ChainLink {
            id: DelegationId::from_bytes([level; 16]).into(),
            kid: if level == 0 {
                issuer.authorities[0].id().clone()
            } else {
                kid("issuerissuerissu")
            },
        };
        let at_depth = |depth: u8, links: u8| {
            let segments = ["a", "b", "c", "d", "e", "f", "g", "h", "i"];
            let name = segments[..usize::from(depth)]
                .iter()
                .fold("agent-42".to_owned(), |name, segment| {
                    format!("{name}.{segment}")
                });
            let parent = name.rsplit_once('.').map(|(parent, _)| parent.to_owned());
            TokenClaims {
                principal: principal_name(&name),
                parent: parent.as_deref().map(principal_name),
                chain: (0..links).map(link).collect(),
                scopes: scopes_of(&[&format!("admin:{name}-*")]),
                ..child_claims(&issuer)
            }
        };
        assert!(
            verifier
                .verify(&issuer.forge(&header, &wire(&at_depth(8, 8))), now, &none)
                .is_ok(),
            "depth 8 is the deepest"
        );
        assert_eq!(
            verifier.verify(&issuer.forge(&header, &wire(&at_depth(8, 9))), now, &none),
            Err(TokenError::Claims(ClaimsViolation::ChainLength {
                links: 9,
                depth: 8
            }))
        );
        assert_eq!(
            verifier.verify(&issuer.forge(&header, &wire(&at_depth(9, 9))), now, &none),
            Err(TokenError::Claims(ClaimsViolation::NotDelegated)),
            "a name nine levels deep isn't an agent name"
        );
    }

    #[test]
    fn the_header_must_carry_one_standard_base64_issuer_certificate_naming_the_kid() {
        use crate::tls::Validity;

        let issuer = Issuer::new("lab", "control-1");
        let verifier = issuer.verifier();
        let claims = wire(&child_claims(&issuer));
        let now = checked_at();
        let none = Revocations::new();
        let one = STANDARD.encode(&issuer.cert);
        let with_x5c = |x5c: Vec<String>| Header {
            x5c: Some(x5c),
            ..issuer.header()
        };
        let verify = |header: &Header| verifier.verify(&issuer.forge(header, &claims), now, &none);

        let count = TokenError::X5c {
            reason: X5cError::Count,
        };
        let encoding = TokenError::X5c {
            reason: X5cError::Encoding,
        };
        assert_eq!(
            verify(&with_x5c(vec![one.clone(), one.clone()])),
            Err(count.clone())
        );
        assert_eq!(verify(&with_x5c(Vec::new())), Err(count));
        let url = URL_SAFE_NO_PAD.encode(&issuer.cert);
        assert_ne!(url, one, "the two encodings differ for this certificate");
        assert_eq!(verify(&with_x5c(vec![url])), Err(encoding.clone()));
        if one.ends_with('=') {
            let unpadded = one.trim_end_matches('=').to_owned();
            assert_eq!(verify(&with_x5c(vec![unpadded])), Err(encoding.clone()));
        }
        assert_eq!(verify(&with_x5c(vec![format!(" {one}")])), Err(encoding));

        // A node's or a client's certificate from the same authority (ADR-0047, point 1.5).
        let validity = Validity::new(ISSUED, 30 * DAY);
        let node = issuer
            .authority
            .issue_node(&"n1".parse().expect("a node id"), &[], validity)
            .expect("a node certificate");
        let client = issuer
            .authority
            .issue_client(&"alice".parse().expect("a principal"), validity)
            .expect("a client certificate");
        for pem in [node.certificate_pem(), client.certificate_pem()] {
            let got = verify(&with_x5c(vec![STANDARD.encode(der_of(pem))]));
            assert!(
                matches!(got, Err(TokenError::IssuerCertificate(_))),
                "{got:?}"
            );
        }

        // `kid` naming another key than the certificate's: the authority's, or a sibling
        // issuer's, whose certificate the token then can't carry with this issuer's signature.
        let sibling = issuer.sibling("control-2");
        let authority_kid = Header {
            kid: issuer.authorities[0].id().to_string(),
            ..issuer.header()
        };
        assert_eq!(verify(&authority_kid), Err(TokenError::KidMismatch));
        let sibling_kid = Header {
            kid: sibling.signer().key_id().to_string(),
            ..issuer.header()
        };
        assert_eq!(verify(&sibling_kid), Err(TokenError::KidMismatch));
        assert_eq!(verify(&sibling.header()), Err(TokenError::Signature));

        // No refusal quotes the certificate or the token.
        for header in [&authority_kid, &sibling.header()] {
            let token = issuer.forge(header, &claims);
            let error = verifier
                .verify(&token, now, &none)
                .expect_err("refused")
                .to_string();
            assert!(!error.contains(&one[..32]), "{error}");
            assert!(!error.contains(&token.expose()[..32]), "{error}");
        }
    }

    /// A forged chain: an issuer certificate from an authority the node doesn't trust, or one
    /// the trusted authority signed that `IssuerCertificate::check` refuses.
    #[test]
    fn an_issuer_certificate_check_refuses_is_refused_with_its_reason() {
        use rcgen::{CertificateParams, KeyPair, PKCS_ED25519, SanType};

        let issuer = Issuer::new("lab", "control-1");
        let verifier = issuer.verifier();
        let now = checked_at();
        let none = Revocations::new();
        let claims = child_claims(&issuer);

        let rogue = Issuer::new("lab", "control-1");
        let token = rogue.signer().sign(&claims).expect("signs");
        assert_eq!(
            verifier.verify(&token, now, &none),
            Err(TokenError::IssuerCertificate(
                IssuerCertError::UnknownAuthority
            ))
        );

        // The trusted authority signs an issuer certificate that also names a DNS name.
        let key = KeyPair::generate_for(&PKCS_ED25519).expect("a key");
        let uri = "fleetring://lab/issuer/control-1";
        let mut params = CertificateParams::default();
        params.not_before =
            time::OffsetDateTime::from_unix_timestamp(1_800_000_000).expect("a date");
        params.not_after = params.not_before + time::Duration::days(30);
        params.is_ca = rcgen::IsCa::ExplicitNoCa;
        params.key_usages = vec![rcgen::KeyUsagePurpose::DigitalSignature];
        params.subject_alt_names = vec![
            SanType::URI(uri.try_into().expect("an IA5 string")),
            SanType::DnsName("control-1.lab".try_into().expect("an IA5 string")),
        ];
        let cert = issuer
            .authority
            .sign_for_test(&params, &key)
            .expect("signs")
            .der()
            .to_vec();
        let pair = Ed25519KeyPair::from_pkcs8_maybe_unchecked(&key.serialize_der())
            .expect("an Ed25519 key");
        let header = Header {
            alg: ALGORITHM.to_owned(),
            typ: Some(TYPE.to_owned()),
            kid: key_id_of(pair.public_key().as_ref()).to_string(),
            x5c: Some(vec![STANDARD.encode(&cert)]),
        };
        let token = BearerToken::new(signed_text(&header, &wire(&claims), &pair));
        assert_eq!(
            verifier.verify(&token, now, &none),
            Err(TokenError::IssuerCertificate(IssuerCertError::ExtraName))
        );
    }

    /// The plan's P11: the cache skips the parse and the certificate's signature check, never
    /// the validity window.
    #[test]
    fn x5c_cache_never_skips_the_validity_window() {
        let issuer = Issuer::new("lab", "control-1");
        let verifier = issuer.verifier();
        let signer = issuer.signer();
        let none = Revocations::new();
        // A token for the certificate's last hour.
        let end = signer.expires_at();
        let start = Timestamp::from_millis(end.as_millis() - 3_600_000);
        let last_hour = TokenClaims {
            issued_at: Some(start),
            not_before: start,
            expires_at: end,
            ..child_claims(&issuer)
        };
        let token = signer.sign(&last_hour).expect("signs");
        assert_eq!(verifier.certificates.len(), 0);
        assert!(verifier.verify(&token, start, &none).is_ok());
        assert_eq!(verifier.certificates.len(), 1, "the certificate is cached");
        assert!(
            verifier
                .verify(
                    &token,
                    Timestamp::from_millis(end.as_millis() - 1_000),
                    &none
                )
                .is_ok()
        );
        assert_eq!(
            verifier.verify(&token, after(end, Duration::from_millis(1)), &none),
            Err(TokenError::IssuerCertificate(IssuerCertError::Expired)),
            "the certificate expired between two calls"
        );

        // Before the certificate starts, beyond the 30 seconds of leeway.
        let early = signer.sign(&child_claims(&issuer)).expect("signs");
        let too_early = Timestamp::from_millis(ISSUED.as_millis() - 31_000);
        assert_eq!(
            verifier.verify(&early, too_early, &none),
            Err(TokenError::IssuerCertificate(IssuerCertError::NotYetValid))
        );
        assert_eq!(verifier.certificates.len(), 1, "one entry per certificate");
    }

    /// ADR-0047 point 3's lineage revocation, point 1.4.8 and its dated note, and the release
    /// blocker's chains: a grandchild minted by issuer B from the copy of its parent issuer A
    /// signed is refused by every ID and key in its lineage.
    #[test]
    fn revoking_the_issuer_key_its_authority_its_id_or_any_ancestor_refuses_the_token() {
        let a = Issuer::new("lab", "control-1");
        let b = a.sibling("control-2");
        let c7: TokenId = DelegationId::from_bytes([7; 16]).into();
        let root = child_claims(&a).chain[0].clone();
        let r1 = TokenClaims {
            principal: principal_name("agent-42.c7.r1"),
            id: DelegationId::from_bytes([9; 16]).into(),
            scopes: scopes_of(&["admin:agent-42.c7.r1-*"]),
            parent: Some(principal_name("agent-42.c7")),
            chain: vec![
                root.clone(),
                ChainLink {
                    id: c7.clone(),
                    kid: a.signer().key_id().clone(),
                },
            ],
            ..child_claims(&a)
        };
        let token = b.signer().sign(&r1).expect("signs");
        let verifier = a.verifier();
        let now = checked_at();
        let list =
            |revoked: Revoked| -> Revocations { [(revoked, Timestamp::MAX)].into_iter().collect() };
        assert!(verifier.verify(&token, now, &Revocations::new()).is_ok());
        for revoked in [
            Revoked::Key(b.signer().key_id().clone()),
            Revoked::Key(a.authorities[0].id().clone()),
            Revoked::Token(r1.id.clone()),
            Revoked::Token(root.id.clone()),
            Revoked::Key(root.kid.clone()),
            Revoked::Token(c7.clone()),
            Revoked::Key(a.signer().key_id().clone()),
        ] {
            let refusal = verifier
                .verify_identified(&token, now, &list(revoked.clone()))
                .expect_err("refused");
            assert_eq!(refusal.token(), Some(&r1.id), "{revoked:?}");
            assert_eq!(refusal.into_error(), TokenError::Revoked, "{revoked:?}");
        }
        for unrelated in [
            Revoked::Token("tok-2".parse().expect("an id")),
            Revoked::Key(kid("unrelatedunrelat")),
        ] {
            assert!(
                verifier
                    .verify(&token, now, &list(unrelated.clone()))
                    .is_ok(),
                "{unrelated:?}"
            );
        }
    }

    /// No token outlives its issuer certificate, which the stream recheck relies on: a stolen
    /// issuer key signing a 24-hour token a minute before the certificate's end gets a token
    /// every node refuses.
    #[test]
    fn a_delegated_token_that_outlives_its_issuer_certificate_is_refused() {
        let issuer = Issuer::new("lab", "control-1");
        let verifier = issuer.verifier();
        let header = issuer.header();
        let none = Revocations::new();
        let end = issuer.signer().expires_at();
        let start = Timestamp::from_millis(end.as_millis() - 3_600_000);
        let last_hour = TokenClaims {
            issued_at: Some(start),
            not_before: start,
            expires_at: end,
            ..child_claims(&issuer)
        };
        let verify = |claims: &TokenClaims, now| {
            verifier.verify(&issuer.forge(&header, &wire(claims)), now, &none)
        };
        assert!(
            verify(&last_hour, start).is_ok(),
            "its last second is the latest exp"
        );
        let beyond = TokenClaims {
            expires_at: after(end, Duration::from_secs(1)),
            ..last_hour
        };
        assert_eq!(verify(&beyond, start), Err(TokenError::OutlivesCertificate));
        let minute_before = Timestamp::from_millis(end.as_millis() - 60_000);
        let a_day = TokenClaims {
            issued_at: Some(minute_before),
            not_before: minute_before,
            expires_at: after(minute_before, DAY),
            ..child_claims(&issuer)
        };
        assert_eq!(
            verify(&a_day, minute_before),
            Err(TokenError::OutlivesCertificate)
        );
    }

    /// The root of an issuer-signed token's lineage must have been signed by a trusted
    /// authority key, so removing an authority from the `[tls] ca` bundle ends the descendants
    /// of the root tokens it signed, whichever authority certified their issuers.
    #[test]
    fn a_delegated_token_whose_root_no_trusted_authority_signed_is_refused() {
        let issuer = Issuer::new("lab", "control-1");
        let header = issuer.header();
        let now = checked_at();
        let none = Revocations::new();
        let base = child_claims(&issuer);
        let verify = |verifier: &TokenVerifier, claims: &TokenClaims| {
            verifier.verify(&issuer.forge(&header, &wire(claims)), now, &none)
        };
        assert!(verify(&issuer.verifier(), &base).is_ok());
        for stray in [kid("rootrootrootroot"), issuer.signer().key_id().clone()] {
            let claims = TokenClaims {
                chain: vec![ChainLink {
                    id: base.chain[0].id.clone(),
                    kid: stray,
                }],
                ..base.clone()
            };
            assert_eq!(
                verify(&issuer.verifier(), &claims),
                Err(TokenError::UntrustedRoot),
                "{claims:?}"
            );
        }

        // The root's authority, `old`, leaves the bundle; the issuer's authority stays.
        let old = Issuer::new("lab", "control-2");
        let from_old = TokenClaims {
            chain: vec![ChainLink {
                id: base.chain[0].id.clone(),
                kid: old.authorities[0].id().clone(),
            }],
            ..base.clone()
        };
        let both = TokenVerifier::new(
            "lab",
            [old.authorities[0].clone(), issuer.authorities[0].clone()],
        );
        assert!(verify(&both, &from_old).is_ok(), "while both are trusted");
        assert_eq!(
            verify(&issuer.verifier(), &from_old),
            Err(TokenError::UntrustedRoot),
            "once the root's authority is removed"
        );
    }

    /// Re-signs `token` with `key` after `edit` changes its header and claims JSON, for fields
    /// the typed header and claims can't hold.
    fn resigned(
        token: &BearerToken,
        key: &Ed25519KeyPair,
        edit: impl FnOnce(&mut serde_json::Value, &mut serde_json::Value),
    ) -> BearerToken {
        let mut parts = token.expose().split('.');
        let mut header: serde_json::Value =
            decode_json(parts.next().expect("a header")).expect("header JSON");
        let mut claims: serde_json::Value =
            decode_json(parts.next().expect("claims")).expect("claims JSON");
        edit(&mut header, &mut claims);
        let mut text = encode_json(&header);
        text.push('.');
        text.push_str(&encode_json(&claims));
        let signature = key.sign(text.as_bytes());
        text.push('.');
        text.push_str(&URL_SAFE_NO_PAD.encode(signature.as_ref()));
        BearerToken::new(text)
    }

    /// `null` is never read as an absent header field or claim (token spec v1, section 6.3):
    /// an authority-signed token with `"fleetring.chain": null` would otherwise pass as one
    /// without a lineage.
    #[test]
    fn null_header_fields_and_claims_are_refused() {
        let signer = key(7, "prod");
        let verifier = TokenVerifier::new("prod", [signer.verifying_key()]);
        let mut root = spec(&["read:x"]);
        root.principal = "agent-42".parse().expect("a principal");
        let root = root.agent("alice".parse().expect("a principal"));
        let token = signer.issue(&root).expect("issues");
        let now = at_secs(2_000);
        let none = Revocations::new();
        assert!(
            verifier
                .verify(&resigned(&token, &signer.key, |_, _| {}), now, &none)
                .is_ok(),
            "re-signed unchanged, it verifies"
        );
        for field in ["typ", "x5c"] {
            let nulled = resigned(&token, &signer.key, |header, _| {
                header[field] = serde_json::Value::Null;
            });
            assert_eq!(
                verifier.verify(&nulled, now, &none),
                Err(TokenError::Malformed),
                "{field}"
            );
        }
        for claim in [
            "iat",
            "fleetring.kind",
            "fleetring.owner",
            "fleetring.parent",
            "fleetring.chain",
        ] {
            let nulled = resigned(&token, &signer.key, |_, claims| {
                claims[claim] = serde_json::Value::Null;
            });
            assert_eq!(
                verifier.verify(&nulled, now, &none),
                Err(TokenError::Malformed),
                "{claim}"
            );
        }

        // An issuer-signed token's optional fields alike.
        let issuer = Issuer::new("lab", "control-1");
        let delegated = issuer.signer().sign(&child_claims(&issuer)).expect("signs");
        let issuer_key = issuer.key_pair();
        assert!(
            issuer
                .verifier()
                .verify(
                    &resigned(&delegated, &issuer_key, |_, _| {}),
                    checked_at(),
                    &none
                )
                .is_ok()
        );
        for claim in ["iat", "fleetring.owner"] {
            let nulled = resigned(&delegated, &issuer_key, |_, claims| {
                claims[claim] = serde_json::Value::Null;
            });
            assert_eq!(
                issuer.verifier().verify(&nulled, checked_at(), &none),
                Err(TokenError::Malformed),
                "{claim}"
            );
        }
    }
}
