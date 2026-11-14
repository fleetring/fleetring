//! What a token claims: its subject, ID, times, scopes, and, for an agent, its owner and its
//! lineage (ADR-0047, point 2).

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

use crate::scope::Scopes;
use crate::{KeyId, Millis, PrincipalName, Timestamp, TokenId, TokenRef};

use super::AgentName;
use super::tier::{self, TierViolation};

/// The longest an issuer-signed token lasts, from its `nbf` to its `exp`: 24 hours (ADR-0047,
/// points 3.8 and 5.4).
pub const MAX_DELEGATED_LIFETIME: Millis = Millis::new(24 * 60 * 60 * 1_000);

/// The furthest after its `iat` a recorded delegation's `nbf` may be: 24 hours (ADR-0047,
/// points 5.4 and 5.9). Nodes don't check it; [`TokenClaims::check_recordable`] does.
pub const MAX_NOT_BEFORE_LEAD: Millis = Millis::new(24 * 60 * 60 * 1_000);

/// The bytes a [`DelegationId`] encodes: 128 random bits (ADR-0047, point 2.4).
const DELEGATION_ID_BYTES: usize = 16;

/// The length of a [`DelegationId`]'s text: 16 bytes in unpadded base64url.
const DELEGATION_ID_LEN: usize = 22;

/// The base64url alphabet (RFC 4648 §5), indexed by six-bit value.
const BASE64URL: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";

/// How far a root token's signer's clock may run ahead of control time (ADR-0047, point 3): a
/// principal revocation refuses a root token whose `iat` is up to this much after its `before`.
const AUTHORITY_ISSUED_AT_LEEWAY: Millis = Millis::new(30_000);

/// The six-bit value of a base64url character, or `None` for any other byte.
fn base64url_value(byte: u8) -> Option<u8> {
    BASE64URL
        .iter()
        .position(|&symbol| symbol == byte)
        .and_then(|value| u8::try_from(value).ok())
}

/// A delegated token's ID, its `jti` (ADR-0047, point 2.4): 128 bits the proposing control node
/// draws from its injected generator, as 22 characters of unpadded base64url.
///
/// It is a [`TokenId`] too, so the denylist names it as `Revoked::Token`, and every copy of one
/// delegation, whichever issuer signed it, has it. Only the canonical encoding of 16 bytes is
/// accepted, so each ID has one spelling: the last character is `A`, `Q`, `g`, or `w`, whose low
/// four bits are zero.
///
/// # Examples
///
/// ```
/// use fleetring_control::delegation::DelegationId;
///
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let id = DelegationId::from_bytes([0xfb; 16]);
/// assert_eq!(id.as_str(), "-_v7-_v7-_v7-_v7-_v7-w");
/// assert_eq!(id.as_str().parse::<DelegationId>()?, id);
/// assert!("-_v7-_v7-_v7-_v7-_v7-x".parse::<DelegationId>().is_err());
/// assert!("short".parse::<DelegationId>().is_err());
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct DelegationId(TokenId);

impl DelegationId {
    /// The ID that encodes `bytes`, drawn at random by the proposing control node.
    #[must_use]
    pub fn from_bytes(bytes: [u8; DELEGATION_ID_BYTES]) -> Self {
        let symbol = |value: u8| char::from(BASE64URL[usize::from(value & 0x3f)]);
        let mut text = String::with_capacity(DELEGATION_ID_LEN);
        for group in bytes.chunks(3) {
            let (a, b, c) = (
                group[0],
                group.get(1).copied().unwrap_or(0),
                group.get(2).copied().unwrap_or(0),
            );
            text.push(symbol(a >> 2));
            text.push(symbol((a << 4) | (b >> 4)));
            if group.len() > 1 {
                text.push(symbol((b << 2) | (c >> 6)));
            }
            if group.len() > 2 {
                text.push(symbol(c));
            }
        }
        // Base64url's alphabet is within the token ID grammar, and 22 characters within its
        // 64-byte bound.
        Self(TokenId::new_unchecked(text))
    }

    /// The ID as text.
    #[must_use]
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }

    /// The ID as the token ID it is, as `Revoked::Token` and a chain name it.
    #[must_use]
    pub fn as_token_id(&self) -> &TokenId {
        &self.0
    }
}

/// Whether `text` is the canonical unpadded base64url of 16 bytes.
fn is_delegation_id(text: &str) -> bool {
    let bytes = text.as_bytes();
    let Some((&last, _)) = bytes.split_last() else {
        return false;
    };
    bytes.len() == DELEGATION_ID_LEN
        && bytes.iter().all(|&byte| base64url_value(byte).is_some())
        && base64url_value(last).is_some_and(|value| value & 0x0f == 0)
}

impl fmt::Display for DelegationId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl TryFrom<TokenId> for DelegationId {
    type Error = ParseDelegationIdError;

    fn try_from(id: TokenId) -> Result<Self, Self::Error> {
        if is_delegation_id(id.as_str()) {
            Ok(Self(id))
        } else {
            Err(ParseDelegationIdError::Invalid)
        }
    }
}

impl TryFrom<String> for DelegationId {
    type Error = ParseDelegationIdError;

    fn try_from(text: String) -> Result<Self, Self::Error> {
        if !is_delegation_id(&text) {
            return Err(ParseDelegationIdError::Invalid);
        }
        TokenId::try_from(text)
            .map_err(|_| ParseDelegationIdError::Invalid)
            .and_then(Self::try_from)
    }
}

impl FromStr for DelegationId {
    type Err = ParseDelegationIdError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        Self::try_from(text.to_owned())
    }
}

impl From<DelegationId> for TokenId {
    fn from(id: DelegationId) -> Self {
        id.0
    }
}

impl From<DelegationId> for String {
    fn from(id: DelegationId) -> Self {
        id.0.into()
    }
}

/// Why a string is not a valid [`DelegationId`].
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum ParseDelegationIdError {
    /// The ID isn't 22 characters of base64url encoding 16 bytes.
    #[error("a delegation id is 22 base64url characters encoding 16 bytes")]
    Invalid,
}

/// One ancestor in a delegated token's chain (ADR-0047, point 2.2): the ancestor's token ID, and
/// the key that signed the copy of the ancestor's token this one was minted from.
///
/// For the root, `id` is its token's `jti` and `kid` the authority's key; for a delegated
/// ancestor, `id` is its [`DelegationId`] and `kid` an issuer's key.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChainLink {
    /// The ancestor's token ID.
    pub id: TokenId,
    /// The ID of the key that signed the ancestor's copy.
    pub kid: KeyId,
}

/// What kind of principal a token's subject is, its `fleetring.kind` claim.
///
/// Only an agent's token can delegate (ADR-0047, point 2.6). The enum is exhaustive on purpose:
/// a new kind must be considered by every check that matches on it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TokenKind {
    /// An agent, `"agent"`.
    Agent,
}

/// Who signed a copy of a token: the fleet's authority, or a control node's delegation issuer.
///
/// A node knows it from the token, which carries an issuer certificate in `x5c` exactly when an
/// issuer signed it. It decides how a principal revocation judges the token's `iat`
/// ([`SignerKind::issued_at_leeway`]). The enum is exhaustive on purpose: a new signer must be
/// considered by every check that matches on it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum SignerKind {
    /// The authority's key signed it: a root agent's token, a service's, or a person's.
    Authority,
    /// A delegation issuer's key signed it: always a delegated agent's token.
    Issuer,
}

impl SignerKind {
    /// The signer a token naming `principal` has: an issuer for a delegated agent name, the
    /// authority for any other (ADR-0047, points 2.6 and 3).
    ///
    /// Nodes refuse a token whose signer and subject disagree, so [`apply`](crate::apply), which
    /// never sees a token, may derive a caller's signer from its name.
    #[must_use]
    pub fn for_principal(principal: &PrincipalName) -> Self {
        match AgentName::try_from(principal.clone()) {
            Ok(name) if name.is_delegated() => Self::Issuer,
            Ok(_) | Err(_) => Self::Authority,
        }
    }

    /// How much later than a principal revocation's `before` a token's `iat` may be and still be
    /// refused by it (ADR-0047, point 3): 30 seconds for the authority's tokens, whose `iat`
    /// comes from their signer's clock, and none for an issuer's, whose `iat` is control time.
    #[must_use]
    pub fn issued_at_leeway(self) -> Millis {
        match self {
            Self::Authority => AUTHORITY_ISSUED_AT_LEEWAY,
            Self::Issuer => Millis::new(0),
        }
    }
}

/// Every claim a token carries but its key ID (`kid`) and issuer certificate (`x5c`), which
/// belong to one signed copy (ADR-0047, point 2.2).
///
/// The control plane records a delegation's claims so that any control node can sign them
/// again, and compares a delegated caller's presented claims with that record. Times are
/// milliseconds; a JWT's are whole seconds.
///
/// # What the type doesn't enforce
///
/// Its fields are public and its deserialization checks only each field's own grammar, so a
/// value, from a command or a token, may hold claims no rule allows. In particular:
///
/// - `fleet` is any string, never compared with a fleet name;
/// - `scopes` deserializes with no bound of [`MAX_SCOPES`](crate::scope::MAX_SCOPES) and no check
///   that the set is in canonical form ([`Scopes::normalized`]), sorted with nothing repeated or
///   covered;
/// - `chain` has no length bound, and nothing ties it to `principal`'s depth;
/// - `issued_at` may be `None`, and nothing orders the three times;
/// - `kind`, `owner`, and `parent` may be present or absent in any combination.
///
/// [`check_issuer_signed`](Self::check_issuer_signed) and
/// [`check_recordable`](Self::check_recordable) check the chain, the times, the kind, and the
/// parent, and the tier of the scopes. What they leave out, `apply` checks before it records a
/// delegation (ADR-0047, points 5.9 and 6.5): the scope count, the scopes' canonical form, the
/// owner, and the whole-second times. The fleet a child records is its caller's, which `apply`
/// compares with a delegated caller's record; control state holds no fleet name to compare a
/// root caller's with, so the handler, which checked the presented token's `iss`, is what ties
/// it to the fleet.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TokenClaims {
    /// The fleet, `iss`.
    pub fleet: Box<str>,
    /// The subject, `sub`.
    pub principal: PrincipalName,
    /// The token's ID, `jti`: a delegated token's is its [`DelegationId`].
    pub id: TokenId,
    /// When it was issued, `iat`, if it says.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub issued_at: Option<Timestamp>,
    /// The start of its validity, `nbf`.
    pub not_before: Timestamp,
    /// Its expiry, `exp`.
    pub expires_at: Timestamp,
    /// Its scopes, `fleetring.scopes`.
    pub scopes: Scopes,
    /// What kind of principal the subject is, `fleetring.kind`, if it says.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kind: Option<TokenKind>,
    /// The principal every agent in the lineage answers to, `fleetring.owner`, if it says.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner: Option<PrincipalName>,
    /// The principal that delegated to the subject, `fleetring.parent`; only a delegated token
    /// has it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent: Option<PrincipalName>,
    /// The lineage, `fleetring.chain`: one link per ancestor, root first, ending with the
    /// parent; empty for a token the authority signed.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub chain: Vec<ChainLink>,
}

impl TokenClaims {
    /// Checks the structure every node requires of an issuer-signed token (ADR-0047, point 3.1
    /// to 3.8), and returns its subject as the delegated agent name it must be.
    ///
    /// In point order, the first that fails is named:
    ///
    /// 1. `sub` is a delegated agent name (point 3.1);
    /// 2. `kind` is `agent`, and `parent` is `sub` less its last segment (point 3.2);
    /// 3. the chain has one link per level of depth, so at most [`MAX_DEPTH`](super::MAX_DEPTH)
    ///    (point 3.3; each link's ID and key ID are valid by their types);
    /// 4. the scopes are in the agent tier for `sub`, by [`tier::check`] (points 3.4 to 3.7);
    /// 5. `iat` is present and `iat ≤ nbf < exp`, and `exp − nbf` is at most
    ///    [`MAX_DELEGATED_LIFETIME`] (point 3.8).
    ///
    /// It is the one implementation of these rules: a node verifying a token, the signer before
    /// it signs, and [`apply`](crate::apply) before it records (through
    /// [`check_recordable`](Self::check_recordable)) all call it. Point 3.9, that `iss` is the
    /// node's own fleet, is the caller's check, against the fleet it serves. What this doesn't
    /// check of the fields is listed on [`TokenClaims`].
    ///
    /// # Errors
    ///
    /// The [`ClaimsViolation`] of the first rule broken, in the order above.
    pub fn check_issuer_signed(&self) -> Result<AgentName, ClaimsViolation> {
        let name = AgentName::try_from(self.principal.clone())
            .ok()
            .filter(AgentName::is_delegated)
            .ok_or(ClaimsViolation::NotDelegated)?;

        if self.kind != Some(TokenKind::Agent) {
            return Err(ClaimsViolation::NotAgent);
        }
        let parent = name.parent();
        if self.parent.as_ref() != parent.as_ref().map(AgentName::as_principal) {
            return Err(ClaimsViolation::Parent);
        }

        if self.chain.len() != name.depth() {
            return Err(ClaimsViolation::ChainLength {
                links: self.chain.len(),
                depth: name.depth(),
            });
        }

        tier::check(&name, &self.scopes)?;

        let Some(issued_at) = self.issued_at else {
            return Err(ClaimsViolation::NoIssuedAt);
        };
        if issued_at > self.not_before {
            return Err(ClaimsViolation::IssuedAfterStart);
        }
        if self.not_before >= self.expires_at {
            return Err(ClaimsViolation::EndsBeforeStart);
        }
        if self.expires_at > self.not_before.saturating_add(MAX_DELEGATED_LIFETIME) {
            return Err(ClaimsViolation::TooLong);
        }
        Ok(name)
    }

    /// Checks what [`apply`](crate::apply) requires of a delegation's claims before it records
    /// them, and the signer before it signs them: everything
    /// [`check_issuer_signed`](Self::check_issuer_signed) checks, and that `nbf` is at most
    /// [`MAX_NOT_BEFORE_LEAD`] after `iat` (ADR-0047, point 5.9), so that the bounds of point 6.3
    /// hold whoever proposed the delegation.
    ///
    /// Nodes don't check the lead: point 3 doesn't ask it of a token, and every token the
    /// control plane signs was recorded with it.
    ///
    /// # Errors
    ///
    /// What [`check_issuer_signed`](Self::check_issuer_signed) returns, then
    /// [`ClaimsViolation::StartsTooLate`].
    pub fn check_recordable(&self) -> Result<AgentName, ClaimsViolation> {
        let name = self.check_issuer_signed()?;
        let Some(issued_at) = self.issued_at else {
            return Err(ClaimsViolation::NoIssuedAt);
        };
        if self.not_before > issued_at.saturating_add(MAX_NOT_BEFORE_LEAD) {
            return Err(ClaimsViolation::StartsTooLate);
        }
        Ok(name)
    }

    /// What [`Revocations::refuses_token`](crate::Revocations::refuses_token) needs of the copy
    /// of this token that `kid` signed, a key of `signer`'s.
    #[must_use]
    pub fn token_ref<'a>(&'a self, kid: &'a KeyId, signer: SignerKind) -> TokenRef<'a> {
        TokenRef {
            id: &self.id,
            kid,
            chain: &self.chain,
            principal: &self.principal,
            issued_at: self.issued_at,
            signer,
        }
    }
}

/// Why a token's claims can't be an issuer-signed token's (ADR-0047, point 3.1 to 3.8), or a
/// recorded delegation's (point 5.9): what [`TokenClaims::check_issuer_signed`] and
/// [`TokenClaims::check_recordable`] return.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum ClaimsViolation {
    /// `sub` isn't a delegated agent name (point 3.1).
    #[error("an issuer-signed token's subject must be a delegated agent name")]
    NotDelegated,

    /// `fleetring.kind` isn't `agent` (point 3.2).
    #[error("an issuer-signed token's kind must be agent")]
    NotAgent,

    /// `fleetring.parent` isn't `sub` less its last segment (point 3.2).
    #[error("an issuer-signed token's parent must be its subject less its last segment")]
    Parent,

    /// `fleetring.chain` doesn't have one link per level of depth (point 3.3).
    #[error(
        "an issuer-signed token's chain has {links} links, but its subject is {depth} levels \
         below its root"
    )]
    ChainLength {
        /// How many links the chain has.
        links: usize,
        /// How many levels below its root the subject is.
        depth: usize,
    },

    /// A scope isn't in the agent tier for `sub` (points 3.4 to 3.7).
    #[error(transparent)]
    Tier(#[from] TierViolation),

    /// The token doesn't say when it was issued (point 3.8).
    #[error("an issuer-signed token must say when it was issued")]
    NoIssuedAt,

    /// `iat` is after `nbf` (point 3.8).
    #[error("an issuer-signed token can't be issued after its validity starts")]
    IssuedAfterStart,

    /// `exp` is at or before `nbf` (point 3.8).
    #[error("an issuer-signed token's validity must end after it starts")]
    EndsBeforeStart,

    /// `exp − nbf` is more than 24 hours (point 3.8).
    #[error("an issuer-signed token lasts at most 24 hours")]
    TooLong,

    /// `nbf` is more than 24 hours after `iat` (point 5.9), which only a recorded delegation is
    /// checked for.
    #[error("a delegation's validity must start at most 24 hours after it is recorded")]
    StartsTooLate,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn delegation_ids_encode_sixteen_bytes_in_base64url() {
        assert_eq!(
            DelegationId::from_bytes([0; 16]).as_str(),
            "AAAAAAAAAAAAAAAAAAAAAA"
        );
        let counting: [u8; 16] = std::array::from_fn(|i| u8::try_from(i).expect("small"));
        // RFC 4648 base64 of 00 01 … 0f, unpadded, in the URL-safe alphabet.
        assert_eq!(
            DelegationId::from_bytes(counting).as_str(),
            "AAECAwQFBgcICQoLDA0ODw"
        );
        assert_eq!(
            DelegationId::from_bytes([0xff; 16]).as_str(),
            "_____________________w"
        );
    }

    #[test]
    fn every_drawn_id_parses_back_and_no_other_spelling_does() {
        for seed in 0_u8..=255 {
            let bytes: [u8; 16] = std::array::from_fn(|i| {
                seed.wrapping_mul(31)
                    .wrapping_add(u8::try_from(i).expect("small"))
            });
            let id = DelegationId::from_bytes(bytes);
            assert_eq!(id.as_str().len(), 22);
            assert_eq!(id.as_str().parse::<DelegationId>(), Ok(id.clone()));
        }
        for bad in [
            "",
            "AAAAAAAAAAAAAAAAAAAAA",
            "AAAAAAAAAAAAAAAAAAAAAAA",
            "AAAAAAAAAAAAAAAAAAAAAB",
            "AAAAAAAAAAAAAAAAAAAA+A",
            "AAAAAAAAAAAAAAAAAAAA=A",
        ] {
            assert_eq!(
                bad.parse::<DelegationId>(),
                Err(ParseDelegationIdError::Invalid),
                "{bad}"
            );
        }
    }

    #[test]
    fn the_leeway_is_the_authoritys_alone() {
        assert_eq!(
            SignerKind::Authority.issued_at_leeway(),
            Millis::new(30_000)
        );
        assert_eq!(SignerKind::Issuer.issued_at_leeway(), Millis::new(0));
    }
}
