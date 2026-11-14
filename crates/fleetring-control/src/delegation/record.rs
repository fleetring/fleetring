//! What control state records of a delegation: the record itself, the operation that made it,
//! and what a `RecordDelegation` command carries (ADR-0047, points 5.7, 5.9, 6.1, and 6.2).

use std::fmt;

use fleetring::NodeId;
use serde::{Deserialize, Serialize};

use crate::fork::OperationId;
use crate::scope::Scopes;
use crate::{KeyId, Millis, PrincipalName, Timestamp, TokenId};

use super::{AgentName, DelegationId, TokenClaims};

/// One recorded delegation (ADR-0047, point 6.1): the complete claims of the child's token, so
/// that any control node can sign them again, and who asked for it, when, and through which
/// control node.
///
/// Only [`apply`](crate::apply) makes one. A record is *kept* until `kept_until`, then forgotten,
/// and a revocation that covers it prunes it earlier; it is *live* while it is kept and its
/// token's `exp` is after a command's own stamp (point 5.9).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct Delegation {
    /// The child's claims, every claim but `kid` and `x5c`: what every copy of its token says.
    pub claims: TokenClaims,
    /// The ID of the token the parent presented, the last link of the child's chain.
    pub parent_token: TokenId,
    /// The operation that recorded it.
    pub operation: OperationId,
    /// The parent principal that asked for it.
    pub requested_by: PrincipalName,
    /// The control node that logged the command, as the log layer stamped it (point 6.1). It is
    /// advisory against a compromised leader, which stamps what it likes.
    pub recorded_by: NodeId,
    /// The command's effective time, which the child's `iat` is, rounded down to the second.
    pub recorded_at: Timestamp,
    /// When the record may be forgotten: the effective time, plus the token's life counted from
    /// the command's stamp, plus 30 seconds and 1 hour (point 6.1).
    pub kept_until: Timestamp,
}

impl Delegation {
    /// The child's name: its token's subject, a delegated agent name.
    #[must_use]
    pub fn principal(&self) -> &PrincipalName {
        &self.claims.principal
    }

    /// Whether the record is live for a command stamped `at` that runs at `now`: kept at `now`,
    /// and its token's `exp` after `at` (ADR-0047, point 5.9).
    #[must_use]
    pub fn is_live(&self, at: Timestamp, now: Timestamp) -> bool {
        self.is_kept(now) && self.claims.expires_at > at
    }

    /// Whether the record is still kept at `now`, its `kept_until` not yet reached.
    #[must_use]
    pub fn is_kept(&self, now: Timestamp) -> bool {
        self.kept_until > now
    }

    /// Whether the record's token is `token`, or descends from it: its ID or any ID in its chain
    /// is `token` (ADR-0047, point 11.4).
    #[must_use]
    pub fn descends_from(&self, token: &TokenId) -> bool {
        self.claims.id == *token || self.claims.chain.iter().any(|link| link.id == *token)
    }
}

/// The parent's request as it asked for it (ADR-0047, point 5.7): what a resend with the same
/// operation ID must repeat to return the same delegation.
///
/// It holds nothing resolved: not the default scopes, not an absolute time for an absent
/// `not_before`, not an expiry. The control node resolves those once and carries them in the
/// command beside the request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CanonicalRequest {
    /// The child's last segment: its name is the parent's followed by `.<segment>`.
    pub segment: Box<str>,
    /// The scopes asked for, or `None` for the defaults. A resend compares them as normalized
    /// sets, so their order doesn't matter.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scopes: Option<Scopes>,
    /// The lifetime asked for, if one was.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lifetime: Option<Millis>,
    /// The start of validity asked for, if one was.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub not_before: Option<Timestamp>,
    /// The ID of the token the parent presented.
    pub caller_token: TokenId,
}

impl CanonicalRequest {
    /// The first field, in the order segment, scopes, lifetime, `not_before`, caller token, in
    /// which `other` differs from `self`; `None` if they are the same request. Scopes compare as
    /// normalized sets.
    #[must_use]
    pub fn first_difference(&self, other: &Self) -> Option<RequestField> {
        let scopes = |request: &Self| request.scopes.as_ref().map(Scopes::normalized);
        if self.segment != other.segment {
            Some(RequestField::Segment)
        } else if scopes(self) != scopes(other) {
            Some(RequestField::Scopes)
        } else if self.lifetime != other.lifetime {
            Some(RequestField::Lifetime)
        } else if self.not_before != other.not_before {
            Some(RequestField::NotBefore)
        } else if self.caller_token != other.caller_token {
            Some(RequestField::CallerToken)
        } else {
            None
        }
    }
}

/// A field of a [`CanonicalRequest`], as a conflicting resend names it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum RequestField {
    /// The child's segment.
    Segment,
    /// The scopes asked for.
    Scopes,
    /// The lifetime asked for.
    Lifetime,
    /// The start of validity asked for.
    NotBefore,
    /// The token the parent presented.
    CallerToken,
    /// The key that signed the copy of its token the parent presented: a resend must present
    /// the copy the delegation was minted from, whose key the child's chain names.
    CallerKey,
}

impl fmt::Display for RequestField {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Segment => "segment",
            Self::Scopes => "scopes",
            Self::Lifetime => "lifetime",
            Self::NotBefore => "not_before",
            Self::CallerToken => "caller token",
            Self::CallerKey => "caller token's signing key",
        })
    }
}

/// The parent's token as it presented it (ADR-0047, point 5.9): every claim, and the ID of the
/// key that signed the copy it presented. [`apply`](crate::apply) never sees the token itself.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CallerToken {
    /// Every claim of the presented token but `kid` and `x5c`.
    pub claims: TokenClaims,
    /// The ID of the key that signed the presented copy.
    pub kid: KeyId,
}

/// What became of a delegation operation's delegation (ADR-0047, point 6.2).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum OperationOutcome {
    /// The delegation is recorded, and stays so until it is forgotten or revoked.
    Recorded,
    /// The *tombstone*: a revocation pruned the delegation's record at `at`, so a resend is
    /// refused rather than recorded again.
    Revoked {
        /// The effective time of the revocation.
        at: Timestamp,
    },
}

/// A delegation operation (ADR-0047, point 6.2): who asked, what they asked, the delegation it
/// recorded, and what became of it, kept until `kept_until`.
///
/// Delegation operations are separate from fork operations: one ID in both names two
/// operations.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct DelegationOperation {
    /// The parent principal that asked, the only one that may resend it.
    pub requested_by: PrincipalName,
    /// What it asked, with its scopes, if any, normalized.
    pub request: CanonicalRequest,
    /// The delegation it recorded.
    pub delegation: DelegationId,
    /// What became of the delegation.
    pub outcome: OperationOutcome,
    /// When the operation may be forgotten: its delegation's `kept_until` plus 1 hour, or 1 hour
    /// from the revocation that tombstoned it.
    pub kept_until: Timestamp,
}

impl DelegationOperation {
    /// Whether the operation is still kept at `now`.
    #[must_use]
    pub fn is_kept(&self, now: Timestamp) -> bool {
        self.kept_until > now
    }
}

/// Whether a `RecordDelegation` recorded a new delegation or answered a resend of a known
/// operation with the delegation it recorded (ADR-0047, point 5.7).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Resent {
    /// The delegation is new.
    No,
    /// The command resent a known operation; nothing changed.
    Yes,
}

/// What a rate window counts: a kind of command, within a scope (ADR-0047, point 6.2).
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RateKey {
    /// The kind of command counted.
    pub kind: RateKind,
    /// Whose commands are counted.
    pub scope: RateScope,
}

/// A kind of command a rate window counts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum RateKind {
    /// Recorded delegations (ADR-0047, point 5.10).
    Delegation,
    /// Revocations under the ancestor rule (ADR-0047, point 11.6).
    AncestorRevocation,
}

/// Whose commands a rate window counts.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum RateScope {
    /// One caller principal's.
    Caller(PrincipalName),
    /// Those of the delegated agents of one root lineage, named by its root, never the root
    /// agent's own.
    Lineage(AgentName),
}

/// A limit a `RecordDelegation` may meet (ADR-0047, points 5.10 and 6.3).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum Limit {
    /// The lineage holds [`MAX_LIVE_PER_LINEAGE`](super::MAX_LIVE_PER_LINEAGE) live delegations.
    Live,
    /// The caller recorded [`CALLER_RATE`](super::CALLER_RATE) delegations in the last minute.
    CallerRate,
    /// The lineage's delegated agents recorded [`LINEAGE_RATE`](super::LINEAGE_RATE) delegations
    /// in the last minute.
    LineageRate,
    /// The child would be more than [`MAX_DEPTH`](super::MAX_DEPTH) levels below its root.
    Depth,
    /// The lineage holds [`MAX_RECORDS_PER_LINEAGE`](super::MAX_RECORDS_PER_LINEAGE) unpruned
    /// records, the backstop the rates should never let it reach.
    Backstop,
}

impl fmt::Display for Limit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Live => "at most 256 live delegations per root lineage",
            Self::CallerRate => "at most 30 delegations a minute per caller",
            Self::LineageRate => {
                "at most 60 delegations a minute from a root lineage's delegated agents"
            }
            Self::Depth => "at most 8 levels below the root",
            Self::Backstop => "at most 8192 unpruned delegation records per root lineage",
        })
    }
}

/// What a revocation under the ancestor rule ends (ADR-0047, point 11.3): a token, a principal
/// and every name below it, or both at once. A key is never a target.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum AncestorTarget {
    /// The token the caller presented, or a delegation recorded below the caller.
    Token(TokenId),
    /// The caller itself, or the principal of a delegation recorded below it.
    Principal(AgentName),
    /// A principal and a token, each as above, both revoked or neither. An agent removing
    /// itself names its principal and the token it presented (point 11.7), and that token
    /// authenticates no second command once either lands. A token the principal covers, a
    /// delegation recorded at or below it, gets no entry of its own.
    PrincipalAndToken {
        /// The principal.
        principal: AgentName,
        /// The token.
        token: TokenId,
    },
}

impl fmt::Display for AncestorTarget {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Token(id) => write!(f, "token '{id}'"),
            Self::Principal(principal) => write!(f, "principal '{principal}'"),
            Self::PrincipalAndToken { principal, token } => {
                write!(f, "principal '{principal}' and token '{token}'")
            }
        }
    }
}

/// Why the ancestor rule doesn't let a caller revoke a target (ADR-0047, point 11.3).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum AncestorViolation {
    /// The caller isn't an agent: only agents revoke without `fleet`.
    CallerNotAgent,
    /// The principal named is neither the caller nor that of a live delegation recorded below
    /// it.
    NotBelowCaller,
}

impl fmt::Display for AncestorViolation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::CallerNotAgent => "only an agent revokes without the fleet scope",
            Self::NotBelowCaller => {
                "an agent revokes only itself or a principal it or its descendants delegated to"
            }
        })
    }
}

/// A limit a revocation under the ancestor rule may meet (ADR-0047, point 11.6).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum RevocationLimit {
    /// The lineage's delegated agents have added
    /// [`MAX_ANCESTOR_ENTRIES_PER_LINEAGE`](super::MAX_ANCESTOR_ENTRIES_PER_LINEAGE) live
    /// entries the cap counts between them, and the revocation, by one of them, would add more
    /// than it replaces.
    Entries {
        /// The caller's child whose subtree holds the most of them, if any does: revoking it
        /// frees them.
        busiest: Option<AgentName>,
    },
    /// The root agent has added
    /// [`MAX_ANCESTOR_ENTRIES_PER_LINEAGE`](super::MAX_ANCESTOR_ENTRIES_PER_LINEAGE) live
    /// entries the cap counts, its own budget, which its delegated agents never use, and the
    /// revocation, by the root, would add more of its own than it replaces.
    RootEntries,
    /// The caller made [`ANCESTOR_CALLER_RATE`](super::ANCESTOR_CALLER_RATE) revocations under
    /// the ancestor rule in the last minute.
    CallerRate,
    /// The lineage's delegated agents made
    /// [`ANCESTOR_LINEAGE_RATE`](super::ANCESTOR_LINEAGE_RATE) revocations under the ancestor
    /// rule in the last minute.
    LineageRate,
}

impl fmt::Display for RevocationLimit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Entries {
                busiest: Some(child),
            } => write!(
                f,
                "at most 1024 ancestor-rule entries from a root lineage's delegated agents; \
                 revoking {child}, whose subtree holds the most, frees them"
            ),
            Self::Entries { busiest: None } => f.write_str(
                "at most 1024 ancestor-rule entries from a root lineage's delegated agents",
            ),
            Self::RootEntries => f.write_str(
                "at most 1024 ancestor-rule entries from the root agent itself: the root's own \
                 budget is full; wait for its entries to expire, revoke the root itself, or ask \
                 an operator with the fleet scope",
            ),
            Self::CallerRate => {
                f.write_str("at most 30 ancestor-rule revocations a minute per caller")
            }
            Self::LineageRate => f.write_str(
                "at most 60 ancestor-rule revocations a minute from a root lineage's delegated \
                 agents",
            ),
        }
    }
}

/// What a `RecordDelegation` breaks of the delegation's structure, or of its parent's record
/// (ADR-0047, point 5.9).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum StructureViolation {
    /// The requester isn't the principal of the token it presented.
    RequesterNotCaller,
    /// The caller isn't an agent.
    CallerNotAgent,
    /// The caller's token doesn't say it is an agent's.
    CallerKindNotAgent,
    /// The caller's token names no owner.
    NoOwner,
    /// The request's segment isn't 1 to 32 of `a-z`, `0-9`, and `_`, or the child's name would
    /// be longer than 64 bytes.
    Segment,
    /// The caller presented another token than its request names.
    CallerTokenDiffers,
    /// `not_before` or `exp` isn't a whole number of seconds.
    NotWholeSeconds,
    /// The scopes number more than [`MAX_SCOPES`](crate::scope::MAX_SCOPES).
    TooManyScopes,
    /// The child's `exp` is after the caller's token's.
    ExpiresAfterCaller,
    /// The child's owner isn't the parent record's.
    OwnerDiffers,
    /// The child's chain isn't the parent record's followed by the parent's copy.
    ChainDiffers,
    /// The child's `exp` is after the parent record's.
    ExpiresAfterParent,
}

impl fmt::Display for StructureViolation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::RequesterNotCaller => "the requester isn't the presented token's principal",
            Self::CallerNotAgent => "only an agent can delegate",
            Self::CallerKindNotAgent => "the presented token isn't an agent's",
            Self::NoOwner => "the presented token names no owner",
            Self::Segment => {
                "a segment is 1 to 32 of a-z, 0-9, and '_', and an agent name at most 64 bytes"
            }
            Self::CallerTokenDiffers => "the request names another token than the one presented",
            Self::NotWholeSeconds => "a delegation's times are whole seconds",
            Self::TooManyScopes => "a delegation holds at most 256 scopes",
            Self::ExpiresAfterCaller => "a child can't expire after its parent's token",
            Self::OwnerDiffers => "a child's owner is its parent record's",
            Self::ChainDiffers => {
                "a child's chain is its parent record's followed by the parent's copy"
            }
            Self::ExpiresAfterParent => "a child can't expire after its parent record",
        })
    }
}

/// Why a delegated caller's lineage can't be vouched for (ADR-0047, point 5.9, I6).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum Forgery {
    /// No delegation with the caller's token ID is recorded.
    Unrecorded,
    /// The caller's delegation is recorded but no longer live.
    NotLive,
    /// The claims the caller presented differ from its record in this field.
    Differs {
        /// The first field that differs, in the claims' order.
        field: ClaimField,
    },
}

impl fmt::Display for Forgery {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unrecorded => f.write_str("its delegation isn't recorded"),
            Self::NotLive => f.write_str("its delegation is no longer live"),
            Self::Differs { field } => write!(f, "its {field} differs from its record"),
        }
    }
}

/// A claim of a token, as a forged-lineage refusal names it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum ClaimField {
    /// `iss`.
    Fleet,
    /// `sub`.
    Principal,
    /// `jti`.
    Id,
    /// `iat`.
    IssuedAt,
    /// `nbf`.
    NotBefore,
    /// `exp`.
    ExpiresAt,
    /// `fleetring.scopes`.
    Scopes,
    /// `fleetring.kind`.
    Kind,
    /// `fleetring.owner`.
    Owner,
    /// `fleetring.parent`.
    Parent,
    /// `fleetring.chain`.
    Chain,
}

impl ClaimField {
    /// The first field, in the claims' order, in which `presented` differs from `recorded`.
    #[must_use]
    pub fn first_difference(recorded: &TokenClaims, presented: &TokenClaims) -> Option<Self> {
        let TokenClaims {
            fleet,
            principal,
            id,
            issued_at,
            not_before,
            expires_at,
            scopes,
            kind,
            owner,
            parent,
            chain,
        } = recorded;
        [
            (*fleet != presented.fleet, Self::Fleet),
            (*principal != presented.principal, Self::Principal),
            (*id != presented.id, Self::Id),
            (*issued_at != presented.issued_at, Self::IssuedAt),
            (*not_before != presented.not_before, Self::NotBefore),
            (*expires_at != presented.expires_at, Self::ExpiresAt),
            (*scopes != presented.scopes, Self::Scopes),
            (*kind != presented.kind, Self::Kind),
            (*owner != presented.owner, Self::Owner),
            (*parent != presented.parent, Self::Parent),
            (*chain != presented.chain, Self::Chain),
        ]
        .into_iter()
        .find_map(|(differs, field)| differs.then_some(field))
    }
}

impl fmt::Display for ClaimField {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Fleet => "iss",
            Self::Principal => "sub",
            Self::Id => "jti",
            Self::IssuedAt => "iat",
            Self::NotBefore => "nbf",
            Self::ExpiresAt => "exp",
            Self::Scopes => "fleetring.scopes",
            Self::Kind => "fleetring.kind",
            Self::Owner => "fleetring.owner",
            Self::Parent => "fleetring.parent",
            Self::Chain => "fleetring.chain",
        })
    }
}
