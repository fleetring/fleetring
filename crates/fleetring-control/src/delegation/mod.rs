//! The lineage vocabulary of online delegation (ADR-0047): agent names, the claims a delegated
//! token carries, its chain of ancestors, who signed it, and the agent tier.
//!
//! A parent agent asks the control plane for a child's token; the control plane records the
//! delegation and its lineage, then signs the child's token with its delegation issuer's key.
//! Nodes verifying a token, the stream recheck, the control plane's handler, and
//! [`apply`](crate::apply) all decide with the functions here, so no two of them can read a name,
//! a chain, or a scope differently (the plan's principle P1):
//!
//! - [`AgentName`]: `agent-<id>`, a root agent, or `agent-<id>` followed by one to
//!   [`MAX_DEPTH`] `.<segment>`, a delegated agent; with
//!   [`PrincipalName::is_at_or_below`](crate::PrincipalName::is_at_or_below), the comparison a
//!   principal revocation and the connection rule use.
//! - [`TokenClaims`]: every claim of a token but its key ID and its issuer certificate, with the
//!   [`ChainLink`]s naming its ancestors, root first, and the [`DelegationId`] that is a
//!   delegated token's ID.
//! - [`SignerKind`]: whether the authority or a delegation issuer signed a copy, which decides
//!   how a principal revocation judges it.
//! - [`TokenClaims::check_issuer_signed`]: ADR-0047 point 3.1 to 3.8, the structure of every
//!   issuer-signed token, which nodes, the signer, and `apply` share; with
//!   [`TokenClaims::check_recordable`], what `apply` adds before it records a delegation.
//! - [`tier::check`]: ADR-0047 point 3.4 to 3.7, the scopes an issuer may sign at all, decided
//!   with the scope lattice ([`crate::scope`]) rather than text.
//! - [`Revocations::refuses_token`](crate::Revocations::refuses_token): whether the denylist
//!   refuses a token, by its own ID and key and by every ID and key in its chain.
//!
//! Control state records each delegation ([`Delegation`]) and the operation that made it
//! ([`DelegationOperation`]), and counts delegations in rate windows ([`RateKey`]), each bounded
//! as ADR-0047 points 5.10 and 6.3 say, and pruned in `apply` and by
//! [`Tick`](crate::Action::Tick) ([`tick_due`]).
//!
//! Revoking any ancestor ends the subtree below it (ADR-0047, point 11): an agent revokes the
//! token it presented, itself, or what it or its descendants delegated, under the ancestor rule
//! ([`AncestorTarget`]), within the cap and rates of point 11.6; a principal revocation replaces
//! the entries below it that it covers, and every revocation prunes the records it ends.
//!
//! Everything here is pure: no clock, randomness, or I/O. Times arrive as arguments.

#![deny(clippy::wildcard_enum_match_arm)]

pub(crate) mod apply;
mod claims;
mod limits;
mod name;
pub(crate) mod prune;
mod record;
pub(crate) mod revoke;
pub mod tier;

pub use self::claims::{
    ChainLink, ClaimsViolation, DelegationId, MAX_DELEGATED_LIFETIME, MAX_NOT_BEFORE_LEAD,
    ParseDelegationIdError, SignerKind, TokenClaims, TokenKind,
};
pub use self::limits::{
    ANCESTOR_CALLER_RATE, ANCESTOR_LINEAGE_RATE, CALLER_RATE, LINEAGE_RATE,
    MAX_ANCESTOR_ENTRIES_PER_LINEAGE, MAX_LIVE_PER_LINEAGE, MAX_RECORDS_PER_LINEAGE,
    OPERATION_MARGIN, RATE_WINDOW, RECORD_MARGIN, REVOCATION_MARGIN, ROOT_PRINCIPAL_LIFE,
    TOMBSTONE_LIFE,
};
pub use self::name::{AgentName, MAX_DEPTH, ParseAgentNameError};
pub use self::prune::tick_due;
pub use self::record::{
    AncestorTarget, AncestorViolation, CallerToken, CanonicalRequest, ClaimField, Delegation,
    DelegationOperation, Forgery, Limit, OperationOutcome, RateKey, RateKind, RateScope,
    RequestField, Resent, RevocationLimit, StructureViolation,
};
