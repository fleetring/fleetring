//! Authorization: principals, their scopes, and the bearer tokens that carry them (ADR-0025).
//!
//! A request comes from a node or from a principal. Nodes make the node-to-node calls, each
//! bound to placement: a node acts only on databases placed on it, in the role control state
//! gives it there ([`Binding`], ADR-0031). A principal's [`Scopes`] say which databases it may
//! read, write, fork, administer, or stream, and whether it may run the control operations that
//! name no database (`fleet`); everything else is denied. A `grant` scope authorizes no request:
//! it bounds what an agent's token may delegate ([`Scopes::grant_step`]).
//!
//! Every `match` over an enum here lists its variants, so that a new action, caller, or peer
//! can't fall through a wildcard arm to a permissive one. ADR-0044 §3.6 names the example: the
//! `_ =>` arm of `resolve_identified` mapped any peer that is neither a node nor a principal to
//! `Caller::Anonymous`, which `Caller::authorize` lets make every call.
#![deny(clippy::wildcard_enum_match_arm)]

mod binding;
mod boundary;
mod denylist;
mod policy;
mod scope;
mod token;

pub use binding::{BINDING_KEY, Binding, BindingDenied, Unbound, refused_binding};
pub(crate) use boundary::Refusal;
pub use boundary::{
    AUTHORIZATION, AuthLayer, AuthService, Authorized, Authorizer, BoxStream, STREAM_RECHECK,
    authorized, require_database,
};
pub use denylist::Denylist;
pub use policy::{Caller, Credential, Denied, ForNodes, Needed, RPCS, Requirement, Rpc, rpc_at};
pub use scope::{
    Action, GrantStepError, MAX_SCOPES, ParseScopeError, Scope, Scopes, Target, TooManyScopes,
};
pub use token::{
    BearerToken, DEFAULT_TOKEN_LIFETIME, IssueError, MAX_TOKEN_LEN, MAX_TOKEN_LIFETIME,
    NOT_BEFORE_LEEWAY_SECS, TokenError, TokenSigner, TokenSpec, TokenVerifier, Verified,
    VerifyingKey,
};
