//! Authorization: principals, their scopes, and the bearer tokens that carry them (ADR-0025).
//!
//! A request comes from a node or from a principal. Nodes act with full scope towards each
//! other. A principal's [`Scopes`] say which keys it may read, write, administer, or stream, and
//! whether it may run key-less control operations (`cluster`); everything else is denied.

mod boundary;
mod denylist;
mod policy;
mod scope;
mod token;

pub use boundary::{
    AUTHORIZATION, AuthLayer, AuthService, Authorized, Authorizer, BoxStream, STREAM_RECHECK,
    authorized, require_key,
};
pub use denylist::Denylist;
pub use policy::{Caller, Credential, Denied, ForNodes, Needed, RPCS, Requirement, Rpc, rpc_at};
pub use scope::{Action, ParseScopeError, Scope, Scopes, Target};
pub use token::{
    BearerToken, DEFAULT_TOKEN_LIFETIME, Grant, IssueError, MAX_TOKEN_LEN, MAX_TOKEN_LIFETIME,
    NOT_BEFORE_LEEWAY_SECS, TokenError, TokenSigner, TokenVerifier, Verified, VerifyingKey,
};
