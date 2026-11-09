//! Authorization: principals, their scopes, and the bearer tokens that carry them (ADR-0025).
//!
//! A request comes from a node or from a principal. Nodes make the node-to-node calls, each
//! bound to placement: a node acts only on databases placed on it, in the role control state
//! gives it there ([`Binding`], ADR-0031). A principal's [`Scopes`] say which databases it may
//! read, write, administer, or stream, and whether it may run the control operations that name
//! no database (`fleet`); everything else is denied.

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
pub use scope::{Action, ParseScopeError, Scope, Scopes, Target};
pub use token::{
    BearerToken, DEFAULT_TOKEN_LIFETIME, IssueError, MAX_TOKEN_LEN, MAX_TOKEN_LIFETIME,
    NOT_BEFORE_LEEWAY_SECS, TokenError, TokenSigner, TokenSpec, TokenVerifier, Verified,
    VerifyingKey,
};
