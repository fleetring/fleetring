//! Authorization: principals, their scopes, and the bearer tokens that carry them (ADR-0025).
//!
//! A request comes from a node or from a principal. Nodes act with full scope towards each
//! other. A principal's [`Scopes`] say which keys it may read, write, administer, or stream, and
//! whether it may run key-less control operations (`cluster`); everything else is denied.

mod scope;

pub use scope::{Action, ParseScopeError, Scope, Scopes, Target};
