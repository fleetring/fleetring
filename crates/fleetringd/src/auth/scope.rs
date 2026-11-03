//! Scopes: what a principal may do, as prefix rules on keys (ADR-0025).
//!
//! A scope is `<action>:<pattern>`, where the action is `read`, `write`, `admin`, or `stream` and
//! the pattern is an exact key or a prefix ending in `*`, the matcher the change sinks use. The
//! word `cluster` alone is the scope for control operations that name no key. `write` implies
//! `read`, `admin` implies both, and `stream` implies nothing and is implied by nothing, so a
//! change consumer can't read tables. Everything not allowed is denied.
//!
//! The policy is pure: [`Scopes::allows`] decides from the scopes and the request alone.

use std::fmt;
use std::str::FromStr;

use fleetring_control::DatabaseName;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::changes::DatabaseMatcher;

/// What a request does to a key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[non_exhaustive]
pub enum Action {
    /// Reads a database: queries, its placement, its lease.
    Read,
    /// Changes a database's contents. Implies [`Action::Read`].
    Write,
    /// Creates, drops, or reconfigures a database, or moves its lease. Implies
    /// [`Action::Write`] and [`Action::Read`].
    Admin,
    /// Follows a database's changes. Neither implies nor is implied by any other action.
    Stream,
}

impl Action {
    /// Every action, in the order of their names.
    pub const ALL: [Self; 4] = [Self::Admin, Self::Read, Self::Stream, Self::Write];

    /// Whether a scope for `self` also covers `wanted`.
    #[must_use]
    pub fn covers(self, wanted: Self) -> bool {
        match (self, wanted) {
            (Self::Admin, Self::Admin | Self::Write | Self::Read)
            | (Self::Write, Self::Write | Self::Read)
            | (Self::Read, Self::Read)
            | (Self::Stream, Self::Stream) => true,
            (Self::Admin | Self::Write | Self::Read, Self::Stream)
            | (Self::Write | Self::Read | Self::Stream, Self::Admin)
            | (Self::Read | Self::Stream, Self::Write)
            | (Self::Stream, Self::Read) => false,
        }
    }

    /// The action's name as scopes spell it.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Read => "read",
            Self::Write => "write",
            Self::Admin => "admin",
            Self::Stream => "stream",
        }
    }
}

impl fmt::Display for Action {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for Action {
    type Err = ParseScopeError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        match text {
            "read" => Ok(Self::Read),
            "write" => Ok(Self::Write),
            "admin" => Ok(Self::Admin),
            "stream" => Ok(Self::Stream),
            _ => Err(ParseScopeError::UnknownAction {
                action: text.to_owned(),
            }),
        }
    }
}

/// What a request touches: one key, or the cluster itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Target<'a> {
    /// The request acts on this key with this action.
    Key(Action, &'a DatabaseName),
    /// The request is a control operation that names no key: membership, draining, revocation.
    Cluster,
}

impl fmt::Display for Target<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Key(action, key) => write!(f, "{action} on '{key}'"),
            Self::Cluster => f.write_str("cluster"),
        }
    }
}

/// One rule: an action over the keys a pattern names, or `cluster`.
///
/// ```
/// use fleetringd::auth::{Action, Scope, Target};
///
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let scope: Scope = "write:tenant:acme/*".parse()?;
/// let key = "tenant:acme/orders".parse()?;
/// assert!(scope.allows(Target::Key(Action::Read, &key)));
/// assert!(scope.allows(Target::Key(Action::Write, &key)));
/// assert!(!scope.allows(Target::Key(Action::Admin, &key)));
/// assert!(!scope.allows(Target::Cluster));
/// assert_eq!(scope.to_string(), "write:tenant:acme/*");
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Scope {
    rule: Rule,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
enum Rule {
    Keys {
        action: Action,
        pattern: DatabaseMatcher,
    },
    Cluster,
}

/// The word that spells the cluster scope.
const CLUSTER: &str = "cluster";

impl Scope {
    /// The scope for `action` over the keys `pattern` names.
    #[must_use]
    pub fn new(action: Action, pattern: DatabaseMatcher) -> Self {
        Self {
            rule: Rule::Keys { action, pattern },
        }
    }

    /// The scope for control operations that name no key.
    #[must_use]
    pub fn cluster() -> Self {
        Self {
            rule: Rule::Cluster,
        }
    }

    /// Whether this scope alone allows `target`.
    #[must_use]
    pub fn allows(&self, target: Target<'_>) -> bool {
        match (&self.rule, target) {
            (Rule::Keys { action, pattern }, Target::Key(wanted, key)) => {
                action.covers(wanted) && pattern.matches(key)
            }
            (Rule::Cluster, Target::Cluster) => true,
            (Rule::Keys { .. }, Target::Cluster) | (Rule::Cluster, Target::Key(..)) => false,
        }
    }

    /// Whether this scope covers `wanted` on at least one key.
    #[must_use]
    pub fn covers_some(&self, wanted: Action) -> bool {
        match &self.rule {
            Rule::Keys { action, .. } => action.covers(wanted),
            Rule::Cluster => false,
        }
    }
}

impl fmt::Display for Scope {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.rule {
            Rule::Keys { action, pattern } => write!(f, "{action}:{pattern}"),
            Rule::Cluster => f.write_str(CLUSTER),
        }
    }
}

/// The error returned for text that isn't a scope.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum ParseScopeError {
    /// The text has no `:` and isn't `cluster`.
    #[error("a scope is `<action>:<pattern>` or `cluster`, not '{scope}'")]
    Shape {
        /// The text.
        scope: String,
    },
    /// The action isn't one of `read`, `write`, `admin`, or `stream`.
    #[error("unknown scope action '{action}'; expected read, write, admin, or stream")]
    UnknownAction {
        /// The action as given.
        action: String,
    },
    /// The pattern is empty.
    #[error("the scope '{scope}' has an empty pattern")]
    EmptyPattern {
        /// The text.
        scope: String,
    },
}

impl FromStr for Scope {
    type Err = ParseScopeError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        if text == CLUSTER {
            return Ok(Self::cluster());
        }
        let Some((action, pattern)) = text.split_once(':') else {
            return Err(ParseScopeError::Shape {
                scope: text.to_owned(),
            });
        };
        let action: Action = action.parse()?;
        let pattern: DatabaseMatcher =
            pattern.parse().map_err(|_| ParseScopeError::EmptyPattern {
                scope: text.to_owned(),
            })?;
        Ok(Self::new(action, pattern))
    }
}

impl Serialize for Scope {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for Scope {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let text = String::deserialize(deserializer)?;
        text.parse().map_err(serde::de::Error::custom)
    }
}

/// A principal's scopes: a request is allowed if any of them allows it, and denied otherwise.
///
/// ```
/// use fleetringd::auth::{Action, Scopes, Target};
///
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let scopes: Scopes = ["read:tenant:*", "stream:ledger"].iter().map(|s| s.parse()).collect::<Result<_, _>>()?;
/// let acme = "tenant:acme".parse()?;
/// let ledger = "ledger".parse()?;
/// assert!(scopes.allows(Target::Key(Action::Read, &acme)));
/// assert!(!scopes.allows(Target::Key(Action::Write, &acme)));
/// assert!(!scopes.allows(Target::Key(Action::Read, &ledger)));
/// assert!(scopes.allows(Target::Key(Action::Stream, &ledger)));
/// assert!(!scopes.allows(Target::Cluster));
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Scopes {
    scopes: Vec<Scope>,
}

impl Scopes {
    /// No scopes: every request is denied.
    #[must_use]
    pub fn none() -> Self {
        Self::default()
    }

    /// The set of `scopes`.
    #[must_use]
    pub fn new(scopes: impl IntoIterator<Item = Scope>) -> Self {
        Self {
            scopes: scopes.into_iter().collect(),
        }
    }

    /// Whether any scope allows `target`.
    #[must_use]
    pub fn allows(&self, target: Target<'_>) -> bool {
        self.scopes.iter().any(|scope| scope.allows(target))
    }

    /// Whether any scope covers `action` on at least one key, so that a listing filtered by
    /// `action` may show the caller something.
    #[must_use]
    pub fn covers_some(&self, action: Action) -> bool {
        self.scopes.iter().any(|scope| scope.covers_some(action))
    }

    /// The scopes, in order.
    #[must_use]
    pub fn as_slice(&self) -> &[Scope] {
        &self.scopes
    }

    /// Whether there are no scopes.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.scopes.is_empty()
    }

    /// How many scopes there are.
    #[must_use]
    pub fn len(&self) -> usize {
        self.scopes.len()
    }
}

impl FromIterator<Scope> for Scopes {
    fn from_iter<I: IntoIterator<Item = Scope>>(scopes: I) -> Self {
        Self::new(scopes)
    }
}

impl Extend<Scope> for Scopes {
    fn extend<I: IntoIterator<Item = Scope>>(&mut self, scopes: I) {
        self.scopes.extend(scopes);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(text: &str) -> DatabaseName {
        text.parse().expect("a database name")
    }

    fn scope(text: &str) -> Scope {
        text.parse()
            .unwrap_or_else(|e| panic!("'{text}' is a scope: {e}"))
    }

    fn scopes(texts: &[&str]) -> Scopes {
        texts.iter().map(|text| scope(text)).collect()
    }

    #[test]
    fn write_implies_read_admin_implies_both_and_stream_is_separate() {
        use Action::{Admin, Read, Stream, Write};
        let implied = [
            (Admin, vec![Admin, Read, Write]),
            (Read, vec![Read]),
            (Stream, vec![Stream]),
            (Write, vec![Read, Write]),
        ];
        for (held, covered) in implied {
            for wanted in Action::ALL {
                assert_eq!(
                    held.covers(wanted),
                    covered.contains(&wanted),
                    "{held} covers {wanted}"
                );
            }
        }
    }

    #[test]
    fn a_read_scope_on_a_prefix_reads_inside_it_and_nothing_else() {
        let reader = scopes(&["read:tenant:*"]);
        let acme = key("tenant:acme");
        assert!(reader.allows(Target::Key(Action::Read, &acme)));
        assert!(!reader.allows(Target::Key(Action::Write, &acme)));
        assert!(!reader.allows(Target::Key(Action::Admin, &acme)));
        assert!(!reader.allows(Target::Key(Action::Stream, &acme)));
        assert!(!reader.allows(Target::Key(Action::Read, &key("ledger"))));
        assert!(!reader.allows(Target::Cluster));
    }

    #[test]
    fn a_prefix_scope_is_a_string_prefix_and_an_exact_scope_names_one_key() {
        let acme = scopes(&["write:tenant:acme/*", "read:ledger"]);
        assert!(acme.allows(Target::Key(Action::Write, &key("tenant:acme/orders"))));
        assert!(acme.allows(Target::Key(Action::Write, &key("tenant:acme/"))));
        assert!(!acme.allows(Target::Key(Action::Read, &key("tenant:acme"))));
        assert!(!acme.allows(Target::Key(Action::Read, &key("tenant:acmeco/orders"))));
        assert!(acme.allows(Target::Key(Action::Read, &key("ledger"))));
        assert!(!acme.allows(Target::Key(Action::Read, &key("ledger-2"))));
        assert!(!acme.allows(Target::Key(Action::Write, &key("ledger"))));
    }

    #[test]
    fn admin_on_everything_still_lacks_stream_and_cluster() {
        let admin = scopes(&["admin:*"]);
        let any = key("anything");
        for action in [Action::Admin, Action::Write, Action::Read] {
            assert!(admin.allows(Target::Key(action, &any)), "{action}");
        }
        assert!(!admin.allows(Target::Key(Action::Stream, &any)));
        assert!(!admin.allows(Target::Cluster));
    }

    #[test]
    fn cluster_allows_key_less_operations_only() {
        let cluster = scopes(&["cluster"]);
        assert!(cluster.allows(Target::Cluster));
        for action in Action::ALL {
            assert!(!cluster.allows(Target::Key(action, &key("tenant:acme"))));
        }
        assert!(!cluster.covers_some(Action::Read));
    }

    #[test]
    fn no_scopes_deny_everything() {
        let none = Scopes::none();
        assert!(none.is_empty());
        assert!(!none.allows(Target::Cluster));
        for action in Action::ALL {
            assert!(!none.allows(Target::Key(action, &key("x"))));
            assert!(!none.covers_some(action));
        }
    }

    #[test]
    fn covers_some_follows_the_implications() {
        let writer = scopes(&["write:tenant:*"]);
        assert!(writer.covers_some(Action::Read));
        assert!(writer.covers_some(Action::Write));
        assert!(!writer.covers_some(Action::Stream));
        assert!(!writer.covers_some(Action::Admin));
    }

    #[test]
    fn scopes_parse_and_print_back() {
        for text in [
            "read:tenant:acme/*",
            "write:ledger",
            "admin:*",
            "stream:ledger",
            "cluster",
            "read:a:b:c",
        ] {
            assert_eq!(scope(text).to_string(), text);
        }
        assert_eq!(
            "read".parse::<Scope>(),
            Err(ParseScopeError::Shape {
                scope: "read".to_owned()
            })
        );
        assert_eq!(
            "delete:x".parse::<Scope>(),
            Err(ParseScopeError::UnknownAction {
                action: "delete".to_owned()
            })
        );
        assert_eq!(
            "read:".parse::<Scope>(),
            Err(ParseScopeError::EmptyPattern {
                scope: "read:".to_owned()
            })
        );
        assert!("cluster:x".parse::<Scope>().is_err());
        assert!("Read:x".parse::<Scope>().is_err());
        assert!("".parse::<Scope>().is_err());
    }

    #[test]
    fn scopes_round_trip_through_json_as_strings() {
        let set = scopes(&["read:tenant:*", "cluster"]);
        let json = serde_json::to_string(&set).expect("scopes serialize");
        assert_eq!(json, r#"["read:tenant:*","cluster"]"#);
        let back: Scopes = serde_json::from_str(&json).expect("scopes deserialize");
        assert_eq!(back, set);
        assert!(serde_json::from_str::<Scopes>(r#"["nope"]"#).is_err());
    }
}
