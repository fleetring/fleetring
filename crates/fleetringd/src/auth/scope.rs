//! Scopes: what a principal may do, as prefix rules on database names (ADR-0025).
//!
//! A scope is `<action>:<pattern>`, where the action is `read`, `write`, `admin`, or `stream` and
//! the pattern is an exact database name or a prefix ending in `*`, the matcher the change sinks
//! use. The word `fleet` alone is the scope for control operations that name no database.
//! `write` implies `read`, `admin` implies both, and `stream` implies nothing and is implied by
//! nothing, so a change consumer can't read tables. Everything not allowed is denied.
//!
//! The policy is pure: [`Scopes::allows`] decides from the scopes and the request alone.

use std::fmt;
use std::str::FromStr;

use fleetring_control::DatabaseName;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::changes::DatabaseMatcher;

/// What a request does to a database.
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

/// What a request touches: one database, or the fleet itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Target<'a> {
    /// The request acts on this database with this action.
    Database(Action, &'a DatabaseName),
    /// The request is a control operation that names no database: membership, draining,
    /// revocation.
    Fleet,
}

impl fmt::Display for Target<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Database(action, database) => write!(f, "{action} on '{database}'"),
            Self::Fleet => f.write_str("fleet"),
        }
    }
}

/// One rule: an action over the databases a pattern names, or `fleet`.
///
/// ```
/// use fleetringd::auth::{Action, Scope, Target};
///
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let scope: Scope = "write:tenant:acme/*".parse()?;
/// let database = "tenant:acme/orders".parse()?;
/// assert!(scope.allows(Target::Database(Action::Read, &database)));
/// assert!(scope.allows(Target::Database(Action::Write, &database)));
/// assert!(!scope.allows(Target::Database(Action::Admin, &database)));
/// assert!(!scope.allows(Target::Fleet));
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
    Databases {
        action: Action,
        pattern: DatabaseMatcher,
    },
    Fleet,
}

/// The word that spells the fleet scope.
const FLEET: &str = "fleet";

impl Scope {
    /// The scope for `action` over the databases `pattern` names.
    #[must_use]
    pub fn new(action: Action, pattern: DatabaseMatcher) -> Self {
        Self {
            rule: Rule::Databases { action, pattern },
        }
    }

    /// The scope for control operations that name no database.
    #[must_use]
    pub fn fleet() -> Self {
        Self { rule: Rule::Fleet }
    }

    /// Whether this scope alone allows `target`.
    #[must_use]
    pub fn allows(&self, target: Target<'_>) -> bool {
        match (&self.rule, target) {
            (Rule::Databases { action, pattern }, Target::Database(wanted, database)) => {
                action.covers(wanted) && pattern.matches(database)
            }
            (Rule::Fleet, Target::Fleet) => true,
            (Rule::Databases { .. }, Target::Fleet) | (Rule::Fleet, Target::Database(..)) => false,
        }
    }

    /// Whether this scope covers `wanted` on at least one database.
    #[must_use]
    pub fn covers_some(&self, wanted: Action) -> bool {
        match &self.rule {
            Rule::Databases { action, .. } => action.covers(wanted),
            Rule::Fleet => false,
        }
    }

    /// A warning for a prefix that ends without a separator, or `None` (ADR-0041, point 5).
    ///
    /// A pattern ending in `*` matches every name that starts with the bytes before it, so
    /// `admin:agent-42*` covers `agent-420` as well as `agent-42-notes`. The warning names such a
    /// database and the scope with a separator; a prefix that ends in anything but an ASCII
    /// letter or digit, an exact name, `*` alone, and `fleet` draw none.
    ///
    /// ```
    /// use fleetringd::auth::Scope;
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let loose: Scope = "admin:agent-42*".parse()?;
    /// let warning = loose.separator_warning().ok_or("a warning")?;
    /// assert!(warning.contains("agent-420"), "{warning}");
    /// let separated: Scope = "admin:agent-42-*".parse()?;
    /// assert_eq!(separated.separator_warning(), None);
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn separator_warning(&self) -> Option<String> {
        let Rule::Databases { action, pattern } = &self.rule else {
            return None;
        };
        let prefix = pattern.prefix()?;
        if !prefix.bytes().last()?.is_ascii_alphanumeric() {
            return None;
        }
        Some(format!(
            "the scope {self} also covers databases such as {prefix}0…; end the prefix with a \
             separator, such as {action}:{prefix}-*"
        ))
    }
}

impl fmt::Display for Scope {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.rule {
            Rule::Databases { action, pattern } => write!(f, "{action}:{pattern}"),
            Rule::Fleet => f.write_str(FLEET),
        }
    }
}

/// The error returned for text that isn't a scope.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum ParseScopeError {
    /// The text has no `:` and isn't `fleet`.
    #[error("a scope is `<action>:<pattern>` or `fleet`, not '{scope}'")]
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
    /// The scope is `fork` or `grant`, alone or as an action, which delegation will use
    /// (ADR-0043, point 8.11).
    #[error("'{word}' is reserved for delegation, which this release doesn't support")]
    Reserved {
        /// The reserved word.
        word: &'static str,
    },
}

/// The words reserved for delegation, as a bare scope and as an action (ADR-0043, point 8.11).
const RESERVED: [&str; 2] = ["fork", "grant"];

impl FromStr for Scope {
    type Err = ParseScopeError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        if text == FLEET {
            return Ok(Self::fleet());
        }
        let word = text.split_once(':').map_or(text, |(action, _)| action);
        if let Some(word) = RESERVED.into_iter().find(|reserved| *reserved == word) {
            return Err(ParseScopeError::Reserved { word });
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
/// assert!(scopes.allows(Target::Database(Action::Read, &acme)));
/// assert!(!scopes.allows(Target::Database(Action::Write, &acme)));
/// assert!(!scopes.allows(Target::Database(Action::Read, &ledger)));
/// assert!(scopes.allows(Target::Database(Action::Stream, &ledger)));
/// assert!(!scopes.allows(Target::Fleet));
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

    /// Whether any scope covers `action` on at least one database, so that a listing filtered by
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

    /// Each scope's [`Scope::separator_warning`], in order.
    pub fn separator_warnings(&self) -> impl Iterator<Item = String> + '_ {
        self.scopes.iter().filter_map(Scope::separator_warning)
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

    fn database(text: &str) -> DatabaseName {
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
    fn reserved_scope_words_are_refused() {
        for (text, word) in [
            ("fork", "fork"),
            ("grant", "grant"),
            ("fork:agent-42-*", "fork"),
            ("grant:agent-42.*", "grant"),
            ("grant:", "grant"),
        ] {
            let error = text.parse::<Scope>().expect_err(text);
            assert_eq!(error, ParseScopeError::Reserved { word }, "{text}");
            assert_eq!(
                error.to_string(),
                format!("'{word}' is reserved for delegation, which this release doesn't support")
            );
        }
        // A pattern may still hold the words; only the action is reserved.
        for text in ["read:fork", "admin:grant-*", "forks:x"] {
            assert!(
                !matches!(text.parse::<Scope>(), Err(ParseScopeError::Reserved { .. })),
                "{text}"
            );
        }
    }

    #[test]
    fn a_prefix_without_a_separator_draws_a_warning() {
        assert_eq!(
            scope("admin:agent-42*").separator_warning().as_deref(),
            Some(
                "the scope admin:agent-42* also covers databases such as agent-420…; end the \
                 prefix with a separator, such as admin:agent-42-*"
            )
        );
        assert!(scope("read:Tenant*").separator_warning().is_some());
        for quiet in [
            "admin:agent-42-*",
            "write:tenant:acme/*",
            "read:*",
            "admin:agent-42",
            "fleet",
        ] {
            assert_eq!(scope(quiet).separator_warning(), None, "{quiet}");
        }
        let warned: Vec<String> = scopes(&["admin:a*", "read:b-*", "write:c*"])
            .separator_warnings()
            .collect();
        assert_eq!(warned.len(), 2, "{warned:?}");
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
        let acme = database("tenant:acme");
        assert!(reader.allows(Target::Database(Action::Read, &acme)));
        assert!(!reader.allows(Target::Database(Action::Write, &acme)));
        assert!(!reader.allows(Target::Database(Action::Admin, &acme)));
        assert!(!reader.allows(Target::Database(Action::Stream, &acme)));
        assert!(!reader.allows(Target::Database(Action::Read, &database("ledger"))));
        assert!(!reader.allows(Target::Fleet));
    }

    #[test]
    fn a_prefix_scope_is_a_string_prefix_and_an_exact_scope_names_one_key() {
        let acme = scopes(&["write:tenant:acme/*", "read:ledger"]);
        assert!(acme.allows(Target::Database(
            Action::Write,
            &database("tenant:acme/orders")
        )));
        assert!(acme.allows(Target::Database(Action::Write, &database("tenant:acme/"))));
        assert!(!acme.allows(Target::Database(Action::Read, &database("tenant:acme"))));
        assert!(!acme.allows(Target::Database(
            Action::Read,
            &database("tenant:acmeco/orders")
        )));
        assert!(acme.allows(Target::Database(Action::Read, &database("ledger"))));
        assert!(!acme.allows(Target::Database(Action::Read, &database("ledger-2"))));
        assert!(!acme.allows(Target::Database(Action::Write, &database("ledger"))));
    }

    #[test]
    fn admin_on_everything_still_lacks_stream_and_fleet() {
        let admin = scopes(&["admin:*"]);
        let any = database("anything");
        for action in [Action::Admin, Action::Write, Action::Read] {
            assert!(admin.allows(Target::Database(action, &any)), "{action}");
        }
        assert!(!admin.allows(Target::Database(Action::Stream, &any)));
        assert!(!admin.allows(Target::Fleet));
    }

    #[test]
    fn fleet_allows_key_less_operations_only() {
        let fleet = scopes(&["fleet"]);
        assert!(fleet.allows(Target::Fleet));
        for action in Action::ALL {
            assert!(!fleet.allows(Target::Database(action, &database("tenant:acme"))));
        }
        assert!(!fleet.covers_some(Action::Read));
    }

    #[test]
    fn no_scopes_deny_everything() {
        let none = Scopes::none();
        assert!(none.is_empty());
        assert!(!none.allows(Target::Fleet));
        for action in Action::ALL {
            assert!(!none.allows(Target::Database(action, &database("x"))));
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
            "fleet",
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
        assert!("fleet:x".parse::<Scope>().is_err());
        // 0.2's spelling of the fleet scope is gone, and the error names the new one.
        let old = "cluster"
            .parse::<Scope>()
            .expect_err("cluster isn't a scope");
        assert_eq!(
            old,
            ParseScopeError::Shape {
                scope: "cluster".to_owned()
            }
        );
        assert!(old.to_string().contains("`fleet`"), "{old}");
        assert!("Read:x".parse::<Scope>().is_err());
        assert!("".parse::<Scope>().is_err());
    }

    #[test]
    fn scopes_round_trip_through_json_as_strings() {
        let set = scopes(&["read:tenant:*", "fleet"]);
        let json = serde_json::to_string(&set).expect("scopes serialize");
        assert_eq!(json, r#"["read:tenant:*","fleet"]"#);
        let back: Scopes = serde_json::from_str(&json).expect("scopes deserialize");
        assert_eq!(back, set);
        assert!(serde_json::from_str::<Scopes>(r#"["nope"]"#).is_err());
    }
}
