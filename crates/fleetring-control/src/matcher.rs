//! Which databases a sink streams: exact names, or prefixes ending in `*`.
//!
//! # Consensus code
//!
//! A scope's pattern is a [`DatabaseMatcher`], so [`apply`](crate::apply) decides with this module
//! through the scope lattice ([`crate::scope`]), and every control replica must parse and match
//! patterns identically (ADR-0047, point 6.5). Any change to how a pattern parses, prints, or
//! matches that changes whether `apply` accepts a command needs a fleet feature (ADR-0038,
//! point 4).

#![deny(clippy::wildcard_enum_match_arm)]

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Deserializer};

use crate::DatabaseName;

/// One rule naming databases: an exact name, or a prefix followed by `*`.
///
/// ```
/// use fleetring_control::matcher::DatabaseMatcher;
///
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let tenants: DatabaseMatcher = "tenant:*".parse()?;
/// assert!(tenants.matches(&"tenant:acme".parse()?));
/// assert!(!tenants.matches(&"agent:acme".parse()?));
/// let exact: DatabaseMatcher = "ledger".parse()?;
/// assert!(exact.matches(&"ledger".parse()?));
/// assert!(!exact.matches(&"ledger-2".parse()?));
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DatabaseMatcher {
    rule: Rule,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
enum Rule {
    Exact(String),
    Prefix(String),
}

impl DatabaseMatcher {
    /// Whether `name` is one of the databases the rule names.
    #[must_use]
    pub fn matches(&self, name: &DatabaseName) -> bool {
        match &self.rule {
            Rule::Exact(exact) => name.as_str() == exact,
            Rule::Prefix(prefix) => name.as_str().starts_with(prefix),
        }
    }

    /// The bytes a name must start with, for a rule ending in `*`; `None` for an exact name.
    #[must_use]
    pub fn prefix(&self) -> Option<&str> {
        match &self.rule {
            Rule::Exact(_) => None,
            Rule::Prefix(prefix) => Some(prefix),
        }
    }

    /// The rule matching every name that starts with `prefix`, which any bytes may be: the rule
    /// `"<prefix>*"` parses to.
    pub(crate) fn from_prefix(prefix: String) -> Self {
        Self {
            rule: Rule::Prefix(prefix),
        }
    }

    /// The rule as an exact name or a prefix, for comparing rules byte for byte.
    pub(crate) fn pattern(&self) -> Pattern<'_> {
        match &self.rule {
            Rule::Exact(exact) => Pattern::Exact(exact),
            Rule::Prefix(prefix) => Pattern::Prefix(prefix),
        }
    }
}

/// A rule's shape: the name it matches exactly, or the bytes every name it matches starts with
/// (`*` alone is `Prefix("")`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Pattern<'a> {
    /// Matches exactly this name.
    Exact(&'a str),
    /// Matches every name whose bytes start with these.
    Prefix(&'a str),
}

/// The error returned for a rule that names nothing.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("a database rule is an exact name or a prefix ending in `*`, not empty")]
pub struct ParseMatcherError;

impl FromStr for DatabaseMatcher {
    type Err = ParseMatcherError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        if text.is_empty() {
            return Err(ParseMatcherError);
        }
        let rule = match text.strip_suffix('*') {
            Some(prefix) => Rule::Prefix(prefix.to_owned()),
            None => Rule::Exact(text.to_owned()),
        };
        Ok(Self { rule })
    }
}

impl fmt::Display for DatabaseMatcher {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.rule {
            Rule::Exact(exact) => f.write_str(exact),
            Rule::Prefix(prefix) => write!(f, "{prefix}*"),
        }
    }
}

impl<'de> Deserialize<'de> for DatabaseMatcher {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let text = String::deserialize(deserializer)?;
        text.parse().map_err(serde::de::Error::custom)
    }
}

/// A set of rules; a database matches if any rule does.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct DatabaseMatchers {
    rules: Vec<DatabaseMatcher>,
}

impl DatabaseMatchers {
    /// Rules that match no database.
    #[must_use]
    pub fn none() -> Self {
        Self::default()
    }

    /// The set of `rules`.
    #[must_use]
    pub fn new(rules: impl IntoIterator<Item = DatabaseMatcher>) -> Self {
        Self {
            rules: rules.into_iter().collect(),
        }
    }

    /// Whether any rule names `name`.
    #[must_use]
    pub fn matches(&self, name: &DatabaseName) -> bool {
        self.rules.iter().any(|rule| rule.matches(name))
    }

    /// Whether there are no rules.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.rules.is_empty()
    }

    /// The rules, in order.
    #[must_use]
    pub fn rules(&self) -> &[DatabaseMatcher] {
        &self.rules
    }
}

impl Extend<DatabaseMatcher> for DatabaseMatchers {
    fn extend<I: IntoIterator<Item = DatabaseMatcher>>(&mut self, rules: I) {
        self.rules.extend(rules);
    }
}

impl FromIterator<DatabaseMatcher> for DatabaseMatchers {
    fn from_iter<I: IntoIterator<Item = DatabaseMatcher>>(rules: I) -> Self {
        Self::new(rules)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_star_alone_matches_everything_and_an_empty_rule_is_refused() {
        let all: DatabaseMatcher = "*".parse().expect("a star");
        assert!(all.matches(&"anything".parse().expect("name")));
        assert_eq!("".parse::<DatabaseMatcher>(), Err(ParseMatcherError));
        assert_eq!(
            "tenant:*"
                .parse::<DatabaseMatcher>()
                .map(|m| m.to_string())
                .as_deref(),
            Ok("tenant:*")
        );
    }

    #[test]
    fn a_rule_from_a_prefix_is_the_rule_its_text_parses_to() {
        for prefix in ["", "agent-42-", "agent-42.c7.", "*", "a*b", "tenant:é/"] {
            assert_eq!(
                Ok(DatabaseMatcher::from_prefix(prefix.to_owned())),
                format!("{prefix}*").parse(),
                "{prefix:?}"
            );
        }
    }
}
