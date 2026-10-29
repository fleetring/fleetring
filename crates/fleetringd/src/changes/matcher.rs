//! Which databases a sink streams: exact names, or prefixes ending in `*`.

use std::fmt;
use std::str::FromStr;

use fleetring_control::DatabaseName;
use serde::{Deserialize, Deserializer};

/// One rule naming databases: an exact name, or a prefix followed by `*`.
///
/// ```
/// use fleetringd::changes::DatabaseMatcher;
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
}
