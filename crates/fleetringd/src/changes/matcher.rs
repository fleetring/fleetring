//! Which databases record their row changes, and how much of each row (ADR-0024).

use fleetring_control::DatabaseName;
use fleetring_control::matcher::DatabaseMatchers;

use crate::engine::CaptureMode;

/// Which databases record their row changes, and how much of each row (ADR-0024).
///
/// Each rule pairs the databases some logical sink names with the [`CaptureMode`] it asks for. A
/// database named by several rules records in the strongest mode among them, so that every sink
/// gets at least what it asked for.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct CaptureRules {
    rules: Vec<(DatabaseMatchers, CaptureMode)>,
}

impl CaptureRules {
    /// Rules that record no database.
    #[must_use]
    pub fn none() -> Self {
        Self::default()
    }

    /// The set of `rules`.
    #[must_use]
    pub fn new(rules: impl IntoIterator<Item = (DatabaseMatchers, CaptureMode)>) -> Self {
        Self {
            rules: rules.into_iter().collect(),
        }
    }

    /// Every database that `databases` names, recorded in `mode`.
    #[must_use]
    pub fn every(databases: DatabaseMatchers, mode: CaptureMode) -> Self {
        Self::new([(databases, mode)])
    }

    /// The mode `name` records in: the strongest any rule naming it asks for, or `None` if no
    /// rule names it.
    #[must_use]
    pub fn mode_for(&self, name: &DatabaseName) -> Option<CaptureMode> {
        self.rules
            .iter()
            .filter(|(databases, _)| databases.matches(name))
            .map(|(_, mode)| *mode)
            .max()
    }

    /// Whether there are no rules.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.rules.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_database_records_in_the_strongest_mode_any_rule_asks_for() {
        let rules = CaptureRules::new([
            (
                DatabaseMatchers::new(["tenant:*".parse().expect("a prefix")]),
                CaptureMode::Id,
            ),
            (
                DatabaseMatchers::new(["tenant:acme".parse().expect("a name")]),
                CaptureMode::Full,
            ),
            (
                DatabaseMatchers::new(["ledger".parse().expect("a name")]),
                CaptureMode::After,
            ),
        ]);
        let name = |text: &str| text.parse::<DatabaseName>().expect("a database name");
        assert_eq!(
            rules.mode_for(&name("tenant:acme")),
            Some(CaptureMode::Full)
        );
        assert_eq!(rules.mode_for(&name("tenant:zed")), Some(CaptureMode::Id));
        assert_eq!(rules.mode_for(&name("ledger")), Some(CaptureMode::After));
        assert_eq!(rules.mode_for(&name("other")), None);
        assert!(CaptureRules::none().is_empty());
    }
}
