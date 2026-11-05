//! Database metadata as clients send and read it: descriptions, labels, and the filters a
//! listing takes (ADR-0041).

use std::fmt;
use std::str::FromStr;

use fleetring_control::{
    DatabaseName, Description, DescriptionChange, FORBIDDEN_FORMAT_CHARACTERS, LabelKey,
    LabelValue, Labels, MAX_LABELS, MetadataChange, ParseLabelError, Timestamp, parse_label,
};

use crate::data::client::Location;

/// Whether `character` is one a person can't see where it is printed: a format character
/// (Unicode `Cf`, by the fixed table a description refuses, which takes in the bidirectional
/// controls and the zero-width characters) or the line or paragraph separator. A new database
/// name refuses them, and text output escapes them (ADR-0041, amendment 5).
pub(crate) fn is_invisible(character: char) -> bool {
    matches!(character, '\u{2028}' | '\u{2029}')
        || FORBIDDEN_FORMAT_CHARACTERS
            .iter()
            .any(|(first, last)| (*first..=*last).contains(&character))
}

/// Whether a listing includes each database's description text. Listings leave it out by
/// default, since a kibibyte times many databases adds up.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum Descriptions {
    /// Includes the text.
    Include,
    /// Leaves the text out; the rest of the metadata is still there.
    #[default]
    Omit,
}

/// One label requirement of a listing: a key with a value, or a key with any value.
///
/// # Examples
///
/// ```
/// use fleetring_control::{Labels, parse_label};
/// use fleetringd::data::LabelRequirement;
///
/// let labels = Labels::try_from_pairs([parse_label("purpose=memory")?])?;
/// let memory: LabelRequirement = "purpose=memory".parse()?;
/// let any_purpose: LabelRequirement = "purpose".parse()?;
/// let task: LabelRequirement = "purpose=task".parse()?;
/// assert!(memory.matches(&labels) && any_purpose.matches(&labels));
/// assert!(!task.matches(&labels));
/// assert_eq!(any_purpose.to_string(), "purpose");
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum LabelRequirement {
    /// `key=value`: the label is there with this value.
    Equals(LabelKey, LabelValue),
    /// `key`: the label is there, with any value.
    Exists(LabelKey),
}

impl LabelRequirement {
    /// Whether `labels` meet the requirement.
    #[must_use]
    pub fn matches(&self, labels: &Labels) -> bool {
        match self {
            Self::Equals(key, value) => labels.get(key) == Some(value),
            Self::Exists(key) => labels.get(key).is_some(),
        }
    }

    /// Whether a label list as a reply carries it, `(key, value)` pairs, meets the requirement.
    fn matches_pairs(&self, labels: &[(String, String)]) -> bool {
        let (key, value) = match self {
            Self::Equals(key, value) => (key, Some(value)),
            Self::Exists(key) => (key, None),
        };
        labels.iter().any(|(k, v)| {
            k.as_str() == key.as_str() && value.is_none_or(|value| v.as_str() == value.as_str())
        })
    }
}

impl FromStr for LabelRequirement {
    type Err = ParseLabelError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        if text.contains('=') {
            let (key, value) = parse_label(text)?;
            Ok(Self::Equals(key, value))
        } else {
            Ok(Self::Exists(text.parse()?))
        }
    }
}

impl fmt::Display for LabelRequirement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Equals(key, value) => write!(f, "{key}={value}"),
            Self::Exists(key) => write!(f, "{key}"),
        }
    }
}

/// What a creation records besides placement and durability: a description, labels, or both.
/// The control node records the creator itself, from the caller.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct NewMetadata {
    /// The description, if any.
    pub description: Option<Description>,
    /// The labels.
    pub labels: Labels,
}

impl NewMetadata {
    /// Whether the creation asks for no metadata, so a plain creation does.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.description.is_none() && self.labels.is_empty()
    }
}

/// A database's metadata as the control plane reports it. The text is the control node's,
/// unvalidated here: a newer control node may accept what this build doesn't.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct MetadataView {
    /// The description, or `None` if the database has none or a listing left it out.
    pub description: Option<String>,
    /// The principal that last set the description, or `None` if nobody can be named.
    pub description_by: Option<String>,
    /// The labels, sorted by key.
    pub labels: Vec<(String, String)>,
    /// The principal that created the database, if the creation recorded one.
    pub created_by: Option<String>,
    /// When the database was created, if the creation recorded metadata.
    pub created_at: Option<Timestamp>,
    /// When the database expires, if it has an expiry.
    pub expires_at: Option<Timestamp>,
    /// The metadata's version, which `--if-version` names.
    pub version: u64,
}

impl MetadataView {
    /// The value of the label `key`, if the database has it.
    #[must_use]
    pub fn label(&self, key: &str) -> Option<&str> {
        self.labels
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, value)| value.as_str())
    }
}

/// What `db restore` sets on a restored database from the metadata the archive holds
/// (ADR-0041, point 10).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct RestoredMetadata {
    /// The patch to apply with [`Client::update_database`](crate::data::Client::update_database),
    /// empty when there is nothing to set.
    pub change: MetadataChange,
    /// What of the archive's description and labels was left out because it isn't valid, with
    /// why: the archive is data from outside the control plane, and is checked as a request is.
    pub skipped: Vec<String>,
}

/// The patch that restores the description and labels of `archived` onto a database that was
/// created with `flags`.
///
/// - **The description** is set *without an author*, so no principal is recorded for it and
///   every reader treats it as untrusted, whoever restored it. It is left out if the restore
///   was given a description (`flags`), which the creation recorded as the caller's own.
/// - **Labels** are all set, except those `flags` already gave: a flag wins per key.
/// - **The expiry** is never restored, and neither are the archive's creator, author, or
///   version: they are the archive's claims.
///
/// A description or label that wouldn't be accepted if a client sent it is skipped and named
/// in [`RestoredMetadata::skipped`], not set.
#[must_use]
pub fn restored_metadata(archived: &MetadataView, flags: &NewMetadata) -> RestoredMetadata {
    let mut restored = RestoredMetadata::default();
    if flags.description.is_none()
        && let Some(text) = &archived.description
    {
        match text.parse::<Description>() {
            Ok(description) => {
                restored.change.description = DescriptionChange::SetWithoutAuthor(description);
            }
            Err(error) => restored
                .skipped
                .push(format!("the archived description: {error}")),
        }
    }
    let mut labels = Vec::new();
    for (key, value) in &archived.labels {
        match (key.parse::<LabelKey>(), value.parse::<LabelValue>()) {
            (Ok(key), Ok(value)) => {
                if flags.labels.get(&key).is_none() {
                    labels.push((key, value));
                }
            }
            (Err(error), _) | (_, Err(error)) => restored
                .skipped
                .push(format!("the archived label {key}={value}: {error}")),
        }
    }
    if flags.labels.len() + labels.len() > MAX_LABELS {
        restored.skipped.push(format!(
            "the {} archived labels: with the {} given, a database has at most {MAX_LABELS}",
            labels.len(),
            flags.labels.len()
        ));
    } else if let Ok(labels) = Labels::try_from_pairs(labels) {
        restored.change.set_labels = labels;
    }
    restored
}

/// Which databases a listing returns, and how much of each.
///
/// # Examples
///
/// ```
/// use fleetringd::data::{Descriptions, ListFilter};
///
/// let filter = ListFilter {
///     labels: vec!["purpose=memory".parse()?],
///     prefix: Some("agent-42-".to_owned()),
///     limit: Some(100),
///     ..ListFilter::default()
/// };
/// assert_eq!(filter.descriptions, Descriptions::Omit);
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ListFilter {
    /// Label requirements, all of which must hold.
    pub labels: Vec<LabelRequirement>,
    /// Only databases whose names start with this.
    pub prefix: Option<String>,
    /// Whether to include description text.
    pub descriptions: Descriptions,
    /// Only databases after this one in name order: the `next_after` of an earlier page.
    pub after: Option<DatabaseName>,
    /// At most this many databases; `None` or 0 for no limit.
    pub limit: Option<u32>,
}

impl ListFilter {
    /// Applies the filter to `databases`, in name order, as a control node that predates the
    /// filters leaves it to the client; a reply already filtered passes unchanged.
    pub(crate) fn apply(&self, mut databases: Vec<Location>) -> Listing {
        databases.sort_by(|a, b| a.database.cmp(&b.database));
        databases.retain(|location| self.admits(location));
        if self.descriptions == Descriptions::Omit {
            for metadata in databases.iter_mut().filter_map(|l| l.metadata.as_mut()) {
                metadata.description = None;
            }
        }
        let limit = self
            .limit
            .filter(|limit| *limit > 0)
            .and_then(|limit| usize::try_from(limit).ok());
        let next_after = match limit {
            Some(limit) if databases.len() > limit => {
                databases.truncate(limit);
                databases.last().map(|location| location.database.clone())
            }
            _ => None,
        };
        Listing {
            databases,
            next_after,
        }
    }

    fn admits(&self, location: &Location) -> bool {
        let name = location.database.as_str();
        let labels = location
            .metadata
            .as_ref()
            .map_or(&[][..], |metadata| metadata.labels.as_slice());
        self.prefix
            .as_deref()
            .is_none_or(|prefix| name.starts_with(prefix))
            && self
                .labels
                .iter()
                .all(|requirement| requirement.matches_pairs(labels))
            && self
                .after
                .as_ref()
                .is_none_or(|after| name > after.as_str())
    }
}

/// One page of a listing.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct Listing {
    /// The databases, in name order.
    pub databases: Vec<Location>,
    /// The last name listed, when more databases follow: the next page's `after`.
    pub next_after: Option<DatabaseName>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn archived(description: Option<&str>, labels: &[(&str, &str)]) -> MetadataView {
        MetadataView {
            description: description.map(str::to_owned),
            description_by: Some("ops".to_owned()),
            labels: labels
                .iter()
                .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
                .collect(),
            created_by: Some("ops".to_owned()),
            created_at: Some(Timestamp::from_millis(10)),
            expires_at: Some(Timestamp::from_millis(20)),
            version: 9,
        }
    }

    fn flags(description: Option<&str>, labels: &[&str]) -> NewMetadata {
        NewMetadata {
            description: description.map(|text| text.parse().expect("a description")),
            labels: Labels::try_from_pairs(
                labels
                    .iter()
                    .map(|label| parse_label(label).expect("a label")),
            )
            .expect("few labels"),
        }
    }

    #[test]
    fn an_archived_description_is_set_without_an_author_and_labels_are_set() {
        let restored = restored_metadata(
            &archived(Some("Notes."), &[("agent", "7"), ("purpose", "memory")]),
            &NewMetadata::default(),
        );
        assert_eq!(
            restored.change.description,
            DescriptionChange::SetWithoutAuthor("Notes.".parse().expect("description"))
        );
        let keys: Vec<&str> = restored
            .change
            .set_labels
            .iter()
            .map(|(key, _)| key.as_str())
            .collect();
        assert_eq!(keys, ["agent", "purpose"]);
        // Nothing of the archive's claims about the expiry, creator, or author is a change.
        assert!(!restored.change.touches_expiry());
        assert!(restored.change.remove_labels.is_empty());
        assert!(restored.skipped.is_empty());
    }

    #[test]
    fn restore_flags_win_over_the_archive() {
        let restored = restored_metadata(
            &archived(Some("Archived."), &[("agent", "7"), ("purpose", "memory")]),
            &flags(Some("Mine."), &["purpose=scratch"]),
        );
        assert_eq!(restored.change.description, DescriptionChange::Keep);
        let labels: Vec<(&str, &str)> = restored
            .change
            .set_labels
            .iter()
            .map(|(key, value)| (key.as_str(), value.as_str()))
            .collect();
        assert_eq!(labels, [("agent", "7")], "the flag's value stays");
        assert!(restored.skipped.is_empty());

        let only_labels = restored_metadata(&archived(None, &[]), &flags(Some("Mine."), &[]));
        assert!(only_labels.change.is_empty());
    }

    #[test]
    fn invalid_archived_text_is_skipped_and_named() {
        let restored = restored_metadata(
            &archived(
                Some("clear\u{1b}[2J"),
                &[("good", "yes"), ("-bad", "x"), ("fleetring.dev/x", "y")],
            ),
            &NewMetadata::default(),
        );
        assert_eq!(restored.change.description, DescriptionChange::Keep);
        assert_eq!(restored.change.set_labels.len(), 1, "only `good` is valid");
        assert_eq!(restored.skipped.len(), 3, "{:?}", restored.skipped);
    }

    #[test]
    fn archived_labels_over_the_limit_with_the_flags_are_skipped_together() {
        let many: Vec<(String, String)> = (0..MAX_LABELS)
            .map(|n| (format!("k{n}"), "v".to_owned()))
            .collect();
        let borrowed: Vec<(&str, &str)> = many
            .iter()
            .map(|(key, value)| (key.as_str(), value.as_str()))
            .collect();
        let restored = restored_metadata(&archived(None, &borrowed), &flags(None, &["extra=1"]));
        assert!(restored.change.set_labels.is_empty());
        assert_eq!(restored.skipped.len(), 1, "{:?}", restored.skipped);
        // Without the flag they fit.
        let fits = restored_metadata(&archived(None, &borrowed), &NewMetadata::default());
        assert_eq!(fits.change.set_labels.len(), MAX_LABELS);
    }
}
