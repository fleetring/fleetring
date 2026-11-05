//! Database metadata as clients send and read it: descriptions, labels, and the filters a
//! listing takes (ADR-0041).

use std::fmt;
use std::str::FromStr;

use fleetring_control::{
    DatabaseName, Description, FORBIDDEN_FORMAT_CHARACTERS, LabelKey, LabelValue, Labels,
    ParseLabelError, Timestamp, parse_label,
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
