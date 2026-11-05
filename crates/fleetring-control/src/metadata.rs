//! Database metadata: a description, labels, a creator, and an expiry (ADR-0041).
//!
//! Every type here validates when it is built and when it is deserialized, so neither a request
//! nor a logged command can hold an invalid description, label, or principal name.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

use crate::Timestamp;

/// The longest [`Description`], in bytes.
pub const MAX_DESCRIPTION_BYTES: usize = 1024;

/// The most [`Labels`] a database may carry.
pub const MAX_LABELS: usize = 32;

/// The label key prefix reserved for FleetRing, with its subdomains: a client can't set a label
/// such as `fleetring.dev/x` or `ops.fleetring.dev/x`.
pub const RESERVED_LABEL_DOMAIN: &str = "fleetring.dev";

/// Unicode's format characters (general category `Cf`) as of Unicode 16, as inclusive ranges.
///
/// The table is frozen: decoding a log uses exactly this table forever, so that a log written
/// under it replays under every later release. A character a later Unicode version adds to `Cf`
/// is refused by the control service at the request boundary, never here (ADR-0041, point 1).
pub const FORBIDDEN_FORMAT_CHARACTERS: &[(char, char)] = &[
    ('\u{00AD}', '\u{00AD}'),
    ('\u{0600}', '\u{0605}'),
    ('\u{061C}', '\u{061C}'),
    ('\u{06DD}', '\u{06DD}'),
    ('\u{070F}', '\u{070F}'),
    ('\u{0890}', '\u{0891}'),
    ('\u{08E2}', '\u{08E2}'),
    ('\u{180E}', '\u{180E}'),
    ('\u{200B}', '\u{200F}'),
    ('\u{202A}', '\u{202E}'),
    ('\u{2060}', '\u{2064}'),
    ('\u{2066}', '\u{206F}'),
    ('\u{FEFF}', '\u{FEFF}'),
    ('\u{FFF9}', '\u{FFFB}'),
    ('\u{110BD}', '\u{110BD}'),
    ('\u{110CD}', '\u{110CD}'),
    ('\u{13430}', '\u{1343F}'),
    ('\u{1BCA0}', '\u{1BCA3}'),
    ('\u{1D173}', '\u{1D17A}'),
    ('\u{E0001}', '\u{E0001}'),
    ('\u{E0020}', '\u{E007F}'),
];

/// The longest label key prefix, in characters: a DNS subdomain's limit.
const MAX_LABEL_PREFIX_LEN: usize = 253;

/// The longest label key name or label value, in characters.
const MAX_LABEL_NAME_LEN: usize = 63;

/// The longest [`PrincipalName`], in bytes, as a certificate allows it.
const MAX_PRINCIPAL_NAME_LEN: usize = 64;

/// What a database holds and how it was set up: 1 to 1,024 bytes of UTF-8.
///
/// Tab and line feed are allowed. Refused are the other C0 controls, DEL, the C1 controls
/// (U+0080 to U+009F), every character of [`FORBIDDEN_FORMAT_CHARACTERS`] (Unicode's format
/// characters, the zero-width characters, bidirectional overrides, and tag characters among
/// them), and the line and paragraph separators U+2028 and U+2029. The text is stored as given,
/// without normalization.
///
/// # Examples
///
/// ```
/// use fleetring_control::{Description, ParseDescriptionError};
///
/// let description: Description = "Memory for agent 42.\nTables: facts, episodes.".parse()?;
/// assert!(description.as_str().starts_with("Memory"));
/// assert_eq!(
///     "hidden\u{200B}text".parse::<Description>(),
///     Err(ParseDescriptionError::ForbiddenCharacter { code: 0x200B, at: 6 }),
/// );
/// # Ok::<(), ParseDescriptionError>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct Description(Box<str>);

impl Description {
    /// Returns the description's text.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Whether a description refuses `character`.
fn is_forbidden_in_description(character: char) -> bool {
    match character {
        '\t' | '\n' => false,
        '\u{0}'..='\u{1F}' | '\u{7F}'..='\u{9F}' | '\u{2028}' | '\u{2029}' => true,
        _ => FORBIDDEN_FORMAT_CHARACTERS
            .iter()
            .any(|&(first, last)| (first..=last).contains(&character)),
    }
}

impl fmt::Display for Description {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl FromStr for Description {
    type Err = ParseDescriptionError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        Self::try_from(text.to_owned())
    }
}

impl TryFrom<String> for Description {
    type Error = ParseDescriptionError;

    fn try_from(text: String) -> Result<Self, Self::Error> {
        if text.is_empty() {
            return Err(ParseDescriptionError::Empty);
        }
        if text.len() > MAX_DESCRIPTION_BYTES {
            return Err(ParseDescriptionError::TooLong {
                len: text.len(),
                max: MAX_DESCRIPTION_BYTES,
            });
        }
        if let Some((at, character)) = text
            .char_indices()
            .find(|&(_, character)| is_forbidden_in_description(character))
        {
            return Err(ParseDescriptionError::ForbiddenCharacter {
                code: u32::from(character),
                at,
            });
        }
        Ok(Self(text.into_boxed_str()))
    }
}

impl From<Description> for String {
    fn from(description: Description) -> Self {
        description.0.into_string()
    }
}

/// Why a string is not a valid [`Description`].
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum ParseDescriptionError {
    /// The description is empty; an empty description is no description.
    #[error("description is empty")]
    Empty,

    /// The description is longer than [`MAX_DESCRIPTION_BYTES`].
    #[error("description is {len} bytes; the maximum is {max}")]
    TooLong {
        /// The description's length in bytes.
        len: usize,
        /// The maximum length in bytes.
        max: usize,
    },

    /// The description contains a character it refuses.
    #[error("description contains the character U+{code:04X} at byte {at}, which isn't allowed")]
    ForbiddenCharacter {
        /// The character's code point.
        code: u32,
        /// The character's byte offset in the description.
        at: usize,
    },
}

/// Whether `text` is a label key name or a non-empty label value: 1 to 63 characters of ASCII
/// letters, digits, `-`, `_`, and `.`, starting and ending with a letter or digit.
fn is_label_name(text: &str) -> bool {
    let bytes = text.as_bytes();
    match (bytes.first(), bytes.last()) {
        (Some(first), Some(last)) => {
            bytes.len() <= MAX_LABEL_NAME_LEN
                && first.is_ascii_alphanumeric()
                && last.is_ascii_alphanumeric()
                && bytes
                    .iter()
                    .all(|byte| byte.is_ascii_alphanumeric() || b"-_.".contains(byte))
        }
        _ => false,
    }
}

/// Whether `text` is a DNS subdomain: at most 253 characters in dot-separated parts of lowercase
/// letters, digits, and `-`, each starting and ending with a letter or digit.
fn is_dns_subdomain(text: &str) -> bool {
    !text.is_empty()
        && text.len() <= MAX_LABEL_PREFIX_LEN
        && text.split('.').all(|part| {
            let bytes = part.as_bytes();
            let alphanumeric = |byte: &u8| byte.is_ascii_lowercase() || byte.is_ascii_digit();
            match (bytes.first(), bytes.last()) {
                (Some(first), Some(last)) => {
                    alphanumeric(first)
                        && alphanumeric(last)
                        && bytes.iter().all(|byte| alphanumeric(byte) || *byte == b'-')
                }
                _ => false,
            }
        })
}

/// A label's key, by the Kubernetes rules: `[prefix/]name`.
///
/// - The optional prefix is a DNS subdomain: at most 253 characters of lowercase letters,
///   digits, `-`, and `.`, in dot-separated parts that start and end with a letter or digit. It
///   is neither [`RESERVED_LABEL_DOMAIN`] nor a subdomain of it.
/// - The name is 1 to 63 characters of ASCII letters, digits, `-`, `_`, and `.`, starting and
///   ending with a letter or digit.
///
/// Keys are case-sensitive and order by their bytes.
///
/// # Examples
///
/// ```
/// use fleetring_control::{LabelKey, ParseLabelError};
///
/// let key: LabelKey = "app.kubernetes.io/name".parse()?;
/// assert_eq!(key.as_str(), "app.kubernetes.io/name");
/// assert!(matches!(
///     "fleetring.dev/owner".parse::<LabelKey>(),
///     Err(ParseLabelError::ReservedPrefix { .. })
/// ));
/// # Ok::<(), ParseLabelError>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct LabelKey(Box<str>);

impl LabelKey {
    /// Returns the key as text.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for LabelKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl FromStr for LabelKey {
    type Err = ParseLabelError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        Self::try_from(text.to_owned())
    }
}

impl TryFrom<String> for LabelKey {
    type Error = ParseLabelError;

    fn try_from(text: String) -> Result<Self, Self::Error> {
        if text.is_empty() {
            return Err(ParseLabelError::EmptyKey);
        }
        let name = match text.split_once('/') {
            Some((prefix, name)) => {
                if !is_dns_subdomain(prefix) {
                    return Err(ParseLabelError::InvalidPrefix {
                        prefix: prefix.to_owned(),
                    });
                }
                let reserved = prefix
                    .strip_suffix(RESERVED_LABEL_DOMAIN)
                    .is_some_and(|rest| rest.is_empty() || rest.ends_with('.'));
                if reserved {
                    return Err(ParseLabelError::ReservedPrefix {
                        prefix: prefix.to_owned(),
                    });
                }
                name
            }
            None => &text,
        };
        if !is_label_name(name) {
            return Err(ParseLabelError::InvalidName {
                name: name.to_owned(),
            });
        }
        Ok(Self(text.into_boxed_str()))
    }
}

impl From<LabelKey> for String {
    fn from(key: LabelKey) -> Self {
        key.0.into_string()
    }
}

/// A label's value, by the Kubernetes rules: empty, or 1 to 63 characters of ASCII letters,
/// digits, `-`, `_`, and `.`, starting and ending with a letter or digit.
///
/// # Examples
///
/// ```
/// use fleetring_control::{LabelValue, ParseLabelError};
///
/// assert_eq!("memory".parse::<LabelValue>()?.as_str(), "memory");
/// assert!("".parse::<LabelValue>()?.as_str().is_empty());
/// assert!("not valid".parse::<LabelValue>().is_err());
/// # Ok::<(), ParseLabelError>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct LabelValue(Box<str>);

impl LabelValue {
    /// Returns the value as text.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for LabelValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl FromStr for LabelValue {
    type Err = ParseLabelError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        Self::try_from(text.to_owned())
    }
}

impl TryFrom<String> for LabelValue {
    type Error = ParseLabelError;

    fn try_from(text: String) -> Result<Self, Self::Error> {
        if !text.is_empty() && !is_label_name(&text) {
            return Err(ParseLabelError::InvalidValue { value: text });
        }
        Ok(Self(text.into_boxed_str()))
    }
}

impl From<LabelValue> for String {
    fn from(value: LabelValue) -> Self {
        value.0.into_string()
    }
}

/// Why a label, a label key, a label value, or a set of labels is not valid.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum ParseLabelError {
    /// The key is empty.
    #[error("label key is empty")]
    EmptyKey,

    /// The key's prefix isn't a DNS subdomain of at most 253 characters.
    #[error("label key prefix '{prefix}' isn't a DNS subdomain of at most 253 characters")]
    InvalidPrefix {
        /// The prefix.
        prefix: String,
    },

    /// The key's prefix is [`RESERVED_LABEL_DOMAIN`] or a subdomain of it.
    #[error("label key prefix '{prefix}' is reserved for FleetRing")]
    ReservedPrefix {
        /// The prefix.
        prefix: String,
    },

    /// The key's name breaks the name rules.
    #[error(
        "label key name '{name}' must be 1 to 63 characters: letters, digits, '-', '_', '.', \
         starting and ending with a letter or digit"
    )]
    InvalidName {
        /// The name.
        name: String,
    },

    /// The value breaks the value rules.
    #[error(
        "label value '{value}' must be at most 63 characters: letters, digits, '-', '_', '.', \
         starting and ending with a letter or digit"
    )]
    InvalidValue {
        /// The value.
        value: String,
    },

    /// The text isn't `key=value`.
    #[error("'{text}' isn't a label: expected key=value")]
    Shape {
        /// The text.
        text: String,
    },

    /// There are more than [`MAX_LABELS`] labels.
    #[error("{count} labels; a database has at most {max}")]
    TooMany {
        /// How many labels there are.
        count: usize,
        /// The most a database may carry.
        max: usize,
    },
}

/// Parses a label written `key=value`, splitting at the first `=`.
///
/// # Errors
///
/// [`ParseLabelError::Shape`] if the text has no `=`, and the key's or the value's error if
/// either is invalid.
///
/// # Examples
///
/// ```
/// use fleetring_control::parse_label;
///
/// let (key, value) = parse_label("purpose=memory")?;
/// assert_eq!((key.as_str(), value.as_str()), ("purpose", "memory"));
/// assert!(parse_label("purpose").is_err());
/// # Ok::<(), fleetring_control::ParseLabelError>(())
/// ```
pub fn parse_label(text: &str) -> Result<(LabelKey, LabelValue), ParseLabelError> {
    let Some((key, value)) = text.split_once('=') else {
        return Err(ParseLabelError::Shape {
            text: text.to_owned(),
        });
    };
    Ok((key.parse()?, value.parse()?))
}

/// A database's labels: at most [`MAX_LABELS`], in key order.
///
/// # Examples
///
/// ```
/// use fleetring_control::{Labels, parse_label};
///
/// let labels = Labels::try_from_pairs([parse_label("purpose=task")?, parse_label("agent=42")?])?;
/// let keys: Vec<&str> = labels.iter().map(|(key, _)| key.as_str()).collect();
/// assert_eq!(keys, ["agent", "purpose"]);
/// # Ok::<(), fleetring_control::ParseLabelError>(())
/// ```
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    try_from = "BTreeMap<LabelKey, LabelValue>",
    into = "BTreeMap<LabelKey, LabelValue>"
)]
pub struct Labels(BTreeMap<LabelKey, LabelValue>);

impl Labels {
    /// Returns no labels.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns how many labels there are.
    #[must_use]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Returns whether there are no labels.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Returns the value of the label `key`, if there is one.
    #[must_use]
    pub fn get(&self, key: &LabelKey) -> Option<&LabelValue> {
        self.0.get(key)
    }

    /// Returns the labels in key order.
    pub fn iter(&self) -> impl ExactSizeIterator<Item = (&LabelKey, &LabelValue)> {
        self.0.iter()
    }

    /// Builds labels from pairs; a repeated key keeps the last value.
    ///
    /// # Errors
    ///
    /// [`ParseLabelError::TooMany`] if there are more than [`MAX_LABELS`] distinct keys.
    pub fn try_from_pairs(
        pairs: impl IntoIterator<Item = (LabelKey, LabelValue)>,
    ) -> Result<Self, ParseLabelError> {
        Self::try_from(pairs.into_iter().collect::<BTreeMap<_, _>>())
    }

    /// Returns these labels without the keys in `remove`, then with every label of `set`,
    /// replacing the value of a key both hold.
    ///
    /// # Errors
    ///
    /// The number of labels the result would have, if that is more than [`MAX_LABELS`].
    pub(crate) fn patched(&self, remove: &BTreeSet<LabelKey>, set: &Self) -> Result<Self, usize> {
        let mut labels = self.0.clone();
        labels.retain(|key, _| !remove.contains(key));
        labels.extend(set.iter().map(|(key, value)| (key.clone(), value.clone())));
        if labels.len() > MAX_LABELS {
            return Err(labels.len());
        }
        Ok(Self(labels))
    }
}

impl TryFrom<BTreeMap<LabelKey, LabelValue>> for Labels {
    type Error = ParseLabelError;

    fn try_from(labels: BTreeMap<LabelKey, LabelValue>) -> Result<Self, Self::Error> {
        if labels.len() > MAX_LABELS {
            return Err(ParseLabelError::TooMany {
                count: labels.len(),
                max: MAX_LABELS,
            });
        }
        Ok(Self(labels))
    }
}

impl From<Labels> for BTreeMap<LabelKey, LabelValue> {
    fn from(labels: Labels) -> Self {
        labels.0
    }
}

impl<'labels> IntoIterator for &'labels Labels {
    type Item = (&'labels LabelKey, &'labels LabelValue);
    type IntoIter = std::collections::btree_map::Iter<'labels, LabelKey, LabelValue>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}

/// A principal's name, as a client certificate or a token's subject carries it: 1 to 64 bytes
/// of lowercase ASCII letters, digits, `-`, `_`, and `.`.
///
/// Control state records it and decides nothing by it: [`apply`](crate::apply) never compares
/// principals (ADR-0025, point 5).
///
/// # Examples
///
/// ```
/// use fleetring_control::PrincipalName;
///
/// let name: PrincipalName = "agent-42".parse()?;
/// assert_eq!(name.as_str(), "agent-42");
/// assert!("Agent 42".parse::<PrincipalName>().is_err());
/// # Ok::<(), fleetring_control::ParsePrincipalNameError>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct PrincipalName(Box<str>);

impl PrincipalName {
    /// Returns the name as text.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for PrincipalName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl FromStr for PrincipalName {
    type Err = ParsePrincipalNameError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        Self::try_from(text.to_owned())
    }
}

impl TryFrom<String> for PrincipalName {
    type Error = ParsePrincipalNameError;

    fn try_from(text: String) -> Result<Self, Self::Error> {
        if text.is_empty() {
            return Err(ParsePrincipalNameError::Empty);
        }
        if text.len() > MAX_PRINCIPAL_NAME_LEN {
            return Err(ParsePrincipalNameError::TooLong {
                len: text.len(),
                max: MAX_PRINCIPAL_NAME_LEN,
            });
        }
        if let Some((at, character)) = text.char_indices().find(|&(_, character)| {
            !(character.is_ascii_lowercase()
                || character.is_ascii_digit()
                || matches!(character, '-' | '_' | '.'))
        }) {
            return Err(ParsePrincipalNameError::InvalidCharacter {
                code: u32::from(character),
                at,
            });
        }
        Ok(Self(text.into_boxed_str()))
    }
}

impl From<PrincipalName> for String {
    fn from(name: PrincipalName) -> Self {
        name.0.into_string()
    }
}

/// Why a string is not a valid [`PrincipalName`].
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum ParsePrincipalNameError {
    /// The name is empty.
    #[error("principal name is empty")]
    Empty,

    /// The name is longer than 64 bytes.
    #[error("principal name is {len} bytes; the maximum is {max}")]
    TooLong {
        /// The name's length in bytes.
        len: usize,
        /// The maximum length in bytes.
        max: usize,
    },

    /// The name contains a character other than a lowercase ASCII letter, a digit, `-`, `_`,
    /// or `.`.
    #[error(
        "principal name contains the character U+{code:04X} at byte {at}; expected lowercase \
         letters, digits, '-', '_', or '.'"
    )]
    InvalidCharacter {
        /// The character's code point.
        code: u32,
        /// The character's byte offset in the name.
        at: usize,
    },
}

/// A database's metadata in control state (ADR-0041, point 1).
///
/// Only [`apply`](crate::apply) changes it: a creation that carries a [`MetadataSpec`] records
/// it, and every accepted update patches it and grows its version by one.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Metadata {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    description: Option<Description>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    description_by: Option<PrincipalName>,
    #[serde(default, skip_serializing_if = "Labels::is_empty")]
    labels: Labels,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    created_by: Option<PrincipalName>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    created_at: Option<Timestamp>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    expires_at: Option<Timestamp>,
    version: u64,
}

impl Metadata {
    /// The metadata a creation at `now` records from `spec`, at version 1. The description's
    /// author is the creator.
    pub(crate) fn created(spec: MetadataSpec, now: Timestamp) -> Self {
        let MetadataSpec {
            description,
            labels,
            expires_at,
            created_by,
        } = spec;
        Self {
            description_by: description.as_ref().and(created_by.clone()),
            description,
            labels,
            created_by,
            created_at: Some(now),
            expires_at,
            version: 1,
        }
    }

    /// The metadata a database created without any starts its first update from: no creator,
    /// no creation time, at version 0.
    pub(crate) fn empty() -> Self {
        Self {
            description: None,
            description_by: None,
            labels: Labels::new(),
            created_by: None,
            created_at: None,
            expires_at: None,
            version: 0,
        }
    }

    /// Returns the description, if the database has one.
    #[must_use]
    pub fn description(&self) -> Option<&Description> {
        self.description.as_ref()
    }

    /// Returns the principal that last set the description, or `None` when nobody can be named:
    /// an anonymous caller, or a description set without an author, as a restore sets one.
    #[must_use]
    pub fn description_by(&self) -> Option<&PrincipalName> {
        self.description_by.as_ref()
    }

    /// Returns the labels.
    #[must_use]
    pub fn labels(&self) -> &Labels {
        &self.labels
    }

    /// Returns the principal that created the database, if the creation recorded one.
    #[must_use]
    pub fn created_by(&self) -> Option<&PrincipalName> {
        self.created_by.as_ref()
    }

    /// Returns the creation command's time, if the creation recorded metadata.
    #[must_use]
    pub fn created_at(&self) -> Option<Timestamp> {
        self.created_at
    }

    /// Returns when the database expires, if it has an expiry.
    #[must_use]
    pub fn expires_at(&self) -> Option<Timestamp> {
        self.expires_at
    }

    /// Returns the metadata's version: 1 when the database first got metadata, and one more for
    /// every accepted update since.
    #[must_use]
    pub fn version(&self) -> u64 {
        self.version
    }

    pub(crate) fn set_labels(&mut self, labels: Labels) {
        self.labels = labels;
    }

    pub(crate) fn set_description(
        &mut self,
        description: Option<Description>,
        by: Option<PrincipalName>,
    ) {
        self.description = description;
        self.description_by = by;
    }

    pub(crate) fn set_expires_at(&mut self, expires_at: Option<Timestamp>) {
        self.expires_at = expires_at;
    }

    pub(crate) fn set_version(&mut self, version: u64) {
        self.version = version;
    }
}

/// What a creation records as the database's metadata.
///
/// # Examples
///
/// ```
/// use fleetring_control::{Labels, MetadataSpec, parse_label};
///
/// let spec = MetadataSpec {
///     description: Some("Scratch space for task 7.".parse()?),
///     labels: Labels::try_from_pairs([parse_label("purpose=task")?])?,
///     created_by: Some("agent-42".parse()?),
///     ..MetadataSpec::default()
/// };
/// assert!(spec.is_explicit());
/// let creator_only = MetadataSpec {
///     created_by: Some("agent-42".parse()?),
///     ..MetadataSpec::default()
/// };
/// assert!(!creator_only.is_explicit());
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetadataSpec {
    /// The description, if any. Its author is the creator.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<Description>,
    /// The labels.
    #[serde(default, skip_serializing_if = "Labels::is_empty")]
    pub labels: Labels,
    /// When the database expires, if it should. It must be later than the creation's time.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<Timestamp>,
    /// The principal creating the database, as the control node names the caller.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub created_by: Option<PrincipalName>,
}

impl MetadataSpec {
    /// Returns whether the creation asks for metadata itself: a description, labels, or an
    /// expiry, rather than only recording its creator.
    #[must_use]
    pub fn is_explicit(&self) -> bool {
        self.description.is_some() || !self.labels.is_empty() || self.expires_at.is_some()
    }
}

/// What an update does to a database's description.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum DescriptionChange {
    /// Leaves the description as it is.
    #[default]
    Keep,
    /// Sets the text; the update's principal is recorded as its author.
    Set(Description),
    /// Sets the text and clears its author, whoever makes the update: a restored description,
    /// which readers treat as untrusted.
    SetWithoutAuthor(Description),
    /// Removes the description and its author.
    Clear,
}

impl DescriptionChange {
    /// Returns whether the change leaves the description as it is.
    #[must_use]
    pub fn is_keep(&self) -> bool {
        matches!(self, Self::Keep)
    }
}

/// What an update does to a database's expiry.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum ExpiryChange {
    /// Leaves the expiry as it is.
    #[default]
    Keep,
    /// Sets the expiry, which must be later than the update's time.
    Set(Timestamp),
    /// Removes the expiry: the database never expires.
    Clear,
}

impl ExpiryChange {
    /// Returns whether the change leaves the expiry as it is.
    #[must_use]
    pub fn is_keep(&self) -> bool {
        matches!(self, Self::Keep)
    }
}

/// A patch to a database's metadata: what changes, and nothing else. Label removals apply
/// before label sets.
///
/// # Examples
///
/// ```
/// use std::collections::BTreeSet;
///
/// use fleetring_control::{ExpiryChange, Labels, MetadataChange, Timestamp, parse_label};
///
/// let change = MetadataChange {
///     set_labels: Labels::try_from_pairs([parse_label("purpose=memory")?])?,
///     remove_labels: BTreeSet::from(["task".parse()?]),
///     ..MetadataChange::default()
/// };
/// assert!(!change.is_empty() && !change.touches_expiry());
/// let extend = MetadataChange {
///     expiry: ExpiryChange::Set(Timestamp::from_millis(90_000)),
///     ..MetadataChange::default()
/// };
/// assert!(extend.touches_expiry());
/// assert!(MetadataChange::default().is_empty());
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetadataChange {
    /// What happens to the description.
    #[serde(default, skip_serializing_if = "DescriptionChange::is_keep")]
    pub description: DescriptionChange,
    /// Labels to set, replacing the value of a key the database already has.
    #[serde(default, skip_serializing_if = "Labels::is_empty")]
    pub set_labels: Labels,
    /// Label keys to remove; a key the database lacks is ignored.
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    pub remove_labels: BTreeSet<LabelKey>,
    /// What happens to the expiry.
    #[serde(default, skip_serializing_if = "ExpiryChange::is_keep")]
    pub expiry: ExpiryChange,
}

impl MetadataChange {
    /// Returns whether the patch changes nothing.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.description.is_keep()
            && self.set_labels.is_empty()
            && self.remove_labels.is_empty()
            && self.expiry.is_keep()
    }

    /// Returns whether the patch sets or clears the expiry.
    #[must_use]
    pub fn touches_expiry(&self) -> bool {
        !self.expiry.is_keep()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn description(text: &str) -> Result<Description, ParseDescriptionError> {
        text.parse()
    }

    #[test]
    fn descriptions_allow_tab_and_newline_and_refuse_other_controls() {
        assert!(description("one\ttwo\nthree").is_ok());
        for (text, code, at) in [
            ("a\rb", 0x0D, 1),
            ("\0", 0x00, 0),
            ("ab\x1b[2J", 0x1B, 2),
            ("abc\x7f", 0x7F, 3),
            ("é\u{85}", 0x85, 2),
            ("x\u{9F}", 0x9F, 1),
        ] {
            assert_eq!(
                description(text),
                Err(ParseDescriptionError::ForbiddenCharacter { code, at }),
                "{text:?}"
            );
        }
    }

    #[test]
    fn format_characters_and_separators_are_refused() {
        let mut refused: Vec<char> = FORBIDDEN_FORMAT_CHARACTERS
            .iter()
            .flat_map(|&(first, last)| [first, last])
            .collect();
        refused.extend(['\u{2028}', '\u{2029}']);
        // The invisible characters a reviewer can't see: zero-width characters and direction
        // marks, bidirectional overrides and isolates, word joiners and invisible operators, the
        // byte order mark, and tag characters.
        refused.extend([
            '\u{200B}',
            '\u{200C}',
            '\u{200D}',
            '\u{200E}',
            '\u{200F}',
            '\u{202A}',
            '\u{202E}',
            '\u{2060}',
            '\u{2061}',
            '\u{2062}',
            '\u{2063}',
            '\u{2064}',
            '\u{2066}',
            '\u{2069}',
            '\u{FEFF}',
            '\u{E0001}',
            '\u{E0020}',
            '\u{E0041}',
            '\u{E007F}',
        ]);
        for character in refused {
            let text = format!("ok{character}ok");
            assert_eq!(
                description(&text),
                Err(ParseDescriptionError::ForbiddenCharacter {
                    code: u32::from(character),
                    at: 2,
                }),
                "U+{:04X}",
                u32::from(character)
            );
        }
        for allowed in [
            '\u{00AC}',
            '\u{00AE}',
            '\u{200A}',
            '\u{2010}',
            '\u{2065}',
            '\u{FEFE}',
            '\u{E0000}',
            '\u{E0002}',
            '\u{E001F}',
            '\u{E0080}',
            '€',
            '日',
        ] {
            assert!(
                description(&format!("ok{allowed}ok")).is_ok(),
                "U+{:04X}",
                u32::from(allowed)
            );
        }
    }

    #[test]
    fn the_format_table_is_sorted_and_disjoint() {
        for pair in FORBIDDEN_FORMAT_CHARACTERS.windows(2) {
            let [(first, last), (next, _)] = pair else {
                unreachable!("windows of two")
            };
            assert!(first <= last && last < next, "{pair:?}");
        }
    }

    #[test]
    fn descriptions_are_at_most_1_kib() {
        assert!(description(&"a".repeat(1024)).is_ok());
        assert_eq!(
            description(&"a".repeat(1025)),
            Err(ParseDescriptionError::TooLong {
                len: 1025,
                max: 1024
            })
        );
        assert!(description(&"€".repeat(341)).is_ok());
        assert_eq!(
            description(&"€".repeat(342)),
            Err(ParseDescriptionError::TooLong {
                len: 1026,
                max: 1024
            })
        );
        assert_eq!(description(""), Err(ParseDescriptionError::Empty));
    }

    #[test]
    fn descriptions_are_checked_when_decoded() {
        let override_json = serde_json::to_string("a\u{202E}b").expect("a string serializes");
        let decoded: Result<Description, _> = serde_json::from_str(&override_json);
        assert!(decoded.is_err());
        let decoded: Description = serde_json::from_str(r#""a\tb""#).expect("valid");
        assert_eq!(decoded.as_str(), "a\tb");
    }

    #[test]
    fn label_keys_follow_kubernetes_rules() {
        let long_name = "a".repeat(63);
        let valid_prefix = format!("{}.com/a", "a".repeat(249));
        for valid in [
            "a",
            "purpose",
            "app.kubernetes.io/name",
            "example.com/A_b-c.d",
            long_name.as_str(),
            valid_prefix.as_str(),
            "x-1.y/Z",
        ] {
            assert!(valid.parse::<LabelKey>().is_ok(), "{valid}");
        }
        let too_long_name = "a".repeat(64);
        let too_long_prefix = format!("{}/a", "a".repeat(254));
        for (invalid, expected) in [
            ("", ParseLabelError::EmptyKey),
            (
                "-a",
                ParseLabelError::InvalidName {
                    name: "-a".to_owned(),
                },
            ),
            (
                "a-",
                ParseLabelError::InvalidName {
                    name: "a-".to_owned(),
                },
            ),
            (
                too_long_name.as_str(),
                ParseLabelError::InvalidName {
                    name: too_long_name.clone(),
                },
            ),
            (
                "Example.com/a",
                ParseLabelError::InvalidPrefix {
                    prefix: "Example.com".to_owned(),
                },
            ),
            (
                "a..b/c",
                ParseLabelError::InvalidPrefix {
                    prefix: "a..b".to_owned(),
                },
            ),
            (
                "-a.com/c",
                ParseLabelError::InvalidPrefix {
                    prefix: "-a.com".to_owned(),
                },
            ),
            (
                "/a",
                ParseLabelError::InvalidPrefix {
                    prefix: String::new(),
                },
            ),
            (
                "a/",
                ParseLabelError::InvalidName {
                    name: String::new(),
                },
            ),
            (
                "a/b/c",
                ParseLabelError::InvalidName {
                    name: "b/c".to_owned(),
                },
            ),
            (
                too_long_prefix.as_str(),
                ParseLabelError::InvalidPrefix {
                    prefix: "a".repeat(254),
                },
            ),
            (
                "a b",
                ParseLabelError::InvalidName {
                    name: "a b".to_owned(),
                },
            ),
            (
                "naïve",
                ParseLabelError::InvalidName {
                    name: "naïve".to_owned(),
                },
            ),
            (
                "a\u{200B}b",
                ParseLabelError::InvalidName {
                    name: "a\u{200B}b".to_owned(),
                },
            ),
        ] {
            assert_eq!(invalid.parse::<LabelKey>(), Err(expected), "{invalid:?}");
        }
    }

    #[test]
    fn label_values_follow_kubernetes_rules() {
        let long = "v".repeat(63);
        for valid in ["", "memory", "A_b-c.d", "42", long.as_str()] {
            assert!(valid.parse::<LabelValue>().is_ok(), "{valid:?}");
        }
        let too_long = "v".repeat(64);
        for invalid in [
            "x-",
            "-x",
            ".x",
            too_long.as_str(),
            "a b",
            "a/b",
            "a=b",
            "a\u{E0041}",
            "\u{FEFF}a",
        ] {
            assert_eq!(
                invalid.parse::<LabelValue>(),
                Err(ParseLabelError::InvalidValue {
                    value: invalid.to_owned()
                }),
                "{invalid:?}"
            );
        }
    }

    #[test]
    fn the_fleetring_dev_prefix_is_reserved() {
        for reserved in ["fleetring.dev/x", "ops.fleetring.dev/x"] {
            assert!(
                matches!(
                    reserved.parse::<LabelKey>(),
                    Err(ParseLabelError::ReservedPrefix { .. })
                ),
                "{reserved}"
            );
        }
        for allowed in [
            "notfleetring.dev/x",
            "fleetring.dev.example.com/x",
            "fleetring/x",
        ] {
            assert!(allowed.parse::<LabelKey>().is_ok(), "{allowed}");
        }
    }

    #[test]
    fn more_than_32_labels_are_refused_even_when_decoded() {
        let map = |count: usize| {
            let entries: Vec<String> = (0..count).map(|i| format!(r#""k{i}":"v""#)).collect();
            format!("{{{}}}", entries.join(","))
        };
        let labels: Labels = serde_json::from_str(&map(32)).expect("32 labels");
        assert_eq!(labels.len(), 32);
        assert!(serde_json::from_str::<Labels>(&map(33)).is_err());
        assert!(serde_json::from_str::<Labels>(r#"{"fleetring.dev/x":"v"}"#).is_err());
        assert!(serde_json::from_str::<Labels>(r#"{"k":"bad value"}"#).is_err());

        let pairs = (0..33).map(|i| parse_label(&format!("k{i}=v")).expect("a valid label"));
        assert_eq!(
            Labels::try_from_pairs(pairs),
            Err(ParseLabelError::TooMany { count: 33, max: 32 })
        );
    }

    #[test]
    fn a_repeated_key_keeps_the_last_value() {
        let labels = Labels::try_from_pairs([
            parse_label("purpose=task").expect("valid"),
            parse_label("purpose=memory").expect("valid"),
        ])
        .expect("one label");
        assert_eq!(labels.len(), 1);
        let key: LabelKey = "purpose".parse().expect("valid");
        assert_eq!(labels.get(&key).map(LabelValue::as_str), Some("memory"));
    }

    #[test]
    fn parse_label_splits_on_the_first_equals_sign() {
        assert_eq!(
            parse_label("a=b=c"),
            Err(ParseLabelError::InvalidValue {
                value: "b=c".to_owned()
            })
        );
        let (key, value) = parse_label("a=").expect("an empty value is valid");
        assert_eq!((key.as_str(), value.as_str()), ("a", ""));
        assert_eq!(
            parse_label("a"),
            Err(ParseLabelError::Shape {
                text: "a".to_owned()
            })
        );
        assert_eq!(parse_label("=b"), Err(ParseLabelError::EmptyKey));
    }

    #[test]
    fn principal_names_follow_the_certificate_grammar() {
        let longest = "p".repeat(64);
        for valid in ["agent-42", "a", "ops_team.eu", "0", longest.as_str()] {
            assert!(valid.parse::<PrincipalName>().is_ok(), "{valid}");
        }
        assert_eq!(
            "".parse::<PrincipalName>(),
            Err(ParsePrincipalNameError::Empty)
        );
        assert_eq!(
            "p".repeat(65).parse::<PrincipalName>(),
            Err(ParsePrincipalNameError::TooLong { len: 65, max: 64 })
        );
        for (invalid, code, at) in [
            ("Agent", u32::from('A'), 0),
            ("agent 42", u32::from(' '), 5),
            ("agent/42", u32::from('/'), 5),
            ("agent:42", u32::from(':'), 5),
            ("agent\u{200B}", 0x200B, 5),
        ] {
            assert_eq!(
                invalid.parse::<PrincipalName>(),
                Err(ParsePrincipalNameError::InvalidCharacter { code, at }),
                "{invalid:?}"
            );
        }
        assert!(serde_json::from_str::<PrincipalName>(r#""Root""#).is_err());
    }

    #[test]
    fn patches_remove_before_they_set_and_stop_at_32_labels() {
        let labels = Labels::try_from_pairs(
            (0..32).map(|i| parse_label(&format!("k{i}=v")).expect("valid")),
        )
        .expect("32 labels");
        let one = |text: &str| Labels::try_from_pairs([parse_label(text).expect("valid")]);
        let key = |text: &str| -> LabelKey { text.parse().expect("valid") };
        assert_eq!(
            labels.patched(&BTreeSet::new(), &one("new=v").expect("valid")),
            Err(33)
        );
        let swapped = labels
            .patched(&BTreeSet::from([key("k0")]), &one("new=v").expect("valid"))
            .expect("32 labels");
        assert_eq!(swapped.len(), 32);
        assert!(swapped.get(&key("k0")).is_none());
        let kept = labels
            .patched(&BTreeSet::from([key("k1")]), &one("k1=w").expect("valid"))
            .expect("32 labels");
        assert_eq!(kept.get(&key("k1")).map(LabelValue::as_str), Some("w"));
    }
}
