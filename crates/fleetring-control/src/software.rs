//! What each member runs: its release, the control-log format it reads, and the fleet features it
//! supports (ADR-0038).

use std::cmp::Ordering;
use std::collections::BTreeSet;
use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

/// A `fleetringd` release: `MAJOR.MINOR.PATCH` with an optional prerelease, such as `0.4.0-rc.1`.
///
/// Releases order as semantic versions do: by the three numbers, then a prerelease before the
/// release it precedes, then prerelease identifiers in turn, numeric ones numerically. Build
/// metadata (`+…`) is accepted and ignored.
///
/// # Examples
///
/// ```
/// use fleetring_control::Release;
///
/// let release: Release = "0.4.0-rc.1".parse()?;
/// assert_eq!(release.line(), (0, 4));
/// assert!(release < "0.4.0".parse()?);
/// assert!(release.is_prerelease());
/// assert_eq!(release.to_string(), "0.4.0-rc.1");
/// # Ok::<(), fleetring_control::ParseReleaseError>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct Release {
    major: u64,
    minor: u64,
    patch: u64,
    pre: Option<Box<str>>,
}

impl Release {
    /// Returns the final release `major.minor.patch`.
    #[must_use]
    pub const fn new(major: u64, minor: u64, patch: u64) -> Self {
        Self {
            major,
            minor,
            patch,
            pre: None,
        }
    }

    /// Returns the release line, `(MAJOR, MINOR)`, that the release belongs to.
    #[must_use]
    pub fn line(&self) -> (u64, u64) {
        (self.major, self.minor)
    }

    /// Returns whether this is a prerelease, such as a release candidate.
    #[must_use]
    pub fn is_prerelease(&self) -> bool {
        self.pre.is_some()
    }

    /// Returns whether a fleet may run this release and `other` at the same time: their release
    /// lines are the same, or adjacent within one major (ADR-0038, point 1).
    ///
    /// # Examples
    ///
    /// ```
    /// use fleetring_control::Release;
    ///
    /// let current: Release = "0.4.2".parse()?;
    /// assert!(current.runs_beside(&"0.3.0".parse()?));
    /// assert!(current.runs_beside(&"0.5.0-rc.1".parse()?));
    /// assert!(!current.runs_beside(&"0.6.0".parse()?));
    /// assert!(!current.runs_beside(&"1.4.2".parse()?));
    /// # Ok::<(), fleetring_control::ParseReleaseError>(())
    /// ```
    #[must_use]
    pub fn runs_beside(&self, other: &Self) -> bool {
        self.major == other.major && self.minor.abs_diff(other.minor) <= 1
    }
}

impl fmt::Display for Release {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}.{}", self.major, self.minor, self.patch)?;
        if let Some(pre) = &self.pre {
            write!(f, "-{pre}")?;
        }
        Ok(())
    }
}

impl Ord for Release {
    fn cmp(&self, other: &Self) -> Ordering {
        (self.major, self.minor, self.patch)
            .cmp(&(other.major, other.minor, other.patch))
            .then_with(|| match (&self.pre, &other.pre) {
                (None, None) => Ordering::Equal,
                (None, Some(_)) => Ordering::Greater,
                (Some(_), None) => Ordering::Less,
                (Some(a), Some(b)) => compare_prerelease(a, b),
            })
    }
}

impl PartialOrd for Release {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Compares prerelease strings identifier by identifier: numeric identifiers numerically and
/// before alphanumeric ones, the rest as ASCII, and a shorter list first when one is a prefix of
/// the other.
fn compare_prerelease(a: &str, b: &str) -> Ordering {
    let mut left = a.split('.');
    let mut right = b.split('.');
    loop {
        match (left.next(), right.next()) {
            (None, None) => return Ordering::Equal,
            (None, Some(_)) => return Ordering::Less,
            (Some(_), None) => return Ordering::Greater,
            (Some(x), Some(y)) => {
                let order = match (x.parse::<u64>(), y.parse::<u64>()) {
                    (Ok(x), Ok(y)) => x.cmp(&y),
                    (Ok(_), Err(_)) => Ordering::Less,
                    (Err(_), Ok(_)) => Ordering::Greater,
                    (Err(_), Err(_)) => x.cmp(y),
                };
                if order != Ordering::Equal {
                    return order;
                }
            }
        }
    }
}

impl FromStr for Release {
    type Err = ParseReleaseError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        let invalid = || ParseReleaseError {
            text: text.to_owned(),
        };
        // Build metadata never orders releases, so it is dropped.
        let text_without_build = text.split_once('+').map_or(text, |(release, _)| release);
        let (numbers, pre) = match text_without_build.split_once('-') {
            Some((numbers, pre)) => (numbers, Some(pre)),
            None => (text_without_build, None),
        };
        let mut parts = numbers.split('.');
        let mut number = || -> Result<u64, ParseReleaseError> {
            let part = parts.next().ok_or_else(invalid)?;
            // `01` isn't a version number; leading zeros would make two spellings equal.
            if part.is_empty()
                || !part.bytes().all(|byte| byte.is_ascii_digit())
                || (part.len() > 1 && part.starts_with('0'))
            {
                return Err(invalid());
            }
            part.parse().map_err(|_| invalid())
        };
        let (major, minor, patch) = (number()?, number()?, number()?);
        if parts.next().is_some() {
            return Err(invalid());
        }
        let pre = match pre {
            Some(pre)
                if !pre.is_empty()
                    && pre.split('.').all(|identifier| {
                        !identifier.is_empty()
                            && identifier
                                .bytes()
                                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
                    }) =>
            {
                Some(pre.into())
            }
            Some(_) => return Err(invalid()),
            None => None,
        };
        Ok(Self {
            major,
            minor,
            patch,
            pre,
        })
    }
}

impl TryFrom<String> for Release {
    type Error = ParseReleaseError;

    fn try_from(text: String) -> Result<Self, Self::Error> {
        text.parse()
    }
}

impl From<Release> for String {
    fn from(release: Release) -> Self {
        release.to_string()
    }
}

/// Why a string is not a [`Release`].
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("'{text}' isn't a release: expected MAJOR.MINOR.PATCH, such as 0.4.0 or 0.4.0-rc.1")]
pub struct ParseReleaseError {
    text: String,
}

/// A control command that not every member can read or act on, and so is refused until every
/// member supports it (ADR-0038, point 4).
///
/// The first command that needs a feature, accepted once every member reports it, enables it for
/// good; from then on a member that lacks it can't join.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum Feature {
    /// [`Action::Undrain`](crate::Action::Undrain): clearing a member's draining mark.
    Undrain,
    /// Database metadata (ADR-0041): an [`Action::CreateDatabase`](crate::Action::CreateDatabase)
    /// that carries a [`MetadataSpec`](crate::MetadataSpec), and every
    /// [`Action::UpdateDatabase`](crate::Action::UpdateDatabase) and
    /// [`Action::ExpireDatabase`](crate::Action::ExpireDatabase).
    Metadata,
    /// Database expiry (ADR-0041): a creation or an update that sets or changes an expiry, and
    /// [`Action::ExpireDatabase`](crate::Action::ExpireDatabase). Each of them needs
    /// [`Feature::Metadata`] too.
    Expiry,
}

impl Feature {
    /// Every feature this release knows, in order.
    pub const ALL: [Self; 3] = [Self::Undrain, Self::Metadata, Self::Expiry];

    /// Returns the feature's name, as members report it and errors print it.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Undrain => "undrain",
            Self::Metadata => "metadata",
            Self::Expiry => "expiry",
        }
    }
}

impl fmt::Display for Feature {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for Feature {
    type Err = ParseFeatureError;

    fn from_str(name: &str) -> Result<Self, Self::Err> {
        Self::ALL
            .into_iter()
            .find(|feature| feature.as_str() == name)
            .ok_or_else(|| ParseFeatureError {
                name: name.to_owned(),
            })
    }
}

/// Why a string names no [`Feature`] this release knows. A newer member may report one; it is
/// ignored.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("unknown fleet feature '{name}'")]
pub struct ParseFeatureError {
    name: String,
}

/// The fleet features the 0.5 release line decodes, and so the only names a stored [`Software`]
/// writes in its `features` field (ADR-0041, amendment 1).
///
/// 0.5 decodes `features` strictly: one name it doesn't know fails the whole control log or
/// snapshot, even before any command uses the feature. Every other name goes in
/// `newer_features`, which 0.5 ignores. Releases from 0.6 on decode both fields and keep names
/// they don't know, so this list is frozen at the 0.5 line's set and later lines never extend it.
const FEATURES_THE_0_5_LINE_DECODES: [Feature; 1] = [Feature::Undrain];

/// What a member runs: its release, the newest control-log format it reads, and the fleet
/// features it supports.
///
/// A member may also report feature names this release doesn't know, from a newer release. They
/// are kept, so that they survive the control log and snapshots, but this release never counts
/// them as supported.
///
/// # Examples
///
/// ```
/// use fleetring_control::{Feature, Release, Software};
///
/// let software = Software::new(Release::new(0, 4, 0), 1).with_feature(Feature::Undrain);
/// assert!(software.supports(Feature::Undrain));
/// assert!(!Software::baseline().supports(Feature::Undrain));
/// assert_eq!(Software::baseline().release(), &Release::new(0, 3, 0));
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(from = "SoftwareRecord", into = "SoftwareRecord")]
pub struct Software {
    release: Release,
    control_format: u32,
    features: BTreeSet<Feature>,
    unknown_features: BTreeSet<Box<str>>,
}

/// [`Software`] as the control log and snapshots store it.
///
/// `features` holds only names in [`FEATURES_THE_0_5_LINE_DECODES`], exactly as 0.5 wrote it,
/// and `newer_features` every other name, sorted. Both are read the same way: a name this
/// release knows is a feature, and any other is kept as an unknown name.
#[derive(Serialize, Deserialize)]
struct SoftwareRecord {
    release: Release,
    control_format: u32,
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    features: BTreeSet<String>,
    // Absent from states and logs written before 0.6.
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    newer_features: BTreeSet<String>,
}

impl From<SoftwareRecord> for Software {
    fn from(record: SoftwareRecord) -> Self {
        record.features.iter().chain(&record.newer_features).fold(
            Self::new(record.release, record.control_format),
            |software, name| software.with_feature_name(name),
        )
    }
}

impl From<Software> for SoftwareRecord {
    fn from(software: Software) -> Self {
        let (old, newer): (Vec<Feature>, Vec<Feature>) = software
            .features
            .into_iter()
            .partition(|feature| FEATURES_THE_0_5_LINE_DECODES.contains(feature));
        let name = |feature: Feature| feature.as_str().to_owned();
        Self {
            release: software.release,
            control_format: software.control_format,
            features: old.into_iter().map(name).collect(),
            newer_features: newer
                .into_iter()
                .map(name)
                .chain(software.unknown_features.into_iter().map(String::from))
                .collect(),
        }
    }
}

impl Software {
    /// The release a member that reports no software is taken to run (ADR-0038, point 2).
    pub const BASELINE_RELEASE: Release = Release::new(0, 3, 0);

    /// Describes a member on `release` that reads control logs up to `control_format`, with no
    /// features.
    #[must_use]
    pub fn new(release: Release, control_format: u32) -> Self {
        Self {
            release,
            control_format,
            features: BTreeSet::new(),
            unknown_features: BTreeSet::new(),
        }
    }

    /// What a member that reports nothing runs: 0.3.0, the first release after the 0.3.0 renames,
    /// with control-log format 1 and no features.
    #[must_use]
    pub fn baseline() -> Self {
        Self::new(Self::BASELINE_RELEASE, 1)
    }

    /// Adds `feature` to what the member supports.
    #[must_use]
    pub fn with_feature(mut self, feature: Feature) -> Self {
        self.features.insert(feature);
        self
    }

    /// Adds the feature named `name` to what the member reports: a feature this release knows,
    /// or else a name kept as given, as a newer member reports one, which never counts as
    /// supported here.
    ///
    /// # Examples
    ///
    /// ```
    /// use fleetring_control::{Feature, Release, Software};
    ///
    /// let software = Software::new(Release::new(0, 7, 0), 1)
    ///     .with_feature_name("metadata")
    ///     .with_feature_name("future-x");
    /// assert!(software.supports(Feature::Metadata));
    /// assert_eq!(software.unknown_features().collect::<Vec<_>>(), ["future-x"]);
    /// ```
    #[must_use]
    pub fn with_feature_name(mut self, name: &str) -> Self {
        match name.parse() {
            Ok(feature) => {
                self.features.insert(feature);
            }
            Err(ParseFeatureError { name }) => {
                self.unknown_features.insert(name.into_boxed_str());
            }
        }
        self
    }

    /// Returns the member's release.
    #[must_use]
    pub fn release(&self) -> &Release {
        &self.release
    }

    /// Returns the newest control-log format the member reads.
    #[must_use]
    pub fn control_format(&self) -> u32 {
        self.control_format
    }

    /// Returns the fleet features the member supports, in order.
    pub fn features(&self) -> impl ExactSizeIterator<Item = Feature> + '_ {
        self.features.iter().copied()
    }

    /// Returns whether the member supports `feature`.
    #[must_use]
    pub fn supports(&self, feature: Feature) -> bool {
        self.features.contains(&feature)
    }

    /// Returns the feature names the member reported that this release doesn't know, in order.
    ///
    /// A newer member reports them; they are kept so that they survive the control log and
    /// snapshots, and never count as supported features here.
    pub fn unknown_features(&self) -> impl ExactSizeIterator<Item = &str> {
        self.unknown_features.iter().map(AsRef::as_ref)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn release(text: &str) -> Release {
        text.parse().expect("a valid release")
    }

    #[test]
    fn releases_order_as_semantic_versions() {
        let ordered = [
            "0.3.0",
            "0.3.1",
            "0.4.0-alpha",
            "0.4.0-alpha.1",
            "0.4.0-alpha.beta",
            "0.4.0-beta.2",
            "0.4.0-beta.11",
            "0.4.0-rc.1",
            "0.4.0",
            "1.0.0",
        ];
        for pair in ordered.windows(2) {
            assert!(release(pair[0]) < release(pair[1]), "{pair:?}");
        }
        assert_eq!(release("0.4.0+build.5"), release("0.4.0"));
    }

    #[test]
    fn malformed_releases_are_refused() {
        for text in [
            "",
            "0",
            "0.4",
            "0.4.0.1",
            "v0.4.0",
            "0.04.0",
            "0.4.0-",
            "0.4.0-rc..1",
            "0.4.x",
            "0.4.0-r c",
        ] {
            assert!(text.parse::<Release>().is_err(), "{text}");
        }
    }

    #[test]
    fn the_window_is_two_adjacent_lines_within_a_major() {
        let current = release("0.4.0");
        assert!(current.runs_beside(&release("0.4.9")));
        assert!(current.runs_beside(&release("0.3.0")));
        assert!(current.runs_beside(&release("0.5.0")));
        assert!(!current.runs_beside(&release("0.2.0")));
        assert!(!current.runs_beside(&release("0.6.0")));
        assert!(!release("1.0.0").runs_beside(&release("0.9.0")));
    }

    #[test]
    fn stored_software_keeps_names_the_0_5_line_cannot_decode_out_of_features() {
        let software = Feature::ALL
            .into_iter()
            .fold(Software::new(release("0.6.0"), 1), Software::with_feature);
        let stored = serde_json::to_string(&software).expect("serializes");
        assert_eq!(
            stored,
            r#"{"release":"0.6.0","control_format":1,"features":["undrain"],"newer_features":["expiry","metadata"]}"#
        );
        assert_eq!(
            serde_json::from_str::<Software>(&stored).expect("decodes"),
            software
        );

        // Software of the 0.5 line, and of 0.3.0 with no features, is stored as it was.
        let old = Software::new(release("0.5.0"), 1).with_feature(Feature::Undrain);
        let stored = r#"{"release":"0.5.0","control_format":1,"features":["undrain"]}"#;
        assert_eq!(serde_json::to_string(&old).expect("serializes"), stored);
        assert_eq!(
            serde_json::from_str::<Software>(stored).expect("decodes"),
            old
        );
        let stored = r#"{"release":"0.3.0","control_format":1}"#;
        assert_eq!(
            serde_json::to_string(&Software::baseline()).expect("serializes"),
            stored
        );
    }

    #[test]
    fn unknown_feature_names_round_trip_and_are_never_supported() {
        // A newer release's names, in either field, as a release after this one might write.
        let stored = r#"{"release":"0.7.0","control_format":2,"features":["undrain","future-x"],"newer_features":["metadata","future-y"]}"#;
        let software: Software = serde_json::from_str(stored).expect("decodes");
        assert!(software.supports(Feature::Undrain));
        assert!(software.supports(Feature::Metadata));
        assert!(!software.supports(Feature::Expiry));
        assert_eq!(
            software.unknown_features().collect::<Vec<_>>(),
            ["future-x", "future-y"]
        );
        assert_eq!(
            software.features().collect::<Vec<_>>(),
            [Feature::Undrain, Feature::Metadata]
        );

        let rewritten = serde_json::to_string(&software).expect("serializes");
        assert_eq!(
            rewritten,
            r#"{"release":"0.7.0","control_format":2,"features":["undrain"],"newer_features":["future-x","future-y","metadata"]}"#
        );
        assert_eq!(
            serde_json::from_str::<Software>(&rewritten).expect("decodes"),
            software
        );
    }

    #[test]
    fn features_round_trip_by_name_and_unknown_ones_are_refused() {
        for feature in Feature::ALL {
            assert_eq!(feature.as_str().parse(), Ok(feature));
        }
        assert!("teleport".parse::<Feature>().is_err());
    }
}
