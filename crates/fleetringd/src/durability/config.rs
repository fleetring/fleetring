//! `[[archive]]` entries: the archives a data node writes (ADR-0029, point 13).

use std::num::{NonZeroU32, NonZeroU64};
use std::path::PathBuf;
use std::time::Duration;

use serde::Deserialize;

use super::s3::{CredentialSource, HttpPolicy, PathStyle, S3Settings};
use super::store::check_key;
use crate::changes::{DatabaseMatcher, DatabaseMatchers};
use crate::config::ConfigError;

/// The longest archive name, as for sinks: it names the archive's marks.
const NAME_LEN: usize = 64;

/// The size of each chunk a snapshot's packed stream is cut into, so that no object needs a
/// multipart upload.
pub const CHUNK_BYTES: usize = 8 * 1024 * 1024;

/// One `[[archive]]` entry.
///
/// ```toml
/// [[archive]]
/// name = "backups"
/// databases = ["*"]
/// store = "s3"
/// bucket = "fleetring-backups"
/// region = "eu-west-1"
/// endpoint = "https://s3.eu-west-1.amazonaws.com"   # the default for the region
/// prefix = "fleetring"
/// credentials = "file"                             # or "env"
/// credentials_file = "/etc/fleetring/s3.toml"
///
/// [[archive]]
/// name = "local"
/// databases = ["ledger"]
/// store = "directory"
/// path = "/mnt/backups/fleetring"
/// ```
///
/// The same entries must be on every node that may serve the databases they name, as sinks
/// must, since a new primary continues an archive from its mark (ADR-0028).
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArchiveConfig {
    /// The archive's name: 1 to 64 lowercase letters, digits, `-`, or `_`, unique among the
    /// sinks and archives, since marks are kept by name.
    pub name: String,
    /// The databases it archives: exact names or prefixes ending in `*`.
    pub databases: Vec<DatabaseMatcher>,
    /// Where the archive's objects go.
    pub store: StoreKind,
    /// The key prefix every object is under: a key without a leading or trailing `/`.
    #[serde(default = "default_prefix")]
    pub prefix: String,
    /// For a directory store: the directory.
    #[serde(default)]
    pub path: Option<PathBuf>,
    /// For S3: the bucket.
    #[serde(default)]
    pub bucket: Option<String>,
    /// For S3: the endpoint; `https://s3.<region>.amazonaws.com` if unset.
    #[serde(default)]
    pub endpoint: Option<String>,
    /// For S3: the region requests are signed for.
    #[serde(default)]
    pub region: Option<String>,
    /// For S3: address the bucket in the path, as MinIO needs, rather than in the host name.
    #[serde(default)]
    pub path_style: bool,
    /// For S3: where the static credentials come from.
    #[serde(default)]
    pub credentials: CredentialKind,
    /// For S3 with `credentials = "file"`: the file.
    #[serde(default)]
    pub credentials_file: Option<PathBuf>,
    /// For S3: a PEM bundle of extra authorities to trust, for a private endpoint.
    #[serde(default)]
    pub ca: Option<PathBuf>,
    /// For S3: allow an `http://` endpoint, for a local MinIO and nothing else.
    #[serde(default)]
    pub allow_http: bool,
    /// The least time between two segments of one database, in milliseconds: the archive's
    /// recovery point, at least.
    #[serde(default = "default_segment_interval_ms")]
    pub segment_interval_ms: NonZeroU64,
    /// How old a database's newest base snapshot may get before the next segment brings a new
    /// one, in milliseconds.
    #[serde(default = "default_snapshot_interval_ms")]
    pub snapshot_interval_ms: NonZeroU64,
    /// How many bytes of segments since a database's newest base snapshot bring a new one.
    #[serde(default = "default_snapshot_after_bytes")]
    pub snapshot_after_bytes: NonZeroU64,
    /// How many complete base snapshots of each database retention keeps, with the segments
    /// that continue them.
    #[serde(default = "default_keep_snapshots")]
    pub keep_snapshots: NonZeroU32,
}

fn default_prefix() -> String {
    "fleetring".to_owned()
}

fn default_segment_interval_ms() -> NonZeroU64 {
    NonZeroU64::new(10_000).unwrap_or(NonZeroU64::MIN)
}

fn default_snapshot_interval_ms() -> NonZeroU64 {
    NonZeroU64::new(86_400_000).unwrap_or(NonZeroU64::MIN)
}

fn default_snapshot_after_bytes() -> NonZeroU64 {
    NonZeroU64::new(1024 * 1024 * 1024).unwrap_or(NonZeroU64::MIN)
}

fn default_keep_snapshots() -> NonZeroU32 {
    NonZeroU32::new(7).unwrap_or(NonZeroU32::MIN)
}

/// Where an archive's objects go.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize)]
#[serde(rename_all = "lowercase")]
#[non_exhaustive]
pub enum StoreKind {
    /// An S3-compatible bucket.
    S3,
    /// A local directory, for tests and mounted volumes.
    Directory,
}

/// Where an S3 archive's static credentials come from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default, Deserialize)]
#[serde(rename_all = "lowercase")]
#[non_exhaustive]
pub enum CredentialKind {
    /// The `AWS_*` environment variables. The default.
    #[default]
    Env,
    /// `credentials_file`.
    File,
}

/// How an archive paces, snapshots, and prunes: the parts of its configuration the archive
/// itself runs by.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct ArchiveSettings {
    /// The least time between two segments of one database.
    pub segment_interval: Duration,
    /// How old the newest base snapshot may get before the next segment brings a new one, in
    /// milliseconds.
    pub snapshot_interval_ms: u64,
    /// How many bytes of segments since the newest base snapshot bring a new one.
    pub snapshot_after_bytes: u64,
    /// How many complete base snapshots retention keeps.
    pub keep_snapshots: NonZeroU32,
    /// The size of each chunk of a snapshot.
    pub chunk_bytes: usize,
}

impl ArchiveSettings {
    /// The defaults: a segment every 10 s, a snapshot a day or every GiB of segments, seven
    /// snapshots kept, and 8 MiB chunks.
    #[must_use]
    pub fn new() -> Self {
        Self {
            segment_interval: Duration::from_secs(10),
            snapshot_interval_ms: default_snapshot_interval_ms().get(),
            snapshot_after_bytes: default_snapshot_after_bytes().get(),
            keep_snapshots: default_keep_snapshots(),
            chunk_bytes: CHUNK_BYTES,
        }
    }

    /// The same settings with snapshots cut into `chunk_bytes`-byte chunks.
    #[must_use]
    pub fn with_chunk_bytes(mut self, chunk_bytes: usize) -> Self {
        self.chunk_bytes = chunk_bytes.max(1);
        self
    }
}

impl Default for ArchiveSettings {
    fn default() -> Self {
        Self::new()
    }
}

impl ArchiveConfig {
    /// An entry named `name` archiving `databases` to the directory `path`, with every other
    /// setting at its default.
    #[must_use]
    pub fn directory(
        name: impl Into<String>,
        databases: impl IntoIterator<Item = DatabaseMatcher>,
        path: impl Into<PathBuf>,
    ) -> Self {
        Self {
            name: name.into(),
            databases: databases.into_iter().collect(),
            store: StoreKind::Directory,
            prefix: default_prefix(),
            path: Some(path.into()),
            bucket: None,
            endpoint: None,
            region: None,
            path_style: false,
            credentials: CredentialKind::Env,
            credentials_file: None,
            ca: None,
            allow_http: false,
            segment_interval_ms: default_segment_interval_ms(),
            snapshot_interval_ms: default_snapshot_interval_ms(),
            snapshot_after_bytes: default_snapshot_after_bytes(),
            keep_snapshots: default_keep_snapshots(),
        }
    }

    /// Checks the entry: a valid name, at least one database rule, a valid prefix, and the
    /// settings its store needs and no others.
    ///
    /// # Errors
    ///
    /// [`ConfigError::Invalid`] naming the first problem.
    pub fn validate(&self) -> Result<(), ConfigError> {
        let name_ok = !self.name.is_empty()
            && self.name.len() <= NAME_LEN
            && self.name.bytes().all(|byte| {
                byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-' || byte == b'_'
            });
        if !name_ok {
            return Err(invalid(format!(
                "archive name `{}` must be 1 to {NAME_LEN} lowercase letters, digits, `-`, or `_`",
                self.name
            )));
        }
        let name = &self.name;
        if self.databases.is_empty() {
            return Err(invalid(format!(
                "archive `{name}` names no databases; add `databases = [\"*\"]` to archive every one"
            )));
        }
        if !self.prefix.is_empty() && check_key(&self.prefix).is_err() {
            return Err(invalid(format!(
                "archive `{name}` has prefix `{}`: it must be `/`-separated parts, none empty or \
                 starting with `.`, with no leading or trailing `/`",
                self.prefix
            )));
        }
        let s3_fields = [
            ("bucket", self.bucket.is_some()),
            ("endpoint", self.endpoint.is_some()),
            ("region", self.region.is_some()),
            ("credentials_file", self.credentials_file.is_some()),
            ("ca", self.ca.is_some()),
            ("path_style", self.path_style),
            ("allow_http", self.allow_http),
        ];
        match self.store {
            StoreKind::Directory => {
                if self.path.is_none() {
                    return Err(invalid(format!(
                        "directory archive `{name}` needs a `path`"
                    )));
                }
                if let Some((field, _)) = s3_fields.iter().find(|(_, set)| *set) {
                    return Err(invalid(format!(
                        "directory archive `{name}` takes no `{field}`"
                    )));
                }
            }
            StoreKind::S3 => {
                if self.path.is_some() {
                    return Err(invalid(format!("s3 archive `{name}` takes no `path`")));
                }
                if self.bucket.as_deref().is_none_or(str::is_empty) {
                    return Err(invalid(format!("s3 archive `{name}` needs a `bucket`")));
                }
                if self.region.as_deref().is_none_or(str::is_empty) {
                    return Err(invalid(format!("s3 archive `{name}` needs a `region`")));
                }
                match (self.credentials, &self.credentials_file) {
                    (CredentialKind::File, None) => {
                        return Err(invalid(format!(
                            "s3 archive `{name}` reads credentials from a file, so it needs a \
                             `credentials_file`"
                        )));
                    }
                    (CredentialKind::Env, Some(_)) => {
                        return Err(invalid(format!(
                            "s3 archive `{name}` reads credentials from the environment, so it \
                             takes no `credentials_file`; set `credentials = \"file\"` to use it"
                        )));
                    }
                    _ => {}
                }
                let endpoint = self.s3_endpoint();
                if endpoint.starts_with("http://") && !self.allow_http {
                    return Err(invalid(format!(
                        "s3 archive `{name}` has a plain http endpoint, `{endpoint}`, which needs \
                         `allow_http = true`"
                    )));
                }
                if !endpoint.starts_with("http://") && !endpoint.starts_with("https://") {
                    return Err(invalid(format!(
                        "s3 archive `{name}` has endpoint `{endpoint}`, which isn't an https:// URL"
                    )));
                }
            }
        }
        Ok(())
    }

    /// The rules naming the databases the archive covers.
    #[must_use]
    pub fn matchers(&self) -> DatabaseMatchers {
        self.databases.iter().cloned().collect()
    }

    /// How the archive paces, snapshots, and prunes.
    #[must_use]
    pub fn settings(&self) -> ArchiveSettings {
        ArchiveSettings {
            segment_interval: Duration::from_millis(self.segment_interval_ms.get()),
            snapshot_interval_ms: self.snapshot_interval_ms.get(),
            snapshot_after_bytes: self.snapshot_after_bytes.get(),
            keep_snapshots: self.keep_snapshots,
            chunk_bytes: CHUNK_BYTES,
        }
    }

    fn s3_endpoint(&self) -> String {
        self.endpoint.clone().unwrap_or_else(|| {
            format!(
                "https://s3.{}.amazonaws.com",
                self.region.as_deref().unwrap_or_default()
            )
        })
    }

    /// The S3 store's settings, for an `s3` archive that [`validate`](Self::validate) accepted.
    #[must_use]
    pub fn s3_settings(&self) -> Option<S3Settings> {
        if self.store != StoreKind::S3 {
            return None;
        }
        Some(S3Settings {
            bucket: self.bucket.clone()?,
            endpoint: self.s3_endpoint(),
            region: self.region.clone()?,
            path_style: if self.path_style {
                PathStyle::Path
            } else {
                PathStyle::VirtualHost
            },
            credentials: match self.credentials {
                CredentialKind::Env => CredentialSource::Environment,
                CredentialKind::File => CredentialSource::File(self.credentials_file.clone()?),
            },
            ca: self.ca.clone(),
            http: if self.allow_http {
                HttpPolicy::AllowHttp
            } else {
                HttpPolicy::HttpsOnly
            },
        })
    }
}

fn invalid(reason: String) -> ConfigError {
    ConfigError::Invalid {
        field: "archive",
        reason,
    }
}
