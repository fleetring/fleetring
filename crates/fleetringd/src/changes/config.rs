//! The `[changes]` table: the sinks a data node streams changes to.

use std::path::PathBuf;

use serde::Deserialize;

use crate::changes::{DatabaseMatcher, DatabaseMatchers};
use crate::config::ConfigError;

/// The longest sink name, so that it always fits a path component.
const NAME_LEN: usize = 64;

/// The `[changes]` table: a list of sinks, each a `[[changes.sink]]` entry (FR-039).
///
/// ```toml
/// [[changes.sink]]
/// name = "archive"
/// kind = "file"
/// format = "logical"              # or "physical"
/// databases = ["tenant:*", "ledger"]
/// path = "/var/lib/fleetring/changes"
///
/// [[changes.sink]]
/// name = "hooks"
/// kind = "webhook"
/// databases = ["*"]
/// url = "http://127.0.0.1:8080/changes"
/// timeout_ms = 5000
/// retries = 5
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ChangesConfig {
    /// The sinks, in order.
    pub sink: Vec<SinkConfig>,
}

impl ChangesConfig {
    /// Checks every sink: a valid, unique name, at least one database rule, and the settings
    /// its kind needs.
    ///
    /// # Errors
    ///
    /// [`ConfigError::Invalid`] naming the first problem.
    pub fn validate(&self) -> Result<(), ConfigError> {
        let mut names = Vec::with_capacity(self.sink.len());
        for sink in &self.sink {
            sink.validate()?;
            if names.contains(&sink.name.as_str()) {
                return Err(invalid(format!("sink name `{}` is used twice", sink.name)));
            }
            names.push(sink.name.as_str());
        }
        Ok(())
    }

    /// The databases some logical sink streams, which record their row changes.
    #[must_use]
    pub fn recorded_databases(&self) -> DatabaseMatchers {
        self.sink
            .iter()
            .filter(|sink| sink.format == Format::Logical)
            .flat_map(|sink| sink.databases.iter().cloned())
            .collect()
    }
}

/// One `[[changes.sink]]` entry.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SinkConfig {
    /// The sink's name: 1 to 64 lowercase letters, digits, `-`, or `_`. It names the directory
    /// the sink's positions are kept in.
    pub name: String,
    /// What the sink is.
    pub kind: SinkKind,
    /// Whether it receives row changes or opaque batches.
    #[serde(default)]
    pub format: Format,
    /// The databases it streams: exact names or prefixes ending in `*`.
    pub databases: Vec<DatabaseMatcher>,
    /// For a file sink: the directory that holds one `.ndjson` file per database.
    #[serde(default)]
    pub path: Option<PathBuf>,
    /// For a webhook sink: the URL each change is POSTed to.
    #[serde(default)]
    pub url: Option<String>,
    /// For a webhook sink: how long one request may take, in milliseconds.
    #[serde(default = "default_timeout_ms")]
    pub timeout_ms: u64,
    /// For a webhook sink: how many times a failed request is retried before the change is
    /// handed back to the pump, which retries it again later.
    #[serde(default = "default_retries")]
    pub retries: u32,
}

fn default_timeout_ms() -> u64 {
    5_000
}

fn default_retries() -> u32 {
    5
}

impl SinkConfig {
    fn validate(&self) -> Result<(), ConfigError> {
        let name_ok = !self.name.is_empty()
            && self.name.len() <= NAME_LEN
            && self.name.bytes().all(|byte| {
                byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-' || byte == b'_'
            });
        if !name_ok {
            return Err(invalid(format!(
                "sink name `{}` must be 1 to {NAME_LEN} lowercase letters, digits, `-`, or `_`",
                self.name
            )));
        }
        if self.databases.is_empty() {
            return Err(invalid(format!(
                "sink `{}` names no databases; add `databases = [\"*\"]` to stream every one",
                self.name
            )));
        }
        match self.kind {
            SinkKind::File if self.path.is_none() => {
                Err(invalid(format!("file sink `{}` needs a `path`", self.name)))
            }
            SinkKind::File if self.url.is_some() => {
                Err(invalid(format!("file sink `{}` takes no `url`", self.name)))
            }
            SinkKind::Webhook if self.url.is_none() => Err(invalid(format!(
                "webhook sink `{}` needs a `url`",
                self.name
            ))),
            SinkKind::Webhook if self.path.is_some() => Err(invalid(format!(
                "webhook sink `{}` takes no `path`",
                self.name
            ))),
            SinkKind::Webhook if self.timeout_ms == 0 => Err(invalid(format!(
                "webhook sink `{}` needs a `timeout_ms` of at least 1",
                self.name
            ))),
            _ => Ok(()),
        }
    }

    /// The rules naming the databases the sink streams.
    #[must_use]
    pub fn matchers(&self) -> DatabaseMatchers {
        self.databases.iter().cloned().collect()
    }
}

fn invalid(reason: String) -> ConfigError {
    ConfigError::Invalid {
        field: "changes.sink",
        reason,
    }
}

/// The built-in sink kinds. Adding one means a variant here and a branch in
/// [`build_sink`](crate::changes::build_sink).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize)]
#[serde(rename_all = "lowercase")]
#[non_exhaustive]
pub enum SinkKind {
    /// One append-only NDJSON file per database, under `path`.
    File,
    /// An HTTP POST of each change as JSON to `url`.
    Webhook,
}

/// What a sink receives for each acknowledged batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default, Deserialize)]
#[serde(rename_all = "lowercase")]
#[non_exhaustive]
pub enum Format {
    /// The rows the batch's transactions inserted, updated, and deleted. The default.
    #[default]
    Logical,
    /// The batch's opaque payload, as the engine ships it to followers.
    Physical,
}
