//! The `[changes]` table: the sinks a data node streams changes to.

use std::path::PathBuf;

use serde::Deserialize;

use crate::changes::{
    CaptureRules, ClientCertificate, DatabaseMatcher, DatabaseMatchers, TokenSource,
    WebhookSecurity,
};
use crate::config::ConfigError;
use crate::durability::HttpPolicy;
use crate::engine::CaptureMode;

/// The longest sink name, so that it always fits a path component.
const NAME_LEN: usize = 64;

/// The `[changes]` table: a list of sinks, each a `[[changes.sink]]` entry (FR-039).
///
/// ```toml
/// [[changes.sink]]
/// name = "archive"
/// kind = "file"
/// format = "logical"              # or "physical"
/// capture = "after"               # or "id", or "full"
/// databases = ["tenant:*", "ledger"]
/// path = "/var/lib/fleetring/changes"
///
/// [[changes.sink]]
/// name = "hooks"
/// kind = "webhook"
/// databases = ["*"]
/// url = "https://hooks.example.com/changes"
/// # ca = "/etc/fleetring/hooks-ca.crt"        # trust this authority as well as the platform's
/// # cert = "/etc/fleetring/hooks-client.crt"  # mutual TLS: present this certificate...
/// # key = "/etc/fleetring/hooks-client.key"   # ...with this key
/// # token_file = "/etc/fleetring/hooks.token" # or token_env = "HOOKS_TOKEN"
/// # allow_http = true                         # only to POST to an http:// url
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

    /// The databases some logical sink streams, which record their row changes, each in the
    /// strongest mode a sink naming it asks for.
    #[must_use]
    pub fn capture_rules(&self) -> CaptureRules {
        CaptureRules::new(
            self.sink
                .iter()
                .filter(|sink| sink.format == Format::Logical)
                .map(|sink| (sink.matchers(), sink.capture)),
        )
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
    /// For a logical sink: how much of each changed row the databases it names record. A
    /// database named by several sinks records in the strongest mode among them.
    #[serde(default)]
    pub capture: CaptureMode,
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
    /// For a webhook sink: a PEM bundle of authorities to trust as well as the platform's
    /// roots, for a receiver with a private certificate.
    #[serde(default)]
    pub ca: Option<PathBuf>,
    /// For a webhook sink: the PEM certificate chain to present to the receiver (mutual TLS).
    /// Needs `key`.
    #[serde(default)]
    pub cert: Option<PathBuf>,
    /// For a webhook sink: the PEM private key of `cert`. Needs `cert`.
    #[serde(default)]
    pub key: Option<PathBuf>,
    /// For a webhook sink: a file holding a bearer token to send as `Authorization: Bearer
    /// <token>`, which only its owner may read. At most one of `token_file` and `token_env`.
    #[serde(default)]
    pub token_file: Option<PathBuf>,
    /// For a webhook sink: the environment variable holding the bearer token. At most one of
    /// `token_file` and `token_env`.
    #[serde(default)]
    pub token_env: Option<String>,
    /// For a webhook sink: whether an `http://` `url` is allowed, so that changes leave the node
    /// unencrypted. Without it only `https://` is.
    #[serde(default)]
    pub allow_http: bool,
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
        let webhook_fields = [
            ("url", self.url.is_some()),
            ("ca", self.ca.is_some()),
            ("cert", self.cert.is_some()),
            ("key", self.key.is_some()),
            ("token_file", self.token_file.is_some()),
            ("token_env", self.token_env.is_some()),
            ("allow_http", self.allow_http),
        ];
        match self.kind {
            SinkKind::File => {
                if self.path.is_none() {
                    return Err(invalid(format!("file sink `{}` needs a `path`", self.name)));
                }
                if let Some((field, _)) = webhook_fields.iter().find(|(_, set)| *set) {
                    return Err(invalid(format!(
                        "file sink `{}` takes no `{field}`",
                        self.name
                    )));
                }
                Ok(())
            }
            SinkKind::Webhook => self.validate_webhook(),
        }
    }

    fn validate_webhook(&self) -> Result<(), ConfigError> {
        let name = &self.name;
        let Some(url) = &self.url else {
            return Err(invalid(format!("webhook sink `{name}` needs a `url`")));
        };
        if self.path.is_some() {
            return Err(invalid(format!("webhook sink `{name}` takes no `path`")));
        }
        if self.timeout_ms == 0 {
            return Err(invalid(format!(
                "webhook sink `{name}` needs a `timeout_ms` of at least 1"
            )));
        }
        if url.starts_with("http://") {
            if !self.allow_http {
                return Err(invalid(format!(
                    "webhook sink `{name}` has a plain http `url`, `{url}`, which needs \
                     `allow_http = true`"
                )));
            }
        } else if !url.starts_with("https://") {
            return Err(invalid(format!(
                "webhook sink `{name}` has `url` `{url}`, which isn't an https:// URL"
            )));
        }
        match (&self.cert, &self.key) {
            (Some(_), None) => {
                return Err(invalid(format!(
                    "webhook sink `{name}` has a `cert` but no `key`; mutual TLS needs both"
                )));
            }
            (None, Some(_)) => {
                return Err(invalid(format!(
                    "webhook sink `{name}` has a `key` but no `cert`; mutual TLS needs both"
                )));
            }
            _ => {}
        }
        if self.token_file.is_some() && self.token_env.is_some() {
            return Err(invalid(format!(
                "webhook sink `{name}` sets both `token_file` and `token_env`; use one"
            )));
        }
        if self.token_env.as_deref().is_some_and(str::is_empty) {
            return Err(invalid(format!(
                "webhook sink `{name}` has an empty `token_env`"
            )));
        }
        Ok(())
    }

    /// How a webhook sink secures its connection, for a webhook sink that
    /// [`validate`](ChangesConfig::validate) accepted.
    #[must_use]
    pub fn webhook_security(&self) -> WebhookSecurity {
        WebhookSecurity {
            http: if self.allow_http {
                HttpPolicy::AllowHttp
            } else {
                HttpPolicy::HttpsOnly
            },
            ca: self.ca.clone(),
            client_certificate: match (&self.cert, &self.key) {
                (Some(certificate), Some(key)) => Some(ClientCertificate {
                    certificate: certificate.clone(),
                    key: key.clone(),
                }),
                _ => None,
            },
            token: match (&self.token_file, &self.token_env) {
                (Some(path), _) => Some(TokenSource::File(path.clone())),
                (None, Some(variable)) => Some(TokenSource::Environment(variable.clone())),
                (None, None) => None,
            },
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
    /// An HTTP POST of each change as JSON to `url`, over TLS unless `allow_http` says otherwise.
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
