//! The router's configuration file (ADR-0037): `fleetring router --config <file>`.

use std::fs;
use std::net::SocketAddr;
use std::num::{NonZeroU64, NonZeroUsize};
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::Deserialize;

use crate::config::{ConfigError, LogConfig, Plaintext, TlsConfig};

/// The router's configuration: where it serves Hrana, its identity toward the fleet, and its
/// bounds.
///
/// ```toml
/// listen = "127.0.0.1:8080"
///
/// [tls]
/// ca = "/etc/fleetring/tls/ca.crt"
/// cert = "/etc/fleetring/tls/client.router.crt"
/// key = "/etc/fleetring/tls/client.router.key"
///
/// [control]
/// address = "10.0.0.1:7400"
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct RouterConfig {
    /// Where the router serves Hrana over HTTP.
    pub listen: SocketAddr,
    /// How it serves HTTP: its server certificate, and the URL it advertises.
    #[serde(default)]
    pub http: HttpConfig,
    /// The router's identity toward the fleet: a client certificate for a principal every data
    /// node lists with `forward_tokens = true` and no scopes. The simulation harness may supply
    /// one instead.
    #[serde(default)]
    pub tls: Option<TlsConfig>,
    /// Where the control plane is.
    pub control: RouterControl,
    /// How it finds each database's primary.
    #[serde(default)]
    pub routing: RoutingConfig,
    /// The bounds on Hrana streams and requests.
    #[serde(default)]
    pub streams: StreamsConfig,
    /// How it writes its log lines.
    #[serde(default)]
    pub log: LogConfig,
}

/// The `[http]` table: how the router serves HTTP.
#[derive(Debug, Clone, PartialEq, Eq, Default, Deserialize)]
#[serde(deny_unknown_fields, default)]
#[non_exhaustive]
pub struct HttpConfig {
    /// The router's server certificate chain, PEM, from any authority: application clients check
    /// it against their own trust store. With `key`; without both, the router serves plaintext.
    pub cert: Option<PathBuf>,
    /// The server certificate's private key, PEM.
    pub key: Option<PathBuf>,
    /// The URL clients reach this router instance at, returned as `base_url` with each baton so
    /// that a stream's later requests come back here behind a load balancer.
    pub advertise_url: Option<String>,
    /// Whether plaintext may be served on an address that isn't loopback. Tokens would then
    /// cross the network in the clear.
    pub allow_plaintext: Plaintext,
}

/// The `[control]` table: the control node the router asks where databases live.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct RouterControl {
    /// The control node's address; the router follows leader redirects from there.
    pub address: SocketAddr,
}

/// How long a database's primary is cached when nothing names a new one: 60 seconds.
const DEFAULT_PLACEMENT_TTL_MS: NonZeroU64 = NonZeroU64::new(60_000).expect("nonzero");
/// How many databases' primaries are cached at once.
const DEFAULT_PLACEMENT_CACHE_SIZE: NonZeroUsize = NonZeroUsize::new(100_000).expect("nonzero");
/// How long opening a session retries while a database has no primary: 5 seconds.
const DEFAULT_OPEN_RETRY_MS: u64 = 5_000;
/// How long a call's answer may take before the router gives up on it: 60 seconds, beyond the
/// node's statement deadline and its acknowledgement wait.
const DEFAULT_CALL_TIMEOUT_MS: NonZeroU64 = NonZeroU64::new(60_000).expect("nonzero");

/// The `[routing]` table: how the router finds each database's primary.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields, default)]
#[non_exhaustive]
pub struct RoutingConfig {
    /// With a suffix, a request whose `Host` is `<name>.<host_suffix>` names the database
    /// `<name>`, for names that are DNS labels.
    pub host_suffix: Option<String>,
    /// How long a database's primary is cached, in milliseconds, unless a redirect names a new
    /// one first.
    pub placement_ttl_ms: NonZeroU64,
    /// How many databases' primaries are cached; the oldest goes first.
    pub placement_cache_size: NonZeroUsize,
    /// How long opening a session retries, in milliseconds, while the database has no primary,
    /// as during a promotion.
    pub open_retry_ms: u64,
    /// How long the router waits for a call's answer, in milliseconds, before it gives the call
    /// up as of unknown outcome.
    pub call_timeout_ms: NonZeroU64,
}

impl Default for RoutingConfig {
    fn default() -> Self {
        Self {
            host_suffix: None,
            placement_ttl_ms: DEFAULT_PLACEMENT_TTL_MS,
            placement_cache_size: DEFAULT_PLACEMENT_CACHE_SIZE,
            open_retry_ms: DEFAULT_OPEN_RETRY_MS,
            call_timeout_ms: DEFAULT_CALL_TIMEOUT_MS,
        }
    }
}

impl RoutingConfig {
    /// How long a primary is cached.
    #[must_use]
    pub fn placement_ttl(&self) -> Duration {
        Duration::from_millis(self.placement_ttl_ms.get())
    }

    /// How long opening a session retries.
    #[must_use]
    pub fn open_retry(&self) -> Duration {
        Duration::from_millis(self.open_retry_ms)
    }

    /// How long a call's answer may take.
    #[must_use]
    pub fn call_timeout(&self) -> Duration {
        Duration::from_millis(self.call_timeout_ms.get())
    }
}

/// How long a Hrana stream may sit idle: 10 seconds.
const DEFAULT_IDLE_TIMEOUT_MS: NonZeroU64 = NonZeroU64::new(10_000).expect("nonzero");
/// How many Hrana streams one router holds.
const DEFAULT_MAX_STREAMS: NonZeroUsize = NonZeroUsize::new(10_000).expect("nonzero");
/// The largest request body: 8 MiB.
const DEFAULT_MAX_REQUEST_BYTES: NonZeroUsize =
    NonZeroUsize::new(8 * 1024 * 1024).expect("nonzero");
/// How many SQL texts one stream may store.
const DEFAULT_MAX_SQL_PER_STREAM: NonZeroUsize = NonZeroUsize::new(128).expect("nonzero");
/// How many statements one request may carry: its stream requests and their batch steps.
const DEFAULT_MAX_STATEMENTS: NonZeroUsize = NonZeroUsize::new(1_000).expect("nonzero");

/// The `[streams]` table: the bounds on Hrana streams and requests.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields, default)]
#[non_exhaustive]
pub struct StreamsConfig {
    /// How long a stream may go without a request, in milliseconds, before it expires and its
    /// session closes.
    pub idle_timeout_ms: NonZeroU64,
    /// How many streams the router holds at once; a request for one more gets `503`.
    pub max_streams: NonZeroUsize,
    /// The largest request body, in bytes; a larger one gets `413`.
    pub max_request_bytes: NonZeroUsize,
    /// How many SQL texts a stream may store with `store_sql`.
    pub max_sql_per_stream: NonZeroUsize,
    /// How many statements one request may carry, counting each stream request and each batch
    /// step; a request with more gets `413`.
    pub max_statements: NonZeroUsize,
}

impl Default for StreamsConfig {
    fn default() -> Self {
        Self {
            idle_timeout_ms: DEFAULT_IDLE_TIMEOUT_MS,
            max_streams: DEFAULT_MAX_STREAMS,
            max_request_bytes: DEFAULT_MAX_REQUEST_BYTES,
            max_sql_per_stream: DEFAULT_MAX_SQL_PER_STREAM,
            max_statements: DEFAULT_MAX_STATEMENTS,
        }
    }
}

impl StreamsConfig {
    /// How long a stream may sit idle.
    #[must_use]
    pub fn idle_timeout(&self) -> Duration {
        Duration::from_millis(self.idle_timeout_ms.get())
    }
}

impl RouterConfig {
    /// Reads and validates a router configuration file.
    ///
    /// # Errors
    ///
    /// [`ConfigError`] if the file can't be read or isn't valid.
    pub fn load(path: impl AsRef<Path>) -> Result<Self, ConfigError> {
        let path = path.as_ref();
        let text = fs::read_to_string(path).map_err(|source| ConfigError::Read {
            path: path.to_owned(),
            source,
        })?;
        Self::from_toml(&text)
    }

    /// Parses and validates a router configuration from TOML text.
    ///
    /// # Errors
    ///
    /// [`ConfigError`] if the text isn't valid.
    pub fn from_toml(text: &str) -> Result<Self, ConfigError> {
        let config: Self = toml::from_str(text)?;
        config.validate()?;
        Ok(config)
    }

    /// Whether the router serves HTTPS: it has a server certificate and key.
    #[must_use]
    pub fn serves_tls(&self) -> bool {
        self.http.cert.is_some()
    }

    /// Checks the rules that span fields.
    ///
    /// # Errors
    ///
    /// [`ConfigError::Invalid`] if only one of `http.cert` and `http.key` is set; if the router
    /// would serve plaintext on an address that isn't loopback without `http.allow_plaintext`;
    /// if `http.advertise_url` isn't an absolute `http` or `https` URL without a query; or if
    /// `routing.host_suffix` isn't a DNS name.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.http.cert.is_some() != self.http.key.is_some() {
            return Err(ConfigError::Invalid {
                field: "http.cert",
                reason: "set http.cert and http.key together, or neither".to_owned(),
            });
        }
        if !self.serves_tls()
            && !self.listen.ip().is_loopback()
            && self.http.allow_plaintext == Plaintext::Refused
        {
            return Err(ConfigError::Invalid {
                field: "listen",
                reason: "serving plaintext on an address that isn't loopback would send bearer \
                         tokens in the clear: set http.cert and http.key, or http.allow_plaintext \
                         = true"
                    .to_owned(),
            });
        }
        if let Some(url) = &self.http.advertise_url {
            check_url(url)?;
        }
        if let Some(suffix) = &self.routing.host_suffix
            && !is_dns_name(suffix)
        {
            return Err(ConfigError::Invalid {
                field: "routing.host_suffix",
                reason: format!("'{suffix}' isn't a DNS name, such as db.example.com"),
            });
        }
        Ok(())
    }
}

/// Checks that `url` is an absolute `http` or `https` URL with a host and no query or fragment.
fn check_url(url: &str) -> Result<(), ConfigError> {
    let invalid = |reason: &str| ConfigError::Invalid {
        field: "http.advertise_url",
        reason: format!("'{url}' {reason}"),
    };
    let rest = url
        .strip_prefix("https://")
        .or_else(|| url.strip_prefix("http://"))
        .ok_or_else(|| invalid("isn't an http:// or https:// URL"))?;
    let host = rest.split('/').next().unwrap_or_default();
    if host.is_empty() {
        return Err(invalid("has no host"));
    }
    if url.contains(['?', '#']) {
        return Err(invalid("has a query or a fragment"));
    }
    Ok(())
}

/// Whether `name` is a DNS name: dot-separated labels of letters, digits, and hyphens, none
/// starting or ending with a hyphen.
pub(crate) fn is_dns_name(name: &str) -> bool {
    !name.is_empty() && name.len() <= 253 && name.split('.').all(is_dns_label)
}

/// Whether `label` is one DNS label, in lowercase.
pub(crate) fn is_dns_label(label: &str) -> bool {
    let bytes = label.as_bytes();
    (1..=63).contains(&bytes.len())
        && bytes
            .iter()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || *byte == b'-')
        && bytes.first() != Some(&b'-')
        && bytes.last() != Some(&b'-')
}

#[cfg(test)]
mod tests {
    use super::*;

    const BASE: &str = "[control]\naddress = \"127.0.0.1:7400\"\n";

    fn with(listen: &str, more: &str) -> Result<RouterConfig, ConfigError> {
        RouterConfig::from_toml(&format!("listen = \"{listen}\"\n{more}\n{BASE}"))
    }

    #[test]
    fn plaintext_off_loopback_needs_the_flag() {
        assert!(with("127.0.0.1:8080", "").is_ok());
        assert!(with("[::1]:8080", "").is_ok());
        let refused = with("0.0.0.0:8080", "").expect_err("plaintext on every address");
        assert!(refused.to_string().contains("listen"), "{refused}");
        assert!(with("0.0.0.0:8080", "[http]\nallow_plaintext = true").is_ok());
        let tls = "[http]\ncert = \"server.crt\"\nkey = \"server.key\"";
        assert!(with("0.0.0.0:8080", tls).is_ok());
        let half = with("0.0.0.0:8080", "[http]\ncert = \"server.crt\"").expect_err("no key");
        assert!(half.to_string().contains("http.cert"), "{half}");
    }

    #[test]
    fn defaults_bound_streams_and_requests() {
        let config = with("127.0.0.1:8080", "").expect("valid");
        assert_eq!(config.streams.idle_timeout(), Duration::from_secs(10));
        assert_eq!(config.streams.max_streams.get(), 10_000);
        assert_eq!(config.streams.max_request_bytes.get(), 8 * 1024 * 1024);
        assert_eq!(config.streams.max_sql_per_stream.get(), 128);
        assert_eq!(config.routing.placement_ttl(), Duration::from_secs(60));
        assert_eq!(config.routing.open_retry(), Duration::from_secs(5));
        let unknown = with("127.0.0.1:8080", "[streams]\nmax_stream = 3").expect_err("typo");
        assert!(unknown.to_string().contains("max_stream"), "{unknown}");
    }

    #[test]
    fn advertised_urls_and_host_suffixes_are_checked() {
        for url in [
            "https://router-1.example.com:8080",
            "http://127.0.0.1:8080/",
        ] {
            let config = with(
                "127.0.0.1:8080",
                &format!("[http]\nadvertise_url = \"{url}\""),
            );
            assert!(config.is_ok(), "{url}: {config:?}");
        }
        for url in [
            "ftp://router",
            "https://",
            "https://router?x=1",
            "router:8080",
        ] {
            let refused = with(
                "127.0.0.1:8080",
                &format!("[http]\nadvertise_url = \"{url}\""),
            );
            assert!(refused.is_err(), "{url}");
        }
        assert!(
            with(
                "127.0.0.1:8080",
                "[routing]\nhost_suffix = \"db.example.com\""
            )
            .is_ok()
        );
        for suffix in [
            ".db.example.com",
            "db..example",
            "-db.example.com",
            "DB.example.com",
        ] {
            let refused = with(
                "127.0.0.1:8080",
                &format!("[routing]\nhost_suffix = \"{suffix}\""),
            );
            assert!(refused.is_err(), "{suffix}");
        }
    }
}
