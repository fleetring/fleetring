//! The webhook sink: an HTTP POST of each change as JSON, over TLS by default, with retries.

use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use http_body_util::{BodyExt as _, Full};
use hyper::body::Bytes;
use hyper::header::{AUTHORIZATION, HeaderValue};
use hyper::{Request, StatusCode, Uri};
use tokio::time;
use zeroize::Zeroizing;

use crate::changes::record::ChangeRecord;
use crate::changes::{Change, ChangeSink, Published, SinkError};
use crate::durability::HttpPolicy;
use crate::https::{self, HttpClient};

/// How long the sink waits before its first retry; each later wait doubles.
const FIRST_BACKOFF: Duration = Duration::from_millis(100);

/// The longest wait between retries.
const MAX_BACKOFF: Duration = Duration::from_secs(5);

/// How a webhook sink paces itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WebhookSettings {
    /// How long one request may take.
    pub timeout: Duration,
    /// How many times a failed request is retried before the change is handed back to the
    /// pump, which retries it again later.
    pub retries: u32,
}

impl WebhookSettings {
    /// The defaults: a 5 s timeout and 5 retries.
    #[must_use]
    pub fn new() -> Self {
        Self {
            timeout: Duration::from_secs(5),
            retries: 5,
        }
    }
}

impl Default for WebhookSettings {
    fn default() -> Self {
        Self::new()
    }
}

/// Where a webhook sink gets its bearer token. The token is never in the configuration file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TokenSource {
    /// A file holding the token, which only its owner may read. It is read once, when the sink
    /// is built.
    File(PathBuf),
    /// The environment variable of this name, read once, when the sink is built.
    Environment(String),
}

/// A certificate chain and private key, both PEM files, that a webhook sink presents to the
/// receiver (mutual TLS).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientCertificate {
    /// The PEM certificate chain.
    pub certificate: PathBuf,
    /// The PEM private key.
    pub key: PathBuf,
}

/// How a webhook sink secures its connection to the receiver.
///
/// The default is TLS with the platform's roots, no client certificate, and no token.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct WebhookSecurity {
    /// Whether an `http://` URL is allowed.
    pub http: HttpPolicy,
    /// A PEM bundle of authorities to trust in addition to the platform's roots.
    pub ca: Option<PathBuf>,
    /// The client certificate to present, for a receiver that requires mutual TLS.
    pub client_certificate: Option<ClientCertificate>,
    /// Where to read a bearer token to send as `Authorization: Bearer <token>`.
    pub token: Option<TokenSource>,
}

/// A sink that POSTs each change to one URL as a JSON [`ChangeRecord`], with
/// `content-type: application/json`.
///
/// A 2xx response means the receiver has the change. A 5xx response, a timeout, or a
/// connection failure, a TLS failure such as a certificate the sink doesn't trust included, is
/// retried with exponential backoff, up to the settings' `retries`, and then handed back to the
/// pump as [`SinkError::Failed`], which retries later; nothing is ever skipped. Any other
/// response is [`SinkError::Rejected`]: the pump still retries it, since the stream can't
/// continue past it, but logs it as the receiver's fault.
///
/// An `https://` URL is dialled over TLS 1.2 or 1.3, trusting the platform's roots and the
/// [`WebhookSecurity::ca`] authorities. A bearer token, if configured, is sent on every request
/// and kept out of `Debug` and of every log line.
#[derive(Debug, Clone)]
pub struct WebhookSink {
    inner: Arc<Inner>,
}

#[derive(Debug)]
struct Inner {
    name: String,
    url: Uri,
    settings: WebhookSettings,
    client: HttpClient,
    authorization: Option<Authorization>,
}

/// The `Authorization` header's value, zeroed when dropped and never printed.
struct Authorization(Zeroizing<String>);

impl fmt::Debug for Authorization {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Authorization(<redacted>)")
    }
}

impl Authorization {
    /// The header for `token`, if it can be one.
    fn bearer(token: &str) -> Result<Self, WebhookConfigError> {
        let value = Zeroizing::new(format!("Bearer {token}"));
        HeaderValue::from_str(&value).map_err(|_| WebhookConfigError::Token {
            reason: "it holds characters a header can't carry".to_owned(),
        })?;
        Ok(Self(value))
    }

    /// The value for one request, marked sensitive so that nothing prints it.
    fn header(&self) -> Option<HeaderValue> {
        let mut value = HeaderValue::from_str(&self.0).ok()?;
        value.set_sensitive(true);
        Some(value)
    }
}

/// The error returned for a webhook URL hyper can't dial.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("invalid webhook url `{url}`: {reason}")]
pub struct InvalidUrlError {
    /// The URL.
    pub url: String,
    /// Why.
    pub reason: String,
}

/// The error returned when a [`WebhookSink`] can't be built.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum WebhookConfigError {
    /// The URL isn't one the sink can POST to.
    #[error(transparent)]
    Url(#[from] InvalidUrlError),
    /// The URL is `http://`, which needs `allow_http`.
    #[error("webhook url `{url}` is plain http, which needs allow_http = true")]
    HttpNotAllowed {
        /// The URL.
        url: String,
    },
    /// TLS can't be set up: the authority or the client certificate can't be read, or is
    /// invalid.
    #[error("webhook url `{url}`: {reason}")]
    Tls {
        /// The URL.
        url: String,
        /// Why.
        reason: String,
    },
    /// The bearer token is missing or can't be read.
    #[error("webhook bearer token unavailable: {reason}")]
    Token {
        /// Why. Never the token.
        reason: String,
    },
    /// The token file grants its group or others some permission.
    #[error("webhook token file {} has mode {mode:o}; only its owner may have access", path.display())]
    TokenShared {
        /// The file.
        path: PathBuf,
        /// Its permission bits.
        mode: u32,
    },
}

impl WebhookSink {
    /// A sink named `name` that POSTs to `url`, an `https://` URL, or an `http://` one if
    /// `security` allows it.
    ///
    /// # Errors
    ///
    /// [`WebhookConfigError`] if `url` isn't an absolute `http://` or `https://` URL with a
    /// host, is `http://` without [`HttpPolicy::AllowHttp`], names an authority or a client
    /// certificate that can't be read, or names a token that is missing, unreadable, or in a
    /// file others can read.
    pub fn new(
        name: impl Into<String>,
        url: &str,
        settings: WebhookSettings,
        security: &WebhookSecurity,
    ) -> Result<Self, WebhookConfigError> {
        let name = name.into();
        let invalid = |reason: &str| InvalidUrlError {
            url: url.to_owned(),
            reason: reason.to_owned(),
        };
        let uri: Uri = url.parse().map_err(|error| InvalidUrlError {
            url: url.to_owned(),
            reason: format!("{error}"),
        })?;
        match uri.scheme_str() {
            Some("https") => {}
            Some("http") if security.http == HttpPolicy::AllowHttp => {}
            Some("http") => {
                return Err(WebhookConfigError::HttpNotAllowed {
                    url: url.to_owned(),
                });
            }
            _ => return Err(invalid("only http:// and https:// are supported").into()),
        }
        if uri.authority().is_none() {
            return Err(invalid("the url names no host").into());
        }
        let authorization = security.token.as_ref().map(read_token).transpose()?;
        if authorization.is_some() && uri.scheme_str() == Some("http") {
            tracing::warn!(sink = %name, "this webhook sends its bearer token over plain http");
        }
        let identity = security
            .client_certificate
            .as_ref()
            .map(|pair| (pair.certificate.as_path(), pair.key.as_path()));
        let client =
            https::client(security.ca.as_deref(), identity, security.http).map_err(|error| {
                WebhookConfigError::Tls {
                    url: url.to_owned(),
                    reason: error.to_string(),
                }
            })?;
        Ok(Self {
            inner: Arc::new(Inner {
                name,
                url: uri,
                settings,
                client,
                authorization,
            }),
        })
    }
}

/// Reads the token `source` names and makes the header it becomes.
fn read_token(source: &TokenSource) -> Result<Authorization, WebhookConfigError> {
    let unavailable = |reason: String| WebhookConfigError::Token { reason };
    let text = match source {
        TokenSource::Environment(variable) => Zeroizing::new(
            std::env::var(variable)
                .map_err(|_| unavailable(format!("{variable} is not set to a string")))?,
        ),
        TokenSource::File(path) => {
            check_private(path)?;
            Zeroizing::new(
                std::fs::read_to_string(path)
                    .map_err(|error| unavailable(format!("{}: {error}", path.display())))?,
            )
        }
    };
    let token = text.trim();
    if token.is_empty() {
        return Err(unavailable("it is empty".to_owned()));
    }
    Authorization::bearer(token)
}

/// Refuses a token file whose mode grants its group or others any permission.
#[cfg(unix)]
fn check_private(path: &Path) -> Result<(), WebhookConfigError> {
    use std::os::unix::fs::PermissionsExt as _;
    let mode = std::fs::metadata(path)
        .map_err(|error| WebhookConfigError::Token {
            reason: format!("{}: {error}", path.display()),
        })?
        .permissions()
        .mode();
    if mode & 0o077 != 0 {
        return Err(WebhookConfigError::TokenShared {
            path: path.to_owned(),
            mode: mode & 0o777,
        });
    }
    Ok(())
}

#[cfg(not(unix))]
fn check_private(_: &Path) -> Result<(), WebhookConfigError> {
    Ok(())
}

/// An error and its causes, outermost first. Hyper's own message for a connection that failed is
/// "client error (Connect)"; the reason, such as an untrusted certificate, is a cause.
fn describe(error: &(dyn std::error::Error + 'static)) -> String {
    let mut text = error.to_string();
    let mut source = error.source();
    while let Some(cause) = source {
        text.push_str(": ");
        text.push_str(&cause.to_string());
        source = cause.source();
    }
    text
}

impl ChangeSink for WebhookSink {
    fn name(&self) -> &str {
        &self.inner.name
    }

    fn publish(&self, change: Change) -> Published {
        let inner = Arc::clone(&self.inner);
        Box::pin(async move { inner.publish(&change).await })
    }
}

/// How one request ended.
enum Attempt {
    Accepted,
    /// Worth retrying: a 5xx, a timeout, or a transport failure, TLS included.
    Retry(String),
    /// Not worth retrying: the receiver refused the change.
    Rejected(String),
}

impl Inner {
    async fn publish(&self, change: &Change) -> Result<(), SinkError> {
        let record = ChangeRecord::from(change);
        let body = serde_json::to_vec(&record).map_err(|error| SinkError::Rejected {
            reason: format!("can't encode the change: {error}"),
        })?;
        let body = Bytes::from(body);
        let mut backoff = FIRST_BACKOFF;
        let mut attempts = 0_u32;
        loop {
            attempts += 1;
            let reason = match self.attempt(body.clone()).await {
                Attempt::Accepted => return Ok(()),
                Attempt::Rejected(reason) => return Err(SinkError::Rejected { reason }),
                Attempt::Retry(reason) => reason,
            };
            if attempts > self.settings.retries {
                return Err(SinkError::Failed {
                    reason: format!("{reason} (after {attempts} attempts)"),
                });
            }
            tracing::debug!(sink = %self.name, %reason, attempt = attempts, retry_in = ?backoff, "webhook request failed");
            time::sleep(backoff).await;
            backoff = (backoff * 2).min(MAX_BACKOFF);
        }
    }

    async fn attempt(&self, body: Bytes) -> Attempt {
        let mut request =
            Request::post(self.url.clone()).header("content-type", "application/json");
        if let Some(authorization) = &self.authorization {
            let Some(value) = authorization.header() else {
                return Attempt::Rejected("can't build the authorization header".to_owned());
            };
            request = request.header(AUTHORIZATION, value);
        }
        let request = match request.body(Full::new(body)) {
            Ok(request) => request,
            Err(error) => return Attempt::Rejected(format!("can't build the request: {error}")),
        };
        let response = match time::timeout(self.settings.timeout, self.client.request(request))
            .await
        {
            Ok(Ok(response)) => response,
            Ok(Err(error)) => {
                return Attempt::Retry(format!("request failed: {}", describe(&error)));
            }
            Err(_) => {
                return Attempt::Retry(format!("no response within {:?}", self.settings.timeout));
            }
        };
        let status = response.status();
        // Drain the body so the connection can be reused; its content doesn't matter.
        let _body = response.into_body().collect().await;
        if status.is_success() {
            Attempt::Accepted
        } else if status.is_server_error() || status == StatusCode::TOO_MANY_REQUESTS {
            Attempt::Retry(format!("the receiver answered {status}"))
        } else {
            Attempt::Rejected(format!("the receiver answered {status}"))
        }
    }
}
