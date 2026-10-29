//! The webhook sink: an HTTP POST of each change as JSON, with retries.

use std::sync::Arc;
use std::time::Duration;

use http_body_util::{BodyExt as _, Full};
use hyper::body::Bytes;
use hyper::{Request, StatusCode, Uri};
use hyper_util::client::legacy::Client;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::rt::TokioExecutor;
use tokio::time;

use crate::changes::record::ChangeRecord;
use crate::changes::{Change, ChangeSink, Published, SinkError};

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

/// A sink that POSTs each change to one URL as a JSON [`ChangeRecord`], with
/// `content-type: application/json`.
///
/// A 2xx response means the receiver has the change. A 5xx response, a timeout, or a
/// connection failure is retried with exponential backoff, up to the settings' `retries`, and
/// then handed back to the pump as [`SinkError::Failed`], which retries later; nothing is ever
/// skipped. Any other response is [`SinkError::Rejected`]: the pump still retries it, since the
/// stream can't continue past it, but logs it as the receiver's fault.
///
/// The sink speaks plain HTTP/1.1 over the host's network; put it behind a local proxy for
/// TLS.
#[derive(Debug, Clone)]
pub struct WebhookSink {
    inner: Arc<Inner>,
}

#[derive(Debug)]
struct Inner {
    name: String,
    url: Uri,
    settings: WebhookSettings,
    client: Client<HttpConnector, Full<Bytes>>,
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

impl WebhookSink {
    /// A sink named `name` that POSTs to `url`, an `http://` URL.
    ///
    /// # Errors
    ///
    /// [`InvalidUrlError`] if `url` isn't an absolute `http://` URL.
    pub fn new(
        name: impl Into<String>,
        url: &str,
        settings: WebhookSettings,
    ) -> Result<Self, InvalidUrlError> {
        let invalid = |reason: String| InvalidUrlError {
            url: url.to_owned(),
            reason,
        };
        let uri: Uri = url.parse().map_err(|error| invalid(format!("{error}")))?;
        if uri.scheme_str() != Some("http") {
            return Err(invalid("only http:// is supported".to_owned()));
        }
        if uri.authority().is_none() {
            return Err(invalid("the url names no host".to_owned()));
        }
        Ok(Self {
            inner: Arc::new(Inner {
                name: name.into(),
                url: uri,
                settings,
                client: Client::builder(TokioExecutor::new()).build_http(),
            }),
        })
    }
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
    /// Worth retrying: a 5xx, a timeout, or a transport failure.
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
        let request = Request::post(self.url.clone())
            .header("content-type", "application/json")
            .body(Full::new(body));
        let request = match request {
            Ok(request) => request,
            Err(error) => return Attempt::Rejected(format!("can't build the request: {error}")),
        };
        let response = match time::timeout(self.settings.timeout, self.client.request(request))
            .await
        {
            Ok(Ok(response)) => response,
            Ok(Err(error)) => return Attempt::Retry(format!("request failed: {error}")),
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
