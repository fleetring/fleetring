//! The S3 store: any S3-compatible service, through `rusty-s3`, which signs requests and parses
//! replies without doing I/O, over the daemon's own `hyper` and `rustls` (ADR-0029, point 15).

mod credentials;
mod http;

use std::fmt;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use http_body_util::{BodyExt as _, Full};
use hyper::body::Bytes;
use hyper::{Method, Request, StatusCode};
use rusty_s3::actions::ListObjectsV2;
use rusty_s3::{Bucket, S3Action, UrlStyle};
use tokio::time;

use self::credentials::CredentialStore;
use self::http::HttpClient;
use super::store::{ObjectEntry, ObjectStore, StoreError, StoreFuture, check_key, check_prefix};
use crate::clock::Clock;

/// How long a signed request stays valid.
const SIGNED_FOR: Duration = Duration::from_secs(60);

/// How long one request may take before it counts as failed and is retried.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// How many times a request that failed for a reason that may pass is retried.
const RETRIES: u32 = 3;

/// The wait before the first retry; each later wait doubles.
const FIRST_BACKOFF: Duration = Duration::from_millis(100);

/// How a bucket is addressed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum PathStyle {
    /// `https://<bucket>.<endpoint host>/<key>`, as AWS recommends.
    #[default]
    VirtualHost,
    /// `https://<endpoint host>/<bucket>/<key>`, as MinIO and most self-hosted services need.
    Path,
}

/// Whether a plain `http://` endpoint may be used.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum HttpPolicy {
    /// Only `https://`.
    #[default]
    HttpsOnly,
    /// `http://` too, for a local MinIO and nothing else.
    AllowHttp,
}

/// Where a store's static credentials come from (ADR-0029, point 14).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CredentialSource {
    /// `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, and optionally `AWS_SESSION_TOKEN`, read
    /// once when the store is built.
    Environment,
    /// A TOML file with `access_key_id`, `secret_access_key`, and optionally `session_token`,
    /// which only its owner may read, re-read when it changes.
    File(PathBuf),
}

/// What an [`S3Store`] needs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct S3Settings {
    /// The bucket.
    pub bucket: String,
    /// The service's endpoint, such as `https://s3.eu-west-1.amazonaws.com`.
    pub endpoint: String,
    /// The region requests are signed for.
    pub region: String,
    /// How the bucket is addressed.
    pub path_style: PathStyle,
    /// Where the credentials come from.
    pub credentials: CredentialSource,
    /// A PEM bundle of extra authorities to trust, for a private endpoint.
    pub ca: Option<PathBuf>,
    /// Whether `http://` is allowed.
    pub http: HttpPolicy,
}

/// The error returned when an [`S3Store`] can't be built.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum S3ConfigError {
    /// The endpoint isn't a URL S3 requests can be sent to.
    #[error("invalid S3 endpoint '{endpoint}': {reason}")]
    InvalidEndpoint {
        /// The endpoint.
        endpoint: String,
        /// Why.
        reason: String,
    },
    /// The endpoint is `http://`, which needs `allow_http`.
    #[error("S3 endpoint '{endpoint}' is plain http, which needs allow_http = true")]
    HttpNotAllowed {
        /// The endpoint.
        endpoint: String,
    },
    /// The credentials are missing or can't be read.
    #[error("S3 credentials unavailable: {reason}")]
    Credentials {
        /// Why.
        reason: String,
    },
    /// The credentials file grants its group or others some permission.
    #[error("S3 credentials file {} has mode {mode:o}; only its owner may have access", path.display())]
    CredentialsShared {
        /// The file.
        path: PathBuf,
        /// Its permission bits.
        mode: u32,
    },
    /// The extra authority can't be read.
    #[error("S3 authority {}: {reason}", path.display())]
    Ca {
        /// The file.
        path: PathBuf,
        /// Why.
        reason: String,
    },
    /// TLS can't be set up.
    #[error("can't set up TLS for S3: {reason}")]
    Tls {
        /// Why.
        reason: String,
    },
}

/// A store in an S3-compatible bucket: AWS S3, MinIO, Cloudflare R2, Garage, and the like.
///
/// Each call is one request, or a few for a `list` that pages: signed with the time from the
/// store's clock, valid for a minute. A 5xx answer, a timeout of 30 s, or a connection failure
/// is retried three times with a doubling backoff from 100 ms, and then is
/// [`StoreError::Unavailable`]; any other 4xx is [`StoreError::Rejected`] with S3's error code.
/// A `get` of a missing key is `None` and a `delete` of one succeeds. Cloning is cheap.
#[derive(Clone)]
pub struct S3Store {
    inner: Arc<Inner>,
}

struct Inner {
    bucket: Bucket,
    client: HttpClient,
    credentials: CredentialStore,
    clock: Arc<dyn Clock>,
}

impl fmt::Debug for S3Store {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("S3Store")
            .field("bucket", &self.inner.bucket.name())
            .field("url", &self.inner.bucket.base_url().as_str())
            .field("region", &self.inner.bucket.region())
            .field("credentials", &self.inner.credentials)
            .finish_non_exhaustive()
    }
}

impl S3Store {
    /// A store over `settings`' bucket, signing with times from `clock`.
    ///
    /// # Errors
    ///
    /// [`S3ConfigError`] for an invalid endpoint, an `http://` endpoint without
    /// [`HttpPolicy::AllowHttp`], missing or unreadable credentials, a credentials file others
    /// can read, or an extra authority that can't be read.
    pub fn new(settings: S3Settings, clock: Arc<dyn Clock>) -> Result<Self, S3ConfigError> {
        let invalid = |reason: String| S3ConfigError::InvalidEndpoint {
            endpoint: settings.endpoint.clone(),
            reason,
        };
        let endpoint = match settings.endpoint.parse() {
            Ok(endpoint) => endpoint,
            Err(error) => return Err(invalid(describe(&error))),
        };
        let style = match settings.path_style {
            PathStyle::VirtualHost => UrlStyle::VirtualHost,
            PathStyle::Path => UrlStyle::Path,
        };
        let bucket = Bucket::new(
            endpoint,
            style,
            settings.bucket.clone(),
            settings.region.clone(),
        )
        .map_err(|error| invalid(format!("{error:?}")))?;
        match bucket.base_url().scheme() {
            "https" => {}
            "http" if settings.http == HttpPolicy::AllowHttp => {}
            "http" => {
                return Err(S3ConfigError::HttpNotAllowed {
                    endpoint: settings.endpoint,
                });
            }
            other => return Err(invalid(format!("unsupported scheme {other}"))),
        }
        let credentials = CredentialStore::new(&settings.credentials, Arc::clone(&clock))?;
        let client = http::client(settings.ca.as_deref(), settings.http)?;
        Ok(Self {
            inner: Arc::new(Inner {
                bucket,
                client,
                credentials,
                clock,
            }),
        })
    }
}

/// One request, ready to sign again for each attempt.
enum Call<'a> {
    Put(&'a str, Bytes),
    Get(&'a str),
    Delete(&'a str),
    List {
        prefix: &'a str,
        token: Option<&'a str>,
    },
}

/// A response the store understood.
struct Answer {
    status: StatusCode,
    body: Bytes,
}

impl Inner {
    /// The signed URL and method of `call`.
    fn sign(&self, call: &Call<'_>) -> (Method, String) {
        let credentials = self.credentials.current();
        let millis = i64::try_from(self.clock.now().as_millis()).unwrap_or(i64::MAX);
        let now = jiff::Timestamp::from_millisecond(millis).unwrap_or(jiff::Timestamp::UNIX_EPOCH);
        let credentials = Some(credentials.as_ref());
        match call {
            Call::Put(key, _) => (
                Method::PUT,
                self.bucket
                    .put_object(credentials, key)
                    .sign_with_time(SIGNED_FOR, &now)
                    .to_string(),
            ),
            Call::Get(key) => (
                Method::GET,
                self.bucket
                    .get_object(credentials, key)
                    .sign_with_time(SIGNED_FOR, &now)
                    .to_string(),
            ),
            Call::Delete(key) => (
                Method::DELETE,
                self.bucket
                    .delete_object(credentials, key)
                    .sign_with_time(SIGNED_FOR, &now)
                    .to_string(),
            ),
            Call::List { prefix, token } => {
                let mut list = ListObjectsV2::new(&self.bucket, credentials);
                list.with_prefix(*prefix);
                if let Some(token) = token {
                    list.with_continuation_token(*token);
                }
                (
                    Method::GET,
                    list.sign_with_time(SIGNED_FOR, &now).to_string(),
                )
            }
        }
    }

    /// Sends `call`, retrying what may pass, and returns the first answer that isn't a 5xx or a
    /// 429.
    async fn send(&self, call: &Call<'_>) -> Result<Answer, StoreError> {
        let mut backoff = FIRST_BACKOFF;
        let mut attempt = 0;
        loop {
            attempt += 1;
            let failure = match self.attempt(call).await {
                Ok(answer)
                    if answer.status.is_server_error()
                        || answer.status == StatusCode::TOO_MANY_REQUESTS =>
                {
                    format!("S3 answered {}{}", answer.status, error_code(&answer.body))
                }
                Ok(answer) => return Ok(answer),
                Err(reason) => reason,
            };
            if attempt > RETRIES {
                return Err(StoreError::Unavailable {
                    reason: format!("{failure} (after {attempt} attempts)"),
                });
            }
            tracing::debug!(reason = %failure, attempt, retry_in = ?backoff, "an S3 request failed");
            time::sleep(backoff).await;
            backoff *= 2;
        }
    }

    async fn attempt(&self, call: &Call<'_>) -> Result<Answer, String> {
        let (method, url) = self.sign(call);
        let body = match call {
            Call::Put(_, bytes) => bytes.clone(),
            _ => Bytes::new(),
        };
        let request = Request::builder()
            .method(method)
            .uri(url)
            .body(Full::new(body))
            .map_err(|error| format!("can't build the request: {error}"))?;
        let exchange = async {
            let response = self
                .client
                .request(request)
                .await
                .map_err(|error| format!("request failed: {error}"))?;
            let status = response.status();
            let body = response
                .into_body()
                .collect()
                .await
                .map_err(|error| format!("reading the answer failed: {error}"))?
                .to_bytes();
            Ok(Answer { status, body })
        };
        time::timeout(REQUEST_TIMEOUT, exchange)
            .await
            .map_err(|_| format!("no answer within {REQUEST_TIMEOUT:?}"))?
    }
}

fn describe(error: &dyn fmt::Display) -> String {
    error.to_string()
}

/// What a non-success answer turns into.
fn rejected(answer: &Answer) -> StoreError {
    StoreError::Rejected {
        reason: format!("S3 answered {}{}", answer.status, error_code(&answer.body)),
    }
}

/// `: <Code>` from an S3 error document, or nothing if there is none.
fn error_code(body: &[u8]) -> String {
    let text = String::from_utf8_lossy(body);
    text.split_once("<Code>")
        .and_then(|(_, rest)| rest.split_once("</Code>"))
        .map(|(code, _)| format!(": {code}"))
        .unwrap_or_default()
}

impl ObjectStore for S3Store {
    fn put(&self, key: &str, bytes: Vec<u8>) -> StoreFuture<'_, ()> {
        let key = key.to_owned();
        Box::pin(async move {
            check_key(&key)?;
            let answer = self
                .inner
                .send(&Call::Put(&key, Bytes::from(bytes)))
                .await?;
            if answer.status.is_success() {
                Ok(())
            } else {
                Err(rejected(&answer))
            }
        })
    }

    fn get(&self, key: &str) -> StoreFuture<'_, Option<Vec<u8>>> {
        let key = key.to_owned();
        Box::pin(async move {
            check_key(&key)?;
            let answer = self.inner.send(&Call::Get(&key)).await?;
            match answer.status {
                status if status.is_success() => Ok(Some(answer.body.to_vec())),
                StatusCode::NOT_FOUND => Ok(None),
                _ => Err(rejected(&answer)),
            }
        })
    }

    fn list(&self, prefix: &str) -> StoreFuture<'_, Vec<ObjectEntry>> {
        let prefix = prefix.to_owned();
        Box::pin(async move {
            check_prefix(&prefix)?;
            let mut found = Vec::new();
            let mut token: Option<String> = None;
            loop {
                let call = Call::List {
                    prefix: &prefix,
                    token: token.as_deref(),
                };
                let answer = self.inner.send(&call).await?;
                if !answer.status.is_success() {
                    return Err(rejected(&answer));
                }
                let text =
                    std::str::from_utf8(&answer.body).map_err(|error| StoreError::Unavailable {
                        reason: format!("a listing isn't UTF-8: {error}"),
                    })?;
                let page = ListObjectsV2::parse_response(text).map_err(|error| {
                    StoreError::Unavailable {
                        reason: format!("can't parse a listing: {error}"),
                    }
                })?;
                found.extend(page.contents.into_iter().map(|object| ObjectEntry {
                    size: object.size.unwrap_or(0),
                    key: object.key,
                }));
                match page.next_continuation_token {
                    Some(next) => token = Some(next),
                    None => break,
                }
            }
            found.retain(|entry| entry.key.starts_with(&prefix));
            found.sort();
            Ok(found)
        })
    }

    fn delete(&self, key: &str) -> StoreFuture<'_, ()> {
        let key = key.to_owned();
        Box::pin(async move {
            check_key(&key)?;
            let answer = self.inner.send(&Call::Delete(&key)).await?;
            if answer.status.is_success() || answer.status == StatusCode::NOT_FOUND {
                Ok(())
            } else {
                Err(rejected(&answer))
            }
        })
    }
}
