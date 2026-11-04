//! An in-process fake S3: path-style `PUT`, `GET`, and `DELETE` of objects, and `ListObjectsV2`
//! with two keys a page, over plain HTTP/1.1, keeping objects in memory. It refuses a request
//! that isn't presigned, and can be told to fail or deny the next requests.

use std::collections::BTreeMap;
use std::convert::Infallible;
use std::fmt::Write as _;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex, MutexGuard};

use http_body_util::{BodyExt as _, Full};
use hyper::body::{Bytes, Incoming};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

/// How many keys a listing page holds, small so that the store's paging runs.
const PAGE: usize = 2;

#[derive(Debug, Default)]
struct State {
    objects: BTreeMap<String, Vec<u8>>,
    fail_next: u32,
    deny_next: u32,
    requests: u64,
}

/// The fake's handle. Dropping it stops the server.
#[derive(Debug)]
pub(crate) struct FakeS3 {
    state: Arc<Mutex<State>>,
    server: JoinHandle<()>,
}

impl Drop for FakeS3 {
    fn drop(&mut self) {
        self.server.abort();
    }
}

impl FakeS3 {
    /// Starts the fake on a free local port, serving one bucket, `bucket`.
    pub(crate) async fn start(bucket: &str) -> std::io::Result<(SocketAddr, Self)> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let state = Arc::new(Mutex::new(State::default()));
        let bucket = bucket.to_owned();
        let shared = Arc::clone(&state);
        let server = tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    return;
                };
                let state = Arc::clone(&shared);
                let bucket = bucket.clone();
                tokio::spawn(async move {
                    let service = service_fn(move |request| {
                        let state = Arc::clone(&state);
                        let bucket = bucket.clone();
                        async move { Ok::<_, Infallible>(serve(&state, &bucket, request).await) }
                    });
                    let _ = http1::Builder::new()
                        .serve_connection(TokioIo::new(stream), service)
                        .await;
                });
            }
        });
        Ok((address, Self { state, server }))
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().expect("fake s3 lock")
    }

    /// Answers the next `n` requests with 503.
    pub(crate) fn fail_next(&self, n: u32) {
        self.lock().fail_next = n;
    }

    /// Answers the next `n` requests with 403 `AccessDenied`.
    pub(crate) fn deny_next(&self, n: u32) {
        self.lock().deny_next = n;
    }

    /// How many requests have arrived.
    pub(crate) fn requests(&self) -> u64 {
        self.lock().requests
    }
}

fn answer(status: StatusCode, body: impl Into<Bytes>) -> Response<Full<Bytes>> {
    let mut response = Response::new(Full::new(body.into()));
    *response.status_mut() = status;
    response
}

fn error(status: StatusCode, code: &str) -> Response<Full<Bytes>> {
    answer(
        status,
        format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?><Error><Code>{code}</Code><Message>fake</Message></Error>"
        ),
    )
}

async fn serve(
    state: &Mutex<State>,
    bucket: &str,
    request: Request<Incoming>,
) -> Response<Full<Bytes>> {
    let query: BTreeMap<String, String> = request
        .uri()
        .query()
        .unwrap_or_default()
        .split('&')
        .filter(|pair| !pair.is_empty())
        .map(|pair| {
            let (name, value) = pair.split_once('=').unwrap_or((pair, ""));
            (decode(name), decode(value))
        })
        .collect();
    {
        let mut state = state.lock().expect("fake s3 lock");
        state.requests += 1;
        if state.fail_next > 0 {
            state.fail_next -= 1;
            return error(StatusCode::SERVICE_UNAVAILABLE, "SlowDown");
        }
        if state.deny_next > 0 {
            state.deny_next -= 1;
            return error(StatusCode::FORBIDDEN, "AccessDenied");
        }
    }
    if !query.contains_key("X-Amz-Signature") || !query.contains_key("X-Amz-Credential") {
        return error(StatusCode::FORBIDDEN, "AccessDenied");
    }
    let path = request.uri().path().to_owned();
    let Some(rest) = path.strip_prefix(&format!("/{bucket}")) else {
        return error(StatusCode::NOT_FOUND, "NoSuchBucket");
    };
    let method = request.method().clone();
    let body = match request.into_body().collect().await {
        Ok(body) => body.to_bytes(),
        Err(_) => return error(StatusCode::BAD_REQUEST, "IncompleteBody"),
    };
    let mut state = state.lock().expect("fake s3 lock");
    match (method, rest) {
        (Method::GET, "" | "/") if query.get("list-type").map(String::as_str) == Some("2") => {
            list(&state, bucket, &query)
        }
        (method, key) if key.len() > 1 => {
            let key = decode(&key[1..]);
            match method {
                Method::PUT => {
                    state.objects.insert(key, body.to_vec());
                    answer(StatusCode::OK, Bytes::new())
                }
                Method::GET => match state.objects.get(&key) {
                    Some(bytes) => answer(StatusCode::OK, bytes.clone()),
                    None => error(StatusCode::NOT_FOUND, "NoSuchKey"),
                },
                Method::DELETE => {
                    state.objects.remove(&key);
                    answer(StatusCode::NO_CONTENT, Bytes::new())
                }
                _ => error(StatusCode::METHOD_NOT_ALLOWED, "MethodNotAllowed"),
            }
        }
        _ => error(StatusCode::BAD_REQUEST, "InvalidRequest"),
    }
}

/// A `ListObjectsV2` page, keys URL-encoded as `encoding-type=url` asks.
fn list(state: &State, bucket: &str, query: &BTreeMap<String, String>) -> Response<Full<Bytes>> {
    let prefix = query.get("prefix").cloned().unwrap_or_default();
    let after = query
        .get("continuation-token")
        .map(|token| String::from_utf8(unhex(token)).unwrap_or_default());
    let matching: Vec<(&String, &Vec<u8>)> = state
        .objects
        .iter()
        .filter(|(key, _)| key.starts_with(&prefix))
        .filter(|(key, _)| after.as_ref().is_none_or(|after| *key > after))
        .collect();
    let page = &matching[..matching.len().min(PAGE)];
    let truncated = matching.len() > PAGE;
    let mut xml = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Name>{bucket}</Name><Prefix>{}</Prefix><KeyCount>{}</KeyCount><MaxKeys>{PAGE}</MaxKeys><EncodingType>url</EncodingType><IsTruncated>{truncated}</IsTruncated>",
        encode(&prefix),
        page.len()
    );
    for (key, bytes) in page {
        let _ = write!(
            xml,
            "<Contents><Key>{}</Key><Size>{}</Size><StorageClass>STANDARD</StorageClass></Contents>",
            encode(key),
            bytes.len()
        );
    }
    if truncated && let Some((last, _)) = page.last() {
        let _ = write!(
            xml,
            "<NextContinuationToken>{}</NextContinuationToken>",
            hex(last.as_bytes())
        );
    }
    xml.push_str("</ListBucketResult>");
    answer(StatusCode::OK, xml)
}

/// Percent-decodes `text`.
fn decode(text: &str) -> String {
    let bytes = text.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut at = 0;
    while at < bytes.len() {
        if bytes[at] == b'%'
            && let Some(byte) = text
                .get(at + 1..at + 3)
                .and_then(|pair| u8::from_str_radix(pair, 16).ok())
        {
            out.push(byte);
            at += 3;
        } else {
            out.push(bytes[at]);
            at += 1;
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// Percent-encodes everything in `text` but unreserved characters and `/`, as S3 does for
/// `encoding-type=url`.
fn encode(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for byte in text.bytes() {
        if byte.is_ascii_alphanumeric() || b"-_.~/".contains(&byte) {
            out.push(char::from(byte));
        } else {
            let _ = write!(out, "%{byte:02X}");
        }
    }
    out
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().fold(String::new(), |mut out, byte| {
        let _ = write!(out, "{byte:02x}");
        out
    })
}

fn unhex(text: &str) -> Vec<u8> {
    (0..text.len() / 2)
        .filter_map(|at| u8::from_str_radix(text.get(2 * at..2 * at + 2)?, 16).ok())
        .collect()
}
