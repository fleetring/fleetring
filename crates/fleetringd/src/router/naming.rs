//! Which database a request names, and which Hrana endpoint (ADR-0037, points 6 to 8).
//!
//! - **The path form:** `/db/<database>/` followed by the endpoint, with `<database>` the name
//!   percent-encoded as one path segment, so that any name fits, `/` and `:` included. Empty
//!   segments are ignored, since clients that join paths by concatenation produce them.
//! - **The host form,** when a host suffix is configured: `<label>.<suffix>` names the database
//!   `<label>`, for names that are DNS labels.

use fleetring_control::DatabaseName;
use hyper::Method;

use crate::router::config::is_dns_label;

/// A Hrana version.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum Hrana {
    /// Hrana 2: no `get_autocommit`, no `is_autocommit` condition, no cursor.
    V2,
    /// Hrana 3.
    V3,
}

/// A Hrana endpoint under a database.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Endpoint {
    /// `GET v2` or `GET v3`: the version probe.
    Version,
    /// `POST v2/pipeline` or `POST v3/pipeline`.
    Pipeline(Hrana),
    /// `POST v3/cursor`.
    Cursor,
}

/// The database a request names, the endpoint under it, and the path that named the database,
/// which a `base_url` repeats.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Target {
    pub(crate) database: DatabaseName,
    pub(crate) endpoint: Endpoint,
    /// `/db/<encoded name>` for the path form, and empty for the host form.
    pub(crate) prefix: String,
}

/// Why a request names no database and endpoint, for its `404`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct NotFound(pub(crate) String);

/// The target of a request with `method` on `path` (without its query) for `host`, given the
/// configured `host_suffix`, or why it has none.
pub(crate) fn target(
    method: &Method,
    path: &str,
    host: Option<&str>,
    host_suffix: Option<&str>,
    upgrade: Upgrade,
) -> Result<Target, NotFound> {
    if upgrade == Upgrade::WebSocket {
        return Err(NotFound(
            "this router serves Hrana over HTTP only, not over WebSocket".to_owned(),
        ));
    }
    let segments: Vec<&str> = path
        .split('/')
        .filter(|segment| !segment.is_empty())
        .collect();
    let (database, prefix, rest) = match segments.as_slice() {
        ["db", encoded, rest @ ..] => {
            let name = decode(encoded).ok_or_else(|| not_found(host_suffix))?;
            let database = name.parse().map_err(|_| not_found(host_suffix))?;
            (database, format!("/db/{encoded}"), rest)
        }
        rest => {
            let database =
                host_database(host, host_suffix).ok_or_else(|| not_found(host_suffix))?;
            (database, String::new(), rest)
        }
    };
    let endpoint = match (method, rest) {
        (&Method::GET, ["v2" | "v3"]) => Endpoint::Version,
        (&Method::POST, ["v2", "pipeline"]) => Endpoint::Pipeline(Hrana::V2),
        (&Method::POST, ["v3", "pipeline"]) => Endpoint::Pipeline(Hrana::V3),
        (&Method::POST, ["v3", "cursor"]) => Endpoint::Cursor,
        _ => {
            return Err(NotFound(format!(
                "this router serves GET v2 and v3, POST v2/pipeline and v3/pipeline, and POST \
                 v3/cursor under {}; Hrana over WebSocket and the protobuf encoding are not served",
                forms(host_suffix)
            )));
        }
    };
    Ok(Target {
        database,
        endpoint,
        prefix,
    })
}

/// Whether a request asks to upgrade its connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Upgrade {
    /// It doesn't.
    None,
    /// It asks for a WebSocket.
    WebSocket,
}

/// The database `host` names under `host_suffix`, if it is `<label>.<suffix>`.
fn host_database(host: Option<&str>, host_suffix: Option<&str>) -> Option<DatabaseName> {
    let suffix = host_suffix?;
    let host = host?.to_ascii_lowercase();
    // A port, if any, comes after the last colon; a DNS name has no other.
    let host = host
        .rsplit_once(':')
        .map_or(host.as_str(), |(name, _)| name);
    let label = host.strip_suffix(suffix)?.strip_suffix('.')?;
    is_dns_label(label).then(|| label.parse().ok())?
}

fn not_found(host_suffix: Option<&str>) -> NotFound {
    NotFound(format!(
        "name the database in {}, such as /db/<percent-encoded name>/v3/pipeline",
        forms(host_suffix)
    ))
}

/// The ways a request can name its database.
fn forms(host_suffix: Option<&str>) -> String {
    match host_suffix {
        Some(suffix) => {
            format!("the path, /db/<percent-encoded name>/, or the host, <name>.{suffix}")
        }
        None => "the path, /db/<percent-encoded name>/".to_owned(),
    }
}

/// `segment` with its percent-encoded bytes decoded, if they decode to UTF-8.
fn decode(segment: &str) -> Option<String> {
    let bytes = segment.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while let Some(&byte) = bytes.get(index) {
        if byte == b'%' {
            let high = hex(*bytes.get(index + 1)?)?;
            let low = hex(*bytes.get(index + 2)?)?;
            out.push(high << 4 | low);
            index += 3;
        } else {
            out.push(byte);
            index += 1;
        }
    }
    String::from_utf8(out).ok()
}

fn hex(digit: u8) -> Option<u8> {
    match digit {
        b'0'..=b'9' => Some(digit - b'0'),
        b'a'..=b'f' => Some(digit - b'a' + 10),
        b'A'..=b'F' => Some(digit - b'A' + 10),
        _ => None,
    }
}

/// `name` percent-encoded as one path segment: every byte but unreserved characters
/// (RFC 3986) is encoded.
#[cfg(test)]
pub(crate) fn encode(name: &str) -> String {
    let mut out = String::with_capacity(name.len());
    for byte in name.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~') {
            out.push(char::from(byte));
        } else {
            out.push_str(&format!("%{byte:02X}"));
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn post(path: &str) -> Result<Target, NotFound> {
        target(&Method::POST, path, None, None, Upgrade::None)
    }

    #[test]
    fn the_path_form_decodes_any_name() {
        let named = post("/db/tenant%3Aacme%2Fnotes/v3/pipeline").expect("a target");
        assert_eq!(named.database.as_str(), "tenant:acme/notes");
        assert_eq!(named.endpoint, Endpoint::Pipeline(Hrana::V3));
        assert_eq!(named.prefix, "/db/tenant%3Aacme%2Fnotes");
        let doubled = post("//db//notes//v2//pipeline").expect("doubled slashes");
        assert_eq!(doubled.database.as_str(), "notes");
        assert_eq!(doubled.endpoint, Endpoint::Pipeline(Hrana::V2));
        assert_eq!(
            post("/db/notes/v3/cursor").expect("cursor").endpoint,
            Endpoint::Cursor
        );
        let probe = target(&Method::GET, "/db/notes/v3", None, None, Upgrade::None);
        assert_eq!(probe.expect("probe").endpoint, Endpoint::Version);
        assert_eq!(encode("tenant:acme/notes"), "tenant%3Aacme%2Fnotes");
        assert_eq!(decode(&encode("a b/ç")).as_deref(), Some("a b/ç"));
    }

    #[test]
    fn the_host_form_takes_dns_labels_only() {
        let suffix = Some("db.example.com");
        let host = |host: &str| {
            target(
                &Method::POST,
                "/v3/pipeline",
                Some(host),
                suffix,
                Upgrade::None,
            )
        };
        let named = host("notes.db.example.com:8080").expect("a label");
        assert_eq!(named.database.as_str(), "notes");
        assert_eq!(named.prefix, "");
        assert!(
            host("Notes.DB.example.com").is_ok(),
            "hosts compare without case"
        );
        for refused in [
            "a.b.db.example.com",
            "-x.db.example.com",
            "db.example.com",
            "x.example.com",
        ] {
            assert!(host(refused).is_err(), "{refused}");
        }
        // Without a suffix, the host names nothing.
        let without = target(
            &Method::POST,
            "/v3/pipeline",
            Some("notes.db.example.com"),
            None,
            Upgrade::None,
        );
        assert!(without.is_err());
    }

    #[test]
    fn anything_else_names_both_forms() {
        let suffix = Some("db.example.com");
        for (method, path) in [
            (Method::GET, "/"),
            (Method::POST, "/db/notes/v4/pipeline"),
            (Method::GET, "/db/notes/v3/pipeline"),
            (Method::POST, "/db/notes/v2/cursor"),
            (Method::POST, "/db/notes/v3-protobuf/pipeline"),
            (Method::POST, "/v1/execute"),
            (Method::POST, "/db/%zz/v3/pipeline"),
        ] {
            let NotFound(message) =
                target(&method, path, None, suffix, Upgrade::None).expect_err(path);
            assert!(
                message.contains("/db/<percent-encoded name>/"),
                "{path}: {message}"
            );
            assert!(
                message.contains("<name>.db.example.com"),
                "{path}: {message}"
            );
        }
        let socket = target(&Method::GET, "/db/notes/v3", None, None, Upgrade::WebSocket);
        assert!(socket.expect_err("websocket").0.contains("over HTTP only"));
    }
}
