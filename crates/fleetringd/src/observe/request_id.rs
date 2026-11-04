//! Request IDs: one per request, kept from `x-request-id` or made from a counter, and carried on
//! the calls a node makes while serving the request (ADR-0034 point 8).

use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use fleetring::NodeId;

/// The metadata key a request ID travels in.
pub const REQUEST_ID_KEY: &str = "x-request-id";

/// The longest incoming request ID that is kept.
const MAX_LEN: usize = 64;

/// A request's ID: kept from `x-request-id`, or generated as `<node-id>-<16 hex digits>`.
///
/// Cloning is cheap.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestId(Arc<str>);

impl RequestId {
    /// The ID as text.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for RequestId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Makes request IDs for one daemon: `<node-id>-<16 hex digits>` from a counter.
///
/// The counter needs no randomness, so a simulation seed replays the same IDs (ADR-0020), and the
/// node ID prefix keeps IDs unique across a fleet's logs.
#[derive(Debug)]
pub(crate) struct RequestIds {
    node: NodeId,
    next: AtomicU64,
}

impl RequestIds {
    pub(crate) fn new(node: NodeId) -> Self {
        Self {
            node,
            next: AtomicU64::new(1),
        }
    }

    /// `incoming` if it is 1 to 64 characters of `[A-Za-z0-9._-]`, else a new ID.
    pub(crate) fn resolve(&self, incoming: Option<&str>) -> RequestId {
        match incoming {
            Some(id) if is_valid(id) => RequestId(Arc::from(id)),
            _ => {
                let n = self.next.fetch_add(1, Ordering::Relaxed);
                RequestId(Arc::from(format!("{}-{n:016x}", self.node)))
            }
        }
    }
}

fn is_valid(id: &str) -> bool {
    (1..=MAX_LEN).contains(&id.len())
        && id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'))
}

tokio::task_local! {
    /// The ID of the request the current task serves, if any.
    pub(crate) static CURRENT: RequestId;
}

/// The current task's request ID, if it serves a request.
pub(crate) fn current() -> Option<RequestId> {
    CURRENT.try_with(Clone::clone).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ids(node: &str) -> RequestIds {
        RequestIds::new(node.parse().expect("a node id"))
    }

    #[test]
    fn an_incoming_request_id_is_kept_when_valid() {
        let ids = ids("n1");
        assert_eq!(ids.resolve(Some("abc-1.2_X")).as_str(), "abc-1.2_X");
        let longest = "a".repeat(64);
        assert_eq!(ids.resolve(Some(&longest)).as_str(), longest);
    }

    #[test]
    fn an_invalid_request_id_is_replaced() {
        let ids = ids("n1");
        let too_long = "a".repeat(65);
        let invalid = [
            Some(too_long.as_str()),
            Some("a b"),
            Some("a/b"),
            Some(""),
            None,
        ];
        for (n, incoming) in invalid.into_iter().enumerate() {
            assert_eq!(
                ids.resolve(incoming).as_str(),
                format!("n1-{:016x}", n + 1),
                "{incoming:?}"
            );
        }
    }

    #[test]
    fn generated_ids_count_up_per_daemon() {
        let first = ids("a");
        let second = ids("b");
        assert_eq!(first.resolve(None).as_str(), "a-0000000000000001");
        assert_eq!(first.resolve(None).as_str(), "a-0000000000000002");
        assert_eq!(second.resolve(None).as_str(), "b-0000000000000001");
    }

    #[tokio::test]
    async fn the_current_request_id_is_the_scope_s() {
        assert_eq!(current(), None);
        let id = ids("n1").resolve(Some("req-1"));
        let seen = CURRENT.scope(id.clone(), async { current() }).await;
        assert_eq!(seen, Some(id));
    }
}
