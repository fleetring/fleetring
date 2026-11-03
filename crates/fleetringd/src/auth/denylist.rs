//! The denylist a node checks tokens against, as the control plane last published it.

use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;
use fleetring_control::Revocations;
use tokio::time::Instant;

/// The revoked tokens and signing keys a node refuses, shared by its services.
///
/// A data node replaces the list with the one each heartbeat reply carries, so a revocation
/// reaches it within one heartbeat interval; the control node reads its own state. Reads take no
/// lock. It also remembers when it was last replaced, reading time from `tokio::time`, so a
/// node can tell a list it never received, or one gone stale, from an empty one. Cloning is
/// cheap; clones share the list.
#[derive(Debug, Clone, Default)]
pub struct Denylist {
    current: Arc<ArcSwap<Published>>,
}

#[derive(Debug, Default)]
struct Published {
    revocations: Arc<Revocations>,
    at: Option<Instant>,
}

impl Denylist {
    /// An empty list.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns the list as it is now.
    #[must_use]
    pub fn load(&self) -> Arc<Revocations> {
        Arc::clone(&self.current.load().revocations)
    }

    /// Replaces the list with `revocations`, as of now.
    pub fn replace(&self, revocations: Revocations) {
        self.current.store(Arc::new(Published {
            revocations: Arc::new(revocations),
            at: Some(Instant::now()),
        }));
    }

    /// How long ago the list was last replaced, or `None` if it never was.
    #[must_use]
    pub fn age(&self) -> Option<Duration> {
        self.current.load().at.map(|at| at.elapsed())
    }
}

#[cfg(test)]
mod tests {
    use fleetring_control::{Revoked, Timestamp};

    use super::*;

    #[test]
    fn clones_share_the_list() {
        let list = Denylist::new();
        let clone = list.clone();
        assert!(clone.load().is_empty());
        assert_eq!(clone.age(), None, "never received");
        let revoked = Revoked::Token("t".parse().expect("a token id"));
        list.replace([(revoked.clone(), Timestamp::MAX)].into_iter().collect());
        assert!(clone.load().contains(&revoked));
        assert!(clone.age().is_some());
    }
}
