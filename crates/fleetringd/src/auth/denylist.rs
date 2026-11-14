//! The denylist a node checks tokens against, as the control plane last published it.

use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;
use fleetring_control::Revocations;
use tokio::time::Instant;

/// The revoked tokens, signing keys, and principals a node refuses, shared by its services.
///
/// A data node replaces the list with the one each join or heartbeat reply carries, so a
/// revocation reaches it within one heartbeat interval; the control node reads its own state.
/// Reads take no lock. It also remembers when it was last replaced, reading time from
/// `tokio::time`, so a node can tell a list it never received, or one gone stale, from an empty
/// one. Cloning is cheap; clones share the list.
///
/// A list holding an entry the node can't read is no list (ADR-0047, point 3): it is kept as
/// [`Unusable`], and every token is refused until a list the node reads wholly arrives.
/// Skipping the entry instead would let a node that can't read a principal's revocation accept
/// every token it should refuse.
#[derive(Debug, Clone, Default)]
pub struct Denylist {
    current: Arc<ArcSwap<Published>>,
}

#[derive(Debug, Default)]
struct Published {
    listing: Listing,
    at: Option<Instant>,
}

#[derive(Debug)]
enum Listing {
    Usable(Arc<Revocations>),
    Unusable(Unusable),
}

impl Default for Listing {
    fn default() -> Self {
        Self::Usable(Arc::default())
    }
}

/// A denylist entry this node can't read: its case is absent, unknown to this release, or
/// malformed, or it revokes another fleet's principal, which no 0.8 node reads.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("denylist entry {index} is unreadable: {reason}")]
pub struct UnreadableEntry {
    index: usize,
    reason: String,
}

impl UnreadableEntry {
    /// The entry at `index` of the list, unreadable for `reason`.
    #[must_use]
    pub fn new(index: usize, reason: impl Into<String>) -> Self {
        Self {
            index,
            reason: reason.into(),
        }
    }

    /// Where the entry sits in the list, counting from 0.
    #[must_use]
    pub fn index(&self) -> usize {
        self.index
    }

    /// Why it can't be read.
    #[must_use]
    pub fn reason(&self) -> &str {
        &self.reason
    }
}

/// A denylist that held an entry this node can't read, so every token is refused.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Unusable {
    since: Instant,
    entry: UnreadableEntry,
}

impl Unusable {
    /// When the lists the node received stopped being readable: the first unreadable list since
    /// the last readable one.
    #[must_use]
    pub fn since(&self) -> Instant {
        self.since
    }

    /// The first entry of the latest list the node couldn't read.
    #[must_use]
    pub fn entry(&self) -> &UnreadableEntry {
        &self.entry
    }
}

impl Denylist {
    /// An empty list.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns the list as it is now, or why it can't be used.
    ///
    /// # Errors
    ///
    /// [`Unusable`] while the latest list received held an entry this node can't read.
    pub fn load(&self) -> Result<Arc<Revocations>, Unusable> {
        match &self.current.load().listing {
            Listing::Usable(revocations) => Ok(Arc::clone(revocations)),
            Listing::Unusable(unusable) => Err(unusable.clone()),
        }
    }

    /// How many entries the list holds, or `None` while it is unusable.
    #[must_use]
    pub fn entries(&self) -> Option<usize> {
        match &self.current.load().listing {
            Listing::Usable(revocations) => Some(revocations.len()),
            Listing::Unusable(_) => None,
        }
    }

    /// Replaces the list with `revocations`, as of now.
    pub fn replace(&self, revocations: Revocations) {
        self.receive(Ok(revocations));
    }

    /// Replaces the list with `received`, as of now: the list a join or heartbeat reply carried,
    /// or the first entry of it this node couldn't read, which makes the list unusable until a
    /// readable one arrives.
    ///
    /// The unreadable entry is logged once per distinct entry, not once per heartbeat, and the
    /// first readable list after it once.
    pub fn receive(&self, received: Result<Revocations, UnreadableEntry>) {
        let previous = self.current.load();
        let listing = match received {
            Ok(revocations) => {
                if let Listing::Unusable(unusable) = &previous.listing {
                    tracing::info!(
                        unreadable_for_ms =
                            u64::try_from(unusable.since.elapsed().as_millis()).unwrap_or(u64::MAX),
                        "the token denylist is readable again; tokens are checked against it"
                    );
                }
                Listing::Usable(Arc::new(revocations))
            }
            Err(entry) => {
                let since = match &previous.listing {
                    Listing::Unusable(unusable) if unusable.entry == entry => unusable.since,
                    Listing::Unusable(unusable) => {
                        log_unreadable(&entry);
                        unusable.since
                    }
                    Listing::Usable(_) => {
                        log_unreadable(&entry);
                        Instant::now()
                    }
                };
                Listing::Unusable(Unusable { since, entry })
            }
        };
        self.current.store(Arc::new(Published {
            listing,
            at: Some(Instant::now()),
        }));
    }

    /// How long ago the list was last replaced, or `None` if it never was.
    #[must_use]
    pub fn age(&self) -> Option<Duration> {
        self.current.load().at.map(|at| at.elapsed())
    }
}

fn log_unreadable(entry: &UnreadableEntry) {
    tracing::error!(
        index = entry.index,
        reason = entry.reason,
        "the token denylist holds an entry this node can't read; every token is refused until \
         a readable list arrives (ADR-0047, point 3)"
    );
}

#[cfg(test)]
mod tests {
    use fleetring_control::{Revoked, Timestamp};

    use super::*;

    #[test]
    fn clones_share_the_list() {
        let list = Denylist::new();
        let clone = list.clone();
        assert!(clone.load().expect("usable").is_empty());
        assert_eq!(clone.age(), None, "never received");
        let revoked = Revoked::Token("t".parse().expect("a token id"));
        list.replace([(revoked.clone(), Timestamp::MAX)].into_iter().collect());
        assert!(clone.load().expect("usable").contains(&revoked));
        assert_eq!(clone.entries(), Some(1));
        assert!(clone.age().is_some());
    }

    #[tokio::test]
    async fn an_unreadable_entry_makes_the_whole_list_unusable_until_a_readable_one() {
        let list = Denylist::new();
        let revoked = Revoked::Token("t".parse().expect("a token id"));
        list.replace([(revoked.clone(), Timestamp::MAX)].into_iter().collect());

        let entry = UnreadableEntry::new(1, "a revocation names a token, a key, or a principal");
        list.receive(Err(entry.clone()));
        let unusable = list.load().expect_err("unusable");
        assert_eq!(unusable.entry(), &entry);
        assert_eq!(list.entries(), None);
        assert!(list.age().is_some(), "a list arrived, unreadable as it is");

        // The same entry again keeps when the list stopped being readable.
        let since = unusable.since();
        list.receive(Err(entry));
        assert_eq!(list.load().expect_err("still unusable").since(), since);

        list.replace([(revoked.clone(), Timestamp::MAX)].into_iter().collect());
        assert!(list.load().expect("usable again").contains(&revoked));
    }
}
