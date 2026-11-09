//! Where each database's primary is, as the router last heard (ADR-0037, point 20).
//!
//! An entry lives for the configured time-to-live unless a redirect names a new primary first,
//! and the cache holds a bounded number of entries, evicting the oldest. A miss is filled by
//! `Control.Locate` with the caller's own token, so the control plane answers only what the
//! caller may read. Time is `tokio::time`'s, so a simulation stays deterministic.

use std::collections::BTreeMap;
use std::sync::Mutex;
use std::time::Duration;

use fleetring_control::DatabaseName;
use fleetring_proto::v1::LocateRequest;
use tokio::time::Instant;
use tonic::Status;

use crate::control::ControlClient;
use crate::transport::Credentials;

/// The primaries of recently used databases.
#[derive(Debug)]
pub(crate) struct PlacementCache {
    ttl: Duration,
    capacity: usize,
    entries: Mutex<Entries>,
}

#[derive(Debug, Default)]
struct Entries {
    by_name: BTreeMap<DatabaseName, Cached>,
    /// Each entry's name, by when it was stored, oldest first.
    by_age: BTreeMap<(Instant, u64), DatabaseName>,
    /// Tells apart entries stored at the same instant.
    stored: u64,
}

#[derive(Debug, Clone)]
struct Cached {
    /// The primary's `host:port`, or `None` while the database has none, as during a promotion.
    primary: Option<String>,
    age: (Instant, u64),
}

impl PlacementCache {
    /// A cache whose entries live `ttl`, holding at most `capacity`.
    pub(crate) fn new(ttl: Duration, capacity: usize) -> Self {
        Self {
            ttl,
            capacity: capacity.max(1),
            entries: Mutex::default(),
        }
    }

    /// The primary's address `database` has, from the cache if a fresh entry has it, or else from
    /// the control plane, asked with `credentials`. `None` while the database has no primary.
    ///
    /// # Errors
    ///
    /// The control plane's status: `NOT_FOUND` for a database it doesn't know, and
    /// `PERMISSION_DENIED` for one the caller may not read.
    pub(crate) async fn primary(
        &self,
        database: &DatabaseName,
        control: &ControlClient,
        credentials: &Credentials,
    ) -> Result<Option<String>, Status> {
        if let Some(primary) = self.fresh(database) {
            return Ok(Some(primary));
        }
        let located = control
            .locate_as(
                LocateRequest {
                    database: database.to_string(),
                },
                credentials,
            )
            .await?;
        let placement = located
            .placement
            .ok_or_else(|| Status::internal("the control plane sent no placement"))?;
        let primary = placement.primary.as_deref().and_then(|primary| {
            placement
                .replicas
                .iter()
                .find(|replica| replica.node_id == primary)
                .and_then(|replica| replica.address.clone())
        });
        // A database without a primary is asked again next time, not cached.
        if let Some(address) = &primary {
            self.store(database, Some(address.clone()));
        }
        Ok(primary)
    }

    /// The fresh entry's primary for `database`, if there is one.
    fn fresh(&self, database: &DatabaseName) -> Option<String> {
        let entries = self.lock();
        let cached = entries.by_name.get(database)?;
        let (stored, _) = cached.age;
        (Instant::now().saturating_duration_since(stored) < self.ttl)
            .then(|| cached.primary.clone())
            .flatten()
    }

    /// Records that `primary` is `database`'s primary now, as a redirect named it.
    pub(crate) fn redirected(&self, database: &DatabaseName, primary: String) {
        self.store(database, Some(primary));
    }

    /// Forgets `database`'s entry, so that the next request asks the control plane.
    pub(crate) fn forget(&self, database: &DatabaseName) {
        let mut entries = self.lock();
        if let Some(cached) = entries.by_name.remove(database) {
            entries.by_age.remove(&cached.age);
        }
    }

    fn store(&self, database: &DatabaseName, primary: Option<String>) {
        let mut entries = self.lock();
        entries.stored += 1;
        let age = (Instant::now(), entries.stored);
        if let Some(previous) = entries
            .by_name
            .insert(database.clone(), Cached { primary, age })
        {
            entries.by_age.remove(&previous.age);
        }
        entries.by_age.insert(age, database.clone());
        while entries.by_name.len() > self.capacity {
            let Some((_, oldest)) = entries.by_age.pop_first() else {
                break;
            };
            entries.by_name.remove(&oldest);
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Entries> {
        self.entries.lock().expect("placement cache lock poisoned")
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.lock().by_name.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn name(text: &str) -> DatabaseName {
        text.parse().expect("a name")
    }

    #[tokio::test(start_paused = true)]
    async fn entries_expire_after_the_ttl() {
        let cache = PlacementCache::new(Duration::from_secs(60), 10);
        cache.redirected(&name("a"), "10.0.0.1:7400".to_owned());
        assert_eq!(cache.fresh(&name("a")).as_deref(), Some("10.0.0.1:7400"));
        tokio::time::advance(Duration::from_secs(61)).await;
        assert_eq!(cache.fresh(&name("a")), None);
    }

    #[tokio::test(start_paused = true)]
    async fn the_oldest_entry_is_evicted_past_capacity() {
        let cache = PlacementCache::new(Duration::from_secs(60), 2);
        for database in ["a", "b", "c"] {
            cache.redirected(&name(database), format!("{database}:1"));
        }
        assert_eq!(cache.len(), 2);
        assert_eq!(cache.fresh(&name("a")), None, "the oldest went first");
        assert!(cache.fresh(&name("c")).is_some());
    }

    #[tokio::test(start_paused = true)]
    async fn a_redirect_replaces_the_entry() {
        let cache = PlacementCache::new(Duration::from_secs(60), 10);
        cache.redirected(&name("a"), "old:1".to_owned());
        cache.redirected(&name("a"), "new:1".to_owned());
        assert_eq!(cache.fresh(&name("a")).as_deref(), Some("new:1"));
        assert_eq!(cache.len(), 1);
        cache.forget(&name("a"));
        assert_eq!(cache.fresh(&name("a")), None);
    }
}
