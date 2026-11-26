//! openraft's log storage over `control.db` format 2 ([`ControlStore`], ADR-0040 point 3).
//!
//! [`RaftLog`] is a cheap handle to the store, shared with the state machine. Every call runs
//! where [`StorageIo`] says: on Tokio's blocking pool in production, so SQLite's syncs stay off
//! the executor, or inline under simulation, where one thread keeps a run deterministic
//! (ADR-0020).

use std::fmt::Debug;
use std::io;
use std::ops::{Bound, RangeBounds};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use openraft::storage::{IOFlushed, LogState, RaftLogReader, RaftLogStorage};
use openraft::{OptionalSend, RaftTypeConfig};

use crate::control::LogError;
use crate::control::node::StorageIo;
use crate::control::raft::{ControlTypes, Entry, LogId, Vote};
use crate::control::store::ControlStore;
use crate::observe::ControlMetrics;

/// The store, shared by the log storage and the state machine.
pub(crate) type SharedStore = Arc<Mutex<ControlStore>>;

/// Runs `call` on the store where `io` says, and turns its error into openraft's.
pub(crate) async fn with_store<T, F>(store: &SharedStore, io: StorageIo, call: F) -> io::Result<T>
where
    T: Send + 'static,
    F: FnOnce(&mut ControlStore) -> Result<T, LogError> + Send + 'static,
{
    match io {
        StorageIo::Inline => {
            let mut store = store.lock().expect("control store lock poisoned");
            call(&mut store).map_err(io::Error::other)
        }
        StorageIo::DedicatedThread => {
            let store = Arc::clone(store);
            tokio::task::spawn_blocking(move || {
                let mut store = store.lock().expect("control store lock poisoned");
                call(&mut store)
            })
            .await
            .map_err(io::Error::other)?
            .map_err(io::Error::other)
        }
    }
}

/// openraft's log storage and log reader over the control store.
#[derive(Debug, Clone)]
pub(crate) struct RaftLog {
    store: SharedStore,
    io: StorageIo,
    metrics: ControlMetrics,
    appends: Arc<AtomicU64>,
}

impl RaftLog {
    /// Returns the log storage over `store`, running its calls where `io` says and timing each
    /// append in `metrics`.
    pub(crate) fn new(store: SharedStore, io: StorageIo, metrics: ControlMetrics) -> Self {
        Self {
            store,
            io,
            metrics,
            appends: Arc::new(AtomicU64::new(0)),
        }
    }

    /// How many appends this log storage made: each is one transaction, with one sync.
    #[cfg(test)]
    pub(crate) fn appends(&self) -> u64 {
        self.appends.load(Ordering::Relaxed)
    }
}

/// An owned copy of a range's bounds, which can cross into a blocking task.
fn owned<R: RangeBounds<u64>>(range: &R) -> (Bound<u64>, Bound<u64>) {
    (range.start_bound().cloned(), range.end_bound().cloned())
}

impl RaftLogReader<ControlTypes> for RaftLog {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry>, io::Error> {
        let range = owned(&range);
        with_store(&self.store, self.io, move |store| store.entries(range)).await
    }

    async fn read_vote(&mut self) -> Result<Option<Vote>, io::Error> {
        with_store(&self.store, self.io, |store| store.vote()).await
    }
}

impl RaftLogStorage<ControlTypes> for RaftLog {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<ControlTypes>, io::Error> {
        with_store(&self.store, self.io, |store| {
            Ok(LogState {
                last_purged_log_id: store.purged()?,
                last_log_id: store.last_log_id()?,
            })
        })
        .await
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &Vote) -> Result<(), io::Error> {
        let vote = *vote;
        with_store(&self.store, self.io, move |store| store.save_vote(&vote)).await
    }

    async fn save_committed(&mut self, committed: Option<LogId>) -> Result<(), io::Error> {
        with_store(&self.store, self.io, move |store| {
            store.save_committed(committed.as_ref())
        })
        .await
    }

    async fn read_committed(&mut self) -> Result<Option<LogId>, io::Error> {
        with_store(&self.store, self.io, |store| store.committed()).await
    }

    /// Appends `entries` in one transaction, and reports them durable once it commits.
    async fn append<I>(
        &mut self,
        entries: I,
        callback: IOFlushed<ControlTypes>,
    ) -> Result<(), io::Error>
    where
        I: IntoIterator<Item = <ControlTypes as RaftTypeConfig>::Entry> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let entries: Vec<Entry> = entries.into_iter().collect();
        let started = tokio::time::Instant::now();
        let written = with_store(&self.store, self.io, move |store| {
            store.append_entries(&entries)
        })
        .await;
        match written {
            Ok(()) => {
                self.appends.fetch_add(1, Ordering::Relaxed);
                self.metrics.log_written(started.elapsed());
                callback.io_completed(Ok(()));
                Ok(())
            }
            Err(error) => {
                tracing::error!(%error, "control log append failed");
                callback.io_completed(Err(io::Error::new(error.kind(), error.to_string())));
                Err(error)
            }
        }
    }

    async fn truncate_after(&mut self, last_log_id: Option<LogId>) -> Result<(), io::Error> {
        let from = last_log_id.map_or(0, |log_id| log_id.index.saturating_add(1));
        with_store(&self.store, self.io, move |store| store.truncate_from(from)).await
    }

    async fn purge(&mut self, log_id: LogId) -> Result<(), io::Error> {
        with_store(&self.store, self.io, move |store| {
            store.purge_through(&log_id)
        })
        .await
    }
}
