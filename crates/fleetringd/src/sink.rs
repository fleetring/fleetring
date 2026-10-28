//! Where an open database's committed batches go: replication plugs in here (FR-030).

use std::fmt;
use std::future::{self, Future};
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use fleetring_control::DatabaseName;

use crate::engine::FrameBatch;

/// One committed write transaction's batch, from one database.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommittedBatch {
    /// The database that committed it.
    pub database: DatabaseName,
    /// The batch, with its start and end positions.
    pub batch: FrameBatch,
}

/// Receives every batch that frame-source databases commit, in commit order per database.
///
/// The actor calls [`publish`](Self::publish) right after each commit, before it starts the next
/// write, so the call must not block. The writer that committed the batch then waits for the
/// returned [`Acknowledgement`] before its write counts as done, while the actor moves on, so
/// several batches can be in flight at once (ADR-0016, point 11).
pub trait ReplicationSink: fmt::Debug + Send + Sync + 'static {
    /// Takes one committed batch.
    fn publish(&self, batch: CommittedBatch) -> Acknowledgement;
}

/// Completes when replication has stored a batch as durably as a write requires.
#[must_use = "a write isn't acknowledged until its acknowledgement completes"]
pub struct Acknowledgement(Pin<Box<dyn Future<Output = Result<(), ReplicationError>> + Send>>);

impl Acknowledgement {
    /// An acknowledgement that completes with `future`.
    pub fn new(
        future: impl Future<Output = Result<(), ReplicationError>> + Send + 'static,
    ) -> Self {
        Self(Box::pin(future))
    }

    /// An acknowledgement that is already complete.
    pub fn done() -> Self {
        Self::new(future::ready(Ok(())))
    }

    /// Waits for the acknowledgement.
    ///
    /// # Errors
    ///
    /// [`ReplicationError`] if replication couldn't store the batch.
    pub async fn wait(self) -> Result<(), ReplicationError> {
        self.0.await
    }
}

impl fmt::Debug for Acknowledgement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Acknowledgement")
    }
}

/// The error returned when replication couldn't store a committed batch.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum ReplicationError {
    /// Replication refused or lost the batch.
    #[error("replication failed: {reason}")]
    Failed {
        /// Why.
        reason: String,
    },
}

/// A sink that keeps every batch in memory and acknowledges at once. For tests.
#[derive(Debug, Clone, Default)]
pub struct RecordingSink {
    batches: Arc<Mutex<Vec<CommittedBatch>>>,
}

impl RecordingSink {
    /// An empty sink.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Every batch published so far, in publication order.
    ///
    /// # Panics
    ///
    /// If another thread panicked while publishing.
    #[must_use]
    pub fn batches(&self) -> Vec<CommittedBatch> {
        self.batches.lock().expect("sink lock poisoned").clone()
    }

    /// Removes and returns every batch published so far.
    ///
    /// # Panics
    ///
    /// If another thread panicked while publishing.
    #[must_use]
    pub fn take(&self) -> Vec<CommittedBatch> {
        std::mem::take(&mut *self.batches.lock().expect("sink lock poisoned"))
    }
}

impl ReplicationSink for RecordingSink {
    fn publish(&self, batch: CommittedBatch) -> Acknowledgement {
        self.batches.lock().expect("sink lock poisoned").push(batch);
        Acknowledgement::done()
    }
}

const _: () = {
    const fn assert_debug_send_sync<T: fmt::Debug + Send + Sync>() {}
    const fn assert_debug_send<T: fmt::Debug + Send>() {}
    assert_debug_send::<Acknowledgement>();
    assert_debug_send_sync::<CommittedBatch>();
    assert_debug_send_sync::<RecordingSink>();
    assert_debug_send_sync::<ReplicationError>();
};
