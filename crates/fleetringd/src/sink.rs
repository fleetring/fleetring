//! Where an open database's committed batches go: the replication stream, or a test's sink.

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
/// The actor asks [`admit`](Self::admit) before it runs each write transaction, and calls
/// [`publish`](Self::publish) right after each commit, before it starts the next write, so
/// neither call may block. The writer that committed the batch then waits for the returned
/// [`Acknowledgement`] before its write counts as done, while the actor moves on, so several
/// batches can be in flight at once (ADR-0016, point 11).
pub trait ReplicationSink: fmt::Debug + Send + Sync + 'static {
    /// Decides whether a write to `database` may run now, before anything commits.
    ///
    /// A refusal is the clean failure ADR-0023 asks for when fewer than the database's
    /// `min_replicas` are reachable: the write is never applied, so the client can retry it
    /// without an unknown outcome. The default admits every write.
    ///
    /// # Errors
    ///
    /// [`ReplicationError`] if the write must not run.
    fn admit(&self, database: &DatabaseName) -> Result<(), ReplicationError> {
        let _ = database;
        Ok(())
    }

    /// Takes one committed batch.
    fn publish(&self, batch: CommittedBatch) -> Acknowledgement;
}

/// Completes when replication has stored a batch as durably as a write requires.
#[must_use = "a write isn't acknowledged until its acknowledgement completes"]
pub struct Acknowledgement(
    Pin<Box<dyn Future<Output = Result<Acknowledged, ReplicationError>> + Send>>,
);

impl Acknowledgement {
    /// An acknowledgement that completes with `future`.
    pub fn new(
        future: impl Future<Output = Result<Acknowledged, ReplicationError>> + Send + 'static,
    ) -> Self {
        Self(Box::pin(future))
    }

    /// An acknowledgement that is already complete, for a batch stored on its primary alone,
    /// which is the whole replica set ([`Acknowledged::local`]).
    pub fn done() -> Self {
        Self::new(future::ready(Ok(Acknowledged::local())))
    }

    /// Waits for the acknowledgement.
    ///
    /// # Errors
    ///
    /// [`ReplicationError`] if replication couldn't store the batch.
    pub async fn wait(self) -> Result<Acknowledged, ReplicationError> {
        self.0.await
    }
}

/// How durably an acknowledged batch is stored: how many replicas hold it, and how many the
/// database should have.
///
/// A batch is acknowledged once W replicas hold it (ADR-0018), so `acknowledged_by` is W or more.
/// The other followers usually catch up a moment later, so a count below `replicas` is normal and
/// says nothing about the database's health; [`is_under_replicated`](Self::is_under_replicated)
/// does.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct Acknowledged {
    /// How many replicas, the primary included, held the batch durably when it was acknowledged.
    pub acknowledged_by: u32,
    /// How many replicas, the primary included, were reachable when the batch was published.
    pub reachable: u32,
    /// R, the size of the database's replica set.
    pub replicas: u32,
}

impl Acknowledged {
    /// A batch held by `acknowledged_by` of `replicas` replicas, with `reachable` of them
    /// reachable when it was published.
    #[must_use]
    pub const fn new(acknowledged_by: u32, reachable: u32, replicas: u32) -> Self {
        Self {
            acknowledged_by,
            reachable,
            replicas,
        }
    }

    /// A batch stored on its primary, which is the database's only replica.
    #[must_use]
    pub const fn local() -> Self {
        Self::new(1, 1, 1)
    }

    /// Whether the database is under-replicated: fewer of its replicas were reachable than its
    /// replica set holds, so some of them can't receive the batch until they come back.
    #[must_use]
    pub const fn is_under_replicated(&self) -> bool {
        self.reachable < self.replicas
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
    /// Too few of the database's replicas are reachable to store the batch as durably as a
    /// write requires (ADR-0018). The write committed on the primary but isn't acknowledged.
    #[error(
        "database is under-replicated: {reachable} of the {required} replicas a write needs are reachable"
    )]
    UnderReplicated {
        /// W, the replicas a write needs, the primary included.
        required: usize,
        /// The replicas reachable now, the primary included.
        reachable: usize,
    },
    /// Fewer replicas are reachable than the database's `min_replicas` floor, so the write was
    /// refused before it ran (ADR-0023). Nothing committed; retry once more replicas are back.
    #[error(
        "too few replicas are reachable: {reachable} of the {min_replicas} a write needs; nothing committed"
    )]
    BelowFloor {
        /// The database's `min_replicas`, the primary included.
        min_replicas: usize,
        /// The replicas reachable now, the primary included.
        reachable: usize,
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
    assert_debug_send_sync::<Acknowledged>();
};
