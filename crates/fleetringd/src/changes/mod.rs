//! Change streams: sinks that receive each database's acknowledged changes (FR-039).
//!
//! A [`ChangeSink`] takes one [`Change`] per acknowledged batch: the batch's opaque payload
//! ([`ChangeKind::Physical`]), or the rows its transactions changed
//! ([`ChangeKind::Logical`]), decoded by the engine adapter. Sinks run beside replication, never
//! in its acknowledgement path: a batch reaches a sink only after the write quorum holds it, and
//! a sink that is slow or failing delays no write.
//!
//! A logical sink names a [`CaptureMode`](crate::engine::CaptureMode): `id` records each changed
//! row's key alone, `after` (the default) adds the new values of inserted and updated rows, and
//! `full` adds the old values of updated and deleted rows and the names of the updated columns.
//! The databases a sink names record from the moment they open, in the strongest mode any sink
//! naming them asks for ([`CaptureRules`]), inside the database itself, so that the record
//! replicates with the data and a promoted follower continues the stream (ADR-0024).
//!
//! # Feeding
//!
//! [`ChangeStreams`] starts one pump per sink and database this node is primary for. A pump
//! follows the database's primary through a [`PrimaryFeed`](crate::replication) and keeps a
//! durable position, the last batch end it delivered. Whenever the write quorum's position
//! passes it, the pump reads the log from its position up to the quorum's position as one batch,
//! through the registry, and publishes it; then records the new position. The pump holds one
//! batch at a time: however far a sink falls behind, nothing
//! is buffered in memory, and the log is read again when the sink is ready. A lagging sink keeps
//! its primary from checkpointing, as a lagging follower does, until it has been behind for the
//! replication settings' `lag_limit`; after that the log may restart past it, and the pump then
//! restarts from the log's beginning and counts a reset in its status.
//!
//! # Flushing a dropped database
//!
//! A pump stops once its node no longer serves the database, unless [`ChangeStreams::hold`]
//! keeps it. A dropped database's primary holds its streams before it stops serving, then
//! [`ChangeStreams::flush_database`] has every pump deliver what the copy acknowledged, a paced
//! sink such as an archive at once, for a bounded time, before the copy is deleted (ADR-0041,
//! point 4).
//!
//! # Where positions live
//!
//! When the database's engine can keep sink marks ([`SinkMarks`](crate::engine::SinkMarks),
//! ADR-0028), a pump keeps its position as a mark inside the database: after each delivery it
//! stages the batch's end, and the database's next write transaction records it, so the mark
//! replicates with the data. A pump reads the mark when it starts, on a restart and on a
//! promoted follower alike, and continues from there if the log still continues from it, or from
//! the log's beginning, counting a reset, if not. A failover then repeats only what the old
//! primary delivered after the newest mark that reached the write quorum. An engine that can't
//! keep marks gets the pump a file on this node, `<data_dir>/changes/<sink>/<database>`, as
//! before, and a failover repeats everything since the last checkpoint. The node's status says
//! which ([`MarkHome`]).
//!
//! Delivery is at least once: a sink that fails is retried with backoff and nothing is skipped,
//! and a restart delivers again anything whose position wasn't recorded. A sink receives a
//! database's batches in commit order, each continuing the one before, so a consumer can
//! discard a batch whose position it has seen.
//!
//! # Watchers
//!
//! A watcher is a client following a database's recorded row changes through `WatchChanges`
//! (ADR-0045), served by the database's primary. It is not a sink: it keeps no mark and holds
//! back no checkpoint, since it reads the change table and not the log. While it is connected,
//! [`ChangeStreams::watch`] counts its position among those a release of row changes takes the
//! lowest of, so the sinks don't prune what it is about to read; and at most
//! [`MAX_WATCHERS`] watch at once on one node.
//!
//! # Sinks
//!
//! [`FileSink`] appends one JSON line per batch to a file per database, and [`WebhookSink`]
//! POSTs each batch as JSON. [`build_sink`] constructs a sink from its configuration by kind:
//! that is the plugin point for a Kafka or Pub/Sub sink, which would add a [`SinkKind`] and a
//! branch there, behind a feature so that the default build carries no cloud SDK.

mod config;
mod file;
mod matcher;
mod memory;
mod position;
mod pump;
mod record;
mod webhook;

use std::collections::BTreeMap;
use std::fmt;
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::{Arc, Mutex, MutexGuard, Weak};
use std::time::Duration;

use fleetring_control::{DatabaseName, LeaseEpoch};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, watch};
use tokio::task::JoinHandle;
use tokio::time;

use self::position::PositionStore;
use self::pump::{PumpContext, PumpState};
use crate::engine::{Execution, Payload, Position, RowChange};
use crate::observe::ChangeMetrics;
use crate::replication::{ChainHash, Replication};

pub use self::config::{ChangesConfig, Format, SinkConfig, SinkKind};
pub use self::file::{FileSink, database_file};
pub use self::matcher::{CaptureRules, DatabaseMatcher, DatabaseMatchers, ParseMatcherError};
pub use self::memory::MemorySink;
pub use self::record::{ChangeRecord, NotHex, ParseRecordError, RecordContent, hex, unhex};
pub use self::webhook::{
    ClientCertificate, InvalidUrlError, TokenSource, WebhookConfigError, WebhookSecurity,
    WebhookSettings, WebhookSink,
};

/// Where a sink's delivered position is kept (ADR-0028).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum MarkHome {
    /// Inside the database, replicated with it.
    Replicated,
    /// In a file on this node only, because the engine can't keep marks.
    Local,
}

/// One acknowledged batch of a database, as a sink receives it.
#[derive(Debug, Clone, PartialEq)]
pub struct Change {
    /// The database.
    pub database: DatabaseName,
    /// The epoch of the primary that acknowledged the batch (ADR-0018).
    pub epoch: LeaseEpoch,
    /// The position the batch continues from: the previous change's [`position`](Self::position),
    /// or `None` for a batch read from the log's beginning.
    pub start: Option<Position>,
    /// The engine-defined position just past the batch, opaque above the adapter (ADR-0016).
    pub position: Position,
    /// The batch's content.
    pub kind: ChangeKind,
    /// The hash chain over the log (FR-044) at [`start`](Self::start), as the primary holds it.
    ///
    /// Only physical changes carry the chain, for an archive to record and a restore to check
    /// (ADR-0029). `None` for a logical change, while the primary's chain isn't built yet or no
    /// longer holds the position, and for the log's beginning after a checkpoint, where the
    /// chain is held at the position the log continues from rather than at `None`.
    pub chain_start: Option<ChainHash>,
    /// The hash chain at [`position`](Self::position), as the primary holds it; `None` as for
    /// [`chain_start`](Self::chain_start).
    pub chain_end: Option<ChainHash>,
}

/// What a [`Change`] carries: one of the two shapes of change (ADR-0016).
#[derive(Debug, Clone, PartialEq)]
pub enum ChangeKind {
    /// The batch's opaque payload, as the engine ships it to followers: for a mirror or an
    /// archive that the same engine will apply.
    Physical(Payload),
    /// The rows the batch's transactions inserted, updated, and deleted, in commit order.
    Logical(Vec<RowChange>),
}

/// The future a sink returns from [`ChangeSink::publish`].
pub type Published = Pin<Box<dyn Future<Output = Result<(), SinkError>> + Send>>;

/// Somewhere a database's changes go.
///
/// A sink receives each database's batches in commit order, after the write quorum holds them,
/// and at least once. Its `publish` may take as long as it needs; no write waits for it.
pub trait ChangeSink: fmt::Debug + Send + Sync + 'static {
    /// The sink's name, which names the directory its positions are kept in.
    fn name(&self) -> &str;

    /// Takes one change. Completes once the sink has it as durably as it promises.
    fn publish(&self, change: Change) -> Published;

    /// The least time between the end of one delivery and the pump's next read of the log, so
    /// that a sink that pays per delivery, such as an archive writing one object each, receives
    /// fewer, larger batches. The default is zero: the pump reads again at once.
    fn pace(&self) -> Duration {
        Duration::ZERO
    }
}

/// The error a sink returns when it couldn't take a change. The pump retries either way,
/// since a stream can't skip a change; the variants say whose fault it was.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum SinkError {
    /// The sink couldn't take the change now, for a reason that may pass.
    #[error("sink failed: {reason}")]
    Failed {
        /// Why.
        reason: String,
    },
    /// The sink refused the change.
    #[error("sink rejected the change: {reason}")]
    Rejected {
        /// Why.
        reason: String,
    },
}

/// A sink with the databases it streams and the format it takes.
#[derive(Debug, Clone)]
pub struct ConfiguredSink {
    /// The sink.
    pub sink: Arc<dyn ChangeSink>,
    /// What it receives.
    pub format: Format,
    /// The databases it streams.
    pub databases: DatabaseMatchers,
}

impl ConfiguredSink {
    /// `sink` receiving `format` changes of the databases `databases` names.
    #[must_use]
    pub fn new(sink: impl ChangeSink, format: Format, databases: DatabaseMatchers) -> Self {
        Self {
            sink: Arc::new(sink),
            format,
            databases,
        }
    }
}

/// How pumps retry a sink that fails.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChangeSettings {
    /// How long a pump waits before retrying a failed delivery the first time; each later wait
    /// doubles.
    pub retry_initial: Duration,
    /// The longest wait between retries.
    pub retry_max: Duration,
}

impl ChangeSettings {
    /// The defaults: 50 ms, doubling up to 5 s.
    #[must_use]
    pub fn new() -> Self {
        Self {
            retry_initial: Duration::from_millis(50),
            retry_max: Duration::from_secs(5),
        }
    }
}

impl Default for ChangeSettings {
    fn default() -> Self {
        Self::new()
    }
}

/// What [`ChangeStreams::start`] needs.
#[derive(Debug, Clone)]
pub struct ChangeParts {
    /// The sinks.
    pub sinks: Vec<ConfiguredSink>,
    /// The node's replication, whose primaries the pumps follow and whose registry they read.
    pub replication: Replication,
    /// The directory positions are kept in: `<data_dir>/changes`.
    pub state_dir: PathBuf,
    /// How pumps retry.
    pub settings: ChangeSettings,
    /// Where deliveries and resets are counted: the daemon's
    /// [`Metrics::changes`](crate::observe::Metrics::changes), or a default handle that isn't
    /// registered anywhere.
    pub metrics: ChangeMetrics,
}

/// One sink's progress on one database, as the node's status reports it.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct SinkStatus {
    /// The sink.
    pub sink: String,
    /// The database.
    pub database: DatabaseName,
    /// The last position the sink took, or `None` if it took nothing yet.
    pub position: Option<Position>,
    /// Where the position is kept: in the database, replicated with it, or on this node only.
    /// `None` until the pump has opened its database.
    pub home: Option<MarkHome>,
    /// The write quorum's position, which the sink is behind if it differs.
    pub acknowledged: Option<Position>,
    /// How many batches the primary has published since the sink's position, as far as it
    /// knows.
    pub backlog: u64,
    /// How many deliveries failed and were retried.
    pub failures: u64,
    /// How many times the log moved past the sink's position, so the stream skipped ahead.
    pub resets: u64,
    /// The last delivery failure's reason.
    pub last_error: Option<String>,
}

/// A data node's change streams: the pumps feeding each sink from each database the node is
/// primary for. Cloning is cheap and shares them.
#[derive(Debug, Clone)]
pub struct ChangeStreams {
    inner: Arc<Inner>,
}

#[derive(Debug)]
struct Inner {
    sinks: Vec<ConfiguredSink>,
    replication: Replication,
    state_dir: PathBuf,
    settings: ChangeSettings,
    metrics: ChangeMetrics,
    pumps: Mutex<BTreeMap<(String, DatabaseName), Pump>>,
    /// The databases whose pumps run on after the node stops serving them, each with how many
    /// [`HeldStreams`] keep them.
    held: Mutex<BTreeMap<DatabaseName, usize>>,
    supervisor: Mutex<Option<AbortOnDrop>>,
    /// The connected watchers, by an ID of their own, each with its database and position.
    watchers: Mutex<Watchers>,
    /// One permit per watcher that may connect.
    watcher_slots: Arc<Semaphore>,
}

/// The connected watchers, and the ID the next one gets.
#[derive(Debug, Default)]
struct Watchers {
    next: u64,
    positions: BTreeMap<u64, (DatabaseName, Option<Position>)>,
}

/// How many watchers one node serves at once (ADR-0045, point 8). The next is refused until one
/// leaves.
pub const MAX_WATCHERS: usize = 64;

/// The error [`ChangeStreams::watch`] returns when [`MAX_WATCHERS`] are connected.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("already serves {limit} change watchers")]
#[non_exhaustive]
pub struct TooManyWatchers {
    /// The limit.
    pub limit: usize,
}

/// A connected watcher, counted among the positions that hold back the release of a database's
/// row changes until it is dropped (ADR-0045, point 7).
#[derive(Debug)]
#[must_use = "the watcher stops holding its position once dropped"]
pub struct Watcher {
    inner: Arc<Inner>,
    id: u64,
    _slot: OwnedSemaphorePermit,
}

impl Watcher {
    /// Records that the watcher has read everything up to `position`, so that the release of row
    /// changes may pass what it has.
    ///
    /// # Panics
    ///
    /// If the watchers' lock was poisoned by a panic in another thread.
    pub fn advance(&self, position: Option<Position>) {
        if let Some(entry) = self.inner.lock_watchers().positions.get_mut(&self.id) {
            entry.1 = position;
        }
    }
}

impl Drop for Watcher {
    fn drop(&mut self) {
        self.inner.lock_watchers().positions.remove(&self.id);
    }
}

#[derive(Debug)]
struct Pump {
    epoch: LeaseEpoch,
    format: Format,
    state: Arc<Mutex<PumpState>>,
    /// Set to deliver what is acknowledged at once, whatever the sink's pace.
    cut: watch::Sender<bool>,
    _task: AbortOnDrop,
}

/// How [`ChangeStreams::flush_database`] ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Flushed {
    /// Every change stream of the database delivered everything acknowledged.
    Delivered,
    /// The limit passed first.
    TimedOut,
}

/// Keeps a database's change streams running after the node stops serving it, until dropped:
/// a dropped database's primary flushes them before it deletes its copy (ADR-0041, point 4).
#[derive(Debug)]
#[must_use = "the streams stop once the hold is dropped"]
pub struct HeldStreams {
    inner: Arc<Inner>,
    database: DatabaseName,
}

impl Drop for HeldStreams {
    fn drop(&mut self) {
        {
            let mut held = self.inner.lock_held();
            if let Some(count) = held.get_mut(&self.database) {
                *count = count.saturating_sub(1);
                if *count == 0 {
                    held.remove(&self.database);
                }
            }
        }
        let served = self.inner.replication.served().borrow().clone();
        self.inner.reconcile(&served);
    }
}

/// Aborts a task when dropped.
#[derive(Debug)]
struct AbortOnDrop(JoinHandle<()>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

impl ChangeStreams {
    /// Starts the streams: from now on, every database the node serves as primary is pumped to
    /// each sink that names it.
    #[must_use]
    pub fn start(parts: ChangeParts) -> Self {
        let ChangeParts {
            sinks,
            replication,
            state_dir,
            settings,
            metrics,
        } = parts;
        for configured in &sinks {
            metrics.add_sink(configured.sink.name());
        }
        let served = replication.served();
        let inner = Arc::new(Inner {
            sinks,
            replication,
            state_dir,
            settings,
            metrics,
            pumps: Mutex::default(),
            held: Mutex::default(),
            supervisor: Mutex::default(),
            watchers: Mutex::default(),
            watcher_slots: Arc::new(Semaphore::new(MAX_WATCHERS)),
        });
        let supervisor = tokio::spawn(supervise(Arc::downgrade(&inner), served));
        *inner.lock_supervisor() = Some(AbortOnDrop(supervisor));
        Self { inner }
    }

    /// The sinks, in configuration order.
    #[must_use]
    pub fn sinks(&self) -> &[ConfiguredSink] {
        &self.inner.sinks
    }

    /// Whether any sink is configured.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.inner.sinks.is_empty()
    }

    /// Each sink's progress on each database it is being fed, in sink and database order.
    ///
    /// # Panics
    ///
    /// If the pumps' lock was poisoned by a panic in another thread.
    #[must_use]
    pub fn status(&self) -> Vec<SinkStatus> {
        self.inner
            .lock_pumps()
            .iter()
            .map(|((sink, database), pump)| {
                let state = pump::lock(&pump.state);
                SinkStatus {
                    sink: sink.clone(),
                    database: database.clone(),
                    position: state.position.clone(),
                    home: state.home,
                    acknowledged: state.acknowledged.clone(),
                    backlog: state.backlog,
                    failures: state.failures,
                    resets: state.resets,
                    last_error: state.last_error.clone(),
                }
            })
            .collect()
    }

    /// Waits until every pump has delivered everything acknowledged, or until `limit` passes.
    /// Returns whether they have.
    pub async fn await_caught_up(&self, limit: Duration) -> bool {
        let poll = self.inner.settings.retry_initial;
        time::timeout(limit, async {
            while !self.inner.all_caught_up() {
                time::sleep(poll).await;
            }
        })
        .await
        .is_ok()
    }

    /// Keeps `database`'s change streams running after this node stops serving it, until the
    /// returned hold is dropped, so that [`flush_database`](Self::flush_database) can deliver
    /// what its copy acknowledged before the copy is deleted. Take it before serving stops: the
    /// streams of a database the node no longer serves stop as soon as they notice.
    ///
    /// # Panics
    ///
    /// If the held databases' lock was poisoned by a panic in another thread.
    pub fn hold(&self, database: &DatabaseName) -> HeldStreams {
        *self.inner.lock_held().entry(database.clone()).or_default() += 1;
        HeldStreams {
            inner: Arc::clone(&self.inner),
            database: database.clone(),
        }
    }

    /// Waits until every change stream of `database` has delivered everything acknowledged, up
    /// to the copy's current end, or until `limit` passes. An archive puts its open segment at
    /// once, whatever its `segment_interval_ms`. A database with no change streams is delivered
    /// at once.
    ///
    /// The streams of a database this node no longer serves run only while
    /// [`hold`](Self::hold) keeps them.
    ///
    /// # Panics
    ///
    /// If the pumps' lock was poisoned by a panic in another thread.
    pub async fn flush_database(&self, database: &DatabaseName, limit: Duration) -> Flushed {
        for ((_, pumped), pump) in self.inner.lock_pumps().iter() {
            if pumped == database {
                pump.cut.send_replace(true);
            }
        }
        let poll = self.inner.settings.retry_initial;
        let flushed = time::timeout(limit, async {
            while !self.inner.caught_up(database) {
                time::sleep(poll).await;
            }
        })
        .await;
        match flushed {
            Ok(()) => Flushed::Delivered,
            Err(_) => Flushed::TimedOut,
        }
    }

    /// Admits a watcher of `database` that has read up to `position`: from now until the returned
    /// [`Watcher`] is dropped, a release of the database's row changes doesn't pass its position.
    ///
    /// # Errors
    ///
    /// [`TooManyWatchers`] if [`MAX_WATCHERS`] are connected already.
    ///
    /// # Panics
    ///
    /// If the watchers' lock was poisoned by a panic in another thread.
    pub fn watch(
        &self,
        database: &DatabaseName,
        position: Option<Position>,
    ) -> Result<Watcher, TooManyWatchers> {
        let slot = Arc::clone(&self.inner.watcher_slots)
            .try_acquire_owned()
            .map_err(|_| TooManyWatchers {
                limit: MAX_WATCHERS,
            })?;
        let id = {
            let mut watchers = self.inner.lock_watchers();
            let id = watchers.next;
            watchers.next += 1;
            watchers.positions.insert(id, (database.clone(), position));
            id
        };
        Ok(Watcher {
            inner: Arc::clone(&self.inner),
            id,
            _slot: slot,
        })
    }

    /// How many watchers are connected to this node.
    ///
    /// # Panics
    ///
    /// If the watchers' lock was poisoned by a panic in another thread.
    #[must_use]
    pub fn watchers(&self) -> usize {
        self.inner.lock_watchers().positions.len()
    }

    /// Gives the pumps up to `flush` to deliver what is acknowledged, then stops them. Their
    /// positions are recorded, so they continue from there when the node starts again.
    pub async fn stop(&self, flush: Duration) {
        let _flushed = self.await_caught_up(flush).await;
        let supervisor = self.inner.lock_supervisor().take();
        drop(supervisor);
        let pumps = std::mem::take(&mut *self.inner.lock_pumps());
        drop(pumps);
    }
}

impl Inner {
    fn lock_pumps(&self) -> MutexGuard<'_, BTreeMap<(String, DatabaseName), Pump>> {
        self.pumps.lock().expect("change streams lock poisoned")
    }

    fn lock_supervisor(&self) -> MutexGuard<'_, Option<AbortOnDrop>> {
        self.supervisor
            .lock()
            .expect("change streams lock poisoned")
    }

    fn lock_held(&self) -> MutexGuard<'_, BTreeMap<DatabaseName, usize>> {
        self.held.lock().expect("change streams lock poisoned")
    }

    fn lock_watchers(&self) -> MutexGuard<'_, Watchers> {
        self.watchers.lock().expect("change watchers lock poisoned")
    }

    /// Whether every pump of `database` has delivered the write quorum's position now, as its
    /// primary reports it; true once the node has no primary for it, since nothing more can be
    /// delivered then.
    fn caught_up(&self, database: &DatabaseName) -> bool {
        let Some(feed) = self.replication.primary_feed(database) else {
            return true;
        };
        let acknowledged = feed.acknowledged();
        self.lock_pumps()
            .iter()
            .filter(|((_, pumped), _)| pumped == database)
            .all(|(_, pump)| {
                let state = pump::lock(&pump.state);
                state.caught_up && state.position == acknowledged
            })
    }

    /// Whether every pump's delivered position is the write quorum's position now, as the
    /// primary reports it, so that a write acknowledged a moment ago counts as not yet
    /// delivered even before its pump has woken up.
    fn all_caught_up(&self) -> bool {
        let served = self.replication.served().borrow().clone();
        let pumps = self.lock_pumps();
        let every_pump_started = self.sinks.iter().all(|configured| {
            served
                .iter()
                .filter(|(database, _)| configured.databases.matches(database))
                .all(|(database, epoch)| {
                    pumps
                        .get(&(configured.sink.name().to_owned(), database.clone()))
                        .is_some_and(|pump| pump.epoch == *epoch)
                })
        });
        every_pump_started
            && pumps.iter().all(|((_, database), pump)| {
                let state = pump::lock(&pump.state);
                match self.replication.primary_feed(database) {
                    Some(feed) => state.caught_up && feed.acknowledged() == state.position,
                    None => true,
                }
            })
    }

    /// Starts a pump for every sink and served database that lacks one, restarts those whose
    /// database moved to a new epoch, and stops those whose database this node no longer serves,
    /// unless a [`HeldStreams`] keeps them.
    fn reconcile(self: &Arc<Self>, served: &BTreeMap<DatabaseName, LeaseEpoch>) {
        let mut pumps = self.lock_pumps();
        let held = self.lock_held();
        pumps.retain(|(_, database), pump| {
            held.contains_key(database)
                || served
                    .get(database)
                    .is_some_and(|epoch| *epoch == pump.epoch)
        });
        drop(held);
        for configured in &self.sinks {
            let name = configured.sink.name().to_owned();
            for (database, epoch) in served {
                if !configured.databases.matches(database) {
                    continue;
                }
                let key = (name.clone(), database.clone());
                if pumps.contains_key(&key) {
                    continue;
                }
                let state = Arc::new(Mutex::new(PumpState::default()));
                let (cut, cut_requested) = watch::channel(false);
                let context = PumpContext {
                    sink: Arc::clone(&configured.sink),
                    format: configured.format,
                    database: database.clone(),
                    replication: self.replication.clone(),
                    store: PositionStore::new(&self.state_dir, &name, database),
                    state: Arc::clone(&state),
                    settings: self.settings,
                    metrics: self.metrics.clone(),
                    streams: Arc::downgrade(self),
                    cut: cut_requested,
                };
                let task = tokio::spawn(pump::run(context));
                pumps.insert(
                    key,
                    Pump {
                        epoch: *epoch,
                        format: configured.format,
                        state,
                        cut,
                        _task: AbortOnDrop(task),
                    },
                );
            }
        }
    }
}

impl Inner {
    /// Lets `database` forget the row changes every logical sink has delivered and no connected
    /// watcher still has to read: those up to the lowest position among its logical pumps and
    /// its watchers.
    fn release(self: &Arc<Self>, database: &DatabaseName) {
        let lowest = {
            let pumps = self.lock_pumps();
            let mut positions = pumps
                .iter()
                .filter(|((_, pumped), pump)| pumped == database && pump.format == Format::Logical)
                .map(|(_, pump)| pump::lock(&pump.state).position.clone());
            let Some(first) = positions.next() else {
                return;
            };
            let pumped = positions.fold(first, |lowest, position| lowest.min(position));
            let watchers = self.lock_watchers();
            watchers
                .positions
                .values()
                .filter(|(watched, _)| watched == database)
                .map(|(_, position)| position.clone())
                .fold(pumped, |lowest, position| lowest.min(position))
        };
        let Some(before) = lowest else {
            return;
        };
        let registry = self.replication.registry().clone();
        let database = database.clone();
        tokio::spawn(async move {
            let released = match registry.open(&database).await {
                Ok(handle) => handle.release_changes(before).await,
                Err(error) => Err(error),
            };
            if let Err(error) = released {
                tracing::debug!(%database, %error, "can't release delivered row changes");
            }
        });
    }
}

/// Keeps the pumps in step with the databases the node serves.
async fn supervise(
    inner: Weak<Inner>,
    mut served: watch::Receiver<BTreeMap<DatabaseName, LeaseEpoch>>,
) {
    loop {
        let Some(inner) = inner.upgrade() else {
            return;
        };
        let current = served.borrow_and_update().clone();
        inner.reconcile(&current);
        drop(inner);
        if served.changed().await.is_err() {
            return;
        }
    }
}

/// Builds a sink from its configuration, by kind: the plugin point for new sinks.
///
/// Positions and the pumps are the streams' business; a sink only takes changes, so a new kind
/// is a [`ChangeSink`] implementation, a [`SinkKind`] variant, and a branch here.
///
/// # Errors
///
/// [`BuildSinkError`] if the configuration lacks what the kind needs, which
/// [`ChangesConfig::validate`] rules out first, or names a URL, authority, certificate, or token the sink can't use.
pub fn build_sink(
    config: &SinkConfig,
    execution: Execution,
) -> Result<ConfiguredSink, BuildSinkError> {
    let missing = |field: &'static str| BuildSinkError::Missing {
        sink: config.name.clone(),
        field,
    };
    let sink: Arc<dyn ChangeSink> = match config.kind {
        SinkKind::File => {
            let path = config.path.as_ref().ok_or_else(|| missing("path"))?;
            Arc::new(FileSink::new(&config.name, path).with_execution(execution))
        }
        SinkKind::Webhook => {
            let url = config.url.as_ref().ok_or_else(|| missing("url"))?;
            let settings = WebhookSettings {
                timeout: Duration::from_millis(config.timeout_ms),
                retries: config.retries,
            };
            Arc::new(WebhookSink::new(
                &config.name,
                url,
                settings,
                &config.webhook_security(),
            )?)
        }
    };
    Ok(ConfiguredSink {
        sink,
        format: config.format,
        databases: config.matchers(),
    })
}

/// The error returned when a sink can't be built from its configuration.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum BuildSinkError {
    /// A setting the kind needs is absent.
    #[error("sink `{sink}` needs `{field}`")]
    Missing {
        /// The sink.
        sink: String,
        /// The setting.
        field: &'static str,
    },
    /// The webhook can't be built: its URL is invalid, or its TLS or token can't be set up.
    #[error(transparent)]
    Webhook(#[from] WebhookConfigError),
}

const _: () = {
    const fn assert_debug_send_sync<T: fmt::Debug + Send + Sync>() {}
    assert_debug_send_sync::<BuildSinkError>();
    assert_debug_send_sync::<CaptureRules>();
    assert_debug_send_sync::<Change>();
    assert_debug_send_sync::<ChangeKind>();
    assert_debug_send_sync::<ChangeParts>();
    assert_debug_send_sync::<ChangeRecord>();
    assert_debug_send_sync::<ChangeSettings>();
    assert_debug_send_sync::<ClientCertificate>();
    assert_debug_send_sync::<ChangeStreams>();
    assert_debug_send_sync::<ChangesConfig>();
    assert_debug_send_sync::<ConfiguredSink>();
    assert_debug_send_sync::<DatabaseMatcher>();
    assert_debug_send_sync::<DatabaseMatchers>();
    assert_debug_send_sync::<FileSink>();
    assert_debug_send_sync::<Flushed>();
    assert_debug_send_sync::<Format>();
    assert_debug_send_sync::<HeldStreams>();
    assert_debug_send_sync::<MarkHome>();
    assert_debug_send_sync::<MemorySink>();
    assert_debug_send_sync::<SinkConfig>();
    assert_debug_send_sync::<SinkError>();
    assert_debug_send_sync::<SinkKind>();
    assert_debug_send_sync::<SinkStatus>();
    assert_debug_send_sync::<TokenSource>();
    assert_debug_send_sync::<TooManyWatchers>();
    assert_debug_send_sync::<Watcher>();
    assert_debug_send_sync::<WebhookConfigError>();
    assert_debug_send_sync::<WebhookSecurity>();
    assert_debug_send_sync::<WebhookSettings>();
    assert_debug_send_sync::<WebhookSink>();
};
