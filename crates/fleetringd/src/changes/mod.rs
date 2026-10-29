//! Change streams: sinks that receive each database's acknowledged changes (FR-039).
//!
//! A [`ChangeSink`] takes one [`Change`] per acknowledged batch: the batch's opaque payload
//! ([`ChangeKind::Physical`]), or the rows its transactions changed
//! ([`ChangeKind::Logical`]), decoded by the engine adapter. Sinks run beside replication, never
//! in its acknowledgement path: a batch reaches a sink only after the write quorum holds it, and
//! a sink that is slow or failing delays no write.
//!
//! # Feeding
//!
//! [`ChangeStreams`] starts one pump per sink and database this node is primary for. A pump
//! follows the database's primary through a [`PrimaryFeed`](crate::replication) and keeps a
//! durable position, the last batch end it delivered, in `<data_dir>/changes/<sink>/<database>`.
//! Whenever the write quorum's position passes it, the pump reads the log from its position up
//! to the quorum's position as one batch, through the registry, and publishes it; then records
//! the new position. The pump holds one batch at a time: however far a sink falls behind, nothing
//! is buffered in memory, and the log is read again when the sink is ready. A lagging sink keeps
//! its primary from checkpointing, as a lagging follower does, until it has been behind for the
//! replication settings' `lag_limit`; after that the log may restart past it, and the pump then
//! restarts from the log's beginning and counts a reset in its status.
//!
//! Delivery is at least once: a sink that fails is retried with backoff and nothing is skipped,
//! and a restart delivers again anything whose position wasn't recorded. A sink receives a
//! database's batches in commit order, each continuing the one before, so a consumer can
//! discard a batch whose position it has seen.
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
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time;

use self::position::PositionStore;
use self::pump::{PumpContext, PumpState};
use crate::engine::{Execution, Payload, Position, RowChange};
use crate::replication::Replication;

pub use self::config::{ChangesConfig, Format, SinkConfig, SinkKind};
pub use self::file::{FileSink, database_file};
pub use self::matcher::{DatabaseMatcher, DatabaseMatchers, ParseMatcherError};
pub use self::memory::MemorySink;
pub use self::record::{ChangeRecord, NotHex, ParseRecordError, RecordContent, hex, unhex};
pub use self::webhook::{InvalidUrlError, WebhookSettings, WebhookSink};

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
    pumps: Mutex<BTreeMap<(String, DatabaseName), Pump>>,
    supervisor: Mutex<Option<AbortOnDrop>>,
}

#[derive(Debug)]
struct Pump {
    epoch: LeaseEpoch,
    format: Format,
    state: Arc<Mutex<PumpState>>,
    _task: AbortOnDrop,
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
        } = parts;
        let served = replication.served();
        let inner = Arc::new(Inner {
            sinks,
            replication,
            state_dir,
            settings,
            pumps: Mutex::default(),
            supervisor: Mutex::default(),
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
    /// database moved to a new epoch, and stops those whose database this node no longer serves.
    fn reconcile(self: &Arc<Self>, served: &BTreeMap<DatabaseName, LeaseEpoch>) {
        let mut pumps = self.lock_pumps();
        pumps.retain(|(_, database), pump| {
            served
                .get(database)
                .is_some_and(|epoch| *epoch == pump.epoch)
        });
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
                let context = PumpContext {
                    sink: Arc::clone(&configured.sink),
                    format: configured.format,
                    database: database.clone(),
                    replication: self.replication.clone(),
                    store: PositionStore::new(&self.state_dir, &name, database),
                    state: Arc::clone(&state),
                    settings: self.settings,
                    streams: Arc::downgrade(self),
                };
                let task = tokio::spawn(pump::run(context));
                pumps.insert(
                    key,
                    Pump {
                        epoch: *epoch,
                        format: configured.format,
                        state,
                        _task: AbortOnDrop(task),
                    },
                );
            }
        }
    }
}

impl Inner {
    /// Lets `database` forget the row changes every logical sink has delivered: those up to the
    /// lowest position among its logical pumps.
    fn release(self: &Arc<Self>, database: &DatabaseName) {
        let lowest = {
            let pumps = self.lock_pumps();
            let mut positions = pumps
                .iter()
                .filter(|((_, pumped), pump)| pumped == database && pump.format == Format::Logical)
                .map(|(_, pump)| pump::lock(&pump.state).position.clone());
            match positions.next() {
                None => return,
                Some(first) => positions.fold(first, |lowest, position| lowest.min(position)),
            }
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
/// [`ChangesConfig::validate`] rules out first, or names a URL the sink can't use.
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
            Arc::new(WebhookSink::new(&config.name, url, settings)?)
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
    /// The webhook URL is invalid.
    #[error(transparent)]
    Url(#[from] InvalidUrlError),
}

const _: () = {
    const fn assert_debug_send_sync<T: fmt::Debug + Send + Sync>() {}
    assert_debug_send_sync::<BuildSinkError>();
    assert_debug_send_sync::<Change>();
    assert_debug_send_sync::<ChangeKind>();
    assert_debug_send_sync::<ChangeParts>();
    assert_debug_send_sync::<ChangeRecord>();
    assert_debug_send_sync::<ChangeSettings>();
    assert_debug_send_sync::<ChangeStreams>();
    assert_debug_send_sync::<ChangesConfig>();
    assert_debug_send_sync::<ConfiguredSink>();
    assert_debug_send_sync::<DatabaseMatcher>();
    assert_debug_send_sync::<DatabaseMatchers>();
    assert_debug_send_sync::<FileSink>();
    assert_debug_send_sync::<Format>();
    assert_debug_send_sync::<MemorySink>();
    assert_debug_send_sync::<SinkConfig>();
    assert_debug_send_sync::<SinkError>();
    assert_debug_send_sync::<SinkKind>();
    assert_debug_send_sync::<SinkStatus>();
    assert_debug_send_sync::<WebhookSettings>();
    assert_debug_send_sync::<WebhookSink>();
};
