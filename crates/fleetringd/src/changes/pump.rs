//! The per-sink, per-database pump: tails acknowledged batches and hands them to the sink.

use std::sync::{Arc, Mutex, MutexGuard, Weak};
use std::time::Duration;

use fleetring_control::{DatabaseName, LeaseEpoch};
use tokio::time;

use crate::DatabaseError;
use crate::changes::position::PositionStore;
use crate::changes::{Change, ChangeKind, ChangeSettings, ChangeSink, Format, MarkHome};
use crate::engine::{EngineError, Position};
use crate::observe::ChangeMetrics;
use crate::replication::{PrimaryFeed, Replication};

/// How often a pump that is waiting looks whether its primary was replaced.
const CURRENCY_CHECK: Duration = Duration::from_secs(1);

/// What a pump needs.
pub(super) struct PumpContext {
    pub(super) sink: Arc<dyn ChangeSink>,
    pub(super) format: Format,
    pub(super) database: DatabaseName,
    pub(super) replication: Replication,
    pub(super) store: PositionStore,
    pub(super) state: Arc<Mutex<PumpState>>,
    pub(super) settings: ChangeSettings,
    pub(super) metrics: ChangeMetrics,
    /// The streams, which release delivered row changes once every logical sink has them.
    pub(super) streams: Weak<super::Inner>,
}

/// A pump's progress, as the node's status reports it.
#[derive(Debug, Clone, Default)]
pub(super) struct PumpState {
    /// The last position delivered and recorded.
    pub(super) position: Option<Position>,
    /// Where the position is kept, or `None` until the pump has opened its database.
    pub(super) home: Option<MarkHome>,
    /// The write quorum's position, as of the pump's last look.
    pub(super) acknowledged: Option<Position>,
    /// The batches the primary published since the pump last delivered, as far as it knows.
    pub(super) backlog: u64,
    /// How many deliveries failed and were retried.
    pub(super) failures: u64,
    /// How many times the log moved past the pump's position, so it restarted from the log's
    /// beginning and skipped what it had missed.
    pub(super) resets: u64,
    /// The last delivery failure's reason.
    pub(super) last_error: Option<String>,
    /// Whether the pump has delivered everything acknowledged.
    pub(super) caught_up: bool,
    /// The published count when the pump last delivered.
    delivered_at: u64,
}

pub(super) fn lock(state: &Mutex<PumpState>) -> MutexGuard<'_, PumpState> {
    state.lock().expect("pump state lock poisoned")
}

/// How one delivery attempt ended.
enum Delivery {
    /// The sink took the change.
    Delivered,
    /// The log no longer continues from the pump's position.
    LogMoved,
}

/// Runs the pump until its task is aborted, or until the node no longer serves the database.
pub(super) async fn run(ctx: PumpContext) {
    let sink = ctx.sink.name().to_owned();
    let database = ctx.database.clone();
    let (home, mut position) = recorded_position(&ctx).await;
    {
        let mut state = lock(&ctx.state);
        state.position = position.clone();
        state.home = Some(home);
    }
    let mut backoff = ctx.settings.retry_initial;
    loop {
        let Some(mut feed) = ctx.replication.primary_feed(&database) else {
            return;
        };
        feed.delivered(&sink, position.clone());
        loop {
            let acknowledged = feed.acknowledged();
            {
                let mut state = lock(&ctx.state);
                state.acknowledged = acknowledged.clone();
                state.backlog = feed.published().saturating_sub(state.delivered_at);
            }
            if let Some(until) = acknowledged.filter(|until| Some(until) > position.as_ref()) {
                let epoch = feed.epoch();
                match deliver(&ctx, &feed, epoch, position.clone(), until.clone()).await {
                    Ok(Delivery::Delivered) => {
                        ctx.metrics.delivered(&sink);
                        record(&ctx, home, &until).await;
                        position = Some(until.clone());
                        feed.delivered(&sink, position.clone());
                        {
                            let mut state = lock(&ctx.state);
                            state.position = position.clone();
                            state.delivered_at = feed.published();
                        }
                        if ctx.format == Format::Logical
                            && let Some(streams) = ctx.streams.upgrade()
                        {
                            streams.release(&database);
                        }
                        backoff = ctx.settings.retry_initial;
                        // A paced sink takes what was acknowledged meanwhile as one batch.
                        let pace = ctx.sink.pace();
                        if !pace.is_zero() {
                            time::sleep(pace).await;
                            if !feed.is_current() {
                                break;
                            }
                        }
                        continue;
                    }
                    Ok(Delivery::LogMoved) => {
                        tracing::error!(%sink, %database, ?position, "the log no longer holds the sink's next changes; restarting from the log's beginning");
                        position = None;
                        feed.delivered(&sink, None);
                        lock(&ctx.state).resets += 1;
                        ctx.metrics.reset(&sink);
                        continue;
                    }
                    Err(reason) => {
                        tracing::warn!(%sink, %database, %reason, retry_in = ?backoff, "delivering a change failed");
                        {
                            let mut state = lock(&ctx.state);
                            state.failures += 1;
                            ctx.metrics.delivery_failed(&sink);
                            state.last_error = Some(reason);
                            state.caught_up = false;
                        }
                        time::sleep(backoff).await;
                        backoff = (backoff * 2).min(ctx.settings.retry_max);
                        if !feed.is_current() {
                            break;
                        }
                        continue;
                    }
                }
            }
            lock(&ctx.state).caught_up = true;
            tokio::select! {
                changed = feed.changed() => if changed.is_err() {
                    break;
                },
                () = time::sleep(CURRENCY_CHECK) => {}
            }
            if !feed.is_current() {
                break;
            }
        }
    }
}

/// Where the pump's position is kept and what it is: the mark in the database when its engine
/// keeps marks, which replicates and so outlives the node that recorded it (ADR-0028), else the
/// node's own file.
async fn recorded_position(ctx: &PumpContext) -> (MarkHome, Option<Position>) {
    let sink = ctx.sink.name();
    let database = &ctx.database;
    let marks = match ctx.replication.registry().open(database).await {
        Ok(handle) => handle.sink_marks().await,
        Err(error) => Err(error),
    };
    match marks {
        Ok(Some(mut marks)) => return (MarkHome::Replicated, marks.remove(sink)),
        Ok(None) => {}
        Err(error) => {
            tracing::warn!(%sink, %database, %error, "can't read the sink's mark; keeping its position on this node");
        }
    }
    let position = match ctx.store.load() {
        Ok(position) => position,
        Err(error) => {
            tracing::warn!(%sink, %database, %error, path = %ctx.store.path().display(), "can't read the sink's position; starting from the log's beginning");
            None
        }
    };
    (MarkHome::Local, position)
}

/// Records that the sink has been delivered up to `until`, where its position is kept.
///
/// A mark is staged before the pump asks for a release, so that the adapter never prunes past
/// a mark it hasn't staged. Failing to record is not failing the delivery: the change is
/// delivered again after a restart or a failover, which at-least-once allows, and the next
/// delivery records a newer position.
async fn record(ctx: &PumpContext, home: MarkHome, until: &Position) {
    let sink = ctx.sink.name();
    let database = &ctx.database;
    match home {
        MarkHome::Local => {
            if let Err(error) = ctx.store.save(until) {
                tracing::warn!(%sink, %database, %error, "can't record the sink's position");
            }
        }
        MarkHome::Replicated => {
            let staged = match ctx.replication.registry().open(database).await {
                Ok(handle) => handle.stage_sink_mark(sink.to_owned(), until.clone()).await,
                Err(error) => Err(error),
            };
            if let Err(error) = staged {
                tracing::debug!(%sink, %database, %error, "can't stage the sink's mark");
            }
        }
    }
}

/// Reads the batch from `after` up to `until` and publishes it to the sink, with the primary's
/// chain hashes at both ends if it is physical.
async fn deliver(
    ctx: &PumpContext,
    feed: &PrimaryFeed,
    epoch: LeaseEpoch,
    after: Option<Position>,
    until: Position,
) -> Result<Delivery, String> {
    let handle = ctx
        .replication
        .registry()
        .open(&ctx.database)
        .await
        .map_err(|error| error.to_string())?;
    let batch = match handle.committed_between(after.clone(), until.clone()).await {
        Ok(Some(batch)) => batch,
        // Nothing between the two positions: the sink is at `until` already.
        Ok(None) => return Ok(Delivery::Delivered),
        // A pump already at the log's beginning has nowhere further back to go: the write quorum
        // is at a position the log doesn't hold yet, as when a follower that must take a snapshot
        // still reports its old position. Retrying with backoff, not restarting in a tight loop.
        Err(error) if log_moved(&error) && after.is_some() => return Ok(Delivery::LogMoved),
        Err(error) => return Err(error.to_string()),
    };
    let (kind, chain_start, chain_end) = match ctx.format {
        Format::Physical => {
            // The end first: bringing the chain up to it brings it past the start too.
            let chain_end = feed.chain_at(Some(&until)).await;
            let chain_start = feed.chain_at(after.as_ref()).await;
            (ChangeKind::Physical(batch.payload), chain_start, chain_end)
        }
        Format::Logical => (
            ChangeKind::Logical(
                handle
                    .row_changes(after.clone(), until.clone())
                    .await
                    .map_err(|error| error.to_string())?,
            ),
            None,
            None,
        ),
    };
    let change = Change {
        database: ctx.database.clone(),
        epoch,
        start: after,
        position: until,
        kind,
        chain_start,
        chain_end,
    };
    ctx.sink
        .publish(change)
        .await
        .map_err(|error| error.to_string())?;
    Ok(Delivery::Delivered)
}

/// Whether reading the log failed because it no longer continues from the pump's position.
fn log_moved(error: &DatabaseError) -> bool {
    matches!(
        error.engine_error(),
        Some(EngineError::InvalidPosition { .. })
    )
}
