//! The per-sink, per-database pump: tails acknowledged batches and hands them to the sink.

use std::sync::{Arc, Mutex, MutexGuard, Weak};
use std::time::Duration;

use fleetring_control::{DatabaseName, LeaseEpoch};
use tokio::time;

use crate::DatabaseError;
use crate::changes::position::PositionStore;
use crate::changes::{Change, ChangeKind, ChangeSettings, ChangeSink, Format};
use crate::engine::{EngineError, Position};
use crate::replication::Replication;

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
    /// The streams, which release delivered row changes once every logical sink has them.
    pub(super) streams: Weak<super::Inner>,
}

/// A pump's progress, as the node's status reports it.
#[derive(Debug, Clone, Default)]
pub(super) struct PumpState {
    /// The last position delivered and recorded.
    pub(super) position: Option<Position>,
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
    let mut position = match ctx.store.load() {
        Ok(position) => position,
        Err(error) => {
            tracing::warn!(%sink, %database, %error, path = %ctx.store.path().display(), "can't read the sink's position; starting from the log's beginning");
            None
        }
    };
    lock(&ctx.state).position = position.clone();
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
                match deliver(&ctx, epoch, position.clone(), until.clone()).await {
                    Ok(Delivery::Delivered) => {
                        if let Err(error) = ctx.store.save(&until) {
                            // Delivered but not recorded: the change is delivered again after
                            // a restart, which at-least-once allows.
                            tracing::warn!(%sink, %database, %error, "can't record the sink's position");
                        }
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
                        continue;
                    }
                    Ok(Delivery::LogMoved) => {
                        tracing::error!(%sink, %database, ?position, "the log no longer holds the sink's next changes; restarting from the log's beginning");
                        position = None;
                        feed.delivered(&sink, None);
                        lock(&ctx.state).resets += 1;
                        continue;
                    }
                    Err(reason) => {
                        tracing::warn!(%sink, %database, %reason, retry_in = ?backoff, "delivering a change failed");
                        {
                            let mut state = lock(&ctx.state);
                            state.failures += 1;
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

/// Reads the batch from `after` up to `until` and publishes it to the sink.
async fn deliver(
    ctx: &PumpContext,
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
        Err(error) if log_moved(&error) => return Ok(Delivery::LogMoved),
        Err(error) => return Err(error.to_string()),
    };
    let kind = match ctx.format {
        Format::Physical => ChangeKind::Physical(batch.payload),
        Format::Logical => ChangeKind::Logical(
            handle
                .row_changes(after.clone(), until.clone())
                .await
                .map_err(|error| error.to_string())?,
        ),
    };
    let change = Change {
        database: ctx.database.clone(),
        epoch,
        start: after,
        position: until,
        kind,
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
