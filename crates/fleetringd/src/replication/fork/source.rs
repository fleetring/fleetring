//! The two sides that serve a fork's copy (ADR-0046, points 4.1, 4.2, 4.4, and 7):
//!
//! - **The source's primary** pins the source at the position its write quorum acknowledged,
//!   never at its published tip, and streams it to the attempt's seeder. The pin is bounded in
//!   time and in the WAL it lets grow, and pins are bounded per node, per database, and per
//!   hour; a pin past its bounds ends its stream with `ABORTED` and is released, since the
//!   source's writes always win over a fork.
//! - **The seeder** streams its staged copy to the fork's other replicas, with the values its
//!   sidecar recorded.

use std::collections::{BTreeMap, VecDeque};
use std::fs::File;
use std::pin::Pin;
use std::sync::{Arc, Mutex, MutexGuard};
use std::task::{Context, Poll};
use std::time::Duration;

use fleetring_control::{DatabaseName, Incarnation};
use fleetring_proto::v1::SnapshotChunk;
use tokio::sync::mpsc;
use tokio::time::{self, Instant};
use tokio_stream::Stream;
use tonic::Status;
use tonic::metadata::MetadataValue;

use super::super::Inner;
use super::super::archive::{CHECKSUM_LEN, Packer};
use super::super::binding::{SeedingGrant, SeedingRequest};
use super::super::chain::ChainHash;
use super::super::follower::blocking;
use super::super::primary::{self, Primary};
use super::seed::PIN_EXPIRED_KEY;
use super::staged::{Held, StagedKey};
use super::with_retry_after;
use crate::ForkConfig;
use crate::engine::{EngineError, PinnedSnapshot, SnapshotFile, SnapshotGuard};

/// The largest chunk a seeding stream sends.
const CHUNK_BYTES: usize = 1024 * 1024;

/// How long a seeder waits before asking again when the source's acknowledged position isn't
/// in its log yet, or a peer waits for the seeder's staged copy.
pub(crate) const RETRY_SOON: Duration = Duration::from_millis(250);

/// How long a seeder waits before asking again when the source's primary holds as many pins as
/// it may.
const RETRY_PINNED: Duration = Duration::from_secs(1);

/// The window `max_pin_ms_per_hour` counts pins in.
const HOUR: Duration = Duration::from_secs(60 * 60);

/// The pins a source's primary holds for seeding streams, and those it held in the last hour.
#[derive(Debug, Default)]
pub(crate) struct PinLedger {
    state: Mutex<PinState>,
}

#[derive(Debug, Default)]
struct PinState {
    next: u64,
    /// Each pin held now: its database, when it began, and the WAL growth it has held so far.
    open: BTreeMap<u64, (DatabaseName, Instant, u64)>,
    /// The pins and checkpoint holds that ended in the last hour, by database: when each began
    /// and ended.
    closed: BTreeMap<DatabaseName, VecDeque<(Instant, Instant)>>,
    /// Each database's checkpoint hold for a seeder waiting for its acknowledged position to
    /// enter the log (ADR-0046, point 4.2): when it began and when it ends. It counts toward
    /// `max_pin_ms_per_hour` as a pin does, since it delays the source's checkpoints as one does.
    holds: BTreeMap<DatabaseName, (Instant, Instant)>,
}

impl PinState {
    /// Moves `database`'s checkpoint hold among the ended ones if it ended by `now`.
    fn expire_hold(&mut self, database: &DatabaseName, now: Instant) {
        if let Some((start, until)) = self.holds.get(database).copied()
            && until <= now
        {
            self.holds.remove(database);
            self.closed
                .entry(database.clone())
                .or_default()
                .push_back((start, until));
        }
    }

    /// How long `database` has been pinned or had its checkpoints held in the hour before `now`.
    fn pinned_in_hour(&mut self, database: &DatabaseName, now: Instant) -> Duration {
        let since = now.checked_sub(HOUR).unwrap_or(now);
        self.expire_hold(database, now);
        let held = self
            .holds
            .get(database)
            .map_or(Duration::ZERO, |(start, _)| {
                now.saturating_duration_since((*start).max(since))
            });
        let closed = self.closed.entry(database.clone()).or_default();
        while closed.front().is_some_and(|(_, end)| *end <= since) {
            closed.pop_front();
        }
        let ended: Duration = closed
            .iter()
            .map(|(start, end)| end.saturating_duration_since((*start).max(since)))
            .sum();
        let open: Duration = self
            .open
            .values()
            .filter(|(pinned, ..)| pinned == database)
            .map(|(_, start, _)| now.saturating_duration_since((*start).max(since)))
            .sum();
        ended + open + held
    }

    /// `RESOURCE_EXHAUSTED` for `database`, whose hour of pins is spent, with a retry-after of
    /// when its oldest pin in the hour leaves it.
    fn hour_spent(&self, database: &DatabaseName, now: Instant) -> Status {
        let oldest = self
            .closed
            .get(database)
            .and_then(|closed| closed.iter().map(|(start, _)| *start).min())
            .map_or(RETRY_PINNED, |start| {
                (start + HOUR).saturating_duration_since(now)
            });
        with_retry_after(
            Status::resource_exhausted(format!(
                "{database} has been pinned for seeding, or had its checkpoints held for it, as \
                 long as [fork] max_pin_ms_per_hour allows in the last hour; retry later"
            )),
            oldest.max(RETRY_PINNED),
        )
    }
}

impl PinLedger {
    fn lock(&self) -> MutexGuard<'_, PinState> {
        self.state.lock().expect("pin ledger lock poisoned")
    }

    /// Admits a pin of `database` under `config`, and returns how long it may last, or
    /// `RESOURCE_EXHAUSTED` with a retry-after if the node, the database, or the database's hour
    /// holds as many pins as it may. A refusal doesn't fail the seeder's attempt.
    fn admit(
        &self,
        database: &DatabaseName,
        config: &ForkConfig,
    ) -> Result<(u64, Instant, Duration), Status> {
        let now = Instant::now();
        let mut state = self.lock();
        let held = state.open.len();
        if held >= config.max_pins_per_node.get() {
            return Err(with_retry_after(
                Status::resource_exhausted(format!(
                    "this node holds {held} seeding pins, as many as [fork] max_pins_per_node \
                     allows; retry"
                )),
                RETRY_PINNED,
            ));
        }
        let of_database = state
            .open
            .values()
            .filter(|(pinned, ..)| pinned == database)
            .count();
        if of_database >= config.max_pins_per_database.get() {
            return Err(with_retry_after(
                Status::resource_exhausted(format!(
                    "{database} has {of_database} seeding pins, as many as [fork] \
                     max_pins_per_database allows; retry"
                )),
                RETRY_PINNED,
            ));
        }
        let used = state.pinned_in_hour(database, now);
        let budget = config.max_pin_per_hour().saturating_sub(used);
        if budget.is_zero() {
            return Err(state.hour_spent(database, now));
        }
        let id = state.next;
        state.next += 1;
        state.open.insert(id, (database.clone(), now, 0));
        Ok((id, now, config.max_pin().min(budget)))
    }

    /// Records that pin `id` holds `bytes` of WAL growth so far, and returns the WAL every pin
    /// holds together.
    fn holds(&self, id: u64, bytes: u64) -> u64 {
        let mut state = self.lock();
        if let Some(open) = state.open.get_mut(&id) {
            open.2 = bytes;
        }
        state.open.values().map(|(.., held)| *held).sum()
    }

    /// Holds `database`'s checkpoints for a seeder whose acknowledged position is behind the
    /// log's start, for at most `max_pin_ms` and what is left of `max_pin_ms_per_hour`, and
    /// returns how long the hold in force lasts from now: a new one, or the one in force
    /// already, which isn't extended. The caller arms it on the primary serving now, which may
    /// not be the one that armed it first.
    ///
    /// # Errors
    ///
    /// `RESOURCE_EXHAUSTED` with a retry-after once the hour's budget is spent: a seeder asking
    /// again never re-arms a hold past it.
    fn hold(&self, database: &DatabaseName, config: &ForkConfig) -> Result<Duration, Status> {
        let now = Instant::now();
        let mut state = self.lock();
        let used = state.pinned_in_hour(database, now);
        if let Some((_, until)) = state.holds.get(database) {
            return Ok(until.saturating_duration_since(now));
        }
        let budget = config.max_pin_per_hour().saturating_sub(used);
        if budget.is_zero() {
            return Err(state.hour_spent(database, now));
        }
        let held = config.max_pin().min(budget);
        state.holds.insert(database.clone(), (now, now + held));
        Ok(held)
    }

    /// Ends `database`'s checkpoint hold, if it has one, once a seeder has its pin: the time it
    /// was in force stays counted.
    fn end_hold(&self, database: &DatabaseName) {
        let now = Instant::now();
        let mut state = self.lock();
        if let Some((start, until)) = state.holds.remove(database) {
            state
                .closed
                .entry(database.clone())
                .or_default()
                .push_back((start, until.min(now)));
        }
    }

    /// Withdraws pin `id`'s admission: the copy was never pinned, so it counts for nothing.
    fn withdraw(&self, id: u64) {
        self.lock().open.remove(&id);
    }

    /// Ends pin `id`, and returns how long it was held and the WAL every pin holds now.
    fn release(&self, id: u64) -> (Duration, u64) {
        let now = Instant::now();
        let mut state = self.lock();
        let held = state
            .open
            .remove(&id)
            .map_or(Duration::ZERO, |(database, start, _)| {
                state
                    .closed
                    .entry(database)
                    .or_default()
                    .push_back((start, now));
                now.saturating_duration_since(start)
            });
        (held, state.open.values().map(|(.., held)| *held).sum())
    }
}

/// A pin admitted but not taken yet: withdrawn, without counting as a pin, when dropped.
struct Admitted<'ledger> {
    pins: &'ledger PinLedger,
    id: u64,
}

impl Drop for Admitted<'_> {
    fn drop(&mut self) {
        self.pins.withdraw(self.id);
    }
}

/// A seeding pin, released when dropped.
#[derive(Debug)]
struct SeedingPin {
    inner: Arc<Inner>,
    id: u64,
    database: DatabaseName,
    /// When the pin must be released, whatever the stream's state.
    deadline: Instant,
    /// The source's published bytes when the pin began, which the WAL growth counts from.
    published_from: u64,
    max_wal: u64,
}

impl SeedingPin {
    /// How many bytes the source's WAL grew while the pin was held.
    fn growth(&self, primary: &Primary) -> u64 {
        let growth = primary
            .published_bytes()
            .saturating_sub(self.published_from);
        let held = self.inner.forks.pins().holds(self.id, growth);
        self.inner.metrics.fork_pinned_wal_bytes(held);
        growth
    }
}

impl Drop for SeedingPin {
    fn drop(&mut self) {
        let (held, wal) = self.inner.forks.pins().release(self.id);
        self.inner.metrics.fork_pin(held);
        self.inner.metrics.fork_pinned_wal_bytes(wal);
        tracing::info!(target: "fleetringd::audit", source = %self.database, held_ms = u64::try_from(held.as_millis()).unwrap_or(u64::MAX), "released a seeding pin");
    }
}

/// The stream a seeding `Snapshot` answers with: the chunks, then, if the sender gave up, the
/// status it gave up with, after every chunk it sent.
pub(in crate::replication) struct SeedingStream {
    chunks: mpsc::Receiver<Result<SnapshotChunk, Status>>,
    ending: Arc<Mutex<Option<Status>>>,
}

impl Stream for SeedingStream {
    type Item = Result<SnapshotChunk, Status>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.chunks.poll_recv(cx) {
            Poll::Ready(None) => Poll::Ready(
                self.ending
                    .lock()
                    .expect("seeding stream lock poisoned")
                    .take()
                    .map(Err),
            ),
            other => other,
        }
    }
}

/// What a stream's last chunk carries beside the position and the checksum.
#[derive(Debug, Clone, Copy)]
struct Ending {
    /// The chain hash at the copy's position.
    chain: ChainHash,
    /// The source incarnation stamped on the copy.
    stamp: Incarnation,
    /// The checksum the stream must have, for a staged copy whose sidecar recorded one.
    checksum: Option<[u8; CHECKSUM_LEN]>,
}

/// Serves a seeding `Snapshot` the binding granted: the source's pinned copy, from its primary,
/// or the seeder's staged copy, to a peer.
pub(in crate::replication) async fn serve(
    inner: &Arc<Inner>,
    request: SeedingRequest,
    grant: SeedingGrant,
) -> Result<SeedingStream, Status> {
    if request.staged {
        serve_staged(inner, &request)
    } else {
        serve_source(inner, &request, &grant).await
    }
}

/// Pins the source at its acknowledged position and streams it to the attempt's seeder.
async fn serve_source(
    inner: &Arc<Inner>,
    request: &SeedingRequest,
    grant: &SeedingGrant,
) -> Result<SeedingStream, Status> {
    let source = &request.database;
    let config = inner.forks.config();
    let primary = inner
        .primary(source)
        .filter(|primary| primary.role().epoch == grant.source_epoch)
        .ok_or_else(|| {
            with_retry_after(
                Status::unavailable(format!("{source} has no primary on this node any more")),
                RETRY_SOON,
            )
        })?;
    // The open copy is checked too: it must be the source incarnation the request names.
    let recorded = inner.epochs.record(source).incarnation();
    if recorded != Some(request.source_incarnation)
        && !fleetring_control::mutation::Mutation::SkipSourceIncarnation.is_active()
    {
        return Err(Status::failed_precondition(format!(
            "this node's copy of {source} is incarnation {}, not {}",
            recorded.map_or_else(
                || "unrecorded".to_owned(),
                |incarnation| incarnation.to_string()
            ),
            request.source_incarnation
        )));
    }
    let (id, _, budget) = inner.forks.pins().admit(source, &config)?;
    // Until the copy is pinned, the admission is withdrawn on any return, and counts as no pin.
    let admitted = Admitted {
        pins: inner.forks.pins(),
        id,
    };
    let published_from = primary.published_bytes();
    let acknowledged = match primary.committed() {
        tip if fleetring_control::mutation::Mutation::PinAtTip.is_active() => tip,
        None => None,
        Some(_) => match primary.quorum_held(&inner.followers_of(&primary)) {
            Some(acknowledged) => Some(acknowledged),
            None => {
                return Err(with_retry_after(
                    Status::unavailable(format!(
                        "no position of {source} is acknowledged by its write quorum yet; retry"
                    )),
                    RETRY_SOON,
                ));
            }
        },
    };
    let handle = inner
        .registry()
        .open(source)
        .await
        .map_err(|error| Status::unavailable(error.to_string()))?;
    let pinned = match handle.pin_snapshot_at(acknowledged.clone()).await {
        Ok(pinned) => pinned,
        Err(error)
            if matches!(
                error.engine_error(),
                Some(EngineError::InvalidPosition { .. })
            ) =>
        {
            // The log restarted past the acknowledged position. Hold the next checkpoint, for at
            // most `max_pin_ms`, so that the position catches up inside the log. The hold counts
            // toward the source's hour of pins, and isn't armed again past it. A hold in force is
            // armed again on the primary serving now, until it ends: a primary that replaced the
            // one that armed it, at the same node, would otherwise checkpoint through it.
            let held = inner.forks.pins().hold(source, &config)?;
            let now = inner.clock.now();
            let until = now
                .as_millis()
                .saturating_add(u64::try_from(held.as_millis()).unwrap_or(u64::MAX));
            primary.hold_checkpoints(now, fleetring_control::Timestamp::from_millis(until));
            return Err(with_retry_after(
                Status::unavailable(format!(
                    "{source} checkpointed past the position its write quorum acknowledged; \
                     checkpoints wait until it catches up, retry"
                )),
                RETRY_SOON,
            ));
        }
        Err(error) => {
            return Err(Status::unavailable(format!("can't pin {source}: {error}")));
        }
    };
    primary.release_checkpoints();
    inner.forks.pins().end_hold(source);
    std::mem::forget(admitted);
    let pin = SeedingPin {
        inner: Arc::clone(inner),
        id,
        database: source.clone(),
        deadline: Instant::now() + budget,
        published_from,
        max_wal: config.max_pin_wal_bytes.get(),
    };
    let packer = Packer::new(pinned).map_err(|error| Status::internal(error.to_string()))?;
    primary::chain_to(inner, &primary, &handle, packer.position()).await;
    let chain = primary.hash_at(packer.position()).ok_or_else(|| {
        with_retry_after(
            Status::unavailable(format!(
                "the hash chain of {source} doesn't reach the pinned position yet; retry"
            )),
            RETRY_SOON,
        )
    })?;
    tracing::info!(
        target: "fleetringd::audit",
        source = %source,
        source_incarnation = %request.source_incarnation,
        fork = %request.fork,
        attempt = request.attempt,
        position = ?packer.position(),
        bytes = packer.total_len(),
        budget_ms = u64::try_from(budget.as_millis()).unwrap_or(u64::MAX),
        "pinned the source for a fork's seeder"
    );
    let ending = Ending {
        chain,
        stamp: request.source_incarnation,
        checksum: None,
    };
    inner
        .forks
        .faults()
        .pass(super::FaultPoint::Streaming, request.fork.operation)
        .await;
    Ok(spawn_sender(inner, packer, ending, Some((pin, primary))))
}

/// A guard that says a staged copy's files never change while they are read: nothing writes a
/// staged copy once it is committed.
#[derive(Debug)]
struct Committed;

impl SnapshotGuard for Committed {
    fn confirm(&self) -> Result<(), EngineError> {
        Ok(())
    }
}

/// Streams this node's staged copy of the fork `request` names to a peer, as its sidecar
/// recorded it.
fn serve_staged(inner: &Arc<Inner>, request: &SeedingRequest) -> Result<SeedingStream, Status> {
    let key = StagedKey {
        fork: request.fork.clone(),
        attempt: request.attempt,
    };
    let staged = inner
        .forks
        .store()
        .get(&key)
        .filter(|staged| staged.held == Held::Present)
        .ok_or_else(|| {
            with_retry_after(
                Status::unavailable(format!(
                    "the seeder hasn't staged attempt {} of {} yet; retry",
                    request.attempt, request.fork
                )),
                RETRY_SOON,
            )
        })?;
    let sidecar = staged.sidecar;
    let dir = inner.forks.store().copy_dir(&key);
    let files = sidecar
        .files
        .iter()
        .map(|file| {
            File::open(dir.join(&file.name))
                .map(|opened| SnapshotFile::new(file.name.clone(), opened, file.len))
        })
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| Status::unavailable(format!("can't read the staged copy: {error}")))?;
    let snapshot = PinnedSnapshot::new(Some(sidecar.position.clone()), files, Committed);
    let packer = Packer::new(snapshot).map_err(|error| Status::internal(error.to_string()))?;
    let ending = Ending {
        chain: sidecar.chain_hash,
        stamp: sidecar.source_incarnation,
        checksum: Some(sidecar.checksum),
    };
    Ok(spawn_sender(inner, packer, ending, None))
}

/// Starts streaming `packer`'s chunks, the first with the stream's size and the last with
/// `ending`'s values, bounded by `pin` when it streams a source's pinned copy.
fn spawn_sender(
    inner: &Arc<Inner>,
    packer: Packer,
    ending: Ending,
    pin: Option<(SeedingPin, Arc<Primary>)>,
) -> SeedingStream {
    let (chunks, receiver) = mpsc::channel(2);
    let stop = Arc::new(Mutex::new(None));
    let execution = inner.execution;
    let limit = inner.settings.open_timeout;
    let given_up = Arc::clone(&stop);
    tokio::spawn(async move {
        if let Err(status) = send(packer, ending, pin, execution, limit, &chunks).await {
            *given_up.lock().expect("seeding stream lock poisoned") = Some(status);
        }
    });
    SeedingStream {
        chunks: receiver,
        ending: stop,
    }
}

/// Sends every chunk. Returns the status the stream ends with if it gives up: `ABORTED` once
/// the pin is past its time or its WAL growth.
async fn send(
    mut packer: Packer,
    ending: Ending,
    pin: Option<(SeedingPin, Arc<Primary>)>,
    execution: crate::engine::Execution,
    limit: Duration,
    chunks: &mpsc::Sender<Result<SnapshotChunk, Status>>,
) -> Result<(), Status> {
    let total = packer.total_len();
    let mut first = true;
    loop {
        if let Some((pin, primary)) = &pin {
            check_pin(pin, primary)?;
        }
        let offset = packer.offset();
        let (returned, next) = blocking(execution, move || {
            let next = packer.next_chunk(CHUNK_BYTES);
            (packer, next)
        })
        .await?;
        packer = returned;
        let mut chunk = match next {
            Ok(Some(data)) => SnapshotChunk {
                offset,
                data,
                ..SnapshotChunk::default()
            },
            Ok(None) => {
                let checksum = packer
                    .finish()
                    .map_err(|error| Status::aborted(format!("the copy changed: {error}")))?;
                if ending.checksum.is_some_and(|recorded| recorded != checksum) {
                    return Err(Status::data_loss(
                        "the staged copy no longer matches its sidecar",
                    ));
                }
                SnapshotChunk {
                    offset,
                    position: Some(
                        packer
                            .position()
                            .map(|position| position.as_bytes().to_vec())
                            .unwrap_or_default(),
                    ),
                    checksum: Some(checksum.to_vec()),
                    chain: Some(ending.chain.as_bytes().to_vec()),
                    source_incarnation: Some(ending.stamp.get()),
                    ..SnapshotChunk::default()
                }
            }
            Err(error) => return Err(Status::internal(format!("can't read the copy: {error}"))),
        };
        if first {
            chunk.total_bytes = Some(total);
            first = false;
        }
        let last = chunk.position.is_some();
        let wait = match &pin {
            Some((pin, _)) => limit.min(pin.deadline.saturating_duration_since(Instant::now())),
            None => limit,
        };
        match time::timeout(wait, chunks.send(Ok(chunk))).await {
            Ok(Ok(())) => {}
            // The receiver went away: nothing to tell it.
            Ok(Err(_)) => return Ok(()),
            Err(_) => {
                if let Some((pin, primary)) = &pin {
                    check_pin(pin, primary)?;
                }
                return Err(Status::deadline_exceeded(
                    "the receiver stopped taking the copy",
                ));
            }
        }
        if last {
            return Ok(());
        }
    }
}

/// `ABORTED` once `pin` is past its time or the WAL growth it may hold.
fn check_pin(pin: &SeedingPin, primary: &Primary) -> Result<(), Status> {
    let expired = |message: String| {
        let mut status = Status::aborted(message);
        status
            .metadata_mut()
            .insert(PIN_EXPIRED_KEY, MetadataValue::from_static("1"));
        status
    };
    if Instant::now() >= pin.deadline {
        return Err(expired(format!(
            "the seeding pin on {} expired ([fork] max_pin_ms or max_pin_ms_per_hour); it is \
             released",
            pin.database
        )));
    }
    let growth = pin.growth(primary);
    if growth >= pin.max_wal {
        return Err(expired(format!(
            "{} grew {growth} bytes of WAL while pinned, past [fork] max_pin_wal_bytes; the pin \
             is released",
            pin.database
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;

    use tonic::Code;

    use super::*;
    use crate::replication::fork::retry_after;

    fn config(max_pin_ms: u64, max_pin_ms_per_hour: u64) -> ForkConfig {
        ForkConfig {
            max_pin_ms: NonZeroU64::new(max_pin_ms).expect("nonzero"),
            max_pin_ms_per_hour: NonZeroU64::new(max_pin_ms_per_hour).expect("nonzero"),
            ..ForkConfig::default()
        }
    }

    #[tokio::test(start_paused = true)]
    async fn checkpoint_holds_spend_the_hour_of_pins_and_are_never_re_armed_past_it() {
        let ledger = PinLedger::default();
        let source: DatabaseName = "src".parse().expect("a name");
        let config = config(1_000, 2_500);
        let ms = Duration::from_millis;
        assert_eq!(ledger.hold(&source, &config).ok(), Some(ms(1_000)));
        // A seeder asking again while the hold is in force doesn't extend it, and learns how
        // long it still lasts, to arm it on the primary serving now.
        time::advance(ms(400)).await;
        assert_eq!(ledger.hold(&source, &config).ok(), Some(ms(600)));
        // Once it lapses, a new one gets what is left of the hour, at most `max_pin_ms`.
        time::advance(ms(600)).await;
        assert_eq!(ledger.hold(&source, &config).ok(), Some(ms(1_000)));
        time::advance(ms(1_000)).await;
        assert_eq!(ledger.hold(&source, &config).ok(), Some(ms(500)));
        time::advance(ms(500)).await;
        // The hour is spent: no hold is armed, and no pin is admitted either.
        let refused = ledger
            .hold(&source, &config)
            .expect_err("the hour is spent");
        assert_eq!(refused.code(), Code::ResourceExhausted);
        assert!(retry_after(&refused).is_some_and(|after| after > Duration::ZERO));
        let refused = ledger
            .admit(&source, &config)
            .expect_err("the hour is spent");
        assert_eq!(refused.code(), Code::ResourceExhausted);
        // An hour later the budget is back.
        time::advance(HOUR).await;
        assert_eq!(ledger.hold(&source, &config).ok(), Some(ms(1_000)));
    }

    #[tokio::test(start_paused = true)]
    async fn a_hold_ended_early_counts_only_while_it_was_in_force() {
        let ledger = PinLedger::default();
        let source: DatabaseName = "src".parse().expect("a name");
        let config = config(1_000, 1_000);
        let ms = Duration::from_millis;
        assert_eq!(ledger.hold(&source, &config).ok(), Some(ms(1_000)));
        time::advance(ms(300)).await;
        ledger.end_hold(&source);
        time::advance(ms(10)).await;
        assert_eq!(ledger.hold(&source, &config).ok(), Some(ms(700)));
    }
}
