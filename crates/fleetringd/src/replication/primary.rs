//! The primary side: one stream per follower, the write quorum, and checkpoints.

use std::collections::{BTreeMap, VecDeque};
use std::fmt;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use fleetring::NodeId;
use fleetring_control::{DatabaseName, Timestamp};
use fleetring_proto::v1::replicate_message::Message;
use fleetring_proto::v1::{self as proto, ReplicateAck, ReplicateMessage, ReplicateOpen};
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tokio::time::{self, Instant};
use tokio_stream::wrappers::ReceiverStream;
use tonic::metadata::MetadataValue;

use super::{
    FollowerProgress, Inner, PRIMARY_METADATA_KEY, PrimaryRole, PrimaryStatus, Quorum, failed,
    refused,
};
use crate::DatabaseError;
use crate::engine::{CheckpointMarker, EngineError, FrameBatch, Position};
use crate::sink::{Acknowledged, Acknowledgement, ReplicationError};
use crate::transport::replication_client;

/// One database this node is primary for: what it has published, and how far each follower has
/// got.
#[derive(Debug)]
pub(super) struct Primary {
    name: DatabaseName,
    role: PrimaryRole,
    /// Where the epoch's log began, if this node was promoted to it: followers whose logs came
    /// from earlier epochs truncate back to it (ADR-0018, point 5).
    start: Option<Position>,
    state: Mutex<State>,
    /// Bumped whenever a batch is published, a follower acknowledges, or a checkpoint ends, so
    /// streams and waiting writes look again.
    changed: watch::Sender<u64>,
}

#[derive(Debug, Default)]
struct State {
    /// The end of the last committed batch.
    committed: Option<Position>,
    /// Published batches that some follower may still need, oldest first, each continuing the
    /// one before.
    recent: VecDeque<FrameBatch>,
    /// The position the oldest retained batch continues from, once known.
    base: Option<Option<Position>>,
    recent_bytes: usize,
    /// Payload bytes published since the last checkpoint.
    log_bytes: u64,
    /// How many times a checkpoint has started or ended: odd while one runs.
    checkpoints: u64,
    /// The last checkpoint's marker.
    marker: Option<CheckpointMarker>,
    /// How many batches have been published since this primary started serving.
    published: u64,
    followers: BTreeMap<NodeId, Progress>,
    /// How far each change stream's sink has got (FR-039).
    sinks: BTreeMap<String, SinkProgress>,
}

#[derive(Debug, Clone, Default)]
struct SinkProgress {
    position: Option<Position>,
    behind_since: Option<Timestamp>,
}

#[derive(Debug, Clone, Default)]
struct Progress {
    acked: Option<Position>,
    reported: bool,
    connected: bool,
    behind_since: Option<Timestamp>,
    /// Lagged past the limit, so it no longer blocks checkpoints or keeps batches retained.
    released: bool,
    /// The log no longer holds what it needs.
    needs_snapshot: bool,
}

/// What a stream sends next.
#[derive(Debug)]
enum Plan {
    /// Nothing: the follower has everything published.
    Wait,
    /// A published batch, as it was captured.
    Send(FrameBatch),
    /// Every commit after `after`, read back from the log as one batch, valid only if no
    /// checkpoint started or ended meanwhile.
    Read {
        after: Option<Position>,
        checkpoints: u64,
    },
    /// The log restarted past the follower.
    NeedsSnapshot,
}

impl Primary {
    pub(super) fn new(name: DatabaseName, role: PrimaryRole, start: Option<Position>) -> Self {
        Self {
            name,
            role,
            start,
            state: Mutex::default(),
            changed: watch::Sender::new(0),
        }
    }

    pub(super) fn name(&self) -> &DatabaseName {
        &self.name
    }

    pub(super) fn role(&self) -> PrimaryRole {
        self.role
    }

    pub(super) fn committed(&self) -> Option<Position> {
        self.lock().committed.clone()
    }

    /// How many batches have been published since this primary started serving.
    pub(super) fn published(&self) -> u64 {
        self.lock().published
    }

    /// The highest position the write quorum holds: every batch ending at or before it is
    /// acknowledged, or would be if its writer were still waiting (ADR-0018). `None` while no
    /// batch is.
    ///
    /// With `quorum.needed` followers required, that is the `needed`-th highest position the
    /// followers have reported, capped at the log's end; the primary alone decides when
    /// `needed` is 0.
    pub(super) fn quorum_held(&self, quorum: &Quorum) -> Option<Position> {
        let state = self.lock();
        let committed = state.committed.as_ref()?;
        if quorum.needed == 0 {
            return Some(committed.clone());
        }
        let mut acked: Vec<&Position> = quorum
            .followers
            .iter()
            .filter_map(|node| state.followers.get(node))
            .filter(|progress| progress.reported)
            .filter_map(|progress| progress.acked.as_ref())
            .collect();
        acked.sort_unstable_by(|a, b| b.cmp(a));
        let held = acked.get(quorum.needed - 1)?;
        Some((*held).min(committed).clone())
    }

    /// A subscription to this primary's changes, for a change stream's pump.
    pub(super) fn changes(&self) -> watch::Receiver<u64> {
        self.changed.subscribe()
    }

    /// Records that a change stream's `sink` has delivered up to `position`.
    pub(super) fn sink_delivered(&self, sink: &str, position: Option<Position>, now: Timestamp) {
        let mut state = self.lock();
        let behind = position < state.committed;
        let progress = state.sinks.entry(sink.to_owned()).or_default();
        progress.position = position;
        progress.behind_since = behind.then_some(now);
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().expect("primary state lock poisoned")
    }

    fn notify(&self) {
        self.changed
            .send_modify(|version| *version = version.wrapping_add(1));
    }

    pub(super) fn status(&self, quorum: &Quorum) -> PrimaryStatus {
        let state = self.lock();
        PrimaryStatus {
            epoch: self.role.epoch,
            committed: state.committed.clone(),
            checkpointed: state.marker.as_ref().map(|marker| marker.position.clone()),
            followers: quorum
                .followers
                .iter()
                .map(|node| {
                    let progress = state.followers.get(node).cloned().unwrap_or_default();
                    FollowerProgress {
                        node: node.clone(),
                        acknowledged: progress.acked,
                        reported: progress.reported,
                        connected: progress.connected,
                        needs_snapshot: progress.needs_snapshot,
                    }
                })
                .collect(),
        }
    }

    /// Learns that the log ends at least at `position`, as it does when serving starts.
    fn raise_committed(&self, position: Option<Position>) {
        {
            let mut state = self.lock();
            if position > state.committed {
                state.committed = position;
            }
        }
        self.notify();
    }

    /// How many of `followers` have acknowledged `end` or later.
    fn acknowledged_by(&self, followers: &[NodeId], end: &Position) -> usize {
        let state = self.lock();
        followers
            .iter()
            .filter(|node| {
                state.followers.get(*node).is_some_and(|progress| {
                    progress.reported && progress.acked.as_ref().is_some_and(|acked| acked >= end)
                })
            })
            .count()
    }

    /// How many of `followers` have an open stream and are live.
    pub(super) fn reachable(&self, inner: &Inner, followers: &[NodeId]) -> usize {
        let connected: Vec<&NodeId> = {
            let state = self.lock();
            followers
                .iter()
                .filter(|node| state.followers.get(*node).is_some_and(|p| p.connected))
                .collect()
        };
        connected
            .into_iter()
            .filter(|node| inner.is_live(node))
            .count()
    }

    /// Keeps progress only for `followers`, the current replica set.
    fn retain_followers(&self, followers: &[NodeId], now: Timestamp) {
        let mut state = self.lock();
        state.followers.retain(|node, _| followers.contains(node));
        for node in followers {
            state
                .followers
                .entry(node.clone())
                .or_insert_with(|| Progress {
                    behind_since: Some(now),
                    ..Progress::default()
                });
        }
    }

    /// Records a follower's position, from its first acknowledgement on a new stream or a later
    /// one.
    fn acknowledged(&self, node: &NodeId, position: Option<Position>, retained_bytes: usize) {
        {
            let mut state = self.lock();
            let past_marker = state
                .marker
                .as_ref()
                .is_none_or(|marker| position.as_ref() >= Some(&marker.position));
            let progress = state.followers.entry(node.clone()).or_default();
            // A follower that makes progress isn't lagging, however far behind a busy log it is.
            if !progress.reported || position > progress.acked {
                progress.behind_since = None;
            }
            progress.acked = position;
            progress.reported = true;
            progress.connected = true;
            if past_marker {
                progress.released = false;
                progress.needs_snapshot = false;
            }
            state.prune(retained_bytes);
        }
        self.notify();
    }

    /// Records that the log no longer holds what `node` needs.
    fn needs_snapshot(&self, node: &NodeId) {
        if let Some(progress) = self.lock().followers.get_mut(node) {
            progress.needs_snapshot = true;
            progress.released = true;
        }
        self.notify();
    }

    fn disconnected(&self, node: &NodeId) {
        if let Some(progress) = self.lock().followers.get_mut(node) {
            progress.connected = false;
        }
        self.notify();
    }

    fn checkpoints(&self) -> u64 {
        self.lock().checkpoints
    }

    fn marker(&self) -> Option<CheckpointMarker> {
        self.lock().marker.clone()
    }

    /// Decides what a stream whose follower holds the log up to `sent` sends next.
    fn plan(&self, sent: Option<&Position>) -> Plan {
        let state = self.lock();
        let Some(committed) = &state.committed else {
            return Plan::Wait;
        };
        if sent >= Some(committed) {
            return Plan::Wait;
        }
        let behind_marker = state
            .marker
            .as_ref()
            .is_some_and(|marker| sent < Some(&marker.position));
        let checkpointing = state.checkpoints % 2 == 1;
        let next = state.following(sent).map(|index| {
            let rest = state.recent.len() - index;
            (state.recent.get(index), rest)
        });
        let next = match next {
            Some((Some(batch), rest)) => Some((batch, rest)),
            _ => None,
        };
        match next {
            // A single batch, or a batch from before the last checkpoint, which the log no
            // longer holds: send it as captured.
            Some((batch, rest)) if rest == 1 || behind_marker || checkpointing => {
                Plan::Send(batch.clone())
            }
            _ if behind_marker => Plan::NeedsSnapshot,
            _ if checkpointing => Plan::Wait,
            // Several batches, or ones no longer kept: read them back from the log as one.
            _ => Plan::Read {
                after: sent.cloned(),
                checkpoints: state.checkpoints,
            },
        }
    }

    /// Starts a checkpoint if one is due: enough has been published since the last, and every
    /// follower either holds everything or can still get the rest from the retained batches,
    /// apart from those behind for longer than `lag_limit`. Returns the followers left behind, or
    /// `None` if no checkpoint is due.
    fn checkpoint_due(
        &self,
        followers: &[NodeId],
        now: Timestamp,
        threshold: u64,
        lag_limit: Duration,
    ) -> Option<Vec<NodeId>> {
        let mut state = self.lock();
        if state.checkpoints % 2 == 1 || state.log_bytes < threshold {
            return None;
        }
        let limit = u64::try_from(lag_limit.as_millis()).unwrap_or(u64::MAX);
        // A sink that is behind holds the checkpoint back, until it has been for `lag_limit`.
        let committed = state.committed.clone();
        for progress in state.sinks.values_mut() {
            if progress.position == committed {
                continue;
            }
            let since = *progress.behind_since.get_or_insert(now);
            if now.as_millis().saturating_sub(since.as_millis()) < limit {
                return None;
            }
        }
        let mut excluded = Vec::new();
        for node in followers {
            let resumable = state.followers.get(node).is_some_and(|progress| {
                progress.released || progress.reported && state.resumable(progress.acked.as_ref())
            });
            let progress = state.followers.entry(node.clone()).or_default();
            if resumable {
                continue;
            }
            let since = *progress.behind_since.get_or_insert(now);
            if now.as_millis().saturating_sub(since.as_millis()) < limit {
                return None;
            }
            excluded.push(node.clone());
        }
        state.checkpoints += 1;
        Some(excluded)
    }

    /// Starts a checkpoint now, if every follower in `followers` holds everything.
    pub(super) fn begin_checkpoint(&self, followers: &[NodeId]) -> Result<(), ReplicationError> {
        let mut state = self.lock();
        if state.checkpoints % 2 == 1 {
            return Err(failed("a checkpoint is already running".to_owned()));
        }
        let committed = state.committed.clone();
        for node in followers {
            let caught_up = state
                .followers
                .get(node)
                .is_some_and(|progress| progress.reported && progress.acked == committed);
            if !caught_up {
                return Err(failed(format!("follower {node} is behind")));
            }
        }
        state.checkpoints += 1;
        Ok(())
    }

    fn end_checkpoint(&self, result: Result<&CheckpointMarker, ()>, excluded: &[NodeId]) {
        {
            let mut state = self.lock();
            state.checkpoints += 1;
            if let Ok(marker) = result {
                state.log_bytes = 0;
                for node in excluded {
                    if let Some(progress) = state.followers.get_mut(node)
                        && progress.acked.as_ref() < Some(&marker.position)
                    {
                        progress.released = true;
                    }
                }
                state.marker = Some(marker.clone());
            }
        }
        self.notify();
    }
}

impl State {
    /// Whether a follower that holds the log up to `acked` can get everything after it once the
    /// log restarts: it holds everything, or every later batch is retained.
    fn resumable(&self, acked: Option<&Position>) -> bool {
        if acked == self.committed.as_ref() {
            return true;
        }
        let retained_to_end = self.recent.back().map(|batch| &batch.end) == self.committed.as_ref();
        retained_to_end && self.following(acked).is_some()
    }

    /// The index in `recent` of the batch that continues from `position`, or `recent.len()` if
    /// `position` is the last retained batch's end, or `None` if `position` isn't on the retained
    /// chain.
    fn following(&self, position: Option<&Position>) -> Option<usize> {
        if self
            .base
            .as_ref()
            .is_some_and(|base| base.as_ref() == position)
        {
            return Some(0);
        }
        let position = position?;
        self.recent
            .iter()
            .position(|batch| batch.end == *position)
            .map(|index| index + 1)
    }

    fn append(&mut self, batch: FrameBatch, retained_bytes: usize) {
        self.log_bytes = self
            .log_bytes
            .saturating_add(u64::try_from(batch.payload.len()).unwrap_or(u64::MAX));
        self.recent_bytes += batch.payload.len();
        if self.recent.is_empty() {
            // Batches are published in commit order, each continuing the one before.
            self.base = Some(self.committed.clone());
        }
        if Some(&batch.end) > self.committed.as_ref() {
            self.committed = Some(batch.end.clone());
        }
        self.published = self.published.saturating_add(1);
        self.recent.push_back(batch);
        self.prune(retained_bytes);
    }

    /// Drops published batches every follower has, and the oldest beyond `retained_bytes`.
    fn prune(&mut self, retained_bytes: usize) {
        let everyone = self
            .followers
            .values()
            .filter(|progress| !progress.released)
            .map(|progress| {
                progress
                    .reported
                    .then_some(progress.acked.as_ref())
                    .flatten()
            })
            .min()
            .unwrap_or(self.committed.as_ref());
        while let Some(front) = self.recent.front() {
            let held = everyone.is_some_and(|everyone| front.end <= *everyone);
            if !held && self.recent_bytes <= retained_bytes {
                break;
            }
            self.recent_bytes -= front.payload.len();
            self.base = Some(Some(front.end.clone()));
            self.recent.pop_front();
        }
    }
}

/// Takes a batch the database's actor just committed: keeps it for the streams, and returns the
/// acknowledgement its writer waits for.
pub(super) fn publish(
    inner: &Arc<Inner>,
    primary: &Arc<Primary>,
    batch: FrameBatch,
) -> Acknowledgement {
    let end = batch.end.clone();
    primary.lock().append(batch, inner.settings.retained_bytes);
    primary.notify();
    let quorum = inner.followers_of(primary);
    maybe_checkpoint(inner, primary, &quorum);
    let reachable = primary.reachable(inner, &quorum.followers);
    let counts = Counts {
        reachable: count(reachable + quorum.own),
        replicas: u32::from(primary.role().replication_factor.get()),
    };
    if quorum.needed == 0 {
        // The primary alone is the write quorum: R = 1, or `local` acknowledgements.
        let acknowledged_by = count(1 + primary.acknowledged_by(&quorum.followers, &end));
        return Acknowledgement::new(std::future::ready(Ok(counts.acknowledged(acknowledged_by))));
    }
    if reachable < quorum.needed {
        return refused(ReplicationError::UnderReplicated {
            required: quorum.required,
            reachable: reachable + quorum.own,
        });
    }
    Acknowledgement::new(wait_for_quorum(
        Arc::clone(primary),
        end,
        quorum,
        counts,
        inner.settings.ack_timeout,
    ))
}

/// The replica counts a write's [`Acknowledged`] reports besides how many hold it.
#[derive(Debug, Clone, Copy)]
struct Counts {
    reachable: u32,
    replicas: u32,
}

impl Counts {
    fn acknowledged(self, acknowledged_by: u32) -> Acknowledged {
        Acknowledged::new(acknowledged_by, self.reachable, self.replicas)
    }
}

fn count(replicas: usize) -> u32 {
    u32::try_from(replicas).unwrap_or(u32::MAX)
}

/// Completes once `quorum.needed` followers have acknowledged `end`, with how many replicas,
/// the primary included, hold it by then.
async fn wait_for_quorum(
    primary: Arc<Primary>,
    end: Position,
    quorum: Quorum,
    counts: Counts,
    limit: Duration,
) -> Result<Acknowledged, ReplicationError> {
    let mut changes = primary.changed.subscribe();
    let waited = time::timeout(limit, async {
        while primary.acknowledged_by(&quorum.followers, &end) < quorum.needed {
            if changes.changed().await.is_err() {
                return;
            }
        }
    })
    .await;
    let holders = primary.acknowledged_by(&quorum.followers, &end);
    if waited.is_ok() && holders >= quorum.needed {
        Ok(counts.acknowledged(count(holders + quorum.own)))
    } else {
        Err(failed(format!(
            "{} of the {} followers a write needs did not acknowledge it within {limit:?}",
            quorum.needed - holders.min(quorum.needed),
            quorum.needed,
        )))
    }
}

/// Starts a checkpoint in the background if one is due.
fn maybe_checkpoint(inner: &Arc<Inner>, primary: &Arc<Primary>, quorum: &Quorum) {
    let settings = &inner.settings;
    if let Some(excluded) = primary.checkpoint_due(
        &quorum.followers,
        inner.clock.now(),
        settings.checkpoint_threshold,
        settings.lag_limit,
    ) {
        let inner = Arc::clone(inner);
        let primary = Arc::clone(primary);
        tokio::spawn(async move {
            if let Err(error) = checkpoint(&inner, &primary, excluded).await {
                tracing::warn!(database = %primary.name, %error, "checkpoint failed; will retry");
            }
        });
    }
}

/// Runs a checkpoint that [`Primary::checkpoint_due`] or [`Primary::begin_checkpoint`] started,
/// and records its marker for the streams to ship.
pub(super) async fn checkpoint(
    inner: &Inner,
    primary: &Primary,
    excluded: Vec<NodeId>,
) -> Result<CheckpointMarker, ReplicationError> {
    let result = match inner.registry().open(&primary.name).await {
        Ok(handle) => handle.checkpoint().await,
        Err(error) => Err(error),
    };
    match result {
        Ok(marker) => {
            if !excluded.is_empty() {
                tracing::warn!(
                    database = %primary.name,
                    followers = ?excluded,
                    "checkpointed past lagging followers; they need a snapshot"
                );
            }
            primary.end_checkpoint(Ok(&marker), &excluded);
            Ok(marker)
        }
        Err(error) => {
            primary.end_checkpoint(Err(()), &excluded);
            Err(failed(format!("checkpoint failed: {error}")))
        }
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

/// Keeps one stream open to each follower in the database's replica set, as membership changes,
/// and starts checkpoints that come due while nothing is published.
pub(super) async fn drive(inner: Arc<Inner>, primary: Arc<Primary>) {
    match inner.registry().open(&primary.name).await {
        Ok(handle) => match handle.position().await {
            Ok(position) => primary.raise_committed(position),
            Err(error) => {
                tracing::warn!(database = %primary.name, %error, "can't read the log's end")
            }
        },
        Err(error) => tracing::warn!(database = %primary.name, %error, "can't open the database"),
    }
    let mut membership = inner.membership.clone();
    let mut streams: BTreeMap<NodeId, AbortOnDrop> = BTreeMap::new();
    loop {
        let quorum = inner.followers_of(&primary);
        primary.retain_followers(&quorum.followers, inner.clock.now());
        streams.retain(|node, _| quorum.followers.contains(node));
        for node in &quorum.followers {
            if !streams.contains_key(node) {
                let task = tokio::spawn(follow(
                    Arc::clone(&inner),
                    Arc::clone(&primary),
                    node.clone(),
                ));
                streams.insert(node.clone(), AbortOnDrop(task));
            }
        }
        maybe_checkpoint(&inner, &primary, &quorum);
        tokio::select! {
            changed = membership.changed() => if changed.is_none() {
                time::sleep(inner.settings.retry_interval).await;
            },
            () = time::sleep(inner.settings.retry_interval * 5) => {}
        }
    }
}

/// Why a stream ended.
#[derive(Debug)]
enum StreamEnd {
    /// The follower re-seeded its copy from a snapshot; the next stream continues from there.
    Reseeded,
    /// The stream failed; it is reopened after a pause.
    Failed(String),
}

impl fmt::Display for StreamEnd {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Reseeded => f.write_str("the follower re-seeded from a snapshot"),
            Self::Failed(reason) => f.write_str(reason),
        }
    }
}

fn stream_failed(reason: impl fmt::Display) -> StreamEnd {
    StreamEnd::Failed(reason.to_string())
}

/// Streams to `node` for as long as the task runs, reopening the stream whenever it ends.
async fn follow(inner: Arc<Inner>, primary: Arc<Primary>, node: NodeId) {
    loop {
        let ended = stream(&inner, &primary, &node).await;
        primary.disconnected(&node);
        let pause = match ended {
            StreamEnd::Reseeded => {
                tracing::debug!(database = %primary.name, follower = %node, "follower re-seeded; reopening");
                inner.settings.retry_interval
            }
            StreamEnd::Failed(reason) => {
                tracing::debug!(database = %primary.name, follower = %node, %reason, "replication stream ended; reopening");
                inner.settings.retry_interval
            }
        };
        time::sleep(pause).await;
    }
}

/// Opens one stream to `node` and feeds it until it fails.
async fn stream(inner: &Inner, primary: &Primary, node: &NodeId) -> StreamEnd {
    let settings = &inner.settings;
    let Some(address) = inner.addresses.address(node) else {
        return stream_failed(format!("no address for {node}"));
    };
    let channel = match inner.peers.channel_to(&address) {
        Ok(channel) => channel,
        Err(error) => return stream_failed(error),
    };
    let (outgoing, messages) = mpsc::channel(settings.max_in_flight.get() + 2);
    let open = Message::Open(ReplicateOpen {
        database: primary.name.to_string(),
        epoch: primary.role.epoch.get(),
        epoch_start: primary.start.clone().map(Position::into_bytes),
    });
    if outgoing.send(message(open)).await.is_err() {
        return stream_failed("stream closed");
    }
    let mut request = tonic::Request::new(ReceiverStream::new(messages));
    if let Ok(value) = MetadataValue::try_from(inner.node.as_str()) {
        request.metadata_mut().insert(PRIMARY_METADATA_KEY, value);
    }
    let mut client = replication_client(channel);
    let opened = time::timeout(settings.open_timeout, async {
        let mut acks = client.replicate(request).await?.into_inner();
        let first = acks.message().await?;
        Ok::<_, tonic::Status>((acks, first))
    })
    .await;
    let (mut acks, first) = match opened {
        Ok(Ok((acks, Some(first)))) => (acks, first),
        Ok(Ok((_, None))) => return stream_failed("the follower closed the stream"),
        Ok(Err(status)) => return stream_failed(status),
        Err(_) => {
            inner.peers.forget(&address);
            return stream_failed("opening the stream timed out");
        }
    };
    if first.needs_snapshot {
        // The copy diverges from this epoch's log where it can't be cut back. Its position isn't
        // on this log, so it is never counted as acknowledging anything.
        return reseed(inner, primary, node, &outgoing, &mut acks, 0).await;
    }
    let mut sent = held(first.position);
    primary.acknowledged(node, sent.clone(), settings.retained_bytes);
    tracing::debug!(database = %primary.name, follower = %node, position = ?sent, "replication stream open");

    let mut in_flight: VecDeque<Position> = VecDeque::new();
    let mut marker_sent: Option<Position> = None;
    let mut changes = primary.changed.subscribe();
    let mut deadline = Instant::now() + settings.stall_timeout;
    loop {
        while in_flight.len() < settings.max_in_flight.get() {
            let batch = match primary.plan(sent.as_ref()) {
                Plan::Wait => break,
                Plan::NeedsSnapshot => {
                    return reseed(inner, primary, node, &outgoing, &mut acks, in_flight.len())
                        .await;
                }
                Plan::Send(batch) => batch,
                Plan::Read { after, checkpoints } => {
                    match read_since(inner, primary, after, checkpoints).await {
                        Ok(Some(batch)) => batch,
                        Ok(None) => break,
                        Err(error) if log_moved_past(primary, sent.as_ref(), &error) => {
                            return reseed(
                                inner,
                                primary,
                                node,
                                &outgoing,
                                &mut acks,
                                in_flight.len(),
                            )
                            .await;
                        }
                        Err(error) => return stream_failed(error),
                    }
                }
            };
            if in_flight.is_empty() {
                deadline = Instant::now() + settings.stall_timeout;
            }
            let end = batch.end.clone();
            let shipped = message(Message::Batch(proto::Batch {
                epoch: primary.role.epoch.get(),
                start: sent.clone().map(Position::into_bytes).unwrap_or_default(),
                end: batch.end.into_bytes(),
                payload: batch.payload.into_bytes(),
            }));
            if outgoing.send(shipped).await.is_err() {
                return stream_failed("stream closed");
            }
            sent = Some(end.clone());
            in_flight.push_back(end);
        }
        if let Some(marker) = primary.marker()
            && sent.as_ref() == Some(&marker.position)
            && marker_sent.as_ref() != Some(&marker.position)
        {
            let shipped = message(Message::Checkpoint(proto::CheckpointMarker {
                epoch: primary.role.epoch.get(),
                position: marker.position.clone().into_bytes(),
            }));
            if outgoing.send(shipped).await.is_err() {
                return stream_failed("stream closed");
            }
            marker_sent = Some(marker.position);
        }
        tokio::select! {
            ack = acks.message() => match ack {
                Ok(Some(ack)) if ack.needs_snapshot => {
                    // The follower acknowledges nothing it was sent after this.
                    return reseed(inner, primary, node, &outgoing, &mut acks, 0).await;
                }
                Ok(Some(ack)) => {
                    let position = held(ack.position);
                    while in_flight.front().is_some_and(|end| Some(end) <= position.as_ref()) {
                        in_flight.pop_front();
                    }
                    deadline = Instant::now() + settings.stall_timeout;
                    primary.acknowledged(node, position, settings.retained_bytes);
                    let quorum = inner.followers_of(primary);
                    if let Some(excluded) = primary.checkpoint_due(
                        &quorum.followers,
                        inner.clock.now(),
                        settings.checkpoint_threshold,
                        settings.lag_limit,
                    ) {
                        // Checkpoint inline: the stream has nothing to send until it ends.
                        if let Err(error) = checkpoint(inner, primary, excluded).await {
                            tracing::warn!(database = %primary.name, %error, "checkpoint failed; will retry");
                        }
                    }
                }
                Ok(None) => return stream_failed("the follower closed the stream"),
                Err(status) => return stream_failed(status),
            },
            changed = changes.changed() => if changed.is_err() {
                return stream_failed("the primary stopped");
            },
            () = time::sleep_until(deadline), if !in_flight.is_empty() => {
                inner.peers.forget(&address);
                return stream_failed("the follower stopped acknowledging");
            }
        }
    }
}

/// Reads every commit after `after` back from the log, unless a checkpoint started or ended since
/// the plan was made, in which case the result may skip frames and is dropped.
async fn read_since(
    inner: &Inner,
    primary: &Primary,
    after: Option<Position>,
    checkpoints: u64,
) -> Result<Option<FrameBatch>, DatabaseError> {
    let handle = inner.registry().open(&primary.name).await?;
    let batch = handle.committed_since(after).await?;
    if primary.checkpoints() != checkpoints {
        return Ok(None);
    }
    Ok(batch)
}

/// Whether reading the log for a follower at `sent` failed because the log no longer continues
/// from there, as after a checkpoint the primary restarted since, so that only a snapshot can
/// catch it up. A follower at or past the primary's end isn't behind, and is left to retry.
fn log_moved_past(primary: &Primary, sent: Option<&Position>, error: &DatabaseError) -> bool {
    matches!(
        error.engine_error(),
        Some(EngineError::InvalidPosition { .. })
    ) && sent < primary.committed().as_ref()
}

/// Has `node` replace its copy with a snapshot, because the log no longer holds what it needs or
/// its copy doesn't follow the log, and waits until it acknowledges the snapshot's position. Acknowledgements of the `in_flight`
/// batches sent before arrive first.
async fn reseed(
    inner: &Inner,
    primary: &Primary,
    node: &NodeId,
    outgoing: &mpsc::Sender<ReplicateMessage>,
    acks: &mut tonic::Streaming<ReplicateAck>,
    in_flight: usize,
) -> StreamEnd {
    primary.needs_snapshot(node);
    tracing::info!(database = %primary.name, follower = %node, "the log can't catch the follower up; re-seeding it from a snapshot");
    let request = message(Message::Reseed(proto::Reseed {
        epoch: primary.role.epoch.get(),
    }));
    if outgoing.send(request).await.is_err() {
        return stream_failed("stream closed");
    }
    let reseeded = time::timeout(inner.settings.open_timeout, async {
        let mut skip = in_flight;
        loop {
            match acks.message().await {
                Ok(Some(ack)) if skip > 0 => {
                    skip -= 1;
                    primary.acknowledged(node, held(ack.position), inner.settings.retained_bytes);
                }
                Ok(Some(ack)) => return Ok(held(ack.position)),
                Ok(None) => return Err(stream_failed("the follower closed the stream")),
                Err(status) => return Err(stream_failed(status)),
            }
        }
    })
    .await;
    match reseeded {
        Ok(Ok(position)) => {
            primary.acknowledged(node, position, inner.settings.retained_bytes);
            StreamEnd::Reseeded
        }
        Ok(Err(ended)) => ended,
        Err(_) => stream_failed("the follower didn't re-seed in time"),
    }
}

fn message(message: Message) -> ReplicateMessage {
    ReplicateMessage {
        message: Some(message),
    }
}

/// A position as the protocol carries it: empty bytes for an empty log.
pub(super) fn held(bytes: Vec<u8>) -> Option<Position> {
    (!bytes.is_empty()).then(|| Position::from(bytes))
}
