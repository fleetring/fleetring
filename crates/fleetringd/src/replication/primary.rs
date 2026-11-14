//! The primary side: one stream per follower, the write quorum, and checkpoints.

use std::collections::{BTreeMap, VecDeque};
use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
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

use super::chain::{Anchor, Chain, ChainHash, Links, LogRead};
use super::follower::blocking;
use super::handover::{Origin, RingFollowed, Staleness};
use super::{
    FaultPoint, FollowerProgress, Inner, PRIMARY_METADATA_KEY, PrimaryRole, PrimaryStatus, Quorum,
    Verified, chain, failed, placed_on, refused,
};
use crate::engine::{
    CheckpointMarker, EngineCapability, EngineError, FrameBatch, FrameSourceEngine, Position,
};
use crate::publisher::{Acknowledged, Acknowledgement, ReplicationError};
use crate::transport::replication_client;
use crate::{DatabaseError, DatabaseHandle};

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
    /// Set when this node stops serving the database as primary, so that writes still waiting
    /// for their quorum fail at once instead of at the acknowledgement timeout.
    retired: AtomicBool,
}

#[derive(Debug, Default)]
struct State {
    /// The end of the last committed batch.
    committed: Option<Position>,
    /// Published batches that some follower may still need, oldest first, each continuing the
    /// one before.
    recent: VecDeque<Shipped>,
    /// The hash chain over the log, once [`drive`] has rebuilt it from the record and the log.
    chain: ChainState,
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
    /// Payload bytes published since this primary started serving, which a fork's pin measures
    /// the WAL's growth by.
    published_bytes: u64,
    /// Checkpoints wait until then, while a fork's seeder waits for the acknowledged position to
    /// enter the log (ADR-0046, point 4.2).
    checkpoint_hold: Option<Timestamp>,
}

/// A published batch, with the chain hash at its end if the chain covered it.
#[derive(Debug, Clone)]
struct Shipped {
    batch: FrameBatch,
    chain: Option<ChainHash>,
}

/// Where the primary's chain stands.
#[derive(Debug, Default)]
enum ChainState {
    /// A build from the record and the log is under way; batches ship without a hash.
    #[default]
    Loading,
    Ready(Chain),
    /// A batch was published that the chain couldn't continue from, or the last build failed:
    /// the next stream batch, checkpoint, pass of [`drive`], snapshot, or seeding request
    /// builds it again.
    Lost,
}

/// How one build of the chain from the record and the log ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Rebuild {
    /// The chain is built.
    Built,
    /// The reads are stale, so the build reads again: a batch was published that the chain
    /// they built doesn't reach, or a checkpoint started meanwhile.
    Again,
    /// The build failed, and the chain is lost, so that it is built again when next asked.
    Failed,
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
    Send(Shipped),
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
            retired: AtomicBool::new(false),
        }
    }

    /// Marks this primary as no longer serving, and wakes every write waiting for its quorum.
    ///
    /// Those writes committed here but no follower will acknowledge them now: the streams are
    /// going away, and the next primary may or may not hold them.
    pub(super) fn retire(&self) {
        self.retired.store(true, Ordering::SeqCst);
        self.notify();
    }

    fn is_retired(&self) -> bool {
        self.retired.load(Ordering::SeqCst)
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

    /// How many payload bytes have been published since this primary started serving.
    pub(super) fn published_bytes(&self) -> u64 {
        self.lock().published_bytes
    }

    /// Holds checkpoints until `until`, as a fork's seeding request does while the position its
    /// source's write quorum acknowledged is behind the log's start (ADR-0046, point 4.2), unless
    /// a hold is in force at `now` already: a seeder asking again doesn't extend it, so one hold
    /// lasts at most as long as the first request asked.
    pub(super) fn hold_checkpoints(&self, now: Timestamp, until: Timestamp) {
        let mut state = self.lock();
        if state.checkpoint_hold.is_none_or(|held| held <= now) {
            state.checkpoint_hold = Some(until);
        }
    }

    /// Lets checkpoints run again, once the seeding request that held them has its pin.
    pub(super) fn release_checkpoints(&self) {
        self.lock().checkpoint_hold = None;
    }

    /// The chain hash at `position`, if the chain is built and holds it.
    pub(super) fn hash_at(&self, position: Option<&Position>) -> Option<ChainHash> {
        match &self.lock().chain {
            ChainState::Ready(chain) => chain.hash_at(position),
            _ => None,
        }
    }

    /// Every position the chain holds a hash at, if it is built.
    pub(super) fn chain_history(&self) -> Option<BTreeMap<Position, ChainHash>> {
        match &self.lock().chain {
            ChainState::Ready(chain) => Some(chain.history()),
            _ => None,
        }
    }

    /// The chain's anchor, if it is built.
    fn chain_anchor(&self) -> Option<Anchor> {
        match &self.lock().chain {
            ChainState::Ready(chain) => Some(chain.anchor().clone()),
            _ => None,
        }
    }

    /// Takes what a build read from the record and the log, which it began reading when
    /// `checkpoints` checkpoints had started or ended and none ran.
    ///
    /// A checkpoint that started since may have restarted the log between the build's reads,
    /// which then don't describe one log: neither the chain they built nor their failure counts,
    /// and the build reads again. A build that failed otherwise leaves the chain lost, never
    /// loading, so that it is built again when next asked, with no write needed to set it off
    /// (#156).
    fn chain_built(&self, built: Result<Chain, String>, checkpoints: u64) -> Rebuild {
        let mut state = self.lock();
        if state.checkpoints != checkpoints {
            tracing::debug!(database = %self.name, "a checkpoint started while the chain was being built; reading the log again");
            return Rebuild::Again;
        }
        match built {
            Ok(chain)
                if chain.tip().0 == state.committed.as_ref()
                    || state.chain_can_reach(&chain, state.committed.as_ref()) =>
            {
                state.chain = ChainState::Ready(chain);
                Rebuild::Built
            }
            Ok(_) => Rebuild::Again,
            Err(error) => {
                tracing::warn!(database = %self.name, %error, "can't build the log's hash chain; building it again when next asked");
                state.chain = ChainState::Lost;
                Rebuild::Failed
            }
        }
    }

    /// Whether a build of the chain is under way, which ends with the chain built or lost.
    pub(super) fn chain_loading(&self) -> bool {
        matches!(self.lock().chain, ChainState::Loading)
    }

    /// Chains `batch`, which continues the log from `from`, and returns the hash at its end, or
    /// `None` if the chain isn't built or can't reach `from` even through the retained batches,
    /// in which case it is rebuilt.
    ///
    /// A stream calls this as it ships the batch, so the hashing lands on the stream's task
    /// rather than the database's thread, and only the chain's bookkeeping runs under the lock.
    /// The hash of a retained batch is kept with it, so the other streams reuse it.
    fn link(
        &self,
        engine: &dyn FrameSourceEngine,
        from: Option<&Position>,
        batch: &FrameBatch,
    ) -> Option<ChainHash> {
        let from_hash = {
            let mut state = self.lock();
            if let Some(shipped) = state
                .recent
                .iter()
                .find(|shipped| shipped.batch.end == batch.end)
                && let Some(hash) = shipped.chain
            {
                return Some(hash);
            }
            let ChainState::Ready(chain) = &state.chain else {
                return None;
            };
            if chain.hash_at(from).is_none() {
                let reached = from.is_some_and(|from| state.fill_chain(engine, from));
                if !reached {
                    tracing::warn!(database = %self.name, from = ?from, "the chain doesn't reach a published batch; rebuilding it");
                    state.chain = ChainState::Lost;
                    return None;
                }
            }
            let ChainState::Ready(chain) = &state.chain else {
                return None;
            };
            chain.hash_at(from)?
        };
        let links = match Links::compute(engine, from_hash, from, batch) {
            Ok(links) => links,
            Err(error) => {
                tracing::warn!(database = %self.name, %error, "can't chain a published batch; rebuilding the chain");
                self.lock().chain = ChainState::Lost;
                return None;
            }
        };
        let hash = links.end();
        let mut state = self.lock();
        let ChainState::Ready(chain) = &mut state.chain else {
            return None;
        };
        if let Err(error) = chain.absorb(links) {
            tracing::warn!(database = %self.name, %error, "the chain and a published batch disagree; rebuilding the chain");
            state.chain = ChainState::Lost;
            return None;
        }
        if let Some(shipped) = state
            .recent
            .iter_mut()
            .find(|shipped| shipped.batch.end == batch.end)
        {
            shipped.chain = Some(hash);
        }
        Some(hash)
    }

    /// Chains the retained batches after the chain's tip up to `target`. Returns whether the
    /// chain holds `target` afterwards.
    fn link_retained(&self, engine: &dyn FrameSourceEngine, target: Option<&Position>) -> bool {
        let mut state = self.lock();
        let ChainState::Ready(chain) = &state.chain else {
            return false;
        };
        if chain.hash_at(target).is_some() {
            return true;
        }
        target.is_some_and(|target| state.fill_chain(engine, target))
    }

    /// The position the chain ends at, if it is built.
    fn chain_tip(&self) -> Option<Option<Position>> {
        match &self.lock().chain {
            ChainState::Ready(chain) => Some(chain.tip().0.cloned()),
            _ => None,
        }
    }

    /// Marks the chain lost, so that it is rebuilt from its record and the log. Returns whether
    /// it was built: a chain that is loading or lost already is left as it is.
    fn lose_chain(&self) -> bool {
        let mut state = self.lock();
        let built = matches!(state.chain, ChainState::Ready(_));
        if built {
            state.chain = ChainState::Lost;
        }
        built
    }

    /// Whether the chain was lost and needs rebuilding; sets it loading if so.
    fn take_lost(&self) -> bool {
        let mut state = self.lock();
        if matches!(state.chain, ChainState::Lost) {
            state.chain = ChainState::Loading;
            true
        } else {
            false
        }
    }

    /// Moves the chain's anchor to `position`, where a checkpoint just ended, and returns the
    /// anchor to record.
    fn rebase_chain(&self, position: &Position) -> Option<Anchor> {
        match &mut self.lock().chain {
            ChainState::Ready(chain) => chain.rebase(position),
            _ => None,
        }
    }

    /// The highest position the write quorum holds: every batch ending at or before it is
    /// acknowledged, or would be if its writer were still waiting (ADR-0018). `None` while no
    /// batch is.
    ///
    /// For each write quorum with `needed` followers required, that is the `needed`-th highest
    /// position its followers have reported, capped at the log's end, and the primary alone
    /// decides one whose `needed` is 0; with two write quorums, the lower of the two.
    pub(super) fn quorum_held(&self, quorum: &Quorum) -> Option<Position> {
        let state = self.lock();
        let committed = state.committed.as_ref()?;
        let mut held: Option<Position> = None;
        for group in &quorum.groups {
            let group_held = if group.needed == 0 {
                committed.clone()
            } else {
                let mut acked: Vec<&Position> = group
                    .followers
                    .iter()
                    .filter_map(|node| state.followers.get(node))
                    .filter(|progress| progress.reported)
                    .filter_map(|progress| progress.acked.as_ref())
                    .collect();
                acked.sort_unstable_by(|a, b| b.cmp(a));
                let at = acked.get(group.needed - 1)?;
                (*at).min(committed).clone()
            };
            held = Some(match held {
                Some(so_far) => so_far.min(group_held),
                None => group_held,
            });
        }
        held
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
    pub(super) fn acknowledged_by(&self, followers: &[NodeId], end: &Position) -> usize {
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
        if state.checkpoint_hold.is_some_and(|until| now < until) {
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
    /// Extends the chain through the retained batches that continue from its tip until it
    /// reaches `target`. Returns whether it did. A tip the retained batches don't continue from
    /// reaches nothing through them: hashing a batch onto a position it doesn't follow would
    /// give a wrong chain.
    fn fill_chain(&mut self, engine: &dyn FrameSourceEngine, target: &Position) -> bool {
        let ChainState::Ready(chain) = &self.chain else {
            return false;
        };
        let tip = chain.tip().0.cloned();
        let Some(index) = self.following(tip.as_ref()) else {
            return false;
        };
        let Self { chain, recent, .. } = self;
        let ChainState::Ready(chain) = chain else {
            return false;
        };
        let retained: Vec<&FrameBatch> = recent
            .iter()
            .skip(index)
            .map(|shipped| &shipped.batch)
            .collect();
        fill_gap(chain, engine, &retained, target)
    }

    /// Whether `chain` reaches `position` through the retained batches, which hold what was
    /// published while the chain was being rebuilt.
    fn chain_can_reach(&self, chain: &Chain, position: Option<&Position>) -> bool {
        let Some(position) = position else {
            return chain.tip().0.is_none();
        };
        let (tip, _) = chain.tip();
        if tip == Some(position) {
            return true;
        }
        let Some(index) = self.following(tip) else {
            return false;
        };
        self.recent
            .iter()
            .skip(index)
            .any(|shipped| shipped.batch.end == *position)
    }

    /// Whether a follower that holds the log up to `acked` can get everything after it once the
    /// log restarts: it holds everything, or every later batch is retained.
    fn resumable(&self, acked: Option<&Position>) -> bool {
        if acked == self.committed.as_ref() {
            return true;
        }
        let retained_to_end =
            self.recent.back().map(|shipped| &shipped.batch.end) == self.committed.as_ref();
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
            .position(|shipped| shipped.batch.end == *position)
            .map(|index| index + 1)
    }

    fn append(&mut self, shipped: Shipped, retained_bytes: usize) {
        let batch = &shipped.batch;
        let bytes = u64::try_from(batch.payload.len()).unwrap_or(u64::MAX);
        self.log_bytes = self.log_bytes.saturating_add(bytes);
        self.published_bytes = self.published_bytes.saturating_add(bytes);
        self.recent_bytes += batch.payload.len();
        if self.recent.is_empty() {
            // Batches are published in commit order, each continuing the one before.
            self.base = Some(self.committed.clone());
        }
        if Some(&batch.end) > self.committed.as_ref() {
            self.committed = Some(batch.end.clone());
        }
        self.published = self.published.saturating_add(1);
        self.recent.push_back(shipped);
        self.prune(retained_bytes);
    }

    /// Drops published batches every follower has, and the oldest beyond `retained_bytes`.
    ///
    /// While a checkpoint runs, a batch the chain hasn't reached is kept even when every
    /// follower has it, as it is when the primary has none, and whatever `retained_bytes` says:
    /// once the checkpoint restarts the log, the retained batches are all the chain can reach
    /// the checkpoint's position through (#156). What one checkpoint's run lets land bounds
    /// them.
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
        let chained = match &self.chain {
            ChainState::Ready(chain) if self.checkpoints % 2 == 1 => Some(chain.tip().0.cloned()),
            _ => None,
        };
        while let Some(front) = self.recent.front() {
            let unchained = chained
                .as_ref()
                .is_some_and(|tip| Some(&front.batch.end) > tip.as_ref());
            // Batches are retained in order, so every one after an unchained batch is too.
            if unchained {
                break;
            }
            let held = everyone.is_some_and(|everyone| front.batch.end <= *everyone);
            if !held && self.recent_bytes <= retained_bytes {
                break;
            }
            self.recent_bytes -= front.batch.payload.len();
            self.base = Some(Some(front.batch.end.clone()));
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
    // The batch is hashed onto the chain by the first stream that ships it, off this thread.
    primary.lock().append(
        Shipped { batch, chain: None },
        inner.settings.retained_bytes,
    );
    primary.notify();
    let quorum = inner.followers_of(primary);
    maybe_checkpoint(inner, primary, &quorum);
    let reachable = primary.reachable(inner, &quorum.followers);
    let counts = Counts {
        reachable: count(reachable + quorum.own),
        replicas: u32::from(primary.role().replication_factor.get()),
    };
    if quorum.alone() {
        // The primary alone is the write quorum: R = 1, or `local` acknowledgements.
        let acknowledged_by = count(1 + primary.acknowledged_by(&quorum.followers, &end));
        return Acknowledgement::new(std::future::ready(Ok(counts.acknowledged(acknowledged_by))));
    }
    if let Some((short, reachable)) =
        quorum.short_of(|followers| primary.reachable(inner, followers))
    {
        return refused(ReplicationError::UnderReplicated {
            required: short.required,
            reachable: reachable + short.own,
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

/// Completes once every write quorum's `needed` followers have acknowledged `end`, with how
/// many replicas, the primary included, hold it by then.
async fn wait_for_quorum(
    primary: Arc<Primary>,
    end: Position,
    quorum: Quorum,
    counts: Counts,
    limit: Duration,
) -> Result<Acknowledged, ReplicationError> {
    let mut changes = primary.changed.subscribe();
    let waited = time::timeout(limit, async {
        while quorum
            .short_of(|followers| primary.acknowledged_by(followers, &end))
            .is_some()
            && !primary.is_retired()
        {
            if changes.changed().await.is_err() {
                return;
            }
        }
    })
    .await;
    let holders = primary.acknowledged_by(&quorum.followers, &end);
    let short = quorum
        .short_of(|followers| primary.acknowledged_by(followers, &end))
        .map(|(group, acknowledged)| (group.needed, acknowledged));
    match short {
        None if waited.is_ok() => Ok(counts.acknowledged(count(holders + quorum.own))),
        Some(_) if primary.is_retired() => Err(failed(
            "this node stopped being the database's primary before a quorum acknowledged the write"
                .to_owned(),
        )),
        Some((needed, acknowledged)) => Err(failed(format!(
            "{} of the {needed} followers a write needs did not acknowledge it within {limit:?}",
            needed - acknowledged.min(needed),
        ))),
        None => Err(failed(format!(
            "the followers a write needs did not acknowledge it within {limit:?}"
        ))),
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

/// The engine as a frame source, which every served database's engine is.
fn frame_source(inner: &Inner) -> Option<&dyn FrameSourceEngine> {
    match inner.engine.capability() {
        EngineCapability::FrameSource(engine) => Some(engine),
        EngineCapability::SelfReplicating => None,
    }
}

/// Runs a checkpoint that [`Primary::checkpoint_due`] or [`Primary::begin_checkpoint`] started,
/// and records its marker for the streams to ship, and the chain's anchor there.
pub(super) async fn checkpoint(
    inner: &Arc<Inner>,
    primary: &Arc<Primary>,
    excluded: Vec<NodeId>,
) -> Result<CheckpointMarker, ReplicationError> {
    let result = match inner.registry().open(&primary.name).await {
        Ok(handle) => {
            // The anchor moves to the marker only if the chain reaches it, so bring the chain
            // up to the log's end first, as a database with no follower to ship to needs.
            let committed = primary.committed();
            if !chain_to(inner, primary, &handle, committed.as_ref()).await {
                tracing::debug!(database = %primary.name, "the chain doesn't reach the log's end; the checkpoint records no anchor");
            }
            inner.forks.faults().pass_any(FaultPoint::Checkpoint).await;
            handle.checkpoint().await
        }
        Err(error) => Err(error),
    };
    match result {
        Ok(marker) => {
            // Writes that landed between bringing the chain up and the checkpoint are no longer
            // in the log, which restarted: the chain reaches the marker through the batches
            // retained meanwhile, or never (#156).
            if let Some(engine) = frame_source(inner)
                && !primary.link_retained(engine, Some(&marker.position))
                && primary.lose_chain()
            {
                tracing::warn!(database = %primary.name, position = %marker.position, "the chain doesn't reach the checkpoint; rebuilding it from the restarted log");
            }
            if !excluded.is_empty() {
                tracing::warn!(
                    database = %primary.name,
                    followers = ?excluded,
                    "checkpointed past lagging followers; they need a snapshot"
                );
            }
            primary.end_checkpoint(Ok(&marker), &excluded);
            if let Some(anchor) = primary.rebase_chain(&marker.position) {
                record_anchor(inner, &primary.name, anchor).await;
            }
            // A lost chain is rebuilt from the log the checkpoint restarted, whether or not a
            // stream runs to notice: a lone primary has none (#156).
            rebuild_lost_chain(inner, primary);
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

/// Records `anchor` as `database`'s chain anchor, durably. A record that can't be written only
/// costs a recomputation from the log after a restart, so failure is logged, not returned.
async fn record_anchor(inner: &Inner, database: &DatabaseName, anchor: Anchor) {
    let database = database.clone();
    let stored = blocking(inner.execution, {
        let chains = inner.chains.clone();
        let database = database.clone();
        move || chains.store(&database, &anchor)
    })
    .await;
    match stored {
        Ok(Ok(())) => {}
        Ok(Err(error)) => {
            tracing::warn!(database = %database, %error, "can't record the chain's anchor");
        }
        Err(status) => {
            tracing::warn!(database = %database, %status, "can't record the chain's anchor");
        }
    }
}

/// Brings the chain up to `target`, a position of the log, if it isn't there yet: through the
/// retained batches first, then by reading the log from the chain's tip through `handle`.
/// Returns whether the chain holds `target` afterwards.
///
/// A stream calls this before it ships from a position the chain hasn't reached, a snapshot
/// before it reports the hash at its position, and a checkpoint before it moves the anchor.
pub(super) async fn chain_to(
    inner: &Inner,
    primary: &Primary,
    handle: &DatabaseHandle,
    target: Option<&Position>,
) -> bool {
    let Some(engine) = frame_source(inner) else {
        return false;
    };
    if primary.link_retained(engine, target) {
        return true;
    }
    let (Some(tip), Some(target)) = (primary.chain_tip(), target) else {
        return false;
    };
    if tip.as_ref() >= Some(target) {
        return false;
    }
    match handle.committed_between(tip.clone(), target.clone()).await {
        Ok(Some(batch)) => primary.link(engine, tip.as_ref(), &batch).is_some(),
        Ok(None) => false,
        Err(error) => {
            tracing::debug!(database = %primary.name, from = ?tip, to = %target, %error, "can't read the log to bring the chain up to date");
            false
        }
    }
}

/// Brings the chain up to `target` as [`chain_to`] does, rebuilding it first, here, if it is
/// lost. A snapshot or a fork's seeding request calls this, since it needs the hash at its
/// position now, and the next pass of [`drive`] would only have it asked again.
pub(super) async fn chain_to_rebuilt(
    inner: &Inner,
    primary: &Primary,
    handle: &DatabaseHandle,
    target: Option<&Position>,
) -> bool {
    if primary.take_lost() {
        rebuild_chain(inner, primary).await;
    }
    chain_to(inner, primary, handle, target).await
}

/// Rebuilds the chain in a task of its own if it is lost.
fn rebuild_lost_chain(inner: &Arc<Inner>, primary: &Arc<Primary>) {
    if primary.take_lost() {
        let inner = Arc::clone(inner);
        let primary = Arc::clone(primary);
        tokio::spawn(async move { rebuild_chain(&inner, &primary).await });
    }
}

/// Rebuilds the primary's chain from its record and the log, as [`drive`] does when serving
/// starts, and as a lost chain is when next asked for. Ends with the chain built, or lost if the
/// build failed.
///
/// A checkpoint restarts the log, so the build reads only while none runs, and reads again if
/// one started meanwhile. Never called from inside a checkpoint, which it would wait for.
async fn rebuild_chain(inner: &Inner, primary: &Primary) {
    let mut changes = primary.changed.subscribe();
    loop {
        let checkpoints = primary.checkpoints();
        if checkpoints % 2 == 1 {
            // A checkpoint's end bumps `changed`, whose sender lives as long as the primary.
            let _ = changes.changed().await;
            continue;
        }
        let built = match inner.registry().open(&primary.name).await {
            Ok(handle) => load_chain(inner, primary, &handle).await,
            Err(error) => Err(error.to_string()),
        };
        match primary.chain_built(built, checkpoints) {
            Rebuild::Built | Rebuild::Failed => return,
            Rebuild::Again => {}
        }
    }
}

/// Reads the chain's record and the log through `handle`, and builds the chain.
async fn load_chain(
    inner: &Inner,
    primary: &Primary,
    handle: &DatabaseHandle,
) -> Result<Chain, String> {
    let Some(engine) = frame_source(inner) else {
        return Err("the engine isn't a frame source".to_owned());
    };
    if inner.forks.faults().take_chain_build_failure() {
        return Err("the fault hooks failed this build".to_owned());
    }
    let recorded = {
        let chains = inner.chains.clone();
        let database = primary.name.clone();
        blocking(inner.execution, move || chains.load(&database))
            .await
            .map_err(|status| status.to_string())?
    };
    let recorded = match recorded {
        Ok(recorded) => recorded,
        Err(error) => {
            tracing::warn!(database = %primary.name, %error, "can't read the chain's record; deriving one");
            None
        }
    };
    let position = handle.position().await.map_err(|error| error.to_string())?;
    let anchored_at = recorded.as_ref().map(Anchor::position);
    let read = match anchored_at {
        Some(at) if at == position => LogRead::Continues(None),
        Some(at) => match handle.committed_since(at).await {
            Ok(batch) => LogRead::Continues(batch),
            Err(error)
                if matches!(
                    error.engine_error(),
                    Some(EngineError::InvalidPosition { .. })
                ) =>
            {
                LogRead::Restarted(log_from_its_start(handle).await?)
            }
            Err(error) => return Err(error.to_string()),
        },
        None => LogRead::Restarted(log_from_its_start(handle).await?),
    };
    let recovered = chain::recover(engine, recorded, position.as_ref(), read)
        .map_err(|error| error.to_string())?;
    if let Some(anchor) = recovered.record {
        record_anchor(inner, &primary.name, anchor).await;
    }
    let chain = recovered.chain;
    if chain.is_derived() {
        tracing::info!(database = %primary.name, anchor = ?chain.anchor().position(), "no chain record fits the log; the chain is derived from where the log begins");
    }
    Ok(chain)
}

/// The current log as a chain derived from where it begins hashes it: read back from the batch's
/// own start, as [`verify`] reads it, so that bytes before that position, such as a generation's
/// header, are in neither.
async fn log_from_its_start(handle: &DatabaseHandle) -> Result<Option<FrameBatch>, String> {
    let Some(whole) = handle
        .committed_since(None)
        .await
        .map_err(|error| error.to_string())?
    else {
        return Ok(None);
    };
    let tail = handle
        .committed_since(Some(whole.start.clone()))
        .await
        .map_err(|error| error.to_string())?;
    Ok(tail.or(Some(whole)))
}

/// Reports the chain at `at`, or at the log's end, recomputed from the log on disk.
pub(super) async fn verify(
    inner: &Inner,
    primary: &Primary,
    at: Option<Position>,
) -> Result<Verified, tonic::Status> {
    let Some(engine) = frame_source(inner) else {
        return Err(tonic::Status::failed_precondition(format!(
            "{} databases have no replication log to verify",
            inner.engine.kind()
        )));
    };
    let handle = inner
        .registry()
        .open(&primary.name)
        .await
        .map_err(|error| tonic::Status::unavailable(error.to_string()))?;
    // The position the primary published, rather than what its log reads back to now.
    let at = match at.or_else(|| primary.committed()) {
        Some(at) => Some(at),
        None => handle
            .position()
            .await
            .map_err(|error| tonic::Status::unavailable(error.to_string()))?,
    };
    let anchor = primary.chain_anchor().ok_or_else(|| {
        tonic::Status::unavailable(format!(
            "the chain of {} isn't built yet; retry",
            primary.name
        ))
    })?;
    let held = primary.hash_at(at.as_ref());
    let hash = match (&at, anchor.position()) {
        (Some(at), anchored) if Some(at) > anchored.as_ref() => {
            let batch = handle
                .committed_between(anchored.clone(), at.clone())
                .await
                .map_err(|error| {
                    read_back_status(&primary.name, anchored.as_ref(), at, error.engine_error())
                })?;
            chain::recompute(engine, &anchor, batch.as_ref(), at)
                .map_err(|error| unreadable(&primary.name, at, &error.to_string()))?
        }
        _ => held.ok_or_else(|| {
            tonic::Status::not_found(format!(
                "{} holds no chain hash at {}",
                primary.name,
                at.as_ref()
                    .map_or_else(|| "the empty log".to_owned(), ToString::to_string)
            ))
        })?,
    };
    if let Some(held) = held
        && held != hash
    {
        tracing::warn!(database = %primary.name, at = ?at, %held, recomputed = %hash, "the log on disk no longer hashes to what the chain recorded");
    }
    Ok(Verified {
        epoch: primary.role.epoch,
        position: at,
        hash,
    })
}

/// The status for a log that can't be read back to `at`.
pub(super) fn unreadable(database: &DatabaseName, at: &Position, reason: &str) -> tonic::Status {
    tonic::Status::failed_precondition(format!(
        "the log of {database} can't be read back to {at}: {reason}"
    ))
}

/// The status for a read of the log from `anchored` to `at` that failed with `error`: a log
/// that no longer continues from the anchor restarted at a checkpoint whose new anchor isn't
/// recorded yet, which a retry settles; anything else is a log that can't be read back to `at`.
pub(super) fn read_back_status(
    database: &DatabaseName,
    anchored: Option<&Position>,
    at: &Position,
    error: Option<&EngineError>,
) -> tonic::Status {
    match error {
        Some(EngineError::InvalidPosition { position }) if Some(position) == anchored => {
            tonic::Status::unavailable(format!(
                "the log of {database} restarted past the chain's anchor; retry"
            ))
        }
        Some(error) => unreadable(database, at, &error.to_string()),
        None => unreadable(database, at, "the database is closed"),
    }
}

/// Extends `chain` through `recent`, the retained batches from the one that continues its tip
/// on, until it reaches `target`. Returns whether it did.
fn fill_gap(
    chain: &mut Chain,
    engine: &dyn FrameSourceEngine,
    recent: &[&FrameBatch],
    target: &Position,
) -> bool {
    for batch in recent {
        let tip = chain.tip().0.cloned();
        if tip.as_ref() == Some(target) {
            return true;
        }
        if batch.end > *target || chain.extend(engine, tip.as_ref(), batch).is_err() {
            return false;
        }
    }
    chain.tip().0 == Some(target)
}

/// Keeps one stream open to each follower in the database's replica set, as membership changes,
/// and starts checkpoints that come due while nothing is published.
///
/// It also reads the database's replica sets from control state again, in a task of its own,
/// whenever this node's ring is newer than the placement it counts, and while a membership
/// change keeps the old replica set, so that it learns of the release (ADR-0031, amendment 2).
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
    rebuild_chain(&inner, &primary).await;
    let mut membership = inner.membership.clone();
    let mut streams: BTreeMap<NodeId, AbortOnDrop> = BTreeMap::new();
    let mut reading: Option<AbortOnDrop> = None;
    let mut last_read: Option<Instant> = None;
    loop {
        let read_due = match inner.replica_sets_staleness(&primary.name) {
            Staleness::Fresh => false,
            Staleness::Periodic => {
                last_read.is_none_or(|at| at.elapsed() >= inner.settings.retry_interval * 5)
            }
            // Not more than once per retry interval, so that a control node that can't answer
            // isn't asked in a loop.
            Staleness::Now => {
                last_read.is_none_or(|at| at.elapsed() >= inner.settings.retry_interval)
            }
        };
        if reading.is_none() && read_due {
            last_read = Some(Instant::now());
            reading = Some(AbortOnDrop(tokio::spawn(read_replica_sets(
                Arc::clone(&inner),
                Arc::clone(&primary),
            ))));
        }
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
        // Streams rebuild a chain they find lost; a lone primary has none to (#156). A build
        // that failed is tried again here too, with no write needed to set it off.
        rebuild_lost_chain(&inner, &primary);
        tokio::select! {
            changed = membership.changed() => if changed.is_none() {
                time::sleep(inner.settings.retry_interval).await;
            },
            () = time::sleep(inner.settings.retry_interval * 5) => {}
            () = read(&mut reading) => reading = None,
        }
    }
}

/// Waits for the read of the replica sets under way, if there is one, and forever otherwise.
async fn read(reading: &mut Option<AbortOnDrop>) {
    match reading {
        Some(AbortOnDrop(task)) => {
            let _ = task.await;
        }
        None => std::future::pending().await,
    }
}

/// Reads the replica sets of `primary`'s database from control state and follows them, giving
/// up after the acknowledgement timeout: the stream supervisor asks again on its next round.
/// An answer without sets, from a control node that predates them, pins placement by the ring
/// this node had when it asked, unless sets were followed meanwhile (ADR-0031, amendment 2).
async fn read_replica_sets(inner: Arc<Inner>, primary: Arc<Primary>) {
    let database = &primary.name;
    let asked = inner.sets.generation(database);
    let ring = inner.membership.view();
    match time::timeout(inner.settings.ack_timeout, inner.authority.view(database)).await {
        Ok(Ok(view)) => match view.sets {
            Some(sets) => {
                inner.sets.follow(database, sets, Origin::Asked);
            }
            None => {
                let placed = ring
                    .ring()
                    .map_or_else(Vec::new, |ring| placed_on(ring, &primary));
                if inner
                    .sets
                    .follow_ring(database, ring.epoch(), placed, asked)
                    == RingFollowed::ReplacedSets
                {
                    tracing::warn!(database = %database, "the control plane no longer shows replica sets, as after a rollback; counting placement by the ring");
                }
            }
        },
        Ok(Err(status)) => {
            tracing::debug!(database = %database, %status, "can't read the replica sets; will retry");
        }
        Err(_) => {
            tracing::debug!(database = %database, "reading the replica sets timed out; will retry");
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
async fn stream(inner: &Arc<Inner>, primary: &Arc<Primary>, node: &NodeId) -> StreamEnd {
    let settings = &inner.settings;
    let Some(address) = inner.addresses.address(node) else {
        return stream_failed(format!("no address for {node}"));
    };
    let channel = match inner.peers.channel_to(&address) {
        Ok(channel) => channel,
        Err(error) => return stream_failed(error),
    };
    let (outgoing, messages) = mpsc::channel(settings.max_in_flight.get() + 2);
    // A follower that truncated back to the epoch's start may hold no hash there any more: it
    // takes this one rather than lead a chain of its own until the next batch.
    let epoch_start_chain = primary
        .start
        .as_ref()
        .and_then(|start| primary.hash_at(Some(start)))
        .map(|hash| hash.as_bytes().to_vec());
    let open = Message::Open(ReplicateOpen {
        database: primary.name.to_string(),
        epoch: primary.role.epoch.get(),
        epoch_start: primary.start.clone().map(Position::into_bytes),
        epoch_start_chain,
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
            let shipped = match primary.plan(sent.as_ref()) {
                Plan::Wait => break,
                Plan::NeedsSnapshot => {
                    return reseed(inner, primary, node, &outgoing, &mut acks, in_flight.len())
                        .await;
                }
                Plan::Send(mut shipped) => {
                    if shipped.chain.is_none() {
                        shipped.chain =
                            link_from(inner, primary, sent.as_ref(), &shipped.batch).await;
                    }
                    shipped
                }
                Plan::Read { after, checkpoints } => {
                    match read_since(inner, primary, after, checkpoints).await {
                        Ok(Some(batch)) => {
                            let chain = link_from(inner, primary, sent.as_ref(), &batch).await;
                            Shipped { batch, chain }
                        }
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
            rebuild_lost_chain(inner, primary);
            if in_flight.is_empty() {
                deadline = Instant::now() + settings.stall_timeout;
            }
            let Shipped { batch, chain } = shipped;
            let end = batch.end.clone();
            let bytes = batch.payload.len();
            let shipped = message(Message::Batch(proto::Batch {
                epoch: primary.role.epoch.get(),
                start: sent.clone().map(Position::into_bytes).unwrap_or_default(),
                end: batch.end.into_bytes(),
                payload: batch.payload.into_bytes(),
                chain: chain.map(|hash| hash.as_bytes().to_vec()),
            }));
            if outgoing.send(shipped).await.is_err() {
                return stream_failed("stream closed");
            }
            inner.metrics.batch_sent(bytes);
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

/// Chains `batch`, which a stream ships from `from`, bringing the chain up to `from` first if
/// it isn't there; `None` if the chain isn't built or can't reach `from`, which leaves it to be
/// rebuilt.
async fn link_from(
    inner: &Inner,
    primary: &Primary,
    from: Option<&Position>,
    batch: &FrameBatch,
) -> Option<ChainHash> {
    let engine = frame_source(inner)?;
    if primary.hash_at(from).is_none()
        && let Ok(handle) = inner.registry().open(&primary.name).await
    {
        chain_to(inner, primary, &handle, from).await;
    }
    primary.link(engine, from, batch)
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
    inner.metrics.reseed();
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

#[cfg(test)]
mod tests {
    use fleetring::ReplicationFactor;
    use fleetring_control::LeaseEpoch;

    use super::*;

    fn primary() -> Primary {
        let role = PrimaryRole::new(ReplicationFactor::ONE, LeaseEpoch::new(1));
        Primary::new("db".parse().expect("a valid name"), role, None)
    }

    /// A build that a checkpoint started during reads the log again, whether its reads built a
    /// chain or failed, since the checkpoint may have restarted the log between them (#156).
    #[test]
    fn a_build_a_checkpoint_raced_reads_again() {
        let primary = primary();
        let before = primary.checkpoints();
        primary.begin_checkpoint(&[]).expect("no checkpoint runs");
        let raced = Err("position 0020 does not fit the log".to_owned());
        assert_eq!(primary.chain_built(raced, before), Rebuild::Again);
        assert!(primary.chain_loading(), "the build goes on");
        primary.end_checkpoint(Err(()), &[]);
        assert_eq!(
            primary.chain_built(Ok(Chain::genesis()), before),
            Rebuild::Again
        );
        assert_eq!(
            primary.chain_built(Ok(Chain::genesis()), primary.checkpoints()),
            Rebuild::Built
        );
        assert!(primary.hash_at(None).is_some());
    }

    /// A build that fails with no checkpoint in between leaves the chain lost, not loading, so
    /// that it is built again when next asked, with no write to set it off (#156).
    #[test]
    fn a_failed_build_leaves_the_chain_lost() {
        let primary = primary();
        let failed = Err("the log can't be read".to_owned());
        assert_eq!(
            primary.chain_built(failed, primary.checkpoints()),
            Rebuild::Failed
        );
        assert!(!primary.chain_loading());
        assert!(primary.take_lost(), "the next asker builds it again");
        assert!(primary.chain_loading());
    }
}
