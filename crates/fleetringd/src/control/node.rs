//! The control node: a control group of one or more voters (ADR-0035, ADR-0040), its proposer,
//! and a handle to both.
//!
//! A single control node is a group of one voter, which elects itself as soon as it opens
//! (ADR-0040 point 9). Its leader screens, stamps, and proposes each submitted command
//! (`raft::proposer`), and every member applies the committed log with
//! `fleetring_control::apply` (`raft::state_machine`), publishing the state after each command.
//! The commands queued together are proposed as one batch, which openraft appends in one
//! transaction, so a burst of heartbeats costs one sync, not one each.

use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::num::{NonZeroU64, NonZeroUsize};
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use fleetring_control::fork::ForkLimits;
use fleetring_control::{
    Action, Command, ControlConfig, DEFAULT_TOMBSTONE_WINDOW, Millis, Outcome, Rejection, State,
};
use openraft::errors::{LinearizableReadError, RaftError};
use openraft::raft::ReadPolicy;
use openraft::rt::watch::WatchReceiver;
use openraft::{EntryPayload, SnapshotPolicy};
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinHandle;

use crate::clock::Clock;
use crate::control::format1::{LogError, LogIndex};
use crate::control::raft::codec::{SNAPSHOT_CHUNK_BYTES, VoteWire};
use crate::control::raft::gate::GroupGate;
use crate::control::raft::log_storage::{RaftLog, SharedStore};
use crate::control::raft::network::NoPeers;
use crate::control::raft::proposer::{ControlRaft, Mail, Proposer, ProposerSettings, Readiness};
use crate::control::raft::runtime::with_generator;
use crate::control::raft::state_machine::{ControlStateMachine, Published};
use crate::control::raft::{ControlPeer, Membership, RaftId, RaftIdError, RaftSeed, check_raft_id};
use crate::control::store::ControlStore;
use crate::environment::Network;
use crate::observe::{ControlMetrics, RaftSample};
use crate::router::Entropy;

/// How many log entries the control group takes between control snapshots, by default.
const DEFAULT_SNAPSHOT_EVERY: NonZeroU64 = NonZeroU64::new(1_000).expect("1000 is nonzero");

/// How many submitted commands may wait for the proposer, by default. Callers beyond it wait.
const DEFAULT_MAILBOX: NonZeroUsize = NonZeroUsize::new(256).expect("256 is nonzero");

/// The control group's shortest election timeout, by default.
const DEFAULT_ELECTION_TIMEOUT: Duration = Duration::from_secs(1);

/// How often the control group's leader sends Raft heartbeats, by default.
const DEFAULT_RAFT_HEARTBEAT: Duration = Duration::from_millis(250);

/// How many log entries a control node keeps behind its newest control snapshot, by default.
const DEFAULT_LOG_KEEP: u64 = 1_000;

/// How many control snapshots a control node keeps, by default.
const DEFAULT_SNAPSHOTS_RETAINED: NonZeroUsize = NonZeroUsize::new(2).expect("2 is nonzero");

/// How many log entries openraft purges at once at most (ADR-0040 point 7). openraft purges
/// only once a whole batch is due, so a batch never exceeds `snapshot_every`: each new control
/// snapshot then purges the entries it covers, and the log keeps at most `snapshot_every` plus
/// `log_keep` entries.
const PURGE_BATCH: u64 = 256;

/// The name openraft's configuration gives the control group.
const GROUP_NAME: &str = "fleetring-control";

/// Where the control node runs its blocking SQLite calls (ADR-0013, ADR-0020).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[non_exhaustive]
pub enum StorageIo {
    /// On Tokio's blocking pool, off the async executor. The default, for production.
    #[default]
    DedicatedThread,
    /// Inline, on the task that calls the store. Use it under simulation, where a single
    /// simulation thread keeps runs deterministic.
    Inline,
}

/// What a control node with an empty control log does (`[control] bootstrap`, ADR-0040 point 8).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
#[non_exhaustive]
pub enum Bootstrap {
    /// It starts a control group with itself as the only voter. The default.
    #[default]
    New,
    /// It waits to be added to an existing control group.
    Join,
}

/// How to run a control node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ControlOptions {
    config: ControlConfig,
    snapshot_every: NonZeroU64,
    mailbox: NonZeroUsize,
    storage_io: StorageIo,
    tombstone_window: Millis,
    bootstrap: Bootstrap,
    election_timeout: Duration,
    raft_heartbeat: Duration,
    log_keep: u64,
    snapshots_retained: NonZeroUsize,
    fork_limits: Option<ForkLimits>,
}

impl ControlOptions {
    /// Returns options that create new state under `config`, start a new control group, take a
    /// control snapshot every 1,000 entries and keep two, keep 1,000 entries behind the newest,
    /// queue up to 256 commands, run SQLite on Tokio's blocking pool, and time elections at 1 s
    /// with a 250 ms Raft heartbeat.
    #[must_use]
    pub const fn new(config: ControlConfig) -> Self {
        Self {
            config,
            snapshot_every: DEFAULT_SNAPSHOT_EVERY,
            mailbox: DEFAULT_MAILBOX,
            storage_io: StorageIo::DedicatedThread,
            tombstone_window: DEFAULT_TOMBSTONE_WINDOW,
            bootstrap: Bootstrap::New,
            election_timeout: DEFAULT_ELECTION_TIMEOUT,
            raft_heartbeat: DEFAULT_RAFT_HEARTBEAT,
            log_keep: DEFAULT_LOG_KEEP,
            snapshots_retained: DEFAULT_SNAPSHOTS_RETAINED,
            fork_limits: None,
        }
    }

    /// Takes a control snapshot after every `snapshot_every` log entries (blank and membership
    /// entries count).
    #[must_use]
    pub const fn with_snapshot_every(mut self, snapshot_every: NonZeroU64) -> Self {
        self.snapshot_every = snapshot_every;
        self
    }

    /// Lets up to `capacity` submitted commands wait for the proposer.
    #[must_use]
    pub const fn with_mailbox_capacity(mut self, capacity: NonZeroUsize) -> Self {
        self.mailbox = capacity;
        self
    }

    /// Runs SQLite as `storage_io` says.
    #[must_use]
    pub const fn with_storage_io(mut self, storage_io: StorageIo) -> Self {
        self.storage_io = storage_io;
        self
    }

    /// Keeps a new tombstone for `window` (ADR-0050). The leader logs it before the next drop
    /// whenever control state's window differs, so a replay never reads the configuration.
    #[must_use]
    pub const fn with_tombstone_window(mut self, window: Millis) -> Self {
        self.tombstone_window = window;
        self
    }

    /// Does `bootstrap` with an empty control log.
    #[must_use]
    pub const fn with_bootstrap(mut self, bootstrap: Bootstrap) -> Self {
        self.bootstrap = bootstrap;
        self
    }

    /// Draws each election timeout between `shortest` and twice it.
    #[must_use]
    pub const fn with_election_timeout(mut self, shortest: Duration) -> Self {
        self.election_timeout = shortest;
        self
    }

    /// Sends a Raft heartbeat every `interval` while this node leads.
    #[must_use]
    pub const fn with_raft_heartbeat(mut self, interval: Duration) -> Self {
        self.raft_heartbeat = interval;
        self
    }

    /// Keeps `entries` log entries behind the newest control snapshot when the log is purged.
    #[must_use]
    pub const fn with_log_keep(mut self, entries: u64) -> Self {
        self.log_keep = entries;
        self
    }

    /// Keeps the newest `retained` control snapshots on disk.
    #[must_use]
    pub const fn with_snapshots_retained(mut self, retained: NonZeroUsize) -> Self {
        self.snapshots_retained = retained;
        self
    }

    /// Compares the `[fork]` limits control state records with `limits` when this node becomes
    /// leader, and warns if they differ (ADR-0040 point 13).
    #[must_use]
    pub const fn with_fork_limits(mut self, limits: ForkLimits) -> Self {
        self.fork_limits = Some(limits);
        self
    }

    /// Returns the configuration a new log starts with. An existing log keeps its own.
    #[must_use]
    pub const fn config(&self) -> ControlConfig {
        self.config
    }

    /// Returns how many log entries pass between control snapshots.
    #[must_use]
    pub const fn snapshot_every(&self) -> NonZeroU64 {
        self.snapshot_every
    }

    /// Returns what this node does with an empty control log.
    #[must_use]
    pub const fn bootstrap(&self) -> Bootstrap {
        self.bootstrap
    }

    /// openraft's configuration for these options (ADR-0040 point 7).
    fn raft_config(&self) -> Result<openraft::Config, openraft::ConfigError> {
        let millis = |duration: Duration| u64::try_from(duration.as_millis()).unwrap_or(u64::MAX);
        let shortest = millis(self.election_timeout);
        openraft::Config {
            cluster_name: GROUP_NAME.to_owned(),
            election_timeout_min: shortest,
            election_timeout_max: shortest.saturating_mul(crate::config::ELECTION_TIMEOUT_SPREAD),
            heartbeat_interval: millis(self.raft_heartbeat),
            snapshot_policy: SnapshotPolicy::LogsSinceLast(self.snapshot_every.get()),
            max_in_snapshot_log_to_keep: self.log_keep,
            purge_batch_size: PURGE_BATCH.min(self.snapshot_every.get()),
            snapshot_max_chunk_size: u64::try_from(SNAPSHOT_CHUNK_BYTES).unwrap_or(u64::MAX),
            ..openraft::Config::default()
        }
        .validate()
    }
}

impl Default for ControlOptions {
    fn default() -> Self {
        Self::new(ControlConfig::new())
    }
}

/// The error returned when a control node can't open.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum OpenError {
    /// The control log couldn't be opened, migrated, or recovered.
    #[error("can't recover the control log")]
    Log(#[from] LogError),
    /// The node's ID can't be a Raft ID.
    #[error(transparent)]
    RaftId(#[from] RaftIdError),
    /// The control group's timings don't fit together.
    #[error("the control group's settings are invalid")]
    RaftConfig(#[source] openraft::ConfigError),
    /// openraft couldn't start the control group.
    #[error("can't start the control group")]
    Raft(#[source] Box<dyn Error + Send + Sync + 'static>),
}

/// The error returned when a command can't be committed.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum SubmitError {
    /// The control node has stopped, after a shutdown or a storage failure.
    #[error("the control node is not running")]
    Stopped,
    /// Logging the command failed. The command may or may not be durable; the node must restart
    /// to recover it.
    #[error("can't log the command")]
    Storage(#[source] LogError),
    /// The command was refused before it was logged ([`State::check_admission`], ADR-0038,
    /// point 4; the group gate, ADR-0040 point 4): a join whose release is outside the window or
    /// lacks an enabled feature, or a command that needs a fleet feature a member doesn't
    /// support. Nothing changed, and nothing was logged, so a control node of an older release,
    /// which would accept the command, never meets it in its log. A command that needs the
    /// `delegation` fleet feature is refused here, unlogged, for any rejection `apply` would give
    /// it (ADR-0047, point 6.2).
    #[error(transparent)]
    Refused(Rejection),
    /// This node doesn't lead the control group, or stopped leading before the command was
    /// logged. Nothing changed unless openraft later commits an entry it had appended, which a
    /// retry then meets as already done.
    #[error("not the control leader")]
    NotLeader {
        /// The leader's address, if this node knows it.
        leader: Option<String>,
    },
    /// This node leads the control group but hasn't applied an entry of its term yet, so it
    /// screens nothing.
    #[error("the control leader is catching up; retry")]
    CatchingUp {
        /// The leader's address: this node's.
        leader: Option<String>,
    },
    /// The entry was appended under another leader than the one that screened it, or its stamp
    /// names another node, so every state machine skipped it. Nothing changed.
    #[error("the control leader changed before the command was logged; retry")]
    Superseded,
    /// openraft discarded the entry from this node's log, truncated by a later leader or purged
    /// behind a snapshot, before its outcome was known here. Another leader may still have
    /// committed it, so whether it took effect is unknown; a retry is safe, since the next leader
    /// screens it against the state its log reaches (ADR-0040, amendment 1).
    #[error("the control leader changed before the command was known to be logged; retry")]
    Unknown,
}

/// A command that was logged and applied, or a resend of a delegation the log already records,
/// which the leader answers from the applied state without logging it again (ADR-0047, point
/// 5.7).
#[derive(Debug, Clone)]
pub struct Committed {
    index: LogIndex,
    command: Command,
    outcome: Outcome,
    state: Arc<State>,
}

impl Committed {
    pub(crate) fn new(
        index: LogIndex,
        command: Command,
        outcome: Outcome,
        state: Arc<State>,
    ) -> Self {
        Self {
            index,
            command,
            outcome,
            state,
        }
    }

    /// Returns the command's position in the log: for a resend answered from the state, the last
    /// entry applied when it was answered, whose state recorded what the resend asked for.
    #[must_use]
    pub fn index(&self) -> LogIndex {
        self.index
    }

    /// Returns the command as logged, with its stamp.
    #[must_use]
    pub fn command(&self) -> &Command {
        &self.command
    }

    /// Returns what applying the command did.
    #[must_use]
    pub fn outcome(&self) -> &Outcome {
        &self.outcome
    }

    /// Returns the state right after the command.
    #[must_use]
    pub fn state(&self) -> &State {
        &self.state
    }

    /// Returns the outcome, consuming the record.
    pub fn into_outcome(self) -> Outcome {
        self.outcome
    }

    pub(crate) fn into_parts(self) -> (Outcome, Arc<State>) {
        (self.outcome, self.state)
    }
}

/// The control group's membership, as this node knows it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ControlGroup {
    voters: Vec<ControlPeer>,
    learners: Vec<ControlPeer>,
}

impl ControlGroup {
    /// Returns the voters, by Raft ID.
    #[must_use]
    pub fn voters(&self) -> &[ControlPeer] {
        &self.voters
    }

    /// Returns the learners, by Raft ID.
    #[must_use]
    pub fn learners(&self) -> &[ControlPeer] {
        &self.learners
    }

    /// Whether the node `node_id` is a voter or a learner.
    #[must_use]
    pub fn contains(&self, node_id: &str) -> bool {
        self.voters
            .iter()
            .chain(&self.learners)
            .any(|peer| peer.node_id == node_id)
    }
}

/// A cheap, cloneable handle that submits commands to the control node and reads its state.
#[derive(Clone)]
pub struct ControlHandle {
    mailbox: mpsc::Sender<Mail>,
    published: Published,
    readiness: watch::Receiver<Readiness>,
    /// The control group's Raft; `None` on a node that opened stopped at an entry it can't
    /// decode, which runs no group.
    raft: Option<ControlRaft>,
    in_flight: Arc<AtomicUsize>,
    gate: GroupGate,
    own: ControlPeer,
    #[cfg(test)]
    faults: Arc<crate::control::raft::proposer::Faults>,
}

impl std::fmt::Debug for ControlHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ControlHandle")
            .field("own", &self.own)
            .field("readiness", &*self.readiness.borrow())
            .field("queued", &self.queued())
            .finish_non_exhaustive()
    }
}

impl ControlHandle {
    /// Screens, stamps, and proposes `action` on the control group's leader, and returns the
    /// result once its entry is committed and applied.
    ///
    /// Commands from every handle reach the log in the order the proposer screens them; those
    /// queued together are appended with one sync. When the mailbox is full, this waits for room.
    ///
    /// # Errors
    ///
    /// [`SubmitError`] if the node has stopped or can't log the command, refused it before
    /// logging it, isn't the ready leader ([`SubmitError::NotLeader`],
    /// [`SubmitError::CatchingUp`]), lost leadership before the entry took effect
    /// ([`SubmitError::Superseded`]), or lost it before the entry's outcome was known
    /// ([`SubmitError::Unknown`]). A command the state machine rejects is otherwise not an
    /// error here: it is logged, and its [`Committed::outcome`] holds the rejection. The
    /// exception is a command that needs the `delegation` fleet feature, whose rejection is
    /// [`SubmitError::Refused`] and is never logged (ADR-0047, point 6.2); and a resend of a
    /// delegation the log already records, which is answered with that record,
    /// [`Resent::Yes`](fleetring_control::delegation::Resent::Yes), once every entry proposed
    /// before it is committed and applied, and is never logged either.
    pub async fn submit(&self, action: Action) -> Result<Committed, SubmitError> {
        self.send(action, None).await
    }

    /// Submits `action` as [`ControlHandle::submit`] does, but only while this node is ready
    /// under `leader`: under any other term the proposer answers [`SubmitError::NotLeader`]
    /// without screening it. The leader's timers submit this way, so that a command they decided
    /// on in one term never reaches the log of the next before its settle window has passed
    /// (ADR-0040 point 6).
    ///
    /// # Errors
    ///
    /// As [`ControlHandle::submit`].
    pub(crate) async fn submit_under(
        &self,
        leader: crate::control::raft::LeaderRef,
        action: Action,
    ) -> Result<Committed, SubmitError> {
        self.send(action, Some(leader)).await
    }

    async fn send(
        &self,
        action: Action,
        under: Option<crate::control::raft::LeaderRef>,
    ) -> Result<Committed, SubmitError> {
        let (reply, receive) = oneshot::channel();
        self.mailbox
            .send(Mail::Submit {
                action: Box::new(action),
                under,
                reply,
            })
            .await
            .map_err(|_| SubmitError::Stopped)?;
        receive.await.map_err(|_| SubmitError::Stopped)?
    }

    /// Returns the latest published state: the state after the last applied command.
    #[must_use]
    pub fn state(&self) -> Arc<State> {
        Arc::clone(&self.published.state.borrow())
    }

    /// Returns a receiver that sees each newly published state.
    #[must_use]
    pub fn watch(&self) -> watch::Receiver<Arc<State>> {
        self.published.state.clone()
    }

    /// Returns the index of the last entry this node applied.
    #[must_use]
    pub fn applied(&self) -> LogIndex {
        LogIndex::new(self.published.applied.borrow().index())
    }

    /// Waits until this node, as leader, has confirmed its leadership with a quorum and applied
    /// every entry committed before the call, so that a read of [`ControlHandle::state`] then is
    /// linearizable (`ReadPolicy::ReadIndex`, ADR-0040 point 2).
    ///
    /// # Errors
    ///
    /// [`SubmitError::NotLeader`] if this node doesn't lead, [`SubmitError::CatchingUp`] if no
    /// quorum answers, and [`SubmitError::Stopped`] if the node has stopped.
    pub async fn ensure_linearizable(&self) -> Result<(), SubmitError> {
        let Some(raft) = &self.raft else {
            return Err(SubmitError::Stopped);
        };
        match raft.ensure_linearizable(ReadPolicy::ReadIndex).await {
            Ok(_) => Ok(()),
            Err(RaftError::APIError(LinearizableReadError::ForwardToLeader(forward))) => {
                Err(SubmitError::NotLeader {
                    leader: forward.leader_node.map(|peer| peer.address),
                })
            }
            Err(RaftError::APIError(LinearizableReadError::QuorumNotEnough(_))) => {
                Err(SubmitError::CatchingUp {
                    leader: Some(self.own.address.clone()),
                })
            }
            Err(RaftError::Fatal(_)) => Err(SubmitError::Stopped),
        }
    }

    /// Returns whether this node may propose, and under which leader.
    #[must_use]
    pub fn leadership(&self) -> Readiness {
        self.readiness.borrow().clone()
    }

    /// Returns a receiver that sees each change of [`ControlHandle::leadership`].
    #[must_use]
    pub fn watch_leadership(&self) -> watch::Receiver<Readiness> {
        self.readiness.clone()
    }

    /// Returns the control group's membership, as this node knows it.
    #[must_use]
    pub fn group(&self) -> ControlGroup {
        let Some(raft) = &self.raft else {
            return ControlGroup::default();
        };
        let metrics = raft.metrics();
        let metrics = metrics.borrow_watched();
        let membership = metrics.membership_config.membership();
        let peer = |id: RaftId| membership.get_node(&id).cloned();
        ControlGroup {
            voters: membership.voter_ids().filter_map(peer).collect(),
            learners: membership.non_write_voter_ids().filter_map(peer).collect(),
        }
    }

    /// Returns the leader this node knows of, if any: itself or another member.
    #[must_use]
    pub fn leader(&self) -> Option<ControlPeer> {
        match self.leadership() {
            Readiness::Follower { leader } => leader,
            Readiness::CatchingUp { .. } | Readiness::Ready { .. } => Some(self.own.clone()),
        }
    }

    /// Returns this node's peer: its node ID and address.
    #[must_use]
    pub fn own(&self) -> &ControlPeer {
        &self.own
    }

    /// Returns the group gate this node's proposals pass (ADR-0040 point 4).
    #[must_use]
    pub fn gate(&self) -> GroupGate {
        self.gate
    }

    /// Why the control log stopped applying, if it met an entry this build can't decode
    /// (ADR-0040 point 4).
    #[must_use]
    pub fn halted(&self) -> Option<String> {
        self.published.halted.borrow().clone()
    }

    /// Returns a receiver that sees [`ControlHandle::halted`] change.
    #[must_use]
    pub fn watch_halted(&self) -> watch::Receiver<Option<String>> {
        self.published.halted.clone()
    }

    /// Whether the node still takes commands: `false` once it stopped, after a shutdown or a
    /// storage failure.
    #[must_use]
    pub fn is_running(&self) -> bool {
        !self.mailbox.is_closed()
    }

    /// How many commands wait for the proposer or for their entries to be applied.
    #[must_use]
    pub fn queued(&self) -> usize {
        self.mailbox
            .max_capacity()
            .saturating_sub(self.mailbox.capacity())
            .saturating_add(self.in_flight.load(Ordering::Relaxed))
    }

    /// The openraft handle, for tests.
    #[cfg(test)]
    pub(crate) fn raft(&self) -> &ControlRaft {
        self.raft.as_ref().expect("the node runs its group")
    }

    /// How many proposals wait for openraft's answer, for tests.
    #[cfg(test)]
    pub(crate) fn in_flight(&self) -> usize {
        self.in_flight.load(Ordering::SeqCst)
    }

    /// The faults a test injects into the proposer.
    #[cfg(test)]
    pub(crate) fn faults(&self) -> &crate::control::raft::proposer::Faults {
        &self.faults
    }
}

/// A running control node: its control group, its proposer, and a handle to both.
#[derive(Debug)]
pub struct ControlNode {
    handle: ControlHandle,
    recovered: RecoverySummary,
    #[cfg_attr(not(test), expect(dead_code, reason = "the tests count its appends"))]
    log: RaftLog,
    tasks: Vec<JoinHandle<()>>,
    /// Tells the proposer to stop even while it waits for a batch to be answered.
    stopping: watch::Sender<bool>,
}

/// What a control node found when it opened its log.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecoverySummary {
    /// The index of the last entry in the log.
    pub last_index: LogIndex,
    /// The newest control snapshot, if any.
    pub snapshot: Option<LogIndex>,
    /// How many committed entries follow the snapshot, which the node applies again.
    pub replayed: u64,
}

impl ControlNode {
    /// Opens the control log at `path`, migrating a format-1 log, and starts the control group
    /// with this node, `own`, as its only voter if the log is new, and the proposer.
    ///
    /// openraft's randomness is seeded from eight bytes of `entropy` (ADR-0020, ADR-0040 point 1);
    /// `network` is what the control group reaches its peers over.
    ///
    /// # Errors
    ///
    /// [`OpenError`] if the log can't be opened, migrated, or recovered, the node's ID can't be a
    /// Raft ID, the control group's settings are invalid, or openraft can't start.
    pub async fn open(
        path: impl Into<PathBuf>,
        options: ControlOptions,
        clock: impl Clock + 'static,
        own: ControlPeer,
        network: Network,
        entropy: &dyn Entropy,
    ) -> Result<Self, OpenError> {
        Self::open_with_metrics(
            path,
            options,
            clock,
            own,
            network,
            entropy,
            ControlMetrics::default(),
        )
        .await
    }

    /// Opens the control node as [`ControlNode::open`] does, counting in `metrics`.
    ///
    /// # Errors
    ///
    /// As [`ControlNode::open`].
    pub(crate) async fn open_with_metrics(
        path: impl Into<PathBuf>,
        options: ControlOptions,
        clock: impl Clock + 'static,
        own: ControlPeer,
        network: Network,
        entropy: &dyn Entropy,
        metrics: ControlMetrics,
    ) -> Result<Self, OpenError> {
        let own_raft = check_raft_id(&own.node_id, [])?;
        let seed = RaftSeed::from_entropy(entropy);
        let opening = Opening {
            path: path.into(),
            options,
            clock: Box::new(clock),
            own,
            own_raft,
            network,
            metrics,
        };
        with_generator(seed, own_raft, opening.start()).await
    }

    /// Returns a handle for submitting commands and reading state.
    #[must_use]
    pub fn handle(&self) -> ControlHandle {
        self.handle.clone()
    }

    /// Returns what recovery found when the node opened.
    #[must_use]
    pub fn recovered(&self) -> RecoverySummary {
        self.recovered
    }

    /// How many appends the node's log storage made, each one transaction with one sync.
    #[cfg(test)]
    pub(crate) fn appends(&self) -> u64 {
        self.log.appends()
    }

    /// Stops the proposer after the commands already submitted, then the control group, and
    /// closes the log.
    ///
    /// Commands submitted after this fail with [`SubmitError::Stopped`]. State watchers see their
    /// streams end.
    pub async fn shutdown(self) {
        self.stopping.send_replace(true);
        let (done, stopped) = oneshot::channel();
        if self.handle.mailbox.send(Mail::Stop { done }).await.is_ok() {
            // An error means the proposer already exited, which is what shutdown wants.
            let _stopped = stopped.await;
        }
        if let Some(raft) = &self.handle.raft {
            let _stopped = raft.shutdown().await;
        }
        for task in self.tasks {
            task.abort();
        }
    }
}

/// Everything [`ControlNode::open_with_metrics`] needs, inside the generator's scope.
struct Opening {
    path: PathBuf,
    options: ControlOptions,
    clock: Box<dyn Clock>,
    own: ControlPeer,
    own_raft: RaftId,
    network: Network,
    metrics: ControlMetrics,
}

impl Opening {
    async fn start(self) -> Result<ControlNode, OpenError> {
        let Self {
            path,
            options,
            clock,
            own,
            own_raft,
            network,
            metrics,
        } = self;
        let io = options.storage_io;
        let config = options.config;
        let peer = own.clone();
        let opened = tokio_blocking(io, move || {
            ControlStore::open(path, config, Some((own_raft, peer)))
        })
        .await?;
        if opened.genesis() != options.config {
            tracing::warn!(
                configured = ?options.config,
                genesis = ?opened.genesis(),
                "the control log keeps the timings it was created with",
            );
        }
        let store: SharedStore = Arc::new(Mutex::new(opened));
        let found = {
            let store = Arc::clone(&store);
            tokio_blocking(io, move || {
                Found::read(
                    &mut store.lock().expect("control store lock poisoned"),
                    own_raft,
                )
            })
            .await?
        };
        let raft_config = options.raft_config().map_err(OpenError::RaftConfig)?;
        let log = RaftLog::new(Arc::clone(&store), io, metrics.clone());
        let (machine, published) = ControlStateMachine::open(
            Arc::clone(&store),
            io,
            options.snapshots_retained,
            metrics.clone(),
        )?;
        let stopped_at = published.halted.borrow().clone();
        if let Some(reason) = stopped_at {
            // Stopped at an entry this build can't decode: openraft would apply it again as it
            // starts, and fail, so the node runs no group. It opens, says why in `control_log`,
            // and a node that also hosts data keeps serving it (ADR-0040 point 4).
            tracing::error!("the control group doesn't start: {reason}");
            drop(machine);
            let (mailbox, inbox) = mpsc::channel(1);
            drop(inbox);
            let (_readiness, readiness) = watch::channel(Readiness::Follower { leader: None });
            return Ok(ControlNode {
                handle: ControlHandle {
                    mailbox,
                    published,
                    readiness,
                    raft: None,
                    in_flight: Arc::new(AtomicUsize::new(0)),
                    gate: GroupGate::new(),
                    own,
                    #[cfg(test)]
                    faults: Arc::default(),
                },
                recovered: found.summary,
                log,
                tasks: Vec::new(),
                stopping: watch::channel(false).0,
            });
        }
        let raft = ControlRaft::new(
            own_raft,
            Arc::new(raft_config),
            NoPeers::new(network),
            log.clone(),
            machine,
        )
        .await
        .map_err(|error| OpenError::Raft(Box::new(error)))?;
        let mut alone = found.alone;
        if found.empty {
            match options.bootstrap {
                Bootstrap::New => {
                    let membership = Membership::new(
                        vec![BTreeSet::from([own_raft])],
                        BTreeMap::from([(own_raft, own.clone())]),
                    )
                    .map_err(|error| OpenError::Raft(Box::new(error)))?;
                    raft.initialize(membership)
                        .await
                        .map_err(|error| OpenError::Raft(Box::new(error)))?;
                    alone = true;
                }
                Bootstrap::Join => {
                    tracing::info!("waiting to be added to a control group");
                }
            }
        }
        // A group of one voter elects itself at once rather than wait an election timeout
        // (ADR-0040 point 9). A former leader already campaigns as it starts. The election may
        // commit an entry this build can't decode, which stops the group as it opens: the node
        // opens all the same, and says why.
        if alone
            && !found.led
            && let Err(error) = raft.trigger().elect(false).await
            && published.halted.borrow().is_none()
        {
            return Err(OpenError::Raft(Box::new(error)));
        }
        let (mailbox, inbox) = mpsc::channel(options.mailbox.get());
        let (stopping, stopping_receiver) = watch::channel(false);
        let running = Proposer::spawn(
            raft.clone(),
            clock,
            ProposerSettings {
                own: own
                    .node_id
                    .parse()
                    .map_err(|error| OpenError::Raft(Box::new(error)))?,
                peer: own.clone(),
                tombstone_window: options.tombstone_window,
                fork_limits: options.fork_limits,
            },
            published.applied.clone(),
            metrics.clone(),
            inbox,
            stopping_receiver,
        );
        let watcher = tokio::spawn(watch_metrics(raft.clone(), metrics));
        if alone {
            // A voter alone elects itself within a round of its own storage, so its callers find
            // it ready as they found the single writer; a node that can't elect itself still
            // opens, and answers `CatchingUp` or `NotLeader` until it can, and one that stops at
            // an entry it can't decode opens at once.
            let mut readiness = running.readiness.clone();
            let mut halted = published.halted.clone();
            let limit = options.election_timeout.saturating_mul(4);
            let settled = async {
                tokio::select! {
                    ready = readiness.wait_for(Readiness::is_ready) => ready.map(drop),
                    halted = halted.wait_for(Option::is_some) => halted.map(drop),
                }
            };
            let ready = tokio::time::timeout(limit, settled).await;
            if ready.is_err() {
                tracing::warn!(
                    "the control node didn't elect itself within {} ms",
                    limit.as_millis()
                );
            }
        }
        let recovered = found.summary;
        tracing::info!(
            last_index = recovered.last_index.get(),
            snapshot = ?recovered.snapshot.map(LogIndex::get),
            replayed = recovered.replayed,
            "control state recovered",
        );
        Ok(ControlNode {
            handle: ControlHandle {
                mailbox,
                published,
                readiness: running.readiness,
                raft: Some(raft),
                in_flight: running.in_flight,
                gate: GroupGate::new(),
                own,
                #[cfg(test)]
                faults: running.faults,
            },
            recovered,
            log,
            tasks: vec![running.task, watcher],
            stopping,
        })
    }
}

/// What a control node reads from its store before it starts the group.
#[derive(Debug)]
struct Found {
    summary: RecoverySummary,
    /// Whether the store holds no Raft state at all.
    empty: bool,
    /// Whether the node is its group's only voter.
    alone: bool,
    /// Whether the node's saved vote is a committed vote for itself: it led when it stopped.
    led: bool,
}

impl Found {
    fn read(store: &mut ControlStore, own: RaftId) -> Result<Self, LogError> {
        let last = store.last_log_id()?;
        let snapshot = store.latest_control_snapshot()?;
        let committed = store.committed()?;
        let snapshot_index = snapshot
            .as_ref()
            .and_then(|snapshot| snapshot.meta.last_log_id)
            .map(|log_id| log_id.index);
        let mut voters: Option<Vec<RaftId>> = snapshot
            .as_ref()
            .map(|snapshot| snapshot.meta.last_membership.voter_ids().collect());
        let from = snapshot_index.map_or(0, |index| index.saturating_add(1));
        for entry in store.entries(from..)? {
            if let EntryPayload::Membership(membership) = &entry.payload {
                voters = Some(membership.voter_ids().collect());
            }
        }
        let led = store.vote()?.is_some_and(|vote| {
            let vote = VoteWire::from(&vote);
            vote.committed && vote.node.0 == own
        });
        let replayed = committed
            .map(|log_id| log_id.index)
            .unwrap_or_default()
            .saturating_sub(snapshot_index.unwrap_or_default());
        Ok(Self {
            summary: RecoverySummary {
                last_index: LogIndex::new(last.map_or(0, |log_id| log_id.index)),
                snapshot: snapshot_index.map(LogIndex::new),
                replayed,
            },
            empty: last.is_none() && snapshot.is_none(),
            alone: voters.as_deref() == Some(&[own]),
            led,
        })
    }
}

/// Runs `call` where `io` says: inline, or on Tokio's blocking pool.
async fn tokio_blocking<T, F>(io: StorageIo, call: F) -> Result<T, OpenError>
where
    T: Send + 'static,
    F: FnOnce() -> Result<T, LogError> + Send + 'static,
{
    match io {
        StorageIo::Inline => Ok(call()?),
        StorageIo::DedicatedThread => Ok(tokio::task::spawn_blocking(call)
            .await
            .map_err(|error| OpenError::Raft(Box::new(error)))??),
    }
}

/// Sets the control group's gauges from openraft's metrics, and counts each term this node sees
/// begin, until the group stops (ADR-0040 point 10).
async fn watch_metrics(raft: ControlRaft, metrics: ControlMetrics) {
    let mut receiver = raft.metrics();
    let mut term = None;
    loop {
        let sample = {
            let raft_metrics = receiver.borrow_watched();
            if term.is_some_and(|seen| raft_metrics.current_term > seen) {
                metrics.election();
            }
            term = Some(raft_metrics.current_term);
            let membership = raft_metrics.membership_config.membership();
            let last_log_index = raft_metrics.last_log_index.unwrap_or_default();
            let replication_lag_max = raft_metrics
                .replication
                .as_ref()
                .map(|replication| {
                    replication
                        .values()
                        .map(|matched| {
                            last_log_index.saturating_sub(matched.map_or(0, |log_id| log_id.index))
                        })
                        .max()
                        .unwrap_or_default()
                })
                .unwrap_or_default();
            RaftSample {
                last_log_index,
                committed_index: raft_metrics
                    .local_committed
                    .map_or(0, |log_id| log_id.index),
                applied_index: raft_metrics.last_applied.map_or(0, |log_id| log_id.index),
                snapshot_index: raft_metrics.snapshot.map_or(0, |log_id| log_id.index),
                term: raft_metrics.current_term,
                voters: membership.voter_ids().count(),
                learners: membership.non_write_voter_ids().count(),
                replication_lag_max,
            }
        };
        metrics.raft(&sample);
        if receiver.changed().await.is_err() {
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use fleetring::NodeId;
    use fleetring_control::{Timestamp, apply};

    use super::*;
    use crate::clock::SimClock;
    use crate::control::test_support::{logged_commands, open_node, own};

    /// The proposer stamps its own node ID beside the time on every command it logs, and a
    /// recovery replays the stamp from the log (ADR-0047, point 6.1). Every control node stamps
    /// one: the state machine supersedes a command without it (decision D5), so a node opened
    /// again stamps its own node ID too.
    #[tokio::test(start_paused = true)]
    async fn control_writer_stamps_its_node_id() {
        let dir = tempfile::tempdir().expect("a directory");
        let path = dir.path().join("control.db");
        let base = Timestamp::from_millis(1_000);
        let options = ControlOptions::default().with_storage_io(StorageIo::Inline);
        let node = open_node(&path, options.clone(), SimClock::new(base))
            .await
            .expect("opens");
        let committed = node.handle().submit(Action::Tick).await.expect("commits");
        assert_eq!(
            committed.command().recorded_by.as_ref().map(NodeId::as_str),
            Some("control-1")
        );
        node.shutdown().await;

        let node = open_node(&path, options, SimClock::new(base))
            .await
            .expect("reopens");
        let committed = node.handle().submit(Action::Tick).await.expect("commits");
        assert_eq!(
            committed.command().recorded_by.as_ref().map(NodeId::as_str),
            Some(own().node_id.as_str())
        );
        node.shutdown().await;

        let stamps: Vec<Option<String>> = logged_commands(&path)
            .into_iter()
            .map(|command| command.recorded_by.map(|by| by.as_str().to_owned()))
            .collect();
        assert_eq!(
            stamps,
            [Some("control-1".to_owned()), Some("control-1".to_owned())]
        );
    }

    /// The `EnableHandover` the proposer logs before the first membership change that keeps a
    /// replica set carries the proposer's node ID, as every command it logs does (ADR-0031,
    /// amendment 2; ADR-0047, point 6.1), and comes just before that change in the log.
    #[tokio::test(start_paused = true)]
    async fn control_writer_stamps_the_handover_it_enables() {
        use fleetring::ReplicationFactor;
        use fleetring_control::{Engine, Feature, Incarnation, Member, Release, Software};

        let software = Feature::ALL.into_iter().fold(
            Software::new(Release::new(0, 8, 0), 1),
            Software::with_feature,
        );
        let join = |name: &str| Action::Join {
            member: Member::new(name.parse().expect("an id")).with_software(software.clone()),
        };
        let create = |name: &str| Action::CreateDatabase {
            name: name.parse().expect("a name"),
            replication_factor: ReplicationFactor::new(3).expect("nonzero"),
            engine: Engine::Sqlite,
            durability: None,
            metadata: None,
            incarnation: Incarnation::LEGACY,
            incarnation_rule: fleetring_control::IncarnationRule::OnceForked,
        };
        // A database whose replica set `d`'s join moves, found on a state of its own.
        let (name, primary) = (0..200)
            .find_map(|n| {
                let name = format!("t{n}");
                let mut state = State::new(ControlConfig::new());
                for member in ["a", "b", "c"] {
                    apply(&mut state, Command::new(Timestamp::ZERO, join(member))).ok()?;
                }
                apply(&mut state, Command::new(Timestamp::ZERO, create(&name))).ok()?;
                let database = name.parse().expect("a name");
                let before = state.replica_set(&database).ok()?;
                apply(&mut state, Command::new(Timestamp::ZERO, join("d"))).ok()?;
                let after = state.replica_set(&database).ok()?;
                (after != before).then(|| (name, before[0].clone()))
            })
            .expect("a name that d's join moves");

        let dir = tempfile::tempdir().expect("a directory");
        let path = dir.path().join("control.db");
        let options = ControlOptions::default().with_storage_io(StorageIo::Inline);
        let node = open_node(&path, options, SimClock::new(Timestamp::from_millis(1_000)))
            .await
            .expect("opens");
        let handle = node.handle();
        let lease = Action::AcquireLease {
            database: name.parse().expect("a name"),
            node: primary,
        };
        for action in [
            join("a"),
            join("b"),
            join("c"),
            create(&name),
            lease,
            join("d"),
        ] {
            let committed = handle.submit(action).await.expect("commits");
            assert!(committed.outcome().is_ok(), "{:?}", committed.outcome());
        }
        assert!(handle.state().is_enabled(Feature::Handover));
        node.shutdown().await;

        let commands = logged_commands(&path);
        let enabled = commands
            .iter()
            .position(|command| matches!(command.action, Action::EnableHandover))
            .expect("the proposer logged EnableHandover");
        assert!(
            matches!(&commands[enabled + 1].action, Action::Join { member } if member.id().as_str() == "d"),
            "{:?}",
            commands[enabled + 1]
        );
        assert!(
            commands.iter().all(
                |command| command.recorded_by.as_ref().map(NodeId::as_str) == Some("control-1")
            ),
            "{commands:?}"
        );
    }

    /// The proposer logs `EnableIncarnations` just before the first creation under `always`,
    /// once every member supports the feature, so a release without it refuses the log there
    /// rather than replay the creation to the legacy incarnation; it refuses such a creation
    /// unlogged while a member lacks the feature (ADR-0046's FR-110 amendment).
    #[tokio::test(start_paused = true)]
    async fn control_writer_enables_incarnations_before_the_first_creation_under_always() {
        use fleetring::ReplicationFactor;
        use fleetring_control::{
            Engine, Feature, Incarnation, IncarnationRule, Member, Rejection, Release, Software,
        };

        let every = Feature::ALL.into_iter().fold(
            Software::new(Release::new(0, 8, 1), 1),
            Software::with_feature,
        );
        let without = Feature::ALL
            .into_iter()
            .filter(|feature| *feature != Feature::Incarnations)
            .fold(
                Software::new(Release::new(0, 8, 0), 1),
                Software::with_feature,
            );
        let join = |name: &str, software: &Software| Action::Join {
            member: Member::new(name.parse().expect("an id")).with_software(software.clone()),
        };
        let always = |name: &str, incarnation: u64| Action::CreateDatabase {
            name: name.parse().expect("a name"),
            replication_factor: ReplicationFactor::new(3).expect("nonzero"),
            engine: Engine::Sqlite,
            durability: None,
            metadata: None,
            incarnation: Incarnation::new(incarnation),
            incarnation_rule: IncarnationRule::Always,
        };

        let dir = tempfile::tempdir().expect("a directory");
        let path = dir.path().join("control.db");
        let options = ControlOptions::default().with_storage_io(StorageIo::Inline);
        let node = open_node(&path, options, SimClock::new(Timestamp::from_millis(1_000)))
            .await
            .expect("opens");
        let handle = node.handle();
        for action in [join("a", &every), join("b", &every), join("c", &without)] {
            let committed = handle.submit(action).await.expect("commits");
            assert!(committed.outcome().is_ok(), "{:?}", committed.outcome());
        }
        let refused = handle.submit(always("early", 0x11)).await;
        assert!(
            matches!(
                refused,
                Err(SubmitError::Refused(Rejection::FeatureUnsupported {
                    feature: Feature::Incarnations,
                    ..
                }))
            ),
            "{refused:?}"
        );
        let committed = handle.submit(join("c", &every)).await.expect("commits");
        assert!(committed.outcome().is_ok(), "{:?}", committed.outcome());
        let committed = handle.submit(always("fresh", 0x22)).await.expect("commits");
        assert!(committed.outcome().is_ok(), "{:?}", committed.outcome());
        assert!(handle.state().is_enabled(Feature::Incarnations));
        let committed = handle.submit(always("later", 0x33)).await.expect("commits");
        assert!(committed.outcome().is_ok(), "{:?}", committed.outcome());
        node.shutdown().await;

        let actions: Vec<Action> = logged_commands(&path)
            .into_iter()
            .map(|command| command.action)
            .collect();
        let enabling: Vec<usize> = actions
            .iter()
            .enumerate()
            .filter(|(_, action)| matches!(action, Action::EnableIncarnations))
            .map(|(index, _)| index)
            .collect();
        assert_eq!(enabling.len(), 1, "{actions:?}");
        let created_next = matches!(
            &actions[enabling[0] + 1],
            Action::CreateDatabase { name, .. } if name.as_str() == "fresh"
        );
        assert!(created_next, "{actions:?}");
        let logged_early = actions.iter().any(
            |action| matches!(action, Action::CreateDatabase { name, .. } if name.as_str() == "early"),
        );
        assert!(!logged_early, "{actions:?}");
    }

    /// A delegation `apply` refuses is answered `Refused` and never logged, so a refusal costs
    /// the log nothing and a refused first use leaves no command the previous release line can't
    /// decode while the feature stays off (ADR-0047, point 6.2; ADR-0038, point 4). The accepted
    /// delegations around it are logged as before, and the log alone replays to the state the
    /// node published.
    #[tokio::test(start_paused = true)]
    async fn refused_delegations_are_never_logged() {
        use fleetring_control::Feature;
        use fleetring_control::delegation::{Limit, StructureViolation};

        use crate::control::test_support::{member_with_every_feature, root_delegation};

        let delegation = |now, n| root_delegation(now, n, 60_000);

        let dir = tempfile::tempdir().expect("a directory");
        let path = dir.path().join("control.db");
        let options = ControlOptions::default().with_storage_io(StorageIo::Inline);
        let clock = SimClock::new(Timestamp::from_millis(1_000_000));
        let node = open_node(&path, options, clock).await.expect("opens");
        let handle = node.handle();
        let member = member_with_every_feature("data-1");
        handle
            .submit(Action::Join { member })
            .await
            .expect("the join commits");

        // A first use that `apply` refuses: the requester isn't the caller.
        let mut planted = delegation(clock.now(), 0);
        if let Action::RecordDelegation { requested_by, .. } = &mut planted {
            *requested_by = "agent-43".parse().expect("a name");
        }
        let before = handle.state();
        let refused = handle.submit(planted).await;
        assert!(
            matches!(
                refused,
                Err(SubmitError::Refused(Rejection::DelegationStructure {
                    reason: StructureViolation::RequesterNotCaller
                }))
            ),
            "{refused:?}"
        );
        assert!(!handle.state().is_enabled(Feature::Delegation));
        assert_eq!(*handle.state(), *before);

        // The caller's rate: 30 a minute, then refused, unlogged, and judged again later.
        for n in 1..=30 {
            let committed = handle
                .submit(delegation(clock.now(), n))
                .await
                .expect("commits");
            assert!(committed.outcome().is_ok(), "{:?}", committed.outcome());
        }
        for n in 31..=40 {
            let refused = handle.submit(delegation(clock.now(), n)).await;
            assert!(
                matches!(
                    refused,
                    Err(SubmitError::Refused(Rejection::DelegationLimit {
                        limit: Limit::CallerRate
                    }))
                ),
                "{refused:?}"
            );
        }
        tokio::time::sleep(std::time::Duration::from_secs(61)).await;
        let later = handle
            .submit(delegation(clock.now(), 41))
            .await
            .expect("commits");
        assert!(later.outcome().is_ok(), "{:?}", later.outcome());
        let published = handle.state();
        node.shutdown().await;

        let commands = logged_commands(&path);
        let logged = commands
            .iter()
            .filter(|command| matches!(command.action, Action::RecordDelegation { .. }))
            .count();
        assert_eq!(logged, 31, "only the accepted delegations are logged");
        let mut replayed = State::new(ControlConfig::new());
        for command in commands {
            let _outcome = apply(&mut replayed, command);
        }
        assert_eq!(replayed, *published);
    }

    /// Racing identical `DelegateToken` calls submit the same `RecordDelegation` at once: the
    /// first is logged, and every other is answered with the same delegation, `Resent::Yes`,
    /// without a log entry of its own (ADR-0047, point 5.7), whether it queued in the same batch
    /// or a later one.
    #[tokio::test(start_paused = true)]
    async fn racing_identical_delegations_log_once() {
        use fleetring_control::Applied;
        use fleetring_control::delegation::Resent;

        use crate::control::test_support::{member_with_every_feature, root_delegation};

        let dir = tempfile::tempdir().expect("a directory");
        let path = dir.path().join("control.db");
        let options = ControlOptions::default().with_storage_io(StorageIo::Inline);
        let clock = SimClock::new(Timestamp::from_millis(1_000_000));
        let node = open_node(&path, options, clock).await.expect("opens");
        let handle = node.handle();
        handle
            .submit(Action::Join {
                member: member_with_every_feature("data-1"),
            })
            .await
            .expect("the join commits");

        let action = root_delegation(clock.now(), 1, 60_000);
        let racing: Vec<_> = (0..8)
            .map(|_| {
                let handle = handle.clone();
                let action = action.clone();
                tokio::spawn(async move { handle.submit(action).await })
            })
            .collect();
        let mut answers = Vec::new();
        for task in racing {
            answers.push(task.await.expect("joins").expect("answered"));
        }
        // And one more, after the batch that recorded it.
        answers.push(handle.submit(action).await.expect("answered"));
        let mut new = 0;
        let mut ids = std::collections::BTreeSet::new();
        for answer in &answers {
            match answer.outcome() {
                Ok(Applied::DelegationRecorded { id, resent, .. }) => {
                    ids.insert(id.clone());
                    if *resent == Resent::No {
                        new += 1;
                    }
                }
                other => panic!("expected the delegation: {other:?}"),
            }
        }
        assert_eq!(new, 1, "recorded once");
        assert_eq!(ids.len(), 1, "every answer names the one delegation");
        node.shutdown().await;

        let logged = logged_commands(&path)
            .iter()
            .filter(|command| matches!(command.action, Action::RecordDelegation { .. }))
            .count();
        assert_eq!(logged, 1, "the resends cost the log nothing");
    }

    /// A new control node starts a group of one voter and elects itself at once: it is ready
    /// before the shortest election timeout has passed (ADR-0040 point 9).
    #[tokio::test(start_paused = true)]
    async fn a_new_control_node_elects_itself_without_waiting() {
        let dir = tempfile::tempdir().expect("a directory");
        let path = dir.path().join("control.db");
        let started = tokio::time::Instant::now();
        let options = ControlOptions::default().with_storage_io(StorageIo::Inline);
        let node = open_node(&path, options.clone(), SimClock::new(Timestamp::ZERO))
            .await
            .expect("opens");
        assert!(node.handle().leadership().is_ready());
        assert!(
            started.elapsed() < DEFAULT_ELECTION_TIMEOUT,
            "{:?}",
            started.elapsed()
        );
        let group = node.handle().group();
        assert_eq!(group.voters(), [own()]);
        assert!(group.learners().is_empty());
        node.shutdown().await;

        // So does it when it opens its log again.
        let started = tokio::time::Instant::now();
        let node = open_node(&path, options, SimClock::new(Timestamp::ZERO))
            .await
            .expect("reopens");
        assert!(node.handle().leadership().is_ready());
        assert!(
            started.elapsed() < DEFAULT_ELECTION_TIMEOUT,
            "{:?}",
            started.elapsed()
        );
        node.handle().submit(Action::Tick).await.expect("commits");
        node.shutdown().await;
    }

    /// A node started with `bootstrap = "join"` and an empty log starts no group: it waits to be
    /// added, leads nothing, and answers every submission `NotLeader` without a leader to name.
    #[tokio::test(start_paused = true)]
    async fn a_joining_control_node_waits_to_be_added() {
        let dir = tempfile::tempdir().expect("a directory");
        let path = dir.path().join("control.db");
        let options = ControlOptions::default()
            .with_storage_io(StorageIo::Inline)
            .with_bootstrap(Bootstrap::Join);
        let node = open_node(&path, options, SimClock::new(Timestamp::ZERO))
            .await
            .expect("opens");
        tokio::time::sleep(DEFAULT_ELECTION_TIMEOUT * 4).await;
        let handle = node.handle();
        assert_eq!(handle.leadership(), Readiness::Follower { leader: None });
        assert!(handle.group().voters().is_empty());
        let refused = handle.submit(Action::Tick).await;
        assert!(
            matches!(refused, Err(SubmitError::NotLeader { leader: None })),
            "{refused:?}"
        );
        node.shutdown().await;
        let store = ControlStore::open(&path, ControlConfig::new(), None).expect("opens");
        assert!(store.is_fresh().expect("readable"), "nothing was logged");
    }

    /// The 0.9.0 fixture, a format-1 log, migrates when a control node opens it, and the node
    /// elects itself with the state the fixture's commands replay to (ADR-0040 point 8).
    #[tokio::test(start_paused = true)]
    async fn the_migrated_fixture_elects_itself_with_the_same_state() {
        let fixture =
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/control-0.9.0");
        let text = std::fs::read_to_string(fixture.join("commands.json")).expect("readable");
        let commands: Vec<Command> = serde_json::from_str(&text).expect("decodes");
        let mut replayed = State::new(ControlConfig::new());
        for command in commands {
            let _outcome = apply(&mut replayed, command);
        }
        let dir = tempfile::tempdir().expect("a directory");
        let path = dir.path().join("control.db");
        std::fs::copy(fixture.join("control.db"), &path).expect("copied");
        let options = ControlOptions::default().with_storage_io(StorageIo::Inline);
        let node = open_node(&path, options, SimClock::new(Timestamp::ZERO))
            .await
            .expect("opens");
        let handle = node.handle();
        assert!(handle.leadership().is_ready());
        assert_eq!(*handle.state(), replayed);
        let committed = handle.submit(Action::Tick).await.expect("commits");
        assert!(committed.outcome().is_ok(), "{:?}", committed.outcome());
        assert!(
            handle
                .state()
                .is_enabled(fleetring_control::Feature::Delegation)
        );
        node.shutdown().await;
    }

    /// With `snapshot_every = 10`, `log_keep = 5`, and two snapshots retained, a hundred ticks
    /// leave at most 15 entries and two control snapshots, and the state survives reopening
    /// (ADR-0040 point 7).
    #[tokio::test(start_paused = true)]
    async fn snapshots_compact_the_log() {
        let dir = tempfile::tempdir().expect("a directory");
        let path = dir.path().join("control.db");
        let options = ControlOptions::default()
            .with_storage_io(StorageIo::Inline)
            .with_snapshot_every(NonZeroU64::new(10).expect("nonzero"))
            .with_log_keep(5)
            .with_snapshots_retained(NonZeroUsize::new(2).expect("nonzero"));
        let clock = SimClock::new(Timestamp::from_millis(1_000));
        let node = open_node(&path, options.clone(), clock)
            .await
            .expect("opens");
        let handle = node.handle();
        for _ in 0..100 {
            tokio::time::sleep(Duration::from_millis(10)).await;
            handle.submit(Action::Tick).await.expect("commits");
        }
        // openraft builds and purges in the background.
        tokio::time::sleep(Duration::from_secs(1)).await;
        let published = handle.state();
        node.shutdown().await;

        let store = ControlStore::open(&path, ControlConfig::new(), None).expect("opens");
        let entries = store.entries(..).expect("readable");
        assert!(entries.len() <= 15, "{} entries", entries.len());
        let snapshots = store.control_snapshots().expect("readable");
        assert!(
            (1..=2).contains(&snapshots.len()),
            "{} snapshots",
            snapshots.len()
        );
        drop(store);

        let node = open_node(&path, options, clock).await.expect("reopens");
        assert_eq!(node.handle().state(), published);
        node.shutdown().await;
    }
}
