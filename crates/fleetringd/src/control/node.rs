//! The control node: one writer that stamps, logs, applies, and publishes every command.
//!
//! The writer takes the commands queued behind the one it picks up as a group: it logs the group
//! in one transaction, so a sync covers all of them, then applies them one at a time in log
//! order and answers each. Each command still gets its own index and stamp, and a replay of the
//! log can't tell which commands shared a sync. Without grouping, every command costs a sync of
//! its own, and on a disk whose syncs are slow the writer falls behind a steady stream of
//! heartbeats and ticks: its queue, and every control call's wait, then grow without bound.

use std::num::{NonZeroU64, NonZeroUsize};
use std::path::PathBuf;
use std::sync::Arc;
use std::{io, thread};

use fleetring::NodeId;
use fleetring_control::{Action, Command, ControlConfig, Outcome, Rejection, State, apply};
use tokio::sync::{mpsc, oneshot, watch};

use crate::clock::Clock;
use crate::control::log::{CommandLog, LogError, LogIndex, Recovered};
use crate::observe::ControlMetrics;
use crate::observe::codes::action_label;

/// How many commands the control node applies between snapshots, by default.
const DEFAULT_SNAPSHOT_EVERY: NonZeroU64 = NonZeroU64::new(1_000).expect("1000 is nonzero");

/// How many submitted commands may wait for the writer, by default. Callers beyond it wait.
const DEFAULT_MAILBOX: NonZeroUsize = NonZeroUsize::new(256).expect("256 is nonzero");

/// Where the control node runs its blocking SQLite calls (ADR-0013, ADR-0020).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[non_exhaustive]
pub enum StorageIo {
    /// On a dedicated thread, off the async executor. The default, for production.
    #[default]
    DedicatedThread,
    /// Inline, on the task that runs the writer. Use it under simulation, where a single
    /// simulation thread keeps runs deterministic.
    Inline,
}

/// How to run a control node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ControlOptions {
    config: ControlConfig,
    snapshot_every: NonZeroU64,
    mailbox: NonZeroUsize,
    storage_io: StorageIo,
    recorder: Option<NodeId>,
}

impl ControlOptions {
    /// Returns options that create new state under `config`, snapshot every 1,000 commands, queue
    /// up to 256 commands, and run SQLite on a dedicated thread.
    #[must_use]
    pub const fn new(config: ControlConfig) -> Self {
        Self {
            config,
            snapshot_every: DEFAULT_SNAPSHOT_EVERY,
            mailbox: DEFAULT_MAILBOX,
            storage_io: StorageIo::DedicatedThread,
            recorder: None,
        }
    }

    /// Stamps every command this node logs with `recorder`, its own node ID, beside the time it
    /// stamps (ADR-0047, point 6.1). Without one, commands carry no recorder, as before 0.8,
    /// and `apply` refuses a delegation.
    #[must_use]
    pub fn with_recorder(mut self, recorder: NodeId) -> Self {
        self.recorder = Some(recorder);
        self
    }

    /// Returns the node ID this node stamps on the commands it logs, if any.
    #[must_use]
    pub fn recorder(&self) -> Option<&NodeId> {
        self.recorder.as_ref()
    }

    /// Takes a snapshot after every `snapshot_every` commands.
    #[must_use]
    pub const fn with_snapshot_every(mut self, snapshot_every: NonZeroU64) -> Self {
        self.snapshot_every = snapshot_every;
        self
    }

    /// Lets up to `capacity` submitted commands wait for the writer.
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

    /// Returns the configuration a new log starts with. An existing log keeps its own.
    #[must_use]
    pub const fn config(&self) -> ControlConfig {
        self.config
    }

    /// Returns how many commands pass between snapshots.
    #[must_use]
    pub const fn snapshot_every(&self) -> NonZeroU64 {
        self.snapshot_every
    }
}

impl Default for ControlOptions {
    fn default() -> Self {
        Self::new(ControlConfig::new())
    }
}

/// The error returned when a control node can't open its log.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum OpenError {
    /// The log couldn't be opened or recovered.
    #[error("can't recover the control log")]
    Log(#[from] LogError),
    /// The writer thread couldn't be started.
    #[error("can't start the control writer thread")]
    Thread(#[source] io::Error),
    /// The writer stopped before it finished recovering.
    #[error("the control writer stopped during recovery")]
    Stopped,
}

/// The error returned when a command can't be committed.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum SubmitError {
    /// The writer has stopped, after a shutdown or a storage failure.
    #[error("the control node is not running")]
    Stopped,
    /// Logging the command failed. The command may or may not be durable; the writer has stopped
    /// and the node must restart to recover it.
    #[error("can't log the command")]
    Storage(#[source] LogError),
    /// The command was refused for software before it was logged ([`State::check_admission`],
    /// ADR-0038, point 4): a join whose release is outside the window or lacks an enabled
    /// feature, or a command that needs a fleet feature a member doesn't support. Nothing changed,
    /// and nothing was logged, so a control node of an older release, which would accept the
    /// command, never meets it in its log. A command that needs the `delegation` fleet feature
    /// is refused here, unlogged, for any rejection `apply` would give it (ADR-0047, point 6.2).
    #[error(transparent)]
    Refused(Rejection),
}

/// A command that was logged and applied.
#[derive(Debug, Clone)]
pub struct Committed {
    index: LogIndex,
    command: Command,
    outcome: Outcome,
    state: Arc<State>,
}

impl Committed {
    /// Returns the command's position in the log.
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

type Reply = oneshot::Sender<Result<Committed, SubmitError>>;

enum Mail {
    // Boxed: an action is large, and a stop request is small.
    Submit { action: Box<Action>, reply: Reply },
    Stop { done: oneshot::Sender<()> },
}

/// A cheap, cloneable handle that submits commands to the control node and reads its state.
#[derive(Debug, Clone)]
pub struct ControlHandle {
    mailbox: mpsc::Sender<Mail>,
    state: watch::Receiver<Arc<State>>,
}

impl ControlHandle {
    /// Stamps, logs, and applies `action`, and returns the result once it is durable.
    ///
    /// Commands from every handle apply one at a time, in log order; those queued together are
    /// logged with one sync. When the mailbox is full, this waits for room.
    ///
    /// # Errors
    ///
    /// [`SubmitError`] if the writer has stopped or can't log the command, or refused it before
    /// logging it. A command the state machine rejects is otherwise not an error here: it is
    /// logged, and its [`Committed::outcome`] holds the rejection. The exception is a command
    /// that needs the `delegation` fleet feature, whose rejection is [`SubmitError::Refused`]
    /// and is never logged (ADR-0047, point 6.2).
    pub async fn submit(&self, action: Action) -> Result<Committed, SubmitError> {
        let (reply, receive) = oneshot::channel();
        self.mailbox
            .send(Mail::Submit {
                action: Box::new(action),
                reply,
            })
            .await
            .map_err(|_| SubmitError::Stopped)?;
        receive.await.map_err(|_| SubmitError::Stopped)?
    }

    /// Returns the latest published state: the state after the last accepted command.
    #[must_use]
    pub fn state(&self) -> Arc<State> {
        Arc::clone(&self.state.borrow())
    }

    /// Returns a receiver that sees each newly published state.
    #[must_use]
    pub fn watch(&self) -> watch::Receiver<Arc<State>> {
        self.state.clone()
    }

    /// Whether the writer still takes commands: `false` once it stopped, after a shutdown or a
    /// storage failure.
    #[must_use]
    pub fn is_running(&self) -> bool {
        !self.mailbox.is_closed()
    }

    /// How many commands wait for the writer.
    #[must_use]
    pub fn queued(&self) -> usize {
        self.mailbox
            .max_capacity()
            .saturating_sub(self.mailbox.capacity())
    }
}

/// A running control node: its writer and a handle to it.
#[derive(Debug)]
pub struct ControlNode {
    handle: ControlHandle,
    recovered: RecoverySummary,
}

/// What a control node found when it opened its log.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecoverySummary {
    /// The index of the last logged command.
    pub last_index: LogIndex,
    /// The snapshot recovery started from, if any.
    pub snapshot: Option<LogIndex>,
    /// How many commands were replayed after the snapshot.
    pub replayed: u64,
}

impl ControlNode {
    /// Opens the command log at `path`, recovers the state, and starts the writer.
    ///
    /// # Errors
    ///
    /// [`OpenError`] if the log can't be opened or recovered, or the writer can't start.
    pub async fn open(
        path: impl Into<PathBuf>,
        options: ControlOptions,
        clock: impl Clock + 'static,
    ) -> Result<Self, OpenError> {
        Self::open_with_metrics(path, options, clock, ControlMetrics::default()).await
    }

    /// Opens the control node as [`ControlNode::open`] does, counting every command it logs in
    /// `metrics`, and timing every group's write.
    ///
    /// # Errors
    ///
    /// [`OpenError`] if the log can't be opened or recovered, or the writer can't start.
    pub(crate) async fn open_with_metrics(
        path: impl Into<PathBuf>,
        options: ControlOptions,
        clock: impl Clock + 'static,
        metrics: ControlMetrics,
    ) -> Result<Self, OpenError> {
        let path = path.into();
        let clock: Box<dyn Clock> = Box::new(clock);
        let (mailbox, inbox) = mpsc::channel(options.mailbox.get());
        match options.storage_io {
            StorageIo::Inline => {
                let (writer, state, recovered) = Writer::open(path, options, clock, metrics)?;
                tokio::spawn(async move { writer.run(inbox).await });
                Ok(Self::started(mailbox, state, recovered))
            }
            StorageIo::DedicatedThread => {
                let (ready, opened) = oneshot::channel();
                thread::Builder::new()
                    .name("fleetring-control".into())
                    .spawn(move || match Writer::open(path, options, clock, metrics) {
                        Ok((writer, state, recovered)) => {
                            if ready.send(Ok((state, recovered))).is_ok() {
                                writer.run_blocking(inbox);
                            }
                        }
                        Err(error) => {
                            // The opener is gone only if it was cancelled; nobody is left to tell.
                            let _unheard = ready.send(Err(error));
                        }
                    })
                    .map_err(OpenError::Thread)?;
                let (state, recovered) = opened.await.map_err(|_| OpenError::Stopped)??;
                Ok(Self::started(mailbox, state, recovered))
            }
        }
    }

    fn started(
        mailbox: mpsc::Sender<Mail>,
        state: watch::Receiver<Arc<State>>,
        recovered: RecoverySummary,
    ) -> Self {
        tracing::info!(
            last_index = recovered.last_index.get(),
            snapshot = ?recovered.snapshot.map(LogIndex::get),
            replayed = recovered.replayed,
            "control state recovered",
        );
        Self {
            handle: ControlHandle { mailbox, state },
            recovered,
        }
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

    /// Stops the writer after the commands already submitted, and closes the log.
    ///
    /// Commands submitted after this fail with [`SubmitError::Stopped`]. State watchers see their
    /// streams end.
    pub async fn shutdown(self) {
        let (done, stopped) = oneshot::channel();
        if self.handle.mailbox.send(Mail::Stop { done }).await.is_ok() {
            // An error means the writer already exited, which is what shutdown wants.
            let _stopped = stopped.await;
        }
    }
}

struct Writer {
    log: CommandLog,
    state: State,
    last_index: LogIndex,
    snapshot_every: NonZeroU64,
    clock: Box<dyn Clock>,
    recorder: Option<NodeId>,
    publish: watch::Sender<Arc<State>>,
    metrics: ControlMetrics,
}

/// What the writer does after handling one piece of mail.
enum Flow {
    Continue,
    Stop(Option<oneshot::Sender<()>>),
}

impl Writer {
    fn open(
        path: PathBuf,
        options: ControlOptions,
        clock: Box<dyn Clock>,
        metrics: ControlMetrics,
    ) -> Result<(Self, watch::Receiver<Arc<State>>, RecoverySummary), OpenError> {
        let log = CommandLog::open(path, options.config)?;
        if log.genesis() != options.config {
            tracing::warn!(
                configured = ?options.config,
                genesis = ?log.genesis(),
                "the control log keeps the timings it was created with",
            );
        }
        let Recovered {
            state,
            last_index,
            snapshot,
            replayed,
        } = log.recover()?;
        let (publish, watcher) = watch::channel(Arc::new(state.clone()));
        let summary = RecoverySummary {
            last_index,
            snapshot,
            replayed,
        };
        let writer = Self {
            log,
            state,
            last_index,
            snapshot_every: options.snapshot_every,
            clock,
            recorder: options.recorder,
            publish,
            metrics,
        };
        writer
            .metrics
            .recovered(last_index.get(), snapshot.map_or(0, LogIndex::get));
        Ok((writer, watcher, summary))
    }

    async fn run(mut self, mut inbox: mpsc::Receiver<Mail>) {
        while let Some(first) = inbox.recv().await {
            if let Flow::Stop(done) = self.handle_group(first, &mut inbox) {
                self.stop(done);
                return;
            }
        }
    }

    fn run_blocking(mut self, mut inbox: mpsc::Receiver<Mail>) {
        while let Some(first) = inbox.blocking_recv() {
            if let Flow::Stop(done) = self.handle_group(first, &mut inbox) {
                self.stop(done);
                return;
            }
        }
    }

    fn stop(self, done: Option<oneshot::Sender<()>>) {
        drop(self);
        if let Some(done) = done {
            // The caller may have stopped waiting; the writer is closed either way.
            let _unheard = done.send(());
        }
    }

    /// Handles `first` and everything queued behind it as one group. A stop request ends the
    /// group: the commands before it are committed, and the writer stops after them.
    fn handle_group(&mut self, first: Mail, inbox: &mut mpsc::Receiver<Mail>) -> Flow {
        let mut submits = Vec::new();
        let mut stop = None;
        let mut next = Some(first);
        while let Some(mail) = next.take() {
            match mail {
                Mail::Submit { action, reply } => submits.push((*action, reply)),
                Mail::Stop { done } => {
                    stop = Some(done);
                    break;
                }
            }
            next = inbox.try_recv().ok();
        }
        match (self.commit_group(submits), stop) {
            (Flow::Continue, None) => Flow::Continue,
            (Flow::Continue, Some(done)) => Flow::Stop(Some(done)),
            (Flow::Stop(_), stop) => Flow::Stop(stop),
        }
    }

    /// Returns `action` stamped with the time and, if this node has one, its recorder, as every
    /// command it logs is, its own commands included (ADR-0047, point 6.1).
    fn stamp(&self, action: Action) -> Command {
        match &self.recorder {
            Some(by) => Command::stamped(self.clock.now(), by.clone(), action),
            None => Command::new(self.clock.now(), action),
        }
    }

    /// Stamps and numbers `submits` in order, each with the time and the recorder, logs them in
    /// one transaction, then applies, publishes, and answers each. Nothing is answered before the whole group is durable.
    ///
    /// A command `apply` would refuse for software, a join or a command that needs a fleet
    /// feature some member lacks, is refused at once and never logged ([`State::check_admission`]).
    /// It is checked against the state the commands queued before it in the group leave, which is
    /// the state `apply` will see. A join or a leave that [`State::handover_due`] says would keep
    /// the first replica set is preceded by [`Action::EnableHandover`], logged by the writer
    /// itself in the same group.
    ///
    /// A command that needs the `delegation` fleet feature is answered with
    /// [`SubmitError::Refused`] and never logged when `apply` would refuse it on that state
    /// ([`refused_unlogged`]).
    fn commit_group(&mut self, submits: Vec<(Action, Reply)>) -> Flow {
        let mut commands: Vec<(LogIndex, Command)> = Vec::with_capacity(submits.len());
        let mut replies = Vec::with_capacity(submits.len());
        let mut last = self.last_index;
        let mut exhausted = None;
        // The state after the group's commands so far, built only once a command needs screening,
        // or, until `handover` is enabled, once a membership change comes.
        let mut preview: Option<State> = None;
        for (action, reply) in submits {
            let membership = matches!(action, Action::Join { .. } | Action::Leave { .. });
            let mut enable = false;
            if needs_screening(&action)
                || (membership && !self.state.is_enabled(fleetring_control::Feature::Handover))
            {
                let preview = preview.get_or_insert_with(|| {
                    let mut state = self.state.clone();
                    for (_, command) in &commands {
                        let _outcome = apply(&mut state, command.clone());
                    }
                    state
                });
                if needs_screening(&action)
                    && let Err(rejection) = preview.check_admission(&action)
                {
                    let _unheard = reply.send(Err(SubmitError::Refused(rejection)));
                    continue;
                }
                // The first membership change that would keep a replica set, once every member
                // supports `handover`, is preceded by the command that enables it, so that what
                // a change does depends on the log alone (ADR-0031, amendment 2).
                enable = preview.handover_due(&action);
            }
            if enable {
                let Some(index) = last.next() else {
                    exhausted = Some(reply);
                    break;
                };
                last = index;
                let command = self.stamp(Action::EnableHandover);
                if let Some(preview) = &mut preview {
                    let _outcome = apply(preview, command.clone());
                }
                commands.push((index, command));
                replies.push(None);
            }
            let unlogged_if_refused = refused_unlogged(&action);
            let command = self.stamp(action);
            if let Some(preview) = &mut preview {
                let outcome = apply(preview, command.clone());
                // The preview applied exactly the command `apply_logged` would, to exactly the
                // state it would see, so its refusal is the one the log would hold; a refusal
                // changes nothing, `State::now` included, so leaving it out replays the same.
                if unlogged_if_refused && let Err(rejection) = outcome {
                    let _unheard = reply.send(Err(SubmitError::Refused(rejection)));
                    continue;
                }
            }
            let Some(index) = last.next() else {
                exhausted = Some(reply);
                break;
            };
            last = index;
            commands.push((index, command));
            replies.push(Some(reply));
        }
        let started = tokio::time::Instant::now();
        let written = if commands.is_empty() {
            Ok(())
        } else {
            self.log
                .append_all(commands.iter().map(|(index, command)| (*index, command)))
        };
        if !commands.is_empty() && written.is_ok() {
            self.metrics.log_written(started.elapsed());
        }
        if let Err(error) = written {
            tracing::error!(error = %error, "control log write failed; stopping the writer");
            // The first waiter learns why; the rest see the writer stop.
            if let Some(reply) = replies.into_iter().flatten().next() {
                let _unheard = reply.send(Err(SubmitError::Storage(error)));
            }
            return Flow::Stop(None);
        }
        for ((index, command), reply) in commands.into_iter().zip(replies) {
            let committed = self.apply_logged(index, command);
            // A caller that gave up still had its command applied, as logged. The writer's own
            // commands have no caller.
            if let Some(reply) = reply {
                let _unheard = reply.send(Ok(committed));
            }
        }
        if let Some(reply) = exhausted {
            tracing::error!("control log index is exhausted; stopping the writer");
            let _unheard = reply.send(Err(SubmitError::Storage(LogError::Exhausted)));
            return Flow::Stop(None);
        }
        Flow::Continue
    }

    /// Applies the command logged at `index`, publishes the state, and snapshots when one is due.
    fn apply_logged(&mut self, index: LogIndex, command: Command) -> Committed {
        self.last_index = index;
        let action = action_label(&command.action);
        let pending = pending_forks(&self.state);
        let enabled: Vec<fleetring_control::Feature> = self.state.features().collect();
        let outcome = apply(&mut self.state, command.clone());
        let label = if outcome.is_ok() {
            "applied"
        } else {
            "rejected"
        };
        self.metrics.command(index.get(), action, label);
        if outcome.is_ok() {
            count_finished_forks(&self.state, &pending, &self.metrics);
            log_enabled_features(&self.state, &enabled, index);
        }
        let state = if outcome.is_ok() {
            let state = Arc::new(self.state.clone());
            self.publish.send_replace(Arc::clone(&state));
            state
        } else {
            // A rejection changes nothing, so the published state is still current.
            Arc::clone(&self.publish.borrow())
        };
        if index.get().is_multiple_of(self.snapshot_every.get()) {
            match self.log.save_snapshot(index, &self.state) {
                Ok(()) => {
                    self.metrics.snapshot_saved(index.get());
                    tracing::debug!(index = index.get(), "control snapshot saved");
                }
                // The log alone still recovers everything; the next snapshot tries again.
                Err(error) => tracing::warn!(error = %error, "can't save a control snapshot"),
            }
        }
        Committed {
            index,
            command,
            outcome,
            state,
        }
    }
}

/// The fork operations that are still pending in `state`.
fn pending_forks(state: &State) -> Vec<fleetring_control::fork::OperationId> {
    state
        .fork_operations()
        .filter(|(_, record)| !record.outcome().is_terminal())
        .map(|(id, _)| *id)
        .collect()
}

/// Counts each of `pending` that `state` shows at a final outcome, with how long a ready one
/// took from its request (ADR-0046, point 11), and logs it as an audit line.
fn count_finished_forks(
    state: &State,
    pending: &[fleetring_control::fork::OperationId],
    metrics: &crate::observe::ControlMetrics,
) {
    use fleetring_control::fork::{ForkState, OperationOutcome};
    for id in pending {
        let Some((_, record)) = state.fork_operations().find(|(known, _)| *known == id) else {
            continue;
        };
        let outcome = match record.outcome() {
            OperationOutcome::Ready { .. } => "ready",
            OperationOutcome::Failed { .. } => "failed",
            OperationOutcome::Cancelled { .. } => "cancelled",
            _ => continue,
        };
        let took = state
            .database(record.target())
            .and_then(|database| database.fork())
            .and_then(|fork| match fork.state() {
                ForkState::Ready { ready_at, .. } => Some(std::time::Duration::from_millis(
                    ready_at
                        .as_millis()
                        .saturating_sub(fork.requested_at().as_millis()),
                )),
                _ => None,
            });
        metrics.fork_finished(outcome, took);
        tracing::info!(target: "fleetringd::audit", operation = %id, fork = %record.target(), incarnation = %record.target_incarnation(), source = %record.intent().source, source_incarnation = %record.source_incarnation(), outcome, "fork finished");
    }
}

/// Logs, as an audit line, each fleet feature `state` has enabled beyond `before`: the command
/// at `index` enabled it for good, and from then on a release without it can't join, and
/// `upgrade --rollback` to one is refused (ADR-0038, point 4).
fn log_enabled_features(state: &State, before: &[fleetring_control::Feature], index: LogIndex) {
    for feature in state.features().filter(|feature| !before.contains(feature)) {
        tracing::info!(target: "fleetringd::audit", feature = feature.as_str(), index = index.get(), "fleet feature enabled for good: members without it can no longer join, and rolling back to a release without it is refused");
    }
}

/// Whether `action` may be refused for software, so the writer screens it before it logs it: a
/// join, or a command that needs any fleet feature.
fn needs_screening(action: &Action) -> bool {
    !action.features().is_empty() || matches!(action, Action::Join { .. })
}

/// Whether the writer answers `apply`'s refusal of `action` without logging it: a command that
/// needs the `delegation` fleet feature (ADR-0047, points 5.10 and 6.2).
///
/// An agent's token drives these, so a refusal, a rate limit's above all, must cost the log
/// nothing; and a refused command doesn't enable its feature, so logging one would leave a
/// command the previous release line can't decode in a log it may still roll back to
/// (ADR-0038, point 4). Every such command [`needs_screening`], so the preview exists.
fn refused_unlogged(action: &Action) -> bool {
    action
        .features()
        .contains(&fleetring_control::Feature::Delegation)
}

#[cfg(test)]
mod tests {
    use fleetring_control::Timestamp;

    use super::*;
    use crate::clock::SimClock;

    /// The writer stamps its own node ID beside the time on every command it logs, and a
    /// recovery replays the stamp from the log (ADR-0047, point 6.1); without a recorder it
    /// stamps none, as before 0.8.
    #[tokio::test(start_paused = true)]
    async fn control_writer_stamps_its_node_id() {
        let dir = tempfile::tempdir().expect("a directory");
        let path = dir.path().join("control.db");
        let base = Timestamp::from_millis(1_000);
        let options = ControlOptions::default()
            .with_storage_io(StorageIo::Inline)
            .with_recorder("control-1".parse().expect("an id"));
        assert_eq!(options.recorder().map(NodeId::as_str), Some("control-1"));
        let node = ControlNode::open(&path, options, SimClock::new(base))
            .await
            .expect("opens");
        let committed = node.handle().submit(Action::Tick).await.expect("commits");
        assert_eq!(
            committed.command().recorded_by.as_ref().map(NodeId::as_str),
            Some("control-1")
        );
        node.shutdown().await;

        let unstamped = ControlOptions::default().with_storage_io(StorageIo::Inline);
        let node = ControlNode::open(&path, unstamped, SimClock::new(base))
            .await
            .expect("reopens");
        assert_eq!(node.recovered().replayed, 1);
        let committed = node.handle().submit(Action::Tick).await.expect("commits");
        assert_eq!(committed.command().recorded_by, None);
        node.shutdown().await;

        let log = CommandLog::open(&path, ControlConfig::new()).expect("the log opens");
        let stamps: Vec<Option<String>> = log
            .commands()
            .expect("reads")
            .into_iter()
            .map(|(_, command)| command.recorded_by.map(|by| by.as_str().to_owned()))
            .collect();
        assert_eq!(stamps, [Some("control-1".to_owned()), None]);
    }

    /// The `EnableHandover` the writer logs before the first membership change that keeps a
    /// replica set carries the writer's recorder, as every command it logs does (ADR-0031,
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
        let options = ControlOptions::default()
            .with_storage_io(StorageIo::Inline)
            .with_recorder("control-1".parse().expect("an id"));
        let node = ControlNode::open(&path, options, SimClock::new(Timestamp::from_millis(1_000)))
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

        let log = CommandLog::open(&path, ControlConfig::new()).expect("the log opens");
        let commands: Vec<Command> = log
            .commands()
            .expect("reads")
            .into_iter()
            .map(|(_, command)| command)
            .collect();
        let enabled = commands
            .iter()
            .position(|command| matches!(command.action, Action::EnableHandover))
            .expect("the writer logged EnableHandover");
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

    /// A delegation `apply` refuses is answered `Refused` and never logged, so a refusal costs
    /// the log nothing and a refused first use leaves no command the previous release line can't
    /// decode while the feature stays off (ADR-0047, point 6.2; ADR-0038, point 4). The accepted
    /// delegations around it are logged as before, and the log alone replays to the state the
    /// writer published.
    #[tokio::test(start_paused = true)]
    async fn refused_delegations_are_never_logged() {
        use fleetring_control::Feature;
        use fleetring_control::delegation::{Limit, StructureViolation};

        use crate::control::test_support::{member_with_every_feature, root_delegation};

        let delegation = |now, n| root_delegation(now, n, 60_000);

        let dir = tempfile::tempdir().expect("a directory");
        let path = dir.path().join("control.db");
        let options = ControlOptions::default()
            .with_storage_io(StorageIo::Inline)
            .with_recorder("control-1".parse().expect("an id"));
        let clock = SimClock::new(Timestamp::from_millis(1_000_000));
        let node = ControlNode::open(&path, options, clock)
            .await
            .expect("opens");
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

        let log = CommandLog::open(&path, ControlConfig::new()).expect("the log opens");
        let commands = log.commands().expect("reads");
        let logged = commands
            .iter()
            .filter(|(_, command)| matches!(command.action, Action::RecordDelegation { .. }))
            .count();
        assert_eq!(logged, 31, "only the accepted delegations are logged");
        let mut replayed = State::new(log.genesis());
        for (_, command) in commands {
            let _outcome = apply(&mut replayed, command);
        }
        assert_eq!(replayed, *published);
    }
}
