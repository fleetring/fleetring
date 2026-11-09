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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ControlOptions {
    config: ControlConfig,
    snapshot_every: NonZeroU64,
    mailbox: NonZeroUsize,
    storage_io: StorageIo,
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
        }
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
    /// command, never meets it in its log.
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
    /// [`SubmitError`] if the writer has stopped or can't log the command. A command the state
    /// machine rejects is not an error here: it is logged, and its [`Committed::outcome`] holds
    /// the rejection.
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

    /// Stamps and numbers `submits` in order, logs them in one transaction, then applies,
    /// publishes, and answers each. Nothing is answered before the whole group is durable.
    ///
    /// A command `apply` would refuse for software, a join or a command that needs a fleet
    /// feature some member lacks, is refused at once and never logged ([`State::check_admission`]).
    /// It is checked against the state the commands queued before it in the group leave, which is
    /// the state `apply` will see.
    fn commit_group(&mut self, submits: Vec<(Action, Reply)>) -> Flow {
        let mut commands: Vec<(LogIndex, Command)> = Vec::with_capacity(submits.len());
        let mut replies = Vec::with_capacity(submits.len());
        let mut last = self.last_index;
        let mut exhausted = None;
        // The state after the group's commands so far, built only once a command needs screening.
        let mut preview: Option<State> = None;
        for (action, reply) in submits {
            if needs_screening(&action) {
                let preview = preview.get_or_insert_with(|| {
                    let mut state = self.state.clone();
                    for (_, command) in &commands {
                        let _outcome = apply(&mut state, command.clone());
                    }
                    state
                });
                if let Err(rejection) = preview.check_admission(&action) {
                    let _unheard = reply.send(Err(SubmitError::Refused(rejection)));
                    continue;
                }
            }
            let Some(index) = last.next() else {
                exhausted = Some(reply);
                break;
            };
            last = index;
            let command = Command::new(self.clock.now(), action);
            if let Some(preview) = &mut preview {
                let _outcome = apply(preview, command.clone());
            }
            commands.push((index, command));
            replies.push(reply);
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
            if let Some(reply) = replies.into_iter().next() {
                let _unheard = reply.send(Err(SubmitError::Storage(error)));
            }
            return Flow::Stop(None);
        }
        for ((index, command), reply) in commands.into_iter().zip(replies) {
            let committed = self.apply_logged(index, command);
            // A caller that gave up still had its command applied, as logged.
            let _unheard = reply.send(Ok(committed));
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
        let outcome = apply(&mut self.state, command.clone());
        let label = if outcome.is_ok() {
            "applied"
        } else {
            "rejected"
        };
        self.metrics.command(index.get(), action, label);
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

/// Whether `action` may be refused for software, so the writer screens it before it logs it: a
/// join, or a command that needs any fleet feature.
fn needs_screening(action: &Action) -> bool {
    !action.features().is_empty() || matches!(action, Action::Join { .. })
}
