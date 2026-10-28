//! The control node: one writer that stamps, logs, applies, and publishes every command.

use std::num::{NonZeroU64, NonZeroUsize};
use std::path::PathBuf;
use std::sync::Arc;
use std::{io, thread};

use fleetring_control::{Action, Command, ControlConfig, Outcome, State, apply};
use tokio::sync::{mpsc, oneshot, watch};

use crate::clock::Clock;
use crate::control::log::{CommandLog, LogError, LogIndex, Recovered};

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

enum Mail {
    Submit {
        action: Action,
        reply: oneshot::Sender<Result<Committed, SubmitError>>,
    },
    Stop {
        done: oneshot::Sender<()>,
    },
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
    /// Commands from every handle apply one at a time, in log order. When the mailbox is full,
    /// this waits for room.
    ///
    /// # Errors
    ///
    /// [`SubmitError`] if the writer has stopped or can't log the command. A command the state
    /// machine rejects is not an error here: it is logged, and its [`Committed::outcome`] holds
    /// the rejection.
    pub async fn submit(&self, action: Action) -> Result<Committed, SubmitError> {
        let (reply, receive) = oneshot::channel();
        self.mailbox
            .send(Mail::Submit { action, reply })
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
        let path = path.into();
        let clock: Box<dyn Clock> = Box::new(clock);
        let (mailbox, inbox) = mpsc::channel(options.mailbox.get());
        match options.storage_io {
            StorageIo::Inline => {
                let (writer, state, recovered) = Writer::open(path, options, clock)?;
                tokio::spawn(async move { writer.run(inbox).await });
                Ok(Self::started(mailbox, state, recovered))
            }
            StorageIo::DedicatedThread => {
                let (ready, opened) = oneshot::channel();
                thread::Builder::new()
                    .name("fleetring-control".into())
                    .spawn(move || match Writer::open(path, options, clock) {
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
        };
        Ok((writer, watcher, summary))
    }

    async fn run(mut self, mut inbox: mpsc::Receiver<Mail>) {
        while let Some(mail) = inbox.recv().await {
            if let Flow::Stop(done) = self.handle(mail) {
                self.stop(done);
                return;
            }
        }
    }

    fn run_blocking(mut self, mut inbox: mpsc::Receiver<Mail>) {
        while let Some(mail) = inbox.blocking_recv() {
            if let Flow::Stop(done) = self.handle(mail) {
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

    fn handle(&mut self, mail: Mail) -> Flow {
        match mail {
            Mail::Submit { action, reply } => match self.commit(action) {
                Ok(committed) => {
                    // A caller that gave up still had its command applied, as logged.
                    let _unheard = reply.send(Ok(committed));
                    Flow::Continue
                }
                Err(error) => {
                    tracing::error!(error = %error, "control log write failed; stopping the writer");
                    let _unheard = reply.send(Err(SubmitError::Storage(error)));
                    Flow::Stop(None)
                }
            },
            Mail::Stop { done } => Flow::Stop(Some(done)),
        }
    }

    /// Stamps, logs, applies, publishes, and snapshots one command.
    fn commit(&mut self, action: Action) -> Result<Committed, LogError> {
        let index = self.last_index.next().ok_or(LogError::Exhausted)?;
        let command = Command::new(self.clock.now(), action);
        self.log.append(index, &command)?;
        self.last_index = index;
        let outcome = apply(&mut self.state, command.clone());
        let state = if outcome.is_ok() {
            let state = Arc::new(self.state.clone());
            self.publish.send_replace(Arc::clone(&state));
            state
        } else {
            // A rejection changes nothing, so the published state is still current.
            Arc::clone(&self.publish.borrow())
        };
        if index.get() % self.snapshot_every.get() == 0 {
            match self.log.save_snapshot(index, &self.state) {
                Ok(()) => tracing::debug!(index = index.get(), "control snapshot saved"),
                // The log alone still recovers everything; the next snapshot tries again.
                Err(error) => tracing::warn!(error = %error, "can't save a control snapshot"),
            }
        }
        Ok(Committed {
            index,
            command,
            outcome,
            state,
        })
    }
}
