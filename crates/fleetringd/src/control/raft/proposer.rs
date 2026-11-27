//! The proposer: screens, stamps, and proposes control commands on the control group's leader
//! (ADR-0040 point 2).
//!
//! One task per control node takes submissions from a bounded mailbox, in arrival order. It
//! accepts them only while this node leads a term and is *ready*: its state machine has applied
//! an entry of that term, so it has applied every entry its log held when it took over. At
//! readiness it clones the applied state into its screening state, and from then on screens each
//! command against the state its own proposals will reach, through the shared [`Screen`]: what
//! the screen refuses is answered and never proposed, an enabling command the screen adds is
//! proposed just before the command that needs it, and a resend of a recorded delegation is
//! answered from the applied state once every entry proposed before it has been applied.
//!
//! Every proposal carries the leader it was screened under, in its entry and as openraft's
//! expected leader, so an entry that a later leader appends anyway is skipped by every state
//! machine alike. The commands queued together go to openraft as one batch, appended with one
//! sync, and one batch is in flight at a time: the proposer screens the next batch only once
//! openraft has answered every proposal of the previous one (ADR-0040, amendment 1). openraft
//! refuses a write on a leader whose lease has expired while that leader stays leader in the same
//! term, so a batch screened before an earlier batch's answer arrived could be screened against a
//! state holding a command that never took effect. Submissions keep queuing in the mailbox
//! meanwhile, and form the next batch.

use std::sync::Arc;
#[cfg(test)]
use std::sync::atomic::AtomicBool;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use fleetring::NodeId;
use fleetring_control::delegation::Resent;
use fleetring_control::fork::ForkLimits;
use fleetring_control::{Action, Applied, Command, Millis, State, apply};
use openraft::errors::ClientWriteError;
use openraft::impls::ProgressResponder;
use openraft::impls::leader_id_adv::LeaderId;
use openraft::rt::watch::WatchReceiver;
use openraft::type_config::alias::{WatchReceiverOf, WriteResponderOf};
use openraft::{RaftMetrics, ServerState};
use tokio::sync::{Notify, mpsc, oneshot, watch};
use tokio::task::JoinHandle;
use tokio::time::Instant;

use crate::clock::Clock;
use crate::control::format1::{LogError, LogIndex};
use crate::control::node::{Committed, SubmitError};
use crate::control::raft::group::GroupGate;
use crate::control::raft::state_machine::{AppliedState, ControlStateMachine};
use crate::control::raft::{
    ControlPeer, ControlRequest, ControlResponse, ControlTypes, LeaderRef, RaftId,
};
use crate::control::screen::{Screen, Screened, Settings, Verdict};
use crate::observe::ControlMetrics;

/// The control group's Raft, with its state machine.
pub(crate) type ControlRaft = openraft::Raft<ControlTypes, ControlStateMachine>;

/// What a submitter waits for.
pub(crate) type Reply = oneshot::Sender<Result<Committed, SubmitError>>;

/// A piece of mail for the proposer.
pub(crate) enum Mail {
    /// A command to screen and propose. Boxed: an action is large, and a stop request is small.
    Submit {
        /// The action.
        action: Box<Action>,
        /// The leader the submitter acts under, if it acts only under one: the leader's timers,
        /// whose commands mustn't reach the log of a term whose settle window hasn't passed.
        under: Option<LeaderRef>,
        /// Where its answer goes.
        reply: Reply,
    },
    /// Stop the proposer and the group, after the submissions before it.
    Stop {
        /// Told once everything has stopped.
        done: oneshot::Sender<()>,
    },
}

/// Whether this node may propose, and the leader it would screen under (ADR-0040 point 2).
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Readiness {
    /// Another node leads the control group, or none is known.
    Follower {
        /// The leader, if this node knows it.
        leader: Option<ControlPeer>,
    },
    /// This node leads the control group's `term`, but hasn't applied an entry of it yet.
    CatchingUp {
        /// The control group's term.
        term: u64,
    },
    /// This node leads, has applied an entry of its term, and screens under `leader`.
    Ready {
        /// The control group's term and this node's Raft ID.
        leader: LeaderRef,
    },
}

impl Readiness {
    /// Whether this node is a ready leader.
    #[must_use]
    pub fn is_ready(&self) -> bool {
        matches!(self, Self::Ready { .. })
    }

    /// The error a submission gets while this node isn't a ready leader, naming `own` as the
    /// leader that is catching up.
    fn refusal(&self) -> SubmitError {
        match self {
            Self::Follower { leader } => SubmitError::NotLeader {
                leader: leader.as_ref().map(|peer| peer.address.clone()),
            },
            // No hint: it would name this node, and a client would send the call straight back.
            Self::CatchingUp { .. } | Self::Ready { .. } => {
                SubmitError::CatchingUp { leader: None }
            }
        }
    }
}

/// What the proposer stamps and compares: this node's identity and the per-node settings that
/// leak into commands (ADR-0040 point 13).
#[derive(Debug, Clone)]
pub(crate) struct ProposerSettings {
    /// This node's fleet node ID, stamped as `recorded_by` on every command.
    pub(crate) own: NodeId,
    /// This node's peer: its node ID and address.
    pub(crate) peer: ControlPeer,
    /// The tombstone window this node logs before a drop.
    pub(crate) tombstone_window: Millis,
    /// The `[fork]` limits this node records with each fork it accepts, if it knows them.
    pub(crate) fork_limits: Option<ForkLimits>,
    /// How long a batch may wait for openraft's answer before submissions behind it are refused
    /// rather than held: four shortest election timeouts.
    pub(crate) stall_after: Duration,
    /// The group gate the proposals pass (ADR-0040 point 4).
    pub(crate) gate: GroupGate,
}

/// The screening state of a ready leader's term.
#[derive(Debug)]
struct Screening {
    /// The leader the proposals are screened under.
    leader: LeaderRef,
    /// The applied state at readiness, or when the screening state was last cloned again, with
    /// every proposal since applied to it.
    state: State,
    /// Dropped once the last proposal so far has been answered.
    last: Option<watch::Receiver<()>>,
    /// The generation of the screening state, which a proposal answered other than `Applied`
    /// marks as poisoned.
    generation: u64,
}

/// Faults a test injects into a control node's proposer.
#[cfg(test)]
#[derive(Debug, Default)]
pub(crate) struct Faults {
    /// How many of the leader timers' submissions to answer `NotLeader` without screening them.
    pub(crate) refuse: AtomicUsize,
    /// How many proposals to screen under another term than the one they're written in, so the
    /// state machine supersedes them, as it would an entry a truncation left in another term.
    pub(crate) supersede: AtomicUsize,
    /// Whether waiters hold their answers back until `release` is notified.
    pub(crate) hold: AtomicBool,
    /// Lets held answers go.
    pub(crate) release: Notify,
}

/// Takes one from `counter`, if it isn't zero.
#[cfg(test)]
fn take(counter: &AtomicUsize) -> bool {
    let mut current = counter.load(Ordering::SeqCst);
    while current > 0 {
        match counter.compare_exchange(current, current - 1, Ordering::SeqCst, Ordering::SeqCst) {
            Ok(_) => return true,
            Err(now) => current = now,
        }
    }
    false
}

/// Screens, stamps, and proposes control commands on the leader (ADR-0040 point 2).
pub(crate) struct Proposer {
    raft: ControlRaft,
    clock: Box<dyn Clock>,
    own_raft: RaftId,
    peer: ControlPeer,
    readiness: watch::Sender<Readiness>,
    gate: GroupGate,
    /// The control group's term this node last saw; a new one forgets what members reported.
    term: Option<u64>,
    settings: Settings,
    tombstone_window: Millis,
    fork_limits: Option<ForkLimits>,
    stall_after: Duration,
    /// When the batch in flight went to openraft.
    batch_started: Option<Instant>,
    metrics: ControlMetrics,
    in_flight: Arc<AtomicUsize>,
    /// Notified each time a proposal stops counting as in flight.
    answered: Arc<Notify>,
    screening: Option<Screening>,
    /// The last generation handed out.
    generations: u64,
    /// The generation a proposal answered other than `Applied` poisoned; 0 for none.
    poisoned: Arc<AtomicU64>,
    #[cfg(test)]
    faults: Arc<Faults>,
}

impl std::fmt::Debug for Proposer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Proposer")
            .field("peer", &self.peer)
            .field("readiness", &*self.readiness.borrow())
            .finish_non_exhaustive()
    }
}

/// What a running proposer exposes.
#[derive(Debug)]
pub(crate) struct Running {
    /// Whether this node may propose.
    pub(crate) readiness: watch::Receiver<Readiness>,
    /// How many proposals wait for their entries to be applied.
    pub(crate) in_flight: Arc<AtomicUsize>,
    /// The proposer's task.
    pub(crate) task: JoinHandle<()>,
    /// The faults a test injects.
    #[cfg(test)]
    pub(crate) faults: Arc<Faults>,
}

impl Proposer {
    /// Starts the proposer of `raft`, taking submissions from `inbox`, stamping them with `clock`
    /// and `settings`, and reading readiness from openraft's metrics and the state machine's
    /// `applied` state.
    pub(crate) fn spawn(
        raft: ControlRaft,
        clock: Box<dyn Clock>,
        settings: ProposerSettings,
        applied: watch::Receiver<AppliedState>,
        metrics: ControlMetrics,
        inbox: mpsc::Receiver<Mail>,
        stopping: watch::Receiver<bool>,
    ) -> Running {
        let (readiness, readiness_receiver) = watch::channel(Readiness::Follower { leader: None });
        let in_flight = Arc::new(AtomicUsize::new(0));
        #[cfg(test)]
        let faults = Arc::new(Faults::default());
        let proposer = Self {
            own_raft: crate::control::raft::raft_id(settings.own.as_str()),
            raft,
            clock,
            peer: settings.peer,
            readiness,
            gate: settings.gate,
            term: None,
            settings: Settings::new(Some(settings.own), settings.tombstone_window),
            tombstone_window: settings.tombstone_window,
            fork_limits: settings.fork_limits,
            stall_after: settings.stall_after,
            batch_started: None,
            metrics,
            in_flight: Arc::clone(&in_flight),
            answered: Arc::new(Notify::new()),
            screening: None,
            generations: 0,
            poisoned: Arc::new(AtomicU64::new(0)),
            #[cfg(test)]
            faults: Arc::clone(&faults),
        };
        let task = tokio::spawn(proposer.run(inbox, applied, stopping));
        Running {
            readiness: readiness_receiver,
            in_flight,
            task,
            #[cfg(test)]
            faults,
        }
    }

    async fn run(
        mut self,
        mut inbox: mpsc::Receiver<Mail>,
        mut applied: watch::Receiver<AppliedState>,
        mut stopping: watch::Receiver<bool>,
    ) {
        let mut raft_metrics = self.raft.metrics();
        self.refresh(&raft_metrics, &applied);
        loop {
            // One batch in flight: while a ready leader waits for openraft to answer a proposal,
            // submissions wait in the mailbox. A node that isn't a ready leader screens nothing,
            // so it answers them at once, whatever it still waits for.
            let idle = self.in_flight.load(Ordering::SeqCst) == 0;
            if idle {
                // Only this task hands batches to openraft, so with nothing in flight every batch
                // so far has been answered, and changed reports settle.
                self.gate.group().idle();
            }
            let taking = idle || self.screening.is_none();
            // A batch openraft hasn't answered within four election timeouts, as on a leader cut
            // off from its quorum that still believes it leads: what queues behind it is refused
            // at once, unscreened, so clients try another control node rather than wait.
            let stalled_at = self
                .batch_started
                .map_or_else(Instant::now, |started| started + self.stall_after);
            let stalled = !taking && Instant::now() >= stalled_at;
            tokio::select! {
                biased;
                changed = raft_metrics.changed() => {
                    if changed.is_err() {
                        break;
                    }
                    self.refresh(&raft_metrics, &applied);
                }
                changed = applied.changed() => {
                    if changed.is_err() {
                        break;
                    }
                    self.refresh(&raft_metrics, &applied);
                }
                () = self.answered.notified(), if !taking => {}
                () = tokio::time::sleep_until(stalled_at), if !taking && !stalled => {}
                // A stop request queued behind waiting submissions can't be read yet, so a
                // shutdown also says so here; stopping the group then answers what is in flight.
                Ok(()) = async { stopping.wait_for(|stop| *stop).await.map(drop) }, if !taking => {
                    break;
                }
                mail = inbox.recv(), if taking || stalled => {
                    let Some(first) = mail else { break };
                    if stalled {
                        match first {
                            Mail::Submit { reply, .. } => {
                                let refused = SubmitError::CatchingUp { leader: None };
                                let _unheard = reply.send(Err(refused));
                                continue;
                            }
                            Mail::Stop { done } => {
                                self.stop().await;
                                let _unheard = done.send(());
                                return;
                            }
                        }
                    }
                    let (batch, stop) = gather(first, &mut inbox);
                    if !batch.is_empty() {
                        self.propose(batch, &applied).await;
                    }
                    if let Some(done) = stop {
                        self.stop().await;
                        let _unheard = done.send(());
                        return;
                    }
                }
            }
        }
        self.stop().await;
        // What still waits in the mailbox is answered `Stopped`, and a stop request among it is
        // told the proposer has stopped.
        inbox.close();
        while let Some(mail) = inbox.recv().await {
            match mail {
                Mail::Submit { reply, .. } => {
                    let _unheard = reply.send(Err(SubmitError::Stopped));
                }
                Mail::Stop { done } => {
                    let _unheard = done.send(());
                }
            }
        }
    }

    async fn stop(&mut self) {
        self.screening = None;
        self.readiness
            .send_replace(Readiness::Follower { leader: None });
        if let Err(error) = self.raft.shutdown().await {
            tracing::warn!(%error, "the control group's Raft didn't stop cleanly");
        }
    }

    /// Recomputes readiness from openraft's metrics and the state machine's applied entry, and
    /// drops or builds the screening state to match.
    fn refresh(
        &mut self,
        raft_metrics: &WatchReceiverOf<ControlTypes, RaftMetrics<ControlTypes>>,
        applied: &watch::Receiver<AppliedState>,
    ) {
        let readiness = {
            let metrics = raft_metrics.borrow_watched();
            let term = metrics.current_term;
            // What members reported belongs to the term it was heard in (ADR-0040 point 4).
            if self.term != Some(term) {
                self.term = Some(term);
                self.gate.group().clear();
            }
            self.gate.group().set_members(
                metrics
                    .membership_config
                    .membership()
                    .nodes()
                    .map(|(id, peer)| (*id, peer.clone()))
                    .collect(),
            );
            let leads = metrics.current_leader == Some(self.own_raft)
                && metrics.state == ServerState::Leader;
            if leads {
                let applied = applied.borrow();
                let own_term = applied.log_id.is_some_and(|log_id| {
                    log_id.leader_id.term == term && log_id.leader_id.node_id == self.own_raft
                });
                if own_term {
                    Readiness::Ready {
                        leader: LeaderRef {
                            term,
                            node: self.own_raft,
                        },
                    }
                } else {
                    Readiness::CatchingUp { term }
                }
            } else {
                Readiness::Follower {
                    leader: metrics.current_leader.and_then(|leader| {
                        metrics
                            .membership_config
                            .membership()
                            .get_node(&leader)
                            .cloned()
                    }),
                }
            }
        };
        match (&readiness, &self.screening) {
            (Readiness::Ready { leader }, Some(screening)) if screening.leader == *leader => {}
            (Readiness::Ready { leader }, _) => {
                let state = State::clone(&applied.borrow().state);
                self.warn_settings(&state);
                tracing::info!(leader = %leader, "control leader ready");
                self.generations += 1;
                self.screening = Some(Screening {
                    leader: *leader,
                    state,
                    last: None,
                    generation: self.generations,
                });
            }
            (Readiness::Follower { .. } | Readiness::CatchingUp { .. }, _) => {
                self.screening = None;
            }
        }
        self.readiness.send_if_modified(|current| {
            if *current == readiness {
                false
            } else {
                *current = readiness;
                true
            }
        });
    }
    /// Logs one warning for each per-node setting that control state records differently from
    /// this node's configuration (ADR-0040 point 13).
    fn warn_settings(&self, state: &State) {
        for warning in setting_warnings(state, self.tombstone_window, self.fork_limits) {
            tracing::warn!("{warning}");
        }
    }

    /// Whether the screening state can be screened against: it exists. A screening state that a
    /// proposal answered other than `Applied` poisoned is cloned again from the applied state
    /// first: with one batch in flight, every proposal made from it has been answered by then,
    /// so the applied state holds exactly what took effect.
    fn usable(&mut self, applied: &watch::Receiver<AppliedState>) -> bool {
        let poisoned = self.poisoned.load(Ordering::SeqCst);
        let Some(screening) = self.screening.as_mut() else {
            return false;
        };
        if poisoned != screening.generation {
            return true;
        }
        debug_assert_eq!(
            self.in_flight.load(Ordering::SeqCst),
            0,
            "a batch was taken while another was in flight"
        );
        self.generations += 1;
        screening.state = State::clone(&applied.borrow().state);
        screening.last = None;
        screening.generation = self.generations;
        tracing::info!(
            leader = %screening.leader,
            "a proposal didn't take effect; the control leader screens from its applied state again"
        );
        true
    }

    /// Screens, stamps, and proposes `batch` in order, as one openraft batch.
    async fn propose(
        &mut self,
        batch: Vec<(Action, Option<LeaderRef>, Reply)>,
        applied: &watch::Receiver<AppliedState>,
    ) {
        if !self.usable(applied) {
            let readiness = self.readiness.borrow().clone();
            for (_, _, reply) in batch {
                let _unheard = reply.send(Err(readiness.refusal()));
            }
            return;
        }
        let Some(screening) = self.screening.as_mut() else {
            return;
        };
        let leader = screening.leader;
        let mut last = screening.last.take();
        let mut accepted: Vec<Command> = Vec::new();
        let mut writes: Vec<(ControlRequest, Option<WriteResponderOf<ControlTypes>>)> = Vec::new();
        let waiter = Waiter {
            metrics: self.metrics.clone(),
            in_flight: Arc::clone(&self.in_flight),
            answered: Arc::clone(&self.answered),
            poisoned: Arc::clone(&self.poisoned),
            generation: screening.generation,
            #[cfg(test)]
            faults: Arc::clone(&self.faults),
        };
        let preview = {
            let mut screen = Screen::gated(&screening.state, &self.settings, &self.gate);
            for (action, under, reply) in batch {
                // A timer's command for another term than this one is refused: that term's
                // settle window may not have passed (ADR-0040 point 6).
                if under.is_some_and(|under| under != leader) {
                    let _unheard = reply.send(Err(SubmitError::NotLeader { leader: None }));
                    continue;
                }
                #[cfg(test)]
                if under.is_some() && take(&self.faults.refuse) {
                    let _unheard = reply.send(Err(SubmitError::NotLeader { leader: None }));
                    continue;
                }
                let Screened { enabling, verdict } =
                    screen.screen(action, self.clock.now(), &accepted);
                if let Some(enabling) = enabling {
                    accepted.push(enabling.clone());
                    if let Some(write) = waiter.entry(leader, enabling, None, &mut last) {
                        writes.push(write);
                    }
                }
                match verdict {
                    Verdict::Append(command) => {
                        accepted.push(command.clone());
                        if let Some(write) = waiter.entry(leader, command, Some(reply), &mut last) {
                            writes.push(write);
                        }
                    }
                    Verdict::Refused(rejection) => {
                        self.metrics.proposal("refused");
                        let _unheard = reply.send(Err(SubmitError::Refused(rejection)));
                    }
                    Verdict::Resent { command } => {
                        answer_resend(command, reply, last.clone(), applied.clone());
                    }
                }
            }
            screen.into_preview()
        };
        match preview {
            Some(state) => screening.state = state,
            None => {
                for command in &accepted {
                    let _outcome = apply(&mut screening.state, command.clone());
                }
            }
        }
        screening.last = last;
        if writes.is_empty() {
            return;
        }
        self.gate.group().proposing();
        self.batch_started = Some(Instant::now());
        let expected = LeaderId {
            term: leader.term,
            node_id: leader.node,
        };
        if let Err(fatal) = self.raft.write_many(writes).with_leader(expected).await {
            // The responders went with the batch, so every waiter answers `Stopped`.
            tracing::warn!(%fatal, "the control group's Raft stopped before a batch reached it");
        }
    }
}

/// Takes `first` and every piece of mail already queued behind it: the submissions, up to a
/// stop request, and the stop request if there is one.
#[expect(
    clippy::type_complexity,
    reason = "a batch and a stop request, taken apart at the one call site"
)]
fn gather(
    first: Mail,
    inbox: &mut mpsc::Receiver<Mail>,
) -> (
    Vec<(Action, Option<LeaderRef>, Reply)>,
    Option<oneshot::Sender<()>>,
) {
    let mut batch = Vec::new();
    let mut next = Some(first);
    while let Some(mail) = next.take() {
        match mail {
            Mail::Submit {
                action,
                under,
                reply,
            } => batch.push((*action, under, reply)),
            Mail::Stop { done } => return (batch, Some(done)),
        }
        next = inbox.try_recv().ok();
    }
    (batch, None)
}

/// Starts the tasks that wait for proposals' entries to be applied.
#[derive(Debug, Clone)]
struct Waiter {
    metrics: ControlMetrics,
    in_flight: Arc<AtomicUsize>,
    /// Notified once a proposal stops counting as in flight.
    answered: Arc<Notify>,
    /// Set to `generation` when a proposal is answered other than `Applied`.
    poisoned: Arc<AtomicU64>,
    /// The generation of the screening state the proposals were screened against.
    generation: u64,
    #[cfg(test)]
    faults: Arc<Faults>,
}

impl Waiter {
    /// Encodes `command` as an entry screened under `leader`, and starts the task that answers
    /// `reply`, if any, once openraft answers the entry. `last` becomes the entry's signal. A
    /// command that can't be encoded is answered at once, and poisons the screening state, which
    /// already holds it.
    fn entry(
        &self,
        leader: LeaderRef,
        command: Command,
        reply: Option<Reply>,
        last: &mut Option<watch::Receiver<()>>,
    ) -> Option<(ControlRequest, Option<WriteResponderOf<ControlTypes>>)> {
        #[cfg(test)]
        let leader = if take(&self.faults.supersede) {
            LeaderRef {
                term: leader.term + 1,
                ..leader
            }
        } else {
            leader
        };
        let request = match ControlRequest::new(leader, command.clone()) {
            Ok(request) => request,
            Err(source) => {
                tracing::error!(%source, "a screened command couldn't be encoded");
                self.poisoned.store(self.generation, Ordering::SeqCst);
                if let Some(reply) = reply {
                    let _unheard = reply.send(Err(SubmitError::Storage(LogError::Encode {
                        what: "a command",
                        source,
                    })));
                }
                return None;
            }
        };
        let (responder, answered): (WriteResponderOf<ControlTypes>, _) =
            ProgressResponder::complete_only();
        let (done, signal) = watch::channel(());
        *last = Some(signal);
        self.in_flight.fetch_add(1, Ordering::SeqCst);
        let waiter = self.clone();
        tokio::spawn(async move {
            let answered = answered.await;
            #[cfg(test)]
            if waiter.faults.hold.load(Ordering::SeqCst) {
                waiter.faults.release.notified().await;
            }
            let answer = match answered {
                Err(_) => Err(SubmitError::Stopped),
                Ok(Ok(response)) => match response.data {
                    ControlResponse::Applied { outcome, state } => Ok(Committed::new(
                        LogIndex::new(response.log_id.index),
                        command,
                        outcome,
                        state,
                    )),
                    ControlResponse::Superseded | ControlResponse::Raft => {
                        Err(SubmitError::Superseded)
                    }
                },
                Ok(Err(ClientWriteError::ForwardToLeader(forward))) => {
                    Err(SubmitError::NotLeader {
                        leader: forward.leader_node.map(|peer| peer.address),
                    })
                }
                // Truncated by a later leader, or purged behind a snapshot: another leader may
                // still commit it, so its outcome is unknown (ADR-0040, amendment 1).
                Ok(Err(ClientWriteError::LogEntryDiscarded(_))) => Err(SubmitError::Unknown),
                Ok(Err(
                    ClientWriteError::ChangeMembershipError(_)
                    | ClientWriteError::PreconditionFailed(_),
                )) => Err(SubmitError::Superseded),
            };
            let outcome = match &answer {
                Ok(_) => Some("committed"),
                Err(SubmitError::Superseded) => Some("superseded"),
                Err(SubmitError::NotLeader { .. } | SubmitError::Unknown) => Some("not_leader"),
                Err(
                    SubmitError::Stopped
                    | SubmitError::Storage(_)
                    | SubmitError::Refused(_)
                    | SubmitError::CatchingUp { .. },
                ) => None,
            };
            if let Some(outcome) = outcome {
                waiter.metrics.proposal(outcome);
            }
            // Anything but `Applied` means the screening state holds a command that took no
            // effect, or whose effect is unknown: the proposer clones it again from the applied
            // state before it screens more. Marked before the proposal stops counting as in
            // flight, so the proposer never clones before it sees the mark.
            if answer.is_err() {
                waiter.poisoned.store(waiter.generation, Ordering::SeqCst);
            }
            if let Some(reply) = reply {
                let _unheard = reply.send(answer);
            }
            waiter.in_flight.fetch_sub(1, Ordering::SeqCst);
            waiter.answered.notify_one();
            // Last, so a resend waiting on this entry is answered after its submitter.
            drop(done);
        });
        Some((request, Some(responder)))
    }
}

/// Answers a resend of a recorded delegation once `last`, the last entry proposed before it, has
/// been answered, from the state then applied: the record, [`Resent::Yes`], or
/// [`SubmitError::Superseded`] if the applied state lacks it, because the entry that held it was
/// superseded or truncated (ADR-0040 point 2, ADR-0047).
fn answer_resend(
    command: Command,
    reply: Reply,
    last: Option<watch::Receiver<()>>,
    applied: watch::Receiver<AppliedState>,
) {
    tokio::spawn(async move {
        if let Some(mut last) = last {
            while last.changed().await.is_ok() {}
        }
        let applied = applied.borrow().clone();
        let answer = match applied.state.delegation_resend(&command.action, command.at) {
            Some(Ok((id, record))) => {
                let outcome = Ok(Applied::DelegationRecorded {
                    id: id.clone(),
                    claims: record.claims.clone(),
                    resent: Resent::Yes,
                });
                Ok(Committed::new(
                    LogIndex::new(applied.index()),
                    command,
                    outcome,
                    Arc::clone(&applied.state),
                ))
            }
            Some(Err(_)) | None => Err(SubmitError::Superseded),
        };
        let _unheard = reply.send(answer);
    });
}

/// One warning for each per-node setting that `state` records differently from this node's
/// configuration, naming both values (ADR-0040 point 13): the tombstone window, and the `[fork]`
/// limits of the newest fork `state` holds.
fn setting_warnings(
    state: &State,
    tombstone_window: Millis,
    fork_limits: Option<ForkLimits>,
) -> Vec<String> {
    let mut warnings = Vec::new();
    if let Some(window) = state.tombstones().window()
        && window != tombstone_window
    {
        warnings.push(format!(
            "control state's tombstone window ({} ms) differs from this node's [control] \
             tombstone_window_ms ({} ms); this node sets its own before its next drop, so set it \
             equally on every control node",
            window.get(),
            tombstone_window.get(),
        ));
    }
    let newest = state
        .databases()
        .filter_map(|database| database.fork())
        .max_by_key(|fork| fork.requested_at());
    if let (Some(fork), Some(configured)) = (newest, fork_limits)
        && fork.limits() != configured
    {
        warnings.push(format!(
            "the [fork] limits of control state's newest fork ({:?}) differ from this node's \
             ({configured:?}); this node records its own with each fork it accepts, so set them \
             equally on every control node",
            fork.limits(),
        ));
    }
    warnings
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};
    use std::sync::atomic::AtomicU64;

    use fleetring_control::delegation::Resent;
    use fleetring_control::{
        Applied, ControlConfig, Feature, Member, Rejection, Release, Software, Timestamp,
    };

    use super::*;
    use crate::clock::SimClock;
    use crate::control::test_support::{
        logged_commands, member_with_every_feature, open_node, own, root_delegation,
    };
    use crate::control::{Bootstrap, ControlOptions, StorageIo};

    fn options() -> ControlOptions {
        ControlOptions::default().with_storage_io(StorageIo::Inline)
    }

    /// A clock that reads one millisecond later on every call, so stamps show the order the
    /// proposer screened in.
    #[derive(Debug, Default)]
    struct TickClock(AtomicU64);

    impl Clock for TickClock {
        fn now(&self) -> Timestamp {
            Timestamp::from_millis(self.0.fetch_add(1, Ordering::SeqCst) + 1)
        }
    }

    fn software(release: Release) -> Software {
        Feature::ALL
            .into_iter()
            .fold(Software::new(release, 1), Software::with_feature)
    }

    fn join(name: &str, release: Release) -> Action {
        Action::Join {
            member: Member::new(name.parse().expect("an id")).with_software(software(release)),
        }
    }

    /// A node waiting to join a group leads nothing, so its proposer refuses every command; once
    /// it leads a term of its own and has applied an entry of it, the proposer screens again.
    #[tokio::test(start_paused = true)]
    async fn the_proposer_refuses_until_its_term_is_applied() {
        let dir = tempfile::tempdir().expect("a directory");
        let node = open_node(
            dir.path().join("control.db"),
            options().with_bootstrap(Bootstrap::Join),
            SimClock::new(Timestamp::ZERO),
        )
        .await
        .expect("opens");
        let handle = node.handle();
        let refused = handle.submit(Action::Tick).await;
        assert!(
            matches!(refused, Err(SubmitError::NotLeader { leader: None })),
            "{refused:?}"
        );
        let peer = own();
        let id = crate::control::raft::raft_id(&peer.node_id);
        let membership = crate::control::raft::Membership::new(
            vec![BTreeSet::from([id])],
            BTreeMap::from([(id, peer)]),
        )
        .expect("a membership");
        handle
            .raft()
            .initialize(membership)
            .await
            .expect("initialized");
        handle.raft().trigger().elect(false).await.expect("elects");
        let mut leadership = handle.watch_leadership();
        leadership
            .wait_for(Readiness::is_ready)
            .await
            .expect("ready");
        handle.submit(Action::Tick).await.expect("commits");
        node.shutdown().await;
    }

    /// A join two release lines away from a join still in flight is screened against the state
    /// that join leaves: refused, and never logged (ADR-0040 point 2).
    #[tokio::test(start_paused = true)]
    async fn a_join_screened_behind_a_pending_join_sees_it() {
        let dir = tempfile::tempdir().expect("a directory");
        let path = dir.path().join("control.db");
        let node = open_node(&path, options(), SimClock::new(Timestamp::ZERO))
            .await
            .expect("opens");
        let first = {
            let handle = node.handle();
            tokio::spawn(async move { handle.submit(join("old", Release::new(0, 9, 0))).await })
        };
        let second = {
            let handle = node.handle();
            tokio::spawn(async move { handle.submit(join("far", Release::new(0, 11, 0))).await })
        };
        let first = first.await.expect("joins").expect("the first join commits");
        assert!(first.outcome().is_ok(), "{:?}", first.outcome());
        let second = second.await.expect("joins");
        assert!(
            matches!(
                second,
                Err(SubmitError::Refused(Rejection::IncompatibleVersion { .. }))
            ),
            "{second:?}"
        );
        node.shutdown().await;
        let joined: Vec<String> = logged_commands(&path)
            .into_iter()
            .filter_map(|command| match command.action {
                Action::Join { member } => Some(member.id().to_string()),
                _ => None,
            })
            .collect();
        assert_eq!(joined, ["old"]);
    }

    /// A join refused for its software is answered `Refused` and never reaches the log.
    #[tokio::test(start_paused = true)]
    async fn a_join_refused_for_its_software_is_never_logged() {
        let dir = tempfile::tempdir().expect("a directory");
        let path = dir.path().join("control.db");
        let node = open_node(&path, options(), SimClock::new(Timestamp::ZERO))
            .await
            .expect("opens");
        let handle = node.handle();
        handle
            .submit(join("a", Release::new(0, 9, 0)))
            .await
            .expect("commits");
        let refused = handle.submit(join("far", Release::new(9, 0, 0))).await;
        assert!(
            matches!(
                refused,
                Err(SubmitError::Refused(Rejection::IncompatibleVersion { .. }))
            ),
            "{refused:?}"
        );
        node.shutdown().await;
        let logged_far = logged_commands(&path).into_iter().any(|command| {
            matches!(command.action, Action::Join { member } if member.id().as_str() == "far")
        });
        assert!(!logged_far);
    }

    /// A delegation `apply` would reject is answered `Refused` and never reaches the log
    /// (ADR-0047, point 6.2).
    #[tokio::test(start_paused = true)]
    async fn a_refused_delegation_is_never_logged() {
        let dir = tempfile::tempdir().expect("a directory");
        let path = dir.path().join("control.db");
        let clock = SimClock::new(Timestamp::from_millis(1_000_000));
        let node = open_node(&path, options(), clock).await.expect("opens");
        let handle = node.handle();
        handle
            .submit(Action::Join {
                member: member_with_every_feature("data-1"),
            })
            .await
            .expect("commits");
        let mut planted = root_delegation(clock.now(), 1, 60_000);
        if let Action::RecordDelegation { requested_by, .. } = &mut planted {
            *requested_by = "agent-43".parse().expect("a name");
        }
        let refused = handle.submit(planted).await;
        assert!(
            matches!(refused, Err(SubmitError::Refused(_))),
            "{refused:?}"
        );
        node.shutdown().await;
        let delegations = logged_commands(&path)
            .into_iter()
            .filter(|command| matches!(command.action, Action::RecordDelegation { .. }))
            .count();
        assert_eq!(delegations, 0);
    }

    /// The 0.9.0 fixture's actions, its enabling commands left out and each submitted at its
    /// own time, are logged with the same enabling commands at the same places as 0.9.0's
    /// single writer logged them.
    #[tokio::test(start_paused = true)]
    async fn enabling_commands_precede_their_trigger_as_the_writer_logged_them() {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/control-0.9.0/commands.json");
        let text = std::fs::read_to_string(path).expect("readable");
        let fixture: Vec<Command> = serde_json::from_str(&text).expect("decodes");
        let base = fixture.first().expect("commands").at;
        let dir = tempfile::tempdir().expect("a directory");
        let path = dir.path().join("control.db");
        let clock = SimClock::new(base);
        let started = tokio::time::Instant::now();
        let node = open_node(&path, options(), clock).await.expect("opens");
        let handle = node.handle();
        for command in &fixture {
            if matches!(
                command.action,
                Action::EnableHandover
                    | Action::EnableIncarnations
                    | Action::SetTombstoneWindow { .. }
            ) {
                continue;
            }
            let offset = command.at.as_millis().saturating_sub(base.as_millis());
            tokio::time::sleep_until(started + std::time::Duration::from_millis(offset)).await;
            // Rejections are logged as the writer logged them; only refusals would differ.
            handle
                .submit(command.action.clone())
                .await
                .expect("logged as the writer logged it");
        }
        node.shutdown().await;
        let logged: Vec<Action> = logged_commands(&path)
            .into_iter()
            .map(|command| command.action)
            .collect();
        let expected: Vec<Action> = fixture.into_iter().map(|command| command.action).collect();
        assert_eq!(logged, expected);
    }

    /// A `RecordDelegation` and its resend submitted together: the resend is answered with the
    /// record, `Resent::Yes`, only after the record's own submitter was answered, which happens
    /// once its entry has been applied.
    #[tokio::test(start_paused = true)]
    async fn a_resend_is_answered_only_after_its_record_commits() {
        let dir = tempfile::tempdir().expect("a directory");
        let clock = SimClock::new(Timestamp::from_millis(1_000_000));
        let node = open_node(dir.path().join("control.db"), options(), clock)
            .await
            .expect("opens");
        let handle = node.handle();
        handle
            .submit(Action::Join {
                member: member_with_every_feature("data-1"),
            })
            .await
            .expect("commits");
        let action = root_delegation(clock.now(), 1, 60_000);
        let answered = Arc::new(std::sync::Mutex::new(Vec::new()));
        let submit = |label: &'static str, action: Action| {
            let handle = handle.clone();
            let answered = Arc::clone(&answered);
            tokio::spawn(async move {
                let answer = handle.submit(action).await;
                let applied = handle.applied();
                answered.lock().expect("lock").push((label, applied));
                answer
            })
        };
        let recorded = submit("recorded", action.clone());
        let resent = submit("resent", action);
        let recorded = recorded.await.expect("joins").expect("recorded");
        let resent = resent.await.expect("joins").expect("answered");
        assert!(
            matches!(
                recorded.outcome(),
                Ok(Applied::DelegationRecorded {
                    resent: Resent::No,
                    ..
                })
            ),
            "{:?}",
            recorded.outcome()
        );
        assert!(
            matches!(
                resent.outcome(),
                Ok(Applied::DelegationRecorded {
                    resent: Resent::Yes,
                    ..
                })
            ),
            "{:?}",
            resent.outcome()
        );
        let order: Vec<(&str, crate::control::LogIndex)> = answered.lock().expect("lock").clone();
        let labels: Vec<&str> = order.iter().map(|(label, _)| *label).collect();
        assert_eq!(labels, ["recorded", "resent"], "{order:?}");
        // Answered from the state once the record's entry was applied.
        assert!(order[1].1 >= recorded.index(), "{order:?}");
        assert_eq!(resent.index(), recorded.index());
        assert!(resent.state().delegations().len() > 0);
        node.shutdown().await;
    }

    /// A resend submitted behind its record, whose entry every state machine supersedes (as a
    /// truncation would leave it under another leader), is answered `Superseded`, retry, and not
    /// with a record the applied state lacks. A one-voter group can't truncate, so the proposer
    /// screens the record under another term than it writes it in.
    #[tokio::test(start_paused = true)]
    async fn a_resend_whose_record_was_truncated_is_answered_retry() {
        let dir = tempfile::tempdir().expect("a directory");
        let clock = SimClock::new(Timestamp::from_millis(1_000_000));
        let node = open_node(dir.path().join("control.db"), options(), clock)
            .await
            .expect("opens");
        let handle = node.handle();
        handle
            .submit(Action::Join {
                member: member_with_every_feature("data-1"),
            })
            .await
            .expect("commits");
        let action = root_delegation(clock.now(), 1, 60_000);
        handle.faults().supersede.store(1, Ordering::SeqCst);
        let submit = |action: Action| {
            let handle = handle.clone();
            tokio::spawn(async move { handle.submit(action).await })
        };
        let recorded = submit(action.clone());
        let resent = submit(action.clone());
        let recorded = recorded.await.expect("joins");
        let resent = resent.await.expect("joins");
        assert!(
            matches!(recorded, Err(SubmitError::Superseded)),
            "{recorded:?}"
        );
        assert!(matches!(resent, Err(SubmitError::Superseded)), "{resent:?}");
        assert_eq!(handle.state().delegations().len(), 0);
        // The poisoned screening state is cloned again: the same delegation now records.
        let retried = handle.submit(action).await.expect("recorded");
        assert!(
            matches!(
                retried.outcome(),
                Ok(Applied::DelegationRecorded {
                    resent: Resent::No,
                    ..
                })
            ),
            "{:?}",
            retried.outcome()
        );
        node.shutdown().await;
    }

    /// A proposal answered other than `Applied` leaves the screening state holding a command
    /// that took no effect. The proposer screens the next batch from the applied state again: a
    /// join two release lines from a join that was superseded is accepted, as the log holds no
    /// trace of the first.
    #[tokio::test(start_paused = true)]
    async fn a_proposal_that_takes_no_effect_resets_the_screening_state() {
        let dir = tempfile::tempdir().expect("a directory");
        let path = dir.path().join("control.db");
        let node = open_node(&path, options(), SimClock::new(Timestamp::ZERO))
            .await
            .expect("opens");
        let handle = node.handle();
        handle.faults().supersede.store(1, Ordering::SeqCst);
        let first = handle.submit(join("old", Release::new(0, 8, 0))).await;
        assert!(matches!(first, Err(SubmitError::Superseded)), "{first:?}");
        let second = handle
            .submit(join("far", Release::new(0, 10, 0)))
            .await
            .expect("commits");
        assert!(second.outcome().is_ok(), "{:?}", second.outcome());
        node.shutdown().await;
        let joined: Vec<String> = logged_commands(&path)
            .into_iter()
            .filter_map(|command| match command.action {
                Action::Join { member } => Some(member.id().to_string()),
                _ => None,
            })
            .collect();
        assert_eq!(joined, ["far"]);
    }

    /// A batch submitted while a proposal that takes no effect is still in flight waits in the
    /// mailbox until that proposal is answered, and is then screened against the state cloned
    /// again from the applied state, not against the screening state that held the command
    /// (ADR-0040, amendment 1): a join two release lines from the superseded join is accepted.
    #[tokio::test(start_paused = true)]
    async fn a_batch_behind_a_refused_proposal_is_screened_against_the_applied_state() {
        let dir = tempfile::tempdir().expect("a directory");
        let path = dir.path().join("control.db");
        let node = open_node(&path, options(), SimClock::new(Timestamp::ZERO))
            .await
            .expect("opens");
        let handle = node.handle();
        handle.faults().supersede.store(1, Ordering::SeqCst);
        handle.faults().hold.store(true, Ordering::SeqCst);
        let first = {
            let handle = handle.clone();
            tokio::spawn(async move { handle.submit(join("old", Release::new(0, 8, 0))).await })
        };
        while handle.in_flight() == 0 {
            tokio::task::yield_now().await;
        }
        let second = {
            let handle = handle.clone();
            tokio::spawn(async move { handle.submit(join("far", Release::new(0, 10, 0))).await })
        };
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        assert!(
            !second.is_finished(),
            "the second batch waits for the first's answer"
        );
        handle.faults().hold.store(false, Ordering::SeqCst);
        handle.faults().release.notify_one();
        let first = first.await.expect("joins");
        assert!(matches!(first, Err(SubmitError::Superseded)), "{first:?}");
        let second = second.await.expect("joins").expect("commits");
        assert!(second.outcome().is_ok(), "{:?}", second.outcome());
        node.shutdown().await;
        let joined: Vec<String> = logged_commands(&path)
            .into_iter()
            .filter_map(|command| match command.action {
                Action::Join { member } => Some(member.id().to_string()),
                _ => None,
            })
            .collect();
        assert_eq!(joined, ["far"]);
    }

    /// A batch openraft hasn't answered within four election timeouts stalls the proposer: what
    /// queues behind it is answered `CatchingUp` without a leader to name, unscreened, and once
    /// the batch is answered the proposer screens again.
    #[tokio::test(start_paused = true)]
    async fn submissions_behind_a_stalled_batch_are_refused_at_once() {
        let dir = tempfile::tempdir().expect("a directory");
        let node = open_node(
            dir.path().join("control.db"),
            options(),
            SimClock::new(Timestamp::ZERO),
        )
        .await
        .expect("opens");
        let handle = node.handle();
        handle.faults().hold.store(true, Ordering::SeqCst);
        let first = {
            let handle = handle.clone();
            tokio::spawn(async move { handle.submit(Action::Tick).await })
        };
        while handle.in_flight() == 0 {
            tokio::task::yield_now().await;
        }
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
        let refused = handle.submit(Action::Tick).await;
        assert!(
            matches!(refused, Err(SubmitError::CatchingUp { leader: None })),
            "{refused:?}"
        );
        handle.faults().hold.store(false, Ordering::SeqCst);
        handle.faults().release.notify_one();
        first.await.expect("joins").expect("the held tick commits");
        handle.submit(Action::Tick).await.expect("screens again");
        node.shutdown().await;
    }

    /// A shutdown while a batch is held stops the proposer anyway: the held submitter, and one
    /// queued behind it, are answered, and the shutdown returns.
    #[tokio::test(start_paused = true)]
    async fn a_shutdown_while_a_batch_is_held_answers_everyone() {
        let dir = tempfile::tempdir().expect("a directory");
        let node = open_node(
            dir.path().join("control.db"),
            options(),
            SimClock::new(Timestamp::ZERO),
        )
        .await
        .expect("opens");
        let handle = node.handle();
        handle.faults().hold.store(true, Ordering::SeqCst);
        let held = {
            let handle = handle.clone();
            tokio::spawn(async move { handle.submit(Action::Tick).await })
        };
        while handle.in_flight() == 0 {
            tokio::task::yield_now().await;
        }
        let queued = {
            let handle = handle.clone();
            tokio::spawn(async move { handle.submit(Action::Tick).await })
        };
        tokio::task::yield_now().await;
        tokio::time::timeout(std::time::Duration::from_secs(5), node.shutdown())
            .await
            .expect("the shutdown returns");
        handle.faults().release.notify_one();
        let queued = queued.await.expect("joins");
        assert!(matches!(queued, Err(SubmitError::Stopped)), "{queued:?}");
        // The held one was answered by its entry or by the group stopping.
        let _answered = held.await.expect("joins");
    }

    /// Commands reach the log in the order the proposer screened them: by index, their stamps
    /// only grow.
    #[tokio::test(start_paused = true)]
    async fn proposals_enter_the_log_in_screening_order() {
        let dir = tempfile::tempdir().expect("a directory");
        let path = dir.path().join("control.db");
        let node = open_node(&path, options(), TickClock::default())
            .await
            .expect("opens");
        let submits: Vec<_> = (0..30)
            .map(|_| {
                let handle = node.handle();
                tokio::spawn(async move { handle.submit(Action::Tick).await })
            })
            .collect();
        for submit in submits {
            submit.await.expect("joins").expect("commits");
        }
        node.shutdown().await;
        let store =
            crate::control::ControlStore::open(&path, ControlConfig::new(), None).expect("opens");
        let stamps: Vec<u64> = store
            .commands()
            .expect("readable")
            .into_iter()
            .map(|(_, command)| command.at.as_millis())
            .collect();
        assert_eq!(stamps.len(), 30);
        assert!(
            stamps.windows(2).all(|pair| pair[0] < pair[1]),
            "{stamps:?}"
        );
    }

    /// Commands submitted together share appends, and so syncs: fifty take far fewer than
    /// fifty appends.
    #[tokio::test(start_paused = true)]
    async fn queued_commands_share_a_sync() {
        let dir = tempfile::tempdir().expect("a directory");
        let node = open_node(
            dir.path().join("control.db"),
            options(),
            SimClock::new(Timestamp::ZERO),
        )
        .await
        .expect("opens");
        let before = node.appends();
        let submits: Vec<_> = (0..50)
            .map(|_| {
                let handle = node.handle();
                tokio::spawn(async move { handle.submit(Action::Tick).await })
            })
            .collect();
        for submit in submits {
            submit.await.expect("joins").expect("commits");
        }
        let appends = node.appends() - before;
        assert!(appends < 50, "{appends} appends for 50 commands");
        node.shutdown().await;
    }

    /// At readiness the leader warns when control state's tombstone window, or the newest
    /// fork's limits, differ from its own configuration, naming both values (ADR-0040 point
    /// 13); equal settings warn nothing.
    #[test]
    fn a_tombstone_window_unlike_the_states_is_warned_at_readiness() {
        let mut state = State::new(ControlConfig::new());
        let joined = apply(
            &mut state,
            Command::new(
                Timestamp::from_millis(1),
                Action::Join {
                    member: member_with_every_feature("data-1"),
                },
            ),
        );
        assert!(joined.is_ok(), "{joined:?}");
        let set = Command::stamped(
            Timestamp::from_millis(1),
            "control-1".parse().expect("an id"),
            Action::SetTombstoneWindow {
                window: Millis::new(60_000),
            },
        );
        let _outcome = apply(&mut state, set);
        let warnings = setting_warnings(&state, Millis::new(120_000), None);
        assert_eq!(warnings.len(), 1, "{warnings:?}");
        assert!(warnings[0].contains("60000 ms"), "{}", warnings[0]);
        assert!(warnings[0].contains("120000 ms"), "{}", warnings[0]);
        assert!(setting_warnings(&state, Millis::new(60_000), None).is_empty());
        assert!(
            setting_warnings(&state, Millis::new(60_000), Some(ForkLimits::default())).is_empty(),
            "no fork, nothing to compare"
        );
    }
}
