//! The control group's state machine: control state, applied entry by entry (ADR-0040 points 2,
//! 3, 4, and 7).
//!
//! [`ControlStateMachine`] applies openraft's committed entries to [`State`] with
//! `fleetring_control::apply`, and publishes the state after every entry. Before it applies a
//! command entry it runs the skip rules, which every replica and every replay without openraft
//! run alike ([`decide`]):
//!
//! 1. A command screened under another leader than the one that appended it is skipped as
//!    superseded, without decoding it.
//! 2. A command this build can't decode stops the state machine at that entry for good: it never
//!    applies past it, and the node's `control_log` readiness check says why (fail-stop).
//! 3. A command whose `recorded_by` is missing, or isn't the node that appended it, is skipped as
//!    superseded (decision D5).
//!
//! Blank and membership entries are openraft's own and are never skipped. Snapshots use the
//! store's encoding, and every snapshot read or installed is checked against the genesis
//! configuration.

use std::io;
use std::num::NonZeroUsize;
use std::sync::Arc;

use fleetring_control::{Command, ControlConfig, Feature, State, apply};
use openraft::storage::{EntryResponder, RaftSnapshotBuilder, RaftStateMachine, Snapshot};
use openraft::{EntryPayload, OptionalSend};
use tokio::sync::watch;
use tokio_stream::{Stream, StreamExt};

use crate::control::LogError;
use crate::control::format1::check_state;
use crate::control::node::StorageIo;
use crate::control::raft::log_storage::{SharedStore, with_store};
use crate::control::raft::{
    ControlRequest, ControlResponse, ControlTypes, LogId, SnapshotMeta, StoredMembership, raft_id,
};
use crate::control::store::ControlSnapshot;
use crate::observe::ControlMetrics;
use crate::observe::codes::action_label;

/// What a control snapshot carries over the wire: the state, as `control.db` stores it.
pub type SnapshotData = Vec<u8>;

/// A snapshot of the control group.
pub(crate) type ControlSnapshotOf =
    openraft::type_config::alias::SnapshotOf<ControlTypes, SnapshotData>;

/// The state after an applied entry, published after every entry.
#[derive(Debug, Clone)]
pub(crate) struct AppliedState {
    /// The last applied entry's log ID, or the newest snapshot's; `None` before anything.
    pub(crate) log_id: Option<LogId>,
    /// The state after it.
    pub(crate) state: Arc<State>,
}

impl AppliedState {
    /// The last applied entry's index; 0 before anything.
    pub(crate) fn index(&self) -> u64 {
        self.log_id.map_or(0, |log_id| log_id.index)
    }
}

/// What the state machine does with a command entry, by the skip rules.
#[derive(Debug)]
pub(crate) enum Decision {
    /// Apply the command.
    Apply(Box<Command>),
    /// Skip it as superseded: it changes nothing.
    Superseded,
    /// This build can't decode it: stop here.
    Undecodable(serde_json::Error),
}

/// Runs the skip rules on the command entry at `log_id` (ADR-0040 point 2, decision D5): the
/// leader it was screened under, then the decoded command, then its `recorded_by`. Pure, so
/// every replica and every replay without openraft decides alike.
pub(crate) fn decide(log_id: &LogId, request: &ControlRequest) -> Decision {
    let leader = &log_id.leader_id;
    if request.screened.term != leader.term || request.screened.node != leader.node_id {
        return Decision::Superseded;
    }
    let command = match request.command() {
        Some(command) => command.clone(),
        None => match request.decode() {
            Ok(command) => command,
            Err(error) => return Decision::Undecodable(error),
        },
    };
    match &command.recorded_by {
        Some(by) if raft_id(by.as_str()) == leader.node_id => Decision::Apply(Box::new(command)),
        Some(_) | None => Decision::Superseded,
    }
}

/// The readiness message of a node stopped at an entry it can't decode (ADR-0040 point 4).
fn undecodable(index: u64, error: &serde_json::Error) -> String {
    format!(
        "entry {index} can't be read by fleetring {} ({error}); upgrade this node",
        crate::software::release()
    )
}

/// The control group's state machine over the control store.
#[derive(Debug)]
pub(crate) struct ControlStateMachine {
    store: SharedStore,
    io: StorageIo,
    state: State,
    last_applied: Option<LogId>,
    membership: StoredMembership,
    snapshots_retained: NonZeroUsize,
    published: watch::Sender<Arc<State>>,
    applied: watch::Sender<AppliedState>,
    halted: watch::Sender<Option<String>>,
    metrics: ControlMetrics,
    /// The committed index the store held when the node opened: openraft applies the entries up
    /// to it again, which this node already counted and audited before it stopped.
    recovered_through: u64,
}

/// The receivers a state machine publishes to.
#[derive(Debug, Clone)]
pub(crate) struct Published {
    /// The state after the last command that changed it.
    pub(crate) state: watch::Receiver<Arc<State>>,
    /// The state and log ID after every applied entry.
    pub(crate) applied: watch::Receiver<AppliedState>,
    /// Why the state machine stopped, once it met an entry it can't decode.
    pub(crate) halted: watch::Receiver<Option<String>>,
}

impl ControlStateMachine {
    /// Returns the state machine over `store`, starting from its newest control snapshot, or from
    /// its genesis state, keeping `snapshots_retained` snapshots, and counting every command it
    /// applies in `metrics`.
    ///
    /// # Errors
    ///
    /// [`LogError`] if the newest snapshot can't be read.
    pub(crate) fn open(
        store: SharedStore,
        io: StorageIo,
        snapshots_retained: NonZeroUsize,
        metrics: ControlMetrics,
    ) -> Result<(Self, Published), LogError> {
        let (state, last_applied, membership, recovered_through, stopped_at) = {
            let store = store.lock().expect("control store lock poisoned");
            let (state, last_applied, membership) = match store.latest_control_snapshot()? {
                Some(ControlSnapshot { meta, state }) => {
                    (state, meta.last_log_id, meta.last_membership)
                }
                None => (
                    State::new(store.genesis()),
                    None,
                    StoredMembership::default(),
                ),
            };
            // openraft applies the committed entries after the snapshot again as the node opens.
            // A node that stopped at an entry it can't decode starts stopped there, so it opens,
            // says why, and keeps serving its data role, rather than fail to open at all.
            let recovered_through = store.committed()?.map_or(0, |log_id| log_id.index);
            let from = last_applied.map_or(0, |log_id| log_id.index.saturating_add(1));
            let mut stopped_at = None;
            if recovered_through >= from {
                for entry in store.entries(from..=recovered_through)? {
                    if let EntryPayload::Normal(request) = &entry.payload
                        && let Decision::Undecodable(error) = decide(&entry.log_id, request)
                    {
                        stopped_at = Some(undecodable(entry.log_id.index, &error));
                        break;
                    }
                }
            }
            (
                state,
                last_applied,
                membership,
                recovered_through,
                stopped_at,
            )
        };
        if let Some(reason) = &stopped_at {
            tracing::error!("{reason}");
        }
        let shared = Arc::new(state.clone());
        let (published, state_receiver) = watch::channel(Arc::clone(&shared));
        let (applied, applied_receiver) = watch::channel(AppliedState {
            log_id: last_applied,
            state: shared,
        });
        let (halted, halted_receiver) = watch::channel(stopped_at);
        let machine = Self {
            store,
            io,
            state,
            last_applied,
            membership,
            snapshots_retained,
            published,
            applied,
            halted,
            metrics,
            recovered_through,
        };
        Ok((
            machine,
            Published {
                state: state_receiver,
                applied: applied_receiver,
                halted: halted_receiver,
            },
        ))
    }

    /// Applies one entry and returns its response, or why the state machine stops at it.
    fn apply_entry(
        &mut self,
        log_id: LogId,
        payload: openraft::type_config::alias::PayloadOf<ControlTypes>,
    ) -> Result<ControlResponse, String> {
        let response = match payload {
            EntryPayload::Blank => ControlResponse::Raft,
            EntryPayload::Membership(membership) => {
                self.membership = StoredMembership::new(Some(log_id), membership);
                ControlResponse::Raft
            }
            EntryPayload::Normal(request) => match decide(&log_id, &request) {
                Decision::Superseded => ControlResponse::Superseded,
                Decision::Undecodable(error) => return Err(undecodable(log_id.index, &error)),
                Decision::Apply(command) => self.apply_command(log_id.index, *command),
            },
        };
        self.last_applied = Some(log_id);
        self.applied.send_replace(AppliedState {
            log_id: Some(log_id),
            state: Arc::clone(&self.published.borrow()),
        });
        Ok(response)
    }

    /// Applies `command`, logged at `index`, publishes the state if it changed, and returns the
    /// response.
    fn apply_command(&mut self, index: u64, command: Command) -> ControlResponse {
        let action = action_label(&command.action);
        let pending = pending_forks(&self.state);
        let enabled: Vec<Feature> = self.state.features().collect();
        let outcome = apply(&mut self.state, command);
        let label = if outcome.is_ok() {
            "applied"
        } else {
            "rejected"
        };
        // An entry applied again as the node opens was counted and audited the first time.
        let first_time = index > self.recovered_through;
        if first_time {
            self.metrics.command(index, action, label);
        }
        let state = if outcome.is_ok() {
            if first_time {
                count_finished_forks(&self.state, &pending, &self.metrics);
                log_enabled_features(&self.state, &enabled, index);
            }
            let state = Arc::new(self.state.clone());
            self.published.send_replace(Arc::clone(&state));
            state
        } else {
            // A rejection changes nothing, so the published state is still current.
            Arc::clone(&self.published.borrow())
        };
        ControlResponse::Applied { outcome, state }
    }

    /// Replaces the state with a snapshot's, and publishes it.
    fn restore(&mut self, meta: &SnapshotMeta, state: State) {
        self.state = state;
        self.last_applied = meta.last_log_id;
        self.membership = meta.last_membership.clone();
        let shared = Arc::new(self.state.clone());
        self.published.send_replace(Arc::clone(&shared));
        self.applied.send_replace(AppliedState {
            log_id: self.last_applied,
            state: shared,
        });
    }
}

/// Builds a control snapshot of the state a state machine had when it made the builder.
#[derive(Debug)]
pub(crate) struct ControlSnapshotBuilder {
    store: SharedStore,
    io: StorageIo,
    meta: SnapshotMeta,
    state: State,
    retain: NonZeroUsize,
    metrics: ControlMetrics,
}

impl RaftSnapshotBuilder<ControlTypes> for ControlSnapshotBuilder {
    type SnapshotData = SnapshotData;

    async fn build_snapshot(&mut self) -> Result<ControlSnapshotOf, io::Error> {
        let data = serde_json::to_vec(&self.state).map_err(io::Error::other)?;
        let meta = self.meta.clone();
        let state = self.state.clone();
        let retain = self.retain;
        with_store(&self.store, self.io, move |store| {
            store.save_control_snapshot(&meta, &state, retain)
        })
        .await?;
        let index = self.meta.last_log_id.map_or(0, |log_id| log_id.index);
        self.metrics.snapshot_saved(index);
        tracing::debug!(index, "control snapshot saved");
        Ok(Snapshot {
            meta: self.meta.clone(),
            snapshot: data,
        })
    }
}

impl RaftStateMachine<ControlTypes> for ControlStateMachine {
    type SnapshotData = SnapshotData;
    type SnapshotBuilder = ControlSnapshotBuilder;

    async fn applied_state(&mut self) -> Result<(Option<LogId>, StoredMembership), io::Error> {
        Ok((self.last_applied, self.membership.clone()))
    }

    async fn apply<Strm>(&mut self, mut entries: Strm) -> Result<(), io::Error>
    where
        Strm: Stream<Item = Result<EntryResponder<ControlTypes>, io::Error>> + Unpin + OptionalSend,
    {
        if let Some(reason) = self.halted.borrow().clone() {
            return Err(io::Error::other(reason));
        }
        while let Some(next) = entries.next().await {
            let (entry, responder) = next?;
            match self.apply_entry(entry.log_id, entry.payload) {
                Ok(response) => {
                    if let Some(responder) = responder {
                        responder.send(response);
                    }
                }
                Err(reason) => {
                    tracing::error!("{reason}");
                    self.halted.send_replace(Some(reason.clone()));
                    return Err(io::Error::other(reason));
                }
            }
        }
        Ok(())
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        ControlSnapshotBuilder {
            store: Arc::clone(&self.store),
            io: self.io,
            meta: SnapshotMeta {
                last_log_id: self.last_applied,
                last_membership: self.membership.clone(),
            },
            state: self.state.clone(),
            retain: self.snapshots_retained,
            metrics: self.metrics.clone(),
        }
    }

    /// Installs a snapshot the leader sent: its state is checked against the store's genesis
    /// configuration, or, on a fresh joiner whose store holds nothing yet, becomes the store's
    /// genesis.
    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta,
        snapshot: Self::SnapshotData,
    ) -> Result<(), io::Error> {
        let state: State = serde_json::from_slice(&snapshot).map_err(io::Error::other)?;
        let saved_meta = meta.clone();
        let saved_state = state.clone();
        let retain = self.snapshots_retained;
        with_store(&self.store, self.io, move |store| {
            let config: ControlConfig = *saved_state.config();
            if store.is_fresh()? {
                store.set_genesis(config)?;
            }
            check_state(&saved_state, &store.genesis()).map_err(|reason| {
                LogError::CorruptSnapshot {
                    index: crate::control::LogIndex::new(
                        saved_meta.last_log_id.map_or(0, |log_id| log_id.index),
                    ),
                    reason,
                }
            })?;
            store.save_control_snapshot(&saved_meta, &saved_state, retain)
        })
        .await?;
        self.restore(meta, state);
        Ok(())
    }

    async fn get_current_snapshot(&mut self) -> Result<Option<ControlSnapshotOf>, io::Error> {
        let snapshot = with_store(&self.store, self.io, |store| {
            store.latest_control_snapshot()
        })
        .await?;
        snapshot
            .map(|ControlSnapshot { meta, state }| {
                let data = serde_json::to_vec(&state).map_err(io::Error::other)?;
                Ok(Snapshot {
                    meta,
                    snapshot: data,
                })
            })
            .transpose()
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
    metrics: &ControlMetrics,
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
fn log_enabled_features(state: &State, before: &[Feature], index: u64) {
    for feature in state.features().filter(|feature| !before.contains(feature)) {
        tracing::info!(target: "fleetringd::audit", feature = feature.as_str(), index, "fleet feature enabled for good: members without it can no longer join, and rolling back to a release without it is refused");
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};
    use std::sync::Mutex;

    use fleetring_control::{Action, Timestamp};
    use openraft::impls::leader_id_adv::LeaderId;
    use serde_json::value::RawValue;

    use super::*;
    use crate::control::raft::{ControlPeer, Entry, LeaderRef, Membership, RaftId};
    use crate::control::store::ControlStore;

    /// The node the fixture's commands name as `recorded_by`.
    const OWN: &str = "c1";

    fn own() -> RaftId {
        raft_id(OWN)
    }

    fn log_id(term: u64, node: RaftId, index: u64) -> LogId {
        LogId::new(
            LeaderId {
                term,
                node_id: node,
            },
            index,
        )
    }

    /// A command entry at `index`, appended by `own` in `term` and screened in `screened`.
    fn command_entry(term: u64, screened: u64, index: u64, command: Command) -> Entry {
        let request = ControlRequest::new(
            LeaderRef {
                term: screened,
                node: own(),
            },
            command,
        )
        .expect("the command encodes");
        Entry {
            log_id: log_id(term, own(), index),
            payload: EntryPayload::Normal(request),
        }
    }

    fn tick(at: u64) -> Command {
        Command::stamped(
            Timestamp::from_millis(at),
            OWN.parse().expect("an id"),
            Action::Tick,
        )
    }

    fn membership() -> Membership {
        Membership::new(
            vec![BTreeSet::from([own()])],
            BTreeMap::from([(own(), ControlPeer::new(OWN, "127.0.0.1:7400"))]),
        )
        .expect("a membership")
    }

    fn open_machine(
        path: std::path::PathBuf,
        config: ControlConfig,
    ) -> (ControlStateMachine, Published) {
        let store = ControlStore::open(path, config, None).expect("the store opens");
        ControlStateMachine::open(
            Arc::new(Mutex::new(store)),
            StorageIo::Inline,
            NonZeroUsize::new(2).expect("nonzero"),
            ControlMetrics::default(),
        )
        .expect("the state machine opens")
    }

    fn machine(dir: &std::path::Path) -> (ControlStateMachine, Published) {
        open_machine(dir.join("control.db"), ControlConfig::new())
    }

    /// A state machine on a store created under another genesis configuration.
    fn joiner(dir: &std::path::Path) -> (ControlStateMachine, Published) {
        let config = ControlConfig::new().with_lease_duration(fleetring_control::Millis::new(1));
        open_machine(dir.join("joiner.db"), config)
    }

    fn apply_one(machine: &mut ControlStateMachine, entry: Entry) -> ControlResponse {
        machine
            .apply_entry(entry.log_id, entry.payload)
            .expect("the entry applies")
    }

    /// The 0.9.0 fixture's commands, as command entries with a membership and a blank entry
    /// among them, apply to the state `apply` reaches on the commands alone.
    #[tokio::test]
    async fn applying_entries_matches_applying_commands() {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/control-0.9.0/commands.json");
        let text = std::fs::read_to_string(path).expect("readable");
        let commands: Vec<Command> = serde_json::from_str(&text).expect("decodes");
        let mut replayed = State::new(ControlConfig::new());
        for command in commands.clone() {
            let _outcome = apply(&mut replayed, command);
        }
        let dir = tempfile::tempdir().expect("a directory");
        let (mut machine, published) = machine(dir.path());
        let mut entries = vec![Entry {
            log_id: log_id(1, own(), 0),
            payload: EntryPayload::Membership(membership()),
        }];
        for (n, command) in commands.into_iter().enumerate() {
            if n == 20 {
                let index = u64::try_from(entries.len()).expect("small");
                entries.push(Entry {
                    log_id: log_id(1, own(), index),
                    payload: EntryPayload::Blank,
                });
            }
            let index = u64::try_from(entries.len()).expect("small");
            entries.push(command_entry(1, 1, index, command));
        }
        let last = entries.last().expect("entries").log_id;
        let stream = tokio_stream::iter(entries.into_iter().map(|entry| Ok((entry, None))));
        machine.apply(stream).await.expect("every entry applies");
        assert_eq!(*published.state.borrow().as_ref(), replayed);
        assert_eq!(published.applied.borrow().log_id, Some(last));
        assert_eq!(machine.membership.membership(), &membership());
    }

    /// The committed entries openraft applies again as a node opens were counted the first
    /// time: only the entries past the committed index the store held at open are counted.
    #[tokio::test]
    async fn entries_applied_again_at_open_are_not_counted_again() {
        let dir = tempfile::tempdir().expect("a directory");
        let path = dir.path().join("control.db");
        {
            let mut store =
                ControlStore::open(&path, ControlConfig::new(), None).expect("the store opens");
            let entries: Vec<Entry> = (0..3)
                .map(|index| command_entry(1, 1, index, tick(index)))
                .collect();
            store.append_entries(&entries).expect("appended");
            store
                .save_committed(Some(&log_id(1, own(), 2)))
                .expect("committed");
        }
        let (mut machine, _published) = open_machine(path, ControlConfig::new());
        for index in 0..5 {
            let _response = apply_one(&mut machine, command_entry(1, 1, index, tick(index)));
        }
        assert_eq!(machine.metrics.commands("tick", "applied"), 2);
    }

    /// A command `apply` rejects still moves the applied index, and changes nothing else.
    #[test]
    fn a_rejection_still_moves_the_applied_index() {
        let dir = tempfile::tempdir().expect("a directory");
        let (mut machine, published) = machine(dir.path());
        let before = published.state.borrow().clone();
        let leave = Command::stamped(
            Timestamp::from_millis(5),
            OWN.parse().expect("an id"),
            Action::Leave {
                id: "nobody".parse().expect("an id"),
            },
        );
        let response = apply_one(&mut machine, command_entry(1, 1, 3, leave));
        assert!(
            matches!(
                response,
                ControlResponse::Applied {
                    outcome: Err(_),
                    ..
                }
            ),
            "{response:?}"
        );
        assert_eq!(published.applied.borrow().index(), 3);
        assert_eq!(*published.state.borrow(), before);
    }

    /// A command entry screened under another term than the entry's own is superseded without
    /// being decoded: its JSON isn't a command at all, and nothing fails.
    #[test]
    fn an_entry_appended_in_another_term_is_superseded_without_decoding() {
        let dir = tempfile::tempdir().expect("a directory");
        let (mut machine, published) = machine(dir.path());
        let request = ControlRequest::from_json(
            LeaderRef {
                term: 1,
                node: own(),
            },
            RawValue::from_string(r#"{"not":"a command"}"#.to_owned()).expect("json"),
        );
        let entry = Entry {
            log_id: log_id(2, own(), 4),
            payload: EntryPayload::Normal(request),
        };
        let response = apply_one(&mut machine, entry);
        assert!(
            matches!(response, ControlResponse::Superseded),
            "{response:?}"
        );
        assert_eq!(published.applied.borrow().index(), 4);
        assert!(published.halted.borrow().is_none());
    }

    /// A command stamped by another node than the entry's leader is superseded (decision D5).
    #[test]
    fn an_entry_whose_recorded_by_is_not_its_leader_is_superseded() {
        let dir = tempfile::tempdir().expect("a directory");
        let (mut machine, published) = machine(dir.path());
        let before = published.state.borrow().clone();
        let other = Command::stamped(
            Timestamp::from_millis(5),
            "c2".parse().expect("an id"),
            Action::Tick,
        );
        let response = apply_one(&mut machine, command_entry(1, 1, 1, other));
        assert!(
            matches!(response, ControlResponse::Superseded),
            "{response:?}"
        );
        assert_eq!(*published.state.borrow(), before);
    }

    /// A command without `recorded_by` is superseded (decision D5).
    #[test]
    fn an_entry_without_recorded_by_is_superseded() {
        let dir = tempfile::tempdir().expect("a directory");
        let (mut machine, published) = machine(dir.path());
        let before = published.state.borrow().clone();
        let unstamped = Command::new(Timestamp::from_millis(5), Action::Tick);
        let response = apply_one(&mut machine, command_entry(1, 1, 1, unstamped));
        assert!(
            matches!(response, ControlResponse::Superseded),
            "{response:?}"
        );
        assert_eq!(*published.state.borrow(), before);
    }

    /// Blank and membership entries are openraft's own: always applied, never skipped, whatever
    /// leader appended them.
    #[test]
    fn blank_and_membership_entries_are_never_skipped() {
        let dir = tempfile::tempdir().expect("a directory");
        let (mut machine, published) = machine(dir.path());
        let response = apply_one(
            &mut machine,
            Entry {
                log_id: log_id(1, raft_id("someone-else"), 0),
                payload: EntryPayload::Membership(membership()),
            },
        );
        assert!(matches!(response, ControlResponse::Raft), "{response:?}");
        let response = apply_one(
            &mut machine,
            Entry {
                log_id: log_id(2, raft_id("someone-else"), 1),
                payload: EntryPayload::Blank,
            },
        );
        assert!(matches!(response, ControlResponse::Raft), "{response:?}");
        assert_eq!(published.applied.borrow().index(), 1);
        assert_eq!(machine.membership.membership(), &membership());
    }

    /// A matching command entry this build can't decode stops the state machine at that entry:
    /// the applied index stays before it, and the reason names the entry (ADR-0040 point 4).
    #[tokio::test]
    async fn an_undecodable_command_stops_applying_and_says_why() {
        let dir = tempfile::tempdir().expect("a directory");
        let (mut machine, published) = machine(dir.path());
        let unknown = ControlRequest::from_json(
            LeaderRef {
                term: 1,
                node: own(),
            },
            RawValue::from_string(
                r#"{"at":5,"recorded_by":"c1","action":{"split_brain":{}}}"#.to_owned(),
            )
            .expect("json"),
        );
        let entries = vec![
            command_entry(1, 1, 1, tick(1)),
            Entry {
                log_id: log_id(1, own(), 2),
                payload: EntryPayload::Normal(unknown),
            },
            command_entry(1, 1, 3, tick(3)),
        ];
        let stream = tokio_stream::iter(entries.into_iter().map(|entry| Ok((entry, None))));
        let stopped = machine.apply(stream).await;
        assert!(stopped.is_err(), "{stopped:?}");
        assert_eq!(published.applied.borrow().index(), 1);
        let reason = published.halted.borrow().clone().expect("halted");
        assert!(
            reason.starts_with("entry 2 can't be read by fleetring"),
            "{reason}"
        );
        assert!(reason.ends_with("upgrade this node"), "{reason}");
        // It never applies past the entry, however often it's asked.
        let again = tokio_stream::iter([Ok((command_entry(1, 1, 3, tick(3)), None))]);
        assert!(machine.apply(again).await.is_err());
        assert_eq!(published.applied.borrow().index(), 1);
    }

    /// A control snapshot built on one node installs to the same state, membership, and applied
    /// log ID on a fresh joiner, whose store takes the snapshot's genesis configuration.
    #[tokio::test]
    async fn a_built_snapshot_installs_to_the_same_state() {
        let dir = tempfile::tempdir().expect("a directory");
        let (mut machine, published) = machine(dir.path());
        let _raft = apply_one(
            &mut machine,
            Entry {
                log_id: log_id(1, own(), 0),
                payload: EntryPayload::Membership(membership()),
            },
        );
        for index in 1..5 {
            let _response = apply_one(&mut machine, command_entry(1, 1, index, tick(index)));
        }
        let mut builder = machine.get_snapshot_builder().await;
        let snapshot = builder.build_snapshot().await.expect("a snapshot");
        let current = machine
            .get_current_snapshot()
            .await
            .expect("readable")
            .expect("the snapshot is kept");
        assert_eq!(current.meta, snapshot.meta);

        let (mut joiner, joined) = joiner(dir.path());
        // openraft saves the leader's vote before it installs the snapshot on a joiner.
        let vote = crate::control::raft::Vote::from(crate::control::raft::codec::VoteWire {
            term: 1,
            node: crate::control::raft::codec::HexId(own()),
            committed: true,
        });
        joiner
            .store
            .lock()
            .expect("lock")
            .save_vote(&vote)
            .expect("a vote");
        joiner
            .install_snapshot(&snapshot.meta, snapshot.snapshot)
            .await
            .expect("the snapshot installs");
        assert_eq!(*joined.state.borrow(), *published.state.borrow());
        assert_eq!(joined.applied.borrow().log_id, snapshot.meta.last_log_id);
        let (applied, membership) = joiner.applied_state().await.expect("readable");
        assert_eq!(applied, snapshot.meta.last_log_id);
        assert_eq!(membership, snapshot.meta.last_membership);
        let genesis = joiner.store.lock().expect("lock").genesis();
        assert_eq!(genesis, ControlConfig::new());
    }

    /// A snapshot whose state was created under another genesis configuration than a store that
    /// already holds Raft state is refused, and changes nothing.
    #[tokio::test]
    async fn a_snapshot_with_another_configuration_is_refused() {
        let dir = tempfile::tempdir().expect("a directory");
        let (mut machine, _published) = machine(dir.path());
        let _response = apply_one(&mut machine, command_entry(1, 1, 1, tick(1)));
        let snapshot = machine
            .get_snapshot_builder()
            .await
            .build_snapshot()
            .await
            .expect("a snapshot");

        let (mut other, published) = joiner(dir.path());
        other
            .store
            .lock()
            .expect("lock")
            .append_entries(&[command_entry(1, 1, 1, tick(1))])
            .expect("an entry");
        let before = published.state.borrow().clone();
        let refused = other
            .install_snapshot(&snapshot.meta, snapshot.snapshot)
            .await;
        assert!(refused.is_err(), "{refused:?}");
        assert_eq!(*published.state.borrow(), before);
    }
}
