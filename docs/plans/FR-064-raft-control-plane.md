---
id: FR-064
title: A Raft control plane: three or five control nodes on openraft
status: todo
priority: P1
size: L
milestone: M13 Control plane
depends_on: [FR-027, FR-028, FR-031, FR-050]
adrs: [ADR-0017, ADR-0018, ADR-0020, ADR-0022, ADR-0031, ADR-0035]
---

# FR-064: A Raft control plane: three or five control nodes on openraft

## Context

The control plane is one node (ADR-0017). While it is down, primaries keep serving (ADR-0018 point 6), but nothing else moves: no failover, no new database, no join, no lease transfer, and after the heartbeat timeout data nodes refuse tokens (ADR-0026). The roadmap's "No single point of failure" criterion (E5) asks for three or five control nodes. ADR-0017 built the control plane for this: every change is a stamped `Command` applied by the pure `apply`, the client already follows `fleetring-leader` redirects, and a replay test guards determinism. ADR-0035 decides what ADR-0017 left open: how openraft's log and state machine sit on the control log, how control nodes are numbered and added, who serves reads, which clock stamps commands after a leader change, how snapshots and compaction work, how a 0.3 control log migrates, and what data nodes do while a leader is elected.

## Scope

- **The control group:** openraft 0.9, with its log storage and state machine over `control.db` (format 2) and its network as a new `Raft` gRPC service. One voter by default, three or five in production. A single control node is a one-voter group, so there is one code path.
- **Leader-only control:** every `Control` RPC is answered by the leader; followers redirect with `fleetring-leader`. The leader stamps commands with its clock, and its failover timer waits a lease duration plus the failover margin after it becomes leader before it starts any promotion.
- **Changing the group:** `fleetringd control add|remove|list|set-voters`, with Raft IDs derived from node IDs, learners caught up before they vote, and voter counts kept odd.
- **Snapshots and compaction:** a control snapshot every `snapshot_every` commands, the log purged behind it with a bounded tail kept, and at most `snapshots_retained` control snapshots on disk.
- **Bootstrap and upgrade:** a fresh control node with `[control] bootstrap = "new"` (the default) starts a group; `"join"` waits to be added. A format-1 `control.db` from 0.3 migrates on first start to a one-voter group with the same state.
- **Data nodes:** `[control] addresses` (a list) as an alternative to `address`; the client learns the group from join and heartbeat replies, rotates through addresses on `UNAVAILABLE`, and follows redirects. Nothing on a data node changes while a leader is elected except retries.
- **Simulation:** control groups of three and five in the harness, with leader crashes, partitions, and skewed clocks, and new invariants (ADR-0020).
- **Not in scope:** follower reads, heartbeats off the log (roadmap E4), and certificates that mark a node as a control node.

## Acceptance criteria

- [ ] ADR-0035 is accepted and listed in `docs/adr/README.md`.
- [ ] A new single control node needs no new configuration and behaves as before; a format-1 `control.db` fixture written by 0.3 migrates on first start to the same `State`, and the node elects itself.
- [ ] `fleetringd control add` twice and `fleetringd control set-voters` grow a running fleet from one control node to three while a client keeps writing; no write fails for longer than one election.
- [ ] In simulation, crashing or isolating the leader of a three- and a five-node group elects a new leader, data nodes reach it through redirects, and no primary changes epoch because of the election alone.
- [ ] On the fixed seeds: no acknowledged control command is lost across leader crashes (`AcknowledgedControlSurvives`), committed commands agree on every control node (new `ControlLogsAgree`), at most one leader holds each term (new `OneLeaderPerTerm`), and every control node's `control.db` replays to the state it published (`ControlLogReplays`).
- [ ] With `snapshot_every = 100`, a control node's log never holds more than `snapshot_every` plus the kept tail, at most `snapshots_retained` control snapshots stay on disk, and a learner added after compaction catches up from a control snapshot.
- [ ] The `Raft` RPCs are in `auth::RPCS` as node calls bound to `control peer`; a node certificate outside the group is refused with `PERMISSION_DENIED`; the group RPCs need the `fleet` scope.
- [ ] A seed replays exactly under `--cfg tokio_unstable`: openraft's randomness comes from the simulation's seed.
- [ ] The daemon README ("Control group"), `deploy/fleetringd.toml`, `docs/cli.md`, the lexicon, `SECURITY.md`, and `docs/roadmap.md` (E5, and E2's compaction line) are updated.

## Implementation plan

Each step is about one commit, with the subject given and the trailer `Refs: FR-064`. Every path is under `crates/fleetringd/` unless it says otherwise. New public types derive `Debug` and are added to `src/lib.rs`'s `assert_debug_send_sync` list; errors are `#[non_exhaustive]`, derive `thiserror::Error`, and have lowercase messages; no `unwrap()` outside tests; no `bool` parameters; no `HashMap` or `HashSet`; time comes from `Clock` or `tokio::time`, randomness from an injected generator. `fleetring-control` doesn't change in any step: `apply`, `State`, `Command`, their serde forms, and `crates/fleetring-control/tests/it/replay.rs` stay as they are, and must keep passing.

Words: "control group" for the control nodes that replicate the control log; "voter" and "learner" for its two kinds of member; "term" only qualified, as "the control group's term" or "Raft term", never for a lease epoch; "leader" only for the control group's leader; "fleet", never "cluster", except where openraft's own API names a field `cluster_name`.

### Step 0: a format-1 control log fixture (`test(fleetringd): check in a 0.3 control log`)

Before any format change, so the fixture is written by the code that will later have to be migrated.

- `tests/control.rs`: add

  ```rust
  /// Writes `tests/fixtures/control-0.3.0/control.db` and `commands.json` with the format-1 log.
  /// Run once with `cargo test -p fleetringd --test control -- --ignored write_control_fixture`
  /// before the format changes, and commit both files; never run it again.
  #[test]
  #[ignore = "writes a fixture; run by hand"]
  fn write_control_fixture() -> Result<(), Box<dyn Error>>
  ```

  It opens `CommandLog::open(<dir>/control.db, ControlConfig::new())`, appends 40 commands at indexes 1..=40 with stamps 1,000 ms apart: joins of `n1`..`n4` with zones `a`, `b`, `c`, `a` and addresses `10.0.0.<k>:7400`; `CreateDatabase` of `tenant:acme/notes` (R = 3) and `tenant:acme/billing` (R = 3, `Acks::All`); a heartbeat of each node reporting both databases; `AcquireLease` of each database by its replica set's first node; four `RenewLease`s; a `Drain` of `n4`; a `RevokeToken` of a key; `Tick`s to fill. It saves a snapshot at index 20 (`save_snapshot`), so the fixture exercises snapshot-then-replay. It writes `commands.json`, the 40 commands as a JSON array (`serde_json::to_string_pretty`).
- `tests/fixtures/control-0.3.0/README.md`: two sentences: what the files are, and that they were written by `write_control_fixture` at the commit that added them and must never be regenerated.
- Test `the_control_fixture_recovers_to_its_replay` (not ignored): `CommandLog::open(...).recover()` on a copy of the fixture equals replaying `commands.json` with `apply` from `State::new(ControlConfig::new())`. It guards the fixture itself.

### Step 1: openraft and the type configuration (`feat(fleetringd): add openraft and the control group's types`)

- `Cargo.toml` (crate): `openraft = { version = "=0.9.<newest>", features = ["serde"] }`, pinned with `=` to the newest 0.9.x on crates.io at the time; `rand = "0.8"` (the version openraft 0.9 uses) for the simulation runtime. Check `cargo deny check` passes (licenses are MIT/Apache-2.0) and the MSRV check of `fleetringd` (1.89) still builds.
- **Check before writing anything else:** openraft 0.9's `AsyncRuntime` trait must expose the randomness openraft uses for election timeouts (in 0.9.x, `type ThreadLocalRng` and `fn thread_rng()`). If the pinned version has no such hook, stop: record it in this ticket's Progress and on the issue, and don't work around it (for example by patching openraft); the owner decides. Also record in the module docs the exact signatures of `RaftLogStorage`, `RaftLogReader`, `RaftStateMachine`, `RaftSnapshotBuilder`, `RaftNetworkFactory`, and `RaftNetwork` in the pinned version. Where a signature below differs in detail (an argument's type, an `Option` wrapper), follow openraft and keep this plan's behaviour.
- `src/control/raft/mod.rs` (new; `mod raft;` in `src/control/mod.rs`): module docs summarizing ADR-0035 points 1–4; the type configuration:

  ```rust
  openraft::declare_raft_types!(
      /// The control group's openraft types (ADR-0035 point 1).
      pub ControlTypes:
          D = ControlRequest,
          R = ControlResponse,
          NodeId = RaftId,
          Node = ControlPeer,
          Entry = openraft::Entry<ControlTypes>,
          SnapshotData = std::io::Cursor<Vec<u8>>,
          AsyncRuntime = ControlRuntime,
  );

  /// A control node's Raft ID: the XXH3-64 hash of its node ID (ADR-0035 point 2).
  pub type RaftId = u64;

  #[cfg(not(feature = "simulation"))]
  pub type ControlRuntime = openraft::TokioRuntime;
  #[cfg(feature = "simulation")]
  pub type ControlRuntime = crate::control::raft::sim_runtime::SimRuntime;
  ```

- `src/control/raft/types.rs`:

  ```rust
  /// A control node as the group knows it: its fleet node ID and the address peers dial.
  #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Default, Serialize, Deserialize)]
  pub struct ControlPeer { pub node_id: String, pub address: String }

  /// What a control group's log carries for the state machine.
  #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
  #[serde(rename_all = "snake_case")]
  #[non_exhaustive]
  pub enum ControlRequest { Command(Command) }

  /// What applying a request produced. Built by the local state machine and never sent.
  #[derive(Debug, Clone, Serialize, Deserialize)]
  pub struct ControlResponse {
      pub outcome: Outcome,
      #[serde(skip)]
      pub state: Option<Arc<State>>,
  }
  ```

  `ControlPeer` implements `fmt::Display` as `<node_id>@<address>`. `ControlResponse` implements `Default` only if openraft requires it (outcome `Ok(Applied::Ticked { at: Timestamp::ZERO })` would be wrong; if `Default` is required, use an `Option<Outcome>` field and document that `None` never reaches a caller).
- `src/control/raft/sim_runtime.rs` (only with `simulation`):

  ```rust
  /// openraft's Tokio runtime, but with randomness from the simulation's seed (ADR-0035 point 15).
  #[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
  pub struct SimRuntime;

  /// Seeds the generator every control node on this thread draws from.
  pub fn seed_raft_rng(seed: u64);
  ```

  `SimRuntime` implements `AsyncRuntime` by delegating every associated type and function to `openraft::TokioRuntime`, except `ThreadLocalRng`, which is `SimThreadRng`: a zero-sized `rand::RngCore` that borrows a `thread_local!` `RefCell<rand::rngs::StdRng>`, seeded with `StdRng::seed_from_u64(0)` until `seed_raft_rng` replaces it. Under turmoil every host runs on one thread in an order the seed fixes, so the draws replay.
- Tests in `src/control/raft/types.rs`: `a_control_request_round_trips_through_json` (a `Command` with every field set survives `serde_json`); `a_response_skips_its_state` (serializing a response with `Some(state)` and reading it back gives `None`). In `sim_runtime.rs`: `the_same_seed_draws_the_same_numbers`.

### Step 2: Raft IDs (`feat(fleetringd): derive control nodes' Raft IDs from their node IDs`)

- `src/control/raft/id.rs`:

  ```rust
  /// The Raft ID of the control node `node`: XXH3-64 of its UTF-8 bytes.
  pub fn raft_id(node: &NodeId) -> RaftId;

  /// Why a node can't join a control group under its Raft ID.
  #[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
  #[non_exhaustive]
  pub enum RaftIdError {
      #[error("node '{node}' has raft id 0, which openraft reserves; give it another node id")]
      Zero { node: NodeId },
      #[error("node '{node}' has the same raft id as '{other}'; give one of them another node id")]
      Collision { node: NodeId, other: NodeId },
  }

  /// Checks that `node` can join a group whose members are `members`.
  pub fn check_raft_id(node: &NodeId, members: &BTreeMap<RaftId, ControlPeer>) -> Result<RaftId, RaftIdError>;
  ```

  Use `twox_hash::XxHash3_64::oneshot(node.as_str().as_bytes())`. A member whose `node_id` equals `node` is the same node, not a collision.
- Tests: `raft_ids_are_stable` (the ID of `"control-1"` equals a literal value computed once and pasted in, so a hasher change fails the test); `a_collision_names_both_nodes` (a members map with another node under the same ID).

### Step 3: the format-2 store (`feat(fleetringd): store the control log in Raft's format`)

- `src/control/log.rs` becomes the format-2 store. Keep the type name `CommandLog` (it is the control log), `LogIndex`, and `LogError`. `FORMAT_VERSION` becomes `2`. Schema:

  ```sql
  CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL) STRICT;
  CREATE TABLE IF NOT EXISTS log (
      log_index INTEGER PRIMARY KEY, term INTEGER NOT NULL, leader_node INTEGER NOT NULL,
      entry TEXT NOT NULL) STRICT;
  CREATE TABLE IF NOT EXISTS snapshots (
      log_index INTEGER PRIMARY KEY, term INTEGER NOT NULL, leader_node INTEGER NOT NULL,
      membership TEXT NOT NULL, state TEXT NOT NULL) STRICT;
  ```

  `meta` keys: `format` (`"2"`), `genesis` (JSON `ControlConfig`), `vote` (JSON `openraft::Vote<RaftId>`), `committed` (JSON `LogId<RaftId>`), `purged` (JSON `LogId<RaftId>`). `entry` is the JSON of `openraft::EntryPayload<ControlTypes>` (normal, membership, or blank). `membership` is the JSON of `StoredMembership<RaftId, ControlPeer>`. WAL mode and `synchronous=FULL` as today.
- `LogError` gains `Migration { reason: String }` ("can't migrate the control log from format 1: {reason}") and `CorruptEntry { index: LogIndex, source: serde_json::Error }` ("control log entry {index} is corrupt"); `CorruptCommand` stays for format 1.
- `CommandLog` methods (blocking; callers run them as point 6 of step 5 says):

  ```rust
  pub fn open(path: impl Into<PathBuf>, config: ControlConfig, own: Option<(RaftId, ControlPeer)>) -> Result<Self, LogError>;
  pub fn genesis(&self) -> ControlConfig;
  pub fn vote(&self) -> Result<Option<Vote<RaftId>>, LogError>;
  pub fn save_vote(&mut self, vote: &Vote<RaftId>) -> Result<(), LogError>;
  pub fn committed(&self) -> Result<Option<LogId<RaftId>>, LogError>;
  pub fn save_committed(&mut self, committed: Option<LogId<RaftId>>) -> Result<(), LogError>;
  pub fn purged(&self) -> Result<Option<LogId<RaftId>>, LogError>;
  pub fn last_log_id(&self) -> Result<Option<LogId<RaftId>>, LogError>;
  /// Appends every entry in one transaction and waits until they are on stable storage.
  pub fn append_entries(&mut self, entries: &[Entry<ControlTypes>]) -> Result<(), LogError>;
  /// Removes the entries at `from` and after.
  pub fn truncate_from(&mut self, from: LogIndex) -> Result<(), LogError>;
  /// Removes the entries up to and including `upto`, and records it as purged.
  pub fn purge_through(&mut self, upto: &LogId<RaftId>) -> Result<(), LogError>;
  pub fn entries(&self, range: Range<u64>) -> Result<Vec<Entry<ControlTypes>>, LogError>;
  pub fn save_control_snapshot(&mut self, snapshot: &StoredSnapshot, retained: NonZeroU32) -> Result<(), LogError>;
  pub fn control_snapshots(&self) -> Result<Vec<StoredSnapshot>, LogError>; // oldest first
  pub fn latest_control_snapshot(&self) -> Result<Option<StoredSnapshot>, LogError>;
  /// The committed control commands after `after`, with their indexes, skipping Raft's own entries.
  pub fn committed_commands_after(&self, after: LogIndex) -> Result<Vec<(LogIndex, Command)>, LogError>;
  ```

  ```rust
  /// A control snapshot as stored: the state after the entry at `last_log_id`, and the group then.
  #[derive(Debug, Clone, PartialEq)]
  pub struct StoredSnapshot {
      pub last_log_id: LogId<RaftId>,
      pub membership: StoredMembership<RaftId, ControlPeer>,
      pub state: State,
  }
  ```

  `save_control_snapshot` inserts the row and deletes all but the newest `retained` rows, in one transaction. `check_state` runs on every snapshot read, as today. Format-1 functions (`append`, `append_all`, `save_snapshot`, `commands`, `recover`) move to a private `format1` module used only by migration, and by the step-0 fixture test, which switches to `fleetringd::control::format1_recover(path)` (`#[doc(hidden)] pub`) so the fixture keeps being checked.
- **Migration** in `open`: if `meta.format` is `"1"`:
  1. Recover with the format-1 code: `Recovered { state, last_index: N, .. }`.
  2. `own` must be `Some`; otherwise `LogError::Migration { reason: "the node's raft id is needed" }`.
  3. In one `BEGIN IMMEDIATE` transaction: `ALTER TABLE commands RENAME TO commands_v1`; if the format-1 `snapshots` table exists, rename it to `snapshots_v1`; create the format-2 tables; if N > 0, insert a snapshot row at `log_index = N, term = 0, leader_node = 0` with `membership = StoredMembership::new(Some(LogId::new(CommittedLeaderId::new(0, 0), N)), Membership::new(vec![btreeset!{own_id}], btreemap!{own_id => own_peer}))` and the recovered state, and set `purged` and `committed` to that log ID; set `format` to `"2"`; commit.
  4. Log at info: `migrated the control log from format 1 at index {N}`.

  An empty format-1 log (N = 0) gets the format-2 tables and no snapshot; it bootstraps as new (step 5).
- `src/control/raft/store.rs`: `ControlLogStore` (`Clone`, holding `Arc<Mutex<CommandLog>>` and a `StorageIo`) implementing `RaftLogStorage<ControlTypes>` and `RaftLogReader<ControlTypes>`: `get_log_state` (last purged and last log ID), `save_vote`/`read_vote`, `save_committed`/`read_committed`, `append(entries, callback)` (write, then `callback.log_io_completed(Ok(()))`), `truncate(log_id)` (`truncate_from(log_id.index)`), `purge(log_id)`, `try_get_log_entries(range)`, `get_log_reader`. Under `StorageIo::DedicatedThread` every call runs in `tokio::task::spawn_blocking`; under `StorageIo::Inline` it runs on the calling task.
- Tests in `tests/control.rs` (module `log`):
  - `appended_entries_survive_reopening`: append 10 entries (normal, membership, blank), reopen, `entries(1..11)` equals them; `last_log_id` is the tenth.
  - `truncation_removes_the_tail_and_purging_the_head`: append 10, `truncate_from(8)`, `purge_through(index 3)`; entries 4..=7 remain and `purged` is index 3.
  - `vote_and_committed_survive_reopening`.
  - `only_the_newest_snapshots_are_kept`: save 4 snapshots with `retained = 2`; `control_snapshots()` returns the last two.
  - `committed_commands_skip_raft_entries_and_stop_at_committed`: entries 1..=6 with a membership at 2 and a blank at 4, committed at 5; returns the commands at 1, 3, 5.
  - `a_format_one_log_migrates_to_a_snapshot_of_the_same_state`: copy the step-0 fixture, `open` with `own = Some(..)`; `latest_control_snapshot()` is at index 40 with the fixture's replayed state and a one-voter membership; `commands_v1` still holds 40 rows; `format` is `2`.
  - `an_empty_format_one_log_migrates_to_an_empty_store`.
  - `a_format_one_log_without_an_id_is_refused`: `own = None` gives `LogError::Migration`.
  - `a_newer_format_is_refused`: `format = 3` gives `UnsupportedFormat { found: "3", supported: 2 }`.

### Step 4: the state machine (`feat(fleetringd): apply committed control commands as openraft's state machine`)

- `src/control/raft/state_machine.rs`:

  ```rust
  /// The control state machine: `apply` over committed entries, with control snapshots (ADR-0035 point 4).
  #[derive(Debug, Clone)]
  pub(crate) struct ControlStateMachine { inner: Arc<Mutex<Inner>>, log: ControlLogStore, published: watch::Sender<Applied>, retained: NonZeroU32 }

  struct Inner { state: State, last_applied: Option<LogId<RaftId>>, membership: StoredMembership<RaftId, ControlPeer> }

  /// The state after the last applied entry, with that entry's index.
  #[derive(Debug, Clone)]
  pub struct AppliedState { pub index: LogIndex, pub state: Arc<State> }
  ```

  `ControlStateMachine::open(log: ControlLogStore, genesis: ControlConfig, retained: NonZeroU32) -> Result<(Self, watch::Receiver<AppliedState>), LogError>` starts from the newest control snapshot (or `State::new(genesis)` with nothing applied) and does **not** replay: openraft re-applies the committed entries after `last_applied` itself at startup, because `read_committed` returns the saved `committed`.
- `RaftStateMachine<ControlTypes>`:
  - `applied_state()` returns `(last_applied, membership)`.
  - `apply(entries)`: for each entry in order: a normal `ControlRequest::Command(command)` runs `fleetring_control::apply(&mut state, command)`; publishes `AppliedState { index, state: Arc::new(state.clone()) }` on `published` (`send_replace`) after every entry, accepted or rejected, so the published index always moves; returns `ControlResponse { outcome, state: Some(<the Arc just published>) }`. A membership entry updates `membership` and returns a response with outcome `Ok(Applied::Ticked { at: state.now() })` and `state: None` (openraft discards it). A blank entry returns the same. `last_applied` moves to each entry's log ID.
  - `get_snapshot_builder()` returns a builder over a clone of `Inner`; `build_snapshot()` serializes `{"state": <State>}` with `serde_json`, stores a `StoredSnapshot` via `save_control_snapshot(.., retained)`, and returns `Snapshot { meta: SnapshotMeta { last_log_id, last_membership, snapshot_id: format!("{}-{}", term, index) }, snapshot: Box::new(Cursor::new(bytes)) }`.
  - `begin_receiving_snapshot()` returns an empty cursor.
  - `install_snapshot(meta, data)`: parses `{"state": State}`; checks it with `check_state` against the store's genesis, **except** on a node whose store has no `genesis` yet (a fresh joiner), which records the snapshot state's `config()` as its genesis first; stores it; replaces `Inner`; publishes it.
  - `get_current_snapshot()` returns the newest stored control snapshot.
- Tests in `src/control/raft/state_machine.rs` (`#[cfg(test)]`, `#[tokio::test]`):
  - `applying_entries_matches_applying_commands`: the step-0 fixture's 40 commands as normal entries, plus a membership entry at 5 and a blank at 9; the final state equals replaying the commands with `apply`; outcomes are equal in order.
  - `a_rejection_still_moves_the_applied_index`.
  - `a_built_snapshot_installs_to_the_same_state`: build on one machine, install on another fresh one; states equal, `applied_state()` equal.
  - `a_snapshot_with_another_configuration_is_refused`: a node with genesis A refuses a snapshot whose state has config B (`check_state`).

### Step 5: the control node over Raft (`feat(fleetringd): run the control node as a Raft group`)

- `src/control/node.rs` replaces the writer. Public surface:

  ```rust
  /// Whether a control node with an empty store starts a group or waits to be added (ADR-0035 point 10).
  #[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
  #[serde(rename_all = "lowercase")]
  #[non_exhaustive]
  pub enum Bootstrap { #[default] New, Join }

  impl ControlOptions {
      pub const fn with_election_timeout(self, timeout: Duration) -> Self;   // default 1 s
      pub const fn with_raft_heartbeat(self, interval: Duration) -> Self;    // default 250 ms
      pub const fn with_log_keep(self, entries: NonZeroU64) -> Self;         // default 1,000
      pub const fn with_snapshots_retained(self, count: NonZeroU32) -> Self; // default 2
      pub const fn with_bootstrap(self, bootstrap: Bootstrap) -> Self;       // default New
  }

  impl ControlNode {
      pub async fn open(
          path: impl Into<PathBuf>,
          options: ControlOptions,
          clock: impl Clock + 'static,
          own: ControlPeer,
          network: Network,
      ) -> Result<Self, OpenError>;
      /// The service that answers this node's peers' Raft calls.
      pub fn raft_service(&self) -> RaftService;
  }
  ```

  `with_mailbox_capacity` stays and now bounds concurrent `submit` calls with a `tokio::sync::Semaphore` of that many permits. `StorageIo` stays (step 3).
- `open`:
  1. `own_id = raft_id(&own.node_id.parse()?)`; a node ID that doesn't parse or hashes to 0 is `OpenError::RaftId(RaftIdError)`.
  2. `CommandLog::open(path, options.config(), Some((own_id, own.clone())))`, wrapped in `ControlLogStore`; `ControlStateMachine::open`.
  3. `openraft::Config { cluster_name: "fleetring-control".into(), heartbeat_interval: raft_heartbeat_ms, election_timeout_min: election_timeout_ms, election_timeout_max: 2 * election_timeout_ms, snapshot_policy: SnapshotPolicy::LogsSinceLast(snapshot_every), max_in_snapshot_log_to_keep: log_keep, purge_batch_size: 256, snapshot_max_chunk_size: 1 << 20, ..Default::default() }.validate()`; an invalid configuration is `OpenError::RaftConfig { reason }`.
  4. `Raft::new(own_id, config, ControlNetwork::new(network), log_store, state_machine)`; a failure is `OpenError::Raft { reason }`.
  5. If the store has no vote, no log, and no snapshot: with `Bootstrap::New` call `raft.initialize(btreemap!{own_id => own})`; with `Bootstrap::Join` log at info `waiting to be added to a control group` and do nothing.
  6. Log `control state recovered` with the last applied index, the snapshot index, and the bootstrap taken, as `started` does today.
- `OpenError` gains `RaftId(#[from] RaftIdError)`, `RaftConfig { reason: String }` ("invalid raft configuration: {reason}"), and `Raft { reason: String }` ("can't start the control group: {reason}"). `Thread` and `Stopped` go.
- `SubmitError` gains `NotLeader { leader: Option<String> }` ("this control node isn't the leader"); `Storage` stays for a fatal openraft storage error; `Stopped` for a shut-down group.
- `ControlHandle` (still `Clone`, cheap):

  ```rust
  impl ControlHandle {
      pub async fn submit(&self, action: Action) -> Result<Committed, SubmitError>;
      pub fn state(&self) -> Arc<State>;
      pub fn watch(&self) -> watch::Receiver<Arc<State>>;
      /// The state after the last applied entry, with its index.
      pub fn applied(&self) -> AppliedState;
      /// Waits until a read here sees every command committed before it was called.
      pub async fn ensure_linearizable(&self) -> Result<(), SubmitError>;
      pub fn leadership(&self) -> watch::Receiver<Leadership>;
      pub fn group(&self) -> ControlGroupView;
      pub async fn add_node(&self, node: NodeId, address: String) -> Result<ControlGroupView, GroupError>;
      pub async fn set_voters(&self, voters: BTreeSet<NodeId>) -> Result<ControlGroupView, GroupError>;
      pub async fn remove_node(&self, node: NodeId) -> Result<ControlGroupView, GroupError>;
  }

  /// Who leads the control group, as this node sees it.
  #[derive(Debug, Clone, PartialEq, Eq)]
  pub struct Leadership { pub term: u64, pub leader: Option<ControlPeer>, pub own: ControlPeer }
  impl Leadership { pub fn is_own(&self) -> bool; }

  /// The control group as this node sees it.
  #[derive(Debug, Clone, PartialEq, Eq)]
  pub struct ControlGroupView { pub members: Vec<GroupMember>, pub term: u64, pub committed_index: LogIndex, pub leader: Option<ControlPeer> }

  #[derive(Debug, Clone, PartialEq, Eq)]
  pub struct GroupMember { pub peer: ControlPeer, pub voter: Voting, pub matched_index: Option<LogIndex> }

  #[derive(Debug, Clone, Copy, PartialEq, Eq)]
  #[non_exhaustive]
  pub enum Voting { Voter, Learner }
  ```

  `submit`: acquire a permit; `let command = Command::new(self.clock.now(), action)` immediately before `self.raft.client_write(ControlRequest::Command(command.clone())).await`; on success return `Committed { index: LogIndex::new(response.log_id.index), command, outcome: response.data.outcome, state: response.data.state.unwrap_or_else(|| self.state()) }`; `ClientWriteError::ForwardToLeader(f)` is `NotLeader { leader: f.leader_node.map(|peer| peer.address) }`; a fatal error is `Storage` if openraft reports a storage error, else `Stopped`. `state()` and `watch()` read the published `AppliedState` (the `watch()` receiver maps through a `tokio::sync::watch` of `Arc<State>` updated alongside, so existing callers are unchanged). `ensure_linearizable` maps `CheckIsLeaderError::ForwardToLeader` to `NotLeader`. `leadership()` is derived from `raft.metrics()` (`current_term`, `current_leader`, the membership's node for that ID) by a task that sends only on change. `group()` reads `raft.metrics()`: members from `membership_config`, matched indexes from `replication` (leader only; `None` elsewhere).
- `GroupError` (in `node.rs`):

  ```rust
  #[derive(Debug, thiserror::Error)]
  #[non_exhaustive]
  pub enum GroupError {
      #[error("this control node isn't the leader")] NotLeader { leader: Option<String> },
      #[error(transparent)] RaftId(#[from] RaftIdError),
      #[error("{count} voters can't form a majority that survives a failure; give an odd number")] EvenVoters { count: usize },
      #[error("{count} voters is more than the 7 a control group may have")] TooManyVoters { count: usize },
      #[error("node '{node}' isn't a member of the control group; add it first with `fleetringd control add`")] UnknownNode { node: NodeId },
      #[error("node '{node}' is {behind} entries behind the leader; wait until it catches up")] NotCaughtUp { node: NodeId, behind: u64 },
      #[error("node '{node}' is a voter; remove it from the voters with `fleetringd control set-voters` first")] StillVoter { node: NodeId },
      #[error("the control group refused the change: {reason}")] Refused { reason: String },
      #[error("the control node is not running")] Stopped,
  }
  ```

  - `add_node`: `check_raft_id` against the current members; `raft.add_learner(id, ControlPeer { node_id, address }, true)` (blocking until caught up); returns `group()`. Adding a node already in the group with the same address returns the group unchanged.
  - `set_voters`: refuses an empty set or an even count (`EvenVoters`), more than 7 (`TooManyVoters`), a node not in the group (`UnknownNode`), and a learner more than `CAUGHT_UP = 100` entries behind the leader's last log index (`NotCaughtUp`); then `raft.change_membership(ids, false)` (joint consensus; learners not named stay learners).
  - `remove_node`: refuses a voter (`StillVoter`) and an unknown node; then `change_membership(ChangeMembers::RemoveNodes(btreeset!{id}), false)`.
- `src/control/node.rs` keeps `RecoverySummary` with `last_index` (last applied), `snapshot`, and `replayed` (openraft's re-applied count, from the state machine's counter).
- `tests/control.rs` keeps every existing test of the control node, run unchanged against a one-voter group (they exercise `submit`, rejections, recovery, and snapshots); adjust only constructors (`ControlNode::open` takes `own` and `network`; tests pass `ControlPeer { node_id: "control".into(), address: "127.0.0.1:0".into() }` and `Network::tcp(TransportConfig::default())`). New tests:
  - `a_new_control_node_elects_itself`: `open` on an empty directory; `leadership()` reaches `is_own()`; a `Join` submits.
  - `a_joining_control_node_waits_to_be_added`: `Bootstrap::Join` on an empty directory; `submit` returns `NotLeader { leader: None }` after 3 election timeouts.
  - `the_migrated_fixture_elects_itself_with_the_same_state`: copy the step-0 fixture; `open`; `state()` equals the fixture's replayed state; a submitted `Tick` is committed at an index greater than 40 (openraft may append a blank entry first, so assert only that).
  - `snapshots_compact_the_log`: `snapshot_every = 10`, `log_keep = 5`, `snapshots_retained = 2`; submit 100 ticks; the store holds at most 15 log entries and 2 control snapshots, and reopening gives the same state.

### Step 6: the Raft network and its binding (`feat(fleetringd): carry Raft messages over gRPC, bound to the control group`)

- `crates/fleetring-proto/proto/fleetring/v1/fleetring.proto`, after the `Replication` service:

  ```proto
  // ---- Raft -------------------------------------------------------------------------------------

  // The control group's Raft messages (ADR-0035). Payloads are openraft's request and response
  // types as JSON: internal to the control group, versioned by the control log's format and the
  // protocol's minor version, and opaque to everything else.
  service Raft {
    // Scope: node.
    // Binding: control peer.
    rpc AppendEntries(RaftPayload) returns (RaftPayload);
    // Scope: node.
    // Binding: control peer.
    rpc Vote(RaftPayload) returns (RaftPayload);
    // Scope: node.
    // Binding: control peer.
    rpc InstallSnapshot(RaftPayload) returns (RaftPayload);
  }

  message RaftPayload {
    bytes json = 1;
  }
  ```

  Extend the header comment's list of bindings with `` `control peer` (a voter or learner of the receiver's control group) ``.
- `crates/fleetring-proto/src/lib.rs`: `PROTOCOL_VERSION` minor + 1 (from `1.0` to `1.1`, or to the next minor if FR-066 landed first). Update the module docs to name the `Raft` service.
- `src/auth/binding.rs`: `Binding::ControlPeer` (`as_str` `"control peer"`); `Unbound::NotControlPeer` with the reason `not a member of the control group`.
- `src/auth/policy.rs`: three rows `rpc("/fleetring.v1.Raft/AppendEntries", Requirement::Node, Binding::ControlPeer)` and the same for `Vote` and `InstallSnapshot`. The table tests that compare `RPCS` with the proto's `Scope:` and `Binding:` lines pick them up; the README's table gains the rows (step 13).
- `src/transport.rs`: `raft_client(channel: Channel) -> RaftClient<Channel>` and `raft_server<S: Raft>(service: S) -> RaftServer<S>`, with `MAX_MESSAGE_BYTES` like the others.
- `src/control/raft/network.rs`:

  ```rust
  /// Dials the control group's peers over the node's network (ADR-0035 point 9).
  #[derive(Debug, Clone)]
  pub(crate) struct ControlNetwork { peers: Arc<Peers> }
  impl RaftNetworkFactory<ControlTypes> for ControlNetwork { type Network = PeerConnection; async fn new_client(&mut self, target: RaftId, node: &ControlPeer) -> PeerConnection; }
  pub(crate) struct PeerConnection { target: RaftId, address: String, peers: Arc<Peers> }
  impl RaftNetwork<ControlTypes> for PeerConnection { append_entries, vote, install_snapshot }
  ```

  Each call serializes the openraft request with `serde_json::to_vec`, sends it as `RaftPayload { json }` with the timeout from `RPCOption::hard_ttl()`, and deserializes the reply's `json` into openraft's response type (whose `Result` carries openraft's own remote errors). A dial failure, a timeout, or `UNAVAILABLE` is `RPCError::Unreachable` (and the channel is forgotten with `peers.forget(&address)`); any other status, or a payload that doesn't decode, is `RPCError::Network` with the status's message.
- `src/control/raft/service.rs`:

  ```rust
  /// Answers the control group's Raft calls for one node.
  #[derive(Debug, Clone)]
  pub struct RaftService { raft: Raft<ControlTypes> }
  #[tonic::async_trait] impl fleetring_proto::v1::raft_server::Raft for RaftService { ... }
  ```

  Each handler first checks the binding: on a TLS connection, `Peer::of(&request)`'s node ID must be the `node_id` of a member (voter or learner) of `raft.metrics().borrow().membership_config`; if that membership is empty (a node that has never been added), any node certificate passes. A failure is `BindingDenied` with `Binding::ControlPeer` and `Unbound::NotControlPeer`, audited as ADR-0031 point 5 says. Plaintext connections make no check (ADR-0031 amendment). Then it decodes the payload (`INVALID_ARGUMENT` if it doesn't decode), calls `raft.append_entries`, `raft.vote`, or `raft.install_snapshot`, and encodes the result, `Ok` or openraft's error, as JSON.
- `src/daemon.rs`: `serve` adds `.add_optional_service(control.as_ref().map(|role| raft_server(role.node.raft_service())))`.
- Tests in `tests/control.rs` (module `raft_network`), three control nodes in one process over TCP with node certificates from `support`:
  - `three_control_nodes_elect_one_leader`: open `c1` (`New`), `c2` and `c3` (`Join`); `c1.handle().add_node` twice, `set_voters({c1, c2, c3})`; every node's `leadership()` agrees on one leader and term; a `Join` submitted on the leader shows in all three `state()`s.
  - `a_node_outside_the_group_is_refused_raft_calls`: a fourth node certificate `n9` calls `Raft.Vote` on `c1` with a valid payload: `PERMISSION_DENIED`, `fleetring-binding: control peer`.
  - `a_fresh_joiner_accepts_its_first_leader`: `c2` with `Join` and no state accepts `AppendEntries` from `c1` before it has a membership (covered by the first test; assert explicitly that `c2`'s first membership names `c1`).

### Step 7: the service on a group (`feat(fleetringd): answer control calls on the leader and redirect elsewhere`)

- `crates/fleetring-proto/proto/fleetring/v1/fleetring.proto`:

  ```proto
  // A member of the control group (ADR-0035).
  message ControlNode {
    string node_id = 1;
    // `host:port`.
    string address = 2;
    // Set for a voter; unset for a learner.
    bool voter = 3;
    // Set for the leader.
    bool leader = 4;
    // The last log index the leader knows this member holds; 0 if unknown.
    uint64 matched_index = 5;
  }

  message JoinResponse {
    ...
    // The control group, so the node can reach another member when this one fails. Empty from
    // a control node that predates the field.
    repeated ControlNode control_nodes = 4;
  }

  message HeartbeatResponse {
    ...
    // As in `JoinResponse`.
    repeated ControlNode control_nodes = 4;
  }

  message AddControlNodeRequest {
    string node_id = 1;
    // `host:port`.
    string address = 2;
  }
  message RemoveControlNodeRequest { string node_id = 1; }
  message SetControlVotersRequest { repeated string node_ids = 1; }
  message DescribeControlGroupRequest {}
  message ControlGroup {
    repeated ControlNode nodes = 1;
    // The control group's Raft term.
    uint64 term = 2;
    uint64 committed_index = 3;
    // The leader's node ID; empty while there is none.
    string leader = 4;
  }
  ```

  In `service Control`, after `RevokeToken`:

  ```proto
  // Adds a control node to the control group as a learner, and returns once it has caught up
  // (ADR-0035). It votes only once SetControlVoters names it.
  // Scope: fleet.
  rpc AddControlNode(AddControlNodeRequest) returns (ControlGroup);
  // Removes a learner from the control group.
  // Scope: fleet.
  rpc RemoveControlNode(RemoveControlNodeRequest) returns (ControlGroup);
  // Makes exactly these members the voters: an odd number, at most seven, each caught up.
  // Scope: fleet.
  rpc SetControlVoters(SetControlVotersRequest) returns (ControlGroup);
  // Scope: fleet.
  rpc DescribeControlGroup(DescribeControlGroupRequest) returns (ControlGroup);
  ```

  Extend the `Control` header comment: every `Control` RPC is answered by the control group's leader, and another control node answers `UNAVAILABLE` naming the leader in `fleetring-leader`.
- `src/auth/policy.rs`: the four group RPCs as `rpc(path, Requirement::Fleet, Binding::None)`, so node certificates are refused.
- `src/control/service.rs`:
  - `submit_status`: `SubmitError::NotLeader { leader: Some(address) }` is `not_leader(&address)`; `NotLeader { leader: None }` is `Status::unavailable("no control leader yet; retry")`. `GroupError::NotLeader` maps the same way; `RaftId`, `EvenVoters`, `TooManyVoters` are `INVALID_ARGUMENT`; `UnknownNode` is `NOT_FOUND`; `NotCaughtUp`, `StillVoter` are `FAILED_PRECONDITION`; `Refused` and `Stopped` are `UNAVAILABLE`.
  - `locate`, `list_databases`, and `watch_membership` call `self.node.ensure_linearizable().await.map_err(submit_status)?` before reading state.
  - `watch_membership` merges the state stream with `self.node.leadership()`: when leadership changes so that `!is_own()`, the stream yields `Err(not_leader(address))` (or the no-leader `UNAVAILABLE`) and ends.
  - `join` and `heartbeat` fill `control_nodes` from `self.node.group()` via `fn control_nodes_to_proto(view: &ControlGroupView) -> Vec<proto::ControlNode>` (in `src/data/convert.rs`).
  - The four group RPCs call `add_node`, `remove_node`, `set_voters`, and `group()` (after `ensure_linearizable`), and convert the view with `fn control_group_to_proto(view: &ControlGroupView) -> proto::ControlGroup`.
- `src/control/absent.rs`: `NoControl` refuses the four new RPCs like the rest.
- Tests in `tests/control.rs`:
  - `a_follower_redirects_every_control_call_to_the_leader`: a three-node group; for every `Control` RPC sent to a follower with a `fleet` principal or a node certificate as the table allows, the status is `UNAVAILABLE` with `fleetring-leader` equal to the leader's address (iterate over `auth::RPCS` rows of service `Control`, so a new RPC is covered automatically).
  - `a_read_after_a_write_sees_it_after_a_leader_change`: create a database on the leader; shut the leader down; once another leads, `Locate` through a `ControlClient` given all three addresses finds the database.
  - `watch_membership_ends_when_its_node_stops_leading`: open the stream on the leader of a three-node group; partition nothing, but shut the leader's daemon down gracefully; the stream ends with `UNAVAILABLE`; reopening it through a `ControlClient` given all three addresses reaches the new leader, and a `Join` submitted afterwards arrives on the reopened stream as an update.
  - `a_watch_on_a_follower_is_redirected`: opening `WatchMembership` on a follower fails at once with `UNAVAILABLE` and `fleetring-leader` naming the leader.
  - `join_and_heartbeat_replies_name_the_control_group`.
  - `group_calls_need_the_fleet_scope`: a principal with `admin:*` only gets `PERMISSION_DENIED` on each group RPC; a node certificate too.

### Step 8: the failover timer on the leader only (`feat(fleetringd): run failover on the control group's leader, after a settle window`)

- `src/control/failover.rs`: `run` becomes:

  ```text
  loop {
      wait until handle.leadership().borrow().is_own() (or the handle is gone: return)
      let term = leadership.term; let started = clock.now(); notified.clear();
      log info "leading the control group; promotions wait until {settled}"
      while leadership is own and its term == term {
          the existing tick body, with `started` as above
          a submit that returns NotLeader breaks this inner loop; Stopped or Storage returns
      }
  }
  ```

  The settle window, a lease duration plus the failover margin from `started`, therefore restarts at every new term this node leads (ADR-0022 point 6, ADR-0035 point 7). Ticks are submitted only by the leader.
- Tests in `tests/control.rs` (paused Tokio time, one-voter group with `SimClock`):
  - `a_new_leader_waits_a_lease_duration_before_promoting`: a database whose lease expired before the node starts; no `StartPromotion` is logged before `lease_duration + failover_margin` after the node becomes leader, and one is logged after.
  - `a_follower_submits_no_ticks`: in a three-node group, the followers' logs contain no `Tick` they proposed (every `Tick` in the committed log was proposed while its proposer led: check that `Tick` stamps never come from a node's clock while it wasn't leader, by giving each node a distinct clock base 1,000,000 ms apart and checking that each `Tick`'s stamp falls in the leader's range at that index).

### Step 9: configuration and the daemon (`feat(fleetringd): configure the control group`)

- `src/config.rs`, `ControlSettings` gains (all `#[serde(default)]` through the struct's default):

  | Field | Type | Default | Meaning |
  |---|---|---|---|
  | `addresses` | `Vec<SocketAddr>` | empty | Control nodes to reach, as an alternative to `address`. |
  | `bootstrap` | `Bootstrap` | `"new"` | What a control node with an empty store does. |
  | `election_timeout_ms` | `NonZeroU64` | 1,000 | The shortest election timeout; the longest is twice it. |
  | `raft_heartbeat_ms` | `NonZeroU64` | 250 | How often the leader sends Raft heartbeats. |
  | `log_keep` | `NonZeroU64` | 1,000 | Log entries kept behind the newest control snapshot. |
  | `snapshots_retained` | `NonZeroU32` | 2 | Control snapshots kept on disk. |

  Update the `[control]` doc example to list them. `ControlSettings::control_addresses(&self) -> Vec<SocketAddr>` returns `addresses` if non-empty, else `address` as a one-element list, else empty.
- `Config::validate` additions, in this order:
  - A data-only node with both `address` and non-empty `addresses`: `ConfigError::Invalid { field: "control.addresses", reason: "set control.address or control.addresses, not both" }`. With neither: the existing `Missing { field: "control.address", reason: "a data-only node must know a control node's address: set control.address or control.addresses" }`.
  - `election_timeout_ms < 3 * raft_heartbeat_ms`: `Invalid { field: "control.election_timeout_ms", reason: "{e} ms is less than 3 raft heartbeats (3 × control.raft_heartbeat_ms = {3h} ms)" }`.
  - `lease_duration_ms < 2 * (2 * election_timeout_ms)`: `Invalid { field: "control.lease_duration_ms", reason: "{l} ms is less than twice the longest election timeout (2 × 2 × control.election_timeout_ms = {x} ms)" }`.
  - Config tests for each, plus `the_defaults_validate`.
- `Config::control_target` returns the first of `control_addresses()`, or `listen` for a control role as today.
- `src/environment.rs`: `Network::control_client(&self, addresses: Vec<String>) -> ControlClient` (was one address).
- `src/daemon.rs`:
  - Bind the listener **before** opening the control node, so the control node knows its advertised address: `own = ControlPeer { node_id: config.node.id.to_string(), address: advertised(&config, bound).to_string() }`.
  - `open_control` passes `own`, `environment.network().clone()`, and the new options from `ControlSettings`.
  - `control_address` becomes `control_addresses(config, bound) -> Vec<String>`: the configured list, or the node's own listener when it runs control.
  - `NoControl::new` takes the first configured address, as today.
- `deploy/fleetringd.toml`: the new `[control]` fields, commented, with their defaults and one sentence each.

### Step 10: the client on a group (`feat(fleetringd): follow the control group from any of its addresses`)

- `src/control/client.rs`:
  - `Shared.leader: Mutex<String>` becomes `Shared.addresses: Mutex<Addresses>`:

    ```rust
    struct Addresses { configured: Vec<String>, learned: Vec<String>, current: String }
    ```

  - `ControlClient::new(address, transport)` stays (one address); add `ControlClient::with_addresses(addresses: Vec<String>, transport: TransportConfig) -> Self` and `with_connector_at(addresses: Vec<String>, connect: …) -> Self`; `with_connector(address, connect)` stays as `with_connector_at(vec![address], connect)`. An empty list is a programming error: `# Panics` documents it, as `with_credentials` does.
  - `learn(&self, nodes: &[fleetring_proto::v1::ControlNode])`: replaces `learned` with the nodes' addresses (deduplicated against `configured`), and if one is marked `leader`, makes it `current`.
  - `leader()` returns `current`.
  - `following`: on success, return. On a redirect hint, make it `current` (adding it to `learned` if unknown). On `UNAVAILABLE` without a hint, or when `current()` can't build a channel, move `current` to the next address after it in `configured` then `learned` order (wrapping), and try again. The attempt gives up after `MAX_REDIRECTS + <number of known addresses>` moves and returns the last status, for `retry` to back off and call again.
- `src/membership.rs`: after every successful `join` and `heartbeat`, call `control.learn(&response.control_nodes)`.
- Tests in `tests/control.rs` (module `client`), against stub `Control` servers on TCP (as the existing redirect tests do):
  - `the_client_moves_to_the_next_address_when_one_is_down`: addresses `[dead, live]`; `heartbeat` succeeds against `live`.
  - `the_client_follows_a_learned_leader`: one configured address that answers `join` with `control_nodes` naming another as leader; the next call goes to it.
  - `the_client_gives_up_after_every_address_has_failed`: three dead addresses; `UNAVAILABLE` after bounded moves (assert the stub call counts).

### Step 11: the `control` command (`feat(fleetringd): add fleetringd control add, remove, set-voters, and list`)

- `src/control/client.rs`: `add_control_node`, `remove_control_node`, `set_control_voters` (sent once per leader, through `following`), and `describe_control_group` (retried), each returning `ControlGroup`.
- `src/main.rs`:

  ```rust
  /// Changes and shows the control group: the control nodes that replicate the control log.
  #[command(subcommand)]
  Control(ControlCommand),

  #[derive(Debug, Subcommand)]
  enum ControlCommand {
      /// Adds a control node as a learner and waits until it has caught up. It votes once
      /// `control set-voters` names it. Start the node with `[control] bootstrap = "join"` first.
      Add { id: NodeId, #[arg(long)] address: String, #[command(flatten)] target: Target },
      /// Makes exactly these control nodes the voters: an odd number, at most seven.
      SetVoters { #[arg(long = "voter", required = true)] voters: Vec<NodeId>, #[command(flatten)] target: Target },
      /// Removes a control node that isn't a voter.
      Remove { id: NodeId, #[arg(long)] yes: bool, #[command(flatten)] target: Target },
      /// Lists the control group's members.
      List { #[command(flatten)] target: Target },
  }
  ```

  `remove` asks `remove control node '<id>' from the control group? type its id to confirm:` on a terminal, and without a terminal and without `--yes` refuses with exit code 2, as `db drop` does. Output of `add`, `set-voters`, `remove`, and `list` is the group as a table, then a summary line:

  ```text
  NODE  ADDRESS        VOTER  LEADER  MATCHED
  c1    10.0.0.1:7400  yes    yes     1043
  c2    10.0.0.2:7400  yes    no      1043
  c3    10.0.0.3:7400  no     no      1040
  term 7, committed index 1043
  ```

  When FR-056 has landed, `list` also takes `-o json` with fields `nodes` (`node_id`, `address`, `voter`, `leader`, `matched_index`), `term`, `committed_index`, `leader`; if it hasn't, leave a `// FR-056:` note at the formatter.
- Tests in `tests/daemon.rs`: `control_list_shows_a_single_control_node_as_its_voter_and_leader` (run the built binary as other CLI tests do, or call the formatter directly if the file has no binary tests).
- `docs/cli.md`: add `control` to the nouns in section 1, and a row "Control group commands | `control add`, `set-voters`, `remove`, `list` | Done (FR-064)" to "Where `fleetringd` stands".

### Step 12: the simulation harness on groups (`feat(fleetringd): run control groups in the simulation`)

- `src/sim/fleet.rs`:
  - `Timings` gains `election_timeout: Duration` (default 250 ms; `daemon_defaults()`: 1 s), `raft_heartbeat: Duration` (default 50 ms; 250 ms), and `log_keep: NonZeroU64` (default `NonZeroU64::MAX`, so the default fleet never purges and replay from genesis keeps working; `daemon_defaults()`: 1,000). `host_config` sets the new `[control]` fields from them. The default `lease_duration` of 2 s satisfies the new validation (2 × 2 × 250 ms = 1 s).
  - `FleetBuilder::control_group(self, names: &[&str]) -> Self`: adds each as a host with `NodeRole::Control`; the first gets `bootstrap = "new"`, the rest `"join"`. `validate` returns every control host (`Vec<String>`) instead of exactly one; data hosts' configs get `control.addresses` with every control host's address (and `address` unset).
  - `FleetBuilder::clock_skew(self, host: &str, millis: i64) -> Self`: when any host has a skew, every host's `SimClock` base is `sim_elapsed + SKEW_FLOOR_MS + skew` (`SKEW_FLOOR_MS = 600_000`); `validate` refuses `|skew| > SKEW_FLOOR_MS`. `run_host` takes the base offset as a parameter. Without skews, bases are as today.
  - `Fleet::build` calls `control::raft::sim_runtime::seed_raft_rng(seed ^ RAFT_SALT)` (`RAFT_SALT = 0x5241_4654`), then, when there is more than one control host, runs `form_control_group(limit = 10 s)`: as a client host named `operator` with the `workload` principal (which has `fleet`), it calls `AddControlNode` for every joining host and then `SetControlVoters` with every control host, through `ControlClient` over the simulated network, and waits until every control host reports the same leader and term.
  - `Fleet::control_hosts(&self) -> &[String]`; `Fleet::control_host(&self) -> &str` keeps returning the first; `Fleet::control_leader(&self) -> Option<String>` (the running control host whose handle's `leadership().is_own()`); `Fleet::control_handle()` returns the leader's handle, else the first running control host's; `Fleet::control_log_of(&self, host: &str) -> PathBuf`; `control_log()` stays the first host's.
  - `Fleet::await_control_converged(&mut self, limit: Duration) -> Result<(), SimError>`: steps until every running control host's `applied().index` equals the leader's committed index.
  - Leadership observations: `run_host` spawns, for a control role, a task over `control.leadership()` that records `EventKind::Leader { term: u64, leader: Option<String> }` (shown as `leads term {t}` / `no leader in term {t}`) whenever it changes. `src/sim/trace.rs` gains the variant; `Replay::Order` comparison includes it.
  - `Fleet::finish`: after stopping every host, for **each** control host: `check::control_log_replays(path, published)` where `published` is the last `AppliedState` the host published (recorded by `run_host` into `Shared` at stop), `check::acknowledged_control_survives`, then `check::control_logs_agree(&[(host, path)])` and `check::one_leader_per_term(&trace)`. `lease_events_match_control` runs on the first host's `ControlLog` only when it replayed from genesis (`replayed_from == LogIndex::ZERO`).
- `src/sim/check.rs`:
  - `Invariant::ControlLogsAgree` ("control logs agree") and `Invariant::OneLeaderPerTerm` ("one leader per term").
  - `control_log_replays(path: &Path, published: Option<&AppliedState>) -> Result<ControlLog, Violation>`, rewritten for format 2: open the store read-only; take the oldest retained control snapshot (or genesis if `purged` is `None`), replay `committed_commands_after(its index)` with `apply` up to the committed index; at each newer retained snapshot's index the replayed state must equal that snapshot's state ("replaying from the snapshot at {a} doesn't reproduce the snapshot at {b}"); if `published` is given, the state replayed up to `published.index` must equal `published.state` ("the state published at {i} differs from the replay"). `ControlLog` gains `replayed_from: LogIndex`.
  - `control_logs_agree(logs: &[(String, PathBuf)]) -> Result<(), Violation>`: for every pair, every index both hold up to both their committed indexes must hold the same term and the same entry; the detail names both hosts and the index.
  - `one_leader_per_term(trace: &Trace) -> Result<(), Violation>`: the `Leader` events with a leader, grouped by term, name one host per term.
- Tests in `src/sim/check.rs`: one per new check with a hand-built violation (two logs that differ at index 5; a trace with two leaders in term 3; a published state that differs from the replay).
- Every existing simulation test keeps one control host and must pass unchanged, with the same traces under `Replay::Exact` except for the added `Leader` events (update any test that compares a full trace).

### Step 13: control-group scenarios (`test(fleetringd): simulate control group failures`)

`tests/sim/control_group.rs` (add `mod control_group;` to `tests/sim/main.rs`); see the simulation plan below for each scenario's faults and assertions.

### Step 14: growing a real fleet (`test(fleetringd): grow a running fleet from one control node to three`)

- `tests/control_group.rs` (a new integration test file, real daemons over TCP with certificates from `tests/support`):
  - `a_fleet_grows_from_one_control_node_to_three_while_writing`: start `c1` (role `both`, `bootstrap = "new"`), `d1`, `d2` (role `data`, `control.address = c1`); create a database (R = 3 over `c1`, `d1`, `d2`); a task writes one row every 20 ms through `data::Client` and records failures with their times. Start `c2` and `c3` (role `control`, `bootstrap = "join"`); `control add c2`, `control add c3`, `control set-voters c1 c2 c3` through `ControlClient` with a `fleet` principal. Stop `c1`'s daemon gracefully. Assert: a new leader among `c2`, `c3` within 3 × the election timeout; the writer's longest run of consecutive failures is shorter than `2 × election_timeout_ms + lease_renewal`; every acknowledged row is readable at the end; `d1`'s heartbeats now reach the new leader (its `ControlClient::leader()` is the new leader's address).
  - `a_migrated_control_node_keeps_its_fleet`: copy the step-0 fixture into a data directory, start a daemon with role `control` on it; `ListDatabases` returns the fixture's two databases with their epochs.

### Step 15: documentation (`docs: describe the control group`)

See the Documentation section.

## Simulation plan

ADR-0020's rules hold: one seed drives turmoil, the workload, and now openraft's elections (`seed_raft_rng`); a failing seed prints its rerun command; a seed replays exactly under `--cfg tokio_unstable`. Faults use the existing `Fault` kinds (partition, repair, crash, stop, restart, latency) plus the new per-host clock skew.

| Scenario (test in `tests/sim/control_group.rs`) | Fleet | Faults | Asserts, beyond `Fleet::finish` |
|---|---|---|---|
| `a_leader_crash_under_writes_loses_no_control_command` | 3 control, 3 data, R = 3 | Crash the leader at 2 s while `CreateAndWrite` runs and a driver joins and heartbeats 12 fake nodes; restart it at 4 s. | A new leader within 3 × election timeout of the crash; every acknowledged join and create is in every control node's state; no database's epoch changed (no primary died). |
| `an_isolated_leader_commits_nothing` | 3 control, 2 data | Partition the leader from both other control nodes (not from data nodes) for 3 s, then repair. | The isolated node's `log` has no committed entry from the term it led after isolation; commands sent to it in that window are `Indeterminate` or `Rejected`, never acknowledged unless they appear in the other nodes' logs; `ControlLogsAgree`. |
| `a_rolling_restart_keeps_the_control_plane` | 3 control, 3 data | Stop and restart each control node in turn, 2 s apart, leader last. | Writes keep succeeding (no gap longer than lease duration); at most three leader changes. |
| `five_control_nodes_survive_two_crashes` | 5 control, 3 data | Crash two control nodes, including the leader. | A leader among the remaining three; control commands keep committing; restart both and `await_control_converged`. |
| `skewed_clocks_never_make_two_primaries` | 3 control (skews −5 s, 0, +5 s), 3 data | Crash the leader twice, so leadership passes to differently skewed clocks; crash one primary meanwhile. | `OnePrimaryPerEpoch` (from `finish`); the history checker passes; `State::now` never decreases across the committed log (check stamps of applied commands are non-decreasing after `max`). |
| `a_fleet_grows_from_one_control_node_to_three` | 1 control (`c1`), then `c2`, `c3` joining; 3 data | None besides the growth: start `c2`, `c3` at 1 s, add and set voters at 1.5 s under `CreateAndWrite`; crash `c1` at 4 s. | The group has three voters; data nodes find the new leader; no write fails for longer than one election plus a lease renewal. |
| `a_late_learner_catches_up_from_a_control_snapshot` | 3 control, 2 data; `snapshot_every = 20`, `log_keep = 10` | Run 600 heartbeats; then add a fourth control node as a learner. | The learner's store received an `InstallSnapshot` (its oldest log entry's index is above 1) and its `applied().state` equals the leader's; the leader's log never held more than 30 entries (checked by polling the store every 100 ms of simulated time); at most 2 control snapshots on every node. |

**Invariants checked** (after every run, by `Fleet::finish`): `ControlLogReplays` and `AcknowledgedControlSurvives` on every control node; `ControlLogsAgree`; `OneLeaderPerTerm`; and everything `finish` checks today (history checker, `OnePrimaryPerEpoch`, `ReplicasConverged`, `ChainAgrees`, `LeaseEventsMatchControl` when replayed from genesis).

**Seed budget.** The scenarios run over `seeds::FIXED_HEAVY` (two seeds), except `five_control_nodes_survive_two_crashes` and `a_late_learner_catches_up_from_a_control_snapshot`, which run over `[1]` alone. Together they must add no more than three minutes to the fixed-seed simulation job on the CI runner; measure on the pull request, and if they exceed it, drop the heavy scenarios to seed `1` and say so in Progress. The nightly sweep (`simulation-sweep.yml`) runs every scenario on its random seeds unchanged.

## Documentation

- `docs/lexicon.md`, new entries (alphabetical), with this text:
  - **Control group**: "The control nodes that replicate the control log with Raft (ADR-0035): voters, which elect the leader and form its quorums, and learners, which copy the log without voting. A fleet with one control node has a group of one voter. — **Applies to:** daemon, CLI, UI. — **Code:** `ControlGroupView`, protocol `ControlGroup`, `ControlNode`. **CLI:** `fleetringd control add|remove|set-voters|list`. — **Not:** cluster, quorum (a quorum is a number of replicas), Raft cluster, ensemble."
  - **Learner**: "A member of the control group that copies the control log but doesn't vote; a new control node is one until `control set-voters` names it. — **Applies to:** daemon, CLI. — **Code:** `Voting::Learner`. **CLI:** `voter no` in `control list`. — **Not:** observer, non-voter, follower (a follower is a database replica)."
  - **Voter**: "A member of the control group that votes in elections and counts toward its quorums. A group has an odd number of them, at most seven. — **Applies to:** daemon, CLI. — **Code:** `Voting::Voter`. **CLI:** `--voter <node-id>`; `voter yes` in `control list`. — **Not:** peer, member (alone), elector."
  - **Term**: "The control group's Raft term: the number of the election that made the current leader. Always say \"the control group's term\" or \"Raft term\"; it is never a lease epoch. — **Applies to:** daemon, CLI. — **Code:** `Leadership::term`, protocol `ControlGroup.term`. **CLI:** `term` in `control list`. — **Not:** epoch (an epoch belongs to a membership or a lease), generation, round."
  - Edit **Control node**: replace "Today a fleet has one; with Raft it will have three or five, of which one is the [leader](#leader)." with "A fleet has one, or a control group of three or five, of which one is the [leader](#leader) (ADR-0035)."
  - Edit **Leader**: replace "Today the only control node; with Raft, the elected one, and others redirect to it." with "The control group's elected voter; other control nodes redirect to it."
  - Edit **Control log**: replace its Not line's "Raft log (until Raft exists)" with "Raft log (the Raft log also holds membership and blank entries; the control log is its committed commands)", and add "Replicated by the [control group](#control-group)." to the definition.
  - Edit **Epoch**'s Not line: "term (a term is the control group's)".
- `crates/fleetringd/README.md`: a "Control group" section: why three or five, `bootstrap`, growing from one to three (the exact commands), replacing a failed control node (add a new one, `set-voters` without the failed one, `remove` it), what data nodes see during an election, the timings and their validation, compaction settings, and the upgrade note (back up `control.db`; 0.3 can't read format 2). The RPC table gains the `Raft` and group rows with their scopes and bindings.
- `deploy/fleetringd.toml`: step 9's fields.
- `docs/cli.md`: step 11's changes.
- `SECURITY.md`: a "Control group" paragraph: a stolen control node key controls the control plane; a stolen key of a node outside the group can't speak Raft to a member (`control peer`); a fresh node started with `bootstrap = "join"` accepts any node certificate of the fleet until it is added, so start it right before `control add` and check `control list` before `set-voters`; control-node certificates would close that window (not yet decided).
- `docs/roadmap.md`: E5's first line names FR-064 and ADR-0035; E2's "Control log compaction and bounded control snapshot retention" line names FR-064.
- `docs/adr/README.md`: ADR-0035's status once accepted; `docs/adr/0017-control-state-machine.md`: a status note "Adding Raft later: resolved by ADR-0035" (status line only, per the README's rules).
- This plan's Progress section.

## Open risks

- **openraft's API between 0.9 patch releases.** The plan pins one 0.9.x with `=`. openraft 0.10 changes storage and runtime traits; upgrading is its own ticket.
- **Randomness hook.** If the pinned openraft has no runtime hook for randomness, simulation runs with control groups don't replay exactly; step 1 stops instead of working around it.
- **Heartbeats through Raft.** Every heartbeat becomes a replicated entry. With 1,000 data nodes at 1 s that is 1,000 entries per second through a three-node group, each fsynced on a majority; group commit in `append` keeps it to a few syncs per second, but this is the first place the control plane's throughput will bind. Roadmap E4 moves heartbeats off the log.
- **Read latency.** `ensure_linearizable` costs one heartbeat round per read; `Locate` per new epoch per replica (ADR-0031) and the CLI are the main readers, which is well within budget, but a router (FR-066) caching placement must not call `Locate` per request.
- **Clock skew between control nodes** moves when failovers start, by up to the skew, after a leader change. Safety is unaffected (ADR-0018); operators should still run NTP, and the README says so.
- **The fresh-joiner window** (ADR-0035 point 13) is a real, if narrow, attack surface until control-node certificates exist.
- **Migration is one-way.** A 0.3 binary can't read format 2; a failed upgrade is restored from the `control.db` backup the upgrade notes ask for.
- **Simulation budget.** Five-node groups and compaction runs are heavy; the seed counts above may need trimming.

## Progress

- Not started.
