---
id: FR-064
title: A Raft control plane: three or five control nodes on openraft
status: in-progress
priority: P1
size: L
milestone: M13 Control plane
depends_on: [FR-027, FR-028, FR-031, FR-050, FR-054, FR-063, FR-068]
adrs: [ADR-0017, ADR-0018, ADR-0020, ADR-0022, ADR-0031, ADR-0034, ADR-0035, ADR-0038, ADR-0039, ADR-0040, ADR-0043, ADR-0046, ADR-0047, ADR-0050]
---

# FR-064: A Raft control plane: three or five control nodes on openraft

## Context

The control plane is one node (ADR-0017). While it is down, primaries keep serving (ADR-0018 point 6), but nothing else moves: no failover, no new database, no join, no lease transfer, no fork progress, no delegation, and after the heartbeat timeout data nodes refuse tokens (ADR-0026). The roadmap's "No single point of failure" criterion (E5) asks for three or five control nodes.

ADR-0035 decides how openraft's log and state machine sit on the control log. ADR-0040 amends it. It was revised against `main` at 0.9.0 and accepted on 2026-11-25, with the owner's decisions D1 to D9:

- **The leader's screen.** The single writer's screen moves to the leader, through one shared module (`control/screen.rs`). It covers the writer's injected enabling commands, its unlogged delegation refusals, and its resend answers, the last only after commit.
- **`recorded_by`.** The proposer stamps it, and the state machine checks it against each entry's leader.
- **Formats.** `control.db` and the Raft wire format are FleetRing's, so a group can mix two release lines.
- **The group gate.** The feature gate and the release window cover control-only nodes, with a fallback that fails closed for each injected enabling command.
- **Compatibility.** The slim path: one 0.9.0 fixture, the `ControlFormat` refusal, staleness, and fail-stop. There is no down-conversion and no software announcement.
- **Dev mode, readiness, and metrics** (ADR-0039, ADR-0034).
- **openraft.** `=0.10.1` with default features off and without `serde`, drawing its randomness from an injected per-host generator (ADR-0020).

Raft ships in 0.10, the release this plan calls R. Nobody runs a release before 0.9, so the one mixed case R meets is 0.9 data nodes beside an R control group. The first mixed *group* can form in R+1. Where ADR-0035 and ADR-0040 differ, ADR-0040 wins. This plan follows both, and lands in eight pull requests, PR 0 to PR 7, one in review at a time.

## Scope

- **The control group:** openraft `=0.10.1`, with `default-features = false` and without `serde`. Its log storage and state machine sit over `control.db` format 2, and its network is a new `Raft` gRPC service. FleetRing owns both byte formats. There is one voter by default and three or five in production. A single control node is a one-voter group, so there is one code path.
- **Leader-only control.** Every `Control` RPC is answered by the leader, and followers redirect with `fleetring-leader`. The leader's proposer screens each command through `control/screen.rs`, against the state its own log will reach. It stamps the command with the leader's clock and node ID, tags it with the leader ID it was screened under, and proposes it with `expect_leader`. The leader's timers wait a lease duration plus the failover margin after readiness, on `tokio::time::Instant`, before they start a promotion, a fork action, or a kept-set release.
- **Changing the group:** `fleetring control add|remove|list|set-voters`.
  - Raft IDs come from node IDs.
  - A joiner is probed before it is added.
  - Learners catch up before they vote, and voter counts stay odd.
  - `node remove` by a principal refuses a group member.
  - A leader that shuts down gracefully hands leadership over first.
- **Versions.** The feature gate and the release window count the group's members. `fleetring upgrade` refuses a rollback to a binary that can't read the node's `control.db`. An R control group runs beside 0.9 data nodes.
- **Snapshots and compaction:** a control snapshot every `snapshot_every` entries, at most `snapshots_retained` kept, and the log purged behind them with `log_keep` entries kept.
- **Bootstrap and upgrade.** `[control] bootstrap = "new"` (the default) starts a group, and `"join"` waits to be added. A format-1 `control.db` from 0.9.x, including one 0.3.0 began, migrates on first start to a one-voter group with the same state.
- **Data nodes and the router:** `[control] addresses` (a list) as an alternative to `address`. The client learns the group from join and heartbeat replies, moves to the next address on `UNAVAILABLE`, and follows redirects.
- **Dev mode:** a one-voter group that elects itself at once; loopback validation only.
- **Simulation:** control groups of three and five, with leader crashes, partitions, skewed clocks, and mixed releases. Every existing invariant runs per control host, beside the new ones (ADR-0020).
- **Not in scope:**
  - follower reads, and heartbeats off the log (roadmap E4);
  - certificates that mark a node as a control node;
  - the software announcement (D4; the owner files its ticket);
  - explicit feature finalization (FR-127, #216);
  - fleet-wide settings in control state (FR-128, #217);
  - a down-conversion of `control.db` (D3).

## Acceptance criteria

- [x] ADR-0040 is accepted. ADR-0035's status line reads "Accepted (points 1, 3, 4, 5, 6, 7, 8, 9, 11, 12, 13, 14, 15 amended by 0040)", and `docs/adr/README.md` agrees (PR 0).
- [ ] A new single control node needs no new configuration, behaves as before, and is ready without waiting an election timeout.
- [ ] The checked-in format-1 fixture, written by 0.9.0's code at its tag, migrates on first start to the state its commands replay to. Its forks, delegations, kept replica sets, incarnations, tombstones, enabled features, and members' software come through intact, and the node elects itself.
- [ ] A join refused for its software, and a delegation `apply` would refuse, never reach any control node's log, across leader changes (simulation and unit tests).
- [ ] The enabling commands the single writer injects (`EnableHandover`, `EnableIncarnations`, `SetTombstoneWindow`) precede their triggering command in the log exactly as they do today. When the group gate refuses one, the fallback of ADR-0040 point 4 applies.
- [ ] `fleetring control add` twice and `fleetring control set-voters` grow a running fleet from one control node to three while a client keeps writing, and no write fails for longer than one election plus a lease renewal.
- [ ] In simulation, crashing or isolating the leader of a three-node and a five-node group elects a new leader, data nodes reach it, and no primary changes epoch because of the election alone.
- [ ] On the fixed seeds, every existing invariant holds on every control host, and so do these:
  - no acknowledged control command is lost (`AcknowledgedControlSurvives`);
  - committed entries agree on every control node (new `ControlLogsAgree`);
  - at most one leader holds each term (new `OneLeaderPerTerm`);
  - every control node's `control.db` replays to the states it published (`ControlLogReplays`).
- [ ] A seed replays exactly under `--cfg tokio_unstable`, and crashing one control host doesn't change another's election timeouts.
- [ ] With `snapshot_every = 100`, a control node's log never holds more than `snapshot_every` plus `log_keep` entries, at most `snapshots_retained` control snapshots stay on disk, and a learner added after compaction catches up from a control snapshot.
- [ ] Access to the new RPCs:
  - the `Raft` RPCs are in `auth::RPCS` as node calls bound to `control peer`;
  - a node certificate outside the group is refused with `PERMISSION_DENIED`;
  - the group RPCs need the `fleet` scope.
- [ ] `fleetring upgrade --rollback --config <file>` on a control node refuses a binary that reads a lower control-log format, before it swaps anything.
- [ ] Mixed releases:
  - a group whose voters run two lines refuses the first use of a feature until every voter and learner supports it;
  - `control add` refuses a node with Raft state, a node outside the window, and a node without a `Raft` service.
- [ ] `fleetring node remove` refuses a control group member when a principal asks, and names the commands that take it out first. A role-`both` voter's own shutdown leave still works.
- [ ] `/readyz` reports `leader` and `control_group` with their ADR-0040 meanings; the control metric families of ADR-0040 point 10 are listed from the first scrape.
- [ ] Delegation under a group (added 2026-11-13 by FR-090 to FR-093's S10; ADR-0047 points 6.1 and 13, I5; reworded 2026-11-25 for D5):
  - the proposer stamps each command's `recorded_by` with its own node ID, and the state machine skips, as superseded, a command entry whose `raft_id(recorded_by)` isn't the entry's leader, so `RecordDelegation` replays the same on every voter;
  - a `RecordDelegation` resend is answered only after every entry the leader proposed before it is committed and applied, from the applied state;
  - every control node verifies tokens against `Source::Control` with `ControlFreshness::Acknowledged`, never `SoleWriter`. A leader counts its freshness from the last quorum acknowledgement, and a follower from the last append or heartbeat it accepted from the current leader. Past the heartbeat timeout, either refuses every token with `UNAVAILABLE`, while certificates keep working;
  - each control node that may lead holds its own delegation issuer pair (`[control] issuer_cert` and `issuer_key`), and a test delegates, rotates, and revokes across a leader change of a three-node group, as `tests/release_blocker.rs` does with a restart;
  - a voter without `delegation` holds back its first use (ADR-0040 point 4, now accepted).
- [ ] The docs are updated:
  - the daemon README ("Control group"), with the per-node settings of ADR-0040 point 13;
  - `deploy/fleetringd.toml`, `docs/cli.md`, the lexicon, and `SECURITY.md`;
  - `docs/roadmap.md` (E5, and E2's compaction line).

## Conventions for every pull request

- **Rust.** Load the `beautiful-rust:rust-best-practices` skill first.
  - New public types derive `Debug` and join `src/lib.rs`'s `assert_debug_send_sync` list.
  - Errors are `#[non_exhaustive]`, derive `thiserror::Error`, and have lowercase messages.
  - No `unwrap()` outside tests, no `bool` parameters, no `HashMap` or `HashSet`.
  - Time comes from `Clock` or `tokio::time`, randomness from the injected generator.
  - Every `match` on `SubmitError` names each variant: no wildcard arm (from PR 1).
- **`fleetring-control` doesn't change in any pull request.** `apply`, `State`, `Command`, their serde forms, and `crates/fleetring-control/tests/it/replay.rs` stay as they are, and keep passing. The field-register test of PR 2 is a test only.
- **Words.**
  - "Control group" for the control nodes that replicate the control log; "voter" and "learner" for its two kinds of member.
  - "Term" only qualified, as "the control group's term" or "Raft term", never for a lease epoch.
  - "Leader" only for the control group's leader.
  - "Fleet", never "cluster", except where openraft's own API names a field `cluster_name`.
  - The CLI is `fleetring` (ADR-0043). The process file keeps its name, `fleetringd.process`, and the git tags keep theirs (`fleetringd-v0.9.0`).
- **Paths** are under `crates/fleetringd/` unless a step says otherwise. Each commit has the trailer `Refs: FR-064`, and each pull request's body says `Refs #67`; only the last says `Fixes #67`.
- **Before pushing,** every pull request runs every gate in `CLAUDE.md`'s Commands once, locally: fmt, clippy, tests, docs, both MSRV checks, and the fixed-seed simulation with `--cfg tokio_unstable`. Docs-only pull requests skip the Rust gates. One pull request is in review at a time, so each costs one CI run.

## Pull requests and order

| PR | Content | Old phases | Size | Depends on |
|---|---|---|---|---|
| 0 | **Docs only.** ADR-0040 revised and accepted, with notes on ADR-0031, 0034, 0035, 0038, 0046, 0047, and 0050; this plan rewritten. | — | S | — |
| 1 | **Refactor, strictly no behaviour change.** Extracts `control/screen.rs` from `commit_group`, and makes `SubmitError` handling exhaustive. | — | M | 0 |
| 2 | **Foundations, the store, and the migration.** openraft, the runtime, the codec and its golden vectors, Raft IDs, `ControlStore`, the migration, the 0.9.0 fixture, the 0.9.x field register, and the `ControlFormat` refusal. The daemon is unchanged. | A1, A2a | L | 1 |
| 3 | **A one-voter group.** The proposer on `Screen`, the state machine, readiness, metrics, the leader's timers with the settle window on `tokio::time::Instant`, and the format-2 replay check, including the chaos caller. | A2b | XL | 2 |
| 4 | **The network and addresses.** The Raft service, `Probe`, `control peer`, and `GroupGate`; `addresses` for data nodes and the router; `control_nodes` (Join 4, Heartbeat 6). | B, E | L | 3 |
| 5 | **The group's commands.** The group RPCs and CLI, linearizable reads, the `node remove` refusal (D7), and leader hand-over on shutdown. | C | L | 4 |
| 6 | **Simulation and docs.** Simulation groups, invariants per control host, the new invariants and scenarios, the per-host `SimEntropy` mix, and the documentation. | D, F | L | 5 |
| 7 | **FR-065 step 11,** under FR-065: control-group faults on real daemons. | — | M | 6 |

PR 1 can merge on its own and lowers PR 3's risk. PR 6's harness-only parts (per-host clock skew, the `Leader` trace events, the new checks with hand-built violations, per-host software) can be drafted in a worktree while PRs 4 and 5 are in review; they rebase onto PR 5 before PR 6 is pushed.

---

## PR 0: docs (`docs: accept ADR-0040 and update the FR-064 plan`)

- **ADR-0040,** revised in place and accepted on 2026-11-25:
  - openraft 0.10.1 and protocol 1.4, with `HeartbeatResponse.control_nodes = 6` and `JoinResponse.control_nodes = 4`;
  - D2 to D9, the 0.9.0 fixture and field register, and the `ControlFormat` refusal;
  - the group gate's fallbacks, `ReadIndex` only, and resends answered only after commit;
  - an "Owner decisions (2026-11-25)" section.
- **Dated notes** on the accepted records whose text it touches: ADR-0031 (amendment 2, `EnableHandover`), ADR-0034 (readiness), ADR-0035 (status line and summary), ADR-0038 (the group in the window and gate, `ControlFormat`), ADR-0046 (FR-110 amendment, `EnableIncarnations`), ADR-0047 (point 6.1's `recorded_by`; "durable" means committed and applied; point 6.6), and ADR-0050 (who logs `SetTombstoneWindow`).
- **`docs/adr/README.md`** rows, and this plan.

---

## PR 1: one screen for the writer and the proposer (`refactor(fleetringd): move the control writer's screening into control/screen.rs`)

**Strictly no behaviour change.** Every existing test passes untouched, no trace changes, and no log a fixture or a simulation writes differs by a byte. The settle window (PR 3) and the per-host `SimEntropy` mix (PR 6) are *not* in this pull request.

### Files

- **`src/control/screen.rs`** (new; `mod screen;` in `src/control/mod.rs`). It is pure: no clock, no I/O, no channel. It holds what `commit_group` decides today, in the same order:

  ```rust
  /// The state the commands screened so far leave, built only once a command needs it
  /// (ADR-0040 point 2). The single writer and, from PR 3, the leader's proposer share it.
  #[derive(Debug)]
  pub(crate) struct Screen<'a> { base: &'a State, preview: Option<State>, tombstone_window: Millis }

  /// What the screen says before an action is stamped.
  #[derive(Debug)]
  pub(crate) enum Before { Refuse(Rejection), Proceed { enabling: Option<Action> } }

  /// What the screen says about a stamped command.
  #[derive(Debug)]
  pub(crate) enum Admit { Refuse(Rejection), Resend { id: DelegationId, claims: Claims }, Log }

  impl<'a> Screen<'a> {
      pub(crate) fn new(base: &'a State, tombstone_window: Millis) -> Self;
      /// `check_admission` for an action that needs screening, then which enabling command, if
      /// any, is due before it: `handover_due`, `incarnations_due`, `tombstones_due`, in that order.
      pub(crate) fn before(&mut self, action: &Action) -> Before;
      /// Records a command the caller logs (an enabling command, or one `admit` passed).
      pub(crate) fn record(&mut self, command: &Command);
      /// A resend answered from the state, a `delegation` command refused unlogged
      /// (`refused_unlogged`), or `Log`, after which the command is applied to the preview.
      pub(crate) fn admit(&mut self, command: &Command) -> Admit;
  }
  ```

  The names may change in review; the split may not: the screen decides, and the caller stamps, numbers, logs, and answers. `needs_screening` and `refused_unlogged` move here.
- **`src/control/node.rs`:** `commit_group` keeps its loop, its log transaction, and its answers, and asks `Screen` at the points where it decides today. The preview is still built lazily, under the same conditions: a command that needs screening, a drop while the window differs, or a membership change while `handover` isn't enabled. The doc comment points at `screen.rs`.
- **Exhaustive `SubmitError` handling.** Every `match` on `SubmitError` in the crate names each variant: `control/failover.rs` (the tick loop's `Err(_) => return`, at about line 296), `control/service.rs`, `sim/client.rs`, and any other. Each arm does what the wildcard did, so behaviour doesn't change. PR 3 then adds `NotLeader`, `CatchingUp`, and `Superseded`, and the compiler finds every place that must decide what they mean.

### Tests

- `src/control/screen.rs`, pure unit tests, one per verdict:
  - `a_join_refused_for_its_software_is_refused_before_stamping`;
  - `the_first_membership_change_that_keeps_a_set_is_preceded_by_enable_handover`;
  - `the_first_creation_under_always_is_preceded_by_enable_incarnations`;
  - `a_drop_while_the_window_differs_is_preceded_by_set_tombstone_window`;
  - `a_delegation_apply_refuses_is_refused_unlogged`;
  - `a_resend_is_answered_from_the_preview`;
  - `the_preview_is_built_only_when_needed`.
- Every existing test in `src/control/node.rs`, `tests/control.rs`, and the simulation passes untouched.

### Acceptance criteria

- Every gate is green, and no existing test, fixture, or trace changes.
- `commit_group` holds no screening logic of its own.
- No `match` over `SubmitError` in the crate has a wildcard arm.

---

## PR 2: foundations, the format-2 store, and the migration (`feat(fleetringd): add openraft, the control runtime, and control.db format 2`)

Nothing the daemon does changes, with one exception: `upgrade` gains the `ControlFormat` refusal, which can't fire until PR 3 makes a `control.db` format 2. The store and the migration are library code, tested on their own; the single writer, `CommandLog`, and format 1 stay as they are, and `software::running()` still reports `control_format = 1`.

### Files: openraft and the runtime

- **`Cargo.toml` (crate):**
  - `openraft = { version = "=0.10.1", default-features = false }`, with no `serde` feature (ADR-0040 point 1). Pin with `=` and record why in a comment. The runtime crate `openraft-rt` comes with it; depend on it directly only if the trait isn't re-exported.
  - Use `rand 0.10`'s traits for the runtime's RNG hook; no `rand_core 0.6`, no second `rand`.
  - Enable `serde_json`'s `raw_value` feature in the workspace.
  - `cargo deny check`: openraft and its dependencies are MIT or Apache-2.0, there are no RUSTSEC entries, and `chrono` still reaches the graph only through rmcp and schemars (so `deny.toml`'s RUSTSEC-2026-0543 reason stays true).
- **`src/rng.rs`** (new; `sim/rng.rs` re-exports it): `SplitMix64`, moved from `sim::rng::SimRng`, which stays as an alias so existing callers don't change.
- **`src/control/raft/mod.rs`** (new; `mod raft;` in `src/control/mod.rs`):
  - module docs summarizing ADR-0035 points 1–4 and ADR-0040 points 1–3;
  - `openraft::declare_raft_types!` for `ControlTypes`: `D = ControlRequest`, `R = ControlResponse`, `NodeId = RaftId`, `Node = ControlPeer`, `SnapshotData` as 0.10 requires, and `AsyncRuntime = ControlRuntime`;
  - `pub type RaftId = u64;`.

  In a doc comment, record the exact 0.10.1 signatures of `RaftLogStorage`, `RaftLogReader`, `RaftStateMachine` (its `apply` takes a stream of entries with responders), `RaftSnapshotBuilder`, `RaftNetworkFactory`, and `RaftNetworkV2` (it sends whole snapshots), and of `AsyncRuntime` in `openraft-rt` (its channel and mutex types, `spawn`, `spawn_blocking`, and the RNG hook).
- **`src/control/raft/runtime.rs`** (new):

  ```rust
  /// openraft's runtime: Tokio's timers and channels, with randomness from the generator in
  /// scope (ADR-0040 point 1).
  #[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
  pub struct ControlRuntime;

  /// A handle that draws from the generator of the task it runs in.
  #[derive(Debug, Default, Clone, Copy)]
  pub struct ControlRng;

  /// The seed of a control node's generator.
  #[derive(Debug, Clone, Copy, PartialEq, Eq)]
  pub struct RaftSeed(u64);

  impl RaftSeed {
      /// Eight bytes from the environment's entropy (`Environment::entropy()`).
      pub fn from_entropy(entropy: &dyn Entropy) -> Self;
  }

  /// Runs `future` with a generator seeded from `seed` in scope, for it and every task it spawns
  /// through [`ControlRuntime`].
  pub(crate) async fn with_generator<F: Future>(seed: RaftSeed, fallback: RaftId, future: F) -> F::Output;
  ```

  - `ControlRuntime` implements 0.10's `AsyncRuntime`. Every associated type and function delegates to Tokio, as openraft's own Tokio runtime does, except:
    - `spawn`, which wraps the future in the spawning task's generator scope;
    - `spawn_blocking`, overridden to run on Tokio, never on an OS thread of its own (0.10's default), which paused time and turmoil can't see;
    - the RNG hook, which returns `ControlRng`.
  - The generator is `tokio::task_local!(static GENERATOR: Arc<Mutex<SplitMix64>>)`.
  - `ControlRng` implements `rand 0.10`'s RNG traits: `next_u64` from the generator in scope, `next_u32` from its high bits, and byte fills from its little-endian words.
  - Outside a scope it draws from a `SplitMix64` seeded with the fallback Raft ID, increments a static counter, and logs at error level once.
  - There is no `Environment::with_raft_seed`. The seed comes from `Environment::entropy()`, which exists (PR 3 wires it).
- **`src/control/raft/id.rs`** (new): `raft_id`, `RaftIdError { Zero, Collision }`, and `check_raft_id`, as ADR-0035 point 2 specifies (`twox_hash::XxHash3_64::oneshot`). Also `fn raft_id_text(RaftId) -> String`, 16 lowercase hex digits, and its parser (ADR-0040 point 3).
- **`src/control/raft/types.rs`** (new):
  - `ControlPeer { node_id: String, address: String }`, with `Display` as `<node_id>@<address>`.
  - `ControlRequest` (`#[non_exhaustive]`), holding a decoded `command: Command` for the proposer, the raw JSON for the log, and `screened: LeaderRef { term: u64, node: RaftId }`.
  - `ControlResponse`, `#[non_exhaustive]`:
    - `Applied { outcome: Outcome, state: Arc<State> }`;
    - `Superseded`;
    - `Raft`, for blank and membership entries.
- **`src/control/raft/codec.rs`** (new). FleetRing's encodings of every openraft value that is stored or sent, with `From`/`TryFrom` both ways:
  - log entries (`kind`, `payload`), votes, log IDs, memberships, and snapshot metadata;
  - the wire structs `AppendEntries`, `AppendEntriesReply` (success, conflict, higher vote, partial success), `VoteRequest`, `VoteReply`, `InstallSnapshotChunk` (chunks of at most 1 MiB of the whole snapshot `RaftNetworkV2` hands over), `InstallSnapshotReply`, and `ProbeReply { node_id, software, has_raft_state }`.

  No struct denies unknown fields, so readers ignore them. Raft IDs are hex strings.

  `CodecError` (`#[non_exhaustive]`): `Json { what, source }` and `RaftId { text }`.
- **`tests/fixtures/raft-wire-v1/`** (new): one pretty-printed JSON file per message, entry kind, vote, log ID, membership (one config and joint), and snapshot metadata, with a `README.md` saying the files are append-only (ADR-0008's rule for golden vectors).

### Files: the 0.9.0 fixture and the field register

- **`tests/fixtures/control-writers/writer_0_9_0.rs`** (new; not compiled in this tree: cargo discovers only `tests/*.rs` and `tests/*/main.rs`). One ignored `#[test] fn write()` that uses only API present at `fleetringd-v0.9.0` (`fleetringd::control::ControlNode` and its handle, or `CommandLog` where the writer's injection isn't needed, and `fleetring_control` types), and writes to `$FIXTURE_OUT`. It drives the single writer, so the enabling commands it injects are in the log as 0.9.0 logs them. It covers every command family through 0.9:
  - joins of `n1` to `n5` (zones `a`, `b`, `c`, `a`, `b`; software 0.9.0 with every feature), heartbeats with software, `unreported`, and free disk;
  - `CreateDatabase` of `tenant:acme/notes` and `tenant:acme/billing` (R = 3, the second with `Acks::All`) and their leases, renewals, and an `UpdateDatabase` of metadata, so that `EnableIncarnations` is logged before the first creation;
  - a `Drain` and `Undrain` of `n4`, and a `Leave` of `n3` as `fleetring node remove` logs it, so `EnableHandover`, a kept replica set, and a `ReleaseKeptReplicas` are logged;
  - a `ForkDatabase` carried to `MarkForkReady` and `ReleaseForkHolders`;
  - a `RecordDelegation`, a resend of it (not logged), a `RevokeToken`, and a `RevokePrincipal`;
  - a `DropDatabase` (with the `SetTombstoneWindow` it injects) and an `ExpireDatabase`, so tombstones are kept;
  - `Tick`s, and a snapshot partway through.

  It also writes `commands.json`, every command in the file in order (`serde_json::to_string_pretty`).
- **`tests/fixtures/control-writers/fields_0_9_0.rs`** (new; run at the 0.9.0 tag like the writer): writes `crates/fleetring-control/tests/it/fixtures/fields-0.9.0.txt`, the sorted JSON field paths of a `State` with every collection non-empty and of every `Action` variant with every optional field set (ADR-0040 point 5's field rule).
- **`crates/fleetring-control/tests/it/fields.rs`** (new; a test only): `every_field_the_previous_line_lacks_is_gated_or_derived`. It computes the same paths for this build, and requires every path missing from `fields-0.9.0.txt` to be in a `NEW_FIELDS` register in the test, each naming its `Feature` or `Derived { from: &[..] }`. The register starts empty. It lists R's fields absent from 0.9.x, because R+1's leaders will send R's voters snapshots.
- **`scripts/write-control-fixtures.sh`** (new). It runs `git worktree add` at `fleetringd-v0.9.0` into a temporary directory and copies the writer in as `crates/fleetringd/tests/control_fixture_writer.rs`. It runs `cargo test -p fleetringd --test control_fixture_writer -- --ignored --exact write` with `FIXTURE_OUT` set, copies the results into `tests/fixtures/control-0.9.0/`, and removes the worktree. A `stray` argument runs the stray-table writer below. It is run once, and its output is committed.
- **`tests/fixtures/control-0.9.0/`:** `control.db`, `commands.json`, and a two-sentence `README.md`: which tag's code wrote the files, with which script, and that they are never regenerated.

### Files: the format-2 store and the migration

- **`src/control/store.rs`** (new), the format-2 store, `ControlStore`.
  - Its schema is ADR-0040 point 3's (`meta`; `log` with `kind` and `payload`, `leader_node` as hex text; `snapshots`), in WAL mode with `synchronous = FULL`.
  - Methods: `open`, `genesis`, `vote`/`save_vote`, `committed`/`save_committed`, `purged`, `last_log_id`, `append_entries`, `truncate_from`, `purge_through`, `entries`, `save_control_snapshot`, `control_snapshots`, `latest_control_snapshot`, and `committed_commands_after`. They take openraft's value types and convert them through `codec.rs`. openraft's traits are not implemented here.
  - `open(path, config, own: Option<(RaftId, ControlPeer)>)`.
  - `STORE_FORMAT = 2`, beside `log.rs`'s `FORMAT_VERSION = 1`, which PR 3 retires.
  - `LogError` gains `Migration { reason }`, `CorruptEntry { index, source }`, and `StrayTable { rows }` ("format-2 control log {path} has a format-1 `commands` table with {rows} rows").
- **The stray `commands` table** (ADR-0040 point 5). When `open` finds format 2:
  - an empty `commands` table is dropped in its own transaction, logged at info as `dropped the empty commands table an older release created`;
  - a non-empty one is `StrayTable`.
- **Migration in `ControlStore::open`** (ADR-0040 point 8), when `meta.format` is `"1"`:
  1. Recover with `log.rs`'s format-1 code.
  2. Replay `commands` from genesis and require equality with the recovered state. Format 1 never purges, so the whole history is there. A mismatch is `LogError::Migration { reason: "the snapshot at {s} doesn't match replaying the log from genesis; the first difference is at {i}" }`, and the node refuses to start. The replay is `apply` alone: the format-2 skip rules, the `recorded_by` check among them, don't apply to format-1 commands.
  3. `own` must be `Some`, else `Migration { reason: "the node's raft id is needed" }`.
  4. In one `BEGIN IMMEDIATE` transaction, do the renames and inserts of ADR-0040 point 8.
  5. Log at info: `migrated the control log from format 1 at index {N}`.

  An empty format-1 file becomes an empty format-2 store.
- **`tests/fixtures/control-writers/open_format2_0_9_0.rs`** (new; run at the 0.9.0 tag by `scripts/write-control-fixtures.sh stray`). It runs 0.9.0's `CommandLog::open` on a copy of `tests/fixtures/control-0.9.0-migrated/control.db`, which an ignored test of this PR writes by migrating the 0.9.0 fixture. It requires `UnsupportedFormat`, and saves the result as `tests/fixtures/control-format2-opened-by-0.9.0/control.db`, which holds the empty `commands` table 0.9.0 leaves behind.

### Files: the upgrade check

- **`src/upgrade/install.rs`:** `Reported` gains `control_format: u32` from `--software`'s `control_format`, beside `config_check`; a binary that doesn't report it reads format 1.
- **`src/upgrade/mod.rs`:**
  - `UpgradeError::ControlFormat { path: PathBuf, found: u32, release: Release, reads: u32 }`, with the message of ADR-0040 point 5 and the hint "roll forward".
  - Before swapping, `install` and `rollback` with `--config` read `meta.format` from `<data_dir>/control.db` when the file exists (read-only, `SQLITE_OPEN_READ_ONLY`) and refuse a target whose `control_format` is lower.
  - `check` reports the same verdict.
- **`src/control/log.rs`:** `pub(crate) fn file_format(path: &Path) -> Result<Option<u32>, LogError>`, which `upgrade` uses.

### Tests

- `tests/control.rs`, module `fixtures`: `the_0_9_0_fixture_recovers_to_its_replay`. `CommandLog::open(..).recover()` on a copy equals replaying `commands.json` with `apply` from `State::new(ControlConfig::new())`. Its state holds the forks, delegations, kept set, incarnations, tombstones, and enabled features the writer made.
- `src/control/raft/runtime.rs`. This is **the runtime-hook check (ADR-0040 point 1), and it gates the rest of the work.** If it fails against 0.10.1, stop and ask the owner.
  - `the_same_seed_draws_the_same_election_timeouts` (through openraft's own election-timeout draw on `ControlRuntime`);
  - `a_spawned_task_draws_from_its_spawners_generator`;
  - `two_generators_draw_independently`, interleaving draws of two scopes;
  - `a_draw_outside_a_scope_is_counted`;
  - `spawn_blocking_runs_under_paused_time`.
- `src/control/raft/id.rs`: `raft_ids_are_stable` (a literal pasted once), `a_collision_names_both_nodes`, and `raft_id_text_round_trips_and_is_sixteen_hex_digits`.
- `src/control/raft/codec.rs`:
  - `every_golden_vector_round_trips`: decode each file into openraft's type, encode it back, and the bytes equal the file;
  - `unknown_fields_are_ignored`;
  - `a_command_payload_is_format_ones_command_json`: the `command` of an entry, `recorded_by` included, is byte-equal to what format 1 stores for the same `Command`;
  - `a_raft_id_above_two_to_the_63_survives_sqlite_and_json`;
  - `a_whole_snapshot_splits_into_chunks_and_reassembles`.
- `tests/control.rs`, module `store`:
  - `appended_entries_survive_reopening`;
  - `truncation_removes_the_tail_and_purging_the_head`;
  - `vote_and_committed_survive_reopening`;
  - `only_the_newest_snapshots_are_kept`;
  - `committed_commands_skip_raft_entries_and_stop_at_committed`;
  - `the_0_9_0_fixture_migrates_to_a_snapshot_of_its_replay`, with its features, members' software, forks, delegations, kept set, incarnations, and tombstones;
  - `a_snapshot_that_disagrees_with_the_replay_refuses_migration` (a copy with its snapshot's state edited);
  - `an_empty_format_one_log_migrates_to_an_empty_store`;
  - `a_format_one_log_without_an_id_is_refused`;
  - `a_newer_format_is_refused` (`format = 3`);
  - `the_empty_table_0_9_0_leaves_is_dropped` (the `control-format2-opened-by-0.9.0` fixture opens, its `commands` table is gone, and its state is the migrated one);
  - `a_non_empty_stray_table_is_refused`.
- `tests/upgrade/main.rs` (fast tests, local release server and stand-in binaries):
  - `a_rollback_to_a_binary_that_reads_an_older_control_log_is_refused`: a data directory whose `control.db` has `format = 2`, and a previous binary whose `--software` prints `control_format: 1`. Nothing is swapped, and the error names both formats;
  - `a_binary_that_reports_no_control_format_reads_format_one`;
  - `a_data_only_node_has_no_control_format_to_check`.

### Acceptance criteria

- Every gate is green, including `cargo +1.89 check -p fleetringd` with openraft in the tree.
- The runtime-hook check passes against 0.10.1.
- The fixture is committed with its writers and the script, recovers to its replay, and migrates to it.
- The stray-table fixture, written by the real 0.9.0 code, opens.
- Every golden vector round-trips byte for byte.
- `upgrade` refuses a format downgrade before swapping anything (tested with a hand-written format-2 `meta`; nothing in the daemon writes format 2 yet).
- The field-list test passes with an empty register.
- No existing test changes.

---

## PR 3: a one-voter group behind the existing API (`feat(fleetringd): run the control node as a one-voter Raft group`)

After PR 3 every control node is a Raft group of one. The `Control` service, `ControlHandle::submit`, `state`, and `watch` keep working, and so does every existing test. The storage, proposer, state machine, leader timers, metrics, and readiness are the group's.

### Files

- **`src/control/log.rs`** becomes a private `format1` module, used only by `ControlStore`'s migration and by `#[doc(hidden)] pub fn format1_recover(path)`, which the fixture test switches to. `ControlStore` takes over `pub use` as the control log, `FORMAT_VERSION` becomes `2`, and `software::running()` reports `control_format = 2`.
- **`src/control/raft/log_storage.rs`:** `RaftLog` (`Clone`; `Arc<Mutex<ControlStore>>` plus `StorageIo`), implementing `RaftLogStorage` and `RaftLogReader`. The append callback fires after the transaction commits. Under `StorageIo::DedicatedThread` every call runs in `tokio::task::spawn_blocking`; under `StorageIo::Inline` it runs on the calling task (simulation).
- **`src/control/raft/state_machine.rs`:** `ControlStateMachine`, implementing 0.10's `RaftStateMachine`. Its `apply` takes the stream of entries and answers each responder.
  - **The skip rules** apply to `kind = 'command'` entries only; blank and membership entries have no `screened` field.
    1. A command entry whose `screened` leader ID differs from its own log ID's leader ID returns `ControlResponse::Superseded`, without decoding the command and without changing state.
    2. A matching entry decodes the raw command. If decoding fails, the node is fail-stop (ADR-0040 point 4). It stops applying at that entry, never applies past it, and its `control_log` readiness check fails with "entry <i> needs fleet feature '<f>', which fleetring <version> lacks; upgrade this node", or names the decoding error when no feature explains it.
    3. **The `recorded_by` check (D5):** a decoded command whose `recorded_by` is missing, or whose `raft_id(recorded_by)` isn't the entry's leader node, returns `Superseded` and changes nothing.
    4. Otherwise it runs `fleetring_control::apply`, publishes `AppliedState { index, state }` after every entry, and returns `Applied`. Membership and blank entries return `Raft`.
  - **Snapshots.** `build_snapshot` and `install_snapshot` use ADR-0040 point 3's encoding. `install_snapshot` runs `check_state` against the store's genesis, or records the snapshot's `config()` as genesis on a fresh joiner.
  - **`ControlMetrics::command`** is counted here, on every node.
- **`src/control/raft/proposer.rs`** (new): the proposer of ADR-0040 point 2, built on PR 1's `Screen`.

  ```rust
  /// Screens, stamps, and proposes control commands on the leader (ADR-0040 point 2).
  pub(crate) struct Proposer { raft: Raft<ControlTypes>, clock: Box<dyn Clock>, own: NodeId, readiness: watch::Receiver<Readiness>, gate: GroupGate, tombstone_window: Millis, metrics: ControlMetrics }

  /// Whether this node may propose, and the leader it would screen under.
  #[derive(Debug, Clone)]
  pub(crate) enum Readiness { Follower { leader: Option<ControlPeer> }, CatchingUp { term: u64 }, Ready { leader: LeaderRef } }
  ```

  - The proposer runs as one task with a bounded mailbox (`with_mailbox_capacity`). It watches `raft.metrics()` and becomes `Ready` when `current_leader == own` and `last_applied`'s leader ID has this node and the current term. It clones the applied state into `S` then.
  - It drops `S`, answering nothing new, on any change of term or leader.
  - For each submission, in the order of ADR-0040 point 2:
    1. `Screen::before`, and `GroupGate::check` for an action that needs screening (a stub that admits everything until PR 4; with one voter, the gate is the fleet's). A refusal is `SubmitError::Refused`, unlogged. An enabling command that is due is stamped, passed through `GroupGate::check`, recorded in `S`, and proposed first. A gate refusal follows ADR-0040 point 4's fallback: no `SetTombstoneWindow` means a drop without a tombstone; no `EnableIncarnations` means the creation is refused for `incarnations` and the service resubmits under `once_forked`; no `EnableHandover` means a membership change that keeps no set.
    2. Stamp with the clock and with `own` as `recorded_by`.
    3. `Screen::admit`:
       - `Refuse` is `SubmitError::Refused`, unlogged;
       - `Resend` waits, off the proposer's path, until this node has committed and applied every entry it proposed before the resend. It then reads the record from the applied state: found, it answers `Resent::Yes`; missing, it answers `SubmitError::Superseded`;
       - `Log` goes on.
    4. Propose with openraft's fire-and-forget write and `WriteOption::expect_leader` set to the screened leader.
    5. Spawn a waiter on the responder that maps:
       - `Applied` to `Committed`, and `Superseded` to `SubmitError::Superseded`;
       - `ForwardToLeader`, or an `expect_leader` refusal, to `NotLeader`;
       - a dropped responder to `Stopped`.
- **`src/control/node.rs`:**
  - `ControlNode::open_with_metrics(path, options, clock, own: ControlPeer, network: Network, entropy: &dyn Entropy)`, run inside `with_generator(RaftSeed::from_entropy(entropy), ..)`. `ControlOptions` keeps `tombstone_window` and gains the Raft settings. Its optional `recorder` goes: the proposer always stamps `own`'s node ID, since the state machine supersedes a command without one (D5), so tests that built a control node without a recorder now get one from `own`.
    1. Open the store and the state machine.
    2. Build `openraft::Config` from the options and `validate()` it: `cluster_name: "fleetring-control"`, the election timeouts, the heartbeat, `SnapshotPolicy::LogsSinceLast(snapshot_every)`, `max_in_snapshot_log_to_keep: log_keep`, `purge_batch_size: 256`, and a snapshot chunk size of 1 MiB.
    3. Call `Raft::new` with a `NoPeers` network factory, which answers every call `Unreachable`; PR 4 replaces it.
    4. If the store is empty, with `Bootstrap::New` call `initialize({own})`. With `Join`, log at info `waiting to be added to a control group`.
    5. If the membership's only voter is `own`, trigger an election at once (ADR-0040 point 9).
    6. Log `control state recovered`.
  - `Bootstrap`, the new `ControlOptions` setters, and `OpenError` (`RaftId`, `RaftConfig`, `Raft`; `Thread` and `Stopped` go).
  - `SubmitError` gains `NotLeader { leader: Option<String> }`, `CatchingUp { leader: Option<String> }` ("the control leader is catching up; retry"), and `Superseded` ("the control leader changed before the command was logged; retry"). `Refused` and `Storage` keep their meaning. PR 1's exhaustive matches make the compiler list every caller.
  - The `ControlHandle` surface: `submit`, `state`, `watch`, `applied`, `ensure_linearizable` (always `ReadPolicy::ReadIndex`), `leadership`, `group`, `is_running`, and `queued`. The group-changing methods arrive in PR 4.
  - `commit_group` and the single writer go; `Screen` stays, used by the proposer.
- **`src/control/service.rs`:**
  - `submit_status` maps `NotLeader { Some(a) }` to `not_leader(a)`, and `NotLeader { None }`, `CatchingUp`, and `Superseded` to `UNAVAILABLE`, the first two with the hint when known.
  - `submit_drawing`'s choice of `IncarnationRule::Always` asks the group gate as well as `State::check_feature` (a stub until PR 4).
- **`src/control/failover.rs`** (ADR-0040 point 6). The leader's timers run only while `Readiness::Ready`, and the expiry sweep (`spawn_expiry_sweep`) starts and stops with them:
  - the ticks (the delegation and tombstone purge ticks included);
  - `StartPromotion` and `Promote` notices;
  - fork actions (`fork::actions`);
  - `release_kept_replicas`.

  How they run:
  - **The settle window.** `started` is a `tokio::time::Instant` taken at readiness, not `clock.now()`. The settle window, a lease duration plus the failover margin, is measured from it, and gates `StartPromotion`, fork actions, and kept-set releases. Notices for promotions already in progress go out at once.
  - **Pausing.** A submit that returns `NotLeader`, `CatchingUp`, or `Superseded` pauses the timers until the next readiness, which restarts the window. They never end on these. Only `Stopped` or `Storage` ends them.
- **`src/config.rs`** (`ControlSettings`): `bootstrap` (default `"new"`), `election_timeout_ms` (1,000), `raft_heartbeat_ms` (250), `log_keep` (1,000), and `snapshots_retained` (2). Validation: the election timeout must be at least three Raft heartbeats, and the lease duration at least twice the longest election timeout. `addresses` arrives in PR 4.
- **`src/dev.rs`:** with `dev = true`, `validate` refuses `control.bootstrap = "join"` ("dev mode runs one control node, which starts its own group").
- **`src/daemon.rs`:**
  - Bind the listener before opening the control node, and pass `own = ControlPeer { node_id, address: advertised(&config, bound) }` and `environment.entropy()`.
  - `ControlRole` keeps its shape.
- **The seed under simulation.** The control node takes its seed from `Environment::entropy()`, one eight-byte draw at open, so this PR adds one draw to a control host's stream. Name in the PR any trace comparison it updates, and check that every seed pinned in a test still reproduces what it pins. The host-name mix in `sim/host.rs` waits for PR 6.
- **`src/observe/`** (ADR-0040 point 10):
  - the new gauges and counters in `names.rs`, with `OUTCOMES_PROPOSAL = ["committed", "refused", "superseded", "not_leader"]`;
  - `ControlMetrics` methods for each;
  - a task over `raft.metrics()` that sets the gauges;
  - `health.rs`: `leader` passes when a leader is known, and `control_group` is new.
- **The per-node settings warning (ADR-0040 point 13).** At readiness, the proposer compares the state's tombstone window, and the `[fork]` limits of the newest fork the state holds, with its own configuration, and logs one warning per differing setting, naming both values.
- **`src/sim/check.rs`:** `control_log_replays` is rewritten for format 2.
  - It starts at the oldest retained snapshot, or at genesis while nothing is purged.
  - At each newer snapshot it must reproduce that snapshot, and it must reach the published state at its index.
  - Superseded command entries are skipped by the same two rules as the state machine: the leader ID, then `recorded_by`.
  - `ControlLog` gains `replayed_from`. There is one control host for now.
- **`crates/fleetring-chaos/src/run.rs`:** its call to `sim::check::control_log_replays(path)` follows the rewrite, and still runs on the one control node's `control.db`.
- **`src/sim/fleet.rs`:** `Timings` gains `election_timeout` (250 ms in the default fleet), `raft_heartbeat` (50 ms), and `log_keep` (`NonZeroU64::MAX`, so the default fleet never purges and every genesis-replay check keeps running). `host_config` sets them.
- **`tests/upgrade/rolling.rs`** (the nightly test). After the control node moves to this build, it asserts that:
  - `status` shows the control node as the only voter (`fleetring control list` once PR 5 lands);
  - every database the fleet created is still listed at its epoch;
  - `fleetring upgrade --rollback --config` back to the 0.9.0 baseline is refused with the `ControlFormat` error.

  It then runs the baseline binary on the data directory directly, as a rollback that bypassed the check would, and expects `UnsupportedFormat`. It restarts this build on the same directory, which must drop the stray `commands` table and serve.

### Tests

- **`tests/control.rs`.** Every existing test keeps passing. Constructors change: `ControlNode::open_with_metrics` takes `own`, `network`, and the entropy, so `tests/support` gains `fn control_peer()`. PR 2's `store` tests keep passing.
- **`src/control/raft/state_machine.rs`:**
  - `applying_entries_matches_applying_commands`, with the 0.9.0 fixture's commands interleaved with a membership and a blank entry;
  - `a_rejection_still_moves_the_applied_index`;
  - `an_entry_appended_in_another_term_is_superseded_without_decoding` (its raw command is invalid JSON for `Command`, and nothing fails);
  - `an_entry_whose_recorded_by_is_not_its_leader_is_superseded`, and `an_entry_without_recorded_by_is_superseded`;
  - `blank_and_membership_entries_are_never_skipped`;
  - `an_undecodable_command_stops_applying_and_says_why` (a matching entry whose action is unknown: the applied index stays before it, and readiness names the entry);
  - `a_built_snapshot_installs_to_the_same_state`;
  - `a_snapshot_with_another_configuration_is_refused`.
- **`src/control/raft/proposer.rs`** (`#[tokio::test(start_paused = true)]`, one voter):
  - `the_proposer_refuses_until_its_term_is_applied`;
  - `a_join_screened_behind_a_pending_join_sees_it`: a 0.9.0 join and a join two lines away are submitted together; the second is `Refused`, and the log holds only the first;
  - `a_join_refused_for_its_software_is_never_logged` (the existing test, now through the proposer);
  - `a_refused_delegation_is_never_logged`;
  - `enabling_commands_precede_their_trigger_as_the_writer_logged_them`: the 0.9.0 fixture's actions, resubmitted through the proposer, log the same enabling commands at the same places;
  - `a_resend_is_answered_only_after_its_record_commits`: a `RecordDelegation` and its resend are submitted together, and the resend's answer waits for the first entry's apply;
  - `a_resend_whose_record_was_truncated_is_answered_retry`;
  - `proposals_enter_the_log_in_screening_order`;
  - `queued_commands_share_a_sync`: 50 concurrent submits take fewer than 50 appends (count `append` calls).
- **Control node:**
  - `a_new_control_node_elects_itself_without_waiting` (ready before the shortest election timeout);
  - `a_joining_control_node_waits_to_be_added`;
  - `the_migrated_fixture_elects_itself_with_the_same_state`;
  - `snapshots_compact_the_log` (`snapshot_every = 10`, `log_keep = 5`, `snapshots_retained = 2`, then 100 ticks: at most 15 entries and 2 snapshots, and the state survives reopening);
  - `a_new_leader_waits_a_lease_duration_before_promoting`, and `a_new_leader_waits_a_lease_duration_before_fork_actions_and_releases`;
  - `a_wall_clock_step_does_not_shorten_the_settle_window` (the `SimClock` jumps 1 h at readiness);
  - `the_timers_pause_on_not_leader_and_resume_at_readiness`;
  - `a_tombstone_window_unlike_the_states_is_warned_at_readiness`.
- **Config:** one test per new validation, `dev_mode_refuses_bootstrap_join`, and `the_defaults_validate`.
- **Observe:** `the_control_group_families_are_listed_from_the_first_scrape`, and `readiness_reports_leader_and_control_group` (ready on a one-voter group; `control_group` fails on a `Join` node).
- **Simulation:** every existing scenario passes on the fixed seeds. A test that compares a full trace under `Replay::Exact` is updated once, and the PR says which.

### Acceptance criteria

- Every gate is green, and every existing control and simulation test passes with constructor changes only. The one exception is a test in `control/node.rs` that expects a command logged without a recorder to carry `recorded_by: None`: it now expects `own`, and the PR says so.
- `fleetring dev` starts and is ready in the same time as before (±200 ms on the CI runner).
- The fixture migrates to its replay under a running daemon.
- The nightly rolling-upgrade test passes from 0.9.0 to this build with the new assertions. Run it once by hand on the PR, as `RELEASING.md` describes.
- A local `fleetring-chaos` run against this build passes its replay check.

---

## PR 4: the Raft network, the group gate, and addresses (`feat(fleetringd): carry Raft messages between control nodes and reach the group at any address`)

### Files: the Raft service

- **`crates/fleetring-proto/proto/fleetring/v1/fleetring.proto`**, after `service Replication`:

  ```proto
  // ---- Raft -------------------------------------------------------------------------------------

  // The control group's Raft messages (ADR-0035, ADR-0040). Payloads are FleetRing's JSON forms
  // of openraft's requests and replies: internal to the control group, fixed by golden vectors,
  // and opaque to everything else.
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
    // What a node would bring to a control group: its node ID, its software, and whether it
    // already holds Raft state. The leader calls it before adding the node.
    // Scope: node.
    // Binding: control peer.
    rpc Probe(RaftPayload) returns (RaftPayload);
  }

  message RaftPayload {
    bytes json = 1;
    // The sender's software (ADR-0040 point 4).
    Software sender = 2;
    // The sender's clock when it sent the message, in milliseconds since the Unix epoch. Watched,
    // never used to decide anything (ADR-0040 point 6).
    uint64 sent_at_ms = 3;
  }

  message ControlNode {
    string node_id = 1;
    string address = 2;
    bool voter = 3;
    bool leader = 4;
    uint64 matched_index = 5;
    Software software = 6;
  }
  ```

  Also:
  - `repeated ControlNode control_nodes = 4;` on `JoinResponse`, and `repeated ControlNode control_nodes = 6;` on `HeartbeatResponse`, whose fields 4 and 5 are `forks` and `tombstones`. Each has the comment "Empty from a control node that predates the field".
  - Add `` `control peer` (a voter or learner of the receiver's control group, or any node of the fleet while the receiver has none) `` to the header's list of bindings, beside `seeding` and `dropped`.
- **`crates/fleetring-proto/src/lib.rs`:** `PROTOCOL_VERSION` 1.3 → **1.4** (ADR-0040 point 11), and the module docs name the `Raft` service.
- **`src/auth/binding.rs`:** `Binding::ControlPeer` (`"control peer"`) and `Unbound::NotControlPeer` ("not a member of the control group").
- **`src/auth/policy.rs`:** four rows, `rpc("/fleetring.v1.Raft/<Method>", Requirement::Node, Binding::ControlPeer)`.
- **`src/transport.rs`:** `raft_client` and `raft_server`, with `MAX_MESSAGE_BYTES`.
- **`src/control/raft/network.rs`:** `ControlNetwork` (`RaftNetworkFactory`) and `PeerConnection` (`RaftNetworkV2`), with the encoding through `codec.rs`.
  - Each request carries `software::running()` and the clock's reading; each reply's `sender` and `sent_at_ms` are recorded in `GroupSoftware`.
  - A whole snapshot from openraft is sent as `InstallSnapshot` chunks of at most 1 MiB.
  - A dial failure, a timeout, or `UNAVAILABLE` is `Unreachable`, and the channel is forgotten. Any other status, or a payload that doesn't decode, is a network error.
  - `ControlNetwork::probe(address) -> Result<ProbeReply, ProbeError>`.
- **`src/control/raft/group.rs`** (new):
  - `GroupSoftware`, a `Mutex<BTreeMap<RaftId, Heard { software: Software, offset_ms: i64, at: tokio::time::Instant, settles_at: Option<LogIndex> }>>`. The network, the service, and the proposer's `GroupGate` share it, and a new term on this node clears it.
  - **Staleness** (ADR-0040 point 4). A report counts as known only while `at` is within one shortest election timeout. A report whose release or features differ from the previous one records `settles_at`, the proposer's last proposed index then. It counts as unknown until the applied index reaches it.
  - `GroupGate::check(&self, action, members: &BTreeSet<RaftId>, state: &State) -> Result<(), Rejection>`, implementing ADR-0040 point 4's first-use and window rules. The "control node '<id>' hasn't reported its software yet" case is a `Rejection::FeatureUnsupported` whose release is `0.0.0`. It is wired into the proposer, for submitted commands and injected enabling commands alike, replacing PR 3's stub, and into `submit_drawing`'s choice of `IncarnationRule`.
  - The largest absolute offset is exported as `fleetring_control_clock_offset_max_seconds`, with a warning, at most once a minute, above the failover margin.
- **`src/control/raft/service.rs`:** `RaftService`.
  - It checks the binding against the membership in `raft.metrics()`; an empty membership accepts any node certificate of the fleet.
  - It records the request's `sender` and `sent_at_ms`, decodes through `codec.rs` (`INVALID_ARGUMENT` if the payload doesn't decode), reassembles snapshot chunks, and answers with its own `sender` and `sent_at_ms`.
  - `Probe` answers `ProbeReply { node_id, software, has_raft_state }`.
- **`src/control/node.rs`:**
  - `ControlNode::open_with_metrics` uses `ControlNetwork` instead of `NoPeers`.
  - `raft_service()`.
  - The `ControlHandle` methods `add_node(node, address)`, `set_voters(voters)`, and `remove_node(node)`, with `GroupError`, including `GroupError::Probe { node, reason }` (`FAILED_PRECONDITION`).
  - `add_node` probes first, and refuses as ADR-0040 point 4 lists: a wrong ID, Raft state present, outside the window, a missing enabled feature, or `UNIMPLEMENTED` (a 0.9 node: "node '<id>' runs fleetring <version>, which has no control group"). Then it adds the node as a learner and waits for it to catch up.
- **`src/daemon.rs`:** `serve` adds the `Raft` service when the node runs the control role.

### Files: addresses for data nodes and the router

- **`src/config.rs`:**
  - `ControlSettings.addresses: Vec<SocketAddr>`, and `control_addresses()`;
  - validation: both `address` and `addresses` is invalid, and neither is missing on a data-only node;
  - `Config::control_target` returns the first address.
- **`src/router/config.rs`** (`RouterConfig`'s `[control]`, FR-066): `addresses` beside `address`, with the same validation, so the router follows the group too; `deploy/router.toml` documents it.
- **`src/dev.rs`:** with `dev = true`, every entry of `control.addresses` must be loopback.
- **`src/environment.rs`:** `Network::control_client(addresses: Vec<String>)`.
- **`src/control/client.rs`:** `ControlClient` becomes multi-address: `Addresses { configured, learned, current }`, `with_addresses`, `with_connector_at`, `learn`, and the rotation in `following`. It reuses `fleetring-client`'s parsing of context address lists (ADR-0043) rather than a second parser.
- **`src/membership.rs`:** `control.learn(&response.control_nodes)` after each successful join and heartbeat.
- **`src/control/service.rs`:** `join` and `heartbeat` fill `control_nodes` from `group()` (every voter and learner, with the leader marked), through `fn control_nodes(&self) -> Vec<proto::ControlNode>`.
- **`src/daemon.rs`:** `control_addresses(config, bound) -> Vec<String>`.
- **`deploy/fleetringd.toml`:** `addresses`, commented.

### Tests

`tests/control.rs`, module `raft_network`: three control nodes in one process over TCP, with node certificates from `support`.

- `three_control_nodes_elect_one_leader`: `c1` with `New`, `c2` and `c3` with `Join`; `add_node` twice and `set_voters({c1, c2, c3})`; every node agrees on one leader and term; a `Join` submitted on the leader shows in all three states.
- `a_node_outside_the_group_is_refused_raft_calls` (`PERMISSION_DENIED`, `fleetring-binding: control peer`).
- `a_fresh_joiner_accepts_its_first_leader`.
- `a_node_with_raft_state_is_not_added`: `c4` started with `New`.
- `a_node_without_a_raft_service_is_not_added`: a stub server answers `UNIMPLEMENTED`.
- `a_node_outside_the_window_is_not_added`: a stub `Probe` reports a release two lines away.
- `the_leader_learns_every_members_software_within_a_heartbeat`.
- `a_report_older_than_an_election_timeout_counts_as_unknown` (paused time: stop `c3`; after one election timeout a first use of a feature is refused as "hasn't reported its software yet").
- `a_changed_report_settles_before_it_counts`: `c3`'s report drops a feature while a first use of it is in flight. The gate treats `c3` as unknown until that proposal is applied, then refuses the next first use, naming `c3`.
- `the_first_use_of_a_feature_waits_for_every_control_node`: `c3` reports software without the feature (set through `Environment::with_software`, added here for tests and PR 6). The first use is `Refused`, naming `c3`. Once `c3` reports the feature, the first use is accepted.
- **The gate's fallbacks** (ADR-0040 point 4), each with `c3` lacking the feature:
  - `a_drop_without_set_tombstone_window_records_no_tombstone`;
  - `a_creation_without_enable_incarnations_is_logged_once_forked`;
  - `a_membership_change_without_enable_handover_keeps_no_set`.
- `a_join_outside_a_control_nodes_window_is_refused`.
- `a_leader_crash_loses_no_acknowledged_command`: crash `c1` mid-stream; every acknowledged `Join` is in the new leader's state; every unacknowledged submitter got an answer.
- `a_snapshot_larger_than_a_chunk_installs_on_a_learner`.
- In `tests/control.rs`, module `client`, against stub `Control` servers:
  - `the_client_moves_to_the_next_address_when_one_is_down`;
  - `the_client_follows_a_learned_leader`;
  - `the_client_gives_up_after_every_address_has_failed`;
  - `join_and_heartbeat_replies_name_the_control_group`.
- Config tests for both and neither, on data nodes and the router, and `dev_mode_refuses_non_loopback_control_addresses`.
- The `auth` table tests pick up the four rows automatically.

### Acceptance criteria

- Every gate is green.
- A three-node group forms and survives a leader crash in-process.
- Raft calls from outside the group are refused and audited.
- The group gate and `Probe` refuse what ADR-0040 point 4 lists, and each injected enabling command's fallback holds.
- A data node configured with `[dead, live]` heartbeats to `live`; a 0.9-shaped reply without `control_nodes` leaves the client on its configured addresses.
- The protocol says 1.4.

---

## PR 5: the control group's commands (`feat(fleetringd): add fleetring control add, remove, set-voters, and list`)

### Files

- **`fleetring.proto`:** the four group RPCs in `service Control`, their request messages, and `ControlGroup { nodes = 1; term = 2; committed_index = 3; leader = 4 }` (`Scope: fleet`). The `Control` header says every RPC is answered by the control group's leader.
- **`src/auth/policy.rs`:** the four rows, `Requirement::Fleet` and `Binding::None`.
- **`src/control/service.rs`:**
  - **Linearizable reads** (`ensure_linearizable` with `ReadPolicy::ReadIndex`, ADR-0040 point 2): `locate`, `list_databases`, `watch_membership`, `describe_control_group`, `get_fork_operation`, and `list_delegations`. `list_tombstones` reads committed state without a read round, still on the leader (ADR-0050 point 8).
  - `watch_membership` ends with the redirect when this node stops leading.
  - The four group RPCs map `GroupError`, with `Probe` → `FAILED_PRECONDITION`.
  - `add_control_node` on a dev node refuses a non-loopback address with `dev::check_loopback`, a new function shared with `dev::validate`.
  - **`leave` (D7):** it refuses a node that is a voter or learner of the group, before it submits, only when the caller is a principal with the `fleet` scope (`fleetring node remove`). A node's own `Leave` under its certificate passes, so a role-`both` voter's `on_shutdown = "leave"` works.
- **`src/control/absent.rs`:** `NoControl` refuses the four RPCs.
- **`src/control/client.rs`:** `add_control_node`, `remove_control_node`, and `set_control_voters`, each sent once through `following`, and `describe_control_group`, retried.
- **Leader hand-over on shutdown (ADR-0040 point 5).** A control node that leads, when it shuts down gracefully, first hands leadership to the most caught-up voter with openraft's leader transfer. It waits at most one election timeout, then stops anyway. `upgrade`'s restart goes through this path, so the rolling-upgrade order (data nodes, then learners, followers, and the leader last) costs no election.
- **`src/main.rs`:** the `control` noun and its four subcommands.
  - `remove` confirms on a terminal, and exits 2 without one unless `--yes`.
  - `set-voters` with one voter prints the warning `a control group of one voter has no redundancy` on stderr.
  - `list` prints a table: `NODE ADDRESS VOTER LEADER MATCHED VERSION`, then `term <t>, committed index <i>`. It takes `-o json` (FR-056).
- **`docs/cli.md`:** `control` in section 1's nouns, and the row "Control group commands | `control add`, `set-voters`, `remove`, `list` | Done (FR-064)".
- **`tests/control_group.rs`** (new integration file, with real daemons over TCP).

### Tests

- In `tests/control.rs`:
  - `a_follower_redirects_every_control_call_to_the_leader` (iterating `auth::RPCS` rows of `Control`);
  - `a_read_after_a_write_sees_it_after_a_leader_change`, for `Locate`, `GetForkOperation`, and `ListDelegations`;
  - `watch_membership_ends_when_its_node_stops_leading`;
  - `a_watch_on_a_follower_is_redirected`;
  - `group_calls_need_the_fleet_scope`;
  - `node_remove_refuses_a_control_group_member` (the message names both commands);
  - `a_voters_own_leave_is_not_refused`;
  - `a_dev_node_refuses_a_non_loopback_control_node`;
  - `a_leader_hands_over_before_it_stops`.
- `tests/control_group.rs`:
  - `a_fleet_grows_from_one_control_node_to_three_while_writing`, with `d1` and `d2` on `control.address = c1`: after `c1` stops they reach the new leader through the addresses they learned;
  - `a_migrated_control_node_keeps_its_fleet` (the 0.9.0 fixture under a real daemon: `ListDatabases` returns its databases at their epochs; `control list` shows one voter).
- `tests/daemon.rs`: `control_list_shows_a_single_control_node_as_its_voter_and_leader`.

### Acceptance criteria

- Every gate is green.
- The growth test passes within its bounds.
- Every `Control` RPC on a follower redirects.
- `node remove` refuses group members; a voter's own leave works.
- The CLI matches `docs/cli.md`.

---

## PR 6: simulation groups, and the docs (`test(fleetringd): simulate control groups, their failures, and mixed releases`)

### Files: the harness

- **`src/sim/host.rs`: the per-host `SimEntropy` mix.** Each host's entropy is seeded with `seed ^ elapsed ^ xxh3_64(host)`, not `seed ^ elapsed`, so two control hosts that start together draw different streams (ADR-0040 point 1). It lands here, not earlier, because it changes every host's random stream. Pinned seeds then stop reproducing the runs they were pinned for, so this PR, which adds scenarios and re-measures the seed budget anyway, re-checks each one. Name in the PR every seed that was re-found or replaced, and every trace comparison updated.
- **`src/sim/fleet.rs`:**
  - `FleetBuilder::control_group(names)`: the first host gets `bootstrap = "new"`, the rest `"join"`, and data hosts get `control.addresses` with every control host;
  - `FleetBuilder::clock_skew(host, millis)`: bases `sim_elapsed + SKEW_FLOOR_MS + skew`, with `|skew|` at most `SKEW_FLOOR_MS = 600_000`;
  - `FleetBuilder::software(host, Software)`, through `Environment::with_software`;
  - `FleetBuilder::previous_line(host)`: the host decodes installed snapshots and log entries through a filter that drops every JSON field missing from `fields-0.9.0.txt` (PR 2), as an older line would. A test-only seam, `Environment::with_decode_filter`, makes this possible;
  - `FleetBuilder::log_keep(n)`, for scenarios that compact;
  - `Fleet::build` forms the group through `AddControlNode` and `SetControlVoters` as an `operator` client;
  - `control_hosts`, `control_leader`, `control_handle` (the leader's), `control_log_of`, and `await_control_converged`;
  - `EventKind::Leader { term, leader }` from each control host's `leadership()`.
- **`src/sim/check.rs`.** **Every current invariant runs per control host** (28 today: `RingConverged` to `ForkDiskAdmission`), not only on the one control host they run on today.
  - **The checks that need a replay from genesis** read `ControlLog::commands()` or the lease holders a replay records:
    - `lease_events_match_control`;
    - `copies_kept_until_dropped`;
    - the fork checks in `sim::forks::check` (`ForkFencing` to `ForkDiskAdmission`) and `fork_disk`.

    They run only when `replayed_from` is genesis, and say so when they are skipped. Every scenario that compacts sets `log_keep` high enough to keep them, except `a_late_learner_catches_up_from_a_control_snapshot`, which skips them on purpose and says so.
  - `Invariant::ControlLogsAgree` and `Invariant::OneLeaderPerTerm`, with `control_logs_agree(&[(host, path)])` and `one_leader_per_term(&trace)`. `control_logs_agree` is public outside `sim`, so `fleetring-chaos` can call it (PR 7).
  - `Invariant::EveryControlNodeApplies` ("every control node applies its log"): no control host ever stops applying at an entry it can't decode.
  - `Invariant::PreviousLineDecidesAlike`: on every host with `previous_line`, the state reached through the filter makes the same decisions as the leader's at the same index. The decisions compared are `liveness` of every member, `promotions_due`, `releases_due`, each lease's holder, epoch, and expiry, the enabled features, and the membership epoch.
  - `control_log_replays` runs for every control host against the last state each published.
- **`src/sim/trace.rs`:** the `Leader` variant; `Replay::Order` includes it.
- **`tests/sim/control_group.rs`** (new; `mod control_group;` in `tests/sim/main.rs`): the scenarios below.

### Scenarios

| Scenario | Fleet | Faults | Asserts, beyond `Fleet::finish` |
|---|---|---|---|
| `a_leader_crash_under_writes_loses_no_control_command` | 3 control, 3 data, R = 3 | Crash the leader at 2 s while `CreateAndWrite` runs and a driver joins and heartbeats 12 fake nodes; restart it at 4 s. | A new leader within 3 × the election timeout; every acknowledged join and create is in every control node's state; no database's epoch changed. |
| `an_isolated_leader_commits_nothing` | 3 control, 2 data | Partition the leader from both other control nodes (not from data nodes) for 3 s, then repair. | Nothing it proposed after isolation commits; no command sent to it then is acknowledged unless it is in the others' logs; `ControlLogsAgree`. |
| `a_refused_join_never_reaches_a_log_across_leader_changes` | 3 control, 2 data | A driver sends joins from software two lines away and from the next line, interleaved, while the leader is crashed twice. | No control node's log holds a join `check_admission` refuses against the state before it; every refused join was answered `FAILED_PRECONDITION`. |
| `a_delegation_resend_across_a_leader_change_is_answered_once_committed` | 3 control, 2 data | A driver records a delegation and resends it while the leader is crashed between the two. | The resend is answered `Resent::Yes` only from a state that holds the record, or `UNAVAILABLE`; no log holds a refused delegation; every `recorded_by` names its entry's leader. |
| `a_mixed_release_group_waits_for_every_voter` | 3 control (one reporting the previous line without a feature), 2 data | First use at 1 s; "upgrade" the old voter (restart with this build's software) at 2 s; first use at 3 s. | The first attempt is refused naming the old voter; the second is accepted; the feature is enabled on every control node. |
| `a_rolling_restart_keeps_the_control_plane` | 3 control, 3 data | Stop and restart each control node in turn, 2 s apart, leader last. | No write gap longer than a lease duration; the leader's restart hands over, so no election timeout passes without a leader. |
| `five_control_nodes_survive_two_crashes` | 5 control, 3 data | Crash two control nodes, the leader included. | A leader among the other three; commands keep committing; after restarts, `await_control_converged`. |
| `skewed_clocks_never_make_two_primaries` | 3 control (−5 s, 0, +5 s), 3 data | Crash the leader twice; crash one primary meanwhile. | `OnePrimaryPerEpoch`; the history checker passes; applied stamps, after clamping, never decrease. |
| `a_fleet_grows_from_one_control_node_to_three` | 1 control, then 2 joining; 3 data | Start `c2` and `c3` at 1 s; add them and set voters at 1.5 s under `CreateAndWrite`; crash `c1` at 4 s. | Three voters; data nodes find the new leader; no write fails longer than one election plus a lease renewal. |
| `a_late_learner_catches_up_from_a_control_snapshot` | 3 control, 2 data; `snapshot_every = 20`, `log_keep = 10` | 600 heartbeats, then add a fourth control node. | The learner installed a snapshot (its first log index is above 1) and its state equals the leader's; the leader's log never held more than 30 entries; at most 2 snapshots on every node. |
| `a_previous_line_voter_decides_alike_from_snapshots` | 3 control, `c3` as `previous_line`; `snapshot_every = 20`, `log_keep = 10` | Isolate `c3` for 300 heartbeats so it catches up by `InstallSnapshot`; crash the leader once. | `PreviousLineDecidesAlike`; `ControlLogsAgree`; `EveryControlNodeApplies`. |
| `one_hosts_crash_does_not_move_anothers_elections` | 3 control | Run seed 7 twice, once crashing `c3` at 1 s. | `c1`'s and `c2`'s election timeouts (logged as trace events) agree up to the crash's first effect on them. |

**Seed budget.** The scenarios run over `seeds::FIXED_HEAVY` (`[1, 2026]`), except three, which run on seed `1` alone: `five_control_nodes_survive_two_crashes`, `a_late_learner_catches_up_from_a_control_snapshot`, and `a_refused_join_never_reaches_a_log_across_leader_changes`. Together, and with the existing invariants now running per control host, they add at most three minutes to the fixed-seed job (owner decision, 2026-11-03). Measure on the PR against the job as it runs today. If they exceed it, move more to seed `1`, and say so in Progress. The nightly sweep runs them all on its random seeds. FR-122 made seeds replay exactly, which `.cargo/config.toml` relies on, so a failing seed prints its rerun command.

### Files: documentation

- **`docs/lexicon.md`:**
  - new entries **Control group**, **Learner**, **Voter**, and **Term**;
  - the edits to **Control node**, **Leader**, **Control log** (its Not line names the Raft log's membership and blank entries, and superseded commands), and **Epoch**.
- **`crates/fleetringd/README.md`,** a "Control group" section:
  - why three or five voters, and `bootstrap`;
  - growing from one to three, with the exact commands;
  - replacing a failed control node: add a new one, `set-voters` without the failed one, `remove` it, then `node remove` if it was also a data node;
  - what data nodes and the router see during an election, and the 0.9 data nodes' single address;
  - the timings and their validation, and compaction;
  - rolling upgrades of a group (data nodes, then learners, followers, the leader last, with its hand-over);
  - the rollback refusal and its reason, and that the first start on 0.10 is one-way for a control node;
  - the per-node settings to keep equal across control nodes, and the leader's warning (ADR-0040 point 13);
  - NTP and the clock-offset gauge.

  The RPC table gains the `Raft` and group rows.
- **`deploy/fleetringd.toml`:** every new `[control]` field, commented, with its default.
- **`SECURITY.md`:** the "Control group" paragraph, plus these points:
  - a control node decodes only commands it applies (superseded entries stay undecoded);
  - `recorded_by` is checked against each entry's leader but stays advisory against a compromised leader;
  - `Probe` reveals a node's ID, its software, and whether it holds Raft state to any node certificate of the fleet.
- **`docs/roadmap.md`:** E5's first line names FR-064 and ADR-0035/0040; E2's compaction line names FR-064.
- **`docs/adr/README.md`:** ADR-0017's status line gains "Adding Raft later: resolved by 0035".
- **This plan's** Progress, and the issue's checklist.

### Acceptance criteria

- Every scenario passes on the fixed seeds, with every invariant `Fleet::finish` checks, on every control host.
- A failing seed prints its rerun command and replays exactly under `--cfg tokio_unstable`.
- Every existing scenario passes, and the PR lists any whose seed or trace comparison changed with the entropy mix.
- The fixed-seed job stays within budget.

---

## PR 7: FR-065 step 11, control-group faults on real daemons

This is `docs/plans/FR-065-fault-injection-harness.md`'s step 11. It lands under FR-065, after PR 6. Beyond what step 11 lists, it needs:

- From PR 5: `DescribeControlGroup`, `bootstrap`/`join`, and `control add`/`set-voters`. From PR 4: `addresses`, and a client constructor that takes several addresses. From PR 6: `control_logs_agree`, callable outside `sim`.
- **`log_keep` raised in the chaos configuration.** With five nodes heartbeating, the defaults (`log_keep = 1000`, `snapshot_every = 1000`) purge the log within a 600 s run, and `lease_events_match_control` and the other genesis-replay checks would silently stop. Raise `log_keep` above the run's entry count, and have `run.rs` fail if a replay didn't start at genesis.
- **The ±60 s control-clock skew** exceeds `MAX_CONTROL_LEAD` (30 s). After a fast leader is replaced, `DelegateToken` is refused with `UNAVAILABLE` for up to about two minutes, and expiries fire early. Both are availability, not safety (ADR-0040 point 6). The checker must not count them as failures, and the run's report should note them.

---

## Owner decisions (2026-11-03)

Decided as recommended when ADR-0035 was accepted, with the release numbers corrected since:

- A control node waiting to join trusts any node certificate of the fleet until it is added. Certificates that mark a node as a control node are a follow-up ticket.
- The protocol minor version: ADR-0038 took 1.1, FR-070 1.2, and FR-066 1.3, so FR-064 takes 1.4 (ADR-0040 point 11).
- `set-voters` may shrink the group to one voter, with a CLI warning.
- Control-group scenarios add at most 3 minutes to the fixed-seed simulation job.
- `commands_v1` is kept by the release that adds Raft and dropped by the line after it. The decision said "0.4"; that release is now 0.10.

## Owner decisions (2026-11-25)

The owner accepted ADR-0040 with every recommendation of the review against `main` at 0.9.0:

- **D1:** openraft `=0.10.1`, with default features off.
- **D2:** the slim compatibility path: one 0.9.0 fixture, the `ControlFormat` refusal, the group gate with staleness, and fail-stop.
- **D3:** no down-conversion.
- **D4:** the software announcement is deferred to the first feature after Raft. The owner files its ticket; this plan files nothing.
- **D5:** the proposer stamps `recorded_by`, and the state machine verifies `raft_id(recorded_by)` against the entry's leader and supersedes the entry otherwise.
- **D6:** the in-entry leader ID is kept as well as `expect_leader`.
- **D7:** only a principal's `node remove` (`Leave`) of a group member is refused.
- **D8:** ADR-0040 is revised in place and accepted.
- **D9:** per-node settings that leak into commands are documented, and the leader warns on a mismatch.

Filed with them: FR-127 (#216), explicit feature finalization in place of injected `Enable*` commands, and FR-128 (#217), fleet-wide settings in control state. FR-064 keeps today's injection, built on the shared screen.

## Questions decided

The open questions of 2026-11-04 are closed:

1. **ADR-0040 or a rewrite of ADR-0035?** ADR-0040 was kept as an amendment, revised in place and accepted (D8).
2. **Should `node remove` refuse a control group member?** Yes, naming `control set-voters` and `control remove`, but only for a principal's `node remove`, not a node's own leave (D7).
3. **Does a down control node block the first use of a new fleet feature?** Yes: it is ADR-0038's rule for down fleet members.
4. **Production seeds for openraft's generator?** OS entropy, through `Environment::entropy()`.
5. **Which openraft?** `=0.10.1` (D1).

The down-conversion (the old question 2) was dropped (D3). Leader transfer before restarting a leader (the old question 7) comes with 0.10 and is in PR 5.

## Follow-ups

- **The software announcement** (D4): `SIGUSR1`, `<data_dir>/fleetringd.next-software`, and `software_settled`, for the first feature after Raft. The owner files its ticket. Until then, fail-stop and the readiness message cover a rollback without `--config`.
- **FR-127 (#216):** explicit feature finalization, replacing the injected `EnableHandover`, `EnableIncarnations`, and `SetTombstoneWindow`, and with them PR 4's per-command fallbacks.
- **FR-128 (#217):** fleet-wide settings in control state, replacing ADR-0040 point 13's warning.
- Control-node certificates (ADR-0035 point 13), follower reads, and heartbeats off the log (roadmap E4).

## Open risks

- **openraft 0.10.1 is new** (released 2026-11-19). It is pinned with `=`, and PR 2's runtime-hook check gates everything after it. A bug found later means a patch release behind the same pin and codec, not a format change.
- **Heartbeats through Raft.** Every heartbeat becomes a replicated entry. Since 0.8, a heartbeat carries `unreported`, free disk, and seed progress, and its reply carries forks, tombstones, and revocations. The entries are bigger than ADR-0035 assumed. Group commit keeps syncs few, but this is where the group's throughput binds first. Roadmap E4 moves heartbeats off the log.
- **In-memory leader state is lost on a leader change:**
  - the fork driver's `SeedProgressBoard`;
  - the failover's `notified` map;
  - drop notices, which tombstones in heartbeat replies cover.

  Clients see availability blips, not safety issues.
- **Chaos skew versus `MAX_CONTROL_LEAD`.** FR-065's ±60 s control skew refuses delegation for up to about two minutes after a fast leader is replaced, and fires expiries early (PR 7). Availability only.
- **Read latency.** `ensure_linearizable` costs a heartbeat round per read. FR-066's router caches placement rather than call `Locate` per request.
- **Clock skew** delays failovers after a leader change by up to the skew, and denylist entries can be forgotten that much early (ADR-0040 point 6). Safety is unaffected, and the clock-offset gauge shows it.
- **The fresh-joiner window** (ADR-0035 point 13) stays until control-node certificates exist. `Probe` narrows the operator's part: a joiner with state is refused.
- **Migration is one-way,** and a replay mismatch leaves the node down (fail-closed). The 0.9.0 fixture and the nightly rolling-upgrade test are the guard.
- **A rollback without `--config`** in a mixed group (R+1 on) can leave a voter stopped at an entry it can't decode until it is rolled forward. It is fail-stop, the readiness check names the fix, and the deferred announcement closes it.
- **An `EnableHandover` refused by the group gate** leaves membership changes without a kept set while a group member lacks `handover`. That is ADR-0031 amendment 2's accepted mixed-fleet window, and R has no mixed group.
- **Simulation budget.** Five-node groups, compaction runs, and 28 invariants per control host are heavy; the seed split may need trimming.
- **The seeds move twice.** PR 3 adds one draw to a control host's stream, and PR 6's host mix changes every stream. Both PRs re-check pinned seeds and say which changed.

## Progress

- 2026-11-04: ADR-0040 proposed, amending ADR-0035 for versions, rollback, dev mode, and the leader's screen. This plan was rewritten into phases A1–F around it. openraft 0.9.25's runtime hook was verified in a scratch crate: the same seed gave the same election timeouts across a spawned task. No Raft code is written yet.
- 2026-11-04: after an independent review, ADR-0040 gained the staleness, announcement, and fail-stop rules, the stray-table rule, the rule on fields an older line drops, and the time assumptions. A2 was split into A2a (store and migration as library code) and A2b (openraft wiring). The build was then paused until the MVP work landed.
- 2026-11-25: the plan was reviewed against `main` at 0.9.0. The single writer had grown injected enabling commands, unlogged delegation refusals, resend answers, and `recorded_by`; the protocol was at 1.3, and `HeartbeatResponse` field 4 was taken; openraft 0.10.1 was final. The owner accepted every recommendation (D1–D9), and FR-127 (#216) and FR-128 (#217) were filed.
- 2026-11-25: PR 0 (docs). ADR-0040 was revised and accepted, with dated notes on ADR-0031, 0034, 0035, 0038, 0046, 0047, and 0050, and this plan was rewritten into PRs 0–7 and un-paused. Next: PR 1, the screen extraction.
