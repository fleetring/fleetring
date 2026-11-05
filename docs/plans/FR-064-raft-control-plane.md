---
id: FR-064
title: A Raft control plane: three or five control nodes on openraft
status: todo
priority: P1
size: L
milestone: M13 Control plane
depends_on: [FR-027, FR-028, FR-031, FR-050, FR-054, FR-063, FR-068]
adrs: [ADR-0017, ADR-0018, ADR-0020, ADR-0022, ADR-0031, ADR-0034, ADR-0035, ADR-0038, ADR-0039, ADR-0040]
---

# FR-064: A Raft control plane: three or five control nodes on openraft

## Context

The control plane is one node (ADR-0017). While it is down, primaries keep serving (ADR-0018 point 6), but nothing else moves: no failover, no new database, no join, no lease transfer, and after the heartbeat timeout data nodes refuse tokens (ADR-0026). The roadmap's "No single point of failure" criterion (E5) asks for three or five control nodes.

ADR-0035 decides how openraft's log and state machine sit on the control log. ADR-0040 amends it for what shipped in 0.4.0:

- the single writer's pre-log screen (ADR-0038), which moves to the leader;
- FleetRing-owned `control.db` and Raft wire formats, so a group can mix two release lines;
- a feature gate and a release window that cover control-only nodes;
- rollback, and the migration of format-1 logs written by 0.4.0 and 0.3.0;
- dev mode (ADR-0039), and readiness and metrics (ADR-0034);
- openraft `=0.9.25`, drawing its randomness from an injected per-host generator (ADR-0020).

Where the two ADRs differ, ADR-0040 wins. This plan follows both and lands in seven pull requests, phases A1 to F below.

## Scope

- **The control group:** openraft `=0.9.25` without its `serde` feature, with its log storage and state machine over `control.db` format 2 and its network as a new `Raft` gRPC service. FleetRing owns both byte formats. There is one voter by default and three or five in production. A single control node is a one-voter group, so there is one code path.
- **Leader-only control:** every `Control` RPC is answered by the leader, and followers redirect with `fleetring-leader`. The leader's proposer screens each command against the state its own log will reach, stamps it with the leader's clock, and tags it with the leader ID it was screened under. Its failover timer waits a lease duration plus the failover margin after it becomes ready before it starts any promotion.
- **Changing the group:** `fleetringd control add|remove|list|set-voters`. Raft IDs come from node IDs, a joiner is probed before it is added, learners catch up before they vote, voter counts stay odd, and `node remove` refuses a group member.
- **Versions:** the feature gate and the release window count the group's members. `fleetringd upgrade` refuses a rollback to a binary that can't read the node's `control.db`, and a 0.5 control group runs beside 0.4 data nodes.
- **Snapshots and compaction:** a control snapshot every `snapshot_every` entries, at most `snapshots_retained` kept, and the log purged behind them with `log_keep` entries kept.
- **Bootstrap and upgrade:** `[control] bootstrap = "new"` (the default) starts a group, and `"join"` waits to be added. A format-1 `control.db` from 0.4.0, or one 0.3.0 began, migrates on first start to a one-voter group with the same state.
- **Data nodes:** `[control] addresses` (a list) as an alternative to `address`. The client learns the group from join and heartbeat replies, moves to the next address on `UNAVAILABLE`, and follows redirects.
- **Dev mode:** a one-voter group that elects itself at once; loopback validation only.
- **Simulation:** control groups of three and five, with leader crashes, partitions, skewed clocks, and mixed releases, and new invariants (ADR-0020).
- **Not in scope:** follower reads, heartbeats off the log (roadmap E4), leader transfer (openraft 0.10), certificates that mark a node as a control node, and openraft 0.10.

## Acceptance criteria

- [ ] ADR-0040 is accepted. ADR-0035's status line reads "Accepted (points 1, 3, 4, 5, 7, 8, 9, 11, 13, 14, 15 amended by 0040)", and `docs/adr/README.md` agrees.
- [ ] A new single control node needs no new configuration, behaves as before, and is ready without waiting an election timeout.
- [ ] Both checked-in format-1 fixtures, one written by 0.3.0's code and one written by 0.4.0's continuing it, migrate on first start to the state their commands replay to, with the `undrain` feature and members' software intact, and the node elects itself.
- [ ] A join refused for its software never reaches any control node's log, across leader changes (simulation and unit tests).
- [ ] `fleetringd control add` twice and `fleetringd control set-voters` grow a running fleet from one control node to three while a client keeps writing, and no write fails for longer than one election plus a lease renewal.
- [ ] In simulation, crashing or isolating the leader of a three-node and a five-node group elects a new leader, data nodes reach it, and no primary changes epoch because of the election alone.
- [ ] On the fixed seeds the invariants hold:
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
- [ ] `fleetringd upgrade --rollback --config <file>` on a control node refuses a binary that reads a lower control-log format, before it swaps anything.
- [ ] Mixed releases:
  - a group whose voters run two lines refuses the first use of a feature until every voter and learner supports it;
  - `control add` refuses a node with Raft state, a node outside the window, and a node without a `Raft` service.
- [ ] `fleetringd node remove` refuses a control group member and names the commands that take it out first.
- [ ] `/readyz` reports `leader` and `control_group` with their ADR-0040 meanings; the control metric families of ADR-0040 point 10 are listed from the first scrape.
- [ ] The docs are updated: the daemon README ("Control group"), `deploy/fleetringd.toml`, `docs/cli.md`, the lexicon, `SECURITY.md`, and `docs/roadmap.md` (E5, and E2's compaction line).

## Conventions for every phase

- **Rust.** Load the `beautiful-rust:rust-best-practices` skill first.
  - New public types derive `Debug` and join `src/lib.rs`'s `assert_debug_send_sync` list.
  - Errors are `#[non_exhaustive]`, derive `thiserror::Error`, and have lowercase messages.
  - No `unwrap()` outside tests, no `bool` parameters, no `HashMap` or `HashSet`.
  - Time comes from `Clock` or `tokio::time`, randomness from the injected generator.
- **`fleetring-control` doesn't change in any phase.** `apply`, `State`, `Command`, their serde forms, and `crates/fleetring-control/tests/it/replay.rs` stay as they are, and keep passing.
- **Words.**
  - "Control group" for the control nodes that replicate the control log; "voter" and "learner" for its two kinds of member.
  - "Term" only qualified, as "the control group's term" or "Raft term", never for a lease epoch.
  - "Leader" only for the control group's leader.
  - "Fleet", never "cluster", except where openraft's own API names a field `cluster_name`.
- **Paths** are under `crates/fleetringd/` unless a step says otherwise. Each commit has the trailer `Refs: FR-064`, and each pull request's body says `Refs #67`; only the last says `Fixes #67`.
- **Before pushing,** every phase runs every gate in `CLAUDE.md`'s Commands locally (fmt, clippy, tests, docs, both MSRV checks, and the fixed-seed simulation with `--cfg tokio_unstable`). Parallel phases work in worktrees and don't push; the lead orders the pushes so each PR costs one CI run.

## Phases and order

| Phase | Pull request | Depends on | Runs in parallel with |
|---|---|---|---|
| A1 | Foundations: fixtures, openraft, the runtime, the codec, Raft IDs, the upgrade check. No behaviour change except a refusal in `upgrade` that can't fire yet. | — | E |
| A2a | The format-2 store and the migration, as library code: no openraft wiring, and the single writer still runs on format 1 | A1 | E; the down-conversion (open question 2) once A2a lands |
| A2b | The openraft wiring: a one-voter group behind the existing API, with the state machine, proposer, failover timer, readiness, metrics, and simulation checker | A2a | E; the down-conversion |
| B | The Raft network service, `Probe`, and the `control peer` binding | A2b | E; D's harness-only parts |
| C | `control add/remove/list/set-voters`, the group gate, `node remove`'s refusal | B, E | D's harness-only parts |
| D | Simulation groups of three and five, with the new invariants and scenarios | C | — |
| E | Data-node `addresses`, learning the group, and address rotation | — | A1, A2a, A2b, B |
| F | Documentation and acceptance | D | — |

E touches only the data-node side and the client. C's real-daemon growth test and D's scenarios need data nodes that move to another control node, so both wait for E.

D's harness-only parts can be drafted in a worktree while B and C are under review: per-host clock skew, the `Leader` trace events, the new checks with hand-built violations, and per-host software. They rebase onto C before D is pushed.

---

## Phase A1: foundations (`build(fleetringd): add openraft, the control runtime, and the control group's formats`)

Nothing the daemon does changes, with one exception: `upgrade` gains the `ControlFormat` refusal, which can't fire until A2b makes a `control.db` format 2. Everything else here is tested in isolation, so A2a and A2b are about storage and wiring.

### Files

- **`tests/fixtures/control-writers/writer_0_3_0.rs` and `writer_0_4_0.rs`** (new; not compiled in this tree: cargo discovers only `tests/*.rs` and `tests/*/main.rs`). Each is one ignored `#[test] fn write()` that uses only API present at its tag (`fleetringd::control::CommandLog::{open, append, save_snapshot}` and `fleetring_control` types) and writes to `$FIXTURE_OUT`.
  - **0.3.0:** opens a new `control.db` with `ControlConfig::new()` and appends 30 commands, stamped 1,000 ms apart:
    - joins of `n1` to `n4` (zones `a`, `b`, `c`, `a`; addresses `10.0.0.<k>:7400`; no software, since 0.3.0 has none);
    - `CreateDatabase` of `tenant:acme/notes` (R = 3) and `tenant:acme/billing` (R = 3, `Acks::All`);
    - a heartbeat from each node reporting both databases;
    - `AcquireLease` of each database by the first node of its replica set, and four `RenewLease`s;
    - a `Drain` of `n4`, a `RevokeToken`, and `Tick`s to fill.

    It saves a snapshot at 20.
  - **0.4.0:** opens a copy of the 0.3.0 file and appends 31 to 60:
    - joins of `n5` (software 0.4.0, `undrain`) and `n1` again (software 0.4.0);
    - a heartbeat from `n2` carrying software 0.4.0, which corrects the record;
    - heartbeats so that every member reports `undrain`;
    - `Undrain` of `n4`, which enables `undrain` for good;
    - `Leave` of `n3`, as `fleetringd node remove` logs it;
    - `Tick`s.

    It saves a snapshot at 45. Each writer also writes `commands.json`, every command in the file in order (`serde_json::to_string_pretty`).
- **`tests/fixtures/control-writers/fields_0_4_0.rs`** (new; run at the 0.4.0 tag like the writers): writes `crates/fleetring-control/tests/it/fixtures/fields-0.4.0.txt`, the sorted JSON field paths of a `State` with every collection non-empty and of every `Action` variant with every optional field set (ADR-0040 point 5's field rule).
- **`crates/fleetring-control/tests/it/fields.rs`** (new; a test only, so the crate's code doesn't change): `every_field_the_previous_line_lacks_is_gated_or_derived`. It computes the same paths for this build and requires every path missing from `fields-0.4.0.txt` to be in a `NEW_FIELDS` register in the test, each naming its `Feature` or `Derived { from: &[..] }`. The register starts empty.
- **`scripts/write-control-fixtures.sh`** (new): for each tag (`fleetringd-v0.3.0`, then `fleetringd-v0.4.0`) it runs `git worktree add` into a temporary directory, copies the writer in as `crates/fleetringd/tests/control_fixture_writer.rs`, runs `cargo test -p fleetringd --test control_fixture_writer -- --ignored --exact write` with `FIXTURE_OUT` set (the 0.4.0 run starts from the 0.3.0 output), copies the results into `tests/fixtures/control-0.3.0/` and `tests/fixtures/control-0.4.0/`, and removes the worktrees. It is run once, and its output is committed.
- **`tests/fixtures/control-0.3.0/` and `control-0.4.0/`:** `control.db`, `commands.json`, and a two-sentence `README.md`: which tag's code wrote the files, with which script, and that they are never regenerated.
- **`Cargo.toml` (crate):**
  - `openraft = "=0.9.25"`, with no features (it has no default ones, and ADR-0040 point 1 leaves out `serde`), and `rand_core = "0.6"`, the trait version openraft's `rand 0.8` needs. Pin with `=` and record why in a comment.
  - Enable `serde_json`'s `raw_value` feature in the workspace.
  - Check `cargo deny check`: openraft and its dependencies are MIT or Apache-2.0, and the duplicate `rand` is a warning only.
- **`src/rng.rs`** (new; `sim/rng.rs` re-exports it): `SplitMix64`, moved from `sim::rng::SimRng`, which stays as an alias so existing callers don't change.
- **`src/control/raft/mod.rs`** (new; `mod raft;` in `src/control/mod.rs`):
  - module docs summarizing ADR-0035 points 1–4 and ADR-0040 points 1–3;
  - `openraft::declare_raft_types!(pub ControlTypes: D = ControlRequest, R = ControlResponse, NodeId = RaftId, Node = ControlPeer, Entry = openraft::Entry<ControlTypes>, SnapshotData = std::io::Cursor<Vec<u8>>, AsyncRuntime = ControlRuntime)`;
  - `pub type RaftId = u64;`.

  In a doc comment, record the exact 0.9.25 signatures of `RaftLogStorage`, `RaftLogReader`, `RaftStateMachine`, `RaftSnapshotBuilder`, `RaftNetworkFactory`, and `RaftNetwork`.
- **`src/control/raft/runtime.rs`** (new):

  ```rust
  /// openraft's runtime: Tokio's timers, with randomness from the generator in scope (ADR-0040 point 1).
  #[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
  pub struct ControlRuntime;

  /// A handle that draws from the generator of the task it runs in.
  #[derive(Debug, Default, Clone, Copy)]
  pub struct ControlRng;

  /// The seed of a control node's generator: the simulation's seed and host, or the OS.
  #[derive(Debug, Clone, Copy, PartialEq, Eq)]
  pub struct RaftSeed(u64);

  impl RaftSeed {
      pub const fn new(seed: u64) -> Self;
      /// The simulation's seed for `host`: `seed ^ xxh3_64(host)`.
      pub fn for_host(seed: u64, host: &str) -> Self;
  }

  /// Runs `future` with a generator seeded from `seed` in scope, for it and every task it spawns
  /// through [`ControlRuntime`].
  pub(crate) async fn with_generator<F: Future>(seed: RaftSeed, fallback: RaftId, future: F) -> F::Output;
  ```

  - `ControlRuntime` implements `openraft::AsyncRuntime`. Every associated type and function delegates to `openraft::TokioRuntime`, except:
    - `spawn`, which wraps the future in the spawning task's generator scope;
    - `type ThreadLocalRng = ControlRng` and `thread_rng()`.
  - The generator is `tokio::task_local!(static GENERATOR: Arc<Mutex<SplitMix64>>)`.
  - `ControlRng` implements `rand_core::RngCore` (`next_u64` from the generator in scope; `next_u32` takes its high bits; `fill_bytes` and `try_fill_bytes` take little-endian chunks of it).
  - Outside a scope it draws from a `SplitMix64` seeded with the fallback Raft ID, increments a static counter, and logs at error level once.
- **`src/control/raft/id.rs`** (new): `raft_id`, `RaftIdError { Zero, Collision }`, and `check_raft_id`, as ADR-0035 point 2 and the old plan's step 2 specified (`twox_hash::XxHash3_64::oneshot`). Also `fn raft_id_text(RaftId) -> String`, 16 lowercase hex digits, and its parser (ADR-0040 point 3).
- **`src/control/raft/types.rs`** (new):
  - `ControlPeer { node_id: String, address: String }`, with `Display` as `<node_id>@<address>`.
  - `ControlRequest` (`#[non_exhaustive]`), holding a decoded `command: Command` for the proposer and the raw JSON for the log, and `screened: LeaderRef { term: u64, node: RaftId }`.
  - `ControlResponse`, `#[non_exhaustive]`:
    - `Applied { outcome: Outcome, state: Arc<State> }`;
    - `Superseded`;
    - `Raft`, for blank and membership entries.

  Without `serde`, openraft 0.9.25 asks only `OptionalSend + OptionalSync + 'static` of `AppData` and `AppDataResponse`.
- **`src/control/raft/codec.rs`** (new). FleetRing's encodings of every openraft value that is stored or sent, with `From`/`TryFrom` both ways:
  - log entries (`kind`, `payload`), votes, log IDs, memberships, and snapshot metadata;
  - the wire structs `AppendEntries`, `AppendEntriesReply` (success, conflict, higher vote, partial success), `VoteRequest`, `VoteReply`, `InstallSnapshotChunk`, `InstallSnapshotReply`, and `ProbeReply { node_id, software, has_raft_state }`.

  No struct denies unknown fields, so readers ignore them. Raft IDs are hex strings.

  `CodecError` (`#[non_exhaustive]`): `Json { what, source }` and `RaftId { text }`.
- **`tests/fixtures/raft-wire-v1/`** (new): one pretty-printed JSON file per message, entry kind, vote, log ID, membership (one config and joint), and snapshot metadata, with a `README.md` saying the files are append-only (ADR-0008's rule for golden vectors).
- **`src/upgrade/install.rs`:** `Reported` gains `control_format: u32` from `--software`'s `control_format`; a binary without `--software` (0.3.0) reports 1.
- **`src/upgrade/mod.rs`:**
  - `UpgradeError::ControlFormat { path: PathBuf, found: u32, release: Release, reads: u32 }`, with the message of ADR-0040 point 5 and the hint "roll forward, or see `fleetringd control downgrade` in the README" if open question 2 is accepted, else "roll forward".
  - Before swapping, `install` and `rollback` with `--config` read `meta.format` from `<data_dir>/control.db` when the file exists (read-only, `SQLITE_OPEN_READ_ONLY`) and refuse a target whose `control_format` is lower.
  - `check` reports the same verdict.
- **`src/control/log.rs`:** `pub(crate) fn file_format(path: &Path) -> Result<Option<u32>, LogError>`, which `upgrade` uses.

### Tests

- `tests/control.rs`, module `fixtures`: `the_control_fixtures_recover_to_their_replay`. For each fixture, `CommandLog::open(..).recover()` on a copy equals replaying `commands.json` with `apply` from `State::new(ControlConfig::new())`, and the 0.4.0 fixture's state has `undrain` enabled and `n2`'s software recorded.
- `src/control/raft/runtime.rs`:
  - `the_same_seed_draws_the_same_election_timeouts` (via `openraft::Config::new_rand_election_timeout::<ControlRuntime>()`);
  - `a_spawned_task_draws_from_its_spawners_generator`;
  - `two_generators_draw_independently`, interleaving draws of two scopes;
  - `a_draw_outside_a_scope_is_counted`.
- `src/control/raft/id.rs`: `raft_ids_are_stable` (a literal pasted once), `a_collision_names_both_nodes`, and `raft_id_text_round_trips_and_is_sixteen_hex_digits`.
- `src/control/raft/codec.rs`:
  - `every_golden_vector_round_trips`: decode each file into openraft's type, encode it back, and the bytes equal the file;
  - `unknown_fields_are_ignored`;
  - `a_command_payload_is_format_ones_command_json`: the `command` of an entry is byte-equal to what format 1 stores for the same `Command`;
  - `a_raft_id_above_two_to_the_63_survives_sqlite_and_json`.
- `tests/upgrade/main.rs` (fast tests, local release server and stand-in binaries):
  - `a_rollback_to_a_binary_that_reads_an_older_control_log_is_refused`: a data directory whose `control.db` has `format = 2`, a previous binary whose `--software` prints `control_format: 1`; nothing is swapped and the error names both formats;
  - `a_binary_without_software_reads_format_one`;
  - `a_data_only_node_has_no_control_format_to_check`.

### Acceptance criteria

- Every gate is green, including `cargo +1.89 check -p fleetringd` with openraft in the tree.
- Both fixtures are committed with their writers and the script, and recover to their replay.
- Every golden vector round-trips byte for byte.
- The runtime tests show per-scope determinism.
- `upgrade` refuses a format downgrade before swapping anything (tested with a hand-written format-2 `meta`; nothing in the daemon writes format 2 yet).
- The field-list test passes with an empty register.
- No existing test changes.

---

## Phase A2a: the format-2 store and the migration (`feat(fleetringd): add control.db format 2 and its migration from format 1`)

A2a adds `control.db` format 2 and the migration as library code, tested on its own. The daemon doesn't use it yet: the single writer, `CommandLog`, and format 1 stay as they are, and `software::running()` still reports `control_format = 1`. A2b switches the daemon over.

### Files

- **`src/control/store.rs`** (new), the format-2 store, `ControlStore`.
  - Its schema is ADR-0040 point 3's (`meta`; `log` with `kind` and `payload`, `leader_node` as hex text; `snapshots`), in WAL mode with `synchronous = FULL`.
  - Methods: `open`, `genesis`, `vote`/`save_vote`, `committed`/`save_committed`, `purged`, `last_log_id`, `append_entries`, `truncate_from`, `purge_through`, `entries`, `save_control_snapshot`, `control_snapshots`, `latest_control_snapshot`, and `committed_commands_after`. They take openraft's value types (`LogId`, `Vote`, `Entry`, `StoredMembership`) and convert them through `codec.rs`. openraft's traits are not implemented here.
  - `open(path, config, own: Option<(RaftId, ControlPeer)>)`.
  - `STORE_FORMAT = 2`, beside `log.rs`'s `FORMAT_VERSION = 1`, which A2b retires.
  - `LogError` gains `Migration { reason }`, `CorruptEntry { index, source }`, and `StrayTable { rows }` ("format-2 control log {path} has a format-1 `commands` table with {rows} rows").
- **The stray `commands` table** (ADR-0040 point 5). When `open` finds format 2:
  - an empty `commands` table is dropped in its own transaction, logged at info as `dropped the empty commands table an older release created`;
  - a non-empty one is `StrayTable`.
- **Migration in `ControlStore::open`** (ADR-0040 point 8), when `meta.format` is `"1"`:
  1. Recover with `log.rs`'s format-1 code.
  2. If `commands` starts at index 1 with no gaps, replay it all from genesis and require equality with the recovered state. A mismatch is `LogError::Migration { reason: "the snapshot at {s} doesn't match replaying the log from genesis; the first difference is at {i}" }`. A file without a full history, such as a down-converted one (open question 2), skips this step and says so at info level.
  3. `own` must be `Some`, else `Migration { reason: "the node's raft id is needed" }`.
  4. In one `BEGIN IMMEDIATE` transaction, do the renames and inserts of ADR-0040 point 8.
  5. Log at info: `migrated the control log from format 1 at index {N}`.

  An empty format-1 file becomes an empty format-2 store.
- **`tests/fixtures/control-writers/open_format2_0_4_0.rs`** (new; run at the 0.4.0 tag by `scripts/write-control-fixtures.sh stray`). It runs 0.4.0's `CommandLog::open` on a copy of `tests/fixtures/control-0.4.0-migrated/control.db`, which an ignored A2a test writes by migrating the 0.4.0 fixture. It requires `UnsupportedFormat`, and saves the result as `tests/fixtures/control-format2-opened-by-0.4.0/control.db`, which holds the empty `commands` table 0.4.0 leaves behind.

### Tests

`tests/control.rs`, module `store`:

- `appended_entries_survive_reopening`;
- `truncation_removes_the_tail_and_purging_the_head`;
- `vote_and_committed_survive_reopening`;
- `only_the_newest_snapshots_are_kept`;
- `committed_commands_skip_raft_entries_and_stop_at_committed`;
- `the_0_3_0_fixture_migrates_to_a_snapshot_of_its_replay`;
- `the_0_4_0_fixture_migrates_with_its_features_and_software`;
- `a_snapshot_that_disagrees_with_the_replay_refuses_migration` (a copy with its snapshot's state edited);
- `an_empty_format_one_log_migrates_to_an_empty_store`;
- `a_format_one_log_without_an_id_is_refused`;
- `a_newer_format_is_refused` (`format = 3`);
- `the_empty_table_0_4_0_leaves_is_dropped` (the `control-format2-opened-by-0.4.0` fixture opens, its `commands` table is gone, and its state is the migrated one);
- `a_non_empty_stray_table_is_refused`.

### Acceptance criteria

- Every gate is green.
- Both format-1 fixtures migrate to their replay.
- The stray-table fixture, written by the real 0.4.0 code, opens.
- The daemon's behaviour is unchanged: every existing test passes untouched.

---

## Phase A2b: a one-voter group behind the existing API (`feat(fleetringd): run the control node as a one-voter Raft group`)

After A2b every control node is a Raft group of one. The `Control` service, `ControlHandle::submit`, `state`, and `watch`, and every existing test keep working, but the storage, proposer, state machine, failover timer, metrics, and readiness are the group's.

### Files

- **`src/control/log.rs`** becomes a private `format1` module, used only by `ControlStore`'s migration and by `#[doc(hidden)] pub fn format1_recover(path)`, which the fixture test switches to. `ControlStore` takes over `pub use` as the control log, `FORMAT_VERSION` becomes `2`, and `software::running()` reports `control_format = 2`.
- **`src/control/raft/log_storage.rs`:** `RaftLog` (`Clone`; `Arc<Mutex<ControlStore>>` plus `StorageIo`), implementing `RaftLogStorage` and `RaftLogReader`. The callback `append(entries, callback)` fires after the transaction commits. Under `StorageIo::DedicatedThread` every call runs in `tokio::task::spawn_blocking`; under `StorageIo::Inline` it runs on the calling task (simulation).
- **`src/control/raft/state_machine.rs`:** `ControlStateMachine`, as the old plan's step 4 specified, with ADR-0040's changes:
  - **`apply(entries)`.** The skip rule applies to `kind = 'command'` entries only; blank and membership entries have no `screened` field. A command entry whose `screened` leader ID differs from its own log ID's leader ID returns `ControlResponse::Superseded` without decoding the command and without changing state. A command entry that matches decodes the raw command. If decoding fails, the node is fail-stop (ADR-0040 point 4): it stops applying at that entry, never applies past it, and its `control_log` readiness check fails with "entry <i> needs fleet feature '<f>', which fleetringd <version> lacks; upgrade this node", or names the decoding error when no feature explains it. Otherwise it runs `fleetring_control::apply`, publishes `AppliedState { index, state }` after every entry, and returns `Applied`. Membership and blank entries return `Raft`.
  - **Snapshots.** `build_snapshot` and `install_snapshot` use point 3's encoding. `install_snapshot` runs `check_state` against the store's genesis, or records the snapshot's `config()` as genesis on a fresh joiner.
  - **`ControlMetrics::command`** is counted here, on every node.
- **`src/control/raft/proposer.rs`** (new): the proposer of ADR-0040 point 2.

  ```rust
  /// Screens, stamps, and proposes control commands on the leader (ADR-0040 point 2).
  pub(crate) struct Proposer { raft: Raft<ControlTypes>, clock: Box<dyn Clock>, readiness: watch::Receiver<Readiness>, gate: GroupGate, metrics: ControlMetrics }

  /// Whether this node may propose, and the screening state if it may.
  #[derive(Debug, Clone)]
  pub(crate) enum Readiness { Follower { leader: Option<ControlPeer> }, CatchingUp { term: u64 }, Ready { leader: LeaderRef } }
  ```

  - The proposer runs as one task with a bounded mailbox (`with_mailbox_capacity`). It watches `raft.metrics()` and becomes `Ready` when `current_leader == own`, and `last_applied`'s leader ID has this node and the current term. It clones the applied state into `S` then.
  - It drops `S`, answering nothing new, on any change of term or leader.
  - For each submission:
    1. stamp;
    2. screen with `S.check_admission` and `GroupGate::check`;
    3. `apply` to `S`;
    4. `client_write_ff(ControlRequest { screened, command })`;
    5. spawn a waiter on the responder that maps `Applied` to `Committed`, `Superseded` to `SubmitError::Superseded`, `ForwardToLeader` to `NotLeader`, and a dropped responder to `Stopped`.
  - `GroupGate` is a stub in A2b that admits everything; with one voter, the gate is the fleet's. B fills it.
- **`src/control/node.rs`:**
  - `ControlNode::open(path, options, clock, own: ControlPeer, network: Network, seed: RaftSeed)`, run inside `with_generator(seed, ..)`.
    1. Open the store and the state machine.
    2. Build `openraft::Config` from the options (`cluster_name: "fleetring-control"`, the election timeouts, the heartbeat, `SnapshotPolicy::LogsSinceLast(snapshot_every)`, `max_in_snapshot_log_to_keep: log_keep`, `purge_batch_size: 256`, `snapshot_max_chunk_size: 1 << 20`) and `validate()` it.
    3. Call `Raft::new` with a `NoPeers` network factory, which answers every call `Unreachable`; B replaces it.
    4. If the store is empty, with `Bootstrap::New` call `initialize({own})`. With `Join`, log at info `waiting to be added to a control group`.
    5. If the membership's only voter is `own`, call `trigger_elect()` (ADR-0040 point 9).
    6. Log `control state recovered`.
  - `Bootstrap`, the new `ControlOptions` setters, `OpenError` (`RaftId`, `RaftConfig`, `Raft`; `Thread` and `Stopped` go), and `SubmitError`. New `SubmitError` variants: `NotLeader { leader: Option<String> }`, `CatchingUp { leader: Option<String> }` ("the control leader is catching up; retry"), and `Superseded` ("the control leader changed before the command was logged; retry"). `Refused` and `Storage` keep their meaning.
  - The `ControlHandle` surface of the old plan's step 5: `submit`, `state`, `watch`, `applied`, `ensure_linearizable`, `leadership`, `group`, `is_running`, `queued`. The group-changing methods arrive in B.
- **`src/control/service.rs`:** `submit_status` maps `NotLeader { Some(a) }` to `not_leader(a)`, and `NotLeader { None }`, `CatchingUp`, and `Superseded` to `UNAVAILABLE` (the first two with the hint when known).
- **`src/control/failover.rs`:** runs only while `Readiness::Ready`. `started` is a `tokio::time::Instant` taken at readiness, and the settle window is measured from it (ADR-0040 point 6). Notices for promotions already in progress go out at once. A submit that returns `NotLeader`, `CatchingUp`, or `Superseded` ends the inner loop, and `Stopped` or `Storage` ends the task.
- **`src/config.rs`** (`ControlSettings`): `bootstrap` (default `"new"`), `election_timeout_ms` (1,000), `raft_heartbeat_ms` (250), `log_keep` (1,000), and `snapshots_retained` (2). The validation of the old plan's step 9: the election timeout must be at least three Raft heartbeats, and the lease duration at least twice the longest election timeout. `addresses` arrives in E.
- **`src/dev.rs`:** with `dev = true`, `validate` refuses `control.bootstrap = "join"` ("dev mode runs one control node, which starts its own group").
- **`src/environment.rs`:** `Environment::with_raft_seed(RaftSeed)` and `raft_seed()`. Production reads 8 bytes from `ring::rand::SystemRandom` in `Environment::production()`. The simulation sets `RaftSeed::for_host(seed, host)` in `run_host`.
- **`src/daemon.rs`:**
  - Bind the listener before opening the control node, and pass `own = ControlPeer { node_id, address: advertised(&config, bound) }` and the seed.
  - `ControlRole` keeps its shape.
- **`src/observe/`** (ADR-0040 point 10):
  - the new gauges and counters in `names.rs`, with `OUTCOMES_PROPOSAL = ["committed", "refused", "superseded", "not_leader"]`;
  - `ControlMetrics` methods for each;
  - a task over `raft.metrics()` that sets the gauges;
  - `health.rs`: `leader` passes when a leader is known, and `control_group` is new.
- **`src/sim/check.rs`:** `control_log_replays` is rewritten for format 2, as the old plan's step 12 specified: it starts at the oldest retained snapshot, or at genesis while nothing is purged; at each newer snapshot it must reproduce that snapshot; and it must reach the published state at its index. Superseded command entries are skipped. `ControlLog` gains `replayed_from`. One control host for now.
- **`src/sim/fleet.rs`:** `Timings` gains `election_timeout` (250 ms in the default fleet), `raft_heartbeat` (50 ms), and `log_keep` (`NonZeroU64::MAX`, so the default fleet never purges). `host_config` sets them.
- **`tests/upgrade/rolling.rs`** (the nightly test): after the control node moves to this build, it asserts that `fleetringd control list`, once C lands (until then `status`), shows the control node as the only voter, that every database the fleet created is still listed at its epoch, and that `fleetringd upgrade --rollback --config` back to the baseline is refused with the `ControlFormat` error. It then runs the baseline binary on the data directory directly, as a rollback that bypassed the check would, expects `UnsupportedFormat`, and restarts this build on the same directory, which must drop the stray `commands` table and serve.

### Tests

- **`tests/control.rs`.** Every existing test keeps passing. Constructors change, and `ControlNode::open` takes `own`, `network`, and `seed`, so `tests/support` gains `fn control_peer()` and `fn test_seed()`. A2a's `store` tests keep passing.
- **`src/control/raft/state_machine.rs`:**
  - `applying_entries_matches_applying_commands`, with the 0.4.0 fixture's 60 commands interleaved with a membership and a blank entry;
  - `a_rejection_still_moves_the_applied_index`;
  - `an_entry_appended_in_another_term_is_superseded_without_decoding` (its raw command is invalid JSON for `Command`, and nothing fails);
  - `blank_and_membership_entries_are_never_skipped`;
  - `an_undecodable_command_stops_applying_and_says_why` (a matching entry whose action is unknown: the applied index stays before it, and readiness names the entry).
  - `a_built_snapshot_installs_to_the_same_state`;
  - `a_snapshot_with_another_configuration_is_refused`.
- **`src/control/raft/proposer.rs`** (`#[tokio::test(start_paused = true)]`, one voter):
  - `the_proposer_refuses_until_its_term_is_applied`;
  - `a_join_screened_behind_a_pending_join_sees_it`: a 0.4.0 join and a join two lines away are submitted together; the second is `Refused` and the log holds only the first;
  - `a_join_refused_for_its_software_is_never_logged` (the existing test, now through the proposer);
  - `proposals_enter_the_log_in_screening_order`;
  - `queued_commands_share_a_sync`: 50 concurrent submits take fewer than 50 appends (count `append` calls).
- **Control node:**
  - `a_new_control_node_elects_itself_without_waiting` (ready before `election_timeout_min`);
  - `a_joining_control_node_waits_to_be_added`;
  - `the_migrated_fixtures_elect_themselves_with_the_same_state`;
  - `snapshots_compact_the_log` (`snapshot_every = 10`, `log_keep = 5`, `snapshots_retained = 2`, then 100 ticks: at most 15 entries and 2 snapshots, and the state survives reopening);
  - `a_new_leader_waits_a_lease_duration_before_promoting`;
  - `a_wall_clock_step_does_not_shorten_the_settle_window` (the `SimClock` jumps 1 h at readiness).
- **Config:** one test per new validation, `dev_mode_refuses_bootstrap_join`, and `the_defaults_validate`.
- **Observe:** `the_control_group_families_are_listed_from_the_first_scrape`, and `readiness_reports_leader_and_control_group` (ready on a one-voter group; `control_group` fails on a `Join` node).
- **Simulation:** every existing scenario passes on the fixed seeds. A test that compares a full trace under `Replay::Exact` is updated once, and the PR says which.

### Acceptance criteria

- Every gate is green, and every existing control and simulation test passes with constructor changes only.
- `fleetringd dev` starts and is ready in the same time as before (±200 ms on the CI runner).
- Both fixtures migrate to their replay under a running daemon, and the nightly rolling-upgrade test passes from 0.4.0 to this build with the new assertions (run it once by hand on the PR, as `RELEASING.md` describes).

---

## Phase B: the Raft network, `Probe`, and the `control peer` binding (`feat(fleetringd): carry Raft messages between control nodes, bound to the control group`)

### Files

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
  ```

  Add `` `control peer` (a voter or learner of the receiver's control group, or any node of the fleet while the receiver has none) `` to the header's list of bindings.
- **`crates/fleetring-proto/src/lib.rs`:** `PROTOCOL_VERSION` 1.1 → 1.2 (ADR-0040 point 11), and the module docs name the `Raft` service.
- **`src/auth/binding.rs`:** `Binding::ControlPeer` (`"control peer"`) and `Unbound::NotControlPeer` ("not a member of the control group").
- **`src/auth/policy.rs`:** four rows, `rpc("/fleetring.v1.Raft/<Method>", Requirement::Node, Binding::ControlPeer)`.
- **`src/transport.rs`:** `raft_client` and `raft_server`, with `MAX_MESSAGE_BYTES`.
- **`src/control/raft/network.rs`:** `ControlNetwork` (`RaftNetworkFactory`) and `PeerConnection` (`RaftNetwork`), as the old plan's step 6 specified, with the encoding through `codec.rs`.
  - Each request carries `software::running()` and the clock's reading; each reply's `sender` and `sent_at_ms` are recorded in `GroupSoftware`.
  - A dial failure, a timeout, or `UNAVAILABLE` is `Unreachable`, and the channel is forgotten. Any other status, or a payload that doesn't decode, is a network error.
  - `ControlNetwork::probe(address) -> Result<ProbeReply, ProbeError>`.
- **`src/control/raft/group.rs`** (new):
  - `GroupSoftware`, a `Mutex<BTreeMap<RaftId, Heard { software: Software, offset_ms: i64, at: tokio::time::Instant, settles_at: Option<LogIndex> }>>`, shared by the network, the service, and the proposer's `GroupGate`. A new term on this node clears it.
  - **Staleness** (ADR-0040 point 4). A report counts as known only while `at` is within one shortest election timeout. A report whose release or features differ from the previous one records `settles_at`, the proposer's last proposed index then, and counts as unknown until the applied index reaches it.
  - `GroupGate::check(&self, action, members: &BTreeSet<RaftId>, state: &State) -> Result<(), Rejection>`, implementing ADR-0040 point 4's first-use and window rules; the "control node '<id>' hasn't reported its software yet" case is a `Rejection::FeatureUnsupported` whose release is `0.0.0`. Wired into the proposer, replacing A2b's stub.
  - The largest absolute offset is exported as `fleetring_control_clock_offset_max_seconds`, with a warning, at most once a minute, above the failover margin.
- **`src/control/raft/service.rs`:** `RaftService`, as the old plan's step 6 specified, with these changes:
  - It checks the binding against the membership in `raft.metrics()`; an empty membership accepts any node certificate of the fleet.
  - It records the request's `sender` and `sent_at_ms`, decodes through `codec.rs` (`INVALID_ARGUMENT` if the payload doesn't decode), and answers with its own `sender` and `sent_at_ms`.
  - `Probe` answers `ProbeReply { node_id, software, has_raft_state }`.
- **`src/control/node.rs`:**
  - `ControlNode::open` uses `ControlNetwork` instead of `NoPeers`.
  - `raft_service()`.
  - The `ControlHandle` methods `add_node(node, address)`, `set_voters(voters)`, and `remove_node(node)`, with `GroupError` as the old plan's step 5 specified, plus `GroupError::Probe { node, reason }` (`FAILED_PRECONDITION`).
  - `add_node` probes first and refuses as ADR-0040 point 4 lists: wrong ID, Raft state present, outside the window, a missing enabled feature, or `UNIMPLEMENTED`. Then it calls `add_learner(id, peer, true)`.
- **`src/daemon.rs`:** `serve` adds the `Raft` service when the node runs the control role.

### Tests

`tests/control.rs`, module `raft_network`: three control nodes in one process over TCP, with node certificates from `support`.

- `three_control_nodes_elect_one_leader`: `c1` with `New`, `c2` and `c3` with `Join`; `add_node` twice and `set_voters({c1, c2, c3})`; every node agrees on one leader and term; a `Join` submitted on the leader shows in all three states.
- `a_node_outside_the_group_is_refused_raft_calls` (`PERMISSION_DENIED`, `fleetring-binding: control peer`).
- `a_fresh_joiner_accepts_its_first_leader`.
- `a_node_with_raft_state_is_not_added`: `c4` started with `New`.
- `a_node_without_a_raft_service_is_not_added`: a stub server answers `UNIMPLEMENTED`.
- `a_node_outside_the_window_is_not_added`: a stub `Probe` reports 0.7.0.
- `the_leader_learns_every_members_software_within_a_heartbeat`.
- `a_report_older_than_an_election_timeout_counts_as_unknown` (paused time: stop `c3`; after one election timeout a first use of `undrain` is refused as "hasn't reported its software yet").
- `a_changed_report_settles_before_it_counts`: `c3`'s report drops `undrain` while a first use of `undrain` is in flight; the gate treats `c3` as unknown until that proposal is applied, then refuses the next first use naming `c3`.
- `the_first_use_of_a_feature_waits_for_every_control_node`: `c3` reports software without `undrain` (set through `Environment::with_software`, added here for tests and D); `Undrain` is `Refused` naming `c3`; after `c3` reports it, `Undrain` is accepted.
- `a_join_outside_a_control_nodes_window_is_refused`.
- `a_leader_crash_loses_no_acknowledged_command`: crash `c1` mid-stream; every acknowledged `Join` is in the new leader's state; every unacknowledged submitter got an answer.
- The `auth` table tests pick up the four rows automatically.

### Acceptance criteria

- Every gate is green.
- A three-node group forms and survives a leader crash in-process.
- Raft calls from outside the group are refused and audited.
- The group gate and `Probe` refuse what ADR-0040 point 4 lists.
- The protocol says 1.2.

---

## Phase C: the control group's commands (`feat(fleetringd): add fleetringd control add, remove, set-voters, and list`)

### Files

- **`fleetring.proto`:** the four group RPCs in `service Control` and the request messages and `ControlGroup`, as the old plan's step 7 specified (`Scope: fleet`). The `Control` header says every RPC is answered by the control group's leader. `ControlNode` comes from E; if E hasn't landed, C adds it as E specifies, and E drops it from its diff.
- **`src/auth/policy.rs`:** the four rows, `Requirement::Fleet` and `Binding::None`.
- **`src/control/service.rs`:**
  - `locate`, `list_databases`, `watch_membership`, and `describe_control_group` call `ensure_linearizable` first.
  - `watch_membership` ends with the redirect when this node stops leading.
  - `join` and `heartbeat` fill `control_nodes` from `group()`, replacing E's single-node view.
  - The four group RPCs map `GroupError` as the old plan's step 7 specified, plus `Probe` → `FAILED_PRECONDITION`.
  - `add_control_node` on a dev node refuses a non-loopback address with `dev::check_loopback`, a new function shared with `dev::validate`.
  - `leave` refuses a node that is a voter or learner of the group (ADR-0040 point 9) before it submits.
- **`src/control/absent.rs`:** `NoControl` refuses the four RPCs.
- **`src/control/client.rs`:** `add_control_node`, `remove_control_node`, and `set_control_voters`, each sent once through `following`, and `describe_control_group`, retried.
- **The announcement of the next software** (ADR-0040 point 4):
  - `src/upgrade/mod.rs`: on a node whose data directory holds a `control.db`, `install` and `rollback` with `--config` write `<data_dir>/fleetringd.next-software` (the target's `--software` JSON), send `SIGUSR1` through the process file as the restart does, and poll `DescribeControlGroup` until this node's software there is the reduced one and marked settled (bounded by `--timeout`; on timeout they undo the announcement and refuse). Only then do they check enabled features and swap. If a feature the target lacks was enabled meanwhile, they delete the file, signal again, and refuse with `MissingFeature`.
  - `src/daemon.rs`: `SIGUSR1` re-reads the file, and the daemon reports the lower release and the shared features in every `RaftPayload`. At startup it deletes the file once its running software matches it.
  - `ControlNode` in `DescribeControlGroup` gains `bool software_settled = 7`.
- **`src/main.rs`:** the `control` noun and its four subcommands, as the old plan's step 11 specified.
  - `remove` confirms on a terminal, and exits 2 without one unless `--yes`.
  - `set-voters` with one voter prints the warning `a control group of one voter has no redundancy` on stderr.
  - `list` prints a table: `NODE ADDRESS VOTER LEADER MATCHED VERSION`, then `term <t>, committed index <i>`. If FR-056 has landed, it also takes `-o json`; if not, a `// FR-056:` note marks the place.
- **`docs/cli.md`:** `control` in section 1's nouns, and the row "Control group commands | `control add`, `set-voters`, `remove`, `list` | Done (FR-064)".
- **`tests/control_group.rs`** (new integration file, with real daemons over TCP).

### Tests

- In `tests/control.rs`:
  - `a_follower_redirects_every_control_call_to_the_leader` (iterating `auth::RPCS` rows of `Control`);
  - `a_read_after_a_write_sees_it_after_a_leader_change`;
  - `watch_membership_ends_when_its_node_stops_leading`;
  - `a_watch_on_a_follower_is_redirected`;
  - `join_and_heartbeat_replies_name_the_control_group`;
  - `group_calls_need_the_fleet_scope`;
  - `node_remove_refuses_a_control_group_member` (the message names both commands);
  - `a_dev_node_refuses_a_non_loopback_control_node`.
- `tests/control_group.rs`:
  - `a_fleet_grows_from_one_control_node_to_three_while_writing`, as the old plan's step 14 specified, with `d1` and `d2` on `control.address = c1`: after `c1` stops they reach the new leader through the addresses they learned (E);
  - `a_migrated_control_node_keeps_its_fleet` (the 0.4.0 fixture under a real daemon: `ListDatabases` returns its two databases at their epochs; `control list` shows one voter).
- `tests/daemon.rs`: `control_list_shows_a_single_control_node_as_its_voter_and_leader`.
- `tests/control_group.rs`:
  - `an_announced_rollback_blocks_a_first_use_until_it_completes`: three real control nodes; announce the previous line's software on `c3` (write the file, send `SIGUSR1`); once settled, `node undrain` is refused naming `c3`;
  - `a_first_use_racing_an_announcement_makes_upgrade_refuse`: the first use commits before the announcement settles, and the rollback is refused with `MissingFeature` and nothing swapped.
- `tests/upgrade/main.rs`: `an_announcement_that_never_settles_is_undone` (a stub control plane that never reports settled: the file is removed and nothing swapped).

### Acceptance criteria

- Every gate is green.
- The growth test passes within its bounds.
- Every `Control` RPC on a follower redirects.
- `node remove` refuses group members.
- The CLI matches `docs/cli.md`.

---

## Phase D: simulation groups of three and five (`test(fleetringd): simulate control groups, their failures, and mixed releases`)

### Files

- **`src/sim/fleet.rs`:**
  - `FleetBuilder::control_group(names)` (the first host gets `bootstrap = "new"`, the rest `"join"`; data hosts get `control.addresses` with every control host);
  - `FleetBuilder::clock_skew(host, millis)` (bases `sim_elapsed + SKEW_FLOOR_MS + skew`; `|skew|` at most `SKEW_FLOOR_MS = 600_000`);
  - `FleetBuilder::software(host, Software)` (through `Environment::with_software`);
  - `FleetBuilder::previous_line(host)`: the host decodes installed snapshots and log entries through a filter that drops every JSON field missing from `fields-0.4.0.txt` (A1), as an older line would. A test-only seam, `Environment::with_decode_filter`, makes this possible;
  - `Fleet::announce_software(host, Software)`: the same hook `SIGUSR1` drives;
  - `Fleet::build` forms the group through `AddControlNode` and `SetControlVoters` as an `operator` client;
  - `control_hosts`, `control_leader`, `control_handle` (the leader's), `control_log_of`, and `await_control_converged`;
  - `EventKind::Leader { term, leader }` from each control host's `leadership()`.

  These are all as the old plan's step 12 specified. Per-host Raft seeds already come from A2b.
- **`src/sim/check.rs`:**
  - `Invariant::ControlLogsAgree` and `Invariant::OneLeaderPerTerm`, with `control_logs_agree(&[(host, path)])` and `one_leader_per_term(&trace)`;
  - `Invariant::EveryControlNodeApplies` ("every control node applies its log"): no control host ever stops applying at an entry it can't decode;
  - `Invariant::PreviousLineDecidesAlike`: on every host with `previous_line`, the state reached through the filter makes the same decisions as the leader's at the same index (`liveness` of every member, `promotions_due`, each lease's holder, epoch, and expiry, the enabled features, the membership epoch);
  - `control_log_replays` runs for every control host against the last state each published;
  - `lease_events_match_control` runs only when replay started at genesis.
- **`src/sim/trace.rs`:** the `Leader` variant; `Replay::Order` includes it.
- **`tests/sim/control_group.rs`** (new; `mod control_group;` in `tests/sim/main.rs`): the scenarios below.

### Scenarios

| Scenario | Fleet | Faults | Asserts, beyond `Fleet::finish` |
|---|---|---|---|
| `a_leader_crash_under_writes_loses_no_control_command` | 3 control, 3 data, R = 3 | Crash the leader at 2 s while `CreateAndWrite` runs and a driver joins and heartbeats 12 fake nodes; restart it at 4 s. | A new leader within 3 × the election timeout; every acknowledged join and create is in every control node's state; no database's epoch changed. |
| `an_isolated_leader_commits_nothing` | 3 control, 2 data | Partition the leader from both other control nodes (not from data nodes) for 3 s, then repair. | Nothing it proposed after isolation commits; no command sent to it then is acknowledged unless it is in the others' logs; `ControlLogsAgree`. |
| `a_refused_join_never_reaches_a_log_across_leader_changes` | 3 control, 2 data on 0.5 | A driver sends joins from software two lines away and from the next line, interleaved, while the leader is crashed twice. | No control node's log holds a join `check_admission` refuses against the state before it; every refused join was answered `FAILED_PRECONDITION`. |
| `a_mixed_release_group_waits_for_every_voter` | 3 control (one reporting the previous line without `undrain`), 2 data | `Undrain` at 1 s; "upgrade" the old voter (restart with this build's software) at 2 s; `Undrain` at 3 s. | The first `Undrain` is refused naming the old voter; the second is accepted; `undrain` is enabled on every control node. |
| `a_rolling_restart_keeps_the_control_plane` | 3 control, 3 data | Stop and restart each control node in turn, 2 s apart, leader last. | No write gap longer than a lease duration; at most three leader changes. |
| `five_control_nodes_survive_two_crashes` | 5 control, 3 data | Crash two control nodes, the leader included. | A leader among the other three; commands keep committing; after restarts, `await_control_converged`. |
| `skewed_clocks_never_make_two_primaries` | 3 control (−5 s, 0, +5 s), 3 data | Crash the leader twice; crash one primary meanwhile. | `OnePrimaryPerEpoch`; the history checker passes; applied stamps, after clamping, never decrease. |
| `a_fleet_grows_from_one_control_node_to_three` | 1 control, then 2 joining; 3 data | Start `c2` and `c3` at 1 s; add them and set voters at 1.5 s under `CreateAndWrite`; crash `c1` at 4 s. | Three voters; data nodes find the new leader; no write fails longer than one election plus a lease renewal. |
| `a_late_learner_catches_up_from_a_control_snapshot` | 3 control, 2 data; `snapshot_every = 20`, `log_keep = 10` | 600 heartbeats, then add a fourth control node. | The learner installed a snapshot (its first log index is above 1) and its state equals the leader's; the leader's log never held more than 30 entries; at most 2 snapshots on every node. |
| `a_rollback_racing_a_first_use_never_strands_a_voter` | 3 control, 2 data | A driver issues the first `Undrain` at random times while `c3`'s next software (the previous line, without `undrain`) is announced; `c3` restarts as `previous_line` once the announcement settles. | Either `Undrain` committed first and `c3`'s rollback is abandoned (the announcement undone), or `Undrain` is refused naming `c3`; `EveryControlNodeApplies`. |
| `a_previous_line_voter_decides_alike_from_snapshots` | 3 control, `c3` as `previous_line`; `snapshot_every = 20`, `log_keep = 10` | Isolate `c3` for 300 heartbeats so it catches up by `InstallSnapshot`; crash the leader once. | `PreviousLineDecidesAlike`; `ControlLogsAgree`. |
| `one_hosts_crash_does_not_move_anothers_elections` | 3 control | Run seed 7 twice, once crashing `c3` at 1 s. | `c1`'s and `c2`'s election timeouts (logged as trace events) agree up to the crash's first effect on them. |

**Seed budget.** The scenarios run over `seeds::FIXED_HEAVY`, except `five_control_nodes_survive_two_crashes`, `a_late_learner_catches_up_from_a_control_snapshot`, and `a_refused_join_never_reaches_a_log_across_leader_changes`, which run on seed `1` alone. Together they add at most three minutes to the fixed-seed job (owner decision, 2026-11-03). Measure on the PR; if they exceed it, move more to seed `1` and say so in Progress. The nightly sweep runs them all on its random seeds.

### Acceptance criteria

- Every scenario passes on the fixed seeds, with every invariant `Fleet::finish` checks.
- A failing seed prints its rerun command and replays exactly under `--cfg tokio_unstable`.
- Every existing scenario passes unchanged.
- The fixed-seed job stays within budget.

---

## Phase E: data-node `addresses` and rotation (`feat(fleetringd): reach the control group through any of its addresses`)

E doesn't depend on Raft. Today's single control node answers with itself, and C switches the answer to the group.

### Files

- **`fleetring.proto`:**
  - `message ControlNode { string node_id = 1; string address = 2; bool voter = 3; bool leader = 4; uint64 matched_index = 5; Software software = 6; }`;
  - `repeated ControlNode control_nodes = 4;` on `JoinResponse` and `HeartbeatResponse`, with the comment "Empty from a control node that predates the field".

  These are part of protocol 1.2 whichever of B and E lands first. The constant only has to be right at release, and B bumps it.
- **`src/config.rs`:**
  - `ControlSettings.addresses: Vec<SocketAddr>`, and `control_addresses()`, as the old plan's step 9 specified;
  - validation: both `address` and `addresses` is invalid, and neither is missing;
  - `Config::control_target` returns the first address.
- **`src/dev.rs`:** with `dev = true`, every entry of `control.addresses` must be loopback.
- **`src/environment.rs`:** `Network::control_client(addresses: Vec<String>)`.
- **`src/control/client.rs`:** `Addresses { configured, learned, current }`, `with_addresses`, `with_connector_at`, `learn`, and the rotation in `following`, as the old plan's step 10 specified.
- **`src/membership.rs`:** `control.learn(&response.control_nodes)` after each successful join and heartbeat.
- **`src/control/service.rs`:** `join` and `heartbeat` fill `control_nodes` with this node alone (voter, leader, its advertised address, `software::running()`), through `fn control_nodes(&self) -> Vec<proto::ControlNode>`, which C replaces.
- **`src/daemon.rs`:** `control_addresses(config, bound) -> Vec<String>`.
- **`deploy/fleetringd.toml`:** `addresses`, commented.

### Tests

- In `tests/control.rs`, module `client`, against stub `Control` servers:
  - `the_client_moves_to_the_next_address_when_one_is_down`;
  - `the_client_follows_a_learned_leader`;
  - `the_client_gives_up_after_every_address_has_failed`;
  - `a_single_control_node_names_itself_in_join_replies`.
- Config tests for both and neither, and `dev_mode_refuses_non_loopback_control_addresses`.

### Acceptance criteria

- Every gate is green.
- A data node configured with `[dead, live]` heartbeats to `live`.
- A 0.4-shaped reply without `control_nodes` leaves the client on its configured addresses.

---

## Phase F: documentation and acceptance (`docs: describe the control group`)

- **`docs/lexicon.md`:**
  - new entries **Control group**, **Learner**, **Voter**, and **Term**, with the old plan's text;
  - the edits to **Control node**, **Leader**, **Control log** (its Not line names the Raft log's membership and blank entries, and superseded commands), and **Epoch**.
- **`crates/fleetringd/README.md`,** a "Control group" section:
  - why three or five voters, and `bootstrap`;
  - growing from one to three, with the exact commands;
  - replacing a failed control node: add a new one, `set-voters` without the failed one, `remove` it, then `node remove` if it was also a data node;
  - what data nodes see during an election, and the 0.4 data nodes' single address;
  - the timings and their validation, and compaction;
  - rolling upgrades of a group (data nodes, then learners, followers, the leader last);
  - the rollback refusal and its reason;
  - NTP and the clock-offset gauge.

  The RPC table gains the `Raft` and group rows.
- **`deploy/fleetringd.toml`:** every new `[control]` field, commented, with its default.
- **`SECURITY.md`:** the "Control group" paragraph of the old plan, plus: a control node decodes only commands it applies (superseded entries stay undecoded), and `Probe` reveals a node's ID, its software, and whether it holds Raft state to any node certificate of the fleet.
- **`docs/roadmap.md`:** E5's first line names FR-064 and ADR-0035/0040; E2's compaction line names FR-064.
- **`docs/adr/README.md`, ADR-0035, and ADR-0040:** the status lines of the first acceptance criterion, once the owner accepts ADR-0040. ADR-0017's status line gains "Adding Raft later: resolved by 0035".
- **This plan's** Progress, and the issue's checklist.

---

## Owner decisions (2026-11-03)

Decided as recommended when ADR-0035 was accepted, with the release numbers corrected for 0.4.0 having shipped:

- A control node waiting to join trusts any node certificate of the fleet until it is added. Certificates that mark a node as a control node are a follow-up ticket.
- The protocol minor version: ADR-0038 has since taken 1.1, so FR-064 takes 1.2 and FR-066 the next, or the reverse if FR-066 lands first (ADR-0040 point 11).
- `set-voters` may shrink the group to one voter, with a CLI warning.
- Control-group scenarios add at most 3 minutes to the fixed-seed simulation job.
- `commands_v1` is kept by the release that adds Raft and dropped by the line after it. The decision said "0.4"; that release is now 0.5.

## Open questions for the owner (2026-11-04)

1. **ADR-0040 or a rewrite of ADR-0035?**
   - Options: keep ADR-0040 as an amendment of the accepted ADR-0035, or fold it into ADR-0035 and set that back to Proposed.
   - **Recommended: keep ADR-0040.** ADR-0035 is accepted, and `docs/adr/README.md` and `CLAUDE.md` say to amend accepted decisions. Folding it in is a mechanical change if you prefer one file.
2. **A down-conversion so a one-voter control node can roll back from 0.5 to 0.4?**
   - Without one, the first start on 0.5 is a one-way door for control nodes. `upgrade` refuses the rollback, and restoring a pre-upgrade backup can reissue lease epochs.
   - **Recommended: yes, as a small PR after A2a, in parallel with A2b and B.** It is offline (daemon stopped), allowed only for a one-voter group, and `fleetringd control downgrade --to-format 1 --config <file>` writes a format-1 file holding the current state as a snapshot at the last applied index N, plus a `Tick` at N, which format 1 requires and recovery never replays. It keeps the format-2 file as `control.db.format2`, reuses an empty `commands` table that a bypassed 0.4 start left behind and refuses a non-empty one, and refuses when the state uses a feature 0.4 lacks. That keeps ADR-0038's promise that rollback is safe until a new command is used.
3. **Should `node remove` refuse a control group member?**
   - Options: refuse; remove from both the fleet and the group; or only warn.
   - **Recommended: refuse, naming `control set-voters` and `control remove`.** Removing from both can't be atomic (Raft configuration and control state are separate), and a warning leaves a dead voter counting toward quorums.
4. **Does a down control node block the first use of a new fleet feature?**
   - Options: block until it reports or is removed; or ignore unknown members.
   - **Recommended: block.** It is ADR-0038's rule for down fleet members, and ignoring it risks a voter that can't decode the log when it returns.
5. **Production seeds for openraft's generator.**
   - Options: OS entropy at startup; or a seed derived from the Raft ID.
   - **Recommended: OS entropy (`ring`'s `SystemRandom`).** Derived seeds would make two nodes that restart together draw related sequences; the simulation still injects its own.
6. **openraft 0.9.25 now, or wait for 0.10.0 final?**
   - **Recommended: 0.9.25 now.** The FleetRing-owned formats make 0.10 a code change later, not a migration, and 0.10 has no release date.
7. **Leader transfer before restarting a leader** (openraft 0.10 has it, 0.9 doesn't).
   - **Recommended: accept one election (1–2 s) per leader restart for now,** and file a follow-up for the 0.10 port.

## Open risks

- **Two `rand` versions.** openraft 0.9 brings `rand 0.8` beside the workspace's 0.9 and 0.10. `cargo deny` only warns about it.
- **Heartbeats through Raft.** Every heartbeat becomes a replicated entry. Group commit keeps syncs few, but this is where the group's throughput binds first. Roadmap E4 moves heartbeats off the log.
- **Read latency.** `ensure_linearizable` costs a heartbeat round per read. FR-066's router must cache placement rather than call `Locate` per request.
- **Clock skew** delays failovers after a leader change by up to the skew, and denylist entries can be forgotten that much early (ADR-0040 point 6). Safety is unaffected, and the clock-offset gauge shows it.
- **The fresh-joiner window** (ADR-0035 point 13) stays until control-node certificates exist. `Probe` narrows the operator's part: a joiner with state is refused.
- **Migration is one-way** unless open question 2 is accepted.
- **A rollback without `--config`** skips the software announcement, and can leave a voter stopped at an entry it can't decode until it is rolled forward (ADR-0040 point 4). It is fail-stop, and the readiness check names the fix.
- **Simulation budget.** Five-node groups and compaction runs are heavy; the seed split above may need trimming.

## Progress

- 2026-11-04: ADR-0040 proposed, amending ADR-0035 for versions, rollback, dev mode, and the leader's screen. This plan was rewritten into phases A1–F around it. openraft 0.9.25's runtime hook was verified in a scratch crate: the same seed gave the same election timeouts across a spawned task. No Raft code is written yet.
- 2026-11-04: after an independent review, ADR-0040 gained the staleness, announcement, and fail-stop rules, the stray-table rule, the rule on fields an older line drops, and the time assumptions. A2 was split into A2a (store and migration as library code) and A2b (openraft wiring).
