# ADR-0035: The control log is replicated by a Raft control group on openraft, with one code path from one voter to five

- **Status:** Proposed
- **Date:** 2026-11-03
- **Related:** ADR-0017 (resolves its "Adding Raft later"), ADR-0018, ADR-0019, ADR-0020, ADR-0022 (extends point 6), ADR-0025, ADR-0026, ADR-0031; tickets FR-064

## Context

The control plane is one node (ADR-0017 point 3). Its command log is a SQLite file on that node, its writer stamps and applies every command, and its published state answers every control RPC. While it is down, primaries keep serving (ADR-0018 point 6), but nothing else moves: no promotion after a primary dies, no new database, no join or leave, no lease transfer for a drain, and after the heartbeat timeout every data node refuses bearer tokens because its denylist has gone stale (ADR-0026, amended). The roadmap's "No single point of failure" criterion and epic E5 ask for three or five control nodes.

ADR-0017 prepared for this. Every change is a stamped `Command`; `fleetring_control::apply` is pure and reads time only from the stamp; the replay test shows that a log, or a snapshot plus the rest of the log, reproduces the state; and `ControlClient` already follows a `fleetring-leader` redirect. ADR-0017's "Adding Raft later" names three pieces of work (log storage over the command log, a state machine over `apply`, a network over gRPC) and leaves open everything around them:

- **Which Raft.** Writing one, or which library, and how its log maps onto the existing `control.db`.
- **Identity.** openraft numbers its nodes with an integer; FleetRing names nodes with strings.
- **Who serves what.** Whether followers answer reads, and how a read after a write sees the write.
- **Time.** Commands are stamped by the control node's clock. Under Raft the stamping node changes with the leader, and two machines' clocks differ.
- **Changing the group.** How a control node is added, removed, and made a voter, and by whom.
- **Compaction.** The log today keeps every command forever and one snapshot. Roadmap E2 asks for log compaction and bounded snapshot retention.
- **Upgrade.** A fleet running 0.3 has a format-1 `control.db` and one control node, and must reach three without losing its state.
- **Data nodes.** They know one control address and must keep working while a leader is elected.
- **Authorization.** Raft messages are node-to-node calls, and ADR-0031 requires every node call to have a binding.
- **Simulation.** Raft elections are randomized, and ADR-0020 requires every random choice to come from the seed.

## Decision

1. **openraft 0.9 replicates the control log.** The daemon depends on the newest openraft 0.9.x at implementation time, with its `serde` feature, and implements its storage-v2 traits: `RaftLogStorage` over the log in `control.db`, and `RaftStateMachine` over `apply`. The type configuration is:

   | openraft type | FleetRing type |
   |---|---|
   | `NodeId` | `u64`, the control node's Raft ID (point 2) |
   | `Node` | `ControlPeer { node_id: String, address: String }`: the fleet node ID and the address peers dial |
   | `D` (request) | `ControlRequest`, a `#[non_exhaustive]` enum with one variant, `Command(Command)` |
   | `R` (response) | `ControlResponse { outcome: Outcome, state: Option<Arc<State>> }`, with `state` skipped by serde: responses are produced by the local state machine and never cross the network |
   | `SnapshotData` | `Cursor<Vec<u8>>` |
   | `AsyncRuntime` | openraft's Tokio runtime in production; under the `simulation` feature, a runtime that delegates to Tokio but draws its randomness, the election timeouts, from a generator seeded from the run's seed (point 15) |

   `fleetring-control` doesn't change: `apply`, `State`, `Command`, and their serde forms stay as they are, and the replay tests stay as they are.

2. **A control node's Raft ID is the XXH3-64 hash of its fleet node ID's UTF-8 bytes.** No counter is stored and no operator picks a number. `control add` refuses a node whose ID hashes to 0 or to the Raft ID of another member of the group, naming both nodes; with at most seven members the chance of either is negligible, and the refusal makes it safe. A node removed and added again gets the same Raft ID, which Raft handles: its old vote and log are overruled by the current term and log.

3. **One code path: a single control node is a group of one voter.** The single writer of ADR-0017 point 3 goes away. `ControlHandle::submit(action)` on the leader stamps `Command::new(clock.now(), action)` immediately before `raft.client_write(ControlRequest::Command(command))`, waits until the command is committed and applied, and returns `Committed { index, command, outcome, state }` as today. On a node that isn't the leader it returns `SubmitError::NotLeader { leader }`, with the leader's address when the node knows it. openraft batches the entries waiting to be appended into one `append` call, which the log writes in one transaction with one sync, so the group commit of FR-028 survives.

   **The group's membership is Raft configuration, not control state.** It lives in openraft's membership entries and in snapshot metadata, and changes through openraft's `add_learner` and `change_membership`, not through `apply`. This is the one exception to "every control change goes through `apply`" (ADR-0017 point 1), and it is deliberate: Raft's own safety depends on configuration changes being handled by Raft (joint consensus, commit rules for configuration entries), and `State` has no use for the group, since placement, leases, and liveness never depend on which machines run the control plane. Every change to control *state* still goes through `apply`.

4. **`control.db` format 2 holds the Raft log, its metadata, and control snapshots.** The same file, in WAL mode with `synchronous=FULL`:

   - `meta(key TEXT PRIMARY KEY, value TEXT NOT NULL)`: `format` (`2`), `genesis` (the `ControlConfig` the first state was created with, as in format 1), `vote` (openraft's vote, JSON), `committed` (the last committed log ID, JSON), and `purged` (the last purged log ID, JSON).
   - `log(log_index INTEGER PRIMARY KEY, term INTEGER NOT NULL, leader_node INTEGER NOT NULL, entry TEXT NOT NULL)`: one row per Raft entry. `entry` is the JSON of the entry's payload: a normal entry's `ControlRequest`, a membership, or a blank entry.
   - `snapshots(log_index INTEGER PRIMARY KEY, term INTEGER NOT NULL, leader_node INTEGER NOT NULL, membership TEXT NOT NULL, state TEXT NOT NULL)`: control snapshots, newest last.

   Each `append` call is one transaction. `truncate` deletes entries from an index on (a follower's uncommitted conflicting tail), `purge` deletes entries up to an index and records `purged`, and `save_committed` records `committed`, so that a restarted node, and the replay check, know which entries are committed. The state machine lives in memory: at startup it loads the newest snapshot and applies the committed entries after it, as format 1's recovery does.

   *The control log* (lexicon) is now the committed normal entries in index order. Replaying them from genesis, or from any retained control snapshot, with `apply` reproduces the state; uncommitted entries and Raft's own entries (membership, blank) are not commands and are skipped.

5. **A format-1 log migrates on first start.** When the daemon opens a format-1 `control.db` it recovers the state with the format-1 code, writes a control snapshot at the last index N with term 0, leader node 0, and the membership {own Raft ID → own `ControlPeer`} as its only voter, sets `purged` to that log ID, renames `commands` to `commands_v1` (kept for inspection, never read again), and sets `format` to 2, all in one transaction. openraft then starts from the snapshot as a one-voter group and elects itself. An empty format-1 log becomes an empty format-2 store and bootstraps as a new group (point 10). 0.3 refuses a format-2 file (`UnsupportedFormat`, as today), so there is no downgrade: the upgrade notes say to back `control.db` up first.

6. **The leader answers every control RPC; followers redirect.** A `Control` RPC on a node that isn't the leader returns `not_leader(<leader address>)` (ADR-0017 point 4), or `UNAVAILABLE` with "no control leader yet" and no hint while an election is in progress. On the leader, reads (`Locate`, `ListDatabases`, and opening `WatchMembership`) first call `raft.ensure_linearizable()`, so a read sees every command acknowledged before it began, even right after a leader change. A `WatchMembership` stream ends with `UNAVAILABLE` and the leader hint when its node stops being leader; the subscriber reopens it, as it reopens any broken stream. The authorizer and the service's binding checks (ADR-0031 point 2) read the node's own applied state, as today.

7. **The leader's clock stamps commands, and a new leader waits before it fails anything over.** The node that stamps is whichever is leader when the command is proposed; the stamp travels in the entry, so every node applies the same time. `apply` already counts a stamp earlier than `State::now` as `State::now`, so a new leader whose clock is behind can't move time back. The failover timer (ticks, `StartPromotion`, `Promote` notices) runs only on the leader, and the settle window of ADR-0022 point 6, a lease duration plus the failover margin before any promotion, restarts each time a node becomes leader: leases that look expired only because nobody could renew them during an election get renewed first. A leader whose clock is ahead can start a failover early, and one whose clock is behind late; neither can make two primaries, because epochs fence and W + Q > R (ADR-0018 point 6). ADR-0017's consequence, "under Raft, the leader's clock takes that role", becomes this rule.

8. **Control snapshots and log compaction are openraft's, with bounds.** openraft builds a control snapshot every `[control] snapshot_every` committed entries (`SnapshotPolicy::LogsSinceLast`, default 1,000, the setting that exists today). It keeps `[control] log_keep` entries behind the newest snapshot (`max_in_snapshot_log_to_keep`, default 1,000) for followers that are behind, and purges older ones in batches of 256. At most `[control] snapshots_retained` control snapshots stay on disk (default 2); building a new one deletes the oldest beyond that. A snapshot's data is the JSON object `{"state": <State>}`; its membership is in openraft's snapshot metadata. A follower too far behind for the kept log gets the snapshot through `InstallSnapshot`, in 1 MiB chunks.

9. **The Raft network is a gRPC service with opaque JSON payloads.**

   ```proto
   service Raft {
     rpc AppendEntries(RaftPayload) returns (RaftPayload);
     rpc Vote(RaftPayload) returns (RaftPayload);
     rpc InstallSnapshot(RaftPayload) returns (RaftPayload);
   }
   message RaftPayload { bytes json = 1; }
   ```

   Each payload is openraft's request or response type as JSON. These types are openraft's, not FleetRing's, and change between openraft versions, so the protocol treats them as opaque bytes, as it treats batch payloads (ADR-0019 point 3); compatibility between control nodes is governed by the control-log format and the protocol's minor version, which this change bumps. The service runs on the node's existing listener, over the existing `Peers` and `Network`: mutual TLS in production, turmoil under simulation. A connection failure or timeout is openraft's `Unreachable`, any other transport failure its network error, so openraft backs off as designed.

10. **A new control node bootstraps or joins, as configured.** `[control] bootstrap = "new"` (the default) on a node with an empty store calls `raft.initialize` with itself as the only voter: the first control node of a new fleet needs no new setting. `bootstrap = "join"` on an empty store waits, as an uninitialized node, to be added. A node with any Raft state ignores the setting.

11. **Operators change the group with `fleetringd control`.** A new CLI noun, `control`, after the lexicon's control group:

    - `fleetringd control add <node-id> --address <host:port>`: the leader computes the Raft ID (point 2), adds the node as a learner with `add_learner`, and returns once it has caught up.
    - `fleetringd control set-voters --voter <node-id> --voter <node-id> …`: makes exactly these nodes the voters, through openraft's `change_membership`, which runs joint consensus. It refuses an even count, more than seven, and any node that isn't already a caught-up member of the group. Growing from one to three is two `add`s and one `set-voters`, so the group never passes through two voters.
    - `fleetringd control remove <node-id>`: removes a learner, or a voter after `set-voters` has demoted it; it asks for confirmation as `docs/cli.md` section 7 requires.
    - `fleetringd control list`: every member with its address, whether it votes, whether it leads, and its matched log index, plus the term and committed index.

    The four RPCs behind them, `AddControlNode`, `RemoveControlNode`, `SetControlVoters`, and `DescribeControlGroup`, need the `fleet` scope and refuse node certificates.

12. **Data nodes find the leader on their own.** `[control]` takes `address` (one) or `addresses` (a list), not both; a data-only node must set one. `ControlClient` keeps the configured addresses plus the control group it learns from every join and heartbeat reply (`control_nodes`, point 14). A redirect hint moves it to the leader as today; a connection failure or `UNAVAILABLE` without a hint moves it to the next known address. During an election, primaries keep serving (ADR-0018 point 6), heartbeats and lease renewals are retried, and no promotion starts before the new leader's settle window passes (point 7). Tokens go `UNAVAILABLE` on a data node only if the election outlasts the heartbeat timeout (ADR-0026's amendment), and the timings make that unlikely: `[control] election_timeout_ms` (default 1,000; openraft's range is that value to twice it) and `[control] raft_heartbeat_ms` (default 250), validated so that the election timeout is at least three Raft heartbeats and the lease duration at least twice the longest election timeout.

13. **Raft calls are bound to the control group.** The `Raft` RPCs are `node` calls (ADR-0026 point 2) with a new binding, `control peer`: the caller's certificate names a node that is a voter or learner of the receiver's current group. A refusal is `PERMISSION_DENIED` with `fleetring-binding: control peer` and the message form of ADR-0031 point 5 (`<Method> refused for node '<node>': not a member of the control group`), audited once. A receiver with no group yet, a fresh node with `bootstrap = "join"`, accepts any node certificate of its fleet until its first membership applies, because it can't know who will add it. That is a residual risk `SECURITY.md` states: a stolen node key can feed a fresh, not-yet-added control node a false log, or disrupt its addition, until the real leader reaches it; operators start a joining node right before `control add`, and `control list` shows its matched index before it votes. Certificates that mark a node as a control node would close it, and are left to a later decision.

14. **The protocol gains the group.** `ControlNode { node_id = 1; address = 2; voter = 3; leader = 4; matched_index = 5 }`; `JoinResponse.control_nodes = 4` and `HeartbeatResponse.control_nodes = 4`, each `repeated ControlNode`; the four group RPCs of point 11, each answering with `ControlGroup { nodes = 1; term = 2; committed_index = 3; leader = 4 }`; and the `Raft` service of point 9. `PROTOCOL_VERSION` gains a minor version. Every new RPC has a `Scope:` line, and the `Raft` RPCs a `Binding:` line, which `auth::RPCS` and its table tests hold equal.

15. **The simulation runs control groups, and checks them.** Under the `simulation` feature openraft's randomness comes from a generator the harness seeds from the run's seed. openraft asks its runtime for randomness without saying which node asks, so the generator belongs to the simulation thread rather than to a host; turmoil runs every host on that one thread in an order the seed fixes, so the draws, and the elections, replay exactly under `--cfg tokio_unstable` (ADR-0020 point 2). The harness runs groups of three and five control hosts, crashes, isolates, and restarts leaders, and skews each host's clock. Two invariants are added to those of ADR-0020 point 4: **control logs agree** (the committed command at each index is the same on every control node) and **one leader per term** (no two nodes lead the control group in the same term). "Replaying the control log reproduces the control state" and "no acknowledged control command is lost" run on every control node.

## Alternatives considered

- **Writing a Raft implementation for FleetRing.** The state machine is already pure, so the remaining piece looks small, but the hard parts of Raft are exactly what a project like this gets wrong first: joint consensus, log truncation on conflicting entries, snapshot installation under a moving leader, and leader stickiness. openraft has these, an asynchronous storage interface that matches the daemon's Tokio code, and a test suite of its own. Rejected.
- **raft-rs (TiKV).** Mature and proven in TiKV, but it is a synchronous core that leaves the driving loop, the transport, ready-state handling, and snapshot transfer to the application, and it builds on prost types of its own. It would be more glue to write and test than the storage and network traits openraft asks for, and nothing about raft-rs's model fits the daemon better. Rejected.
- **etcd, or Consul, as the control store.** ADR-0017 already rejected an external service: it is one more system to run, against the one-binary goal, and `apply`'s determinism would have to be rebuilt on transactions over keys.
- **Follower reads.** Every per-epoch fact in control state is immutable once given (ADR-0031 point 2), so a stale read would be safe for the binding checks, and it would take read load off the leader. But a client that creates a database on the leader and then locates it on a follower could be told it doesn't exist, and a drain that reads its leases from a stale follower would hand over fewer than it holds. Read-your-writes everywhere is worth one heartbeat round per read at the control plane's read rate. Deferred, possibly to E4.
- **Stamping commands on the node that receives them, or on each follower as it applies.** A follower's stamp would make the leader depend on every receiving node's clock, and stamping at apply time would make replay depend on the replaying machine's clock, which breaks ADR-0017 point 2. The leader stamps.
- **A Raft log in its own file beside `control.db`.** Two files would need a rule for which one is ahead after a crash. One SQLite file keeps the log, its metadata, and the snapshots consistent under one transaction, and keeps the log inspectable with the `sqlite3` shell.
- **Keeping the single writer beside Raft for one-node fleets.** It would keep today's path untouched for most users, but two paths would mean two sets of recovery code, two sets of tests, and a migration between them at exactly the moment an operator is trying to add redundancy. A one-voter group behaves like the single writer and is the same code as five.
- **Numbering control nodes with a stored counter, or by operator choice.** A counter needs a place in replicated state that isn't `State`, and an operator-chosen number is one more thing to get wrong. A hash of the node ID needs neither, and its rare collision is refused.
- **Making membership changes control commands that go through `apply`.** It would satisfy the rule literally, but Raft must apply configuration changes on append, not on commit, and only Raft's own entries give that.
- **Typed protobuf messages for Raft.** They would duplicate openraft's types field by field and break with every openraft change that adds one. The Raft messages are internal to the control group, and the control-log format already versions them.

## Consequences

- No single point of failure in the control plane with three or five control nodes: a leader's loss costs one election, about one to two seconds with the defaults, during which control RPCs are redirected or retried and nothing fails over.
- A single control node behaves as before, behind a Raft layer: the first start after the upgrade migrates `control.db` to format 2, which 0.3 can't read.
- Every control command, heartbeats included, is replicated to a quorum before it is acknowledged. Heartbeats are most of the control plane's traffic, so the command rate a group sustains is lower than a single node's; moving heartbeats off the log (roadmap E4) matters more once groups exist.
- The control log is compacted and its snapshots bounded, closing roadmap E2's compaction line. A replay from genesis is possible only while the log hasn't been purged; checks replay from the oldest retained snapshot instead.
- `SECURITY.md` gains the control group's threat: a stolen key of a control node controls the control plane, a stolen key of any other node can't speak Raft to a group member, and the fresh-joiner window of point 13.
- The simulation harness grows control groups, per-host clock skew, and two invariants; the fixed-seed job must stay within its budget, so control-group scenarios run on few seeds and the nightly sweep covers the rest.
- New configuration: `[control] addresses`, `bootstrap`, `election_timeout_ms`, `raft_heartbeat_ms`, `log_keep`, and `snapshots_retained`; a new CLI noun, `control`; new lexicon entries for the control group, voter, learner, and the Raft term.
- Follow-up work: control-node certificates (point 13), follower reads if the leader's read load matters, and heartbeats off the log (E4).
