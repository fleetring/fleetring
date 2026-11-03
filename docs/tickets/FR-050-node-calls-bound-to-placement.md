---
id: FR-050
title: Bind node-to-node calls to placement, so a stolen node key reaches only its own databases
status: done
priority: P1
size: M
milestone: M10 Security
depends_on: [FR-047]
adrs: [ADR-0018, ADR-0022, ADR-0025, ADR-0031]
---

# FR-050: Bind node-to-node calls to placement, so a stolen node key reaches only its own databases

## Context

FR-046 made node IDs unforgeable and FR-047 narrows node certificates to node calls, but a node call still accepts any node for any database: a stolen node key can stream forged batches to any replica at a higher epoch, fence any replica for a promotion that doesn't exist, and read any database through `Snapshot` or `Fetch`. Epochs probably contain the damage, but nothing verifies that the node presenting an epoch is the one control state gave it to. ADR-0031 binds every node call to placement, after Kubernetes' NodeRestriction: each node RPC names a binding (self, replica, holder, candidate, or follower); the control node checks it against its state before submitting, leaving `apply` unchanged; a data node checks a new epoch's holder or candidate against control state once, through `Locate`, and then trusts its durable epoch record; ring-based checks accept the current and the previous ring so membership changes never refuse a legitimate call; and refusals are `PERMISSION_DENIED` with the binding named.

## Scope

- **A binding per node RPC** in `fleetringd::auth::RPCS`, stated in the proto and the README, with a test that refuses a node RPC without one.
- **Control-side checks** for `StartPromotion`, `RecordFenced`, `CompletePromotion`, and `TransferLease` from nodes.
- **Data-side checks** for `Replicate`, `Fence`, `Fetch`, `Snapshot`, `Promote`, and FR-045's `Repair` once it exists.
- **`Placement.candidate`**, so that `Locate` names a promotion's candidate.
- **Not in scope:** checks on what a node reports about itself in heartbeats (inert outside a replica set, ADR-0031 point 6), signed lease certificates, and live migration itself (E6), whose target set plugs into `replicas_of`.

## Acceptance criteria

- [x] ADR-0031 is accepted and linked from `docs/adr/README.md`.
- [x] Every `Requirement::Node` row of `auth::RPCS` names a binding; the table test fails for a node RPC without one, and the proto's `// Binding:` lines and the README's binding column match the table.
- [x] A node outside a database's replica set is refused `Replicate`, `Fence`, `Fetch`, `Snapshot`, and `Promote` for it with `PERMISSION_DENIED` and `fleetring-binding` metadata, and the target replica's epoch record is unchanged afterwards.
- [x] A node that isn't the promotion's candidate is refused `RecordFenced` and `CompletePromotion`; one that doesn't hold a lease is refused `TransferLease` for it; one outside the replica set is refused `StartPromotion`.
- [x] A follower that restarts while the control plane is down still accepts its recorded primary; a holder it hasn't recorded gets `UNAVAILABLE` until the control plane is back.
- [x] Simulation: an impostor holding a node certificate is refused on a database it doesn't host and causes no epoch change, no lost write, and no refused write; and a membership change that moves a replica while writes flow completes with no binding refusal.
- [x] Every existing simulation scenario, the drain scenario included, passes with no binding refusal.

## Implementation plan

Start once FR-047 has merged and ADR-0031 is accepted. Each step is about one commit (`feat(fleetringd): …`, `Refs: FR-050`). Paths are under `crates/fleetringd/` unless they say otherwise. New types derive `Debug` and the applicable traits and join `auth`'s or `replication`'s `assert_debug_send_sync` list; errors are `#[non_exhaustive]` with lowercase messages.

### Step 1: bindings in the requirement table

- New `src/auth/binding.rs`, re-exported from `src/auth/mod.rs`:

  ```rust
  /// What a node must be, for the database a node call names, to make it (ADR-0031).
  #[derive(Debug, Clone, Copy, PartialEq, Eq)]
  #[non_exhaustive]
  pub enum Binding {
      /// Not a node call.
      None,
      /// The request names the calling node itself.
      SelfNode,
      /// The caller is in the database's replica set.
      Replica,
      /// The caller holds the database's lease at the request's (or the current) epoch.
      Holder,
      /// The caller is the candidate of the database's promotion at the request's epoch.
      Candidate,
      /// The caller holds or is promoting to the stream's epoch.
      HolderOrCandidate,
      /// The caller is a follower this primary streams the database to.
      Follower,
  }
  // Display: "none", "self", "replica", "holder", "candidate", "holder or candidate", "follower".

  /// Why a node call was refused.
  #[derive(Debug, Clone, PartialEq, Eq)]
  #[non_exhaustive]
  pub enum Unbound {
      OtherNode { named: NodeId },
      NotReplica { database: DatabaseName },
      NotHolder { database: DatabaseName, epoch: LeaseEpoch },
      NotCandidate { database: DatabaseName, epoch: LeaseEpoch },
      NotFollower { database: DatabaseName },
  }

  /// A refused node call.
  #[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
  #[error("{method} refused for node '{node}': {reason}")]  // reason Display per ADR-0031 point 5
  pub struct BindingDenied { pub method: &'static str, pub node: NodeId, pub reason: Unbound }

  pub const BINDING_KEY: &str = "fleetring-binding";

  impl BindingDenied {
      /// `PERMISSION_DENIED` with the message and `fleetring-binding` metadata; logs the refusal
      /// once on `fleetringd::audit` with node, method, database, epoch, and binding.
      pub fn into_status(self) -> Status;
  }
  ```

- `src/auth/policy.rs`: `Rpc` gains `pub binding: Binding`; `const fn rpc(path, requirement)` becomes `const fn rpc(path, requirement, binding)`; every row gets the binding of ADR-0031 point 1 (`Binding::None` for non-node rows; `Join`, `Heartbeat`, `Leave`, `Drain`, `AcquireLease`, `RenewLease` `SelfNode`; `StartPromotion` `Replica`; `RecordFenced`, `CompletePromotion`, `Fence`, `Fetch`, `Promote` `Candidate`; `TransferLease` `Holder`; `Replicate` `HolderOrCandidate`; `Snapshot` (and `Repair`) `Follower`).
- `crates/fleetring-proto/proto/fleetring/v1/fleetring.proto`: a `// Binding: <binding>.` line under the `// Scope:` line of every node RPC.
- `crates/fleetringd/README.md`: the scope table gains a third column, "Binding", filled for node rows and `—` otherwise.
- Tests in `policy.rs`: `every_node_rpc_names_a_binding` (every `Requirement::Node` row has a binding other than `None`, and no other row has one), `the_proto_comments_state_each_node_rpcs_binding` (as `the_proto_comments_state_each_rpcs_requirement`), `the_daemon_readme_lists_each_rpcs_binding` (rows `| \`S.M\` | <requirement> | <binding> |`). Unit tests in `binding.rs`: `refusal_messages_name_the_binding` (each `Unbound` variant's exact text), `the_status_is_permission_denied_with_metadata`.

### Step 2: control-side checks

- `src/control/service.rs`, before `submit`, using `self.node.state()` and the caller from `auth::authorized(&request)?` (only `Caller::Node(id)` is checked; principals pass through the scope check FR-047 already makes, and `TransferLease` by a principal needs `admin`):
  - `start_promotion`: `id` in `state.replica_set(&database)?`, else `Unbound::NotReplica`.
  - `record_fenced` and `complete_promotion`: `state.database(&database)` has a promotion at `epoch` whose `candidate()` is `id`, else `Unbound::NotCandidate { epoch }` (a missing database or promotion keeps its existing rejection, which `apply` produces).
  - `transfer_lease`: `state.database(&database)?.lease()` is held by `id`, else `Unbound::NotHolder { epoch: database's epoch }`.
  - `acquire_lease` and `renew_lease` stay as they are: FR-046's `check_node` plus `apply`'s rules. Their doc comments say so, citing ADR-0031 point 2.
- `src/control/service.rs`, `placement()`: fill `Placement.candidate` (below) with `db.promotion().filter(|p| p.epoch() == db.epoch()).map(|p| p.candidate())`.
- `crates/fleetring-proto/proto/fleetring/v1/fleetring.proto`, `Placement`: `// The node carrying out the promotion to `epoch`, while one is in progress (FR-050). Unset otherwise, and from a control node that predates the field.` `optional string candidate = 10;`. `src/data/convert.rs` and `data::Location` carry it.
- Tests in `tests/control.rs`, each with three member nodes using their own node certificates (the `tests/support` authority) and a database at R = 2 so one node is outside its replica set:
  - `start_promotion_from_outside_the_replica_set_is_refused`
  - `record_fenced_from_a_node_other_than_the_candidate_is_refused`
  - `complete_promotion_from_a_node_other_than_the_candidate_is_refused`
  - `transfer_lease_from_a_node_that_does_not_hold_the_lease_is_refused` (and the holder's own transfer succeeds)
  - `locate_names_the_candidate_while_a_promotion_is_in_progress`
  Each refusal asserts `PERMISSION_DENIED`, the `fleetring-binding` value, and that the control log's length didn't change.

### Step 3: the epoch authority on data nodes

- New `src/replication/binding.rs`:

  ```rust
  /// Who may lead a database now, as control state says (ADR-0031 point 3).
  #[derive(Debug, Clone, PartialEq, Eq)]
  pub struct EpochHolders { pub epoch: LeaseEpoch, pub holder: Option<NodeId>, pub candidate: Option<NodeId> }

  /// Where a data node asks who leads an epoch.
  pub trait EpochAuthority: fmt::Debug + Send + Sync + 'static {
      /// # Errors
      /// `UNAVAILABLE` when control state can't be reached, `NOT_FOUND` for an unknown database.
      fn holders(&self, database: &DatabaseName) -> Pin<Box<dyn Future<Output = Result<EpochHolders, Status>> + Send + '_>>;
  }
  impl EpochAuthority for ControlClient { … }               // Locate: epoch, primary, candidate
  impl EpochAuthority for BTreeMap<DatabaseName, EpochHolders> { … }  // for tests
  ```

  and `pub(super) fn replicas_of(view: &MembershipView, previous: Option<&Ring>, database: &DatabaseName, factor: ReplicationFactor) -> BTreeSet<NodeId>`: the union of the replica sets under the current and the previous ring (ADR-0031 point 4); the one function live migration will extend.
- `src/membership.rs`, `MembershipView`: keep `previous_ring: Option<Arc<Ring>>`, set to the old ring whenever a view with a new membership epoch is published; `pub fn previous_ring(&self) -> Option<&Arc<Ring>>`.
- `src/replication/mod.rs`: `ReplicationParts` gains `pub authority: Arc<dyn EpochAuthority>`; `src/daemon.rs` passes the node's `ControlClient`; test harnesses (`tests/replication/common.rs`, `tests/changes/common.rs`, `data::leases` tests) pass a map they update as they serve primaries. `FollowerStatus` gains `binding_refusals: u64`, and `Replication::binding_refusals(&self) -> u64` sums every refusal this node made (for the simulation).

### Step 4: data-side checks

- `src/replication/follower.rs`, `ReplicationService`:
  - `replicate`: after reading `ReplicateOpen` and the primary from the certificate (FR-046 already ties the metadata to it): refuse with `NotReplica` unless this node is in `replicas_of` for the database; then, if the epoch record's holder at this epoch is the caller, go on; if the record names another holder at this epoch, refuse as today; otherwise call `authority.holders(database)` and accept only if `epoch == holders.epoch` and the caller is `holder` or `candidate` (else `NotHolder { epoch }`), and only then `admit_from` (which records it). An authority error is returned as is (`UNAVAILABLE`).
  - `fence`: refuse with `OtherNode` unless `request.candidate` is the caller; then require `holders.epoch == epoch && holders.candidate == Some(caller)` (else `NotCandidate`), before the fence records anything.
  - `fetch`: require that the epoch record is at `epoch` with the caller as its holder (the candidate the fence named), else `NotCandidate`. No authority call.
  - `promote`: require `holders.epoch == epoch && holders.candidate == Some(self)`, else `NotCandidate` naming this node; the caller is not checked further (a duplicate notice for a real promotion is harmless).
  - `snapshot` (and `repair` once FR-045 lands): on the primary, require that the database is served at `request.epoch` and the caller is in `Inner::followers_of(&primary)`, the set this primary streams to, extended with the previous ring's set for the same factor; else `NotFollower`.
  - Every refusal goes through `BindingDenied::into_status` and increments `binding_refusals`.
- Tests in a new `tests/replication/binding.rs` (`mod binding;` in `tests/replication/main.rs`), three nodes `a`, `b`, `c` and a fourth `d` outside the database's replica set, all with node certificates:
  - `a_stream_from_a_node_that_does_not_hold_the_epoch_is_refused`: `d` opens `Replicate` to `b` at the current epoch plus one; refused `holder or candidate`; `b`'s epoch record is unchanged; `a`'s stream keeps acknowledging.
  - `a_stream_to_a_node_outside_the_replica_set_is_refused`: the holder streams to `d`: refused `replica`.
  - `the_holder_is_verified_once_and_then_trusted_from_the_record`: the authority map counts calls; a stream reopened at the same epoch makes no second call.
  - `a_fence_naming_another_candidate_than_the_caller_is_refused` and `a_fence_for_a_promotion_control_does_not_know_is_refused`.
  - `fetch_from_a_node_other_than_the_fenced_candidate_is_refused`.
  - `a_snapshot_for_a_node_that_is_not_a_follower_is_refused`.
  - `promote_for_a_promotion_naming_another_candidate_is_refused`.
  - `a_replica_moved_by_a_membership_change_is_accepted_during_the_overlap`: publish a view where `d` replaces `c` in the replica set on `a` before `d` publishes it; `d`'s snapshot request and its stream are accepted.
  - `a_known_holder_is_accepted_without_the_control_plane_and_an_unknown_one_waits`: the authority returns `UNAVAILABLE`; the recorded holder's stream is accepted, a new epoch's holder gets `UNAVAILABLE`, then succeeds once the authority answers.

### Step 5: simulation

- `src/sim/cluster.rs`: `ClusterBuilder::impostor(self, name: &str, as_node: &str) -> Self` adds a client host whose certificate is a node certificate for `as_node` (issued by the cluster's authority), and `Cluster::binding_refusals(&self, host: &str) -> Option<u64>`.
- `src/sim/workload.rs`: `Operation` gains `ImpostorReplicate { database, target: NodeId, epoch: LeaseEpoch }`, `ImpostorFence { database, target: NodeId, epoch: LeaseEpoch }`, `ImpostorSnapshot { database, target: NodeId }`, executed by `src/sim/client.rs` with raw `Replication` clients over the impostor's certificate; `Ack::Refused { code: tonic::Code, binding: Option<String> }` records the outcome; a `Script` drives them.
- `src/sim/check.rs`: `Invariant::NodeCallsBound` ("node calls bound to placement") and `pub fn impostor_refused(history: &History) -> Result<(), Violation>`: every impostor operation ended `PERMISSION_DENIED` with a binding, or `UNAVAILABLE`, and none was acknowledged.
- New `tests/sim/binding.rs` (`mod binding;` in `tests/sim/main.rs`):
  - `an_impostor_node_is_refused_on_databases_it_does_not_host` (`seeds::FIXED`): a control host and four data hosts; `CreateAndWrite::new(1, 3, 300)`; once writes flow, an impostor using `node-4`'s certificate (chosen as a node outside `db-0`'s replica set, read from control state) replicates, fences, and snapshots against each replica of `db-0`, at the current epoch and at the current epoch plus one, repeated every 200 ms for 3 s. Asserts: `impostor_refused` holds; `db-0`'s epoch in control state didn't change during the attack; no write was refused or failed during it; every replica's binding refusals are positive and the legitimate nodes' streams never ended with a binding refusal; `Cluster::finish` passes.
  - `a_membership_change_that_moves_a_replica_completes_without_refusals` (`seeds::FIXED`): three data hosts and a fourth that joins mid-run with weight 4, so that the ring moves at least one replica of the database under test (the test picks, from `db-0` to `db-9`, a database whose replica set the join changes, computed with `membership_ring` before and after); writes flow throughout; the new member's copy converges (`await_replicas_converged`), the history check passes, and `binding_refusals` is 0 on every host.
- `tests/sim/drain.rs` and every other scenario: add, at the end, an assertion that `binding_refusals` is 0 on every host (a helper `Cluster::assert_no_binding_refusals`).

### Step 6: documentation

- `crates/fleetringd/README.md`: the binding column, and a paragraph on what a stolen node key can and can't do (ADR-0031 point 6).
- `SECURITY.md`: the node-key row of the threat model (with FR-048) states the blast radius.
- ADR-0031's status to Accepted with the date, its index row, and this ticket's Progress.

## Open risks

- **One `Locate` per new epoch per replica.** At a failover of many databases at once, the new primaries' followers all ask the control node; the reads are cheap and cached by the epoch record, but a burst is possible. A later change can batch them, or move to signed lease certificates (ADR-0031, alternatives).
- **Two rings of overlap may not cover a slow node** that misses several membership epochs in a row. It then refuses a legitimate stream until it catches up with the membership, and the primary retries; no safety is lost.
- **FR-047 is still in review.** Steps 1 and 2 use its `RPCS`, `Caller`, and `authorized`; follow their final names.

## Progress

- 2026-11-02: ADR-0031 proposed; ticket and implementation plan written.
- 2026-11-03: Implemented on branch `placement-binding`; awaiting review and CI, so the status stays in-progress until CI is green.
  - **Step 1, bindings (`auth::binding`, `auth::policy`):** `Binding`, `Unbound`, `BindingDenied` (`PERMISSION_DENIED`, the ADR's messages, `fleetring-binding` metadata, one audit line), and `refused_binding` to read the metadata back. Every `RPCS` row has a binding; the proto has a `// Binding:` line under each node RPC's `// Scope:` line and the daemon README a binding column, and `every_node_rpc_names_a_binding`, `the_proto_comments_state_each_node_rpcs_binding`, and `the_daemon_readme_lists_each_rpcs_binding` hold the three together. Deviation: `Leave`, `Drain`, and `TransferLease` are client calls a daemon makes as itself (ADR-0026), not `Requirement::Node` rows, but carry `self`, `self`, and `holder`; the test requires a binding on every `node` row, allows one only on rows nodes may make, and names `WatchMembership` and `Locate` as the only node-callable rows with nothing to bind. FR-046's `self` refusals (`Peer::check_node`) keep their messages and gain the metadata.
  - **Step 2, control side (`control::service`):** `StartPromotion` (replica), `RecordFenced` and `CompletePromotion` (candidate), and `TransferLease` from a node (holder) are checked against the published state before anything is submitted; `apply` and the log are unchanged. `Placement.candidate` (field 10) and `data::Location::candidate` name a promotion in progress. Deviations, recorded in ADR-0031's amendment: a candidate check refuses an epoch past the database's (a promotion to it could start before the command applies) and leaves an over or unknown promotion to `apply`; `TransferLease` accepts the holder of the lease at any epoch, so a draining node can hand over again after a stalled promotion. Tests in `tests/control.rs` (`Bound` fixture: three node certificates, R = 2, the failover timer never ticking so the log grows only by the test's calls): the five named tests, each refusal asserting the code, the binding, and the log's last index.
  - **Step 3, membership (`membership`):** `MembershipView::previous_ring`, kept for one membership epoch, and `Membership::manual`/`ManualMembership` to publish views by hand in tests.
  - **Steps 3 and 4, data side (`replication::binding`):** one commit, since the authority has no user without the checks. `EpochAuthority` (implemented for `ControlClient` through `Locate`, and for a `BTreeMap` in tests), `EpochHolders` (epoch, holder, candidate, and also control state's replica set and replication factor, which the follower doesn't otherwise know), `replicas_of`, `ReplicationParts::authority`, `Replication::binding_refusals`, and `FollowerStatus::binding_refusals`. The sender is the node its certificate names; the `fleetring-primary` metadata only has to agree. `Replicate`, `Fence`, `Fetch`, `Snapshot`, and `Promote` are checked as the plan says, with these deviations (ADR-0031 amendment): a stream's receiver is also a replica if control state's replica set names it; a snapshot isn't served on the strength of the previous ring, which lingers until the next membership change (the simulation caught a non-replica reading `db-0` that way), but goes to a follower on the primary's current ring or in control state's replica set; an epoch control state has moved past is `FAILED_PRECONDITION`, not a binding refusal; `Fence` and `Promote` accept the epoch's holder as well as its candidate, since a fence or notice can arrive after the promotion completed; a fence without a candidate is refused. `insecure_plaintext` peers carry no identity and get no binding checks. Tests in `tests/replication/binding.rs` (four nodes over TLS with their own certificates, views and control state set by hand): the ten named tests, which include a non-replica fencing the primary at a higher epoch (`a_fence_for_a_promotion_control_does_not_know_is_refused`), a non-replica pulling a snapshot from the primary and from a follower (`a_snapshot_for_a_node_that_is_not_a_follower_is_refused`), and a follower restarted while the control plane is down (`a_known_holder_is_accepted_without_the_control_plane_and_an_unknown_one_waits`). `tests/tls.rs`'s node fence of an unknown database now gets `NOT_FOUND` from the binding check.
  - **Step 5, simulation:** `ClusterBuilder::impostor`, `Operation::ImpostorReplicate`/`ImpostorFence`/`ImpostorSnapshot` (the snapshot and the fence carry an epoch and a candidate, which the requests need), `Ack::Refused` and `Ack::Served` (an impostor's answer is recorded as acknowledged so that a `Script` doesn't retry it; `Served` is the violation), `check::impostor_refused`, `Invariant::NodeCallsBound`, `Cluster::binding_refusals`, and `Cluster::assert_no_binding_refusals`, which `Cluster::finish` now runs for every cluster without impostors instead of each scenario calling it. `tests/sim/binding.rs` has both named scenarios; the impostor's attack is a small workload in the test that repeats its calls every 200 ms for 3 s, and it also checks that each node's refusal count equals the impostor's refused calls to it, so no legitimate call was refused. Both run on `seeds::FIXED_HEAVY` rather than `FIXED` to keep CI time down. `Cluster::await_replicas_converged` compares every data host, which holds only while every data host is a replica, so the two scenarios compare the database's replica set instead; a replica a membership change moves away keeps its stale copy, which is existing behavior outside this ticket.
  - **Step 6, documentation:** the daemon README's binding column and paragraph, the one `SECURITY.md` sentence that said FR-050 was pending, ADR-0031's amendment, and this ticket. FR-048's threat model states the blast radius in full.
  - **Gates and timing:** fmt, clippy, the workspace tests, rustdoc, and both MSRV checks pass locally, and the simulation passes in every local run, including one built with `--cfg tokio_unstable` as CI's simulation job builds it (29 tests, exact replay). Measured on a shared machine where wall time swung by 4x with other load, the simulation's CPU time went from 133 s (27 tests, `main`) to 162 s (29 tests): 145 s for the existing scenarios, about 9 % more, for the `Locate` each replica makes when it first sees an epoch, and 18 s for the two new ones. On the quietest runs, wall time went from 151–155 s to 193–201 s.
- 2026-11-03: Merged in PR #44; status is done.
