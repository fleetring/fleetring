# ADR-0046: A fork names its source's incarnation and its own, stays `seeding` until a majority of its replicas, and at least a write quorum, holds the staged copy, keeps those holders as its replica set until it is `ready`, becomes `ready` only after a replicated reset, and is driven by operations with IDs, budgets, and bounds

- **Status:** Accepted on 2026-11-10, with point 7's defaults as FR-087 measured them (`docs/perf.md`, "Forks"). Recovery rows and mutations the matrix doesn't run yet are listed in FR-107 (#168). Fixing #157, #163, #164 and #165 blocks the 0.8.0 release. An amendment to points 4.1, 4.4, 4.6 and 6.2, accepted on 2026-11-12 (FR-104, FR-105, FR-106), answers a seeding request made at another moment rather than refusing it, serves a first lease only once control state confirms it, and judges the readiness gate and drop notices by incarnation ([below](#amendment-2026-11-12-fr-104-fr-105-fr-106-late-seeding-requests-the-first-lease-and-incarnation-fences)).
- **Amended by:** [ADR-0031, amendment 2](0031-node-calls-bound-to-placement.md#amendment-2-2026-11-11-fr-102-a-membership-change-hands-a-replica-set-over-on-joint-quorums) (accepted 2026-11-12), on point 3.10 as built: a ready fork's primary counts its holders beside the replica sets control state showed it, not its own ring's placement, follows them before the fork's view, and names their membership epoch in `PlacementCaughtUp`; the same hand-over then serves every database.
- **Date:** 2026-11-08 (revised the same day after two design reviews, with the owner's decisions on the replica set before readiness and on random incarnations)
- **Related:** ADR-0016 (engine adapters), ADR-0017 (control state), ADR-0018 (leases, epochs, quorums), ADR-0020 (simulation), ADR-0021 (engines), ADR-0022 (failover), ADR-0023 (durability), ADR-0024 (logical change capture), ADR-0028 (replicated sink positions), ADR-0029 (archives), ADR-0031 (node calls bound to placement; the `seeding` binding is proposed here), ADR-0035 (Raft control group), ADR-0038 (fleet features), ADR-0040 (screening on the leader), ADR-0041 (metadata; `forked_from` is proposed here), ADR-0042 (no deletion by absence; staged copies follow it), ADR-0044 (umbrella accepted; its §7, specification S2, is superseded for forks by this record); tickets FR-083 to FR-087 (#133 to #137), FR-088 (#138, the `fork` action), FR-097 (#147, the usability trial), FR-074 (#99), FR-077 (#105)

This record replaces ADR-0044's specification S2 (§7) with a fork design that survives node loss, source failover, target replacement, lost and delayed messages, and control-plane restarts. ADR-0044's accepted umbrella still holds:

- `fork` is its own action, implies `read`, and is implied by `admin` (U2, §2.1);
- a fork is a new, independent database that records where it came from;
- FleetRing never merges databases.

## Context

ADR-0044 §7 described a fork in a few lines: a control command creates the target, the target is seeded, and then it opens for writes.

**A first design review found five gaps:**

1. **After completion, the only seeded copy can be lost.** If completion is recorded when one replica holds the copy, and that replica dies before its first lease or before replicating, the fork's position is recorded but no copy holds it. No timeout makes an empty replacement safe.
2. **A name doesn't identify a source.** `apply` removes a dropped database from `State::databases` (`drop_database` calls `remove_database`, which also drops every member's report of it). A new database under the same name starts again at `LeaseEpoch` 0. An outstanding fork that names its source by name alone could copy the recreated database.
3. **Retries were unspecified.** A lost reply to a fork request was ambiguous, and so was a retry after a crash.
4. **Nothing bounded the source's cost.** A fork's database snapshot pins the source's files, so checkpoints wait (`EngineError::CheckpointBlocked`) and the WAL grows. Nothing bounded that pin, the target's disk, or the number of attempts.
5. **Existing databases had no fork state to read.** A new field must read as "an ordinary, fully seeded database" when it is absent.

**A second review found two more:**

6. **Only the source was fenced.** A dropped fork's delayed commands, staged-copy reports, `Snapshot` requests, and staging directories could match a replacement fork with the same name and attempt number.
7. **"Seeded" was treated as "ready".** Completion comes before the reset that clears the change record and sink positions the fork inherited. A crash on either side of that reset must neither skip it nor repeat it after legitimate history exists, and clients must not reach the fork until the reset has committed.

**Two reviews of the first version of this record found more,** each resolved below and mapped to its point in Consequences:

- a holder whose staged copy was lost or damaged could take the first lease and open an empty database;
- deleting a staged copy because a placement shows another incarnation is deletion on a view, which ADR-0042 forbids;
- a staged copy had no log epoch, so promotion couldn't rank it;
- the SQLite adapter checkpoints at its tip, so the quorum-acknowledged position is routinely behind the last checkpoint, where `pin_snapshot_at` refuses it;
- a counter of incarnations in control state repeats values when `control.db` is replaced or rolled back;
- under `acks = local`, one holder completed a fork and one acknowledgement committed its reset, so nothing guaranteed a later primary would see either;
- placement could move a fork between completion and readiness, away from the nodes that hold its copy;
- cancellation, resuming, seeder exclusions, disk refusals, and several smaller points were ambiguous.

What the code has today (checked against `main` on 2026-11-08):

- **`fleetring_control::Database`** holds the name, replication factor, engine, durability and pending durability, lease epoch, lease, promotion, and metadata. It has no incarnation. It is stored as a `DatabaseRecord` whose later fields (`acks`, `min_replicas`, `pending_durability`, `metadata`) decode as optional, and `metadata` is skipped when absent, so a record without it is written byte for byte as before.
- **`MemberEntry::reports`** holds each member's latest `Report` per database: its replica role, position, arrival time, and log epoch.
- **`Replication.Snapshot(SnapshotRequest { database, epoch })`** streams a primary's database snapshot as `SnapshotChunk`s, with the position, the checksum, and the chain hash on the last chunk, under the `follower` binding (ADR-0031).
- **`FrameSource::pin_snapshot_at(Option<&Position>)`**, reached through `DatabaseHandle::pin_snapshot_at`, pins a consistent copy that holds the log up to a commit boundary of the current log and nothing after it, and answers `EngineError::InvalidPosition` for a position before the current log's start. Archives use it at positions their change stream reached, which only acknowledged batches reach (ADR-0029).
- **Checkpoints.** The SQLite adapter checkpoints at its tip, and `Primary::checkpoint_due` waits for lagging sinks but not for acknowledgements. `Primary::committed` is the published tip, not the position a write quorum acknowledged.
- **`Database.Restore`** fills a database that the control plane has already created with `CreateDatabase` and never leased: one replica restores it, takes the first lease, and serves it, and followers seed from it. `Database.Create` takes the first lease of a created database and creates its files.

## Decision

### 1. A database has an incarnation

1. **`Database::incarnation: Incarnation(u64)`** names one life of a database name. It is **64 random bits**, never 0, drawn by the control node that proposes the command from its injected generator (ADR-0020), and carried in the command, as time is. `apply` stays pure: it stores what the command carries, and refuses 0 where it would store it. It also refuses an incarnation that a database in control state, or an operation record still retained, already uses (`Rejection::IncarnationUnavailable`); the proposer then draws again and resubmits. That check guards against a collision; nothing else relies on it.
   - The commands that bring a database name into existence carry one: `CreateDatabase` (which a restore also issues, before `Database.Restore` fills the copy) and `ForkDatabase`.
   - **No rule rests on a counter, or on a value never repeating.** Fencing compares the whole fork identity (point 2), whose operation ID is another 128 random bits; two lives of one name share an incarnation with probability 2⁻⁶⁴. A replaced, recreated, or rolled-back `control.db` therefore can't hand out a value it handed out before in any way a rule depends on.
2. **Absent means 0, the legacy incarnation.**
   - Records written before this ADR decode with incarnation 0.
   - `apply` stores the carried incarnation only once `fork` is enabled (point 8), and 0 before. The proposer always draws one, so a command screened before `fork` was enabled and applied after it still stores a random value. Before enabling, the stored state is byte for byte what it is today, which keeps the change invisible to a fleet that can still roll back.
   - It is safe: no fork can exist before the feature is enabled, so no fork can confuse two legacy lives of one name. After enabling, every new database has a random incarnation, so `(name, 0)` names at most one database for the rest of the fleet's life.
   - **Every fork has a non-zero incarnation**, because `ForkDatabase` itself enables the feature.
3. **Placements carry the incarnation.** `Placement` gains `incarnation` (0 when absent). Every node's view of a database is `(name, incarnation)`. A data node records it beside its copy's highest epoch, and a staged copy records it in its sidecar (point 4.3).
4. **Text prints an incarnation as 16 lowercase hex digits**, and JSON as that string, since a 64-bit integer doesn't survive every JSON reader.

### 2. A fork's record, and what "seeded" and "ready" mean

A database gains an optional `fork: Option<Fork>`. **A database without the field is an ordinary database, fully seeded and ready.** That covers every database that exists today, and every database not made by a fork.

```text
Fork {
  operation:          OperationId,           // 128 bits, chosen by the client (point 5)
  source:             DatabaseName,
  source_incarnation: Incarnation,           // resolved at first apply
  requested_by:       Option<PrincipalName>, // grants nothing, like created_by
  requested_at:       Timestamp,
  source_bytes:       u64,                   // the source's reported size at the request, or 0
  limits:             ForkLimits,            // the limits the fork was made under (point 3.1)
  state:              ForkState,
}
// The fork's own incarnation is Database::incarnation. Every message about this fork carries
// the triple (name, incarnation, operation): its *fork identity*. The caller's whole request,
// ForkIntent, lives in the operation record (point 5.3), not here.

ForkLimits { max_attempts, deadline, attempt_timeout, seeder_timeout, operation_retention }

ForkState =
  | Seeding { attempt: u32, seeder: NodeId,
              seeder_excluded: BTreeSet<NodeId>, disk_refused: BTreeSet<NodeId>,
              attempt_started_at, deadline, last_cause: Option<RetryCause> }
  | Seeded  { attempt, at: Position, chain_hash: ChainHash, holders: BTreeSet<NodeId>,
              lease_from: LeaseEpoch, completed_at }
  | Ready   { at, chain_hash, completed_at, reset_epoch: LeaseEpoch, reset_position: Position, ready_at }
  | Failed  { reason: ForkFailure, attempts: u32, failed_at }
```

- **`Seeded` means "a completion quorum durably holds the copy"** (point 3.2). The recorded `holders` are the fork's replica set until it is `Ready` (point 3.10). A holder may take the first lease. The fork is **not** ready: no client data call, no `db watch`, and no sink runs against it.
- **`Ready` means "the fork-reset write is committed".** It mirrors the in-database marker of point 6, for status and for clients deciding when to call. The database's own marker is authoritative: the control state never decides whether the reset runs. From `Ready` on, the fork is an ordinary database, and its replica set follows placement.
- **`Seeded` and `Ready` keep the provenance** that `forked_from` shows: the source, its incarnation, the position, the chain hash, and the time. It stays for the database's life, and ADR-0041's metadata archive carries it (point 10).
- **`Failed` is permanent until `db drop`.** A failed fork is never served and never leased, and it holds no copy anyone may open.
- **`ForkFailure`** is `#[non_exhaustive]`: `SourceGone`, `SourceRecreated`, `RetriesExhausted { last: RetryCause }`, `DeadlineExceeded { last: RetryCause }`, `SourceTooLarge`, `InsufficientDisk`, `EngineUnsupported`. `RetryCause` keeps the real cause of the last retry: `SeederDown`, `AttemptTimedOut`, `TransferFailed`, `PinExpired`, `HolderLost`, `HoldersLeft`, or `SeederDiskRefused`. `Seeding::last_cause` holds it between attempts, so a failure at the deadline keeps it too (`AttemptTimedOut` if the first attempt was still running). A fork never fails as `InsufficientDisk` because its seeders timed out.
- **Terminal outcomes are exactly `Ready`, `Failed`, and `Cancelled { cause }`,** where `cause` is `Dropped` or `Expired`. `Seeding` and `Seeded` are pending.

### 3. States and transitions

```text
              ForkDatabase (op, incarnation, source@inc_s) creates target@inc_t
                           │
                           ▼
       ┌──────────► Seeding{attempt n, seeder} ──── DropDatabase / ExpireDatabase ──► gone (Cancelled)
       │                   │      │      │
RetryForkSeed              │      │      └─ FailFork / deadline / budget / disk ──► Failed ── DropDatabase ──► gone
(n+1, next seeder)         │      │
       └───────────────────┘      │ CompleteFork(identity, n, at, hash, holders ≥ C)
       ▲                          ▼
       │           Seeded{n, at, hash, holders}: holders are the replica set; no lease yet, not ready
       │                          │       └── DropDatabase / ExpireDatabase ──► gone (Cancelled)
DropForkHolder                    │ AcquireLease by a holder whose report still shows the matching staged copy
(fewer than C holders left)       ▼
       └────────── leased at epoch e, opening: the primary opens only its verified copy, runs the
                   fork-reset write unless the copy holds the marker; clients get UNAVAILABLE
                                  │ reset marker committed at the bootstrap quorum; primary reports it
                                  ▼ MarkForkReady(identity, e, reset_position)
                   Ready: clients, db watch, and sinks served; placement followed again
                                  │
                                  ▼
              an ordinary database; ADR-0018 and ADR-0022 from here on
```

Every transition is a control command applied by the pure `apply`. Time and random values arrive in the command, so the replay test covers every path.

**Two quorums, for the fork's first writes only.** R is the fork's replication factor, W its write quorum under its durability (ADR-0023: ⌊R/2⌋ + 1 for `quorum`, R for `all`, 1 for `local`), and Q its promotion quorum.

- **The completion quorum** is C = max(⌊R/2⌋ + 1, W, `min_replicas`): the holders `CompleteFork` needs.
- **The bootstrap quorum** is W_boot = max(⌊R/2⌋ + 1, W): the acknowledgements the fork-reset write needs before it counts as committed.

Under `quorum` and `all` they are W and its floor, as for any write. Under `local` they are a majority, so W_boot + Q > R holds for a fork's first writes under every durability: a later primary, chosen by ADR-0022 from a majority fenced, holds the copy and, if it committed, the marker. From `Ready` on, the fork's writes follow its own durability, `local` included.

**Fencing.** Every fork command after creation carries the fork identity `(name, target_incarnation, operation)`. `apply` checks it before anything else, and refuses a command whose identity doesn't equal the current database's with `Rejection::StaleForkIdentity`. A command meant for a dropped fork therefore can't touch its replacement, even with the same name and attempt number: it is refused and changes nothing.

1. **`ForkDatabase { operation, intent, incarnation, resolved_expiry, limits }`.**
   - `intent` is the caller's request as given, the target and source names and the requester included (point 5.2).
   - `incarnation` is drawn by the proposer (point 1.1).
   - `resolved_expiry` is optional: the absolute expiry the control service resolved from a relative one, as ADR-0041 resolves `--expires-in`. When it is absent, `apply` resolves a relative expiry as the command's own time plus the span. Either way it is resolved once, at first apply.
   - `limits` is the proposing node's `[fork]` configuration (point 7): `max_attempts`, `deadline`, `attempt_timeout`, `seeder_timeout`, and `operation_retention`. The fork records them, so the driver and `apply` use the fork's own limits, and a replay applies the same ones whatever the configuration is by then.
   - It refuses a target replica set smaller than the completion quorum with `Rejection::TooFewReplicas { needed, available }`: a fleet too small for the fork is not a disk problem.
   - It resolves `source` to its current incarnation, and refuses if:
     - the source doesn't exist;
     - the source isn't ready (a source that is itself a fork must be `Ready`);
     - the source doesn't match the intent's `expect_source_incarnation`, when it has one;
     - the source's engine can't fork (point 9).
   - A known operation ID is a resubmission, judged by point 5.2 before anything else.
   - It refuses an existing target name otherwise (`DatabaseExists`).
   - It creates the target with the carried incarnation, the source's engine (ADR-0021), the intent's durability and metadata (nothing is copied from the source), and `Seeding { attempt: 1, seeder, seeder_excluded: {}, disk_refused: {} }`.
   - `seeder` is the first **live** member of the target's replica set, in preference order (point 3.3 says how exclusions change that).
2. **`CompleteFork { identity, attempt, at, chain_hash, holders }`.** Accepted only when all of these hold:
   - the fork identity matches;
   - the state is `Seeding` at exactly `attempt`;
   - the source still exists with `source_incarnation`;
   - every holder is in the current replica set and not in `disk_refused`;
   - every holder's latest report is a *staged copy* (point 4.5) with log epoch 0 whose `(target_incarnation, operation, attempt, at, chain_hash, source_incarnation)` equals the command's, where `source_incarnation` is the value the source's primary stamped (point 4.1) and equals `Fork::source_incarnation`;
   - `|holders|` ≥ C.

   The state becomes `Seeded { attempt, at, chain_hash, holders, lease_from: the database's epoch }`, and `holders` becomes the fork's replica set (point 3.10).
3. **`RetryForkSeed { identity, attempt, cause, exclude_seeder: Option<NodeId> }`.** Accepted only with a matching identity at the current attempt, while `Seeding`.
   - If `attempt < max_attempts` and `now < deadline`, the state becomes `Seeding { attempt + 1, seeder: next }`, with `exclude_seeder` added to `seeder_excluded`.
   - **`seeder_excluded` is a preference, not a ban.** `next` is the first live member of the replica set, in preference order, that is in neither set; when every live member that isn't disk-refused has been excluded as a seeder, `next` is the first of those in preference order again, and the budget and the deadline bound the loop.
   - **Who is passed over.** A seeder that was down for `seeder_timeout` or left, or that reported a retryable `TransferFailed` or `PinExpired` for the attempt, goes into `seeder_excluded`, so one node's persistent trouble doesn't spend every attempt. An attempt that only timed out excludes nobody.
   - The driver proposes the retry only when a next seeder is live, or when the budget is spent and the retry will fail the fork.
   - Otherwise the state becomes `Failed { RetriesExhausted { last: cause } }` or `Failed { DeadlineExceeded { last: cause } }`.
4. **`RecordForkDiskRefusal { identity, attempt, node }`.** A disk-admission refusal (point 7.2), and nothing else, adds `node` to `disk_refused`. A disk-refused node is never seeder or holder for this fork.
   - If the members of the replica set that aren't disk-refused are fewer than C, the state becomes `Failed { InsufficientDisk }`. Seeder exclusions never count here.
   - If it was the seeder and another replica can seed, the same command moves to the next attempt with `RetryCause::SeederDiskRefused`, as `RetryForkSeed` does, within the budget.
   - If it was the seeder and no other replica can seed now, the refusal is recorded and the attempt keeps running, bounded by `attempt_timeout` and the deadline; the driver starts the next attempt, with that cause, once a seeder is live.
   - A refusal from a node that isn't the seeder is recorded whatever the attempt, since it is about the node.
5. **`FailFork { identity, attempt, reason }`.** For permanent reasons: the source gone or recreated, an engine that can't fork, or `SourceTooLarge`. Accepted only while `Seeding`, with a matching identity.
6. **`DropForkHolder { identity, attempt, holder, cause }`.** Accepted only while `Seeded` at `attempt`, with a matching identity, and only for:
   - **a holder whose latest report says its staged copy is lost** (`copy_lost`, point 4.5), and only while no lease has been issued for this `Seeded` (the database's epoch still equals `lease_from`). After a lease, a holder that lost its copy is re-seeded from the fork's primary as any follower is;
   - **a holder that is no longer a fleet member** (it left or was removed), at any time while `Seeded`. A holder that is merely down stays.

   The holder leaves `holders`. If fewer than C holders remain, the fork goes back to `Seeding { attempt + 1 }` within the budget:
   - with a fresh `deadline`, since time spent seeded isn't spent seeding;
   - with `disk_refused` and `seeder_excluded` cleared, since disk may have been freed and seeders may have recovered meanwhile;
   - with its replica set following placement again;
   - with the lease and any promotion cleared, and the epoch kept, so the next first lease is at a higher epoch and fences any primary of the earlier attempt.

   No client was served before `Ready`, so starting again from the source loses nothing. Past the budget, the fork fails with `RetriesExhausted { last: HolderLost or HoldersLeft }`.

   *As built (2026-11-09):* that holds only before any lease since completion. Once a lease has been issued (the database's epoch is above `lease_from`), the primary may have opened its gate and acknowledged client writes once its reset committed at W_boot, so seeding again from the source would discard them silently. Its primary also counts W over the holders it was given, so dropping even one holder while more than C remain would shrink the set a promotion fences, and its Q, before the primary knows, while a removed holder that is still alive goes on acknowledging its writes. So every `DropForkHolder` is then refused with `Rejection::HolderNeededAfterLease`, the driver doesn't propose it, and the fork stays `Seeded` on all its holders, its lease kept. A holder that left is harmless there: a promotion fences only members, so it never counts toward the fence, and Q is over every holder. It becomes ready if W_boot of them reported the reset, and otherwise stays unusable but safe, as invariant (g) allows, until its holders return or an operator drops it. Seeding again, and the lease and promotion it clears, happens only before any lease, when nothing was served. No log written before this note holds a fork command: forks exist only on the unreleased 0.8 branch, so no recorded outcome changes.
7. **`MarkForkReady { identity, epoch, reset_position }`.** Accepted only when all of these hold:
   - the identity matches;
   - the state is `Seeded`;
   - `lease_from < epoch <= ` the database's epoch, and the database holds a lease now. A reset written under a lease issued before this completion, in an earlier attempt, never counts after a reseed, and a ready fork always has a lease;
   - at least W_boot members of the fork's replica set (its holders) report `reset { identity, epoch, position: reset_position, committed }` (point 4.5).

   The state becomes `Ready`. A second `MarkForkReady` for a ready fork is refused and changes nothing.
8. **`DropDatabase` and `ExpireDatabase`** on a fork in any state remove it as any drop does (ADR-0042). On a fork that is `Seeding` or `Seeded`, the operation's outcome becomes `Cancelled { Dropped }` or `Cancelled { Expired }`; a `Failed` or `Ready` outcome stays as it was. This is the cancellation path; there is no separate cancel command.
9. **Other commands before `Ready`.**
   - `AcquireLease`:
     - is refused while `Seeding` or `Failed` (`Rejection::LeaseOnUnseededFork`);
     - while `Seeded`, is always the fork's first lease since completion: completion records the epoch, a lease once issued changes hands only by promotion (ADR-0022), and seeding again clears it. It is accepted only from a member of the replica set, which is the recorded holders, whose latest report still shows the staged copy `{identity, attempt, at, chain_hash}` with log epoch 0 that `Seeded` recorded. Anything else is `Rejection::StagedCopyNotReported`; there is no separate "not a holder" refusal.

     After the first lease, promotions among the holders follow ADR-0018 and ADR-0022, including after a lease that ended before readiness, and the new primary follows point 6 to decide whether the reset still has to run.
   - `UpdateDatabase` (description, labels, expiry) is allowed, since it touches control state only. An expiry that passes before `Ready` cancels the fork (point 3.8).
   - `ChangeDurability` is refused until `Ready` (`Rejection::ForkNotReady`): the quorums above are fixed by the durability the fork was created with.
10. **The replica set is frozen from completion until `Ready`.** While `Seeded`, `replicas_of` (ADR-0031, point 4) returns the recorded holders in node-ID order, whatever the ring says, and a promotion among holders that rank equal breaks the tie by node ID: a join, a leave, a drain, or a weight change doesn't move a fork that isn't ready, and the first lease goes only to a recorded holder. From `Ready` on, the fork's replica set follows placement as any database's does. While `Seeding`, it follows placement: reports from non-members don't count, and new members copy from the seeder. A protocol that hands a holder's place to another node before `Ready` is deferred.

    *As built (2026-11-09):* the hand-over at `Ready` keeps the holders a little longer. The primary learns that the fork is ready only when it next reads control state, and until then it counts the holders alone toward each write. If placement moved while the fork was seeded, a write it acknowledges in that window may sit only on holders that placement no longer names. A promotion that fenced a quorum of placement would then miss the write. So `MarkForkReady` keeps the holders in `Ready { holders }`, and they stay the replica set (`replicas_of`, the promotion and fencing sets, and `Locate`) until `ReleaseForkHolders { identity, epoch }` releases them. From then on the fork follows placement. The protocol carries them in their own `ForkStatus.kept_holders` field, never in `holders`, so a node can't read them as another state's holders.

    - **The primary's side.** Once a primary sees the fork ready with its holders kept, it streams to the holders and to placement, and counts both write quorums toward every write: W of the holders and W of placement.
    - **The report.** When placement's write quorum holds everything the primary has committed, read after it began counting both, it reports `placement_caught_up { identity, epoch, membership_epoch }` in its heartbeats. `membership_epoch` names the ring whose placement it checked, from the same membership view; a report without it reads as absent. A primary that knows of nothing committed hasn't caught up.
    - **The release.** The driver proposes `ReleaseForkHolders` once the lease holder reports this at the current lease epoch, and at the current membership epoch, with no promotion in progress, and `apply` accepts it only then. A ring that changed after the report may place the fork on nodes that don't hold its log, so the release waits for a report about the ring now.
    - **Why it is safe.** Before the release, every acknowledged write reached W of the holders, under the holders alone or under both quorums, so a promotion's Q of the holders intersects it (W + Q > R on the holders). After the release, every write the primary acknowledged before the check is in the log that W of placement holds, and every write after it counted placement's quorum, so a promotion's Q of placement intersects it.
    - **Promotions during the hand-over.** A promotion between `Ready` and the release fences the holders, and the new primary hands over the same way, at its own epoch.
    - **Views and restarts.** A data node never moves a fork's frozen set backwards: a view that shows it seeded after the node saw it ready, or its holders kept after it saw them released, is ignored. A primary that starts serving a ready fork follows control state's view before its gate opens.
    - **What it costs.** Writes need both quorums for a few heartbeats. A write isn't held back for a group that is short: while either the holders or placement lacks W reachable replicas, as while placement's new members take their database snapshots, it is refused with `UnderReplicated`. `DropForkHolder` applies only while `Seeded`, so a holder that leaves the fleet during the hand-over stays in the set. If the primary is lost then and too few holders remain to fence a quorum, the fork waits, as invariant (g) allows. It never fences placement instead.
    - **Fencing only the holders.** A candidate that can't learn the fork's view from control state stops the attempt, which is tried again, rather than guess the ring. A replica of a fork whose replica set is its holders, seeded or ready with them kept, accepts a `Fence` only if control state names it a holder. Otherwise it refuses with the `replica` binding, so a node that only the ring names never records the candidate, and never accepts the candidate's stream on that record.
11. **The fork driver** runs on the control node (the Raft leader under ADR-0035). It is a pure function, `fork_actions(&State, now, rng) -> Vec<Action>`, so a control restart loses nothing: the next leader recomputes the same proposals, and draws only what a new command carries. It proposes:
    - `CompleteFork` as soon as reports satisfy point 3.2;
    - `RetryForkSeed` when the seeder has been down for `seeder_timeout`, when the attempt has run for `attempt_timeout`, or when the seeder reports a retryable failure for the current identity and attempt, with that cause;
    - `RecordForkDiskRefusal` when a node reports `seed_failure { InsufficientDisk }` for the current identity;
    - `FailFork` when the source is gone or recreated, or when a report gives a permanent reason;
    - `DropForkHolder` for a holder that reports `copy_lost`, or that is no longer a member (*as built:* only before any lease since completion, point 3.6);
    - `MarkForkReady` when W_boot members of the replica set report the reset committed at one epoch and position, under a lease issued since completion (point 3.7);
    - the deadline's `Failed`, through `Tick`.

    Every proposal is checked again by `apply`, so a stale or duplicated one is refused and changes nothing.

### 4. Seeding: one copy from the source, the rest from the seeder, every message fenced by the fork identity

1. **The seeder fetches from the source's primary.** The seeder of attempt *n* calls `Replication.Snapshot` on the source's primary with these new optional `SnapshotRequest` fields:
   - `fork`, `fork_incarnation` and `operation` (the fork identity);
   - `fork_attempt`;
   - `source_incarnation`.

   The primary allows the call under a new binding, **`seeding`** (proposed as an amendment to ADR-0031), only if its own control-state view shows that:
   - it holds the source's lease, at the source's incarnation equal to `source_incarnation`;
   - the database named `fork` has incarnation `fork_incarnation` and operation `operation`;
   - that fork is `Seeding` at `fork_attempt`, with this source and source incarnation;
   - the caller is that attempt's seeder.

   Anything else is `PERMISSION_DENIED` with `fleetring-binding: seeding` metadata (`auth::BINDING_KEY`), audited as every binding refusal is. A request from a dropped fork's seeder therefore fails, even if a new fork with the same name exists. *(Amendment accepted 2026-11-12: a request with the fork's identity from the attempt's seeder, for an attempt that isn't seeding or to a node that no longer holds the source's lease, is answered `UNAVAILABLE` with a retry-after, not refused; see [the amendment](#amendment-2026-11-12-fr-104-fr-105-fr-106-late-seeding-requests-the-first-lease-and-incarnation-fences).)*

   **The open copy is checked too.** The primary compares the incarnation recorded beside the copy it holds open with `source_incarnation`, refuses a mismatch with `FAILED_PRECONDITION`, and stamps the copy's incarnation on the last chunk (`SnapshotChunk.source_incarnation`). The seeder refuses a stream whose stamp differs from what it asked for, and records the stamped value in its sidecar and its report, which `CompleteFork` compares.
2. **The source pins at a quorum-acknowledged position, never at its local tip.**
   - `acknowledged` is the W-th highest of the primary's own tip and its followers' acknowledged positions, under the source's write quorum W. It isn't `Primary::committed`, which is the published tip.
   - The primary calls `pin_snapshot_at(Some(acknowledged))`. The copy therefore never holds a write that a source failover could lose.
   - **A checkpoint past `acknowledged`.** The SQLite adapter checkpoints at its tip, so `acknowledged` can be behind the current log's start, where `pin_snapshot_at` answers `InvalidPosition`. The primary then answers `UNAVAILABLE` with a retry-after, and the seeder retries without counting it against `transfer_retries`. While a seeding request is waiting this way, the primary holds its next checkpoint, as `checkpoint_due` already waits for lagging sinks, for at most `max_pin_ms`, so `acknowledged` catches up inside the log.
   - The first chunk carries `total_bytes`. The last chunk carries the position, the checksum, the chain hash, and the source's incarnation.
3. **Staging is durable, atomic, and named by the fork identity.** The seeder:
   1. writes the stream to a temporary file in `<data_dir>/staged/<fork>/<fork_incarnation>-<operation>/<attempt>/`, with `<fork>` encoded as the databases root encodes names (`layout::database_dir`);
   2. verifies the checksum, and records the primary's chain hash at the position as the hash its chain continues from, as a follower does from a database snapshot, since a snapshot can't prove its own chain;
   3. writes a sidecar holding the full identity, the attempt, the position, the chain hash, the stamped source incarnation, the file's size and checksum, and **log epoch 0**;
   4. fsyncs both, renames the file into place, and fsyncs the directory.

   Only then does it report the staged copy. A staged copy is never served to clients, and is opened as a primary only under point 6.2. Directories for different incarnations, operations, or attempts of the same name never collide.
4. **The other replicas copy from the seeder.** Each other member of the fork's replica set calls `Replication.Snapshot { database: fork, fork_incarnation, operation, fork_attempt: n, staged: true }` on the seeder. The seeder allows the call under the `seeding` binding only if:
   - its control-state view shows the same fork identity at attempt *n*;
   - the caller is in the fork's replica set;
   - it holds a staged copy whose sidecar has that identity and attempt.

   Otherwise it answers `UNAVAILABLE` with a retry-after, or `PERMISSION_DENIED` on an identity mismatch. Peers stage exactly as in point 4.3, with the seeder's sidecar values.

   *Amendment accepted 2026-11-12 (FR-104, FR-105):* a request with this fork's identity for an attempt that isn't seeding is answered, not refused, here and at the source's primary (point 4.1), as is the seeder's request to a node that no longer holds the source's lease; see [the amendment](#amendment-2026-11-12-fr-104-fr-105-fr-106-late-seeding-requests-the-first-lease-and-incarnation-fences) below.
5. **Reports.** `DatabaseStatus` in `HeartbeatRequest` gains optional fields, which `Report` stores and which decode as absent when missing:
   - `staged { fork_incarnation, operation, attempt, position, chain_hash, source_incarnation }`, always at log epoch 0: promotion ranks a staged copy at `(0, at)`. A follower that installed its copy from a staged copy (point 6.2) keeps reporting `staged` until its log holds a frame of the fork's own epoch, so it stays a candidate meanwhile;
   - `copy_lost { fork_incarnation, operation, attempt, reason }`, when a holder finds its staged copy missing or damaged (point 6.2);
   - `seed_failure { fork_incarnation, operation, attempt, reason, retryable }`;
   - `reset { fork_incarnation, operation, epoch, position, committed }`. On the primary, `committed` means its commit index at W_boot covers the marker; on a follower, that the marker is in its durable log. `MarkForkReady` counts these reports across the replica set (point 3.7);
   - `seed_progress { fork_incarnation, operation, attempt, bytes, total_bytes }`, while a node streams a copy, for status only: neither `apply` nor the driver reads it.

   The control plane compares every field with the current fork identity, and ignores a report for another identity.
6. **Deleting staged copies happens only on a positive signal for that identity** (ADR-0042). A node deletes a staged directory, or the files of a fork copy opened from one, only when one of these names the sidecar's identity:
   - a `Replication.DropCopy { database, epoch, incarnation, operation }` notice whose `incarnation` and `operation` equal the sidecar's, judged under the name's lock as ADR-0042 point 2 judges a notice. `DropCopy` gains the two optional fields because a seeding fork's epoch is 0, and the epoch alone can't name it;
   - the same identity seen `Failed`;
   - the same identity seen `Seeded` with this node not a holder;
   - the same identity seen at a later attempt (for the earlier attempt's directory).

   **A placement for the name with another incarnation is a view, not a signal.** It marks the directory an orphan and nothing more. So does a placement that doesn't know the name, or any other directory whose identity the node can't confirm. Staged orphans are listed and removed by `(name, target incarnation, operation, attempt)`: `orphan list` shows them as staged, and `orphan remove` names all four.

### 5. Operations: IDs, status, retries, cancellation

1. **Every fork request carries an `OperationId`**: 16 random bytes chosen by the client, printed as 26 lowercase base32 characters. The CLI makes one and prints it before it calls.
2. **Caller intent and resolved state are separate.**
   - **The caller's intent**, `ForkIntent`, is the request as given: the target name, the source name, `expect_source_incarnation` if any, the replication factor, the durability, the description, the labels, and the expiry as given (a relative `expires_in`, or an absolute `expires_at`). With `requested_by`, it is recorded at first apply.
   - **The resolved state** is what the first apply decided: the source's incarnation, the target's incarnation, the absolute expiry, and the times. It is never decided again.
   - **Resuming** an operation reads it: `Control.GetForkOperation(operation_id)` (point 5.4). The CLI resumes this way, and follows the record until a terminal outcome. Nothing is resent, and nothing is resolved again.
   - **Resubmitting** sends `ForkDatabase` again with a known operation ID, after a lost reply or a resend. It compares the caller's intent only, never the resolved state:
     - **the same requester and the same intent:** returns the existing operation's status, and changes nothing. A source recreated in between doesn't matter: the operation keeps the source incarnation it resolved, so it never forks the new source;
     - **another requester:** refused with `Rejection::OperationNotYours` (`PERMISSION_DENIED`), revealing nothing about the operation;
     - **the same requester and a different intent:** refused with `Rejection::OperationConflict { field }` (`ALREADY_EXISTS`), naming the first field that differs.
   - **Deduplication lasts exactly as long as the operation record:** while the operation is pending, and for `operation_retention` (7 days) after its terminal outcome. After that, the ID is unknown, and a request with it starts a new operation. The CLI never reuses an ID except through an explicit `--operation-id`, and its help and hints say that after 7 days the same command starts a new fork.
   - A new ID for a target name that exists is refused (`DatabaseExists`).
3. **Status persists in control state.** `State::fork_operations: BTreeMap<OperationId, OperationRecord { target, target_incarnation, source, source_incarnation, intent, requested_by, outcome, updated_at, finished_at }>`.
   - `outcome` is pending, `Pending { attempt }` or `Seeded`, or terminal, `Ready { at, chain_hash }`, `Failed { reason }`, or `Cancelled`.
   - `finished_at` is set at the terminal outcome, and retention counts from it. `Tick` prunes records past the `operation_retention` of the fork's own limits, and a record past it counts as unknown even before a `Tick` prunes it. Pending records are never pruned. Records are capped at `max_operations` (10,000); a new fork past the cap is refused with `RESOURCE_EXHAUSTED`.
4. **`Control.GetForkOperation(operation_id)`** returns the record to the requesting principal, or to a principal with `admin` on the target **while the target's name holds the record's target incarnation**. After a drop and a recreate, `admin` on the new database doesn't reveal the old operation; its requester still sees it. `db show <target>` shows the current fork's record.
5. **Cancellation is `db drop`** on a fork that isn't ready, which needs `admin` on the target, or an expiry that passes before `Ready` (point 3.8). The source's pin is released when its stream ends: the seeding binding refuses the dropped identity, and the pin is bounded by `max_pin_ms`.

### 6. The fork-reset write and readiness

The fork inherits the source's logical change record (`_fleetring_cdc`, ADR-0024) and replicated sink positions (`_fleetring_sinks`, ADR-0028), which live in the adapter's reserved namespace inside the database. Both describe the source's history, not the fork's.

1. **The marker lives in the database, written in the same transaction as the reset.** The adapter's `fork_reset(identity)` is one transaction. It clears the change record and the sink positions, and **replaces the contents of `_fleetring_fork` with exactly one row**, `(fork_incarnation, operation, source_incarnation, at)`. Change capture doesn't record this transaction.
   - The transaction is **conditional.** Inside it, the adapter checks for a marker row with this identity, and if one exists it does nothing. Running it twice is harmless.
   - **A fork of a fork** inherits its source's marker row, which names another identity, so its reset runs and leaves only its own row.
   - **Why in the database and not in control state:**
     - **Atomicity.** The marker commits with the reset or not at all. A control-state marker would need a second step that a crash could separate from the reset. A marker that said "done" when the reset wasn't would skip it. A missing marker after a committed reset would let the reset run again later, after clients had written, erasing legitimate change history.
     - **Durability.** The marker replicates through the WAL, and counts as committed only at the bootstrap quorum W_boot (point 3). W_boot + Q > R (ADR-0018) means any later primary's log, chosen by ADR-0022, holds the marker if the reset committed, whatever the fork's `acks`.
     - **Locality.** The primary decides from its own copy, with no round trip.
2. **Opening.** Whichever holder becomes the primary of a fork that isn't `Ready` in its view, by the first lease or by a promotion:
   - **opens only a verified copy and never creates files.** Before it asks for the first lease, it re-reads its sidecar and checks the staged file's size, position, and checksum against `Seeded { attempt, at, chain_hash }`. If the check fails, it reports `copy_lost` and never asks. A promoted holder opens the copy it followed, or, if it never followed one, its verified staged copy. If a check fails after it holds the lease, it serves nothing, reports `copy_lost`, and stops renewing, so the lease expires and another holder takes over. `Database.Create` and `Database.Restore` refuse, with `FAILED_PRECONDITION`, a database whose placement carries a fork that isn't ready, so no path makes an empty database in its place. *(Amendment accepted 2026-11-12: the first holder records its copy as it installs it, and serves the first lease only once control state confirms it, and the readiness gate and drop notices are judged by incarnation; see [items 7 to 9 of the amendment](#the-first-lease-the-readiness-gate-and-drops-by-incarnation-fr-106).)*
   - **offers only the matching staged copy for promotion.** A staged copy ranks at log epoch 0, position `at`, and only one whose `(identity, attempt, at, chain_hash)` equals `Seeded`'s is offered or opened. While the fork is `Seeded`, a report without a staged copy is a promotion candidate only if its log epoch is above `lease_from` and no higher than the database's epoch: a copy opened in an earlier attempt, or an undeleted copy of an earlier database of the name, never outranks the staged copies;
   - checks for the marker for the current fork identity;
   - if it is absent, runs `fork_reset` and waits for it to commit at W_boot;
   - then reports `reset { committed: true, epoch, position }`, as each follower does once the marker is in its durable log; `MarkForkReady` needs W_boot such reports (point 3.7).

   **Other holders become followers from their staged copies.** On the first stream at epoch *e*, a holder whose matching staged copy is at `at` installs it as its copy, with `epoch_start = at` and log epoch 0, and follows from there, instead of taking a full database snapshot. A replica that isn't a holder, which exists only from `Ready` on, takes a database snapshot as today.

   **Until its own commit index covers the marker at W_boot, the primary serves no client:**
   - `Database.*` data calls get `UNAVAILABLE` "fork not ready", with a retry-after;
   - `WatchChanges` gets the same;
   - change-stream sink pumps for the database don't start;
   - archives don't take a base snapshot.
3. **What a crash at each point does.**

   | Crash | Effect | Result |
   |---|---|---|
   | Before the reset | No marker. The next primary runs it. | The reset happens exactly once. |
   | During the reset | The transaction is atomic: either no marker, which is the case above, or committed, which is the case below. | Exactly once. |
   | After a local commit, before W_boot | ADR-0022 picks the log. If the marker survived, the next primary sees it and doesn't run the reset again. If it didn't, nothing after it survived either, because no client was served. | Exactly once in the surviving history. |
   | After W_boot, before `MarkForkReady` | The marker is committed. The next primary sees it and reports it. | Not repeated. The driver proposes `MarkForkReady`. |

   No client write can precede the marker in any surviving history. That is the property that makes a repeated reset impossible to observe, and FR-087 checks it.
4. **The control state's `Ready`** is advisory: it is for status, for `db fork` to stop waiting, and for clients to avoid retrying blindly. It never decides whether the reset runs.
   - A node that sees `Ready` in control state but no marker in its copy is in an impossible state; it refuses to serve and logs an error.
   - The opposite mismatch, a primary whose view still says `Seeded` while control state says `Ready`, is harmless: `Ready` means the marker committed at W_boot, so the primary finds it, doesn't run the reset, and serves once its commit index covers it.

### 7. Limits, budgets, and admission

FR-087 measured a fork on a three-node fleet (the note after the table, and `docs/perf.md`, "Forks"), and the defaults below are set from those measurements; a default *kept* is one no measurement bears on, and stays as first proposed. The control rows `max_attempts`, `deadline`, `attempt_timeout`, `seeder_timeout`, and `operation_retention` travel in `ForkDatabase` as `ForkLimits` and are recorded with the fork (point 3.1): changing the configuration affects new forks only.

| Setting | Where | Default | What happens at the limit |
|---|---|---|---|
| `[fork] max_attempts` | control | 5, *kept* | `Failed { RetriesExhausted { last } }` |
| `[fork] deadline` | control | 1 h per seeding, *measured*: two full attempts and their retries | `Failed { DeadlineExceeded { last } }` |
| `[fork] attempt_timeout` | control | 20 min, *measured*: twice `max_pin_ms` | `RetryForkSeed { AttemptTimedOut }` |
| `[fork] seeder_timeout` | control | 2 min down, *measured*: a restarted seeder is back in seconds | `RetryForkSeed { SeederDown }`, excluding the seeder from seeder selection |
| `[fork] transfer_retries` | data | 3 per attempt, backoff from 1 s, *kept* | the seeder reports a retryable failure |
| `[fork] max_pin_ms` | data (source's primary) | 10 min, *measured* | the stream ends `ABORTED`, and the pin, or the held checkpoint, is released |
| `[fork] max_pin_wal_bytes` | data | 1 GiB of WAL growth while pinned, *measured* | the same; source writes always win over a fork |
| `[fork] max_pin_ms_per_hour` | data, per source database | 20 min of pins in any hour, *measured*: two full pins | `RESOURCE_EXHAUSTED` with a retry-after; not an attempt failure, but the deadline runs |
| `[fork] max_pins_per_node` / `max_pins_per_database` | data | 2 / 1, *kept* | `RESOURCE_EXHAUSTED` with a retry-after; not an attempt failure |
| `[fork] max_seeds_per_node` | data | 2 concurrent staged downloads, *kept* | the same |
| disk admission | data | free − reserved ≥ `total_bytes` × 1.1 + max(1 GiB, 1% of the volume), *measured* | `seed_failure { InsufficientDisk }` for this node only |
| `[fork] admission_min_free_bytes`, `admission_min_free_percent` | data (the bytes also control) | 1 GiB and 1%: the margin of the row above | as above; the control plane's floor (point 7.3) uses the proposing node's bytes, recorded in `ForkLimits` |
| `[fork] operation_retention` | control | 7 days after the terminal outcome | the operation record is pruned; deduplication ends |
| `[fork] max_operations` | control | 10,000 records | `RESOURCE_EXHAUSTED` on new forks |
| client resends | CLI and MCP | 5 within 60 s on `UNAVAILABLE` | the error, and the operation ID to resume with |

*As built (2026-11-09):* the disk-admission margin is configurable, as the row above says, because max(1 GiB, 5% of the volume), as first proposed, refused a three-row fork on a developer's laptop with 50 GiB free of 926 GiB. The ×1.1 reservation is fixed. The configuration `fleetring dev` writes keeps 64 MiB and 0%. A fork made under the default bytes records none in `ForkLimits`, so its command encodes as before.

*Measured (2026-11-09, FR-087):* `cargo bench -p fleetringd --bench fork` forked sources of 10 MiB, 100 MiB and 1 GiB on a three-node fleet in one process, over TLS on loopback (source R = 3 and W = 2; fork R = 3, so C = W_boot = 2), on an M1 Max laptop with an APFS SSD, under the load of other builds. `docs/perf.md`, "Forks", has the tables; the CI Linux runner's row is to come, from the `measure-forks` label. What set each default:

- **The stream's rate.** The source's primary streamed the copy to the seeder at 59 MiB/s at 1 GiB (a 17.4 s pin) and 60 MiB/s at 100 MiB; the peers then copied the staged copy at about the same rate, so the seeding took about twice the pin, 36.9 s at 1 GiB. A shared CI runner, with a quarter of the cores and a network disk, is assumed four times slower, about 15 MiB/s: about 70 s to stream 1 GiB.
- **`max_pin_ms`, 10 min, confirmed.** The bound is at least k × the time to stream 1 GiB at the slowest rate assumed, with k = 4 for scheduling, a disk shared with the source's own writes, and retries: 4 × 70 s ≈ 5 min. 10 min doubles that, and streams about 8 GiB at the assumed slowest rate and 34 GiB at the laptop's; a larger source fails explicitly with `SourceTooLarge`, as point 12 says.
- **`max_pin_wal_bytes`, 1 GiB, confirmed.** A 17 s pin under four writers held 2.8 MiB of WAL. The fastest sustained write rate measured, the bench's own 1 MiB transactions at 1.1 MiB/s, would hold about 680 MiB over a full `max_pin_ms`, so 1 GiB lets a pin run its time on a write-heavy source and still bounds it.
- **`max_pin_ms_per_hour`, 20 min, confirmed:** two full pins an hour, so a pin that expires can be tried once more within the hour.
- **`attempt_timeout`, from 30 min to 20 min.** An attempt holds the seeder's stream, bounded by `max_pin_ms`, and then the peers' copies of the same bytes from the seeder, unpinned and measured at about the stream's rate, so at most another `max_pin_ms`: 2 × 10 min. Longer only delays the retry of an attempt that is stuck.
- **`deadline`, 1 h, confirmed:** two full attempts, 40 min, and the backoff and seeder changes between them.
- **`seeder_timeout`, 2 min, confirmed.** A seeder killed while it staged was back and fetching again within seconds (`tests/fork.rs`), and a daemon restart or upgrade fits well within 2 min, which is a tenth of an attempt.
- **The admission margin, from max(1 GiB, 5%) to max(1 GiB, 1%).** Each node's disk peaked at its source replica and its fork copy, and the fleet's at the final state: a staged copy is renamed into the database's place, so a fork never needs room for two copies, and the ×1.1 reservation covers it. The 1 GiB floor is `max_pin_wal_bytes`, the WAL a pin may add on the source's primary, which in a fleet of three is one of the fork's replicas. Nothing measured grows with the volume's size, so the share is only headroom for the file system: 1% keeps 9 GiB on a 926 GiB disk, where 5% asked for 46 GiB and refused a fork with 50 GiB free.
- **Not set by a measurement:** `max_attempts`, `transfer_retries`, the per-node and per-database pin and download counts, `operation_retention`, `max_operations`, and the client's resends, which bound retries and concurrency rather than a cost the bench measures.
- **What a fork costs a source's writes** stayed within the shared laptop's noise: the p50 of 64-byte writes rose by 5 to 17 ms (5% to 19%), throughput stayed level from 100 MiB, and stalls of 1 to 2 s happened in the baseline too. **A fork takes five control-log commands** (`ForkDatabase`, `CompleteFork`, the first lease, its renewals, `MarkForkReady`); the heartbeats logged meanwhile are the fleet's usual.

1. **Reservations.**
   - A data node keeps an in-memory **reservation ledger.** Admitting a seed reserves `total_bytes` × 1.1 until the copy is staged (the bytes are then on disk) or abandoned. Admission compares free disk *minus* outstanding reservations, so two concurrent seeds never count the same free bytes.
   - The ledger isn't persisted: a restart discards temporary files, and the reserved bytes with them.
   - The control plane's best-effort check (point 7.3) also subtracts each node's pending seeds, using the source sizes of forks it is a replica of that are still `Seeding`.
2. **Disk refusals and seeder exclusions are different things.** A node that refuses on disk reports `seed_failure { InsufficientDisk, retryable: false }` for the identity, and the driver proposes `RecordForkDiskRefusal` (point 3.4): the node is never seeder or holder for this fork. A seeder that times out or fails is only excluded from seeder selection (point 3.3), and may still hold a copy that a later seeder serves it. The fork fails with `InsufficientDisk` only when the members that aren't disk-refused are fewer than C.
   - While `Seeding`, placement fixes the replica set, so "choosing replicas that admit" means choosing the seeder and the holders among them. It never means placing the fork elsewhere.
3. **Control-plane admission, best effort.** Heartbeats gain an optional `disk_free_bytes` per node and `size_bytes` per replica. `ForkDatabase` is refused early (`Rejection::InsufficientDisk`, `RESOURCE_EXHAUSTED`) when fewer than C members of the target's replica set pass the check after reservations. The data node's own check is authoritative.
   - **The control plane checks only the floor:** free bytes, less pending seeds, at least `total_bytes` × 1.1 + 1 GiB. Heartbeats carry free bytes but not the volume's size, so the 5% term is the data node's to check.
   - **Unknown means admitted.** A member that hasn't reported its free bytes counts as having room, and so does every member when the source's size is unknown. Sizes are reported only once `fork` is enabled, so the first fork of a fleet passes this check unconditionally.
   - **Too few replicas isn't a disk refusal:** a replica set smaller than C is refused with `TooFewReplicas` before this check (point 3.1).
4. **The source's exposure.** Each attempt holds at most one pin on the source, for at most `max_pin_ms` and `max_pin_wal_bytes`, and the pins of all forks of one source are bounded by `max_pin_ms_per_hour`. A principal with `fork` on a busy source can therefore delay its checkpoints by at most that much an hour, which the docs state.

### 8. The `fork` fleet feature

- `ForkDatabase`, `CompleteFork`, `RetryForkSeed`, `RecordForkDiskRefusal`, `FailFork`, `DropForkHolder` and `MarkForkReady` need `fork` (ADR-0038, point 4). The first accepted `ForkDatabase` enables it. `ForkDatabase` also needs `metadata`, and `expiry` when it sets an expiry (ADR-0041).
- **Enabling `fork` also starts storing incarnations** (point 1.2). Before then, `apply`'s behaviour for every existing command is byte for byte unchanged, and so is the stored state.
- New protocol fields are optional. Once `fork` is enabled, a node of the previous line can't join (`MissingFeature`), and `upgrade --rollback` refuses, naming `fork`.
- A control plane that predates forks answers `Control.ForkDatabase` and `GetForkOperation` with `UNIMPLEMENTED`, or with `PERMISSION_DENIED` ending `has no scope requirement` (ADR-0041, amendment 4). Clients read both as "the control plane runs a release without forks; upgrade it first".

### 9. Engines

- A fork's engine is its source's, and is recorded (ADR-0021).
- Forking needs a *frame-source* adapter (ADR-0016) that offers `pin_snapshot_at`, `open_follower`, and the conditional `fork_reset` with its marker. That is SQLite in 0.8.
- `ForkDatabase` for a source whose engine can't fork, such as a self-replicating one, is refused at admission with `FAILED_PRECONDITION`, until its adapter implements forking. `UNIMPLEMENTED` stays the answer of an older release (point 8). `Failed { EngineUnsupported }` is left for a source primary whose adapter refuses during seeding.
- Positions, chain hashes, staged copies, and the marker's encoding are opaque above the adapter.

### 10. Archives and metadata

- The fork's provenance, `forked_from`, is part of the metadata that ADR-0041 point 10 archives (proposed metadata, noted in ADR-0041). A restored database shows it as the archive's claim. It is unverified until signed heads exist (FR-074, ADR-0044 §6.6).
- An archive written before this ADR reads with no `forked_from`.
- A fork's archive starts with its own base snapshot after the fork is `Ready`. A restore of it holds the marker, so it never runs the reset again.
- Metadata comes only from the fork command, and from `UpdateDatabase` afterwards. A source's description is another principal's text (ADR-0041 point 9), so it is never copied.
- `forked_from` grants nothing.

### 11. Authorization, metering, and tests

- **Authorization.**
  - `ForkDatabase` needs `fork` on the source and `admin` on the target, and runs `check_new_name` on the target. Until the `fork` action exists (FR-088), `admin` on the source stands in for it.
  - `GetForkOperation` follows point 5.4.
  - `CompleteFork`, `RetryForkSeed`, `RecordForkDiskRefusal`, `FailFork`, `DropForkHolder` and `MarkForkReady` are proposed by the control node only. No RPC accepts them from a client or a data node.
  - The `seeding` binding (point 4.1) governs both `Snapshot` paths.
- **Metering.**
  - An audit line for every request and transition: the caller, the source and its incarnation, the target and its incarnation, the operation, the attempt, and the outcome.
  - Counters `fleetring_forks_total{outcome}` and `fleetring_fork_attempts_total{result}`.
  - Histograms `fleetring_fork_pin_seconds`, `fleetring_fork_seed_bytes` and `fleetring_fork_ready_seconds`.
  - A gauge of WAL bytes held by pins, and one of reserved disk.
  - No quotas.
- **Tests.** The simulation (ADR-0020) runs the recovery table of point 12 as scenarios, with point 12's invariants as history-checker properties (FR-087).

### 12. Recovery: every failure, the quorum it assumes, and what recovers it

"Reachable" means reachable from the control plane, for reports and leases, and from the relevant primary, for replication. R, W, Q, C, and W_boot are the fork's own (point 3; ADR-0018, ADR-0023).

| When | Event | What it assumes | What happens | Outcome |
|---|---|---|---|---|
| any time | The reply to `ForkDatabase` is lost | the client reaches the control plane again within the retention window | resubmit with the same ID and intent from the same principal, or resume with `GetForkOperation` (point 5.2) | the same fork, never a second |
| any time | The control node restarts, or leadership changes, or `control.db` is replaced by an older copy | a control quorum (ADR-0035) | state comes back from the log; the driver recomputes; incarnations are random, so an older log can't repeat one that a rule depends on | continues, or the fork is unknown and its copies are orphans |
| any time | A heartbeat or its reply is lost, duplicated, or delayed | none | reports are compared with the current identity; stale ones are ignored | continues |
| any time | **The target is dropped and recreated** while old commands, `Snapshot` requests, staged reports, or directories are in flight | none | every one of them carries the old `(incarnation, operation)`; `apply` and the binding refuse them; old directories are deleted only on a `DropCopy` naming their identity, and kept as orphans otherwise | the new fork is unaffected |
| any time | **A node sees a placement for the name with another incarnation** (a stale or replaced control state, or a recreated name) while it holds a staged directory | none | the directory is marked an orphan; nothing is deleted on that view (point 4.6) | no copy lost; an operator removes the orphan |
| Seeding, before staging | The source's primary fails over or is drained | the source keeps a reachable write quorum (its own availability) | the stream ends `UNAVAILABLE`; the seeder relocates the primary and refetches up to `transfer_retries`, from a fresh pin | continues; the new pin may be at a later position |
| Seeding, before staging | **The source checkpointed past the acknowledged position** | the source's followers keep acknowledging | `pin_snapshot_at` answers `InvalidPosition`; the primary answers `UNAVAILABLE` with a retry-after and holds its next checkpoint for up to `max_pin_ms`; the seeder retries without spending `transfer_retries` | continues once `acknowledged` is inside the log |
| Seeding, before staging | The pin expires | none | `ABORTED`; a retryable failure is reported | the next attempt, or `Failed { SourceTooLarge }` when every attempt's pin expires before `total_bytes` can move at the measured rate |
| Seeding, before staging | The seeder crashes and restarts | none | temporary files are discarded at start; it refetches | continues within the attempt |
| Seeding, before staging | The seeder is lost permanently | at least C members of the replica set that aren't disk-refused, eventually live | `RetryForkSeed { SeederDown }` after `seeder_timeout`, excluding it from seeder selection | the next attempt; `Failed { RetriesExhausted { SeederDown } }` or `DeadlineExceeded` if the budget or the deadline runs out, never `InsufficientDisk` |
| Seeding, during install | A crash between the temporary write, the rename, and the directory fsync | none | nothing is in place, or the rename is undone; nothing was reported | as above; never a partial staged copy |
| Seeding, after the seeder staged | The seeder crashes, then restarts | none | peers get `UNAVAILABLE` and retry; the seeder reports its staged copy from disk | continues |
| Seeding, after the seeder staged | The seeder is lost permanently | C matching reports exist already, or C members that aren't disk-refused remain for a new attempt | `CompleteFork` if C matching reports exist (a dead node's durable copy counts toward C, as an acknowledged write does; it must report it again before it may take the first lease); otherwise `RetryForkSeed` | completion, or the next attempt |
| Seeding | A peer refuses on disk | at least C members remain that aren't disk-refused | `RecordForkDiskRefusal`; the others continue | completion, or `Failed { InsufficientDisk }` if fewer than C can admit |
| Seeding | **The seeder refuses on disk, and no other replica can seed now** (the others are down or passed over) | a member that isn't disk-refused comes back live before the attempt timeout or the deadline | the refusal is recorded and the attempt keeps running; once a seeder is live, the driver starts the next attempt with `SeederDiskRefused` | the next attempt, or `Failed { DeadlineExceeded { SeederDiskRefused } }`, or `RetriesExhausted`; `InsufficientDisk` only if fewer than C can admit |
| creation | The fleet has fewer members than the completion quorum | none | `ForkDatabase` is refused with `TooFewReplicas { needed, available }` | nothing is created |
| Seeding | Membership changes the replica set | eventually C members of the new set reachable | reports from non-members don't count; new members copy from the seeder | continues, or retries |
| Seeding | The source is dropped or recreated | none | the binding refuses the new incarnation; the primary's open-copy check refuses a mismatch; the driver sees the change and proposes `FailFork` | `Failed { SourceGone }` or `Failed { SourceRecreated }` |
| Seeding or Seeded | The user drops the fork, or its expiry passes | none | gone; `Cancelled`; old messages are fenced as above | cancelled |
| Seeding, `acks = local` | **Completion under `local`** | a majority of the replica set eventually live | `CompleteFork` needs C = ⌊R/2⌋ + 1 holders, not W = 1 | completion only with a majority of copies |
| Seeded, never leased | **A holder lost or damaged its staged copy** | — | it fails its own check before asking for the lease and reports `copy_lost`; `AcquireLease` refuses it without a matching staged report; `DropForkHolder` removes it | another holder takes the lease; below C, `Seeding { attempt + 1 }` |
| Seeded | **A staged copy with a log epoch other than 0, or another identity, attempt, position, or chain hash** | none | it isn't offered for promotion or opened; only the copy matching `Seeded` at `(0, at)` is | the matching copy wins, or the fork stays unavailable |
| Seeded | **Placement changes between `Seeded` and `Ready`** (a join, a leave, a drain) | none | the replica set stays the recorded holders; nothing moves; the first lease goes only to a holder | continues on the holders; placement is followed once `Ready` |
| Ready, holders kept (as built, point 3.10) | **The primary crashes before it has seen the fork ready**, having acknowledged writes on holders that placement no longer names | Q of the holders reachable | the holders are still the replica set; the promotion fences a quorum of them, which holds every acknowledged write; the new primary counts both quorums until placement's holds its log, reports it, and `ReleaseForkHolders` moves the fork onto placement | ready, every acknowledged write kept; placement followed after the release |
| Seeded, never leased since completion (as built; the next row) | **Holders leave the fleet** until fewer than C remain members (none, in particular) | at least C members of the new replica set for the new attempt | `DropForkHolder { HoldersLeft }`; the lease is cleared and the fork goes back to `Seeding { attempt + 1 }` | the next attempt, or `Failed { RetriesExhausted { HoldersLeft } }` |
| Seeded, leased since completion | **Holders leave the fleet** until fewer than C would remain (as built, point 3.6) | none | holders that left are dropped while C remain; the next is kept, and `DropForkHolder` for it is refused; the lease stays | ready if W_boot holders reported the reset; otherwise seeded, unusable but safe, until holders return or an operator drops it; never seeded again over served writes |
| Seeded, never leased | Some holders are down | **at least one holder live and reachable**, still reporting its matching staged copy | that holder takes the first lease | continues to opening |
| Seeded, never leased | Every holder is down for good, but still a member | none | C copies lost, beyond what the durability settings tolerate, as for any database | unavailable; `db drop` and fork again; never served from an empty replica |
| Seeded, leased, opening | The primary crashes before or during the reset | a holder is promotable: ADR-0022 needs Q reachable members, and C + Q > R puts a holder with the copy, or a later copy, among them | the new primary finds no marker and runs the reset | ready, once W_boot is reachable |
| Seeded, leased, opening | The primary crashes after the reset committed at W_boot, before `MarkForkReady` | Q reachable for promotion | the new primary finds the marker, doesn't run the reset, and reports it | ready |
| Seeded, leased, opening | A failover between completion and the reset (the lease expires, or a drain) | as above | as the two rows above, by whether the marker committed | ready, reset exactly once |
| Seeded, leased, opening, `acks = local` | **The reset under `local`** | W_boot = ⌊R/2⌋ + 1 reachable | the reset counts as committed, and `MarkForkReady` is accepted, only at a majority; a promotion fencing a majority sees it | ready, reset exactly once |
| Seeded, leased, opening | The primary's staged copy fails its check after it took the lease | another holder | it serves nothing, reports `copy_lost`, and lets the lease expire; another holder is promoted | continues |
| Seeded, leased, opening | The write quorum is unreachable | — | the reset can't commit; clients keep getting `UNAVAILABLE` | ready when W_boot returns |
| Seeded, after a reseed | **Replicas report again a committed reset from an earlier attempt** (a delayed heartbeat, or a node whose old copy held the marker) | none | `MarkForkReady` counts only resets at an epoch above this completion's `lease_from` and at most the database's epoch, under a current lease; the old reports don't count, and an old copy isn't a promotion candidate (point 6.2) | the new attempt's reset decides; ready only from it |
| Ready | anything | ADR-0018 and ADR-0022 | an ordinary database | as any database |

**Invariants** (asserted in the simulation, FR-087):

- **(a) Fencing.** No lease is issued on a fork before `Seeded`, and the first lease of a `Seeded` fork goes only to a holder whose latest report shows the matching staged copy. A fork primary opens only a verified copy that matches `Seeded`'s `(identity, attempt, at, chain_hash)` at log epoch 0, or one it followed from such a copy, and never creates files. No client data call, `db watch`, or sink is served on a fork before its reset marker commits at W_boot.
- **(b) Acknowledged rows only.** Every row of a fork at `at` was quorum-acknowledged by the source's incarnation `source_incarnation` at or before `at`.
- **(c) No row from another incarnation** of the source.
- **(d) No crossing between forks.** No command, report, binding, or staged directory of one fork identity changes the state of another.
- **(e) Independence.** Writes to a fork never appear in its source, and the reverse.
- **(f) Unambiguous operations.** An operation ID maps to at most one fork identity, every resubmission from its requester with the same intent within retention returns that operation, and its outcome, once terminal, is one of `Ready`, `Failed`, `Cancelled` and never changes.
- **(g) Termination.** Every fork reaches `Ready`, or an explicit `Failed` or `Cancelled`, by its deadline, given the assumptions in the table. While `Seeded`, the deadline doesn't apply: a fork whose quorums stay unreachable stays `Seeded`, unusable but safe, until they return, an operator drops it, or its holders leave and it seeds again.
- **(h) Exactly one reset.** In every surviving history, the fork-reset write appears exactly once and before any client write.
- **(i) Frozen replica set.** Between `Seeded` and `Ready`, the fork's replica set is its recorded holders, whatever placement says. *As built (2026-11-09):* and from `Ready` until `ReleaseForkHolders` (point 3.10).
- **(j) No deletion on a view.** A staged directory, or a fork copy opened from one, is deleted only on a positive signal for its own identity (point 4.6).

## Alternatives considered

- **Completing on one staged replica, with a timeout for a replacement.** No timeout can make an empty replacement safe.
- **Completing on W under `acks = local`.** W is 1, so one lost copy would leave a recorded position no copy holds. Completion and the reset need a majority under every durability; `local` applies from `Ready` on.
- **Letting placement move a fork between completion and readiness.** New replicas would need copies of a fork that isn't ready yet, from holders that may be gone. Freezing the holders until `Ready` keeps the copy where it was counted; a holder-transfer protocol is deferred until a case needs it.
- **Fencing only the source.** A dropped fork's delayed messages could complete or advance its replacement.
- **Treating "seeded" as "ready".** Clients and sinks would see, and act on, the source's change record and sink positions.
- **A readiness marker in control state only.** It can't commit atomically with the reset, so a crash between the two either skips the reset or repeats it after legitimate history.
- **An unconditional reset by each new primary.** It would erase change history recorded after readiness.
- **Granting the first lease to any holder that reported once.** A holder whose copy was lost since would open an empty database; the lease needs a current report and a verified copy.
- **Every replica fetching from the source.** Simpler, but the pin is held R times as long, and the source's load multiplies.
- **Identifying databases by name and creation time.** A collision within one millisecond, or a clock stepping back, breaks it.
- **A counter of incarnations in control state.** A replaced or rolled-back `control.db` repeats its values, so any rule resting on them breaks exactly when control state is lost. Random values drawn by the proposer and carried in the command need no history.
- **Storing incarnations from the start, without the feature gate.** It changes every database record before the feature is used, which ADR-0038's rollback promise rules out.
- **A separate `CancelFork` command.** Two ways to remove one thing would invite disagreement.
- **Deleting staged copies whose identity a node can't see, or on a placement with another incarnation.** Both are deletion by absence, or on a view, which ADR-0042 forbids.
- **Pinning at the primary's published tip.** It can hold writes no quorum acknowledged, which a source failover loses.
- **File clones, and restoring a past position from an archive.** Deferred to after 0.8. File clones come back if FR-087's and FR-094's measurements show forks of over 100 MiB taking over 10 s, or doubling disk use, on a laptop; forks at a past position, from archives, and across fleets come back on their own demand (ADR-0044 §10).

## Consequences

- Forks survive every single failure that ordinary databases survive, under the stated reachability assumptions, and fail explicitly otherwise. No partial or empty copy is ever primary, and no client reaches a fork before its reset.
- Control state gains a random incarnation per database, a fork record per fork, and bounded operation records. Heartbeats gain optional staged, copy-lost, failure, reset, progress, size, and free-disk fields. `SnapshotRequest` gains the fork identity, the attempt, the source incarnation, and `staged`, under the `seeding` binding; `SnapshotChunk` gains `total_bytes` and `source_incarnation`; `DropCopy` gains the incarnation and the operation. On `main` on 2026-11-08 the next free field numbers are 12 in `Placement`, 3 in `SnapshotRequest`, 6 in `SnapshotChunk`, 5 in `DatabaseStatus`, and 4 in `HeartbeatRequest`.
- The proposer of `CreateDatabase` and `ForkDatabase` draws randomness from its injected generator, as ADR-0020 and ADR-0040 already require of the control node.
- `Replication.Snapshot` has two bindings, `follower` or `seeding` by whether the request names a fork, so the RPC table, the proto's `Binding:` line, and the daemon README's row name both. `replicas_of` returns a `Seeded` fork's holders.
- The primary of a source holds a checkpoint while a seeding request waits for `acknowledged` to enter its log, as it already does for lagging sinks, bounded by `max_pin_ms`.
- Promotion ranks a staged copy at `(0, at)`, and a follower can start from its staged copy with `epoch_start = at`.
- The SQLite adapter's reserved namespace gains `_fleetring_fork`, holding exactly one row.
- Sources pay a bounded price: one pinned stream per attempt, for at most `max_pin_ms` and `max_pin_wal_bytes`, and at most `max_pin_ms_per_hour` across forks.
- A fork's history isn't a byte-for-byte prefix of its source after `at`: the reset adds one write, which carries the marker. `db verify` at `at` matches the source; later positions don't. The docs say so.
- A fork can stay `Seeded`, unusable but safe, while its quorums are unreachable. `db show` reports it as opening, and an operator drops it or restores the quorum.
- Staged copies of dropped forks may stay as orphans on nodes that missed the drop until FR-077 (#105) brings tombstones; `orphan list` and `orphan remove` handle them by their four-part identity.
- ADR-0044 §7 (S2) is superseded for forks. §2.1 and §7.6 stand. ADR-0031 lists `seeding` as a proposed binding, ADR-0041 lists `forked_from` as proposed metadata, and ADR-0042 notes that staged copies follow its deletion rule; each note is dated and none rewrites accepted text.
- **Docs:** `docs/cli.md` designs `db fork` and the fork fields of `db show` and `db list`, planned for 0.8.0; the lexicon gains *incarnation*, *operation ID*, *staged copy*, the fork states, the fork identity, and the `seeding` binding; the daemon README designs the MCP tool `db_fork`.
- **Guidance for FR-084's acceptance (#134),** beyond its ticket:
  - incarnations are drawn by the proposer and carried in the command; replay with the carried values gives the identical state; 0 is refused where it would be stored; a command screened before `fork` was enabled and applied after it stores its random value;
  - `CompleteFork` needs C, and a majority under `local`; `MarkForkReady` needs a reset reported committed at W_boot;
  - a `Seeded` fork's replica set is its holders through a join, a leave, and a drain; `AcquireLease` from a non-holder, and a first lease from a holder whose latest report lost its staged copy, are refused;
  - `DropForkHolder` below C, and every holder leaving the fleet, return the fork to `Seeding { attempt + 1 }` with the lease cleared;
  - seeder timeouts never produce `InsufficientDisk`; only `RecordForkDiskRefusal` counts toward it; failures keep their `RetryCause`;
  - `DropDatabase` and `ExpireDatabase` on `Seeding` and `Seeded` record `Cancelled`; retention counts from the terminal outcome only;
  - resubmission compares intent only (same requester and intent, another requester, a different intent naming the field); a resubmission after the source was recreated returns the original operation;
  - `ChangeDurability` before `Ready` is refused; `UpdateDatabase` is allowed;
  - `GetForkOperation` from `admin` on a recreated target doesn't reveal the old operation.
- **Guidance for FR-087's matrix and mutation list (#137),** beyond its ticket. The matrix gains the new rows of point 12: a holder that lost its copy; a stale placement with another incarnation; a staged copy with a non-zero log epoch or another identity; a checkpoint past the acknowledged position; completion and readiness under `acks = local`; placement changes between `Seeded` and `Ready`; holders leaving the fleet; and expiry before `Ready`. The mutation list gains these deliberate bugs, each of which must fail a property:
  - deleting a staged directory on a placement with another incarnation (property j);
  - granting the first lease without a current matching staged report, or opening an unverified staged copy (property a);
  - ranking a staged copy above `(0, at)`, or offering one that doesn't match `Seeded` (property a);
  - pinning at `Primary::committed`, the published tip (property b);
  - completing on W, and committing the reset on W, under `local` (properties a and h);
  - moving a `Seeded` fork's replica set on a placement change (property i);
  - counting a seeder timeout as a disk refusal (the disk row must fail);
  - leaving a dropped `Seeded` fork's outcome as pending (property f);
  - drawing incarnations from a counter that a replaced `control.db` repeats (property d).

### Review findings and where they are resolved

| Finding | Resolved in |
|---|---|
| A holder with a lost or damaged staged copy could take the first lease | points 3.6, 3.9, 4.5 (`copy_lost`), 6.2; invariant (a); rows "A holder lost or damaged…" and "staged copy fails its check" |
| Deleting a staged copy on a placement with another incarnation | point 4.6; invariant (j); row "A node sees a placement…" |
| A staged copy's log epoch | points 4.3, 4.5, 6.2; invariant (a); row "A staged copy with a log epoch other than 0" |
| `pin_snapshot_at` refuses a position behind the last checkpoint; `acknowledged` isn't `Primary::committed` | point 4.2; row "The source checkpointed past…" |
| A counter of incarnations repeats after `control.db` is replaced | point 1.1; Alternatives |
| `acks = local`: one holder completes, one acknowledgement commits the reset | point 3 (C and W_boot), 3.2, 3.7, 6.1, 6.4; rows for `local` |
| Placement changes between `Seeded` and `Ready` | point 3.10; invariant (i); row "Placement changes…" |
| No holder of the frozen set is still a member | point 3.6; row "Holders leave the fleet…" |
| A second database could be created over a fork that isn't ready | point 6.2 (`Database.Create` and `Database.Restore` refuse) |
| A non-primary holder re-seeded in full | point 6.2 (followers from staged copies) |
| A fork of a fork keeps two marker rows | point 6.1 |
| The source's open copy isn't checked against its incarnation | point 4.1; point 3.2 |
| Cancellation of `Seeded` forks and by expiry; terminal outcomes; retention | points 2, 3.8, 5.3, 5.5; invariant (f) |
| Resume versus resubmit | point 5.2 |
| Seeder exclusions versus disk refusals | points 3.3, 3.4, 7.2; `RetryCause` in point 2 |
| Staged orphans' identity | point 4.6 |
| Metadata edits, durability changes, and expiry before `Ready` | point 3.9 |
| The source's exposure to repeated pins | point 7, `max_pin_ms_per_hour`, and 7.4 |
| The staged copy's chain hash | point 4.3 |
| `GetForkOperation` after a recreate | point 5.4 |
| An older control plane | point 8 |

## Open questions

None for the owner. FR-087's measurements set the defaults of point 7 (2026-11-09); the CI Linux runner's measurement is to be added to `docs/perf.md`.

## Amendment (2026-11-12, FR-104, FR-105, FR-106): late seeding requests, the first lease, and incarnation fences

- **Status:** Accepted (2026-11-12, by the owner). The decision above stands; this adds to points 4.1 and 4.4, and, from item 7 on, to points 6.2 and 4.6.
- **Revised:** 2026-11-12, after an independent review's seed sweep, to cover a source's primary that no longer holds the lease (item 5) and a receiver whose own control-state view is late (item 6); and again the same day, after the combined review of the 0.8.0 fork fixes (FR-106), to record the first lease's confirmation (item 7), the incarnation-keyed readiness gate (item 8), and the incarnation fence on drops (item 9); and on 2026-11-13, after the final check of those fixes, to add item 9's control-state condition and the legacy incarnation's exact-epoch rule, which close a forged notice naming a live database's incarnation.

Point 4.4 says the seeder answers `UNAVAILABLE` with a retry-after, or `PERMISSION_DENIED` on an identity mismatch, but not which one a request gets when its identity matches and its attempt isn't the one seeding. As built, both the seeder and the source's primary refused it as point 4.1 refuses "anything else": `PERMISSION_DENIED`, counted and audited as a binding refusal. Such a request is made legitimately, from a view a moment older or newer than the receiver's. A peer asks the seeder just as `CompleteFork` records holders without it, because its stream stalled behind a partition and timed out, because its calls to the control node timed out under link loss (FR-104, FR-105), or with no fault at all, as when a source's primary is drained while the fork seeds. The simulation's check that no legitimate node call is refused for its binding failed on each.

1. **Identity mismatches are refused as before.** A request whose fork name, incarnation, or operation, source, or source incarnation differs from the receiver's control-state view, or whose caller doesn't have the role point 4.1 or 4.4 requires for the attempt it names, gets `PERMISSION_DENIED` with `fleetring-binding: seeding`, counted and audited.
2. **A late or early attempt is answered.** A request whose identity matches but whose attempt isn't the one the receiver's view shows seeding gets `UNAVAILABLE` with a retry-after (`fleetring-retry-after-ms`). That covers a fork seeding a later attempt, a fork past seeding (`Seeded`, `Ready`, or `Failed`), and a receiver whose view hasn't reached the attempt yet. The answer names no binding and isn't counted or audited as a refusal, and nothing is pinned or read. This holds for the seeder's request to the source's primary (point 4.1) and for a peer's request to the seeder (point 4.4).
3. **The caller's role is checked once its attempt is seeding.** A late request is answered before its caller's role is checked, because a seeded fork's frozen replica set (point 3.10) may no longer name a peer that legitimately asked.
4. **Callers treat it as any retry-after:** it costs none of `transfer_retries`, and the fetch asks control state again before it retries, stopping once its own view no longer shows the attempt.
5. **A source's primary that no longer leads is answered too.** A request to the source's primary whose identity and attempt match, from the attempt's seeder, sent to a node whose view no longer shows it holding the source's lease at the source's current epoch, gets the same answer as item 2: the source's lease moved after the seeder looked, as under link loss, when the old primary's own calls to the control node stall. The seeder asks control state for the source's primary again before it retries. A caller that isn't the attempt's seeder is still refused (item 1), because the seeder is checked first.
6. **A receiver whose view is late answers too.** The receiver's control-state views for one request answer within 5 s all told, the bound on a data node's fork control calls; past it, the request gets `UNAVAILABLE` with a retry-after, not the caller's stream-idle deadline, so a stalled control connection at the receiver costs the caller none of `transfer_retries`. So does a seeder's own control call that runs out of time.

`UNAVAILABLE` rather than `FAILED_PRECONDITION`, which ADR-0031's amendment uses for a superseded epoch: point 4.4 already answers a seeder that isn't ready with `UNAVAILABLE` and a retry-after, and the seeding path's callers retry on a retry-after at no cost. `FAILED_PRECONDITION` without one would spend a seeder's transfer retries on a question of timing. As with ADR-0031's superseded epochs, a stale request is stale, not unbound: it still gets nothing, but it is no longer evidence of a misbound caller, so the audit log no longer records it.

### The first lease, the readiness gate, and drops, by incarnation (FR-106)

A fork made again under a dropped fork's name has its first lease at the epoch the dropped fork's had, often on the same first holder (#165), and a first holder's lease can expire before it serves, unseen by it. Point 6.2 judged both by the epoch alone. The sweeps of the 0.8.0 fork fixes found each way that went wrong, and the combined review two more.

7. **A first lease is served only once control state confirms it.** The first holder takes the lease, moves its staged copy into place, and at once records the copy durably: the fork's incarnation, and the lease's epoch with itself as the holder. A holder that installs its staged copy as a follower records the incarnation alone; a record with an incarnation and no epoch counts as a copy at epoch 0. Then the first holder reads control state again, and serves only if it shows this node leading at the lease's epoch, of the fork's incarnation, and the node has seen no later epoch of that incarnation. Otherwise it serves nothing: it doesn't renew the lease, which expires, and a promotion opens the fork, as after a copy that fails its check. A control call that can't be confirmed counts as not confirmed. `Database.Create` and `Database.Restore` do the same with a new database's first lease: they serve only if the placement read after the lease shows this node leading at its epoch, of the incarnation the placement read before it showed, and answer `UNAVAILABLE` otherwise.
8. **The readiness gate is keyed by incarnation and epoch.** A primary that opens the gate records it opened under its lease's incarnation and epoch, and a later start closes the gate unless it opened under that same pair; a gate opened for a dropped database of the name never counts for a later one at the same epoch. An opening whose lease names an incarnation control state no longer holds stops for good, checked before who leads. A start at an epoch the node has seen superseded leaves the gate as the newer primary set it, and only epochs seen for the lease's incarnation supersede it. The gate is decided and closed under the lock an opening marks itself open under. The gate's state, closed or opening, goes with the copy when it is deleted, and with a lease on another database of the name; a primary of a database no fork made opens a gate a dropped fork of the name left closed.
9. **A drop notice is fenced by the copy's incarnation.** A node deletes its copy on a notice that names the copy (ADR-0042, point 2, as amended): when both the notice and the copy name an incarnation, the same one, at the copy's highest epoch or a later one, so a copy of the dropped database that missed its later epochs, as a first holder whose lease was never confirmed, is deleted; a copy of another incarnation is kept, whatever the epoch. When either names none, the epoch alone decides, as before. A copy that goes on a notice goes at its own highest epoch. A notice that names a seeding fork's identity is about its staged copies (point 4.6), as before. **Control state must agree, too:** a notice never stands while control state holds the database its random incarnation names, whatever epoch it names, so a forged notice naming a live database's own incarnation at a later epoch deletes nothing; it fails the `dropped` binding. The legacy incarnation, 0, names every database of the name made before forks, so a notice naming it, like one naming none, is judged by the exact epoch, against the copy and against control state's lower epoch, as ADR-0042 point 2 first said. A copy's incarnation is checked against control state when a stream or a fence brings it a new epoch's holder, and at a stream's end and at start: a copy that records another random incarnation than control state holds is refused the stream or the fence and kept as an orphan, never followed as the new database's. A first holder's copy that a crash left in place before its record was written is recorded at the next start if its record was never written; one whose record exists but can't be read is left in place for an operator, never moved or deleted at start. A restore whose first lease isn't confirmed deletes its copy only while nothing has recorded it.

