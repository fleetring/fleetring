# ADR-0046: A fork names its source's incarnation and its own, stays `seeding` until a write quorum of its replicas holds the staged copy, becomes `ready` only after a replicated reset, and is driven by operations with IDs, budgets, and bounds

- **Status:** Proposed. FR-087's measurements set the defaults of point 7, and the owner accepts this record after FR-087.
- **Date:** 2026-11-08
- **Related:** ADR-0016 (engine adapters), ADR-0017 (control state), ADR-0018 (leases, epochs, quorums), ADR-0020 (simulation), ADR-0021 (engines), ADR-0022 (failover), ADR-0023 (durability), ADR-0024 (logical change capture), ADR-0028 (replicated sink positions), ADR-0029 (archives), ADR-0031 (node calls bound to placement; the `seeding` binding is proposed here), ADR-0035 (Raft control group), ADR-0038 (fleet features), ADR-0041 (metadata; `forked_from` is proposed here), ADR-0042 (no deletion by absence; staged copies follow it), ADR-0044 (umbrella accepted; its §7, specification S2, is superseded for forks by this record); tickets FR-083 to FR-087 (#133 to #137), FR-088 (#138, the `fork` action), FR-097 (#147, the usability trial), FR-074 (#99), FR-077 (#105)

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

What the code has today (checked against `main` on 2026-11-08):

- **`fleetring_control::Database`** holds the name, replication factor, engine, durability and pending durability, lease epoch, lease, promotion, and metadata. It has no incarnation. It is stored as a `DatabaseRecord` whose later fields (`acks`, `min_replicas`, `pending_durability`, `metadata`) decode as optional, and `metadata` is skipped when absent, so a record without it is written byte for byte as before.
- **`MemberEntry::reports`** holds each member's latest `Report` per database: its replica role, position, arrival time, and log epoch.
- **`Replication.Snapshot(SnapshotRequest { database, epoch })`** streams a primary's database snapshot as `SnapshotChunk`s, with the position, the checksum, and the chain hash on the last chunk, under the `follower` binding (ADR-0031).
- **`FrameSource::pin_snapshot_at(Option<&Position>)`**, reached through `DatabaseHandle::pin_snapshot_at`, pins a consistent copy that holds the log up to a commit boundary of the current log and nothing after it. Archives use it at positions their change stream reached, which only acknowledged batches reach (ADR-0029).
- **`Database.Restore`** fills a database that the control plane has already created with `CreateDatabase` and never leased: one replica restores it, takes the first lease, and serves it, and followers seed from it.

## Decision

### 1. A database has an incarnation

1. **`Database::incarnation: Incarnation(u64)`** is assigned by `apply` from a fleet-wide counter, `State::next_incarnation`, which only grows.
   - Every command that brings a database name into existence takes the next value: `CreateDatabase` (which a restore also issues, before `Database.Restore` fills the copy) and `ForkDatabase`.
   - The counter never decreases and no value is reused. Dropping and recreating a database, a fork included, therefore always gives a different incarnation.
2. **Absent means 0, the legacy incarnation.**
   - Records written before this ADR, and records created before the `fork` feature is enabled, decode with incarnation 0.
   - Incarnations are assigned from 1 only once `fork` is enabled (point 8). This keeps the change invisible to a fleet that can still roll back. It is also safe: no fork can exist before the feature is enabled, so no fork can confuse two legacy incarnations of one name.
   - After enabling, a recreated name gets an incarnation of 1 or more, so `(name, 0)` names at most one database for the rest of the fleet's life.
   - **Every fork has an incarnation of 1 or more**, because `ForkDatabase` itself enables the feature.
3. **Placements carry the incarnation.** `Placement` gains `incarnation` (0 when absent). Every node's view of a database is `(name, incarnation)`. A data node records it beside its copy's highest epoch.

### 2. A fork's record, and what "seeded" and "ready" mean

A database gains an optional `fork: Option<Fork>`. **A database without the field is an ordinary database, fully seeded and ready.** That covers every database that exists today, and every database not made by a fork.

```text
Fork {
  operation:          OperationId,          // 128 bits, chosen by the client (point 5)
  source:             DatabaseName,
  source_incarnation: Incarnation,
  requested_by:       PrincipalName,        // grants nothing, like created_by
  requested_at:       Timestamp,
  request:            CanonicalForkRequest, // point 5.2
  state:              ForkState,
}
// The fork's own incarnation is Database::incarnation. Every message about this fork carries
// the triple (name, incarnation, operation): its *fork identity*.

ForkState =
  | Seeding { attempt: u32, seeder: NodeId, excluded: BTreeSet<NodeId>, attempt_started_at, deadline }
  | Seeded  { at: Position, chain_hash: ChainHash, holders: BTreeSet<NodeId>, completed_at }
  | Ready   { at, chain_hash, completed_at, reset_epoch: LeaseEpoch, reset_position: Position, ready_at }
  | Failed  { reason: ForkFailure, attempts: u32, failed_at }
```

- **`Seeded` means "a write quorum durably holds the copy".** The fork may then take its first lease. It is **not** ready: no client data call, no `db watch`, and no sink runs against it.
- **`Ready` means "the fork-reset write has committed".** It mirrors the in-database marker of point 6, for status and for clients deciding when to call. The database's own marker is authoritative: the control state never decides whether the reset runs.
- **`Seeded` and `Ready` keep the provenance** that `forked_from` shows: the source, its incarnation, the position, the chain hash, and the time. It stays for the database's life, and ADR-0041's metadata archive carries it (point 10).
- **`Failed` is permanent until `db drop`.** A failed fork is never served and never leased, and it holds no copy anyone may open.
- **`ForkFailure`** is `#[non_exhaustive]`: `SourceGone`, `SourceRecreated`, `RetriesExhausted { last }`, `DeadlineExceeded`, `SourceTooLarge`, `InsufficientDisk`, `EngineUnsupported`.

### 3. States and transitions

```text
              ForkDatabase (op, source@inc_s) creates target@inc_t
                           │
                           ▼
       ┌──────────► Seeding{attempt n, seeder} ──── DropDatabase ──► gone (op Cancelled)
       │                   │      │      │
RetryForkSeed              │      │      └─ FailFork / deadline / budget ──► Failed ── DropDatabase ──► gone
(n+1, next admitting seeder)      │
       └───────────────────┘      │ CompleteFork(name, inc_t, op, n, at, hash, holders ≥ W)
                                  ▼
                   Seeded{at, hash, holders}: no lease yet, not ready
                                  │ AcquireLease by a live holder (first lease only)
                                  ▼
          leased at epoch e ≥ 1, opening: the primary runs the fork-reset write unless
          its copy already holds the reset marker; clients get UNAVAILABLE "fork not ready"
                                  │ reset marker committed at a write quorum; primary reports it
                                  ▼ MarkForkReady(name, inc_t, op, e, reset_position)
                   Ready: clients, db watch, and sinks served
                                  │
                                  ▼
              an ordinary database; ADR-0018 and ADR-0022 from here on
```

Every transition is a control command applied by the pure `apply`. Time arrives in the command, so the replay test covers every path.

**Fencing.** Every fork command after creation carries the fork identity `(name, target_incarnation, operation)`. `apply` checks it before anything else, and refuses a command whose identity doesn't equal the current database's with `Rejection::StaleForkIdentity`. A command meant for a dropped fork therefore can't touch its replacement, even with the same name and attempt number: it is refused and changes nothing.

1. **`ForkDatabase { operation, name, source, expect_source_incarnation, replication_factor, durability, metadata, requested_by }`.**
   - It resolves `source` to its current incarnation, and refuses if:
     - the source doesn't exist;
     - the source isn't ready (a source that is itself a fork must be `Ready`);
     - the source doesn't match `expect_source_incarnation`;
     - the source's engine can't fork (point 9).
   - It refuses an existing target name, except for an idempotent retry (point 5).
   - It creates the target with a new incarnation, the source's engine (ADR-0021), the command's own durability and metadata (nothing is copied from the source), and `Seeding { attempt: 1, seeder, excluded: {} }`.
   - `seeder` is the first **live** member of the target's replica set, in preference order, that isn't excluded (point 7.2).
2. **`CompleteFork { name, target_incarnation, operation, attempt, at, chain_hash, holders }`.** Accepted only when all of these hold:
   - the fork identity matches;
   - the state is `Seeding` at exactly `attempt`;
   - the source still exists with `source_incarnation`;
   - every holder is in the current replica set;
   - every holder's latest report is a *staged copy* (point 4.5) whose `(target_incarnation, operation, attempt, at, chain_hash, source_incarnation)` equals the command's;
   - `|holders|` is at least the fork's **write quorum** under its durability (ADR-0023): W for `quorum`, R for `all`, and never below `min_replicas`. Under `local`, W is 1, so one holder completes a fork, and the recovery rows below that rest on W + Q > R don't hold, as for any `local` database.

   The state becomes `Seeded`.
3. **`RetryForkSeed { name, target_incarnation, operation, attempt, reason, exclude: Option<NodeId> }`.** Accepted only with a matching identity at the current attempt.
   - If `attempt < max_attempts` and `now < deadline`, the state becomes `Seeding { attempt + 1, seeder: next }`, with `exclude` added to `excluded`. `next` is the next live member of the replica set, in preference order, that isn't excluded.
   - Otherwise the state becomes `Failed { RetriesExhausted }` or `Failed { DeadlineExceeded }`.
4. **`FailFork { name, target_incarnation, operation, attempt, reason }`.** For permanent reasons. Accepted only while `Seeding`, with a matching identity.
5. **`MarkForkReady { name, target_incarnation, operation, epoch, reset_position }`.** Accepted only when all of these hold:
   - the identity matches;
   - the state is `Seeded`;
   - `epoch` is a lease epoch the database has issued;
   - the reporting primary's report shows the reset marker committed for this identity at `reset_position`.

   The state becomes `Ready`. A second `MarkForkReady` for a ready fork is refused and changes nothing.
6. **`DropDatabase`** on a fork in any state removes it as any drop does (ADR-0042). The operation's outcome becomes `Cancelled` while `Seeding`, and stays as it was otherwise. This is the cancellation path; there is no separate cancel command.
7. **`AcquireLease`**:
   - is refused while `Seeding` or `Failed`;
   - is accepted from holders only, while `Seeded` and never leased (epoch 0);
   - otherwise follows ADR-0018 and ADR-0022, including while `Seeded` after a first lease that ended before readiness.

   The new primary then follows point 6 to decide whether the reset still has to run.
8. **The fork driver** runs on the control node (the Raft leader under ADR-0035). It is a pure function, `fork_actions(&State, now) -> Vec<Action>`, so a control restart loses nothing: the next leader recomputes the same proposals. It proposes:
   - `CompleteFork` as soon as reports satisfy point 3.2;
   - `RetryForkSeed` when the seeder has been down for `seeder_timeout`, when the attempt has run for `attempt_timeout`, or when the seeder reports a retryable failure for the current identity and attempt;
   - `FailFork` when the source is gone or recreated, or when a report gives a permanent reason;
   - `MarkForkReady` when the primary's report shows the reset marker committed;
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

   Anything else is `PERMISSION_DENIED` with `fleetring-binding: seeding` metadata (`auth::BINDING_KEY`), audited as every binding refusal is. A request from a dropped fork's seeder therefore fails, even if a new fork with the same name exists.
2. **The source pins at a quorum-acknowledged position, never at its local tip.** The primary calls `pin_snapshot_at(Some(acknowledged))`, where `acknowledged` is the highest position its write quorum has acknowledged. The copy therefore never holds a write that a source failover could lose.
   - The first chunk carries `total_bytes`.
   - The last chunk carries the position, the checksum and the chain hash, as today.
3. **Staging is durable, atomic, and named by the fork identity.** The seeder:
   1. writes the stream to a temporary file in `<data_dir>/staged/<fork>/<fork_incarnation>-<operation>/<attempt>/`, with `<fork>` encoded as the databases root encodes names (`layout::database_dir`);
   2. verifies the checksum and the chain hash;
   3. writes a sidecar holding the full identity, the attempt, the position, the chain hash and the source incarnation;
   4. fsyncs both, renames the file into place, and fsyncs the directory.

   Only then does it report the staged copy. A staged copy is never opened as a primary except by a holder after `CompleteFork`, and never served to clients. Directories for different incarnations or operations of the same name never collide.
4. **The other replicas copy from the seeder.** Each other member of the fork's replica set calls `Replication.Snapshot { database: fork, fork_incarnation, operation, fork_attempt: n, staged: true }` on the seeder. The seeder allows the call under the `seeding` binding only if:
   - its control-state view shows the same fork identity at attempt *n*;
   - the caller is in the fork's replica set;
   - it holds a staged copy whose sidecar has that identity and attempt.

   Otherwise it answers `UNAVAILABLE` with a retry-after, or `PERMISSION_DENIED` on an identity mismatch. Peers stage exactly as in point 4.3.
5. **Reports.** `DatabaseStatus` in `HeartbeatRequest` gains optional fields, which `Report` stores and which decode as absent when missing:
   - `staged { fork_incarnation, operation, attempt, position, chain_hash, source_incarnation }`;
   - `seed_failure { fork_incarnation, operation, attempt, reason, retryable }`;
   - `reset { fork_incarnation, operation, epoch, position, committed }`;
   - `seed_progress { fork_incarnation, operation, attempt, bytes, total_bytes }`, while a node streams a copy, for status only: neither `apply` nor the driver reads it.

   The control plane compares every field with the current fork identity, and ignores a report for another identity.
6. **Deleting staged copies happens only on a positive signal** (ADR-0042):
   - a placement for the same name with a different incarnation, or the same identity at a later attempt;
   - the drop of that identity, seen as ADR-0042 sees drops. `Replication.DropCopy` gains optional `incarnation` and `operation`, because a seeding fork has epoch 0 and the epoch alone can't name it;
   - the identity in `Failed`;
   - completion, when this node isn't a holder.

   A staged directory whose identity the node can't see is kept as an orphan (`orphan list`) and never deleted by absence.

### 5. Operations: IDs, status, retries, cancellation

1. **Every fork request carries an `OperationId`**: 16 random bytes chosen by the client, printed as 26 lowercase base32 characters. The CLI makes one and prints it before it calls.
2. **Retries are idempotent within the retention window, compared on the complete canonical request.**
   - The canonical request is `(name, source, resolved source_incarnation, expect_source_incarnation, replication_factor, durability, metadata, requested_by)`, recorded at first apply.
   - A `ForkDatabase` with a known operation ID changes nothing and returns the operation's current status when it comes **from the same principal** (`requested_by`) and its request equals the canonical one. The resolved source incarnation is compared against the recorded one, so a retry after the source was recreated returns the original operation's outcome, never a fork of the new source.
   - The same ID with any field different is refused with `Rejection::OperationConflict` (`ALREADY_EXISTS`).
   - The same ID from a **different principal** is refused with `Rejection::OperationNotYours` (`PERMISSION_DENIED`), revealing nothing about the operation.
   - **Deduplication lasts exactly as long as the operation record:** for the life of the fork while it is pending, and for `operation_retention` (7 days) after a final outcome. After that, the ID is unknown and a request with it is a new operation. The CLI never reuses an ID except through an explicit `--operation-id`, and the docs say an ID is valid for 7 days after its outcome.
   - A new ID for a target name that exists is refused (`DatabaseExists`).
3. **Status persists in control state.** `State::fork_operations: BTreeMap<OperationId, OperationRecord { target, target_incarnation, source, source_incarnation, request, requested_by, outcome, updated_at }>`.
   - `outcome` is `Pending { attempt }`, `Seeded`, `Ready { at, chain_hash }`, `Failed { reason }`, or `Cancelled`.
   - Records are pruned by `Tick` after `operation_retention`, and capped at `max_operations` (10,000). A new fork past the cap is refused with `RESOURCE_EXHAUSTED`. Pending operations are never pruned.
4. **`Control.GetForkOperation(operation_id)`** returns the record to the requesting principal, or to a principal with `admin` on the target. `db show <target>` shows the same.
5. **Cancellation is `db drop`** on a fork that isn't ready, which needs `admin` on the target. The source's pin is released when its stream ends: the seeding binding refuses the dropped identity, and the pin is bounded by `max_pin_ms`.

### 6. The fork-reset write and readiness

The fork inherits the source's logical change record (`_fleetring_cdc`, ADR-0024) and replicated sink positions (`_fleetring_sinks`, ADR-0028), which live in the adapter's reserved namespace inside the database. Both describe the source's history, not the fork's.

1. **The marker lives in the database, written in the same transaction as the reset.** The adapter's `fork_reset(identity)` is one transaction. It clears the change record and the sink positions, and inserts one row in the reserved namespace, `_fleetring_fork(fork_incarnation, operation, source_incarnation, at)`. Change capture doesn't record this transaction.
   - The transaction is **conditional.** Inside it, the adapter checks for a marker row with this identity, and if one exists it does nothing. Running it twice is harmless.
   - **Why in the database and not in control state:**
     - **Atomicity.** The marker commits with the reset or not at all. A control-state marker would need a second step that a crash could separate from the reset. A marker that said "done" when the reset wasn't would skip it. A missing marker after a committed reset would let the reset run again later, after clients had written, erasing legitimate change history.
     - **Durability.** The marker replicates through the WAL with the primary's write quorum. W + Q > R (ADR-0018) means any later primary's log, chosen by ADR-0022, holds every committed write, and so holds the marker if the reset committed.
     - **Locality.** The primary decides from its own copy, with no round trip.
2. **Opening.** A primary of a fork that isn't `Ready` in its view:
   - opens its copy;
   - checks for the marker for the current fork identity;
   - if it is absent, runs `fork_reset` and waits for it to commit at its write quorum;
   - then reports `reset { committed: true, epoch, position }`.

   Until its own commit index covers the marker, it serves no client:
   - `Database.*` data calls get `UNAVAILABLE` "fork not ready", with a retry-after;
   - `WatchChanges` gets the same;
   - change-stream sink pumps for the database don't start;
   - archives don't take a base snapshot.
3. **What a crash at each point does.**

   | Crash | Effect | Result |
   |---|---|---|
   | Before the reset | No marker. The next primary runs it. | The reset happens exactly once. |
   | During the reset | The transaction is atomic: either no marker, which is the case above, or committed, which is the case below. | Exactly once. |
   | After a local commit, before quorum | ADR-0022 picks the log. If the marker survived, the next primary sees it and doesn't run the reset again. If it didn't, nothing after it survived either, because no client was served. | Exactly once in the surviving history. |
   | After a quorum commit, before `MarkForkReady` | The marker is committed. The next primary sees it and reports it. | Not repeated. The driver proposes `MarkForkReady`. |

   No client write can precede the marker in any surviving history. That is the property that makes a repeated reset impossible to observe, and FR-087 checks it.
4. **The control state's `Ready`** is advisory: it is for status, for `db fork` to stop waiting, and for clients to avoid retrying blindly. It never decides whether the reset runs. A node that sees `Ready` in control state but no marker in its copy is in an impossible state; it refuses to serve and logs an error.

### 7. Limits, budgets, and admission

Every default marked *provisional* is a starting point. FR-087 measures it, and this record is amended with the measured numbers, and the reasoning, before it is accepted.

| Setting | Where | Default | What happens at the limit |
|---|---|---|---|
| `[fork] max_attempts` | control | 5, *provisional* | `Failed { RetriesExhausted }` |
| `[fork] deadline` | control | 1 h, *provisional* | `Failed { DeadlineExceeded }` |
| `[fork] attempt_timeout` | control | 30 min, *provisional* | `RetryForkSeed` |
| `[fork] seeder_timeout` | control | 2 min down, *provisional* | `RetryForkSeed`, excluding the seeder |
| `[fork] transfer_retries` | data | 3 per attempt, backoff from 1 s, *provisional* | the seeder reports a retryable failure |
| `[fork] max_pin_ms` | data (source's primary) | 10 min, *provisional* | the stream ends `ABORTED`, and the pin is released |
| `[fork] max_pin_wal_bytes` | data | 1 GiB of WAL growth while pinned, *provisional* | the same; source writes always win over a fork |
| `[fork] max_pins_per_node` / `max_pins_per_database` | data | 2 / 1, *provisional* | `RESOURCE_EXHAUSTED` with a retry-after; not an attempt failure |
| `[fork] max_seeds_per_node` | data | 2 concurrent staged downloads, *provisional* | the same |
| disk admission | data | free − reserved ≥ `total_bytes` × 1.1 + max(1 GiB, 5% of the volume), *provisional* | `seed_failure { InsufficientDisk }` for this node only |
| `[fork] operation_retention` | control | 7 days after the final outcome | the operation record is pruned; deduplication ends |
| `[fork] max_operations` | control | 10,000 records | `RESOURCE_EXHAUSTED` on new forks |
| client resends | CLI and MCP | 5 within 60 s on `UNAVAILABLE` | the error, and the operation ID to resume with |

1. **Reservations.**
   - A data node keeps an in-memory **reservation ledger.** Admitting a seed reserves `total_bytes` × 1.1 until the copy is staged (the bytes are then on disk) or abandoned. Admission compares free disk *minus* outstanding reservations, so two concurrent seeds never count the same free bytes.
   - The ledger isn't persisted: a restart discards temporary files, and the reserved bytes with them.
   - The control plane's best-effort check (point 7.3) also subtracts each node's pending seeds, using the source sizes of forks it is a replica of that are still `Seeding`.
2. **One refusal doesn't fail a fork.** A node that refuses on disk reports `seed_failure { InsufficientDisk, retryable: false }` for the identity. The driver adds it to `excluded`, and, if it was the seeder, retries with the next admitting member. The fork fails with `InsufficientDisk` only when the members of the replica set that aren't excluded are fewer than the fork's write quorum.
   - Placement fixes the replica set, so "choosing replicas that admit" means choosing the seeder and the holders among them. It never means placing the fork elsewhere.
3. **Control-plane admission, best effort.** Heartbeats gain an optional `disk_free_bytes` per node and `size_bytes` per replica. `ForkDatabase` is refused early (`RESOURCE_EXHAUSTED`) when fewer than W members of the target's replica set pass the formula after reservations. The data node's own check is authoritative.

### 8. The `fork` fleet feature

- `ForkDatabase`, `CompleteFork`, `RetryForkSeed`, `FailFork` and `MarkForkReady` need `fork` (ADR-0038, point 4). The first accepted `ForkDatabase` enables it. `ForkDatabase` also needs `metadata`, and `expiry` when it sets an expiry (ADR-0041).
- **Enabling `fork` also starts incarnation assignment** (point 1.2). Before then, `apply`'s behaviour for every existing command is byte for byte unchanged, and so is the stored state.
- New protocol fields are optional. Once `fork` is enabled, a node of the previous line can't join (`MissingFeature`), and `upgrade --rollback` refuses, naming `fork`.

### 9. Engines

- A fork's engine is its source's, and is recorded (ADR-0021).
- Forking needs a *frame-source* adapter (ADR-0016) that offers `pin_snapshot_at`, `open_follower`, and the conditional `fork_reset` with its marker. That is SQLite in 0.8.
- A self-replicating engine refuses `ForkDatabase` with `UNIMPLEMENTED` at admission until its adapter implements forking.
- Positions, chain hashes, staged copies, and the marker's encoding are opaque above the adapter.

### 10. Archives and metadata

- The fork's provenance, `forked_from`, is part of the metadata that ADR-0041 point 10 archives (proposed metadata, noted in ADR-0041). A restored database shows it as the archive's claim. It is unverified until signed heads exist (FR-074, ADR-0044 §6.6).
- An archive written before this ADR reads with no `forked_from`.
- A fork's archive starts with its own base snapshot after the fork is `Ready`. A restore of it holds the marker, so it never runs the reset again.
- Metadata comes only from the fork command. A source's description is another principal's text (ADR-0041 point 9), so it is never copied.
- `forked_from` grants nothing.

### 11. Authorization, metering, and tests

- **Authorization.**
  - `ForkDatabase` needs `fork` on the source and `admin` on the target, and runs `check_new_name` on the target. Until the `fork` action exists (FR-088), `admin` on the source stands in for it.
  - `GetForkOperation` follows point 5.4.
  - `CompleteFork`, `RetryForkSeed`, `FailFork` and `MarkForkReady` are proposed by the control node only. No RPC accepts them from a client or a data node.
  - The `seeding` binding (point 4.1) governs both `Snapshot` paths.
- **Metering.**
  - An audit line for every request and transition: the caller, the source and its incarnation, the target and its incarnation, the operation, the attempt, and the outcome.
  - Counters `fleetring_forks_total{outcome}` and `fleetring_fork_attempts_total{result}`.
  - Histograms `fleetring_fork_pin_seconds`, `fleetring_fork_seed_bytes` and `fleetring_fork_ready_seconds`.
  - A gauge of WAL bytes held by pins, and one of reserved disk.
  - No quotas.
- **Tests.** The simulation (ADR-0020) runs the recovery table of point 12 as scenarios, with point 12's invariants as history-checker properties (FR-087).

### 12. Recovery: every failure, the quorum it assumes, and what recovers it

"Reachable" means reachable from the control plane, for reports and leases, and from the relevant primary, for replication. "W" and "Q" are the fork's own (ADR-0018, ADR-0023).

| When | Event | What it assumes | What happens | Outcome |
|---|---|---|---|---|
| any time | The reply to `ForkDatabase` is lost | the client reaches the control plane again within the retention window | resend with the same ID from the same principal (point 5.2) | the same fork, never a second |
| any time | The control node restarts, or leadership changes | a control quorum (ADR-0035) | state comes back from the log; the driver recomputes | continues |
| any time | A heartbeat or its reply is lost, duplicated, or delayed | none | reports are compared with the current identity; stale ones are ignored | continues |
| any time | **The target is dropped and recreated** while old commands, `Snapshot` requests, staged reports, or directories are in flight | none | every one of them carries the old `(incarnation, operation)`; `apply` and the binding refuse them; old directories are deleted on a positive signal | the new fork is unaffected |
| Seeding, before staging | The source's primary fails over or is drained | the source keeps a reachable write quorum (its own availability) | the stream ends `UNAVAILABLE`; the seeder relocates the primary and refetches up to `transfer_retries`, from a fresh pin | continues; the new pin may be at a later position |
| Seeding, before staging | The pin expires | none | `ABORTED`; a retryable failure is reported | the next attempt, or `Failed { SourceTooLarge }` when every attempt's pin expires before `total_bytes` can move at the measured rate |
| Seeding, before staging | The seeder crashes and restarts | none | temporary files are discarded at start; it refetches | continues within the attempt |
| Seeding, before staging | The seeder is lost permanently | at least W members of the replica set that aren't excluded, eventually live | `RetryForkSeed` after `seeder_timeout`, excluding it | the next attempt; `Failed` if the budget or the deadline runs out |
| Seeding, during install | A crash between the temporary write, the rename, and the directory fsync | none | nothing is in place, or the rename is undone; nothing was reported | as above; never a partial staged copy |
| Seeding, after the seeder staged | The seeder crashes, then restarts | none | peers get `UNAVAILABLE` and retry; the seeder reports its staged copy from disk | continues |
| Seeding, after the seeder staged | The seeder is lost permanently | W matching reports exist already, or W admitting members remain for a new attempt | `CompleteFork` if W matching reports exist (a dead node's durable copy counts toward W, as an acknowledged write does); otherwise `RetryForkSeed` | completion, or the next attempt |
| Seeding | A peer refuses on disk | at least W admitting members remain | the peer is excluded; the others continue | completion, or `Failed { InsufficientDisk }` if fewer than W can admit |
| Seeding | Membership changes the replica set | eventually W members of the new set reachable | reports from non-members don't count; new members copy from the seeder | continues, or retries |
| Seeding | The source is dropped or recreated | none | the binding refuses the new incarnation; the driver sees the change and proposes `FailFork` | `Failed { SourceGone }` or `Failed { SourceRecreated }` |
| Seeding | The user drops the fork | none | gone; `Cancelled`; old messages are fenced as above | cancelled |
| Seeded, never leased | Some holders are lost | **at least one holder live and reachable**, for the first lease | that holder takes the first lease | continues to opening |
| Seeded, never leased | Every holder is lost permanently | none | W replicas lost, beyond what the durability settings tolerate, as for any database | unavailable; `db drop` and fork again; never served from an empty replica |
| Seeded, leased, opening | The primary crashes before or during the reset | a holder, or a follower holding the copy, is promotable: ADR-0022 needs Q reachable members, and W + Q > R puts a holder or a later copy among them | the new primary finds no marker and runs the reset | ready, once a write quorum is reachable |
| Seeded, leased, opening | The primary crashes after the reset committed at quorum, before `MarkForkReady` | Q reachable for promotion | the new primary finds the marker, doesn't run the reset, and reports it | ready |
| Seeded, leased, opening | A failover between completion and the reset (the lease expires, or a drain) | as above | as the two rows above, by whether the marker committed | ready, reset exactly once |
| Seeded, leased, opening | The write quorum is unreachable | — | the reset can't commit; clients keep getting `UNAVAILABLE` | ready when the quorum returns |
| Ready | anything | ADR-0018 and ADR-0022 | an ordinary database | as any database |

**Invariants** (asserted in the simulation, FR-087):

- **(a) Fencing.** No lease is issued on a fork before `Seeded`, and no client data call, `db watch`, or sink is served on a fork before its reset marker commits. A staged copy is opened as a primary only by a holder after `CompleteFork`.
- **(b) Acknowledged rows only.** Every row of a fork at `at` was quorum-acknowledged by the source's incarnation `source_incarnation` at or before `at`.
- **(c) No row from another incarnation** of the source.
- **(d) No crossing between forks.** No command, report, binding, or staged directory of one fork identity changes the state of another.
- **(e) Independence.** Writes to a fork never appear in its source, and the reverse.
- **(f) Unambiguous operations.** An operation ID maps to at most one fork identity, and every retry from its requester within retention returns the same outcome.
- **(g) Termination.** Every fork reaches `Ready`, or an explicit `Failed` or `Cancelled`, by its deadline, given the assumptions in the table. After `Seeded` the deadline no longer applies: a fork whose write quorum stays unreachable stays `Seeded`, unusable but safe, until the quorum returns or an operator drops it.
- **(h) Exactly one reset.** In every surviving history, the fork-reset write appears exactly once and before any client write.

## Alternatives considered

- **Completing on one staged replica, with a timeout for a replacement.** No timeout can make an empty replacement safe.
- **Fencing only the source.** A dropped fork's delayed messages could complete or advance its replacement.
- **Treating "seeded" as "ready".** Clients and sinks would see, and act on, the source's change record and sink positions.
- **A readiness marker in control state only.** It can't commit atomically with the reset, so a crash between the two either skips the reset or repeats it after legitimate history.
- **An unconditional reset by each new primary.** It would erase change history recorded after readiness.
- **Every replica fetching from the source.** Simpler, but the pin is held R times as long, and the source's load multiplies.
- **Identifying databases by name and creation time.** A collision within one millisecond, or a clock stepping back, breaks it.
- **Assigning incarnations from the start, without the feature gate.** A rollback that drops the field, followed by a roll forward, could reuse values.
- **A separate `CancelFork` command.** Two ways to remove one thing would invite disagreement.
- **Deleting staged copies whose identity a node can't see.** That is deletion by absence, which ADR-0042 forbids.
- **File clones, and restoring a past position from an archive.** Deferred to after 0.8. File clones come back if FR-087's and FR-094's measurements show forks of over 100 MiB taking over 10 s, or doubling disk use, on a laptop; forks at a past position, from archives, and across fleets come back on their own demand (ADR-0044 §10).

## Consequences

- Forks survive every single failure that ordinary databases survive, under the stated reachability assumptions, and fail explicitly otherwise. No partial copy is ever primary, and no client reaches a fork before its reset.
- Control state gains an incarnation per database, a counter, a fork record per fork, and bounded operation records. Heartbeats gain optional staged, failure, reset, progress, size, and free-disk fields. `SnapshotRequest` gains the fork identity, the attempt, the source incarnation, and `staged`, under the `seeding` binding; `SnapshotChunk` gains `total_bytes`; `DropCopy` gains the incarnation and the operation. On `main` on 2026-11-08 the next free field numbers are 12 in `Placement`, 3 in `SnapshotRequest`, 6 in `SnapshotChunk`, 5 in `DatabaseStatus`, and 4 in `HeartbeatRequest`.
- `Replication.Snapshot` has two bindings, `follower` or `seeding` by whether the request names a fork, so the RPC table, the proto's `Binding:` line, and the daemon README's row name both.
- The SQLite adapter's reserved namespace gains `_fleetring_fork`.
- Sources pay a bounded price: one pinned stream per attempt, for at most `max_pin_ms` and `max_pin_wal_bytes`.
- A fork's history isn't a byte-for-byte prefix of its source after `at`: the reset adds one write, which carries the marker. `db verify` at `at` matches the source; later positions don't. The docs say so.
- A fork can stay `Seeded`, unusable but safe, while its write quorum is unreachable. `db show` reports it as opening, and an operator drops it or restores the quorum.
- Staged copies of dropped forks may stay as orphans on nodes that missed the drop until FR-077 (#105) brings tombstones; `orphan list` and `orphan remove` handle them.
- Incarnations become available to ADR-0042's drop handling and to FR-077.
- ADR-0044 §7 (S2) is superseded for forks. §2.1 and §7.6 stand. ADR-0031 lists `seeding` as a proposed binding, ADR-0041 lists `forked_from` as proposed metadata, and ADR-0042 notes that staged copies follow its deletion rule; each note is dated and none rewrites accepted text.
- **Docs:** `docs/cli.md` designs `db fork` and the fork fields of `db show` and `db list`, planned for 0.8.0; the lexicon gains *incarnation*, *operation ID*, *staged copy*, the fork states, the fork identity, and the `seeding` binding; the daemon README designs the MCP tool `db_fork`.

## Open questions

None for the owner. FR-087's measurements set the provisional defaults of point 7 before acceptance.
