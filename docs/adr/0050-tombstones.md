# ADR-0050: A data node deletes a copy only on a tombstone naming its incarnation

- **Status:** Accepted
- **Date:** 2026-11-23
- **Related:** ADR-0017 (control state machine), ADR-0018 (leases, epochs, quorums), ADR-0020 (simulation), ADR-0038 (versions and fleet features), ADR-0040 (Raft amendments), ADR-0041 (expiry; amendment 1), ADR-0042 (no deletion by absence; this is its part 2, and it amends its point 2), ADR-0046 (incarnations); tickets FR-077 (#105), FR-070 (#76), FR-064 (#67), FR-125

*Note (2026-11-25):* [ADR-0040](0040-raft-control-group-versions-and-dev-mode.md), accepted today, says who logs `SetTombstoneWindow` under FR-064's Raft group. Where points 5 and 7 say "the writer", read the leader's proposer, which uses the single writer's screen (ADR-0040 point 2). The window comes from the leader's own configuration, so a new leader whose `tombstone_window_ms` differs from the state's logs `SetTombstoneWindow` again before its next drop; the leader warns at readiness when the two differ, and the daemon README says to set the window equally on every control node (ADR-0040 point 13, decision D9; FR-128, #217, moves it into control state). Point 5's "a tombstone already kept keeps its time" makes that flapping harmless. If the group gate refuses `SetTombstoneWindow`, because a control node lacks `tombstones`, the drop records no tombstone and every replica keeps its copy, as point 7 says of a mixed fleet (ADR-0040 point 4). Point 8 holds: `ListTombstones` and heartbeat replies are answered by the leader from its committed state, with no linearizable read round, since a lagging state can only miss tombstones (ADR-0040 point 2). The text below is unchanged.

## Context

ADR-0042 shipped the first half of #105 in 0.6.0. A data node no longer deletes a copy because a renewal, an ended stream, or the recovery pass found its database unknown; it keeps the copy as an **orphan**, closed, listed in `status --node` and `orphan list`, counted in `fleetring_databases_orphaned`, and removed by `orphan remove`. It deletes a copy only after a `Replication.DropCopy` notice from the control node that applied the drop.

Four things were left:

1. **The notice is still confirmed by absence.** A node accepts one if the notice names its copy and control state "no longer holds" the database, and `NOT_FOUND` counts. So a notice that reaches a node whose control plane was emptied or replaced deletes the copy. Any node certificate can send `DropCopy` (ADR-0042, point 2), so a stolen node key, pointed at a fleet whose `control.db` was just lost, can delete every copy it can name. An integration test recorded this as intended.
2. **A node that misses the notice never learns of the drop.** Notices are best effort and kept nowhere. A replica that is down or partitioned for the few seconds they are retried keeps its copy as an orphan until an operator removes it, including every row of a database someone dropped on purpose.
3. **Stranded followers.** Followers of a database whose primary crashed before the drop have nothing to end their streams, and learn of it only at their next restart.
4. **An orphan can't come back.** After a lost control plane the orphans *are* the data. Bringing one back into the fleet is a separate design, deferred to FR-125 (point 6).

What makes a positive record possible now is ADR-0046: since 0.8.1 every database has a random 64-bit incarnation, and each copy records the incarnation it belongs to.

## Decision

1. **A tombstone.** When control state drops or expires a database, it keeps a record of it in `State`: the name, the **incarnation**, the **last epoch**, the fork operation if the database was a fork, the cause, when it was dropped, and when the record may be forgotten. `apply` writes it as part of `DropDatabase` and `ExpireDatabase`, from the command's own time.

   The incarnation is required, not optional. Checked against the code:
   - **Epochs alone can't name a copy.** A database created again under a name restarts at epoch 1, so a tombstone at epoch 3 of the old one also describes the new one's copy once it reaches epoch 3 (the case `tests/sim/recreated.rs` exercises).
   - **Drop, then create again under the name** draws a new incarnation.
   - **Restore under the same name** is a creation (`Client::restore_database_with_metadata`), so it draws one too.
   - **A fork** has its own incarnation and operation from the start.
   - **A rule keeps it so:** a new database may not take an incarnation a tombstone holds, which `check_incarnation` refuses and the control node draws again.

2. **The one deletion rule.** A data node deletes a copy only when a tombstone in control state names it: **the same random incarnation the copy records, at an epoch at least as high as the copy's highest** (`tombstone_names_copy`). A staged copy of a fork is deleted only on a tombstone naming its incarnation and its operation. Everything else keeps the copy:
   - control state that doesn't know the database, or holds the name at another epoch or incarnation;
   - a copy at a higher epoch than the tombstone's, as after a control state rolled back past a promotion;
   - a copy that records no incarnation.

   This amends ADR-0042, point 2: `DropCopy` no longer carries authority. It says *when to look*, and the node reads the tombstones from control state, under the name's lock, before it deletes anything. A forged notice can't make a tombstone, so it deletes nothing, on a healthy fleet or an emptied one.

3. **The legacy incarnation gets no tombstone.** Incarnation 0 names every database of a name made before incarnations, so a tombstone of one can't tell their copies apart, and a foreign fleet's copy of the same name and epoch would match it. Such a database's replicas keep their copies as orphans when it is dropped. No fleet in use holds one (`incarnations` has been on since 0.8.1).

4. **How nodes learn.** Two paths, and both end in the check of point 2:
   - **The notice**, as now: prompt deletion for every replica that is up.
   - **The heartbeat.** A heartbeat names the databases the node hosts, and now its other copies too (`HeartbeatRequest.unreported`, never logged): its orphans, and followed copies with no position yet, which it doesn't report. Its reply carries the tombstones of those names. A node queues each tombstone that names a copy or a staged copy it holds. So a node that was down or partitioned, and a stranded follower, deletes its copy within one heartbeat of reaching the control node.

   Nodes keep no tombstones of their own. A stale cached record can never outlive a control state that no longer holds it.

5. **Window and purge.**
   - A tombstone is kept for the **tombstone window**, `[control] tombstone_window_ms`, by default **7 days** (owner decision 1). The window is part of control state: the writer logs `SetTombstoneWindow` before a drop whenever the state's window differs from its configuration, so a replay never reads a configuration file. Changing it takes effect at the next drop, and a tombstone already kept keeps its time.
   - `Action::Tick` forgets every tombstone whose `kept_until` has passed at the command's stamp. The control node's timer submits one when a purge is due.
   - **Bounds.** At most 16 per name and 32,768 per fleet (owner decision 2). Past either, the oldest is forgotten, deterministically.
   - **Safety never depends on the window or a clock.** Forgetting a tombstone can only *keep* a copy. A node down for longer than the window, a control clock running fast, or an evicted tombstone all leave an orphan for the operator, and never delete anything.

6. **Bringing an orphan back is deferred to FR-125.** A copy the control plane lost can't be returned to the fleet by a command yet. A design for it was built and reviewed with this record and withdrawn: after a lost control plane its membership is what was lost, so no command can tell from it which other nodes hold or still serve the database, and a replica set that moves within an epoch let one variant adopt a copy beside a primary still acknowledging writes. Until FR-125, the recovery paths are a backup of the control state (`control.db`), restored onto the control node so that the orphans' databases are known again, or `fleetring archive restore` from the database's archive.

7. **A fleet feature that fails closed.** Tombstones are new control state, so they are the fleet feature `tombstones` (ADR-0038, point 4; ADR-0041, amendment 1). The writer enables it, with `SetTombstoneWindow`, just before the first drop once every member reports it.

   **This departs from the ticket's "until every member supports the feature, the old behaviour stays".** The old behaviour is deletion confirmed by absence, the hazard this record removes, so it isn't kept as a fallback. In a mixed or unsupported fleet a drop records no tombstone, and every replica **keeps** its copy as an orphan. Nobody runs an older release, so that costs nothing today.

8. **Raft (ADR-0040).** Tombstones are ordinary control state, changed only through `apply`, so they replicate with the log and appear in snapshots. A tombstone is visible only once its command is committed, so no node deletes on a drop the group could still lose. A lagging or restored control node can only be *missing* tombstones, which keeps copies. Once the control group exists, `ListTombstones` and heartbeats must be answered by the leader, as every control call is today.

## Owner decisions

Made on 2026-11-24, accepting the defaults this record proposed:

1. **The window: 7 days.** Longer than a typical repair or a long weekend, at about 150 bytes per drop (an estimate, not measured). A shorter window leaves more orphans after an outage; a longer one grows every snapshot in fleets with many scratch databases.
2. **The bounds: 16 per name, 32,768 per fleet.** About 5 MB of snapshot at worst (also an estimate), and enough for roughly 4,600 drops a day for the whole window. Eviction only leaves orphans.
3. **Adoption ships separately, as FR-125** (point 6).

## Alternatives considered

- **A tombstone keyed by name and epoch alone**, as the ticket first described. A database created again under the name reaches the same epoch, and the old tombstone would delete it.
- **Keep absence as the confirmation for `DropCopy`.** It leaves a forged or late notice able to delete a copy on an emptied control plane, the exact case this ticket exists for.
- **Keep the old rule until every member supports the feature.** See point 7.
- **Purge with a command of its own.** `Tick` already forgets what time has passed, under the same replay guarantees, and a separate command adds nothing.
- **Tombstones for the legacy incarnation, judged by exact epoch.** Two fleets under one authority can share a name and epoch, so a wrong-fleet tombstone would delete a live copy.
- **Nodes persist the tombstones they learn.** A cached tombstone could outlive the control state that held it, which is a second source of truth for a deletion.

## Consequences

- Nothing on a data node deletes a copy without a positive record of its own incarnation in control state. An emptied, restored, rolled-back, or foreign control plane can't cause a deletion, and neither can a forged notice.
- A replica that was down during a drop deletes its copy when it comes back within the window, so `db drop` now reaches every node that returns in time. After the window it keeps an orphan, as before.
- A database dropped while a member lacks the feature leaves orphans on every replica, which block creating the name again on those nodes until they are removed.
- Control state grows by one small record per drop, bounded as in point 5, and heartbeat replies carry the tombstones of the names a node reported.
- Orphans still need an operator: `orphan remove` once they are no longer wanted, and the paths of point 6 to recover one after a lost control plane.
- `docs/lexicon.md` (tombstone, tombstone window), `docs/cli.md`, the daemon README, `SECURITY.md`, and `docs/capabilities.json` describe this.
