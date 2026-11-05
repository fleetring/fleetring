# ADR-0042: A data node deletes a copy only after it saw the database's drop, and keeps every other copy as an orphan

- **Status:** Proposed
- **Date:** 2026-11-05
- **Related:** ADR-0017 (control state machine), ADR-0018 (leases, epochs, quorums), ADR-0022 (failover), ADR-0025 and ADR-0026 (principals and scopes), ADR-0031 (node calls bound to placement), ADR-0038 (versions and fleet features), ADR-0040 (Raft amendments), ADR-0041 (expiry; amendment 7); tickets FR-077 (#105, part 1), FR-070 (#76)

## Context

Until now a data node deleted its copy of a database whenever the control plane stopped knowing the database:

1. **the recovery pass at start** (`data::lease::recover`) deleted each copy the control plane answered `NOT_FOUND` for, or held at a lower epoch than the copy's;
2. **a lease renewal answered `NOT_FOUND`** retired the primary's copy: flushed its change streams, then deleted it (ADR-0041, point 4);
3. **a follower whose stream ended** asked the control plane, and deleted its copy if the database was gone or at a lower epoch.

Absence isn't a drop. An empty, recreated, or older `control.db`, a control plane rolled back, a lagging Raft member (ADR-0040), or a data node pointed by mistake at another fleet under the same authority all answer the same. Each makes every primary delete its database within one renewal interval, and every follower at its next stream end or restart: every replica in the fleet, at once. ADR-0041 amendment 7 already stopped one such path (a name re-created and never leased) and left these three for #105.

#105 splits the fix (owner, 2026-11-05). Part 1, this ADR, ships in 0.6.0: data nodes stop deleting by absence, with no change to control state and no fleet feature. Part 2 adds tombstones: a positive drop record in control state, a fleet feature, that lets nodes which missed a drop delete safely and lets orphans be adopted.

Part 1 still has to delete the copies of a database an operator dropped, or that expired, or every `db drop` and every expiry would leave a copy on each replica, and a database created again under the name would be refused on those nodes. Agents create and expire scratch databases all the time (ADR-0041), so that isn't acceptable for 0.6.0 either. Some positive signal is needed now, and it can't live in control state yet.

## Decision

1. **The rule.** A data node deletes a copy only when it saw the database's drop itself. Control state that doesn't know a database, or holds the name only at a lower epoch, is never a drop.

2. **Seeing a drop: `Replication.DropCopy`.** The control node that applies a `DropDatabase` or an `ExpireDatabase` tells each node that may hold a copy, its replica set before the drop and every member that reported a copy, through the new data-node RPC `Replication.DropCopy { database, epoch }`, where `epoch` is the database's epoch when it was dropped.
   - The control service sends it after a `DropDatabase` it submitted commits, and the expiry sweep after each `ExpireDatabase`. Both read the recipients from the state before the command.
   - Notices are best effort and kept nowhere: one try, and up to four more a second apart while the node can't be reached. A node that is down or partitioned throughout gets none.
   - The scope is `node`, and the binding is the new `dropped`: the receiving node asks control state (`Locate`) and accepts the notice only if control state no longer holds the database, or holds a database of the name at a lower epoch. A notice for a database control state still holds is refused with `PERMISSION_DENIED` and a `fleetring-binding: dropped` entry, logged on `fleetringd::audit`, and counted as a binding refusal.
   - The node's lease keeper then asks control state again, since a database of the name may have been created and leased since, and deletes the copy only if its highest epoch is at most the notice's.
   - A node that serves the database stops, flushes its change streams for up to `DROP_FLUSH_LIMIT` (ADR-0041, point 4), and deletes its copy. Any other node deletes its copy at once.

3. **Every other path keeps the copy as an orphan.**
   - The recovery pass at start: the database is unknown, or held at a lower epoch.
   - A renewal answered `NOT_FOUND`: the primary stops serving at once, so it admits no write, and flushes its change streams, as for a drop. Flushing an archive is harmless if the database isn't gone. Then it keeps the copy, unless a notice arrived before or during the flush, in which case it deletes it.
   - A renewal refused because the name now belongs to a database that has never been leased (ADR-0041, amendment 7).
   - A follower whose stream ends and finds the database unknown or at a lower epoch.

4. **An orphan is closed and kept.**
   - The node stops serving it, ends any stream following it, closes it, and keeps its files and its epoch record.
   - Streams, fences, fetches, and promotions that name it are refused with `FAILED_PRECONDITION`, which names the orphan and the command that removes it. A database created again under the name therefore can't use the node as a replica until the orphan is removed: under-replicated, never mixed with the old copy.
   - It is logged at warn once, with the database, its epoch, the node, and the reason.
   - It is listed in `status --node`, and in `NodeStatus`'s new `orphans` field with its epoch and size. A caller sees an orphan if it may read a database of its name, or holds `fleet`.
   - It is counted in the gauge `fleetring_databases_orphaned`.
   - A creation or restore of the name on the node is refused with `FAILED_PRECONDITION`, naming the orphan, its epoch, and `fleetringd orphan remove <name> --node <id>`.
   - Orphans are kept in memory. The recovery pass finds them again after a restart, by the same rule.

5. **Only an operator deletes an orphan, with `fleetringd orphan remove <database> --node <id>`.** It calls the node's new `Database.RemoveOrphan`, whose scope is `fleet`. The node:
   - refuses a name it serves or follows, or is retiring, with `FAILED_PRECONDITION`, and a name it holds no copy of with `NOT_FOUND`;
   - deletes the copy's files and epoch record, and logs at warn;
   - holds the name's creation lock throughout, so no create or restore of the name races it.

   The command asks first on a terminal, and refuses elsewhere without `--yes` (`docs/cli.md`, section 7). `fleetringd orphan list` lists every node's orphans, or one node's with `--node` or `--address`, as a table or as JSON. Restarting a node no longer removes an orphan, so ADR-0041 amendment 7's way out, and its error message, point to the command instead.

6. **Compatibility.** Both RPCs are additive, and no fleet feature gates them.
   - A 0.5 data node refuses `DropCopy` as a call it doesn't serve (ADR-0041, amendment 4). The control node stops telling it, and it deletes by its own renewal and stream-end checks, as before.
   - A 0.5 control node sends no notices, so 0.6 data nodes keep the copies of what it drops as orphans. Upgrade control nodes first, as ADR-0038 already recommends.
   - `orphan remove` against a 0.5 node reports that the node keeps no orphans.

7. **Tombstones replace the notice later (#105, part 2).** A tombstone in control state, named by a fleet feature, lets a node that missed a notice, because it was down or partitioned, delete its copy when it next learns of the tombstone, and lets an operator adopt an orphan back into the fleet. Until then such copies stay orphans for the operator.

## Alternatives considered

- **Keep deleting on a renewal answered `NOT_FOUND`.** A primary would still delete its copy within one renewal interval of a control plane that lost its state, and with R = 1 that is the only copy. It leaves the main hazard in place.
- **Delete nothing until tombstones exist.** Every `db drop` and expiry would leave an orphan on each replica, and a name created again would be refused on those nodes until an operator removed them. Scratch databases make that constant.
- **Infer the drop from control-state continuity** (membership epochs, log indexes, or the set of databases a node leads disappearing at once). Each is a heuristic: a rollback to a backup taken moments earlier, or a lagging Raft member, passes it.
- **Let the client that ran `db drop` tell the replicas.** The expiry sweep has no client, and a client can die between the drop and the notices. The control node that applied the command is the one place both drops pass through.
- **Trust a notice without asking control state.** Any node certificate may make a node call, so a stolen node key could then delete every replica in the fleet. Asking control state limits a forged notice to databases control state no longer holds.

## Consequences

- No absence path deletes data any more. Losing or replacing `control.db`, rolling back, or pointing a node at the wrong fleet leaves every copy on disk, closed, listed, and counted, for an operator to recover or remove.
- `db drop` and expiry still delete every replica that is up, through the notice. A replica that is down or unreachable when the drop happens keeps an orphan. So does the stranded follower ADR-0041 amendment 7 describes, if it misses the notice. Operators watch `fleetring_databases_orphaned` and remove orphans with `orphan remove`, or copy them first to keep them.
- A database created again under a dropped name waits on any node still holding an orphan of it: it is refused there, or runs under-replicated if that node is only a follower, until the orphan is removed.
- The positive signal comes from the control node. A control plane that lost its state never sends one, which is what makes it safe. A stolen node key can forge a notice, but only for a database control state no longer holds. When control state was also lost or replaced, that can delete orphans, which the operator should then restore from archives. `SECURITY.md` says so.
- The daemon README, `SECURITY.md`, `docs/cli.md`, the lexicon ("orphan"), and `capabilities.json` describe orphans and the command. The simulation adds an invariant, *copies kept until dropped*: every node its lease watcher saw serving a database the control log never dropped still holds a copy at the end of a run.
- Part 2 (#105): tombstones, a fleet feature with a bounded retention window, adoption of orphans, and the interaction with restore under the same name and with Raft.
