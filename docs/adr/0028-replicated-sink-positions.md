# ADR-0028: Replicated sink positions live in the database, written with the next write after a delivery

- **Status:** Proposed (2026-11-02)
- **Date:** 2026-11-02
- **Related:** ADR-0016 (engine adapters), ADR-0017 (control state machine), ADR-0018 (leases and quorums), ADR-0022 (failover), ADR-0023 (durability modes), ADR-0024 (logical change capture); tickets FR-039, FR-040, FR-041, FR-049

## Context

A change sink's delivered position for a database lives on the primary alone, in `<data_dir>/changes/<sink>/<database>` (FR-039). After a failover the new primary has no such file, or an old one, and its pumps start from its log's beginning: the sink receives again everything recorded since the last checkpoint, up to the 4 MiB checkpoint threshold by default. Consumers dedupe by position, but a sink that feeds a mirror, an archive (FR-049), or a webhook without idempotence pays for every repeat. rqlite's change data capture solves the same problem with a high-water mark replicated through its Raft log, so a new leader resumes where the old one stopped.

FR-041 lays out two places for the position:

- **Control state:** a `SinkDelivered { database, sink, position }` command through `apply`, batched so that a busy stream doesn't flood the control log.
- **The database:** a `_fleetring_sinks` table beside `_fleetring_cdc` (ADR-0024), written by the primary so that it replicates with the data, folded into the next user transaction the way pruning is.

The choice has to hold for every engine: positions are opaque above the adapter (ADR-0016), and a self-replicating engine such as Turso (FR-040) ships no frames. It also has to keep `LogicalChanges::release` from pruning rows that a sink the old primary was feeding hasn't received.

What the two options cost, measured against the system as it is:

| | Control state | In the database |
|---|---|---|
| Write path | none | one row, usually one page, on the next write transaction after a delivery |
| Control log | one command per flush per database with a sink, or a field on `RenewLease` | none |
| Lag of the recorded position | the flush or renewal interval (2 s by default), many batches | until the next write after a delivery, usually one batch |
| `acks = local` | a recorded position can name a point of a log that a failover discarded, and the new log can reach the same bytes with different content: a silent gap unless the chain hash is checked too | the mark is in the log it describes, so it is lost exactly when the delivered writes are |
| Engines | nothing to implement | each adapter stores the marks; one that can't falls back to local positions |
| Scale direction | adds per-database traffic to the command log that roadmap epic E4 is removing | scales with the data, like replication |

## Decision

1. **A sink's delivered position for a database is a mark inside the database.** The engine adapter keeps one mark per sink name: the last position that sink has been delivered, as opaque bytes. Marks are written inside the database, so they commit, replicate, and are fenced exactly as the user's rows are, and a promoted follower reads them from its own copy. The control state doesn't change.

2. **A mark rides on the next write transaction after the delivery.** After a pump delivers a batch it *stages* the batch's end as its sink's mark (`SinkMarks::stage`). The adapter keeps staged marks in memory, newest per sink, and writes them at the start of the next write transaction, before the user's statements and before any prune, in the same transaction, so that the user's write, the marks, and the prune commit or roll back together. A rolled-back transaction keeps the marks staged. No transaction is ever started just to write a mark: such a write would be a new batch, whose delivery would stage a new mark, and so on forever. The cost is one small row update per sink, usually one more page in the log, on a transaction that follows a delivery; `docs/perf.md` records it beside the capture costs.

3. **For SQLite, the marks are the reserved table `_fleetring_sinks`.** `CREATE TABLE IF NOT EXISTS _fleetring_sinks (sink TEXT PRIMARY KEY, position BLOB NOT NULL) STRICT, WITHOUT ROWID`, created by the first write that carries a mark, upserted by sink, and read whole. The `_fleetring_` prefix is already reserved (ADR-0024 point 4), so user statements can't read or forge it, and capture triggers never cover it. The table exists in a database some sink names, logical or physical, and nowhere else.

4. **Through an engine-neutral capability.** `Database::sink_marks()` returns a `SinkMarks` recorder, with `marks()` to read every mark and `stage(sink, position)` to stage one, or `None` when the engine can't keep marks. The trait sees only sink names and opaque positions. A Turso adapter (FR-040) keeps the same table in its database, which Turso's own replication carries to the new primary; a Postgres adapter would keep a table too. An engine without the capability keeps today's per-node file, and `status --node` reports each sink's position as `replicated` or `local`, so the difference is visible.

5. **On promotion, and on every pump start, the replicated mark wins.** A pump reads its sink's mark from the copy it now serves. With a mark, it starts from there; without one (the sink is new, or no write followed its first delivery), it starts from the log's beginning, as today. The per-node file is no longer read for a database whose engine keeps marks. If the log doesn't continue from the mark, because the log restarted past it after the lag limit, the pump restarts from the log's beginning and counts a reset, exactly as it does now with a local position.

6. **What a failover repeats, exactly.** The new primary's log holds every transaction the write quorum acknowledged (ADR-0018), and a pump only ever delivers what the write quorum holds (FR-039). So the newest mark in the new primary's copy is the newest mark whose carrying transaction was acknowledged, and it names a position on the same log, never past its end. A failover repeats only what the old primary delivered after that mark: with writes flowing, the last delivery, and the one before it when the next write was already running as the mark was staged; with no write after the last delivery, that delivery. Never a gap: a mark is staged only after the sink took the batch. In `acks = local`, a failover can lose acknowledged writes together with the marks that came after them, so the stream repeats from an older mark and never skips, while the sink may already hold writes the new log lacks, which is the loss `local` accepts (ADR-0023).

7. **Pruning follows the marks without a second rule.** `LogicalChanges::release` keeps its contract: it may forget rows up to the lowest position every logical pump of the database has delivered. Pumps now start from the replicated marks, so a new primary's lowest position is never past a mark a sink was fed to, and its first release can't drop rows that sink still needs. A pump stages its mark before it asks for a release, and the adapter writes staged marks before it prunes, in the same transaction, so no copy ever holds a prune without the marks that justify it. Sink configuration must be the same on every node, as `[[principal]]` must (ADR-0026): a sink configured only on the old primary isn't fed by the new one, and its rows are pruned under the sinks that are.

8. **Exactly-once stays out of scope.** Delivery is still at least once; this decision only bounds what a failover repeats.

## Alternatives considered

- **Control state, through a `SinkDelivered` command.** Engine-neutral with no adapter work, and the position survives the loss of every replica's data. It adds a logged command per database with a sink per flush, which is the per-database control traffic roadmap epic E4 is removing; between flushes it repeats many batches after a failover; and in `acks = local` the recorded position can point into a log that no longer exists, so a resume would need the chain hash too to avoid a gap.
- **Control state, on `RenewLease`.** No new command, and the holder and epoch checks come free. Its lag is the renewal interval (2 s), and it ties sink positions to per-database renewals, which E4's node-level leases replace.
- **A standalone transaction for each mark.** The bound on repeats would be one delivery, but every delivery would create a batch to deliver, a loop that only a special case for mark-only batches could break, and each mark would cost a commit and an fsync of its own.
- **Gating each delivery on the previous mark being acknowledged.** It would make the bound exactly one delivery, but the last writes of a burst would wait in the log until the next write arrived to carry the mark.
- **Keeping positions per node, as now.** Simple, and a failover repeats up to a checkpoint's worth of changes, which is what FR-041 exists to stop.

## Consequences

- `fleetringd::engine` gains the `SinkMarks` capability, implemented by the SQLite adapter; the actor gains two requests, and the pump chooses where its position lives. The control crate and the protocol don't change, apart from one additive status field.
- Writes that follow a delivery carry one more row, usually one more page. A database whose streams are idle pays nothing.
- A promoted primary, or a node that serves a database again after its data directory was lost, resumes each stream from the mark in its copy. A repeat after a failover is bounded by what was delivered after the newest acknowledged mark.
- FR-040's Turso adapter implements `SinkMarks`; until then a Turso database's streams keep local positions and say so.
- FR-049's archive is a physical sink, so it inherits the marks: a new primary continues the archive where the old one stopped.
- ADR-0024's last consequence, "sink positions stay per node for now", is settled by this record.
