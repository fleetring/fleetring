---
id: FR-037
title: "CLI polish: db list, db drop, status --node, clear answers from data-only nodes"
status: done
priority: P1
size: M
milestone: M7 Durability and operations
depends_on: [FR-033]
adrs: [ADR-0017, ADR-0019, ADR-0022]
---

# FR-037: CLI polish: `db list`, `db drop`, `status --node`, clear answers from data-only nodes

## Goal

Make a running cluster inspectable and tidy from the command line: see every database and where it lives, drop one for good, ask a single data node what it believes, and get a clear answer when a control call lands on a node that runs no control plane.

## Scope

- **`db list`:** a `ListDatabases` RPC on the `Control` service (additive) returning every database's name, engine, epoch, replication factor, primary, and replica set. `fleetringd db list [--node <id>]` prints a table sorted by name, `--node` keeps the databases that node is primary for, and a last line counts primaries per node.
- **`db drop <name>`:** a `DropDatabase` RPC and an `Action::DropDatabase` in `fleetring-control`, additive and replay-safe. The primary and the followers close the database and delete its files once they learn it is gone; `locate` answers `NOT_FOUND`; the name is free again.
- **`status --node <addr>`:** a `NodeStatus` RPC on the `Database` service (additive) returning the node's own membership epoch, ring fingerprint, and the databases it serves with their roles. `status --control` is unchanged.
- **Clear answers from data-only nodes:** a control call sent to a node without the control role answers `FAILED_PRECONDITION` with "this node runs no control plane; the control node is `<addr>`", from its configuration, instead of a bare `UNIMPLEMENTED`.
- **Help text:** `--control` is documented as the control node's address, not any node to ask.

## Acceptance criteria

- [x] Over real TCP, with a control daemon and three data daemons, `db list` shows every database with the right primary, and `db list --node` filters.
- [x] `db drop` removes the database from the control plane and deletes its files on all three nodes within a few seconds; writes to it answer `NOT_FOUND`; the name can be created again.
- [x] `status --node` on each data node shows the control plane's ring fingerprint.
- [x] A control call to a data node names the control node.
- [x] `fleetring-control` scenarios cover a drop: it removes the lease and the promotion, a renewal for a dropped database is rejected, and the replay tests cover the new action.

## Progress

- 2026-10-29: Done.
  - **Control state machine:** `Action::DropDatabase { name }` removes the database with its lease and any promotion, and every member's report of it, so that a snapshot taken right after the drop equals one taken after the next heartbeats. Every later command that names the database is rejected as `UnknownDatabase`. `Applied::DatabaseDropped { database, epoch }` carries the epoch the database was at. Old logs never contain the action; the generator now emits rare drops, and the property tests treat a database created again under a dropped name as a new one.
  - **Protocol (additive):** `Control.DropDatabase`, `Control.ListDatabases` (a `Placement` per database, in name order), and `Database.NodeStatus` (node ID, membership epoch, optional ring fingerprint, and `DatabaseStatus` entries as the node's heartbeats report them).
  - **Drop protocol.** Dropping is a control command at the database's current epoch; the control plane forgets the database at once, so `Locate` and `Execute` through the client answer `NOT_FOUND` from then on. The primary learns of the drop when its next `RenewLease` is refused as an unknown database (`NOT_FOUND` with the `fleetring-missing-database` metadata), which is within one renewal interval, and stops serving it. Its streams end with it, and a follower whose stream ends asks the control plane whether the database still exists (`Replication::ended_streams`, watched by the lease keeper); a node that was away makes the same check for every database it has a record of when it recovers, and also discards a copy whose recorded epoch is above the placement's, which can only belong to a past database of the same name. Every replica then runs `Replication::discard`: it stops serving as primary, ends any stream applying to the copy, closes the database through the registry, deletes its files (`Registry::remove`, which refuses requests for the database meanwhile), and forgets its epoch record, so a database created again under the name starts over at epoch 1. Safety: a stale primary never acknowledges a write after the drop plus one renewal; before that, its writes are to a database the control plane has already forgotten, which is the same window a lost lease has today.
  - **Data-only nodes** serve `control::NoControl` in the `Control` service's place, which answers every call with `FAILED_PRECONDITION` naming `[control] address`.
  - **Client:** `Client::drop_database`, `Client::list_databases`, and `Client::node_status`, with `data::NodeStatus` and `data::ServedDatabase`.
  - **CLI:** `db list [--node <id>]`, `db drop <name> [--yes]` (asks on a terminal, refuses without one unless `--yes`), and `status --node <addr>`, which needs neither `--control` nor `--config`.
  - **Tests:** `tests/database.rs` adds four TCP tests and a second binary test; `fleetring-control` adds two scenarios and extends the generator and the replay coverage check.
  - **Follow-ups:** `db drop` could carry the epoch it expects (compare-and-drop) so that a stale script never drops a database that was dropped and created again meanwhile; a `Node` service could take over `NodeStatus` and later health checks; the simulation harness has no drop operation yet (FR-036 owns it).
