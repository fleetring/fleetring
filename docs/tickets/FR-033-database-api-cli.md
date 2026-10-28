---
id: FR-033
title: Database API and CLI for fleetringd v0.1
status: done
priority: P1
size: M
milestone: M6 Replication and failover
depends_on: [FR-026, FR-028]
adrs: [ADR-0015, ADR-0019]
---

# FR-033: Database API and CLI for `fleetringd` v0.1

## Goal

Create databases, run SQL on their primaries, and see where they live, before the router exists (MVP step 4).

## Scope

- **`Database` gRPC service:** `Execute` and `Query` on a database's primary. A request that reaches another node is forwarded or redirected to the current primary.
- **Creation:** `CreateDatabase` places the database with the ring and opens it on its primary and followers.
- **CLI:** `fleetring db create`, `fleetring db exec`, `fleetring db locate` (primary, followers, epoch, and positions), and `fleetring status` (members, liveness, and ring fingerprint).
- Each write reports how many replicas acknowledged it and whether the database is under-replicated.

## Acceptance criteria

- [x] On a three-node local cluster, `db create`, `db exec`, and `db locate` work end to end, and `db exec` keeps working through a primary failover after a retry.
- [x] `fleetring status` shows the same ring fingerprint on every node.
- [x] A request sent to a node that isn't the primary is redirected correctly.

## Progress

- 2026-10-28: Everything but failover is in place. The first criterion's "through a primary failover after a retry" waits for FR-031, which adds promotion; the ticket stays in progress until then.
  - **Daemon:** a data role now owns a `Registry` under `<data_dir>/databases`, built from `[engine]` (inline under simulation), and closes it after the server stops. It serves the `Database` service on `listen` next to `Control`. `Daemon::registry()` is new. `StartError::Engine` reports an invalid page size.
  - **Replication hook:** `daemon::replication_sink()` returns the sink passed to `Registry::new`. Today it is `UnreplicatedSink`, which acknowledges every batch at once; FR-030 replaces it there, and replaces `data::service::ACKNOWLEDGED_BY` (1) with the count its acknowledgement reports.
  - **Peer addresses:** `[node] advertise` (default: the bound listen address, loopback for an unspecified IP) is sent as `Node.address` (proto field 5, optional) in `Join`. `fleetring_control::Member` stores it (`with_address`, `address`; serde-defaulted, so old logs and snapshots load). It isn't a placement attribute, so `to_node` ignores it, but a new address bumps the membership epoch so every node learns it. The replay generator now changes addresses too.
  - **Placements** gain `replicas` (proto field 6): the replica set in preference order with each member's address, liveness, and last reported role and position.
  - **Creation:** driven by the primary itself. The client calls `Control.CreateDatabase`, then the new `Database.Create` RPC on the first live replica in preference order. That node takes the first lease with `AcquireLease` and creates the file through its registry; repeating it on the primary is harmless, and `Client::create_database` finishes a creation that stopped halfway. Followers get copies in FR-030 and FR-032.
  - **Routing:** a node serves a database while it holds its lease, which it learns when it takes it, or after a restart from `Locate`. Other nodes answer `UNAVAILABLE` with `fleetring-primary: host:port` metadata, as control redirects do (`data::not_primary`, `data::primary_hint`). Redirecting rather than forwarding keeps a write to one hop and never doubles a lost reply.
  - **Errors:** failed statements and transaction control map to `INVALID_ARGUMENT`; an unknown database to `NOT_FOUND`; a closed database to `UNAVAILABLE`; an interrupted request or a replication failure (outcome unknown) to `ABORTED`; storage failures to `INTERNAL`.
  - **Writes** report `acknowledged_by`, `replicas` (new proto field 4), and `under_replicated`; until FR-030 that is 1, R, and R > 1.
  - **Client:** `fleetringd::data::Client` creates, locates, executes, and queries, and reads the membership (`status`). It follows up to four redirects; `with_entry` starts at a chosen node.
  - **CLI:** the binary keeps `fleetringd --config` for the daemon and adds `db create <name> [--replication-factor N]`, `db exec <name> <sql> [params...]`, `db query <name> <sql> [params...]`, `db locate <name>`, and `status`, each with `--control <addr>` or `--config <file>`. `exec` and `query` take `--node` to start at a given node. One binary keeps the one-artifact install of ADR-0015; a `fleetring` name can be a link to it later.
  - **Tests:** `tests/database.rs` runs a control daemon and three data daemons over TCP: create, exec, query, and locate end to end; redirects from a follower; error statuses; `status` against every node's fingerprint; a primary that restarts on a new port and serves its database again; and the same through the binary. The simulation adds `Operation::Create` and `Operation::Write`, `Ack::Opened` and `Ack::Written`, `History::acknowledged_writes`, the `CreateAndWrite` workload, and a test that writes under partitions and a primary crash.
  - **For FR-030:** report positions in heartbeats so `locate` shows them; count follower acknowledgements in `ExecuteResponse`; followers find peers through `Membership::view()` (members now carry addresses) or a placement's `replicas`; add the lost-write check over `History::acknowledged_writes`.
  - **For FR-031:** a node keeps serving a database once it holds its lease, without renewing it or checking expiry. Renewal, demotion when a newer epoch appears, and dropping the local lease entry (`data::node::DataNode`) belong there, as does the failover half of the first criterion.
- 2026-10-28: Acknowledgement counts are real. FR-030's replication runs in the daemon: `ExecuteResponse.acknowledged_by` is how many replicas held the write when its write quorum was reached (W or more), `replicas` is the database's replication factor (from `Placement.replication_factor`, proto field 7), and `under_replicated` says fewer than R replicas were reachable. `data::service::ACKNOWLEDGED_BY`, `daemon::replication_sink()`, and `UnreplicatedSink` are gone. A write that commits without reaching its quorum is still `ABORTED` (outcome unknown). The sim database test now expects 2 or 3 of 3, and `Cluster::finish` checks every acknowledged write against each host's copy.
- 2026-10-28: Done with FR-031. `db exec` keeps working through a primary failover after a retry: `tests/database.rs::db_exec_keeps_working_through_a_primary_failover_after_a_retry` stops the primary of a three-node TCP cluster and retries the write until the new primary acknowledges it at epoch 2, and the simulation's failover tests do the same through `data::Client::execute`. While a promotion runs, the database has no primary, and `execute` fails with `UNAVAILABLE` (failing over), which is worth retrying. `Written` gains `primary` and `epoch` (no longer `Copy`).
