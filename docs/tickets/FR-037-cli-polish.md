---
id: FR-037
title: "CLI: list databases, per-node status, and clear answers from data-only nodes"
status: todo
priority: P2
size: S
milestone: M7 Durability and operations
depends_on: [FR-033]
adrs: [ADR-0015, ADR-0019]
---

# FR-037: CLI: list databases, per-node status, and clear answers from data-only nodes

## Goal

Make the first hands-on session with a cluster self-explanatory. These came out of running the 0.1.0 tutorial on a three-node cluster.

## Scope

- **`fleetringd db list`:** every database the control plane knows, with its engine, epoch, primary, and replication factor. Sorted by name, with a `--node <id>` filter for the databases a node is primary for, so `for i in …; do db locate; done | grep | sort | uniq -c` stops being the way to see how primaries are spread.
- **Per-node status:** `fleetringd status --node <addr>` asks a data node for its own view: the membership epoch and ring fingerprint it has published, so an operator can confirm every node agrees with the control plane. Needs a small RPC on the data node's service.
- **Clear answers from data-only nodes:** a control call sent to a data-only node (today `status --control 127.0.0.1:7401` says "Operation is not implemented or not supported") answers `FAILED_PRECONDITION` with "this node runs no control plane; the control node is <addr>", taken from the node's configuration.
- **Help text:** `--control` is documented as the control node's address, not the node to ask.

## Acceptance criteria

- [ ] `db list` on a cluster with 50 databases shows all 50 with their primaries, and `db list --node node-2` shows only node-2's.
- [ ] `status --node 127.0.0.1:7401` shows the same epoch and fingerprint as `status --control 127.0.0.1:7400`.
- [ ] `status --control <data node>` names the control node in its error.
