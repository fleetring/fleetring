---
id: FR-028
title: Control node service and client
status: todo
priority: P0
size: M
milestone: M5 Daemon foundations
depends_on: [FR-023, FR-027]
adrs: [ADR-0017, ADR-0019]
---

# FR-028: Control node service and client

## Goal

Run the control state machine as a durable service on the control node, and give data nodes a client that already copes with a leader that moves (ADR-0017).

## Scope

- **Log and snapshots:** each command is appended to a log table in the control node's SQLite database and fsynced before it is applied. A snapshot is taken every N commands. On startup, the node loads the latest snapshot and replays the rest of the log.
- **Time:** the control node stamps each command with its clock before logging it.
- **Service:** the gRPC `Control` service (FR-023) over the state machine, with all mutations serialized through one task.
- **Client:** retries with backoff, and follows a "not the leader, try X" reply. A single control node never sends one, but tests can inject it.

## Acceptance criteria

- [ ] Killing and restarting the control node at any point never loses a command it acknowledged, and replays to the same state (simulation test).
- [ ] The client follows an injected leader redirect without the caller noticing.
- [ ] Under concurrent callers, commands are applied one at a time, in log order.
