---
id: FR-028
title: Control node service and client
status: done
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

- [x] Killing and restarting the control node at any point never loses a command it acknowledged, and replays to the same state (simulation test).
- [x] The client follows an injected leader redirect without the caller noticing.
- [x] Under concurrent callers, commands are applied one at a time, in log order.

## Progress

- 2026-10-28: Done.
  - **Log:** `<data_dir>/control.db` is SQLite in WAL mode with `synchronous=FULL`, with tables for metadata (the format version and genesis `ControlConfig`), commands, and snapshots, encoded as JSON. Each command is stamped, appended durably, then applied. Rejected commands are logged too, since replay recomputes every outcome. Only the newest snapshot is kept; log compaction comes later.
  - **Recovery:** load the newest snapshot, check its invariants, then replay the contiguous tail. A gap, corrupt JSON, or a broken invariant is an error, never a panic. Replay always starts from the genesis config, so changing TOML timings later can't change history.
  - **Writer:** one writer task, with a bounded mailbox, publishes each accepted state on a `watch` channel. It runs on a dedicated thread, or inline under simulation.
  - **Clock:** a `Clock` trait. `SystemClock` is the only reader of `SystemTime`, and clippy bans `SystemTime::now` and `Instant::now` everywhere else (ADR-0020). `SimClock` follows `tokio::time`.
  - **Service:** the `Control` service maps rejections to `NOT_FOUND`, `ALREADY_EXISTS`, `INVALID_ARGUMENT`, or `FAILED_PRECONDITION`, with lowercase messages. The redirect convention is `UNAVAILABLE` plus `fleetring-leader: host:port` metadata.
  - **Client:** `ControlClient` follows redirects (up to four hops) and retries only idempotent calls. `create_database` and `acquire_lease` follow redirects but aren't retried.
  - **Config:** the control address moved into a `[control]` table (`address`, `lease_duration_ms`, `failover_margin_ms`, `heartbeat_timeout_ms`, `snapshot_every`), because TOML can't have both a `control` key and a `[control]` table.
  - **Tests:** a real-TCP lifecycle with restart and recovery; redirects; 8 concurrent callers showing log order equals apply order; corrupt snapshots and damaged logs; and a turmoil test with 147 crashes over about 10,000 steps that never loses an acknowledged command.
