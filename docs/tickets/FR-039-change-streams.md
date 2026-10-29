---
id: FR-039
title: Change streams: a sink trait that sends each database's changes elsewhere
status: todo
priority: P2
size: L
milestone: M8 Access and integration
depends_on: [FR-030]
adrs: [ADR-0016]
---

# FR-039: Change streams: a sink trait that sends each database's changes elsewhere

## Goal

Let an operator fan a database's committed changes out to other systems, such as Kafka, Google Pub/Sub, a webhook, an analytics store, or another FleetRing cluster, through a pluggable sink, without touching replication or placement.

## Scope

- **A `ChangeSink` trait** in `fleetringd`: `publish(database, change) -> Future<Result<Ack>>`, fed from the same committed-batch stream the replication primary uses, after the batch is acknowledged (so sinks never see unacknowledged writes). Sinks run beside replication, never in its acknowledgement path.
- **Two shapes of change:** physical (the opaque batch, for a mirror or an archive) and logical (row changes, decoded from SQLite's session extension or by diffing pages; decide in design). Logical is what Kafka consumers want.
- **Built-in sinks:** a file or directory sink (for tests and archives), and a webhook sink. Kafka and Pub/Sub as separate feature-gated crates or plugins, so the daemon's default build has no cloud SDKs.
- **Delivery semantics:** at-least-once with per-database positions, so a restarted sink resumes from its last acknowledged position. Positions are the engine-defined opaque positions (ADR-0016).
- **Configuration per database or per name prefix**, so `tenant:*` can stream to one topic and `agent:*` to another.
- **Not in scope:** exactly-once, transformations, and schema registries.

## Acceptance criteria

- [ ] A database's writes arrive at a file sink in commit order, each exactly once after a clean run and at least once across a sink restart.
- [ ] A slow or failing sink never delays a write's acknowledgement.
- [ ] A webhook sink delivers logical row changes with the database name, position, and epoch.
