# Roadmap to "safe to use"

The placement crate `fleetring` is released (0.1.0) and its placement is frozen by spec. The daemon `fleetringd` 0.1.0 runs clusters on machines you own, with replication and failover verified in simulation. It is **not yet safe to depend on**: `SECURITY.md` and the READMEs say so. This page lists what has to be true before that changes, as epics. Each epic links its tickets, and the ticket index in `docs/tickets/README.md` is the source of truth for status.

"Safe to use" means, concretely:

1. **No known way to lose an acknowledged write**, shown by the history checker (FR-036) on every simulation run, nightly sweeps over random seeds, and a fault-injection run on real processes.
2. **Secure by default across machines:** TLS between nodes, an authenticated data plane, and a written threat model.
3. **Recoverable:** backups with point-in-time restore, and a tested rolling upgrade from the previous release.
4. **Operable:** metrics, health checks, documented limits and tuning, and a drain for planned maintenance.
5. **No single point of failure** in the control plane, or the single-node behaviour documented and bounded.
6. **Reviewed:** the protocol and the auth design reviewed by someone outside the project.

## Epics

### E1. Correctness, verified
Every simulation run checked against the guarantees each database promises, and the same checks on real processes.
- FR-036 history checker (in progress); nightly sweep of 500 seeds, with failing seeds filed as bugs.
- A Jepsen-style run on real daemons with network and process faults (a `fleetring-jepsen` harness, or Jepsen itself), before the first "safe" release.
- Fuzzing of the daemon's protocol inputs (batches, snapshots, control RPCs), next to the existing spec fuzzing.

### E2. Durability and operations
- FR-035 configurable durability: `acks`, `min_replicas`, per database (ADR-0023, proposed).
- FR-037 `db list`, `db drop`, per-node status (in progress).
- FR-038 graceful departure: drain primaries before a planned stop.
- Backups: S3-compatible snapshots plus WAL archiving for point-in-time restore (MVP step 5), as a durability sink separate from replicas (ADR-0016, point 3).
- Control log compaction and bounded snapshot retention.
- Metrics (Prometheus), health and readiness endpoints, structured logs, and a tracing span per request.

### E3. Security
- TLS between nodes with a per-cluster certificate bootstrap (`fleetring up` issues them); mutual TLS between nodes.
- An authenticated data plane: API keys scoped to name prefixes for applications, with Authem-style delegated tokens later.
- A threat model in `SECURITY.md` (what a compromised node, client, or network can and can't do), a dependency audit in CI (`cargo deny` advisories already run), and signed, provenance-attested release artifacts.
- An external review of the replication and failover protocol and of the auth design.

### E4. Scale
Today's design is sound for about a hundred nodes and tens of thousands of databases. Four changes take it to thousands of nodes and millions of databases; see `docs/review.md` for the reasoning.
- Node-level leases (one liveness record per node, as CockroachDB's epoch leases), so renewals are O(nodes), not O(databases).
- Heartbeats off the command log: liveness kept in memory on the control node, with only membership transitions logged.
- A shared SQLite thread pool instead of a thread per open database, and an incremental WAL reader instead of a rescan per commit.
- Placement cached in clients and the router, so the control node isn't on the request path.
- Benchmarks at 10,000 databases per node and 1,000 nodes in simulation, recorded in `docs/perf.md`.

### E5. Control plane availability
- Raft for the control log with openraft, three or five control nodes (ADR-0017 planned it; the command log and replay tests are ready for it).
- Leader redirects already exist in the client; data nodes keep serving through control outages already.

### E6. Movement
- Live migration (MVP step 3): move a database between nodes on purpose, with snapshot, catch-up, and cutover under a new epoch, driven by `Ring::diff` when nodes join or leave.
- A rebalance planner for hot spots, and cells for home-versus-cloud placement (ADR-0012) so quorums stay local.
- Engine-changing migrations, such as SQLite to Postgres (ADR-0021), after live migration.

### E7. Access and integration
- The router (MVP step 4): the libSQL/Hrana front door, so existing libSQL and Turso clients work unchanged. The enforcement point for auth.
- The Rust client that embeds the ring and skips the hop.
- The MCP server for agents (MVP step 6): `memory.write`, `memory.search`, `db.query`, `db.fork`.
- FR-039 change streams: a sink trait to Kafka, Pub/Sub, webhooks, or analytics stores.
- Framework adapters: a middleware that maps a request to its tenant's database (Axum/Tower first; the libSQL client covers Node and Python through the router).

### E8. Packaging, docs, and examples
- A `docker compose` three-node example, a Raspberry Pi and ARM guide, and a cloud VM guide (the demo in the private plan).
- An operator guide: configuration reference, tuning (leases, heartbeat intervals, checkpoint thresholds, open-database caps), limits, and what each failure looks like.
- An `examples/` directory for the daemon: a multi-tenant API, an agent with its own database, a change-stream consumer.
- A changelog for the daemon, and a compatibility policy for the config file, the protocol, and the on-disk formats (rolling upgrades N to N+1 tested in CI).

### E9. Release engineering
- Nightly simulation sweeps kept green for a sustained period before a release is called safe.
- Compatibility tests between the previous and current release: mixed-version clusters, old logs and snapshots.
- Chaos on a real three-node cluster in CI (kill, pause, partition with `tc`), once E1's harness exists.

## Order

E1 and E2's first three tickets are in progress or next. E6's live migration and E7's router come before E3, because the router is where auth attaches. E4 and E5 can start as soon as the ADRs for node-level leases and Raft are written, in parallel with E7. E8 and E9 run alongside everything and gate the "safe" label.
