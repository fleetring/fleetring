# Roadmap to "safe to use"

The placement crate `fleetring` is released (0.1.0) and its placement is frozen by spec. The daemon `fleetringd` has five releases, and 0.5.0 is the latest. 0.1.0 runs fleets on machines you own, with replication and failover verified in simulation; 0.2.0 adds mutual TLS, scopes and tokens, drain, change streams, and signed releases; 0.3.0 adds archives with point-in-time restore and the fleet vocabulary; 0.4.0 adds metrics and health checks, verified upgrades, and a tested rolling upgrade; 0.5.0 adds `fleetringd dev`, a one-command local node, and webhook sinks over HTTPS. It is still **not yet safe to depend on**: `SECURITY.md` and the READMEs say so. This page lists what has to be true before that changes, as epics. Each epic links its tickets. Status lives in [GitHub Issues](https://github.com/fleetring/fleetring/issues); `docs/tickets/README.md` archives the tickets finished before the move, and `docs/plans/` holds the implementation plans.

"Safe to use" means, concretely:

1. **No known way to lose an acknowledged write**, shown by the history checker (FR-036) on every simulation run, nightly sweeps over random seeds, and a fault-injection run on real processes. *Progress: the checker and the nightly sweep are in place; fault injection on real processes (FR-065) is not started.*
2. **Secure by default across machines:** TLS between nodes, an authenticated data plane, and a written threat model. *Progress: done in 0.2.0, except the external review (criterion 6).*
3. **Recoverable:** backups with point-in-time restore, and a tested rolling upgrade from the previous release. *Progress: backups and restore done in 0.3.0; the tested rolling upgrade (FR-054) done in 0.4.0.*
4. **Operable:** metrics, health checks, documented limits and tuning, and a drain for planned maintenance. *Progress: the drain is done in 0.2.0; metrics, health and readiness (FR-063) done in 0.4.0; limits and tuning are not written yet.*
5. **No single point of failure** in the control plane, or the single-node behaviour documented and bounded. *Progress: not started (FR-064).*
6. **Reviewed:** the protocol and the auth design reviewed by someone outside the project. *Progress: not started.*

## Epics

### E1. Correctness, verified
Every simulation run checked against the guarantees each database promises, and the same checks on real processes.
- FR-036 history checker (done); nightly sweep of 500 seeds, with failing seeds filed as bugs.
- [FR-065](https://github.com/fleetring/fleetring/issues/68): a Jepsen-style run on real daemons before the first "safe" release. An in-repo harness, `fleetring-chaos`, runs five daemons in containers with partitions, kills, pauses, and clock skew, and judges each run with FR-036's checker, nightly (ADR-0036, accepted; [plan](plans/FR-065-fault-injection-harness.md)).
- Fuzzing of the daemon's protocol inputs (batches, database snapshots, control RPCs), next to the existing spec fuzzing.

### E2. Durability and operations
- FR-035 configurable durability: `acks`, `min_replicas`, per database (ADR-0023; done in 0.2.0).
- FR-037 `db list`, `db drop`, per-node status (done in 0.2.0).
- FR-038: drain a node's primaries before it leaves on a planned stop (done in 0.2.0).
- [FR-049](https://github.com/fleetring/fleetring/issues/52) backups: S3-compatible base snapshots plus log archiving for point-in-time restore (MVP step 5), as an archive, separate from replicas (ADR-0016, point 3, which calls it a durability sink; ADR-0029; [plan](plans/FR-049-s3-durability-sink.md)). Archives write verifiable segments and base snapshots to S3 or a directory; `db restore` builds a new database from one, and `archive restore` a plain SQLite file with no fleet (done in 0.3.0).
- Control log compaction and bounded control snapshot retention, in FR-064 (E5).
- [FR-063](https://github.com/fleetring/fleetring/issues/66): metrics (Prometheus) on a separate admin listener, health and readiness endpoints, JSON logs, and a tracing span per request ([ADR-0034](adr/0034-metrics-health-and-logs.md), accepted; [plan](plans/FR-063-metrics-health-readiness.md)). The admin listener serves `/metrics`, `/healthz`, and `/readyz`, loopback by default and TLS with listed scrapers elsewhere; every request runs in an `rpc` span with a request ID that travels on the calls it causes (done in 0.4.0).

### E3. Security
- FR-046 mutual TLS between nodes and clients, with a built-in certificate authority (`fleetringd cert`), certificates reloaded without a restart (ADR-0025; done in 0.2.0).
- FR-047 principals and scopes: prefix rules with four actions and default deny, client certificates and signed expiring tokens, enforced at every RPC (ADR-0025; done in 0.2.0).
- FR-048 a threat model in `SECURITY.md`, a dependency audit that fails releases, and keyless-signed, attested release artifacts (done in 0.2.0).
- FR-050 node calls bound to placement (ADR-0031): a stolen node key reaches only the databases placed on that node (done in 0.2.0).
- An external review of the replication and failover protocol and of the auth design.

### E4. Scale
Today's design is sound for about a hundred nodes and tens of thousands of databases. Four changes take it to thousands of nodes and millions of databases; see `docs/review.md` for the reasoning.
- Node-level leases (one liveness record per node, as CockroachDB's epoch leases), so renewals are O(nodes), not O(databases).
- Heartbeats off the command log: liveness kept in memory on the control node, with only membership transitions logged.
- A shared SQLite thread pool instead of a thread per open database, and an incremental WAL reader instead of a rescan per commit.
- Placement cached in clients and the router, so the control node isn't on the request path.
- Benchmarks at 10,000 databases per node and 1,000 nodes in simulation, recorded in `docs/perf.md`.

### E5. Control plane availability
- [FR-064](https://github.com/fleetring/fleetring/issues/67): Raft for the control log with openraft, three or five control nodes, with control snapshots, log compaction, and an upgrade from one control node (ADR-0017 planned it; ADR-0035, accepted, decides what it left open; [plan](plans/FR-064-raft-control-plane.md)).
- Leader redirects already exist in the client; data nodes keep serving through control outages already.

### E6. Movement
- Live migration (MVP step 3): move a database between nodes on purpose, with a database snapshot, catch-up, and cutover under a new epoch, driven by `Ring::diff` when nodes join or leave.
- A rebalance planner for hot spots, and cells for home-versus-cloud placement (ADR-0012) so quorums stay local.
- Engine-changing migrations, such as SQLite to Postgres (ADR-0021), after live migration.

### E7. Access and integration
- [FR-066](https://github.com/fleetring/fleetring/issues/69): the router (MVP step 4), the libSQL/Hrana front door, so existing libSQL and Turso clients work unchanged. The first enforcement point for auth, forwarding each caller's token (ADR-0037, accepted; [plan](plans/FR-066-router.md)). FR-061, the Rails adapter ([#63](https://github.com/fleetring/fleetring/issues/63)), waits for it.
- The Rust client that embeds the ring and skips the hop.
- The MCP server for agents (MVP step 6): `memory.write`, `memory.search`, `db.query`, `db.fork`.
- FR-039 change streams: a sink trait to Kafka, Pub/Sub, webhooks, or analytics stores. File and webhook sinks are done (0.2.0), streams resume after a failover (FR-041, 0.3.0), and webhooks speak HTTPS with optional mutual TLS and a bearer token (FR-073, 0.5.0); further sinks are open.
- Framework adapters: a middleware that maps a request to its tenant's database (Axum/Tower first; the libSQL client covers Node and Python through the router).

### E8. Packaging, docs, and examples
- A `docker compose` three-node example, a Raspberry Pi and ARM guide, and a cloud VM guide (the demo in the private plan).
- An operator guide: configuration reference, tuning (leases, heartbeat intervals, checkpoint thresholds, open-database caps), limits, and what each failure looks like.
- An `examples/` directory for the daemon: a multi-tenant API, an agent with its own database, a change-stream consumer.
- A changelog for the daemon, and a compatibility policy for the config file, the protocol, and the on-disk formats (rolling upgrades N to N+1 tested in CI).
- [FR-054](https://github.com/fleetring/fleetring/issues/57): `fleetringd upgrade`, a cosign-verified self-upgrade with rollback and a restart in place; the version compatibility policy (two adjacent release lines, members report their software, new control commands gated as fleet features; ADR-0038); and a nightly rolling upgrade from the previous release ([plan](plans/FR-054-upgrade.md); done in 0.4.0).

### E9. Release engineering
- Nightly simulation sweeps kept green for a sustained period before a release is called safe.
- Compatibility tests between the previous and current release: mixed-version fleets, old control logs and control snapshots.
- Chaos on a real fleet in CI: FR-065's nightly job (kill, pause, partition with `iptables`, clock skew), never on pull requests.

## Order

E1's history checker, E2's durability, drain, and backups, and E3's authentication are done. FR-063 (metrics and health) is done, and so is FR-054 (`fleetringd upgrade` and the tested rolling upgrade). E3 moved ahead of E6 and E7 because node identity gates the worker and keyed-lease designs (FR-042, FR-043), and the router attaches to the principal model rather than the other way round; its external review is still open. E5 (FR-064) can start now that ADR-0035 is accepted, and E4 once the ADR for node-level leases is written, in parallel with E7 (FR-066). FR-063 and FR-065 gate the "safe" label and depend on neither. E8 and E9 run alongside everything and gate the "safe" label.
