# Engineering review: where FleetRing stands, and what to change before it scales

*2026-10-29, after `fleetring` 0.1.0 and `fleetringd` 0.1.0. A snapshot, to be revised as the roadmap (`docs/roadmap.md`) moves.*

## What exists and what it has been shown to do

| Layer | State | Evidence |
|---|---|---|
| Placement (`fleetring`) | released, spec frozen | golden vectors on every CI target, a Python reference, fuzzing, allocation-free lookups |
| Membership and ring publication | works | simulation: convergence within two heartbeat intervals after joins, partitions, crashes, control outages |
| Control plane | one node, durable command log, Raft-ready | replay and snapshot-restore tests; 147 crash-restart cycles in simulation without losing a command |
| Replication | WAL shipping with quorum acknowledgement | followers byte-identical after checkpoints; held-back acks prove writes wait for W |
| Failover | leases, epochs, fencing, log adoption | simulation across crashes, partitions, control outages, flapping: no acknowledged write lost, one primary per epoch |
| Catch-up | pinned, checksummed snapshots; re-seed | cut, corrupted, and held transfers never leave a partial copy |
| CLI | create, exec, query, locate, status | a three-node cluster on one laptop, with a failover and a rejoin, by hand |

## Known bottlenecks and gaps

Each item says where it bites, with the ticket or epic that addresses it.

### On the control node
1. **Every heartbeat is a logged, fsynced command.** At 100 nodes sending one heartbeat a second that is 100 fsyncs a second and a log that grows forever; at 10,000 nodes it is not workable. Kafka's controller keeps broker liveness in memory and logs only registration and fencing changes; CockroachDB keeps one liveness record per node. *Change:* keep heartbeats in memory on the control node, and log only the transitions (member up, member down) as commands the control node issues itself. Replay then reproduces the decisions, not the traffic, and `apply` stays pure. This amends ADR-0017's "every change is a command", so it needs an ADR. (E4)
2. **Lease renewal is per database.** A primary renews each of its databases every `lease_renewal_ms`, and the failover timer scans every database every tick. With a million databases that is a million renewals per interval. CockroachDB solved exactly this with epoch-based leases: one liveness record per node, and every lease the node holds is valid while the node's epoch is. *Change:* node-level leases, with database leases derived from the holder's liveness epoch; promotions still fence per database. (E4, with an ADR)
3. **The control node is a single point** for membership changes, new databases, and failovers, though not for serving. Raft with openraft is the planned fix, and the log, replay tests, and client redirects were built for it. (E5)
4. **The control log is never compacted,** and each accepted command clones the full `State` for publication. Fine at today's sizes; both need bounding before millions of databases. (E2)
5. **Clients ask the control node for the primary on every call** unless they were given an entry node. The router and the Rust client must cache placement and follow redirects, so the control node is off the request path. (E4, E7)

### On data nodes
6. **A thread per open database,** capped at 256 open per node with LRU eviction. Thousands of hot databases per node means thousands of threads. *Change:* a shared pool of SQLite threads with per-database serialization, which is how servers that host many SQLite files (Turso, LiteFS) do it. (E4)
7. **Capture rescans the WAL on every commit.** Cost grows with the WAL between checkpoints, bounded today by the checkpoint threshold. An incremental reader in `fleetring-wal` removes it. (E4)
8. **A long snapshot transfer holds off checkpoints,** so the primary's WAL grows meanwhile, and transfers restart from zero when interrupted. (E2)
9. **Promotion catch-up has no snapshot fallback:** if a candidate can't be served from a peer's log, the promotion stalls until the next attempt. (E2)
10. **Writes below the quorum commit locally, then fail with an unknown outcome.** Kafka refuses before appending (`NotEnoughReplicas`). FR-035's `min_replicas` fixes this. (E2)

### Across the network
11. **No TLS and no authentication.** Fine on one machine or a private network, and nowhere else. (E3)
12. **Reconnect churn under load** exhausted local ports once in stress runs; heartbeats now redial only after three consecutive timeouts, but every client loop should reuse connections and back off. (E4)
13. **A planned stop still fails over the slow way:** the node announces its leave, but its leases expire before anyone else serves. Lease transfer on drain is the standard answer. (FR-038)

## Industry standards: what we adopted, and what we deliberately didn't

| Problem | Common answer | FleetRing | Verdict |
|---|---|---|---|
| Who may write, and no lost acknowledged write | Raft per shard (CockroachDB, TiKV); or a consensus-backed configuration manager plus primary/backup with fencing epochs (PacificA, Kafka's controller + ISR, semi-sync MySQL/Vitess, Patroni) | the second: control plane issues epochs, W + Q > R, log adoption by (epoch, position) | **Keep.** Raft per database costs a log, heartbeats, and elections per database; millions of mostly idle databases make that the wrong fit. The safety argument is Raft's. Revisit only if mass-failover latency or control-plane throughput becomes the limit. |
| Durability knobs | `acks`, `min.insync.replicas` (Kafka), `synchronous_standby_names` (Postgres) | fixed majority today | **Adopt** (FR-035, ADR-0023). |
| Liveness at scale | in-memory heartbeats at the controller, or gossip (SWIM, as in Consul and Cassandra) | logged heartbeats | **Change** to in-memory liveness with logged transitions. Gossip only if the control node's heartbeat load becomes the limit past thousands of nodes. |
| Leases at scale | epoch-based node leases (CockroachDB) | per-database renewals | **Adopt** node-level leases. |
| Planned maintenance | lease or leadership transfer on drain | leave, then expire | **Adopt** (FR-038). |
| Replication transport | physical log shipping (LiteFS, Postgres streaming) | WAL shipping with byte-identical mirrors | **Keep.** Byte-identical followers make verification and anti-entropy cheap. Engine adapters keep the door open for engines that replicate themselves. |
| Verification | Jepsen-style history checking; deterministic simulation (FoundationDB) | simulation with invariant checks; the history checker in progress | **Adopt both**, plus a run on real processes. |
| Control-plane HA | Raft (etcd, KRaft) | single node, Raft-ready | **Adopt** Raft when E5 starts. |
| Membership for very large fleets | gossip plus a small consensus group | control node only | **Defer.** Fine to about a thousand nodes with in-memory liveness. |

## 100 nodes or millions of databases: what changes, and where the seam is

The design choices that depend on scale are kept behind traits or configuration, so the near-term product doesn't pay for the large case, and the large case doesn't need a rewrite.

| Concern | Up to ~100 nodes, ~10⁴ databases | ~10³ nodes, ~10⁶ databases | Seam |
|---|---|---|---|
| Liveness | logged heartbeats (today) | in-memory liveness, logged transitions | control node internals; `apply` unchanged |
| Leases | per database | per node, derived per database | `fleetring-control` commands; the lease keeper |
| Control plane | one node | Raft group of 3–5 | the command log and client redirects (built for it) |
| SQLite execution | thread per open database | shared pool, per-database serialization | the engine adapter's execution mode |
| Placement lookups | ask control, follow redirects | ring embedded in clients and the router | the Rust client and the router |
| Snapshots and catch-up | pinned file copy | the same, resumable, with throttling | `replication/snapshot.rs` |
| Change fan-out | none | `ChangeSink` plugins (FR-039) | the sink trait |
| Engines | SQLite | SQLite, libSQL/Turso, Postgres self-replicating | `DatabaseEngine` (ADR-0016) |

The rule for new work: pick the simple answer for the near target, and put the choice behind a trait or a setting with the large-scale answer named in the ADR, so the switch is a module, not a redesign.

## What "enterprise-grade" adds beyond scale

- Security by default across machines (TLS, authenticated data plane, a threat model), and an outside review.
- Backups with point-in-time restore, tested rolling upgrades, and compatibility policies for the config, the protocol, and on-disk formats.
- Observability: metrics, health endpoints, tracing, and documented failure signatures.
- A verification story a customer can read: the spec, the simulation, the history checker, the sweeps, and the Jepsen-style run.

These are epics E1, E3, E8, and E9 in the roadmap.
