# FleetRing: MVP

*fleetring.dev · github.com/fleetring/fleetring · crate `fleetring` · Oct 27, 2026*

## What it is
An open-source Rust layer that places, routes, replicates, and moves **millions of small SQLite/libSQL databases** (one per agent, tenant, or device) across machines you choose: cloud, on-prem, or old laptops at home. Think Durable Objects, self-hosted.

## Problems it solves
1. Agent platforms and SaaS teams need millions of isolated databases and hand-roll the routing.
2. Cloudflare Durable Objects solves it, but only on Cloudflare. Self-hosted, on-prem, and data-residency deployments have no equivalent.
3. Moving a live database between servers without losing writes is hard, so teams avoid rebalancing.
4. Local AI users run models on several machines with no shared, durable agent memory.
5. Self-hosted SQLite has no easy per-tenant backup and restore.
6. AI coding and evals need instant database forks per run; SQLite has no branching.

## Who it's for
- Teams building agents (coding, support, voice) that need durable, **forkable** state per agent or session.
- Multi-tenant SaaS, on-prem and sovereign deployments, edge sites, and home labs that need many isolated databases on machines they control.
- **Not for:** one app with one database, or under ~10k tenants on one box. Use SQLite or Postgres.

## Design decisions
- **Leadership per database, not per cluster.** A database's first ring node is its primary; the next N-1 are followers pulling its WAL. Leases in a small control database pick the primary. Raft for the control database comes later.
- **Borrowed traits:** consistent hashing, replication factor, hinted handoff, and repair (DynamoDB/Cassandra); per-object databases created on first use and routed by name (Durable Objects); S3-compatible snapshots; libSQL/Hrana wire compatibility (Turso).
- **General-purpose placement core.** The `fleetring` crate knows nothing about SQLite. It is published as general-purpose placement infrastructure, and the SQLite product is built on top of it (ADR-0011).
- **One daemon, two modes.** The crate stays a pure library. `fleetringd` is batteries included: `fleetring up` on a LAN uses mDNS to form a cluster, weights nodes by disk and RAM, and treats a sleeping laptop as a temporarily down node.
- **Agents connect by name:** `fleet.db("agent:sam-researcher")`. Local agents use the bundled MCP server (`memory.write`, `memory.search`, `db.query`, `db.fork`, `db.snapshot`). Cloud agents use the router over HTTPS with API keys scoped to a name prefix.
- **Client:** the Rust client embeds the ring (no extra hop). Everything else goes through the libSQL-compatible router, so existing Turso/libSQL SDKs work on day one. The client caches the ring map, follows redirects when a database moves, writes to the primary, and can opt into replica reads.

## MVP (v0.1, about 4–6 weeks), in build order
1. **Ring crate:** weighted virtual nodes, replication factor, host/zone-aware placement, bounded loads, pluggable rendezvous/vnode-ring strategy. Deterministic and heavily tested.
2. **`fleetringd`:** per-database primary plus followers, leases, heartbeats.
3. **Live migration** on join/leave (snapshot plus WAL tail, cutover) with zero lost writes; deterministic simulation tests from day one.
4. **Router plus Rust client.**
5. **Home lab mode** (`fleetring up`, mDNS) and **S3-compatible snapshots** (MinIO, R2, S3).
6. **MCP agent-memory server** plus three templates: agent memory (episodes, facts, vectors), per-tenant SaaS (migration fan-out), home lab.
7. **CLI:** `up`, `status`, `node add/remove`, `db create/locate/move/snapshot`.
8. **Demo:** 10k agents on 3 old laptops. Close a lid, nothing breaks; open it, it catches up.

**v0.2:** instant fork and time-travel restore; scheduling agents next to the machine running their model (local AI fleets); hot-spot moves; native TypeScript/Python SDKs; metrics; Raft control database; multi-region.

**Not building:** cross-database queries or distributed transactions, a custom storage engine or consensus, device sync, a dashboard in v0.1.

```
agents (MCP)    apps (libSQL SDKs)    Rust client (embedded ring)
       \               |                   /
        router (Hrana) + placement cache ─'
               |
        control db (membership, leases, db→node map)
               |
  node A    node B    node C     fleetringd: per-db primary + WAL followers
      \        |        /
     S3-compatible snapshots
```

## Risks
- **Live migration correctness.** Simulation testing from day one.
- **Scope creep** (scheduler, dashboard). Hold the v0.1 line.
