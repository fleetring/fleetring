# ADR-0023: Configurable durability: acknowledgement modes, a replica floor, and per-database settings

- **Status:** Accepted (2026-10-29)
- **Date:** 2026-10-29
- **Related:** ADR-0016, ADR-0017, ADR-0018, ADR-0022 (failover protocol); ticket FR-035

## Context

ADR-0018 fixes the write quorum at a majority of the replica set: W = ⌊R/2⌋ + 1, with the promotion quorum Q = R − W + 1. That is a sound default, but operators need to trade latency and availability against durability, per workload, as they can in comparable systems:

| System | Durability setting | Floor on live replicas |
|---|---|---|
| Kafka | `acks` = `0`, `1`, or `all` | `min.insync.replicas` |
| Postgres | `synchronous_commit`, plus `synchronous_standby_names = 'ANY k (…)'` | the standby count |
| MySQL and Vitess | semi-sync on or off | `rpl_semi_sync_master_wait_for_slave_count` |

FleetRing's replication follows the same pattern as these systems, the PacificA design. A consensus-backed configuration manager (the control plane) issues fencing epochs, and a primary replicates its log to backups with quorum acknowledgement. ADR-0018's safety rules are Raft's under other names: epochs are terms, W + Q > R is quorum intersection, and adopting the most advanced log ranks by log epoch, then position.

A second gap showed up in FR-030 and FR-031. A primary with too few live followers commits a write locally, then reports it under-replicated with an unknown outcome. Kafka refuses such a write before appending it (`NotEnoughReplicas`). That is clearer for clients, and it leaves nothing to reconcile.

## Decision

1. **Acknowledgement mode, `acks`:**

   | Mode | W | Q | Guarantee |
   |---|---|---|---|
   | `quorum` (the default) | ⌊R/2⌋ + 1 | R − W + 1 | No acknowledged write is lost while a majority survives. |
   | `all` | R | 1 | Every replica holds every acknowledged write. Any one replica down blocks writes. |
   | `local` | 1 | — | Asynchronous: the primary acknowledges after its own fsync. A failover may lose acknowledged writes. Promotion needs a majority fenced and adopts the most advanced log among them. |

   For `quorum` and `all`, Q is always derived so that W + Q > R. Safety doesn't depend on the setting. `local` deliberately gives up that guarantee, and the setting's name and docs say so.

2. **`min_replicas`: a floor on live replicas, checked before the write.** If fewer than `min_replicas` replicas, the primary included, are reachable when a write arrives, the primary refuses it with `UNAVAILABLE` before committing anything. That is a clean failure the client can retry, not an unknown outcome. The default is W. It must satisfy W ≤ `min_replicas` ≤ R.
3. **`ack_timeout`** stays as it is: a write that reached the floor but not W in time fails as an unknown outcome (`ABORTED`), as today.
4. **Per database.** `acks` and `min_replicas` are fixed when a database is created and recorded in control state with the replication factor, through additive, replay-safe fields. A cluster-wide default comes from configuration. Changing a database's durability later is an explicit command (`ChangeDurability`) that records the new settings and applies them when the next lease is granted, so a running primary's guarantee never changes under it. The promotion that moves to the next epoch fences under the settings the current epoch's writes were acknowledged with, so every write it acknowledged reaches a fenced member; the new settings govern writes from the new epoch on.
5. **Readiness.** A node that starts serving a database (after create, restart, or promotion) waits, bounded by `ack_timeout`, for every live replica to connect, then serves only if the floor is met.

## Alternatives considered

- **A Raft group per database (Multi-Raft, as in CockroachDB and TiKV).** It is proven at scale, with coalesced heartbeats and quiescent idle groups. But it would rewrite the replication layer, and it would give up physical WAL shipping, byte-identical mirrors, and engine-neutral adapters (ADR-0016). The safety argument we use is the same as Raft's.
- **A dynamic in-sync set (Kafka's ISR).** Shrinking W to the replicas that are keeping up preserves availability under `all`. But membership in the set must then itself be agreed through the control plane on every change. It is a possible later refinement of `all`.
- **No configuration.** Simpler, but operators couldn't trade latency for durability, which every comparable system allows.

## Consequences

- Writes below the floor fail fast and cleanly, instead of committing locally with an unknown outcome. Refusing before commit needs a pre-commit hook in the database actor: the actor asks the replication sink to admit each write before the engine sees it, and a refusal is `UNAVAILABLE` with nothing committed.
- `local` makes writes as fast as a single fsync, at the cost of possible loss on failover. Tests must check that loss happens only in that mode.
- The control plane, the protocol, and `db create` gain durability fields. `db locate` shows them.
- The simulation history checker (FR-036) must know each database's mode, to assert the right guarantee for it.
