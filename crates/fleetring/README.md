# fleetring

**Deterministic, cross-language placement with zone-aware replicas.**

Give `fleetring` a key and it tells you which nodes hold it, in what order, and what moves when the fleet changes. The answer is bit-identical on every platform and in every language that implements the [placement spec](https://github.com/fleetring/fleetring/blob/main/docs/spec/placement-v1.md).

> **Status:** early development (`0.0.x`). APIs and placement output may change until 0.1.0 freezes placement spec v1.

## Use it for

| You run | A key is | You get |
|---|---|---|
| A database-per-tenant fleet | a database name | a primary and followers in distinct zones |
| A sharded key-value store or cache | a key or hash slot | owners with bounded hot spots and cheap resharding |
| Chat rooms, actors, game sessions | a room, actor, or match ID | one home node with deterministic failover |
| Queues and streams | a partition | balanced assignment with minimal rebalancing |
| LLM inference | a conversation | affinity to the GPU that holds its KV cache |

## Why this one

- **The same answer everywhere.** Integer-only arithmetic, a stable hash (XXH3-64), a versioned spec, and golden test vectors. A port to Go, TypeScript, or Python can prove it agrees with the Rust code.
- **Exact weights.** Weighted rendezvous hashing, the default, honors weights exactly. A weighted vnode ring is available for very large fleets.
- **Replicas that survive failures.** Replica sets spread across zones, then hosts. Filtering out a down node gives exactly the replica set of a ring built without it, so every router fails over the same way.
- **Minimal churn.** Adding, removing, or reweighting one node changes at most one replica of any key.
- **Operations built in.** Bounded loads, ring diffs for migration planning, fingerprints that catch divergent peers, and diagnostics for skewed topologies.
- **Layers when you need them.** A `Cells` layer sends each key to a cell, such as a region or a tenant pool, through directory rules or a weighted ring. The cell's own ring then places the key's replicas, and layers nest to any depth.
- **Small and fast.** No I/O, no async runtime, no unsafe code. Replica lookups don't allocate for up to 8 replicas. They take about 1.5 µs at 100 nodes, and 0.4 µs on a 1000-node vnode ring ([benchmarks](https://github.com/fleetring/fleetring/blob/main/docs/perf.md)).

## Quickstart

```rust
use fleetring::{Node, Ring, Weight};

let ring = Ring::builder()
    .node(Node::new("cache-1".parse()?).with_zone("rack-a".parse()?))
    .node(Node::new("cache-2".parse()?).with_zone("rack-b".parse()?))
    .node(
        Node::new("cache-3".parse()?)
            .with_zone("rack-c".parse()?)
            .with_weight(Weight::new(2).ok_or("zero weight")?),
    )
    .build()?;

let replicas = ring.replicas("user:42:session");
println!("primary {}, all {replicas}", replicas.primary().id());
```

## Part of FleetRing

`fleetring` is the placement core of [FleetRing](https://fleetring.dev), which places, replicates, and live-migrates SQLite databases across machines you own. The crate itself knows nothing about SQLite, so use it anywhere keys need homes. The design decisions are recorded in the [architecture decision records](https://github.com/fleetring/fleetring/tree/main/docs/adr).

## License

Dual-licensed under [MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE), at your option.
