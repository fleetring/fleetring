//! Deterministic, cross-language placement with zone-aware replicas.
//!
//! Every distributed system has to answer the same question: given a key, which nodes hold it,
//! in what order, and what moves when the fleet changes? `fleetring` answers it for any kind of
//! key: databases, tenants, chat rooms, actors, cache shards, game sessions, or LLM
//! conversations pinned to the GPU that holds their KV cache.
//!
//! - **The same answer everywhere.** Placement is bit-identical across platforms, Rust
//!   releases, and languages. Integer-only arithmetic, a stable hash, and a versioned placement
//!   spec with golden vectors make it so.
//! - **Weighted and topology-aware.** Weighted rendezvous hashing (the default) honors weights
//!   exactly; a weighted vnode ring suits very large fleets. Either way, replicas spread across
//!   zones first, then hosts.
//! - **Minimal churn.** Adding, removing, or reweighting one node changes at most one replica of
//!   any key, and filtering out a down node gives exactly the replicas of a ring without it.
//! - **Small and fast.** No I/O, no async runtime, no unsafe code. Replica lookups take
//!   microseconds and, for up to 8 replicas, never allocate.
//!
//! `fleetring` is the placement core of [FleetRing](https://fleetring.dev), which runs fleets
//! of SQLite databases on machines you own, and it is equally at home anywhere else. The design
//! is recorded in the [architecture decision records].
//!
//! # Examples
//!
//! Place chat rooms on three nodes in three zones, then fail over when one node goes down:
//!
//! ```
//! use fleetring::{Node, Ring};
//!
//! let ring = Ring::builder()
//!     .node(Node::new("node-a".parse()?).with_zone("eu-west-1a".parse()?))
//!     .node(Node::new("node-b".parse()?).with_zone("eu-west-1b".parse()?))
//!     .node(Node::new("node-c".parse()?).with_zone("eu-west-1c".parse()?))
//!     .build()?;
//!
//! let home = ring.replicas("room:general");
//! assert_eq!(home.len(), 3);
//!
//! // Every router that sees the primary as down picks the same fallback.
//! let down = home.primary().id();
//! let failover = ring.replicas_where("room:general", |node| node.id() != down).ok_or("all down")?;
//! assert_eq!(failover.primary(), home.followers().next().ok_or("no follower")?);
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```
//!
//! The [examples directory] holds runnable programs from several domains:
//!
//! - `database_fleet`: tenant databases spread across zones, each with a primary and followers.
//! - `kv_sharding`: cache keys sharded over weighted nodes, one owner each.
//! - `chat_failover`: chat rooms that fail over deterministically when nodes go down.
//! - `rebalance`: the migration plan and movement summary for adding a node.
//! - `bounded_loads`: LLM conversations placed on GPU nodes by KV-cache memory.
//! - `regional_cells`: EU and US tenants kept in their regions by residency rules, every other
//!   tenant hashed to a region, and zone-aware rings inside each region.
//! - `spec_roundtrip`: a ring shipped from a control plane to a router as JSON.
//!
//! Run one with `cargo run -p fleetring --example database_fleet`. `spec_roundtrip` also needs
//! `--features serde`.
//!
//! # Concepts
//!
//! ## Keys
//!
//! A key names something that must live somewhere: a database, tenant, room, actor, shard, or
//! session. Lookups take any `impl AsRef<[u8]>`, so `&str`, `String`, and byte slices all work.
//! Each key is placed on its own, with no slot or partition layer in between.
//!
//! Keys are opaque bytes, hashed exactly as given: no Unicode normalization, case folding, or
//! trimming. `Tenant:Acme` and `tenant:acme` are different keys and can land on different nodes,
//! so canonicalize names before they reach the ring, the same way in every language.
//!
//! ## Nodes
//!
//! A [`Node`] is a member of a ring, described by four things:
//!
//! - **Identity:** a [`NodeId`], assigned once and kept for the node's lifetime. Placement hashes
//!   the ID, so it must never be a network address: renaming a node moves its data, like removing
//!   it and adding a new one. Rings hold identities only, so map each `NodeId` to an address
//!   yourself.
//! - **Weight:** a [`Weight`], the node's relative capacity, 1 by default. Only ratios matter:
//!   under the default strategy, a node of weight 2 is the primary for twice as many keys as a
//!   node of weight 1.
//! - **Zone:** a [`Zone`], a failure domain such as a rack, a room, or a cloud availability zone.
//!   Nodes without one share the default zone.
//! - **Host:** a [`Host`], the machine the node runs on. Several nodes can share a host, for
//!   example one node per disk. A node without a host label is its own host, and a host belongs
//!   to exactly one zone.
//!
//! IDs, zones, and hosts are labels: 1 to 64 bytes of lowercase ASCII letters, digits, `-`, `_`,
//! `.`, and `:`, parsed with [`str::parse`].
//!
//! A [`Ring`] is an immutable set of nodes plus the settings that place keys on them: the
//! [`Strategy`], a seed, the [`ReplicationFactor`], and the [`StableHasher`].
//! [`Ring::builder`] collects them, and [`RingBuilder::build`] validates them all at once. A ring
//! keeps its nodes sorted by ID, so it depends only on its set of nodes, never on the order they
//! were added in. A ring with the default [`Xxh3`] hasher is `Send + Sync`, so threads can share
//! one.
//!
//! ## Replica sets and the preferred primary
//!
//! [`Ring::replicas`] returns a key's [`ReplicaSet`]: up to R distinct nodes, best first, where R
//! is the ring's [`ReplicationFactor`], 3 by default. The first node is the *preferred primary*
//! ([`ReplicaSet::primary`]), and the rest are its followers ([`ReplicaSet::followers`]). Each
//! kind of system reads the set in its own way:
//!
//! - A single-leader system makes the preferred primary its leader once a lease or an election
//!   of its own confirms it, and replicates to the followers.
//! - A leaderless system uses the whole set as its quorum.
//! - A cache, or a router that needs one owner per key, uses R = 1 or just [`Ring::primary`].
//!
//! Replicas spread across zones first, then hosts. With Z zones, the first min(R, Z) replicas sit
//! in distinct zones, and two replicas share a host only when there are fewer hosts than
//! replicas. The set for R is always a prefix of the set for R + 1 ([`Ring::replicas_n`]), so
//! raising a key's replication factor never reshuffles its existing replicas.
//!
//! A ring with fewer nodes than R still places every key, on all of its nodes, and
//! [`ReplicaSet::is_under_replicated`] reports the shortfall. [`Ring::diagnostics`] reports
//! topology problems, such as too few zones, or a zone that holds far more than its share of
//! replicas.
//!
//! ## Preference lists
//!
//! A key's *preference list* ranks every node of the ring for that key, best first, and
//! [`Ring::preference`] returns it as a [`Preference`] iterator. The replica set is its first R
//! entries. The entries after that are where traffic or data goes when earlier nodes are
//! unavailable, in an order every process agrees on.
//!
//! The [`Strategy`] ranks the nodes for each key, then selection makes three passes over that
//! ranking: the best node of each zone, then the best node of each unused host, then all the
//! rest. Unlike `replicas`, building a preference list allocates O(N) memory for N nodes.
//!
//! ## Home placement and actual placement
//!
//! The ring computes *home placement*: where a key should live, from nothing but the ring and the
//! key. Any process that holds the ring computes it locally, with no lookup service. *Actual
//! placement* is where the key does live, as recorded by the system that stores it. The two
//! differ while a key migrates to a new ring, and for keys placed by a policy that looks beyond
//! the ring.
//!
//! [`BoundedLoads`] is such a policy: consistent hashing with bounded loads (Mirrokni, Thorup, and
//! Zadimoghaddam, 2018). Hashing balances key counts, but not the bytes or traffic behind each
//! key. When a key is created or re-placed, [`BoundedLoads::load_view`] takes a snapshot of every
//! node's load, in any unit, and [`LoadView::replicas`] places the key on its replica set among
//! the nodes that stay within their weighted share of the total plus some [`Headroom`]. The
//! policy never fails: when too few nodes have room, it fills the remaining slots from the key's
//! preference list and reports the placement as
//! [saturated](BoundedPlacement::is_saturated).
//!
//! The result depends on the loads, so no other process can recompute it. Record every
//! placement, with an override whenever [`BoundedPlacement::is_home`] is false, and have clients
//! that compute home placement follow a redirect for those keys. Apply the policy when you
//! assign a key, never when you route a request.
//!
//! ## Membership and liveness
//!
//! *Membership*, meaning which nodes exist and their weights, zones, and hosts, decides
//! placement, so changing it moves data. *Liveness* never does. A node that is down, asleep, or
//! overloaded stays a member, and lookups skip it with a filter: [`Ring::replicas_where`] and
//! [`Ring::preference_where`] accept any predicate over [`Node`]s.
//!
//! The filter applies before replicas spread across zones, so if a zone's best node is down, that
//! zone's next-best node takes its place. The result is exactly the placement of a ring built
//! without the rejected nodes. Every router that sees the same nodes as down picks the same
//! fallback, no data moves, and when a node comes back, its keys are home again.
//!
//! To change membership, derive the next ring with [`Ring::to_builder`] and plan the migration.
//! [`Ring::diff`] yields a [`Change`] for every key whose replica set differs, and
//! [`Diff::summarize`] counts the changes in a [`MovementSummary`]. Adding, removing, or
//! reweighting one node changes at most one member of any replica set, and
//! [`Ring::estimate_movement`] predicts how many keys change before you have a list of them.
//!
//! ## Epochs and fingerprints
//!
//! Rings travel: a control plane builds them and ships them to routers and clients as a
//! [`RingSpec`], which [`Ring::to_spec`] writes and [`Ring::from_spec`] reads back. With the
//! `serde` feature, a spec serializes to JSON or another self-describing format. Two values let
//! peers compare rings:
//!
//! - The [`Epoch`] is the ring's version. The control plane assigns it, and it only grows within
//!   a cluster, so a peer whose ring has a lower epoch knows its ring is stale. Epochs never
//!   affect placement.
//! - The [`Fingerprint`] identifies the ring's placement function. [`Ring::fingerprint`] computes
//!   it from every input that affects placement and from the placements of 64 probe keys, and
//!   `from_spec` rejects a spec whose stored fingerprint doesn't match.
//!
//! Peers exchange `(epoch, fingerprint)` pairs. The same epoch with a different fingerprint means
//! a bug or a misconfiguration, such as a wrong seed or hasher, and must fail loudly rather than
//! route keys.
//!
//! ## Cells
//!
//! Large fleets place keys in layers: a directory or a ring picks the *cell*, such as a region, a
//! jurisdiction, or a tenant pool, and the cell's own ring picks the nodes. A [`Cells`] layer
//! does both. Its selector applies a [`Directory`] of exact-key and longest-prefix rules first,
//! for residency and pinning, then a selector ring whose nodes are the cells, weighted by their
//! shares of the remaining keys. Each cell is a [`Ring`] or another `Cells` layer, so
//! `Cells<Cells<Ring>>` places keys by region, then by cell.
//!
//! All of a key's replicas live in one cell, spread across that cell's zones and hosts, and
//! [`Cells::replicas`] returns them with the key's path of cells as a [`CellPlacement`]. Cell
//! weights are explicit, so changes inside a cell never move keys to another cell, and
//! [`Cells::diff`] tells cross-cell moves from in-cell changes. Every layer hashes keys with its
//! own seed: [`CellsBuilder::build`] rejects a ring that shares its seed with a selector ring
//! above it.
//!
//! ```
//! use fleetring::{Cells, Directory, Node, Ring};
//!
//! let ring = |seed: u64, ids: [&str; 2]| {
//!     Ring::builder()
//!         .seed(seed)
//!         .nodes(ids.map(|id| Node::new(id.parse().expect("valid id"))))
//!         .build()
//! };
//! let fleet = Cells::builder()
//!     .directory(Directory::new().prefix("eu:", "eu".parse()?).catch_all("us".parse()?))
//!     .cell("eu".parse()?, ring(1, ["eu-1", "eu-2"])?)
//!     .cell("us".parse()?, ring(2, ["us-1", "us-2"])?)
//!     .build()?;
//! let placed = fleet.replicas("eu:tenant:acme");
//! assert_eq!(placed.to_string(), format!("eu {}", placed.replicas()));
//! assert!(placed.replicas().iter().all(|node| node.id().as_str().starts_with("eu-")));
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```
//!
//! # Choosing a strategy
//!
//! A [`Strategy`] ranks the nodes for each key. Both strategies keep membership changes minimal
//! and feed the same zone- and host-aware selection, so every guarantee above holds for either.
//! The strategy is part of the placement function, though: switching strategies moves almost
//! every key, so choose before you place data.
//!
//! | | [`Rendezvous`](Strategy::Rendezvous) (default) | [`Vnodes`](Strategy::Vnodes) |
//! |---|---|---|
//! | Lookup cost | O(N): one hash per node | O(log T): a binary search over T tokens |
//! | `replicas`, R = 3, 100 nodes | 1.55 µs | 0.11 µs |
//! | `replicas`, R = 3, 1000 nodes | 10.3 µs | 0.42 µs |
//! | `build`, 1000 nodes | 0.16 ms | 19 ms |
//! | Memory | the node list | 12 bytes per token, at most 2²² tokens |
//! | Balance | exact: a node's expected share of primaries is its share of the weight | about 1/√tokens per node: ±6% at 256 tokens |
//! | Weights | any `u32` | small integers, because a node's tokens are its weight × tokens per weight |
//!
//! The timings are medians from the [benchmarks], measured on an Apple M-series laptop with
//! weights from 1 to 4 and 256 tokens per weight.
//!
//! Start with rendezvous. It needs no tuning, honors weights exactly, and costs about 1.5 µs per
//! lookup at 100 nodes and 10 µs at 1000. Choose vnodes when lookups are hot enough for O(N) to
//! matter, typically in a router in front of a large fleet, and approximate balance is
//! acceptable. [`Ring::expected_shares`] reports each node's exact share of primaries under
//! either strategy, and [`Ring::diagnostics`] flags vnode nodes with too few tokens to balance
//! well.
//!
//! ```
//! use fleetring::{Node, Ring, Strategy, VnodeParams};
//!
//! let ring = Ring::builder()
//!     .strategy(Strategy::Vnodes(VnodeParams::default()))
//!     .node(Node::new("cache-1".parse()?))
//!     .node(Node::new("cache-2".parse()?))
//!     .node(Node::new("cache-3".parse()?))
//!     .build()?;
//! assert_eq!(ring.replicas("user:42").len(), 3);
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```
//!
//! # Determinism
//!
//! Placement is a pure function of the ring and the key. The crate performs no I/O and reads no
//! clocks, randomness, environment variables, or global state, so the same inputs always give
//! the same outputs. Placement is also a wire format: routers, clients, and ports in other
//! languages must agree on where every key lives, or writes reach the wrong node. The
//! [placement spec] defines placement normatively, down to the bytes each hash consumes, and a
//! port conforms when it reproduces the spec's golden vectors bit for bit.
//!
//! For the same ring and key, `fleetring` guarantees:
//!
//! - **The same nodes in the same order, everywhere:** on 32- and 64-bit targets, on little- and
//!   big-endian ones, and on WebAssembly; under every Rust release; and in every implementation
//!   of the spec. Arithmetic uses integers only, every hash input is explicit little-endian
//!   bytes, and the output of the default [`Xxh3`] hasher has been frozen since xxHash 0.8.0.
//! - **Independence from construction order:** a ring depends on its set of nodes, never on the
//!   order they were added in.
//! - **Stability across releases:** from 0.1.0, which freezes placement spec v1, changing which
//!   nodes any key maps to requires a new spec version, whatever the crate version. Until then,
//!   `0.0.x` releases may still change placement.
//! - **Detectable divergence:** a different hasher, seed, or strategy changes the
//!   [`Fingerprint`], and so does a hasher or port that places any of the 64 probe keys
//!   differently.
//!
//! Bounded-load placements are part of the spec too, and also depend on the loads, the headroom,
//! and the item's load. So are [`Cells`] layers: their paths, replicas, and fingerprints. The spec doesn't cover [`Ring::estimate_movement`],
//! [`Ring::expected_shares`], or [`Ring::diagnostics`], which sample synthetic keys that a later
//! release may change. A custom [`StableHasher`] is only as stable as its implementation, so pin
//! its output with test vectors.
//!
//! # Feature flags
//!
//! | Feature | Default | Enables |
//! |---|---|---|
//! | `std` | yes | Nothing yet: it is reserved so that `no_std` + `alloc` support can land without breaking builds that set `default-features = false`. |
//! | `serde` | no | `Serialize` and `Deserialize` for [`RingSpec`], the types it holds, and [`CellId`], so rings can be stored and shipped. Deserializing validates every value and ignores unknown fields. |
//! | `test-util` | no | The `test_util` module of test doubles, such as a hasher whose outputs a test chooses. Never enable it in production builds. |
//!
//! [architecture decision records]: https://github.com/fleetring/fleetring/tree/main/docs/adr
//! [benchmarks]: https://github.com/fleetring/fleetring/blob/main/docs/perf.md
//! [examples directory]: https://github.com/fleetring/fleetring/tree/main/crates/fleetring/examples
//! [placement spec]: https://github.com/fleetring/fleetring/blob/main/docs/spec/placement-v1.md

// ADR-0003: placement must be bit-identical on every platform, so library code never uses
// floating point.
#![deny(clippy::float_arithmetic)]

mod bounded;
mod cell_diff;
mod cells;
mod diagnostics;
mod diff;
mod directory;
mod epoch;
mod hasher;
mod label;
mod lookup;
mod math;
mod node;
mod rendezvous;
mod replica;
mod ring;
mod select;
#[cfg(feature = "serde")]
mod serde_impls;
mod spec;
mod strategy;
#[cfg(any(test, feature = "test-util"))]
pub mod test_util;
#[cfg(test)]
mod vectors;
mod vnodes;

pub use bounded::{BoundedLoads, BoundedPlacement, Headroom, LoadView, LoadViewError};
pub use cell_diff::{CellChange, CellMovementSummary, CellsDiff};
pub use cells::{BuildCellsError, CellPlacement, Cells, CellsBuilder, Placement};
pub use diagnostics::{Diagnostic, NodeShare, Severity};
pub use diff::{Change, Diff, MovementSummary};
pub use directory::Directory;
pub use epoch::Epoch;
pub use hasher::{StableHasher, Xxh3};
pub use label::{CellId, Host, LabelKind, NodeId, ParseLabelError, Zone};
pub use node::{Node, Weight};
pub use replica::{Preference, ReplicaIter, ReplicaSet, ReplicationFactor};
pub use ring::{BuildRingError, Ring, RingBuilder};
pub use spec::{
    Fingerprint, FromSpecError, NodeSpec, ParseFingerprintError, RingSpec, StrategySpec,
};
pub use strategy::{Strategy, VnodeParams};

// Public types must stay shareable across threads; a refactor that breaks this fails to compile.
const _: () = {
    const fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<BoundedLoads>();
    assert_send_sync::<BoundedPlacement<'static>>();
    assert_send_sync::<BuildCellsError>();
    assert_send_sync::<BuildRingError>();
    assert_send_sync::<CellChange<'static, Vec<u8>>>();
    assert_send_sync::<CellId>();
    assert_send_sync::<CellMovementSummary>();
    assert_send_sync::<CellPlacement<'static>>();
    assert_send_sync::<Cells<Ring<Xxh3>>>();
    assert_send_sync::<Cells<Cells<Ring<Xxh3>>>>();
    assert_send_sync::<CellsBuilder<Ring<Xxh3>>>();
    assert_send_sync::<CellsDiff<'static, Ring<Xxh3>, std::vec::IntoIter<Vec<u8>>>>();
    assert_send_sync::<Change<'static, Vec<u8>>>();
    assert_send_sync::<Diagnostic>();
    assert_send_sync::<Diff<'static, Xxh3, std::vec::IntoIter<Vec<u8>>>>();
    assert_send_sync::<Directory>();
    assert_send_sync::<Epoch>();
    assert_send_sync::<Fingerprint>();
    assert_send_sync::<FromSpecError>();
    assert_send_sync::<Headroom>();
    assert_send_sync::<Host>();
    assert_send_sync::<LoadView<'static, Xxh3>>();
    assert_send_sync::<LoadViewError>();
    assert_send_sync::<MovementSummary>();
    assert_send_sync::<Node>();
    assert_send_sync::<NodeId>();
    assert_send_sync::<NodeShare>();
    assert_send_sync::<NodeSpec>();
    assert_send_sync::<ParseFingerprintError>();
    assert_send_sync::<ParseLabelError>();
    assert_send_sync::<Preference<'static>>();
    assert_send_sync::<ReplicaIter<'static>>();
    assert_send_sync::<ReplicaSet<'static>>();
    assert_send_sync::<ReplicationFactor>();
    assert_send_sync::<Ring<Xxh3>>();
    assert_send_sync::<RingBuilder>();
    assert_send_sync::<RingSpec>();
    assert_send_sync::<Severity>();
    assert_send_sync::<Strategy>();
    assert_send_sync::<StrategySpec>();
    assert_send_sync::<VnodeParams>();
    assert_send_sync::<Weight>();
    assert_send_sync::<Xxh3>();
    assert_send_sync::<Zone>();
};
