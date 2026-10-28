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
//!   microseconds and never allocate.
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
//! [architecture decision records]: https://github.com/fleetring/fleetring/tree/main/docs/adr

// ADR-0003: placement must be bit-identical on every platform, so library code never uses
// floating point.
#![deny(clippy::float_arithmetic)]

mod bounded;
mod diagnostics;
mod diff;
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
mod vnodes;

pub use bounded::{BoundedLoads, BoundedPlacement, Headroom, LoadView, LoadViewError};
pub use diagnostics::{Diagnostic, NodeShare, Severity};
pub use diff::{Change, Diff, MovementSummary};
pub use epoch::Epoch;
pub use hasher::{StableHasher, Xxh3};
pub use label::{Host, LabelKind, NodeId, ParseLabelError, Zone};
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
    assert_send_sync::<BuildRingError>();
    assert_send_sync::<Change<'static, Vec<u8>>>();
    assert_send_sync::<Diagnostic>();
    assert_send_sync::<Diff<'static, Xxh3, std::vec::IntoIter<Vec<u8>>>>();
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
