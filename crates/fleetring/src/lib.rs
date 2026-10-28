//! Consistent-hashing placement for fleets of SQLite, libSQL, and Turso databases.
//!
//! FleetRing decides where each database lives in a fleet of nodes you own: one database per
//! agent, tenant, or device. This crate is the placement core. It is a pure, deterministic
//! library: the same ring and key produce the same placement on every platform, Rust release,
//! and crate release.
//!
//! The design is recorded in the [architecture decision records].
//!
//! [architecture decision records]: https://github.com/fleetring/fleetring/tree/main/docs/adr

// ADR-0003: placement must be bit-identical on every platform, so library code never uses
// floating point.
#![deny(clippy::float_arithmetic)]

mod bounded;
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
mod strategy;
#[cfg(any(test, feature = "test-util"))]
pub mod test_util;
mod vnodes;

pub use bounded::{BoundedLoads, BoundedPlacement, Headroom, LoadView, LoadViewError};
pub use diff::{Change, Diff, MovementSummary};
pub use epoch::Epoch;
pub use hasher::{StableHasher, Xxh3};
pub use label::{Host, LabelKind, NodeId, ParseLabelError, Zone};
pub use node::{Node, Weight};
pub use replica::{Preference, ReplicaIter, ReplicaSet, ReplicationFactor};
pub use ring::{BuildRingError, Ring, RingBuilder};
pub use strategy::{Strategy, VnodeParams};

// Public types must stay shareable across threads; a refactor that breaks this fails to compile.
const _: () = {
    const fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<BoundedLoads>();
    assert_send_sync::<BoundedPlacement<'static>>();
    assert_send_sync::<BuildRingError>();
    assert_send_sync::<Change<'static, Vec<u8>>>();
    assert_send_sync::<Diff<'static, Xxh3, std::vec::IntoIter<Vec<u8>>>>();
    assert_send_sync::<Epoch>();
    assert_send_sync::<Headroom>();
    assert_send_sync::<Host>();
    assert_send_sync::<LoadView<'static, Xxh3>>();
    assert_send_sync::<LoadViewError>();
    assert_send_sync::<MovementSummary>();
    assert_send_sync::<Node>();
    assert_send_sync::<NodeId>();
    assert_send_sync::<ParseLabelError>();
    assert_send_sync::<Preference<'static>>();
    assert_send_sync::<ReplicaIter<'static>>();
    assert_send_sync::<ReplicaSet<'static>>();
    assert_send_sync::<ReplicationFactor>();
    assert_send_sync::<Ring<Xxh3>>();
    assert_send_sync::<RingBuilder>();
    assert_send_sync::<Strategy>();
    assert_send_sync::<VnodeParams>();
    assert_send_sync::<Weight>();
    assert_send_sync::<Xxh3>();
    assert_send_sync::<Zone>();
};
