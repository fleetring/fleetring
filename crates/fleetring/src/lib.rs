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

mod epoch;
mod hasher;
mod label;
#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "the rendezvous strategy (FR-008) is the first caller"
    )
)]
mod math;
mod node;
mod replica;
mod ring;
mod strategy;
#[cfg(any(test, feature = "test-util"))]
pub mod test_util;

pub use epoch::Epoch;
pub use hasher::{StableHasher, Xxh3};
pub use label::{Host, LabelKind, NodeId, ParseLabelError, Zone};
pub use node::{Node, Weight};
pub use replica::ReplicationFactor;
pub use ring::{BuildRingError, Ring, RingBuilder};
pub use strategy::Strategy;

// Public types must stay shareable across threads; a refactor that breaks this fails to compile.
const _: () = {
    const fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<BuildRingError>();
    assert_send_sync::<Epoch>();
    assert_send_sync::<Host>();
    assert_send_sync::<Node>();
    assert_send_sync::<NodeId>();
    assert_send_sync::<ParseLabelError>();
    assert_send_sync::<ReplicationFactor>();
    assert_send_sync::<Ring<Xxh3>>();
    assert_send_sync::<RingBuilder>();
    assert_send_sync::<Strategy>();
    assert_send_sync::<Weight>();
    assert_send_sync::<Xxh3>();
    assert_send_sync::<Zone>();
};
