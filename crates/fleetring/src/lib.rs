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

mod hasher;
#[cfg(any(test, feature = "test-util"))]
pub mod test_util;

pub use hasher::{StableHasher, Xxh3};

// Public types must stay shareable across threads; a refactor that breaks this fails to compile.
const _: () = {
    const fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<Xxh3>();
};
