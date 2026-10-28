//! Shared test harness: key corpora, membership fixtures, and balance statistics (FR-006).
//!
//! Property, statistical, and benchmark code all test against these inputs (ADR-0008, point 7).
//! Everything is deterministic: the corpora are byte-identical on every platform, and balance
//! checks run on fixed corpora, so they never flake.
//!
//! A bench can include the harness with `#[path = "../tests/it/common/mod.rs"] mod common;`, plus
//! `#[allow(dead_code, reason = "...")]` because a bench uses only part of it. The harness's own
//! tests sit in `#[cfg(test)]` modules, so a bench doesn't compile them.

pub(crate) mod corpus;
pub(crate) mod fixtures;
pub(crate) mod prng;
pub(crate) mod stats;
