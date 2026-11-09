//! Tests of `fleetring-wal` against real SQLite, with no library code of their own.
//!
//! They live in their own crate because rusqlite needs a newer toolchain than `fleetring-wal`'s
//! minimum supported Rust version: here they build with the daemon's. They need a C toolchain
//! and files, so they don't run on wasm. Run them with
//! `cargo test -p fleetring-wal-sqlite-tests`.
