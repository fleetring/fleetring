//! Integration tests for `fleetring-wal`, compiled as one binary so they link once and share
//! fixtures.
//!
//! The byte-level modules use hand-built WAL bytes in both checksum orders and no files, so they
//! run on every target, wasm included. The `sqlite` module checks the format against real SQLite.

mod batch;
mod common;
mod format;
mod log_end;
mod position;
mod reader;
#[cfg(not(target_family = "wasm"))]
mod sqlite;
