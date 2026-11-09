//! Integration tests for `fleetring-wal`, compiled as one binary so they link once and share
//! fixtures.
//!
//! The modules use hand-built WAL bytes in both checksum orders and no files, so they run on
//! every target, wasm included. The `fleetring-wal-sqlite-tests` crate checks the format against
//! real SQLite.

mod batch;
mod common;
mod format;
mod log_end;
mod position;
mod reader;
