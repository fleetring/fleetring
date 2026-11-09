//! `fleetring-wal`'s format checked against WAL files written by real SQLite, compiled as one
//! binary so the tests link once and share the primary and follower fixtures.

mod common;
mod format;
