//! The SQLite WAL format, for FleetRing's SQLite engine adapter (ADR-0016).
//!
//! The crate reads committed transactions from a WAL file, validates them with SQLite's own
//! checksums, and mirrors them into a follower's WAL. It works on bytes and files and doesn't
//! link SQLite. Its types stay inside the SQLite adapter: everything above the adapter sees only
//! engine-defined positions and payloads.
