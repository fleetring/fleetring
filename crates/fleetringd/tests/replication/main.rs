//! FR-030, FR-032, FR-042, and FR-044: the replication stream, follower mirroring, the write
//! quorum, snapshot catch-up, the lease watch, and the log's hash chain, over real TCP and real
//! SQLite.

mod chain;
mod common;
mod durability;
mod heartbeat;
mod leases;
mod mirror;
mod perf;
mod protocol;
mod proxy;
mod quorum;
mod snapshot;
#[path = "../support/mod.rs"]
mod support;
