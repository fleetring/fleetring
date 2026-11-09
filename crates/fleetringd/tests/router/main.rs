//! FR-066: the router, a Hrana-over-HTTP front door, and the `Database.Session` stream it runs
//! each Hrana stream on, over real TCP with mutual TLS (ADR-0037).

#[path = "../support/mod.rs"]
mod support;

mod fleet;
mod http;
mod sessions;
