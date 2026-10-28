//! The data plane's `Database` service, and the client that calls it (ADR-0019).
//!
//! Every data node serves the `Database` service. In v0.1 it runs SQL on a database's primary,
//! for tests and the command line; the router replaces it for applications (MVP step 4).
//!
//! - **Who serves a request:** the node that holds the database's lease. A node learns that it
//!   does when it takes the first lease, or, after a restart, from the control plane on the
//!   database's first request.
//! - **Redirects, not forwarding:** any other node answers `UNAVAILABLE` and names the primary as
//!   `host:port` in the [`PRIMARY_METADATA_KEY`] metadata entry, as a control node that isn't the
//!   leader does. A write then crosses one hop, and a lost reply is never doubled by a proxy.
//!   [`Client`] follows redirects on its own.
//! - **Creation is driven by the primary itself.** The client registers the database with the
//!   control plane, which places it with the ring, then calls `Create` on the first live member
//!   of the replica set. That node takes the first lease and creates the database file through
//!   its registry, then serves it as primary through [`Replication`](crate::replication::Replication),
//!   which streams it to its followers; each fetches its first copy from the primary.
//! - **Acknowledgements:** a write is acknowledged once a write quorum of W replicas holds it
//!   (ADR-0018). Its reply says how many replicas held it then (W or more), the replica set's
//!   size R, and whether the database is under-replicated: fewer than R replicas reachable. A
//!   write that committed on the primary without reaching its quorum fails with `ABORTED`, since
//!   it may still replicate later.
//! - **Peers' addresses** come from the control plane: each member advertises one when it joins
//!   (`[node] advertise`), and placements carry it for each replica.

mod client;
mod convert;
mod node;
mod redirect;
mod service;

pub use client::{Client, Location, ReplicaInfo, Written};
pub(crate) use node::DataNode;
pub use redirect::{PRIMARY_METADATA_KEY, not_primary, primary_hint};
pub(crate) use service::DatabaseService;
