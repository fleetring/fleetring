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
//!   its registry. Followers receive their copies from replication (FR-030, FR-032).
//! - **Acknowledgements:** a write's reply says how many replicas hold it and whether that is
//!   fewer than the replica set. Until replication lands (FR-030), only the primary does.
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
