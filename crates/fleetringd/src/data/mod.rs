//! The data plane's `Database` service, and the client that calls it (ADR-0019).
//!
//! Every data node serves the `Database` service. In v0.1 it runs SQL on a database's primary,
//! for tests and the command line; the router replaces it for applications (MVP step 4).
//!
//! - **Who serves a request:** the node that holds the database's lease. A node learns that it
//!   does when it takes the first lease or completes a promotion, or, after a restart, from the
//!   control plane. It renews its leases, and stops serving a database once it learns of a newer
//!   epoch: a renewal the control plane refuses, or a fence or a stream at that epoch.
//! - **Failover:** while a promotion is in progress a database has no primary, and requests for it
//!   fail with `UNAVAILABLE`; a client retries, and reaches the new primary once it serves.
//! - **Draining:** a node that is about to leave hands its leases over first (the `drain` module):
//!   each database answers `UNAVAILABLE` for a moment, then redirects to its new primary.
//! - **Redirects, not forwarding:** any other node answers `UNAVAILABLE` and names the primary as
//!   `host:port` in the [`PRIMARY_METADATA_KEY`] metadata entry, as a control node that isn't the
//!   leader does. A write then crosses one hop, and a lost reply is never doubled by a proxy.
//!   [`Client`] follows redirects on its own.
//! - **Creation is driven by the primary itself.** The client registers the database with the
//!   control plane, which places it with the ring, then calls `Create` on the first live member
//!   of the replica set. That node takes the first lease and creates the database file through
//!   its registry, then serves it as primary through [`Replication`](crate::replication::Replication),
//!   which streams it to its followers; each fetches its first copy from the primary.
//! - **Acknowledgements:** a write is acknowledged once a write quorum of W replicas holds it,
//!   where W comes from the database's `acks` mode (ADR-0018, ADR-0023). Its reply says how
//!   many replicas held it then (W or more), the replica set's size R, and whether the database
//!   is under-replicated: fewer than R replicas reachable. A write that arrives while fewer than
//!   the database's `min_replicas` are reachable is refused with `UNAVAILABLE` before it runs,
//!   so nothing commits. One that committed on the primary without reaching its quorum fails
//!   with `ABORTED`, since it may still replicate later.
//! - **Dropping** is a control command: `Control.DropDatabase` removes the database from the
//!   control plane at its current epoch. The primary learns of it when its next lease renewal is
//!   refused as an unknown database, within one renewal interval, and stops serving it; its
//!   streams end, and each follower whose stream ends asks the control plane whether the database
//!   still exists. Every replica then closes its copy through its registry and deletes the files.
//!   A write to a dropped database fails with `NOT_FOUND`.
//! - **Peers' addresses** come from the control plane: each member advertises one when it joins
//!   (`[node] advertise`), and placements carry it for each replica.
//! - **Metadata:** a database may carry a description and labels in control state (ADR-0041).
//!   [`Client::create_database_with_metadata`] creates one with them, through a call a control
//!   node without the feature refuses, so a creation never silently loses them;
//!   [`Client::update_database`] patches them; [`Client::list_databases_where`] finds databases
//!   by label and prefix, page by page.
//! - **Schema:** [`Client::schema`] asks a database's primary for its tables, views, indexes,
//!   triggers, and virtual tables, with the engine's own definitions and row counts capped at a
//!   limit (ADR-0041, point 6).
//! - **Lease watching:** `WatchLeases` streams the leases one node holds, from the node's own
//!   view (FR-042): the leases held, then a `Gained` or `Lost` event per change, each with its epoch.
//!   A worker beside the node acts on the databases the node is primary for and stops when a
//!   lease moves, without polling the control plane. [`Client::watch_leases`] opens the stream,
//!   and `fleetringd lease watch` prints it.

mod client;
pub(crate) mod convert;
mod drain;
mod lease;
mod leases;
mod metadata;
mod node;
mod redirect;
mod schema;
mod service;

pub use client::{
    ArchiveDescription, ArchiveVerification, ArchivedSnapshot, Client, DatabaseSpec,
    DurabilityChange, LeaseUpdate, LeaseUpdates, Location, NodeStatus, ReplicaInfo, Restoration,
    ServedDatabase, Verification, Written,
};
pub(crate) use drain::DrainSettings;
pub(crate) use lease::{LeaseKeeper, LeaseSettings};
pub(crate) use metadata::is_invisible;
pub use metadata::{
    Descriptions, LabelRequirement, ListFilter, Listing, MetadataView, NewMetadata,
};
pub(crate) use node::DataNode;
pub use redirect::{PRIMARY_METADATA_KEY, not_primary, primary_hint};
pub use schema::SchemaView;
pub(crate) use service::DatabaseService;
