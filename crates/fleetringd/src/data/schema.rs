//! A database's schema as a client reads it (ADR-0041, point 6).

use fleetring::NodeId;
use fleetring_control::{Engine, LeaseEpoch};

use crate::engine::Schema;

/// A database's schema, as its primary reported it.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct SchemaView {
    /// The engine the database runs on, and so what its definitions are written in.
    pub engine: Engine,
    /// The objects, each table followed by its indexes and triggers, and whether the database
    /// has more than the reply holds.
    pub schema: Schema,
    /// The primary that answered, if it said.
    pub primary: Option<NodeId>,
    /// The epoch it answered at, or 0 if it didn't say.
    pub epoch: LeaseEpoch,
}
