//! Node calls bound to placement (ADR-0031): what a node must be, for the database a node call
//! names, to make the call, and how a call that fails its binding is refused.
//!
//! Every node RPC in [`RPCS`](crate::auth::RPCS) names a [`Binding`]. The receiver checks it
//! against control state, or against its own durable epoch record, before it touches any state,
//! and refuses a call that fails it with a [`BindingDenied`]: `PERMISSION_DENIED`, a message
//! naming the method, the node, and the reason, and [`BINDING_KEY`] metadata naming the binding,
//! so that tests and tooling never parse messages. Each refusal is logged once on the
//! `fleetringd::audit` target.

use std::fmt;

use fleetring::NodeId;
use fleetring_control::{DatabaseName, LeaseEpoch};
use tonic::Status;
use tonic::metadata::MetadataValue;

/// The audit log's target.
const AUDIT: &str = "fleetringd::audit";

/// The metadata key of a binding refusal; its value names the binding the call failed.
pub const BINDING_KEY: &str = "fleetring-binding";

/// What a node must be, for the database a node call names, to make it (ADR-0031).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[non_exhaustive]
pub enum Binding {
    /// Not a node call.
    None,
    /// The request names the calling node itself.
    SelfNode,
    /// The caller is in the database's replica set.
    Replica,
    /// The caller holds the database's lease at the request's (or the current) epoch.
    Holder,
    /// The caller is the candidate of the database's promotion at the request's epoch.
    Candidate,
    /// The caller holds or is promoting to the stream's epoch.
    HolderOrCandidate,
    /// The caller is a follower this primary streams the database to.
    Follower,
    /// Control state no longer holds the database at the request's epoch: it was dropped, and
    /// any database of the name now is a newer one at a lower epoch (ADR-0042).
    Dropped,
}

impl Binding {
    /// The binding as the proto comments, the daemon README, and [`BINDING_KEY`] metadata name
    /// it.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::SelfNode => "self",
            Self::Replica => "replica",
            Self::Holder => "holder",
            Self::Candidate => "candidate",
            Self::HolderOrCandidate => "holder or candidate",
            Self::Follower => "follower",
            Self::Dropped => "dropped",
        }
    }
}

impl fmt::Display for Binding {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Why a node call was refused.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Unbound {
    /// The request names another node where it must name the caller.
    OtherNode {
        /// The node the request names.
        named: NodeId,
    },
    /// The caller, or for a stream the receiver, isn't in the database's replica set.
    NotReplica {
        /// The database.
        database: DatabaseName,
    },
    /// The caller doesn't hold the database's lease at the epoch.
    NotHolder {
        /// The database.
        database: DatabaseName,
        /// The epoch.
        epoch: LeaseEpoch,
    },
    /// The caller isn't the candidate of the database's promotion to the epoch.
    NotCandidate {
        /// The database.
        database: DatabaseName,
        /// The epoch.
        epoch: LeaseEpoch,
    },
    /// The caller isn't a follower this node streams the database to.
    NotFollower {
        /// The database.
        database: DatabaseName,
    },
    /// The receiving node's copy isn't at the epoch, or control state still holds the database
    /// at it or later.
    NotDropped {
        /// The database.
        database: DatabaseName,
        /// The epoch.
        epoch: LeaseEpoch,
    },
}

impl Unbound {
    /// The binding the reason says the call failed.
    #[must_use]
    pub const fn binding(&self) -> Binding {
        match self {
            Self::OtherNode { .. } => Binding::SelfNode,
            Self::NotReplica { .. } => Binding::Replica,
            Self::NotHolder { .. } => Binding::Holder,
            Self::NotCandidate { .. } => Binding::Candidate,
            Self::NotFollower { .. } => Binding::Follower,
            Self::NotDropped { .. } => Binding::Dropped,
        }
    }

    /// The database the reason names, if it names one.
    #[must_use]
    pub fn database(&self) -> Option<&DatabaseName> {
        match self {
            Self::OtherNode { .. } => None,
            Self::NotReplica { database }
            | Self::NotHolder { database, .. }
            | Self::NotCandidate { database, .. }
            | Self::NotFollower { database }
            | Self::NotDropped { database, .. } => Some(database),
        }
    }

    /// The epoch the reason names, if it names one.
    #[must_use]
    pub fn epoch(&self) -> Option<LeaseEpoch> {
        match self {
            Self::NotHolder { epoch, .. }
            | Self::NotCandidate { epoch, .. }
            | Self::NotDropped { epoch, .. } => Some(*epoch),
            Self::OtherNode { .. } | Self::NotReplica { .. } | Self::NotFollower { .. } => None,
        }
    }
}

impl fmt::Display for Unbound {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::OtherNode { named } => write!(f, "the request names node '{named}'"),
            Self::NotReplica { database } => write!(f, "not a replica of database '{database}'"),
            Self::NotHolder { database, epoch } => {
                write!(
                    f,
                    "not the holder of database '{database}' at epoch {epoch}"
                )
            }
            Self::NotCandidate { database, epoch } => {
                write!(
                    f,
                    "not the candidate of database '{database}' at epoch {epoch}"
                )
            }
            Self::NotFollower { database } => {
                write!(f, "not a follower of database '{database}' on this node")
            }
            Self::NotDropped { database, epoch } => {
                write!(
                    f,
                    "database '{database}' at epoch {epoch} isn't a dropped copy here"
                )
            }
        }
    }
}

/// A refused node call: the method, the calling node, and why.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{method} refused for node '{node}': {reason}")]
pub struct BindingDenied {
    /// The method's name.
    pub method: &'static str,
    /// The node that made the call.
    pub node: NodeId,
    /// Why it was refused.
    pub reason: Unbound,
    /// The binding the call failed, which [`BINDING_KEY`] names: the reason's own, unless the
    /// call's binding is wider, as a stream's holder-or-candidate check is.
    pub binding: Binding,
}

impl BindingDenied {
    /// `node`'s call to `method`, refused for `reason`.
    #[must_use]
    pub fn new(method: &'static str, node: NodeId, reason: Unbound) -> Self {
        let binding = reason.binding();
        Self {
            method,
            node,
            reason,
            binding,
        }
    }

    /// The same refusal, naming `binding` as the one the call failed.
    #[must_use]
    pub fn naming(mut self, binding: Binding) -> Self {
        self.binding = binding;
        self
    }

    /// `PERMISSION_DENIED` with the message and [`BINDING_KEY`] metadata. Logs the refusal once
    /// on the `fleetringd::audit` target with the node, the method, the database, the epoch,
    /// and the binding.
    #[must_use]
    pub fn into_status(self) -> Status {
        tracing::warn!(
            target: AUDIT,
            principal = %format!("node:{}", self.node),
            rpc = self.method,
            database = self.reason.database().map(DatabaseName::as_str),
            epoch = self.reason.epoch().map(LeaseEpoch::get),
            binding = self.binding.as_str(),
            "denied",
        );
        let mut status = Status::permission_denied(self.to_string());
        status.metadata_mut().insert(
            BINDING_KEY,
            MetadataValue::from_static(self.binding.as_str()),
        );
        status
    }
}

/// The binding a refusal names in its [`BINDING_KEY`] metadata, if `status` is a binding
/// refusal.
#[must_use]
pub fn refused_binding(status: &Status) -> Option<&str> {
    if status.code() != tonic::Code::PermissionDenied {
        return None;
    }
    status.metadata().get(BINDING_KEY)?.to_str().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(text: &str) -> NodeId {
        text.parse().expect("a node id")
    }

    fn database(text: &str) -> DatabaseName {
        text.parse().expect("a database name")
    }

    #[test]
    fn refusal_messages_name_the_binding() {
        let db = database("tenant:acme");
        let epoch = LeaseEpoch::new(7);
        let cases = [
            (
                Unbound::OtherNode { named: node("b") },
                "Fence refused for node 'a': the request names node 'b'",
                "self",
            ),
            (
                Unbound::NotReplica {
                    database: db.clone(),
                },
                "Fence refused for node 'a': not a replica of database 'tenant:acme'",
                "replica",
            ),
            (
                Unbound::NotHolder {
                    database: db.clone(),
                    epoch,
                },
                "Fence refused for node 'a': not the holder of database 'tenant:acme' at epoch 7",
                "holder",
            ),
            (
                Unbound::NotCandidate {
                    database: db.clone(),
                    epoch,
                },
                "Fence refused for node 'a': not the candidate of database 'tenant:acme' at epoch 7",
                "candidate",
            ),
            (
                Unbound::NotFollower {
                    database: db.clone(),
                },
                "Fence refused for node 'a': not a follower of database 'tenant:acme' on this node",
                "follower",
            ),
            (
                Unbound::NotDropped {
                    database: db.clone(),
                    epoch,
                },
                "Fence refused for node 'a': database 'tenant:acme' at epoch 7 isn't a dropped \
                 copy here",
                "dropped",
            ),
        ];
        for (reason, message, binding) in cases {
            let denied = BindingDenied::new("Fence", node("a"), reason);
            assert_eq!(denied.to_string(), message);
            assert_eq!(denied.binding.as_str(), binding, "{message}");
        }
    }

    #[test]
    fn the_status_is_permission_denied_with_metadata() {
        let denied = BindingDenied::new(
            "Replicate",
            node("d"),
            Unbound::NotHolder {
                database: database("orders"),
                epoch: LeaseEpoch::new(3),
            },
        )
        .naming(Binding::HolderOrCandidate);
        let status = denied.into_status();
        assert_eq!(status.code(), tonic::Code::PermissionDenied);
        assert_eq!(
            status.message(),
            "Replicate refused for node 'd': not the holder of database 'orders' at epoch 3"
        );
        assert_eq!(refused_binding(&status), Some("holder or candidate"));
        assert_eq!(refused_binding(&Status::unavailable("down")), None);
    }

    #[test]
    fn bindings_display_as_the_table_names_them() {
        let names: Vec<String> = [
            Binding::None,
            Binding::SelfNode,
            Binding::Replica,
            Binding::Holder,
            Binding::Candidate,
            Binding::HolderOrCandidate,
            Binding::Follower,
            Binding::Dropped,
        ]
        .iter()
        .map(ToString::to_string)
        .collect();
        assert_eq!(
            names,
            [
                "none",
                "self",
                "replica",
                "holder",
                "candidate",
                "holder or candidate",
                "follower",
                "dropped"
            ]
        );
    }
}
