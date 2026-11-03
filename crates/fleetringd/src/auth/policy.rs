//! What each RPC needs from its caller, and who a caller is (ADR-0025).
//!
//! [`RPCS`] lists every RPC the daemon serves with its [`Requirement`]. The request boundary
//! refuses a call whose path isn't in the table, so an RPC added to the protocol without a
//! requirement is denied to everyone until it gets one, and a test fails until then.

use std::fmt;

use fleetring::NodeId;
use fleetring_control::{DatabaseName, KeyId, Timestamp, TokenId};

use crate::auth::{Action, Scopes, Target};
use crate::tls::Principal;

/// What an RPC needs from its caller.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Requirement {
    /// Only nodes call it: replication, leases, promotions, membership reports. Node-to-node
    /// calls are never authorized by key, and no principal may make them.
    Node,
    /// A key-less control operation: a node, or a principal with the `cluster` scope.
    Cluster,
    /// The action on the database the request names.
    Key(Action),
    /// The action on the database the request names, or the `cluster` scope: inspections an
    /// operator makes across tenants, such as finding or verifying a database.
    KeyOrCluster(Action),
    /// A listing over many databases: each part of it shows a database only if the caller may
    /// take that part's action on it, and everything to a node or a principal with `cluster`.
    /// A principal that may take none of the actions on any key, and lacks `cluster`, is
    /// refused.
    Listing(&'static [Action]),
}

/// The actions, as a requirement names them: `read`, or `read or stream`.
fn either(actions: &[Action]) -> String {
    actions
        .iter()
        .map(|action| action.as_str())
        .collect::<Vec<_>>()
        .join(" or ")
}

impl fmt::Display for Requirement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Node => f.write_str("node"),
            Self::Cluster => f.write_str("cluster"),
            Self::Key(action) => write!(f, "{action} on the key"),
            Self::KeyOrCluster(action) => write!(f, "{action} on the key, or cluster"),
            Self::Listing(actions) => write!(f, "{} per key listed, or cluster", either(actions)),
        }
    }
}

/// Whether a node certificate may make an RPC (ADR-0026).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ForNodes {
    /// Nodes may: every `node` RPC, and the client calls a daemon makes with its own
    /// certificate. The handler confines a node to itself where the call names a node or a
    /// lease.
    Allowed,
    /// Nodes may not: a node certificate is judged as a principal with no scopes, so one stolen
    /// node key can't read, write, administer, or stream a database, nor revoke a token.
    Refused,
}

/// One RPC and what it needs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Rpc {
    /// The gRPC path, `/fleetring.v1.<Service>/<Method>`.
    pub path: &'static str,
    /// What the caller needs.
    pub requirement: Requirement,
    /// Whether a node certificate may make it.
    pub nodes: ForNodes,
}

impl Rpc {
    /// The method's name, the part of the path after the service.
    #[must_use]
    pub fn method(&self) -> &'static str {
        self.path.rsplit('/').next().unwrap_or(self.path)
    }

    /// The service's full name, `fleetring.v1.<Service>`.
    #[must_use]
    pub fn service(&self) -> &'static str {
        let trimmed = self.path.strip_prefix('/').unwrap_or(self.path);
        trimmed.split('/').next().unwrap_or(trimmed)
    }
}

/// An RPC whose callers need `requirement`; nodes may make it only if it is a `node` RPC.
const fn rpc(path: &'static str, requirement: Requirement) -> Rpc {
    let nodes = match requirement {
        Requirement::Node => ForNodes::Allowed,
        _ => ForNodes::Refused,
    };
    Rpc {
        path,
        requirement,
        nodes,
    }
}

/// A client RPC that a daemon also makes with its own node certificate.
const fn node_too(path: &'static str, requirement: Requirement) -> Rpc {
    Rpc {
        path,
        requirement,
        nodes: ForNodes::Allowed,
    }
}

/// Every RPC the daemon serves, by service, with what it needs. The daemon README's scope table
/// and the proto comments state the same, and tests hold all three to the proto.
pub const RPCS: &[Rpc] = &[
    // Control.
    rpc("/fleetring.v1.Control/Join", Requirement::Node),
    rpc("/fleetring.v1.Control/Heartbeat", Requirement::Node),
    // Every data node follows the membership.
    node_too(
        "/fleetring.v1.Control/WatchMembership",
        Requirement::Cluster,
    ),
    rpc(
        "/fleetring.v1.Control/CreateDatabase",
        Requirement::Key(Action::Admin),
    ),
    rpc(
        "/fleetring.v1.Control/DropDatabase",
        Requirement::Key(Action::Admin),
    ),
    rpc(
        "/fleetring.v1.Control/ChangeDurability",
        Requirement::Key(Action::Admin),
    ),
    // A data node locates the databases it creates, serves, and drains; it computes the same
    // placement from the membership anyway.
    node_too(
        "/fleetring.v1.Control/Locate",
        Requirement::KeyOrCluster(Action::Read),
    ),
    rpc(
        "/fleetring.v1.Control/ListDatabases",
        Requirement::Listing(&[Action::Read]),
    ),
    rpc("/fleetring.v1.Control/AcquireLease", Requirement::Node),
    rpc("/fleetring.v1.Control/RenewLease", Requirement::Node),
    // A node leaves, and drains, itself when it shuts down; the handler refuses another node's
    // ID from a node certificate.
    node_too("/fleetring.v1.Control/Leave", Requirement::Cluster),
    rpc("/fleetring.v1.Control/StartPromotion", Requirement::Node),
    rpc("/fleetring.v1.Control/RecordFenced", Requirement::Node),
    rpc("/fleetring.v1.Control/CompletePromotion", Requirement::Node),
    node_too("/fleetring.v1.Control/Drain", Requirement::Cluster),
    // A draining node hands its own leases over; the handler refuses a lease it doesn't hold.
    node_too(
        "/fleetring.v1.Control/TransferLease",
        Requirement::Key(Action::Admin),
    ),
    rpc("/fleetring.v1.Control/RevokeToken", Requirement::Cluster),
    // Replication: nodes only.
    rpc("/fleetring.v1.Replication/Replicate", Requirement::Node),
    rpc("/fleetring.v1.Replication/Fence", Requirement::Node),
    rpc("/fleetring.v1.Replication/Snapshot", Requirement::Node),
    rpc("/fleetring.v1.Replication/Fetch", Requirement::Node),
    rpc("/fleetring.v1.Replication/Promote", Requirement::Node),
    // Database.
    rpc(
        "/fleetring.v1.Database/Execute",
        Requirement::Key(Action::Write),
    ),
    rpc(
        "/fleetring.v1.Database/Query",
        Requirement::Key(Action::Read),
    ),
    rpc(
        "/fleetring.v1.Database/Create",
        Requirement::Key(Action::Admin),
    ),
    rpc(
        "/fleetring.v1.Database/NodeStatus",
        Requirement::Listing(&[Action::Read, Action::Stream]),
    ),
    rpc(
        "/fleetring.v1.Database/WatchLeases",
        Requirement::Listing(&[Action::Read]),
    ),
    rpc(
        "/fleetring.v1.Database/Verify",
        Requirement::KeyOrCluster(Action::Read),
    ),
];

/// The RPC served at `path`, or `None` for a path the table doesn't list, which is refused.
#[must_use]
pub fn rpc_at(path: &str) -> Option<&'static Rpc> {
    RPCS.iter().find(|rpc| rpc.path == path)
}

/// How a principal proved who it is on a request.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Credential {
    /// A client certificate from the cluster authority; the scopes come from `[[principal]]`.
    Certificate,
    /// A bearer token; the scopes are the token's.
    Token {
        /// The token's ID, which audit lines name instead of the token.
        id: TokenId,
        /// The key that signed it.
        key: KeyId,
        /// When it expires, which ends any stream opened with it.
        expires_at: Timestamp,
    },
}

/// Who made a request, as the request boundary resolved it.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Caller {
    /// A node of the cluster, by the ID in its certificate. Nodes act with full scope.
    Node(NodeId),
    /// A principal with its scopes.
    Principal {
        /// The principal's name.
        name: Principal,
        /// What it may do.
        scopes: Scopes,
        /// How it proved who it is.
        credential: Credential,
    },
    /// Nobody: a plaintext connection without a token, which only a daemon started with
    /// `insecure_plaintext` accepts. It is not authorized against anything.
    Anonymous,
}

impl Caller {
    /// Whether the caller may see `key` in a listing filtered by `action`.
    #[must_use]
    pub fn sees(&self, action: Action, key: &DatabaseName) -> bool {
        match self {
            Self::Node(_) | Self::Anonymous => true,
            Self::Principal { scopes, .. } => {
                scopes.allows(Target::Cluster) || scopes.allows(Target::Key(action, key))
            }
        }
    }

    /// Checks that the caller may make `rpc` on `key`, the database the request names if the
    /// RPC names one.
    ///
    /// # Errors
    ///
    /// A [`Denied`] naming what was needed: the action and the key, never the caller's scopes.
    pub fn authorize(&self, rpc: &Rpc, key: Option<&DatabaseName>) -> Result<(), Denied> {
        let none = Scopes::none();
        let scopes = match self {
            Self::Anonymous => return Ok(()),
            Self::Node(_) if rpc.nodes == ForNodes::Allowed => return Ok(()),
            // Beyond what nodes do, a node certificate is a principal with no scopes.
            Self::Node(_) => &none,
            Self::Principal { scopes, .. } => scopes,
        };
        let denied = |needed: Needed| Denied {
            method: rpc.method(),
            needed,
        };
        match rpc.requirement {
            Requirement::Node => Err(denied(Needed::Node)),
            Requirement::Cluster if scopes.allows(Target::Cluster) => Ok(()),
            Requirement::Cluster => Err(denied(Needed::Cluster)),
            Requirement::KeyOrCluster(_) if scopes.allows(Target::Cluster) => Ok(()),
            Requirement::Key(action) | Requirement::KeyOrCluster(action) => match key {
                Some(key) if scopes.allows(Target::Key(action, key)) => Ok(()),
                Some(key) => Err(denied(Needed::Key(action, key.clone()))),
                None => Err(denied(Needed::Cluster)),
            },
            Requirement::Listing(actions)
                if scopes.allows(Target::Cluster)
                    || actions.iter().any(|action| scopes.covers_some(*action)) =>
            {
                Ok(())
            }
            Requirement::Listing(actions) => Err(denied(Needed::Some(actions))),
        }
    }
}

/// What a refused request needed.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Needed {
    /// A node certificate.
    Node,
    /// The `cluster` scope.
    Cluster,
    /// The action on the key.
    Key(Action, DatabaseName),
    /// The action on at least one key, or `cluster`.
    Some(&'static [Action]),
}

/// A refused request: the method and what it needed, never what the caller has.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{method} needs {}", needed_text(.needed))]
pub struct Denied {
    /// The method's name.
    pub method: &'static str,
    /// What it needed.
    pub needed: Needed,
}

fn needed_text(needed: &Needed) -> String {
    match needed {
        Needed::Node => "a node certificate".to_owned(),
        Needed::Cluster => "the cluster scope".to_owned(),
        Needed::Key(action, key) => format!("{action} on '{key}'"),
        Needed::Some(actions) => format!("{} on some key, or the cluster scope", either(actions)),
    }
}

impl Denied {
    /// The key the request was refused on, if it named one.
    #[must_use]
    pub fn key(&self) -> Option<&DatabaseName> {
        match &self.needed {
            Needed::Key(_, key) => Some(key),
            Needed::Node | Needed::Cluster | Needed::Some(_) => None,
        }
    }

    /// The `PERMISSION_DENIED` status the caller sees.
    #[must_use]
    pub fn to_status(&self) -> tonic::Status {
        tonic::Status::permission_denied(self.to_string())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use fleetring_proto::v1::{control_server, database_server, replication_server};

    use super::*;

    /// The protocol as written, so the table can't drift from it.
    const PROTO: &str = include_str!("../../../fleetring-proto/proto/fleetring/v1/fleetring.proto");

    /// Every `/package.Service/Method` path the proto declares.
    fn proto_paths() -> BTreeSet<String> {
        let mut package = "";
        let mut service = None;
        let mut paths = BTreeSet::new();
        for line in PROTO.lines().map(str::trim) {
            if let Some(rest) = line.strip_prefix("package ") {
                package = rest.trim_end_matches(';');
            } else if let Some(rest) = line.strip_prefix("service ") {
                service = rest.split_whitespace().next();
            } else if line == "}" {
                service = None;
            } else if let (Some(service), Some(rest)) = (service, line.strip_prefix("rpc ")) {
                let method = rest.split('(').next().unwrap_or(rest).trim();
                paths.insert(format!("/{package}.{service}/{method}"));
            }
        }
        paths
    }

    #[test]
    fn every_rpc_in_the_proto_has_exactly_one_requirement() {
        let declared = proto_paths();
        assert!(declared.len() > 20, "parsed too few rpcs: {declared:?}");
        let listed: BTreeSet<String> = RPCS.iter().map(|rpc| rpc.path.to_owned()).collect();
        assert_eq!(listed.len(), RPCS.len(), "an rpc is listed twice");
        let missing: Vec<_> = declared.difference(&listed).collect();
        assert!(
            missing.is_empty(),
            "rpcs without a scope requirement in auth::policy::RPCS: {missing:?}"
        );
        let stale: Vec<_> = listed.difference(&declared).collect();
        assert!(
            stale.is_empty(),
            "rpcs listed but not in the proto: {stale:?}"
        );
    }

    #[test]
    fn the_proto_comments_state_each_rpcs_requirement() {
        let mut service = None;
        let mut scope = None;
        let mut checked = 0;
        for line in PROTO.lines().map(str::trim) {
            if let Some(rest) = line.strip_prefix("service ") {
                service = rest.split_whitespace().next();
            } else if let Some(stated) = line.strip_prefix("// Scope: ") {
                scope = Some(stated.trim_end_matches('.'));
            } else if let (Some(service), Some(rest)) = (service, line.strip_prefix("rpc ")) {
                let method = rest.split('(').next().unwrap_or(rest).trim();
                let path = format!("/fleetring.v1.{service}/{method}");
                let rpc = rpc_at(&path).unwrap_or_else(|| panic!("{path} isn't in RPCS"));
                assert_eq!(
                    scope.take(),
                    Some(rpc.requirement.to_string().as_str()),
                    "the `// Scope:` line above {path}"
                );
                checked += 1;
            } else if !line.starts_with("//") {
                scope = None;
            }
        }
        assert_eq!(checked, RPCS.len());
    }

    #[test]
    fn the_daemon_readme_lists_each_rpcs_requirement() {
        const README: &str = include_str!("../../README.md");
        for rpc in RPCS {
            let service = rpc.service().trim_start_matches("fleetring.v1.");
            let row = format!("| `{service}.{}` | {} |", rpc.method(), rpc.requirement);
            assert!(
                README.contains(&row),
                "the daemon README lacks the row {row}"
            );
        }
    }

    #[test]
    fn the_table_names_the_generated_services() {
        let services: BTreeSet<&str> = RPCS.iter().map(Rpc::service).collect();
        let generated: BTreeSet<&str> = [
            control_server::SERVICE_NAME,
            replication_server::SERVICE_NAME,
            database_server::SERVICE_NAME,
        ]
        .into_iter()
        .collect();
        assert_eq!(services, generated);
        assert_eq!(
            rpc_at("/fleetring.v1.Database/Execute").map(Rpc::method),
            Some("Execute")
        );
        assert_eq!(rpc_at("/fleetring.v1.Database/Nope"), None);
    }

    #[test]
    fn replication_is_for_nodes_only() {
        for rpc in RPCS
            .iter()
            .filter(|rpc| rpc.service() == replication_server::SERVICE_NAME)
        {
            assert_eq!(rpc.requirement, Requirement::Node, "{}", rpc.path);
        }
    }

    fn principal(scopes: &[&str]) -> Caller {
        Caller::Principal {
            name: "app".parse().expect("a principal"),
            scopes: scopes
                .iter()
                .map(|scope| scope.parse().expect("a scope"))
                .collect(),
            credential: Credential::Certificate,
        }
    }

    fn call(path: &str) -> &'static Rpc {
        rpc_at(path).unwrap_or_else(|| panic!("{path} is in the table"))
    }

    fn key(text: &str) -> DatabaseName {
        text.parse().expect("a database name")
    }

    #[test]
    fn a_reader_of_one_tenant_reads_it_and_nothing_more() {
        let reader = principal(&["read:tenant:acme/*"]);
        let mine = key("tenant:acme/orders");
        let other = key("tenant:zed/orders");
        let ok = |path: &str, key: Option<&DatabaseName>| reader.authorize(call(path), key);

        assert_eq!(ok("/fleetring.v1.Database/Query", Some(&mine)), Ok(()));
        assert_eq!(ok("/fleetring.v1.Control/Locate", Some(&mine)), Ok(()));
        assert_eq!(ok("/fleetring.v1.Database/NodeStatus", None), Ok(()));
        assert_eq!(ok("/fleetring.v1.Database/WatchLeases", None), Ok(()));
        assert_eq!(ok("/fleetring.v1.Control/ListDatabases", None), Ok(()));

        let write = ok("/fleetring.v1.Database/Execute", Some(&mine)).expect_err("no write");
        assert_eq!(
            write.to_string(),
            "Execute needs write on 'tenant:acme/orders'"
        );
        assert_eq!(write.key(), Some(&mine));
        assert!(ok("/fleetring.v1.Database/Query", Some(&other)).is_err());
        for admin in [
            "/fleetring.v1.Control/CreateDatabase",
            "/fleetring.v1.Control/DropDatabase",
            "/fleetring.v1.Control/ChangeDurability",
            "/fleetring.v1.Control/TransferLease",
            "/fleetring.v1.Database/Create",
        ] {
            assert!(ok(admin, Some(&mine)).is_err(), "{admin}");
        }
        for rpc in RPCS
            .iter()
            .filter(|rpc| matches!(rpc.requirement, Requirement::Cluster | Requirement::Node))
        {
            assert!(reader.authorize(rpc, None).is_err(), "{}", rpc.path);
        }

        assert!(reader.sees(Action::Read, &mine));
        assert!(!reader.sees(Action::Read, &other));
        assert!(!reader.sees(Action::Stream, &mine));
    }

    #[test]
    fn an_admin_of_everything_creates_and_drops_but_runs_no_cluster_operation() {
        let admin = principal(&["admin:*"]);
        let db = key("anything");
        for path in [
            "/fleetring.v1.Control/CreateDatabase",
            "/fleetring.v1.Control/DropDatabase",
            "/fleetring.v1.Database/Create",
            "/fleetring.v1.Database/Execute",
        ] {
            assert_eq!(admin.authorize(call(path), Some(&db)), Ok(()), "{path}");
        }
        let drain = admin
            .authorize(call("/fleetring.v1.Control/Drain"), None)
            .expect_err("drain needs cluster");
        assert_eq!(drain.to_string(), "Drain needs the cluster scope");
    }

    #[test]
    fn cluster_runs_control_operations_and_sees_every_listing_but_no_key() {
        let operator = principal(&["cluster"]);
        for path in [
            "/fleetring.v1.Control/Drain",
            "/fleetring.v1.Control/Leave",
            "/fleetring.v1.Control/WatchMembership",
            "/fleetring.v1.Database/NodeStatus",
        ] {
            assert_eq!(operator.authorize(call(path), None), Ok(()), "{path}");
        }
        assert!(
            operator
                .authorize(call("/fleetring.v1.Control/Join"), None)
                .is_err()
        );
        assert!(
            operator
                .authorize(call("/fleetring.v1.Database/Query"), Some(&key("x")))
                .is_err()
        );
        assert!(operator.sees(Action::Stream, &key("x")));
        for path in [
            "/fleetring.v1.Control/Locate",
            "/fleetring.v1.Database/Verify",
        ] {
            assert_eq!(
                operator.authorize(call(path), Some(&key("tenant:any"))),
                Ok(()),
                "{path}"
            );
        }
        let reader = principal(&["read:tenant:acme/*"]);
        let verify = call("/fleetring.v1.Database/Verify");
        assert_eq!(
            reader.authorize(verify, Some(&key("tenant:acme/x"))),
            Ok(())
        );
        assert!(
            reader
                .authorize(verify, Some(&key("tenant:zed/x")))
                .is_err()
        );
    }

    #[test]
    fn a_stream_only_principal_sees_node_status_but_not_leases() {
        let streamer = principal(&["stream:ledger"]);
        assert_eq!(
            streamer.authorize(call("/fleetring.v1.Database/NodeStatus"), None),
            Ok(())
        );
        assert!(streamer.sees(Action::Stream, &key("ledger")));
        assert!(!streamer.sees(Action::Read, &key("ledger")));
        let leases = streamer
            .authorize(call("/fleetring.v1.Database/WatchLeases"), None)
            .expect_err("leases are read");
        assert_eq!(
            leases.to_string(),
            "WatchLeases needs read on some key, or the cluster scope"
        );
        let nobody = principal(&[]);
        let status = nobody
            .authorize(call("/fleetring.v1.Database/NodeStatus"), None)
            .expect_err("refused");
        assert_eq!(
            status.to_string(),
            "NodeStatus needs read or stream on some key, or the cluster scope"
        );
    }

    #[test]
    fn a_principal_without_scopes_is_refused_everything() {
        let nobody = principal(&[]);
        let db = key("x");
        for rpc in RPCS {
            assert!(nobody.authorize(rpc, Some(&db)).is_err(), "{}", rpc.path);
        }
        let listing = nobody
            .authorize(call("/fleetring.v1.Database/WatchLeases"), None)
            .expect_err("refused");
        assert_eq!(
            listing.to_status().code(),
            tonic::Code::PermissionDenied,
            "{listing}"
        );
    }

    #[test]
    fn plaintext_callers_pass_every_check() {
        let db = key("x");
        for rpc in RPCS {
            assert_eq!(
                Caller::Anonymous.authorize(rpc, Some(&db)),
                Ok(()),
                "{}",
                rpc.path
            );
        }
        assert!(Caller::Anonymous.sees(Action::Read, &db));
    }

    #[test]
    fn a_node_certificate_makes_node_calls_and_nothing_else() {
        let node = Caller::Node("n1".parse().expect("node id"));
        let db = key("tenant:acme");
        let allowed: BTreeSet<&str> = RPCS
            .iter()
            .filter(|rpc| node.authorize(rpc, Some(&db)).is_ok())
            .map(|rpc| rpc.path)
            .collect();
        let node_rpcs = RPCS
            .iter()
            .filter(|rpc| rpc.requirement == Requirement::Node)
            .map(|rpc| rpc.path);
        let daemon_calls = [
            "/fleetring.v1.Control/WatchMembership",
            "/fleetring.v1.Control/Locate",
            "/fleetring.v1.Control/Leave",
            "/fleetring.v1.Control/Drain",
            "/fleetring.v1.Control/TransferLease",
        ];
        let expected: BTreeSet<&str> = node_rpcs.chain(daemon_calls).collect();
        assert_eq!(allowed, expected);
        let refusals = [
            (
                "/fleetring.v1.Control/RevokeToken",
                "RevokeToken needs the cluster scope",
            ),
            (
                "/fleetring.v1.Control/DropDatabase",
                "DropDatabase needs admin on 'tenant:acme'",
            ),
            (
                "/fleetring.v1.Database/Execute",
                "Execute needs write on 'tenant:acme'",
            ),
            (
                "/fleetring.v1.Database/Query",
                "Query needs read on 'tenant:acme'",
            ),
        ];
        for (path, message) in refusals {
            let denied = node.authorize(call(path), Some(&db)).expect_err("refused");
            assert_eq!(denied.to_string(), message, "{path}");
        }
    }
}
