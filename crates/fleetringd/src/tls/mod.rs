//! Mutual TLS between nodes and clients, with the fleet's own certificate authority
//! (ADR-0025).
//!
//! Every connection is TLS 1.3 through `rustls`. A node presents a certificate the fleet's
//! [`Authority`] issued, whose URI subject alternative name `fleetring://<fleet>/node/<id>`
//! names it; a client's names its principal, `fleetring://<fleet>/principal/<name>`. Both ends
//! verify the other's certificate against the trusted authority certificates. The server side
//! learns who connected as a [`Peer`], which tonic places in the extensions of every request on
//! the connection, so each service can check the caller before it does anything.
//!
//! An [`Identity`] is the material one process presents; loaded from files, it reloads them
//! when they change, so rotation needs no restart. [`Security`] is the choice between an
//! identity and plaintext, which exists for the simulation harness and local experiments
//! only.
//!
//! A control node may also hold a delegation issuer pair (ADR-0047, point 1): an
//! [`IssuerPair`], whose [`IssuerCertificate`] the authority issued and which never opens a
//! connection. It too reloads when its files change.

#![deny(
    clippy::wildcard_enum_match_arm,
    reason = "a new subject kind, such as the issuer's, must be handled wherever a subject is judged"
)]

mod authority;
mod conn;
mod identity;
mod issuer;
mod issuer_files;
mod names;

use fleetring::NodeId;
use tonic::{Request, Status};

use crate::auth::Binding;

pub use authority::{
    AUTHORITY_CERT_FILE, AUTHORITY_KEY_FILE, Authority, AuthorityError, DEFAULT_AUTHORITY_LIFETIME,
    DEFAULT_CLIENT_LIFETIME, DEFAULT_ISSUER_LIFETIME, DEFAULT_NODE_LIFETIME, ISSUER_LIFETIME_SLACK,
    Issued, MAX_ISSUER_LIFETIME, Validity,
};
pub use conn::{Accepted, Conn, HandshakeError, Raw};
pub(crate) use conn::{HANDSHAKE_TIMEOUT, peer_of};
pub use identity::{Identity, IdentityError};
pub(crate) use issuer::IssuerCache;
pub use issuer::{IssuerCertError, IssuerCertificate};
pub use issuer_files::{
    DEV_ISSUER_CERT_FILE, DEV_ISSUER_KEY_FILE, DEV_ISSUER_LIFETIME, DEV_RENEW_BEFORE,
    DevIssuerOutcome, EXPIRY_WARNING_WINDOW, IssuerFileProblem, IssuerFiles, IssuerFilesError,
    IssuerPair, ensure_dev_pair, write_issuer_pair,
};
pub(crate) use issuer_files::{DEV_ISSUER_FILES, IssuerWatch};
pub use names::{AGENT_PREFIX, FleetName, NameError, Principal, SubjectUri};

/// Who a connection authenticated as.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Peer {
    /// A node of this fleet, by the ID in its certificate.
    Node(NodeId),
    /// A client acting as a principal, by the name in its certificate.
    Principal(Principal),
    /// Nobody: the connection is plaintext, which only `insecure_plaintext` allows.
    Plaintext,
}

impl Peer {
    /// The peer that sent `request`: what its connection authenticated as, or
    /// [`Peer::Plaintext`] on a connection that carried no identity.
    #[must_use]
    pub fn of<T>(request: &Request<T>) -> Self {
        request
            .extensions()
            .get::<Self>()
            .cloned()
            .unwrap_or(Self::Plaintext)
    }

    /// Checks that a call made on behalf of the node `claimed` comes from that node: a node
    /// certificate for another ID is refused, and a principal or a plaintext connection
    /// passes, because tokens and scopes decide for those (FR-047).
    ///
    /// # Errors
    ///
    /// `PERMISSION_DENIED`, naming the call and `claimed`, with
    /// [`BINDING_KEY`](crate::auth::BINDING_KEY) metadata naming the `self` binding, if the peer
    /// is a node other than `claimed`; the log names both IDs.
    pub fn check_node(&self, claimed: &NodeId, call: &'static str) -> Result<(), Status> {
        match self {
            Self::Node(id) if id != claimed => {
                tracing::warn!(
                    call,
                    claimed = %claimed,
                    certificate = %id,
                    "refused: the node id differs from the certificate's",
                );
                // Leaving or draining another member is an operator's call; the rest are the
                // node's own and nobody else's.
                let or_fleet = if matches!(call, "Leave" | "Drain") {
                    ", or the fleet scope"
                } else {
                    ""
                };
                let mut status = Status::permission_denied(format!(
                    "{call} for node '{claimed}' needs that node's certificate{or_fleet}"
                ));
                // The call is bound to `self` (ADR-0031), and says so as every binding refusal
                // does.
                status.metadata_mut().insert(
                    crate::auth::BINDING_KEY,
                    tonic::metadata::MetadataValue::from_static(Binding::SelfNode.as_str()),
                );
                Err(status)
            }
            Self::Node(_) | Self::Principal(_) | Self::Plaintext => Ok(()),
        }
    }

    /// Checks that a node-to-node call comes from a node: a principal is refused, and a
    /// plaintext connection passes.
    ///
    /// # Errors
    ///
    /// `PERMISSION_DENIED` if the peer is a principal.
    pub fn require_node(&self, call: &'static str) -> Result<(), Status> {
        match self {
            Self::Principal(name) => {
                tracing::warn!(call, principal = %name, "refused: nodes only");
                Err(Status::permission_denied(format!(
                    "{call} is for nodes; '{name}' is a principal"
                )))
            }
            Self::Node(_) | Self::Plaintext => Ok(()),
        }
    }
}

/// Whether connections carry an identity, or run in plaintext.
#[derive(Debug, Clone)]
pub enum Security {
    /// No TLS. Every peer is [`Peer::Plaintext`]. For the simulation harness and local
    /// experiments only; the daemon needs `insecure_plaintext` to run this way.
    Plaintext,
    /// Mutual TLS with this identity.
    Tls(Identity),
}

impl Security {
    /// Returns whether connections run in plaintext.
    #[must_use]
    pub fn is_plaintext(&self) -> bool {
        matches!(self, Self::Plaintext)
    }
}
