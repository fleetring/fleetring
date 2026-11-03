//! The names certificates carry: the cluster, and the URI that names a node, a principal, or the
//! authority (ADR-0025).

use std::fmt;
use std::str::FromStr;

use fleetring::NodeId;
use rustls_pki_types::CertificateDer;
use x509_parser::extensions::GeneralName;
use x509_parser::prelude::{FromDer as _, X509Certificate};

/// The URI scheme of every subject alternative name the authority issues.
const SCHEME: &str = "fleetring://";

/// The longest cluster or principal name.
const MAX_NAME_LEN: usize = 64;

/// The error returned when a name doesn't fit in a certificate, or a certificate names nothing.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum NameError {
    /// The name is empty, too long, or has a character outside `a-z`, `0-9`, `-`, `_`, and `.`.
    #[error("invalid {what} '{name}': 1 to 64 lowercase letters, digits, '-', '_', or '.'")]
    Invalid {
        /// What kind of name.
        what: &'static str,
        /// The offending name.
        name: String,
    },
    /// The URI isn't one the authority issues.
    #[error("not a fleetring subject URI: '{uri}'")]
    Uri {
        /// The offending URI.
        uri: String,
    },
    /// The URI names a node with an invalid ID.
    #[error("invalid node id in subject URI: {0}")]
    NodeId(#[from] fleetring::ParseLabelError),
    /// The certificate can't be parsed.
    #[error("the certificate can't be parsed")]
    Certificate,
    /// The certificate carries no fleetring URI, or several.
    #[error("the certificate names {count} fleetring subjects; expected one")]
    Subjects {
        /// How many fleetring URIs the certificate carries.
        count: usize,
    },
}

fn check_name(what: &'static str, name: &str) -> Result<(), NameError> {
    let valid = !name.is_empty()
        && name.len() <= MAX_NAME_LEN
        && name.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || b"-_.".contains(&byte)
        });
    if valid {
        Ok(())
    } else {
        Err(NameError::Invalid {
            what,
            name: name.to_owned(),
        })
    }
}

/// A cluster's name: 1 to 64 lowercase letters, digits, `-`, `_`, or `.`.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct ClusterName(String);

impl ClusterName {
    /// Returns the name as text.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl FromStr for ClusterName {
    type Err = NameError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        check_name("cluster name", text)?;
        Ok(Self(text.to_owned()))
    }
}

impl fmt::Display for ClusterName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// A principal's name, as a client certificate carries it: 1 to 64 lowercase letters, digits,
/// `-`, `_`, or `.`.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Principal(String);

impl Principal {
    /// Returns the name as text.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl FromStr for Principal {
    type Err = NameError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        check_name("principal", text)?;
        Ok(Self(text.to_owned()))
    }
}

impl fmt::Display for Principal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// What a certificate names, as its `fleetring://` subject alternative name says.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum SubjectUri {
    /// `fleetring://<cluster>/node/<id>`: a node of the cluster.
    Node {
        /// The cluster.
        cluster: ClusterName,
        /// The node.
        id: NodeId,
    },
    /// `fleetring://<cluster>/principal/<name>`: a client acting as a principal.
    Principal {
        /// The cluster.
        cluster: ClusterName,
        /// The principal.
        name: Principal,
    },
    /// `fleetring://<cluster>/authority`: the cluster's certificate authority.
    Authority {
        /// The cluster.
        cluster: ClusterName,
    },
}

impl SubjectUri {
    pub(crate) fn node(cluster: &ClusterName, id: &NodeId) -> Self {
        Self::Node {
            cluster: cluster.clone(),
            id: id.clone(),
        }
    }

    pub(crate) fn principal(cluster: &ClusterName, name: &Principal) -> Self {
        Self::Principal {
            cluster: cluster.clone(),
            name: name.clone(),
        }
    }

    pub(crate) fn authority(cluster: &ClusterName) -> Self {
        Self::Authority {
            cluster: cluster.clone(),
        }
    }

    /// Returns the cluster the URI belongs to.
    #[must_use]
    pub fn cluster(&self) -> &ClusterName {
        match self {
            Self::Node { cluster, .. }
            | Self::Principal { cluster, .. }
            | Self::Authority { cluster } => cluster,
        }
    }

    /// The one `fleetring://` URI among `certificate`'s subject alternative names.
    ///
    /// # Errors
    ///
    /// [`NameError`] if the certificate can't be parsed, carries no such URI or several, or the
    /// URI is malformed.
    pub fn of_certificate(certificate: &CertificateDer<'_>) -> Result<Self, NameError> {
        let (_, parsed) =
            X509Certificate::from_der(certificate.as_ref()).map_err(|_| NameError::Certificate)?;
        let names = parsed
            .subject_alternative_name()
            .map_err(|_| NameError::Certificate)?;
        let uris: Vec<&str> = names
            .map(|extension| {
                extension
                    .value
                    .general_names
                    .iter()
                    .filter_map(|name| match name {
                        GeneralName::URI(uri) if uri.starts_with(SCHEME) => Some(*uri),
                        _ => None,
                    })
                    .collect()
            })
            .unwrap_or_default();
        match uris.as_slice() {
            [uri] => uri.parse(),
            other => Err(NameError::Subjects { count: other.len() }),
        }
    }
}

impl FromStr for SubjectUri {
    type Err = NameError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        let malformed = || NameError::Uri {
            uri: text.to_owned(),
        };
        let rest = text.strip_prefix(SCHEME).ok_or_else(malformed)?;
        let (cluster, path) = rest.split_once('/').ok_or_else(malformed)?;
        let cluster: ClusterName = cluster.parse()?;
        match path.split_once('/') {
            Some(("node", id)) => Ok(Self::Node {
                cluster,
                id: id.parse()?,
            }),
            Some(("principal", name)) => Ok(Self::Principal {
                cluster,
                name: name.parse()?,
            }),
            None if path == "authority" => Ok(Self::Authority { cluster }),
            _ => Err(malformed()),
        }
    }
}

impl fmt::Display for SubjectUri {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Node { cluster, id } => write!(f, "{SCHEME}{cluster}/node/{id}"),
            Self::Principal { cluster, name } => {
                write!(f, "{SCHEME}{cluster}/principal/{name}")
            }
            Self::Authority { cluster } => write!(f, "{SCHEME}{cluster}/authority"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn subject_uris_round_trip() {
        for text in [
            "fleetring://lab/node/n1",
            "fleetring://lab/principal/alice",
            "fleetring://lab/authority",
        ] {
            let uri: SubjectUri = text.parse().expect(text);
            assert_eq!(uri.to_string(), text);
            assert_eq!(uri.cluster().as_str(), "lab");
        }
    }

    #[test]
    fn malformed_uris_are_refused() {
        for text in [
            "spiffe://lab/node/n1",
            "fleetring://lab",
            "fleetring://lab/worker/n1",
            "fleetring://Lab/node/n1",
            "fleetring://lab/node/N1",
            "fleetring://lab/authority/x",
            "fleetring:///node/n1",
        ] {
            assert!(text.parse::<SubjectUri>().is_err(), "{text}");
        }
    }

    #[test]
    fn names_are_checked() {
        assert!("tenant.a-1_x".parse::<Principal>().is_ok());
        assert!("".parse::<ClusterName>().is_err());
        assert!("has/slash".parse::<ClusterName>().is_err());
        assert!("x".repeat(65).parse::<Principal>().is_err());
    }
}
