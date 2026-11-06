//! The names certificates carry: the fleet, and the URI that names a node, a principal, or the
//! authority (ADR-0025).

use std::fmt;
use std::str::FromStr;

use fleetring::NodeId;
use rustls_pki_types::CertificateDer;
use x509_parser::extensions::GeneralName;
use x509_parser::prelude::{FromDer as _, X509Certificate};

/// The URI scheme of every subject alternative name the authority issues.
const SCHEME: &str = "fleetring://";

/// The longest fleet or principal name.
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
    /// The name starts with `agent-` and contains `.`, which is reserved for delegated agents.
    #[error(
        "'{name}' is reserved: '.' in a name that starts with agent- marks a delegated agent \
         (agent-<id>.<child>), which only a delegation creates; choose a name without '.'"
    )]
    ReservedAgent {
        /// The offending name.
        name: String,
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

/// A fleet's name: 1 to 64 lowercase letters, digits, `-`, `_`, or `.`.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct FleetName(String);

impl FleetName {
    /// Returns the name as text.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl FromStr for FleetName {
    type Err = NameError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        check_name("fleet name", text)?;
        Ok(Self(text.to_owned()))
    }
}

impl fmt::Display for FleetName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// A principal's name, as a client certificate carries it: 1 to 64 lowercase letters, digits,
/// `-`, `_`, or `.`.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Principal(String);

/// The prefix of an agent's principal name (ADR-0043, point 8.5).
pub const AGENT_PREFIX: &str = "agent-";

impl Principal {
    /// Returns the name as text.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Whether this is a *reserved dotted agent name*: it starts with `agent-` and contains `.`,
    /// which marks a delegated agent's lineage (`agent-42.c7`, ADR-0044 point 1.8). Only a
    /// delegation may create such a name, so from 0.7 no command issues one and no node accepts
    /// one, by certificate, token, or `[[principal]]` entry.
    ///
    /// ```
    /// use fleetringd::tls::Principal;
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// assert!("agent-42.c7".parse::<Principal>()?.is_reserved_agent_name());
    /// assert!(!"agent-42".parse::<Principal>()?.is_reserved_agent_name());
    /// assert!(!"workload.prod".parse::<Principal>()?.is_reserved_agent_name());
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn is_reserved_agent_name(&self) -> bool {
        self.0.starts_with(AGENT_PREFIX) && self.0.contains('.')
    }

    /// Refuses a reserved dotted agent name (see
    /// [`is_reserved_agent_name`](Self::is_reserved_agent_name)), for a command about to issue a
    /// credential for it.
    ///
    /// # Errors
    ///
    /// [`NameError::ReservedAgent`] for a reserved dotted agent name.
    pub fn refuse_reserved(&self) -> Result<(), NameError> {
        if self.is_reserved_agent_name() {
            return Err(NameError::ReservedAgent {
                name: self.0.clone(),
            });
        }
        Ok(())
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

/// The name control state records for a principal, as a database's creator or a description's
/// author (ADR-0041). Both follow the same grammar today, which a test pins; the conversion is
/// fallible so that a later widening of one can't panic a control handler.
impl TryFrom<&Principal> for fleetring_control::PrincipalName {
    type Error = fleetring_control::ParsePrincipalNameError;

    fn try_from(principal: &Principal) -> Result<Self, Self::Error> {
        principal.as_str().parse()
    }
}

impl<'de> serde::Deserialize<'de> for Principal {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let text = String::deserialize(deserializer)?;
        text.parse().map_err(serde::de::Error::custom)
    }
}

/// What a certificate names, as its `fleetring://` subject alternative name says.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum SubjectUri {
    /// `fleetring://<fleet>/node/<id>`: a node of the fleet.
    Node {
        /// The fleet.
        fleet: FleetName,
        /// The node.
        id: NodeId,
    },
    /// `fleetring://<fleet>/principal/<name>`: a client acting as a principal.
    Principal {
        /// The fleet.
        fleet: FleetName,
        /// The principal.
        name: Principal,
    },
    /// `fleetring://<fleet>/authority`: the fleet's certificate authority.
    Authority {
        /// The fleet.
        fleet: FleetName,
    },
}

impl SubjectUri {
    pub(crate) fn node(fleet: &FleetName, id: &NodeId) -> Self {
        Self::Node {
            fleet: fleet.clone(),
            id: id.clone(),
        }
    }

    pub(crate) fn principal(fleet: &FleetName, name: &Principal) -> Self {
        Self::Principal {
            fleet: fleet.clone(),
            name: name.clone(),
        }
    }

    pub(crate) fn authority(fleet: &FleetName) -> Self {
        Self::Authority {
            fleet: fleet.clone(),
        }
    }

    /// Returns the fleet the URI belongs to.
    #[must_use]
    pub fn fleet(&self) -> &FleetName {
        match self {
            Self::Node { fleet, .. }
            | Self::Principal { fleet, .. }
            | Self::Authority { fleet } => fleet,
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
        let (fleet, path) = rest.split_once('/').ok_or_else(malformed)?;
        let fleet: FleetName = fleet.parse()?;
        match path.split_once('/') {
            Some(("node", id)) => Ok(Self::Node {
                fleet,
                id: id.parse()?,
            }),
            Some(("principal", name)) => Ok(Self::Principal {
                fleet,
                name: name.parse()?,
            }),
            None if path == "authority" => Ok(Self::Authority { fleet }),
            _ => Err(malformed()),
        }
    }
}

impl fmt::Display for SubjectUri {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Node { fleet, id } => write!(f, "{SCHEME}{fleet}/node/{id}"),
            Self::Principal { fleet, name } => {
                write!(f, "{SCHEME}{fleet}/principal/{name}")
            }
            Self::Authority { fleet } => write!(f, "{SCHEME}{fleet}/authority"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_principal_converts_to_the_name_control_state_records() {
        let longest = "a".repeat(MAX_NAME_LEN);
        for text in ["agent-42", "a", "x_y.z-0", longest.as_str()] {
            let principal: Principal = text.parse().expect("a principal");
            let recorded = fleetring_control::PrincipalName::try_from(&principal)
                .expect("the two grammars are equal");
            assert_eq!(recorded.as_str(), text);
        }
    }

    #[test]
    fn subject_uris_round_trip() {
        for text in [
            "fleetring://lab/node/n1",
            "fleetring://lab/principal/alice",
            "fleetring://lab/authority",
        ] {
            let uri: SubjectUri = text.parse().expect(text);
            assert_eq!(uri.to_string(), text);
            assert_eq!(uri.fleet().as_str(), "lab");
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
        assert!("".parse::<FleetName>().is_err());
        assert!("has/slash".parse::<FleetName>().is_err());
        assert!("x".repeat(65).parse::<Principal>().is_err());
    }
}
