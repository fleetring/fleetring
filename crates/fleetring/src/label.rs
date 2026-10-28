//! Validated labels: node IDs, zones, and hosts (ADR-0005).

use std::borrow::Borrow;
use std::fmt;
use std::str::FromStr;

/// The longest label allowed, in bytes. Long enough for UUIDs and hostnames; raising it later is
/// non-breaking, lowering it is not.
const MAX_LEN: usize = 64;

/// The kind of label that failed to parse.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum LabelKind {
    /// A [`NodeId`].
    NodeId,
    /// A [`Zone`].
    Zone,
    /// A [`Host`].
    Host,
}

impl fmt::Display for LabelKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::NodeId => "node id",
            Self::Zone => "zone",
            Self::Host => "host",
        })
    }
}

/// The error returned when a string is not a valid [`NodeId`], [`Zone`], or [`Host`].
///
/// Labels are 1 to 64 bytes of lowercase ASCII letters, digits, `-`, `_`, `.`, and `:`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseLabelError {
    kind: LabelKind,
    reason: Reason,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Reason {
    Empty,
    TooLong { len: usize },
    InvalidByte { byte: u8, index: usize },
}

impl ParseLabelError {
    /// Returns the kind of label that failed to parse.
    #[must_use]
    pub fn kind(&self) -> LabelKind {
        self.kind
    }
}

impl fmt::Display for ParseLabelError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let kind = self.kind;
        match self.reason {
            Reason::Empty => write!(f, "{kind} is empty"),
            Reason::TooLong { len } => {
                write!(f, "{kind} is {len} bytes long; the maximum is {MAX_LEN}")
            }
            Reason::InvalidByte { byte, index } => write!(
                f,
                "{kind} has invalid byte '{}' at index {index}; allowed are a-z, 0-9, '-', '_', '.', and ':'",
                byte.escape_ascii()
            ),
        }
    }
}

impl std::error::Error for ParseLabelError {}

fn validate(kind: LabelKind, label: &str) -> Result<(), ParseLabelError> {
    let error = |reason| ParseLabelError { kind, reason };
    if label.is_empty() {
        return Err(error(Reason::Empty));
    }
    if label.len() > MAX_LEN {
        return Err(error(Reason::TooLong { len: label.len() }));
    }
    match label.bytes().position(|byte| !is_label_byte(byte)) {
        Some(index) => Err(error(Reason::InvalidByte {
            byte: label.as_bytes()[index],
            index,
        })),
        None => Ok(()),
    }
}

const fn is_label_byte(byte: u8) -> bool {
    matches!(byte, b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b':')
}

/// Defines a validated label newtype. The three label types differ only in name and docs.
macro_rules! label {
    ($(#[$meta:meta])* pub struct $name:ident(LabelKind::$kind:ident);) => {
        $(#[$meta])*
        #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name(Box<str>);

        impl $name {
            /// Returns the label as a string slice.
            #[must_use]
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(&self.0)
            }
        }

        impl FromStr for $name {
            type Err = ParseLabelError;

            fn from_str(label: &str) -> Result<Self, Self::Err> {
                validate(LabelKind::$kind, label)?;
                Ok(Self(label.into()))
            }
        }

        impl TryFrom<&str> for $name {
            type Error = ParseLabelError;

            fn try_from(label: &str) -> Result<Self, Self::Error> {
                label.parse()
            }
        }

        impl TryFrom<String> for $name {
            type Error = ParseLabelError;

            fn try_from(label: String) -> Result<Self, Self::Error> {
                validate(LabelKind::$kind, &label)?;
                Ok(Self(label.into_boxed_str()))
            }
        }

        impl AsRef<str> for $name {
            fn as_ref(&self) -> &str {
                &self.0
            }
        }

        impl Borrow<str> for $name {
            fn borrow(&self) -> &str {
                &self.0
            }
        }
    };
}

label! {
    /// The stable identity of a node.
    ///
    /// Placement depends on node identity, so an ID is assigned once and kept for the node's
    /// lifetime. It is never a network address: addresses change, and a changed ID moves the
    /// node's data. To the ring, renaming a node is a removal plus an addition.
    ///
    /// IDs are 1 to 64 bytes of lowercase ASCII letters, digits, `-`, `_`, `.`, and `:`, which
    /// fits UUIDs and hostnames.
    ///
    /// # Examples
    ///
    /// ```
    /// use fleetring::NodeId;
    ///
    /// let id: NodeId = "laptop-1".parse()?;
    /// assert_eq!(id.as_str(), "laptop-1");
    /// assert!("Laptop 1".parse::<NodeId>().is_err());
    /// # Ok::<(), fleetring::ParseLabelError>(())
    /// ```
    pub struct NodeId(LabelKind::NodeId);
}

label! {
    /// A failure domain that replicas spread across, such as a rack, room, or cloud zone.
    ///
    /// Nodes without a zone share the default zone. Zones use the same grammar as [`NodeId`].
    ///
    /// # Examples
    ///
    /// ```
    /// use fleetring::Zone;
    ///
    /// let zone: Zone = "eu-west-1a".parse()?;
    /// assert_eq!(zone.to_string(), "eu-west-1a");
    /// # Ok::<(), fleetring::ParseLabelError>(())
    /// ```
    pub struct Zone(LabelKind::Zone);
}

label! {
    /// The physical machine a node runs on.
    ///
    /// Several nodes can share a host, for example one node per disk. Replicas avoid sharing a
    /// host whenever enough hosts exist. A node without a host label is its own host, and a host
    /// belongs to exactly one zone. Hosts use the same grammar as [`NodeId`].
    ///
    /// # Examples
    ///
    /// ```
    /// use fleetring::Host;
    ///
    /// let host = Host::try_from("nas.local")?;
    /// assert_eq!(host.as_str(), "nas.local");
    /// # Ok::<(), fleetring::ParseLabelError>(())
    /// ```
    pub struct Host(LabelKind::Host);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn reason(label: &str) -> Reason {
        validate(LabelKind::Zone, label)
            .expect_err("label should be rejected")
            .reason
    }

    #[test]
    fn reports_the_first_invalid_byte() {
        assert_eq!(
            reason("ab/cD"),
            Reason::InvalidByte {
                byte: b'/',
                index: 2
            }
        );
        assert_eq!(
            reason("é"),
            Reason::InvalidByte {
                byte: 0xc3,
                index: 0
            }
        );
    }

    #[test]
    fn checks_emptiness_then_length_then_bytes() {
        assert_eq!(reason(""), Reason::Empty);
        assert_eq!(reason(&"A".repeat(65)), Reason::TooLong { len: 65 });
    }

    #[test]
    fn accepts_the_boundaries_of_the_grammar() {
        for label in ["a", "0", "-", "_", ".", ":", "z9", &"x".repeat(MAX_LEN)] {
            assert_eq!(validate(LabelKind::Host, label), Ok(()), "{label:?}");
        }
    }
}
