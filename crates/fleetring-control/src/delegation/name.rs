//! Agent names: a root agent's `agent-<id>`, and a delegated agent's `agent-<id>.<segment>…`.

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

use crate::{ParsePrincipalNameError, PrincipalName};

/// How many levels below its root a delegated agent may be (ADR-0047, point 2.5): the most
/// `.<segment>`s an agent name holds, and the most entries a delegated token's chain holds.
pub const MAX_DEPTH: usize = 8;

/// What every agent name starts with.
const AGENT_PREFIX: &str = "agent-";

/// The longest root agent ID or segment, in bytes (ADR-0043, point 8.5).
const MAX_SEGMENT_LEN: usize = 32;

/// The byte that separates a name's segments, which `.` alone does.
const SEPARATOR: char = '.';

/// Whether `part` is a root agent ID or a segment: 1 to 32 of `a-z`, `0-9`, and `_`.
fn is_segment(part: &str) -> bool {
    !part.is_empty()
        && part.len() <= MAX_SEGMENT_LEN
        && part
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
}

/// An agent's principal name (ADR-0047, "Words"): a *root agent name*, `agent-<id>`, or a
/// *delegated agent name*, a root agent name followed by one to [`MAX_DEPTH`] `.<segment>`.
///
/// The ID and every segment are 1 to 32 of `a-z`, `0-9`, and `_`, so neither holds `-` or `.`,
/// and the whole name is a [`PrincipalName`], at most 64 bytes. So `agent-<id>-` and
/// `agent-<id>.` each end the ID, and `.` appears in an agent name only between segments.
///
/// Names compare, order, and print as their text.
///
/// # Examples
///
/// ```
/// use fleetring_control::delegation::AgentName;
///
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let child: AgentName = "agent-42.c7".parse()?;
/// assert!(child.is_delegated());
/// assert_eq!(child.depth(), 1);
/// assert_eq!(child.root().as_str(), "agent-42");
/// assert_eq!(child.parent().map(|parent| parent.to_string()).as_deref(), Some("agent-42"));
/// assert_eq!(child.child("r1")?.as_str(), "agent-42.c7.r1");
/// assert!("agent-42.c-7".parse::<AgentName>().is_err());
/// assert!("workload.prod".parse::<AgentName>().is_err());
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct AgentName(PrincipalName);

impl AgentName {
    /// The root agent of this name's lineage, `agent-<id>`: the name itself for a root.
    #[must_use]
    pub fn root(&self) -> Self {
        let text = self.as_str();
        match text.split_once(SEPARATOR) {
            Some((root, _)) => Self::from_part(root),
            None => self.clone(),
        }
    }

    /// How many levels below its root the name is: the number of `.`s, 0 to [`MAX_DEPTH`].
    #[must_use]
    pub fn depth(&self) -> usize {
        self.as_str().matches(SEPARATOR).count()
    }

    /// The name less its last segment, the agent that delegated to this one; `None` for a root.
    #[must_use]
    pub fn parent(&self) -> Option<Self> {
        self.as_str()
            .rsplit_once(SEPARATOR)
            .map(|(parent, _)| Self::from_part(parent))
    }

    /// The name of this agent's child `segment`: this name followed by `.<segment>`.
    ///
    /// # Errors
    ///
    /// [`ParseAgentNameError::InvalidSegment`] unless `segment` is 1 to 32 of `a-z`, `0-9`, and
    /// `_`; [`ParseAgentNameError::TooDeep`] if this name is already [`MAX_DEPTH`] levels below
    /// its root; and a [`ParseAgentNameError::Principal`] error if the child's name would be
    /// longer than 64 bytes.
    pub fn child(&self, segment: &str) -> Result<Self, ParseAgentNameError> {
        if !is_segment(segment) {
            return Err(ParseAgentNameError::InvalidSegment);
        }
        format!("{self}{SEPARATOR}{segment}").parse()
    }

    /// Whether this is a delegated agent's name, one at least one level below its root.
    #[must_use]
    pub fn is_delegated(&self) -> bool {
        self.as_str().contains(SEPARATOR)
    }

    /// The name as a principal, as a certificate or a token's `sub` names it.
    #[must_use]
    pub fn as_principal(&self) -> &PrincipalName {
        &self.0
    }

    /// The name as text.
    #[must_use]
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }

    /// The name made of a leading part of a valid agent name, ending before a `.`.
    ///
    /// Such a part is `agent-<id>` followed by whole segments: non-empty, shorter than the name,
    /// and of the same bytes, so it is a principal name and an agent name without being parsed
    /// again.
    fn from_part(part: &str) -> Self {
        Self(PrincipalName::new_unchecked(part))
    }
}

impl PrincipalName {
    /// Whether this name is `ancestor` itself, or starts with `ancestor` followed by `.`
    /// (ADR-0047, "Words").
    ///
    /// It is the comparison a principal revocation (ADR-0047, point 11.1) and the connection
    /// rule for an issuer-signed token (point 4.1) both use: `agent-42.c7` is at or below
    /// `agent-42`, and `agent-420` is not. A service name such as `workload.prod` is at or below
    /// `workload` too, which only a revocation with `fleet` can use.
    ///
    /// ```
    /// use fleetring_control::PrincipalName;
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let root: PrincipalName = "agent-42".parse()?;
    /// assert!(root.is_at_or_below(&root));
    /// assert!("agent-42.c7".parse::<PrincipalName>()?.is_at_or_below(&root));
    /// assert!(!"agent-420".parse::<PrincipalName>()?.is_at_or_below(&root));
    /// assert!(!root.is_at_or_below(&"agent-42.c7".parse()?));
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn is_at_or_below(&self, ancestor: &Self) -> bool {
        self.as_str()
            .strip_prefix(ancestor.as_str())
            .is_some_and(|rest| rest.is_empty() || rest.starts_with(SEPARATOR))
    }
}

impl fmt::Display for AgentName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl TryFrom<PrincipalName> for AgentName {
    type Error = ParseAgentNameError;

    fn try_from(principal: PrincipalName) -> Result<Self, Self::Error> {
        let name = || principal.as_str().to_owned();
        let Some(rest) = principal.as_str().strip_prefix(AGENT_PREFIX) else {
            return Err(ParseAgentNameError::NotAgent { name: name() });
        };
        if !rest.split(SEPARATOR).all(is_segment) {
            return Err(ParseAgentNameError::Segment { name: name() });
        }
        let depth = rest.matches(SEPARATOR).count();
        if depth > MAX_DEPTH {
            return Err(ParseAgentNameError::TooDeep {
                name: name(),
                depth,
            });
        }
        Ok(Self(principal))
    }
}

impl TryFrom<String> for AgentName {
    type Error = ParseAgentNameError;

    fn try_from(text: String) -> Result<Self, Self::Error> {
        PrincipalName::try_from(text)?.try_into()
    }
}

impl FromStr for AgentName {
    type Err = ParseAgentNameError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        Self::try_from(text.to_owned())
    }
}

impl From<AgentName> for PrincipalName {
    fn from(name: AgentName) -> Self {
        name.0
    }
}

impl From<AgentName> for String {
    fn from(name: AgentName) -> Self {
        name.0.into()
    }
}

/// Why a name is not an [`AgentName`].
///
/// Only a name already known to be a principal name is quoted, so a message never repeats
/// arbitrary bytes.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum ParseAgentNameError {
    /// The text isn't a principal name: empty, longer than 64 bytes, or holding a character a
    /// principal name can't.
    #[error(transparent)]
    Principal(#[from] ParsePrincipalNameError),

    /// The name doesn't start with `agent-`.
    #[error("'{name}' isn't an agent name, which starts with 'agent-'")]
    NotAgent {
        /// The name.
        name: String,
    },

    /// The ID or a segment is empty, longer than 32 bytes, or holds a byte other than `a-z`,
    /// `0-9`, and `_`.
    #[error(
        "'{name}' isn't an agent name: after 'agent-', each part between dots is 1 to 32 of \
         a-z, 0-9, and '_'"
    )]
    Segment {
        /// The name.
        name: String,
    },

    /// The name is more than [`MAX_DEPTH`] levels below its root.
    #[error("'{name}' is {depth} levels below its root; an agent name is at most 8")]
    TooDeep {
        /// The name.
        name: String,
        /// How many levels below its root it is.
        depth: usize,
    },

    /// A segment given to [`AgentName::child`] isn't 1 to 32 of `a-z`, `0-9`, and `_`.
    #[error("a child's segment is 1 to 32 of a-z, 0-9, and '_'")]
    InvalidSegment,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn name(text: &str) -> AgentName {
        text.parse()
            .unwrap_or_else(|e| panic!("'{text}' is an agent name: {e}"))
    }

    #[test]
    fn root_parent_and_child_walk_the_lineage() {
        let deep = name("agent-42.c7.r1");
        assert_eq!(deep.root(), name("agent-42"));
        assert_eq!(deep.parent(), Some(name("agent-42.c7")));
        assert_eq!(name("agent-42").parent(), None);
        assert_eq!(name("agent-42").root(), name("agent-42"));
        assert_eq!(name("agent-42.c7").child("r1"), Ok(deep));
        for bad in ["", "c-7", "c.7", "C7", &"x".repeat(33)] {
            assert_eq!(
                name("agent-42").child(bad),
                Err(ParseAgentNameError::InvalidSegment),
                "{bad:?}"
            );
        }
    }

    #[test]
    fn a_child_beyond_the_depth_or_the_length_is_refused() {
        let mut at = name("agent-1");
        for _ in 0..MAX_DEPTH {
            at = at.child("a").expect("within the depth");
        }
        assert!(matches!(
            at.child("a"),
            Err(ParseAgentNameError::TooDeep { depth: 9, .. })
        ));
        let long = name(&format!("agent-{}", "x".repeat(32)));
        let long = long.child(&"y".repeat(25)).expect("64 bytes");
        assert_eq!(long.as_str().len(), 64);
        assert!(matches!(
            long.child("z"),
            Err(ParseAgentNameError::Principal(
                ParsePrincipalNameError::TooLong { .. }
            ))
        ));
    }

    #[test]
    fn names_round_trip_through_json() {
        let child = name("agent-42.c7");
        let json = serde_json::to_string(&child).expect("serializes");
        assert_eq!(json, r#""agent-42.c7""#);
        assert_eq!(
            serde_json::from_str::<AgentName>(&json).expect("deserializes"),
            child
        );
        assert!(serde_json::from_str::<AgentName>(r#""workload.prod""#).is_err());
    }
}
