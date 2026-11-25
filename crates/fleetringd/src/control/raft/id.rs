//! Raft IDs: the XXH3-64 hash of a control node's fleet node ID (ADR-0035 point 2), written as
//! 16 lowercase hex digits wherever they are stored or sent (ADR-0040 point 3).

use twox_hash::XxHash3_64;

use crate::control::raft::RaftId;

/// The number of hex digits in a Raft ID's text form.
const TEXT_LEN: usize = 16;

/// Returns the Raft ID of the control node whose fleet node ID is `node_id`: the XXH3-64 hash of
/// its UTF-8 bytes.
///
/// Nothing is stored: a node removed and added again gets the same Raft ID.
#[must_use]
pub fn raft_id(node_id: &str) -> RaftId {
    XxHash3_64::oneshot(node_id.as_bytes())
}

/// The error returned when a node's Raft ID can't be used in a control group.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum RaftIdError {
    /// The node's ID hashes to 0, which openraft can't use.
    #[error("node '{node}' has raft id 0; give the node another id")]
    Zero {
        /// The node.
        node: String,
    },
    /// The node's ID hashes to the Raft ID of another member of the group.
    #[error("node '{node}' and node '{other}' share raft id {}; give one of them another id", raft_id_text(*.id))]
    Collision {
        /// The node being checked.
        node: String,
        /// The member it collides with.
        other: String,
        /// The Raft ID both hash to.
        id: RaftId,
    },
}

/// Returns `node`'s Raft ID, or why it can't join a group whose members are `members` (fleet
/// node IDs; `node` itself among them is ignored).
///
/// # Errors
///
/// [`RaftIdError::Zero`] if the ID hashes to 0, and [`RaftIdError::Collision`] if it hashes to
/// another member's Raft ID.
pub fn check_raft_id<'m>(
    node: &str,
    members: impl IntoIterator<Item = &'m str>,
) -> Result<RaftId, RaftIdError> {
    let id = raft_id(node);
    if id == 0 {
        return Err(RaftIdError::Zero { node: node.into() });
    }
    for other in members {
        if other != node && raft_id(other) == id {
            return Err(RaftIdError::Collision {
                node: node.into(),
                other: other.into(),
                id,
            });
        }
    }
    Ok(id)
}

/// Returns `id` as 16 lowercase hex digits.
#[must_use]
pub fn raft_id_text(id: RaftId) -> String {
    format!("{id:016x}")
}

/// The error returned when a Raft ID's text isn't 16 lowercase hex digits.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("raft id '{text}' is not 16 lowercase hex digits")]
pub struct ParseRaftIdError {
    text: String,
}

impl ParseRaftIdError {
    /// Returns the text that failed to parse.
    #[must_use]
    pub fn text(&self) -> &str {
        &self.text
    }
}

/// Parses a Raft ID written by [`raft_id_text`].
///
/// # Errors
///
/// [`ParseRaftIdError`] unless `text` is exactly 16 lowercase hex digits.
pub fn parse_raft_id(text: &str) -> Result<RaftId, ParseRaftIdError> {
    let well_formed = text.len() == TEXT_LEN
        && text
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte));
    if !well_formed {
        return Err(ParseRaftIdError { text: text.into() });
    }
    RaftId::from_str_radix(text, 16).map_err(|_| ParseRaftIdError { text: text.into() })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn raft_ids_are_stable() {
        // Pasted once from this build: a change of hash or input bytes would move every group.
        assert_eq!(raft_id_text(raft_id("n1")), "d733a26dd4b49b94");
        assert_eq!(raft_id(""), XxHash3_64::oneshot(b""));
    }

    #[test]
    fn a_collision_names_both_nodes() {
        // No two short IDs collide in practice, so check the message on a hand-built error, and
        // that a node never collides with itself.
        assert_eq!(check_raft_id("n1", ["n1", "n2"]), Ok(raft_id("n1")));
        let error = RaftIdError::Collision {
            node: "n1".into(),
            other: "n2".into(),
            id: 0x00ab,
        };
        assert_eq!(
            error.to_string(),
            "node 'n1' and node 'n2' share raft id 00000000000000ab; give one of them another id"
        );
    }

    #[test]
    fn raft_id_text_round_trips_and_is_sixteen_hex_digits() {
        for id in [
            0,
            1,
            0xab,
            u64::from(u32::MAX),
            1 << 63,
            u64::MAX,
            raft_id("n1"),
        ] {
            let text = raft_id_text(id);
            assert_eq!(text.len(), 16, "{text}");
            assert!(
                text.bytes()
                    .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
            );
            assert_eq!(parse_raft_id(&text), Ok(id));
        }
        for bad in [
            "",
            "ab",
            "00000000000000AB",
            "+0000000000000ab",
            "0x000000000000ab",
            "00000000000000000",
        ] {
            assert!(parse_raft_id(bad).is_err(), "{bad}");
        }
    }
}
