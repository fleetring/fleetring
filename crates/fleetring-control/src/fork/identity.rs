//! What names a fork: its operation ID and its fork identity, and the chain hash of its copy.

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

use crate::{DatabaseName, Incarnation};

/// The alphabet an [`OperationId`] is written in: Crockford's base32, lowercase, without the
/// letters `i`, `l`, `o`, and `u`.
const ALPHABET: &[u8; 32] = b"0123456789abcdefghjkmnpqrstvwxyz";

/// How many characters an [`OperationId`] is written in: 128 bits at 5 bits a character.
const OPERATION_ID_LEN: usize = 26;

/// The ID of a fork operation: 16 bytes the client chooses at random, written as 26 characters
/// of lowercase Crockford base32 (ADR-0046, point 5.1).
///
/// The client makes the ID before it asks, so that it can resend the same request after a lost
/// reply and get the same operation back. `apply` never makes one: it reads no randomness.
///
/// # Examples
///
/// ```
/// use fleetring_control::fork::OperationId;
///
/// let id = OperationId::from_bytes([0xAB; 16]);
/// let text = id.to_string();
/// assert_eq!(text.len(), 26);
/// assert_eq!(text.parse::<OperationId>()?, id);
/// assert!("not-an-id".parse::<OperationId>().is_err());
/// # Ok::<(), fleetring_control::fork::ParseOperationIdError>(())
/// ```
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct OperationId([u8; 16]);

impl OperationId {
    /// Returns the ID with these 16 bytes, read big-endian.
    #[must_use]
    pub const fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }

    /// Returns the ID's 16 bytes, big-endian.
    #[must_use]
    pub const fn to_bytes(self) -> [u8; 16] {
        self.0
    }
}

impl fmt::Debug for OperationId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "OperationId({self})")
    }
}

impl fmt::Display for OperationId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut text = [0_u8; OPERATION_ID_LEN];
        let mut value = u128::from_be_bytes(self.0);
        for slot in text.iter_mut().rev() {
            *slot = ALPHABET[usize::from(low_five_bits(value))];
            value >>= 5;
        }
        // Every byte comes from the ASCII alphabet.
        f.write_str(std::str::from_utf8(&text).map_err(|_| fmt::Error)?)
    }
}

/// The lowest five bits of `value`.
fn low_five_bits(value: u128) -> u8 {
    value.to_le_bytes()[0] & 0x1F
}

impl FromStr for OperationId {
    type Err = ParseOperationIdError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        let invalid = || ParseOperationIdError {
            text: text.to_owned(),
        };
        if text.len() != OPERATION_ID_LEN {
            return Err(invalid());
        }
        let mut value: u128 = 0;
        for (index, byte) in text.bytes().enumerate() {
            let digit = ALPHABET
                .iter()
                .position(|letter| *letter == byte)
                .and_then(|digit| u128::try_from(digit).ok())
                .ok_or_else(invalid)?;
            // The first character carries the top 3 of 128 bits; a larger one would overflow.
            if index == 0 && digit > 7 {
                return Err(invalid());
            }
            value = (value << 5) | digit;
        }
        Ok(Self(value.to_be_bytes()))
    }
}

impl TryFrom<String> for OperationId {
    type Error = ParseOperationIdError;

    fn try_from(text: String) -> Result<Self, Self::Error> {
        text.parse()
    }
}

impl From<OperationId> for String {
    fn from(id: OperationId) -> Self {
        id.to_string()
    }
}

/// Why a string is not an [`OperationId`].
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error(
    "'{text}' isn't an operation ID: expected 26 characters of lowercase base32 (0-9 and a-z \
     without i, l, o, u)"
)]
pub struct ParseOperationIdError {
    text: String,
}

/// A fork's identity: its name, its own incarnation, and its operation (ADR-0046, point 2).
///
/// Every command and report about a fork carries it, and `apply` refuses one whose identity
/// isn't the current database's before any other check, so a message meant for a dropped fork
/// can't touch a later database of the same name.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ForkIdentity {
    /// The fork's name.
    pub name: DatabaseName,
    /// The fork's own incarnation, 1 or more.
    pub incarnation: Incarnation,
    /// The operation that made it.
    pub operation: OperationId,
}

impl ForkIdentity {
    /// Returns the identity of the fork `name` at `incarnation`, made by `operation`.
    #[must_use]
    pub fn new(name: DatabaseName, incarnation: Incarnation, operation: OperationId) -> Self {
        Self {
            name,
            incarnation,
            operation,
        }
    }
}

impl fmt::Display for ForkIdentity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}@{} (operation {})",
            self.name, self.incarnation, self.operation
        )
    }
}

/// The chain hash of a copy at a position, as opaque bytes the engine defines (ADR-0016).
///
/// Control state compares chain hashes for equality only and never interprets them.
///
/// # Examples
///
/// ```
/// use fleetring_control::fork::ChainHash;
///
/// assert_eq!(ChainHash::new([1, 2]).as_bytes(), &[1, 2]);
/// ```
#[derive(Debug, Clone, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ChainHash(Vec<u8>);

impl ChainHash {
    /// Returns the chain hash encoded as `bytes`.
    #[must_use]
    pub fn new(bytes: impl Into<Vec<u8>>) -> Self {
        Self(bytes.into())
    }

    /// Returns the chain hash's bytes.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn operation_ids_round_trip_through_text_at_the_extremes() {
        for bytes in [[0; 16], [0xFF; 16], [0x5A; 16], {
            let mut one = [0; 16];
            one[15] = 1;
            one
        }] {
            let id = OperationId::from_bytes(bytes);
            let text = id.to_string();
            assert_eq!(text.len(), OPERATION_ID_LEN, "{text}");
            assert_eq!(text.parse::<OperationId>(), Ok(id), "{text}");
            assert_eq!(id.to_bytes(), bytes);
        }
        assert_eq!(
            OperationId::from_bytes([0xFF; 16]).to_string(),
            "7zzzzzzzzzzzzzzzzzzzzzzzzz"
        );
        assert_eq!(
            OperationId::from_bytes([0; 16]).to_string(),
            "00000000000000000000000000"
        );
    }

    #[test]
    fn malformed_operation_ids_are_refused() {
        for text in [
            "",
            "0000000000000000000000000",
            "000000000000000000000000000",
            "80000000000000000000000000",
            "0000000000000000000000000i",
            "0000000000000000000000000A",
            "0000000000000000000000000-",
        ] {
            assert!(text.parse::<OperationId>().is_err(), "{text}");
        }
    }

    #[test]
    fn operation_ids_serialize_as_their_text() {
        let id = OperationId::from_bytes([7; 16]);
        let json = serde_json::to_string(&id).expect("serializes");
        assert_eq!(json, format!("\"{id}\""));
        assert_eq!(serde_json::from_str::<OperationId>(&json).ok(), Some(id));
    }
}
