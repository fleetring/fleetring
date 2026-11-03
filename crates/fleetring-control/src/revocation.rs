//! Revoked bearer tokens and signing keys: the denylist nodes check before they trust a token
//! (ADR-0025).
//!
//! The control plane only records identifiers and how long to keep them. It never sees a token,
//! a key, or a principal; verification happens at the request boundary.

use std::collections::BTreeMap;
use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

use crate::Timestamp;

/// The longest token or key ID accepted, in bytes.
const MAX_ID_LEN: usize = 64;

/// Why a string is not a valid [`TokenId`] or [`KeyId`].
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum ParseCredentialIdError {
    /// The ID is empty, longer than 64 bytes, or holds a character outside `A-Z a-z 0-9 - _`.
    #[error("a credential id is 1 to 64 characters of A-Z, a-z, 0-9, '-', or '_'")]
    Invalid,
}

fn check_id(text: &str) -> Result<(), ParseCredentialIdError> {
    let valid = !text.is_empty()
        && text.len() <= MAX_ID_LEN
        && text
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_');
    if valid {
        Ok(())
    } else {
        Err(ParseCredentialIdError::Invalid)
    }
}

macro_rules! credential_id {
    ($(#[$doc:meta])* $name:ident) => {
        $(#[$doc])*
        #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
        #[serde(try_from = "String", into = "String")]
        pub struct $name(Box<str>);

        impl $name {
            /// Returns the ID as text.
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
            type Err = ParseCredentialIdError;

            fn from_str(text: &str) -> Result<Self, Self::Err> {
                check_id(text)?;
                Ok(Self(text.into()))
            }
        }

        impl TryFrom<String> for $name {
            type Error = ParseCredentialIdError;

            fn try_from(text: String) -> Result<Self, Self::Error> {
                check_id(&text)?;
                Ok(Self(text.into_boxed_str()))
            }
        }

        impl From<$name> for String {
            fn from(id: $name) -> Self {
                id.0.into_string()
            }
        }
    };
}

credential_id!(
    /// A bearer token's ID, its `jti` claim: 1 to 64 characters of `A-Z a-z 0-9 - _`.
    ///
    /// ```
    /// use fleetring_control::TokenId;
    ///
    /// let id: TokenId = "Zr3x_Q-9".parse()?;
    /// assert_eq!(id.as_str(), "Zr3x_Q-9");
    /// assert!("has space".parse::<TokenId>().is_err());
    /// # Ok::<(), fleetring_control::ParseCredentialIdError>(())
    /// ```
    TokenId
);

credential_id!(
    /// The ID of a key that signs tokens, the `kid` header: 1 to 64 characters of
    /// `A-Z a-z 0-9 - _`.
    KeyId
);

/// What a revocation refuses: one token, or every token a key signed.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Revoked {
    /// The token with this ID.
    Token(TokenId),
    /// Every token signed by the key with this ID.
    Key(KeyId),
}

impl fmt::Display for Revoked {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Token(id) => write!(f, "token '{id}'"),
            Self::Key(id) => write!(f, "key '{id}'"),
        }
    }
}

/// The denylist: each revoked token or key with the time after which the entry may be forgotten.
///
/// A token's entry lasts until the token expires, after which verification refuses it anyway; a
/// key's usually lasts forever ([`Timestamp::MAX`]). Entries leave the list once the control
/// state's time passes them, so short token lifetimes keep it small.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(from = "Vec<(Revoked, Timestamp)>", into = "Vec<(Revoked, Timestamp)>")]
pub struct Revocations {
    entries: BTreeMap<Revoked, Timestamp>,
}

impl Revocations {
    /// An empty denylist.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Whether the token with `token` as its ID, signed by the key `key`, is revoked.
    #[must_use]
    pub fn refuses(&self, token: &TokenId, key: &KeyId) -> bool {
        self.entries.contains_key(&Revoked::Token(token.clone()))
            || self.entries.contains_key(&Revoked::Key(key.clone()))
    }

    /// Whether `revoked` is on the list.
    #[must_use]
    pub fn contains(&self, revoked: &Revoked) -> bool {
        self.entries.contains_key(revoked)
    }

    /// Every entry with the time it may be forgotten after, in order.
    pub fn iter(&self) -> impl ExactSizeIterator<Item = (&Revoked, Timestamp)> {
        self.entries
            .iter()
            .map(|(revoked, until)| (revoked, *until))
    }

    /// How many entries the list holds.
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether the list is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Adds `revoked` until `until`, keeping the later time if it is already listed, and returns
    /// the time it is now kept until.
    pub(crate) fn insert(&mut self, revoked: Revoked, until: Timestamp) -> Timestamp {
        let kept = self.entries.entry(revoked).or_insert(until);
        *kept = (*kept).max(until);
        *kept
    }

    /// Forgets the entries kept until `now` or earlier.
    pub(crate) fn forget_through(&mut self, now: Timestamp) {
        self.entries.retain(|_, until| *until > now);
    }
}

// JSON map keys must be strings, so the list serializes as `[revoked, until]` pairs in order.
impl From<Vec<(Revoked, Timestamp)>> for Revocations {
    fn from(entries: Vec<(Revoked, Timestamp)>) -> Self {
        entries.into_iter().collect()
    }
}

impl From<Revocations> for Vec<(Revoked, Timestamp)> {
    fn from(list: Revocations) -> Self {
        list.entries.into_iter().collect()
    }
}

impl FromIterator<(Revoked, Timestamp)> for Revocations {
    fn from_iter<I: IntoIterator<Item = (Revoked, Timestamp)>>(entries: I) -> Self {
        let mut list = Self::new();
        for (revoked, until) in entries {
            list.insert(revoked, until);
        }
        list
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn token(id: &str) -> TokenId {
        id.parse().expect("a token id")
    }

    fn key(id: &str) -> KeyId {
        id.parse().expect("a key id")
    }

    #[test]
    fn a_revoked_token_or_key_is_refused_until_forgotten() {
        let mut list = Revocations::new();
        list.insert(Revoked::Token(token("t1")), Timestamp::from_millis(100));
        list.insert(Revoked::Key(key("k1")), Timestamp::MAX);
        assert!(list.refuses(&token("t1"), &key("k2")));
        assert!(list.refuses(&token("t2"), &key("k1")));
        assert!(!list.refuses(&token("t2"), &key("k2")));

        list.forget_through(Timestamp::from_millis(99));
        assert_eq!(list.len(), 2);
        list.forget_through(Timestamp::from_millis(100));
        assert!(!list.refuses(&token("t1"), &key("k2")));
        assert!(list.refuses(&token("t1"), &key("k1")));
    }

    #[test]
    fn revoking_twice_keeps_the_later_time() {
        let mut list = Revocations::new();
        let revoked = Revoked::Token(token("t"));
        assert_eq!(
            list.insert(revoked.clone(), Timestamp::from_millis(50)),
            Timestamp::from_millis(50)
        );
        assert_eq!(
            list.insert(revoked.clone(), Timestamp::from_millis(10)),
            Timestamp::from_millis(50)
        );
        assert_eq!(
            list.insert(revoked, Timestamp::from_millis(70)),
            Timestamp::from_millis(70)
        );
    }

    #[test]
    fn ids_are_short_and_url_safe() {
        assert!("abc-_XYZ09".parse::<TokenId>().is_ok());
        assert!("".parse::<TokenId>().is_err());
        assert!("a.b".parse::<KeyId>().is_err());
        assert!("x".repeat(65).parse::<KeyId>().is_err());
        assert!("x".repeat(64).parse::<KeyId>().is_ok());
    }

    #[test]
    fn the_list_round_trips_through_json() {
        let list: Revocations = [
            (Revoked::Token(token("t1")), Timestamp::from_millis(5)),
            (Revoked::Key(key("k1")), Timestamp::MAX),
        ]
        .into_iter()
        .collect();
        let json = serde_json::to_string(&list).expect("serializes");
        let back: Revocations = serde_json::from_str(&json).expect("deserializes");
        assert_eq!(back, list);
        assert!(serde_json::from_str::<Revocations>(r#"[[{"token":"bad id"},1]]"#).is_err());
    }
}
