//! Revoked bearer tokens, signing keys, and principals: the denylist nodes check before they
//! trust a token (ADR-0025; ADR-0047, point 11).
//!
//! The control plane only records identifiers, how long to keep them, and how each entry was
//! added. It never sees a token or a key; verification happens at the request boundary, which
//! asks [`Revocations::refuses_token`] about each token it verifies.

use std::collections::{BTreeMap, btree_map};
use std::fmt;
use std::str::FromStr;

use serde::de::{self, SeqAccess, Visitor};
use serde::ser::SerializeSeq;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::delegation::{AgentName, ChainLink, SignerKind, TokenClaims};
use crate::{PrincipalName, Timestamp};

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

impl TokenId {
    /// The ID `text`, which the caller knows is a token ID because it built it from the ID
    /// grammar's own characters, as a delegation ID's base64url is. It checks nothing in a
    /// release build, so it can't fail on `apply`'s path.
    pub(crate) fn new_unchecked(text: String) -> Self {
        debug_assert!(
            check_id(&text).is_ok(),
            "'{text}' was assumed to be a token id"
        );
        Self(text.into_boxed_str())
    }
}

/// A fleet other than the local one, as a principal revocation may name it (ADR-0044, point
/// 3.8): 1 to 64 lowercase letters, digits, `-`, `_`, or `.`.
///
/// Revoking across fleets isn't in 0.8 (ADR-0047, point 11.8): [`apply`](crate::apply) never
/// writes a [`Revoked::Principal`] that names one, and a node that reads one treats the whole
/// denylist as unreadable. The type exists so that the variant's shape never changes.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct FleetRef(Box<str>);

/// Why a string is not a valid [`FleetRef`].
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum ParseFleetRefError {
    /// The name is empty, longer than 64 bytes, or holds a character outside `a-z 0-9 - _ .`.
    #[error("a fleet name is 1 to 64 characters of a-z, 0-9, '-', '_', or '.'")]
    Invalid,
}

impl FleetRef {
    /// Returns the fleet's name as text.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for FleetRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl FromStr for FleetRef {
    type Err = ParseFleetRefError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        let valid = !text.is_empty()
            && text.len() <= MAX_ID_LEN
            && text.bytes().all(|byte| {
                byte.is_ascii_lowercase() || byte.is_ascii_digit() || b"-_.".contains(&byte)
            });
        if valid {
            Ok(Self(text.into()))
        } else {
            Err(ParseFleetRefError::Invalid)
        }
    }
}

impl TryFrom<String> for FleetRef {
    type Error = ParseFleetRefError;

    fn try_from(text: String) -> Result<Self, Self::Error> {
        text.parse()
    }
}

impl From<FleetRef> for String {
    fn from(fleet: FleetRef) -> Self {
        fleet.0.into_string()
    }
}

/// What a revocation refuses: one token, every token a key signed, or every token a principal,
/// or a name below it, was issued until a time (ADR-0047, point 11.1).
///
/// It is the key of a denylist entry ([`Revocations`]). A principal's revocation keeps the time
/// it refuses tokens issued until, its `before`, in the entry ([`Entry::before`]), so the
/// denylist holds one entry per fleet and principal, raised in place by a later revocation.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Revoked {
    /// The token with this ID.
    Token(TokenId),
    /// Every token signed by the key with this ID.
    Key(KeyId),
    /// Every token whose subject is `principal` or a name below it (`agent-42` and
    /// `agent-42.c7`, never `agent-420`), issued until the entry's `before` (ADR-0047, point
    /// 3's comparison). Needs [`Feature::Delegation`](crate::Feature::Delegation).
    Principal {
        /// Another fleet, or `None` for the local one: always `None` in 0.8.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        fleet: Option<FleetRef>,
        /// The principal.
        principal: PrincipalName,
    },
}

impl Revoked {
    /// The local fleet's principal revocation of `principal`.
    #[must_use]
    pub fn principal(principal: PrincipalName) -> Self {
        Self::Principal {
            fleet: None,
            principal,
        }
    }
}

impl fmt::Display for Revoked {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Token(id) => write!(f, "token '{id}'"),
            Self::Key(id) => write!(f, "key '{id}'"),
            Self::Principal {
                fleet: None,
                principal,
            } => write!(f, "principal '{principal}'"),
            Self::Principal {
                fleet: Some(fleet),
                principal,
            } => write!(f, "principal '{principal}' of fleet '{fleet}'"),
        }
    }
}

/// How a denylist entry was added (ADR-0047, point 11.2).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum Added {
    /// With the `fleet` scope, or before 0.8: every entry a 0.7 fleet holds reads so. Such an
    /// entry is never replaced.
    Fleet,
    /// Under the ancestor rule (ADR-0047, point 11.3), by the agent `by` of the root lineage
    /// `root`. The lineage's cap and rates count it.
    Ancestor {
        /// The root agent of the lineage the entry was added in.
        root: AgentName,
        /// The agent that added it.
        by: AgentName,
    },
}

/// One denylist entry: until when it is kept, how it was added, and, for a principal, the time
/// it refuses tokens issued until (ADR-0047, point 11.2).
///
/// An entry added with `fleet` and holding nothing more serializes as 0.7's entries do,
/// `[revoked, until]`; any other as `[revoked, until, {added, before?, of?}]`, which only
/// [`apply`](crate::apply) with [`Feature::Delegation`](crate::Feature::Delegation) enabled
/// writes.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct Entry {
    /// When the entry may be forgotten.
    pub until: Timestamp,
    /// How it was added.
    pub added: Added,
    /// For a [`Revoked::Principal`] entry, and only for one: it refuses tokens issued at or
    /// before this control time (ADR-0047, point 11.5).
    pub before: Option<Timestamp>,
    /// For a [`Revoked::Token`] entry added under the ancestor rule for a delegation, the
    /// delegation's principal, which decides which principal entries replace it (ADR-0047,
    /// point 11.4). `None` for every other entry, a root token's included.
    pub of: Option<AgentName>,
}

impl Entry {
    /// An entry added with `fleet` that holds nothing but when it may be forgotten: a token's or
    /// a key's, as 0.7 writes them.
    #[must_use]
    pub fn fleet(until: Timestamp) -> Self {
        Self {
            until,
            added: Added::Fleet,
            before: None,
            of: None,
        }
    }

    /// A principal's entry added with `fleet`, refusing tokens issued at or before `before`, as a
    /// node reads one from the control plane.
    #[must_use]
    pub fn fleet_principal(until: Timestamp, before: Timestamp) -> Self {
        Self {
            before: Some(before),
            ..Self::fleet(until)
        }
    }

    /// Whether the entry is added with `fleet` and holds nothing more, 0.7's form.
    fn is_plain(&self) -> bool {
        self.added == Added::Fleet && self.before.is_none() && self.of.is_none()
    }

    /// Raises this entry with `other`, a later revocation of the same key: the later `until`
    /// and `before`, and `Fleet` if either was added with `fleet` (ADR-0047, point 11.2).
    /// Otherwise the first adder stays.
    fn raise(&mut self, other: Self) {
        self.until = self.until.max(other.until);
        self.before = self.before.max(other.before);
        if other.added == Added::Fleet || self.added == Added::Fleet {
            self.added = Added::Fleet;
            self.of = None;
        } else if self.of.is_none() {
            self.of = other.of;
        }
    }

    /// Why `self` can't be the entry of `revoked`, if it can't.
    fn invalid_for(&self, revoked: &Revoked) -> Option<InvalidEntry> {
        let principal = matches!(revoked, Revoked::Principal { .. });
        if principal != self.before.is_some() {
            return Some(InvalidEntry::Before);
        }
        let ancestor = matches!(self.added, Added::Ancestor { .. });
        let delegation_token = ancestor && matches!(revoked, Revoked::Token(_));
        if self.of.is_some() && !delegation_token {
            return Some(InvalidEntry::Of);
        }
        let fleet_wide = matches!(
            revoked,
            Revoked::Key(_) | Revoked::Principal { fleet: Some(_), .. }
        );
        if ancestor && fleet_wide {
            return Some(InvalidEntry::Ancestor);
        }
        None
    }
}

/// Why a stored denylist entry is refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
enum InvalidEntry {
    #[error("a principal's denylist entry, and only one, holds 'before'")]
    Before,
    #[error("only a delegation's token entry added by the ancestor rule holds 'of'")]
    Of,
    #[error("the ancestor rule adds no key's or other fleet's entry")]
    Ancestor,
}

/// The denylist: each revoked token, key, or principal with its [`Entry`].
///
/// A token's entry lasts until the token expires, after which verification refuses it anyway; a
/// key's usually lasts forever ([`Timestamp::MAX`]); a principal's as long as the tokens it may
/// refuse (ADR-0047, point 11.5). Entries leave the list once the control state's time passes
/// them, so short token lifetimes keep it small.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(try_from = "Vec<Listed>", into = "Vec<Listed>")]
pub struct Revocations {
    entries: BTreeMap<Revoked, Entry>,
}

impl Revocations {
    /// An empty denylist.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// The entry that refuses `token`, or `None` if the list refuses it by none (ADR-0047,
    /// point 3).
    ///
    /// A token is refused by its own ID (`Revoked::Token`) or key (`Revoked::Key`), by the ID or
    /// the key of any link in its chain, whatever keys signed which copies, so revoking an
    /// ancestor's token, or a key that signed an ancestor's copy, ends every token below it; and
    /// by a principal entry for its subject or any name its subject is below, when it was issued
    /// at or before that entry's `before`, judged by [`TokenRef::refused_by_principal`].
    ///
    /// The entries are tried in that order: the token's ID, its key, then each link, root first,
    /// its ID before its key, then each principal entry, the shortest name first. The first
    /// that matches is returned, for the audit line.
    ///
    /// The node verifying a token, the stream recheck, and [`apply`](crate::apply) all decide
    /// with this function.
    ///
    /// # Examples
    ///
    /// ```
    /// use fleetring_control::delegation::{ChainLink, SignerKind};
    /// use fleetring_control::{Revocations, Revoked, Timestamp, TokenRef};
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let root = ChainLink { id: "root-jti".parse()?, kid: "authority".parse()? };
    /// let child = TokenRef {
    ///     id: &"AAAAAAAAAAAAAAAAAAAAAA".parse()?,
    ///     kid: &"issuer-a".parse()?,
    ///     chain: &[root.clone()],
    ///     principal: &"agent-42.c7".parse()?,
    ///     issued_at: Some(Timestamp::from_millis(1_000)),
    ///     signer: SignerKind::Issuer,
    /// };
    /// let list: Revocations = [(Revoked::Token(root.id.clone()), Timestamp::MAX)].into_iter().collect();
    /// assert_eq!(list.refuses_token(&child), Some(Revoked::Token(root.id)));
    /// assert_eq!(Revocations::new().refuses_token(&child), None);
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn refuses_token(&self, token: &TokenRef<'_>) -> Option<Revoked> {
        self.walk(
            token.id,
            Some(token.kid),
            token.chain,
            token.principal,
            token.issued_at,
            token.signer,
        )
    }

    /// The entry that refuses every copy of the token `claims` describes, whatever key signs it,
    /// or `None`: [`refuses_token`](Self::refuses_token) without a copy's own key, with the
    /// signer its subject implies ([`SignerKind::for_principal`]).
    ///
    /// `apply` asks it of a recorded delegation before it answers a resend, which any control
    /// node may sign with any issuer key.
    pub(crate) fn refuses_every_copy(&self, claims: &TokenClaims) -> Option<Revoked> {
        self.walk(
            &claims.id,
            None,
            &claims.chain,
            &claims.principal,
            claims.issued_at,
            SignerKind::for_principal(&claims.principal),
        )
    }

    /// The one walk of the list both checks share: the token's ID, then `kid` if given, then
    /// each link of the chain, root first, its ID before its key, then each principal entry,
    /// the shortest name first.
    fn walk(
        &self,
        id: &TokenId,
        kid: Option<&KeyId>,
        chain: &[ChainLink],
        principal: &PrincipalName,
        issued_at: Option<Timestamp>,
        signer: SignerKind,
    ) -> Option<Revoked> {
        let own = std::iter::once(Revoked::Token(id.clone()))
            .chain(kid.map(|kid| Revoked::Key(kid.clone())));
        let chain = chain.iter().flat_map(|link| {
            [
                Revoked::Token(link.id.clone()),
                Revoked::Key(link.kid.clone()),
            ]
        });
        own.chain(chain)
            .find(|revoked| self.entries.contains_key(revoked))
            .or_else(|| self.principal_refusing(principal, issued_at, signer))
    }

    /// The principal entry that refuses a token naming `principal`, issued at `issued_at` and
    /// signed by `signer`, the shortest name first; `None` if none does. It is the principal
    /// half of [`refuses_token`](Self::refuses_token).
    pub(crate) fn principal_refusing(
        &self,
        principal: &PrincipalName,
        issued_at: Option<Timestamp>,
        signer: SignerKind,
    ) -> Option<Revoked> {
        // Principal entries sort last, so the list holds one only if its last entry is one;
        // most lists hold none, and the lookups below build a name each.
        let (last, _) = self.entries.last_key_value()?;
        if !matches!(last, Revoked::Principal { .. }) {
            return None;
        }
        at_or_above(principal)
            .map(Revoked::principal)
            .find(|revoked| {
                let (Some(entry), Revoked::Principal { principal: q, .. }) =
                    (self.entries.get(revoked), revoked)
                else {
                    return false;
                };
                // Every principal entry holds `before` ([`Entry::invalid_for`]); one without
                // would refuse everything, closed.
                let before = entry.before.unwrap_or(Timestamp::MAX);
                principal_covers(principal, issued_at, signer, q, before)
            })
    }

    /// Whether `revoked` is on the list.
    #[must_use]
    pub fn contains(&self, revoked: &Revoked) -> bool {
        self.entries.contains_key(revoked)
    }

    /// The entry of `revoked`, if it is on the list.
    #[must_use]
    pub fn get(&self, revoked: &Revoked) -> Option<&Entry> {
        self.entries.get(revoked)
    }

    /// Every entry, in order.
    pub fn iter(&self) -> impl ExactSizeIterator<Item = (&Revoked, &Entry)> {
        self.entries.iter()
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

    /// Adds `revoked` with `entry`, or raises its existing entry with it (ADR-0047, point 11.2),
    /// and returns the entry it now has.
    pub(crate) fn insert(&mut self, revoked: Revoked, entry: Entry) -> &Entry {
        debug_assert_eq!(entry.invalid_for(&revoked), None, "{entry:?} for {revoked}");
        match self.entries.entry(revoked) {
            btree_map::Entry::Vacant(vacant) => vacant.insert(entry),
            btree_map::Entry::Occupied(occupied) => {
                let kept = occupied.into_mut();
                kept.raise(entry);
                kept
            }
        }
    }

    /// Removes `revoked`'s entry.
    pub(crate) fn remove(&mut self, revoked: &Revoked) {
        self.entries.remove(revoked);
    }

    /// Forgets the entries kept until `now` or earlier.
    pub(crate) fn forget_through(&mut self, now: Timestamp) {
        self.entries.retain(|_, entry| entry.until > now);
    }
}

/// `name`, and every name it is below, the shortest first: each leading part that ends before a
/// `.`, then `name` itself. These are exactly the names `Q` for which `name.is_at_or_below(Q)`.
fn at_or_above(name: &PrincipalName) -> impl Iterator<Item = PrincipalName> + '_ {
    let text = name.as_str();
    text.match_indices('.')
        .filter(|(index, _)| *index > 0)
        .map(|(index, _)| PrincipalName::new_unchecked(&text[..index]))
        .chain(std::iter::once(name.clone()))
}

/// What the denylist needs of one signed copy of a token to decide whether it is revoked
/// ([`Revocations::refuses_token`]).
///
/// [`TokenClaims::token_ref`](crate::delegation::TokenClaims::token_ref) builds one from a
/// token's claims.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TokenRef<'a> {
    /// The token's ID, its `jti`.
    pub id: &'a TokenId,
    /// The ID of the key that signed this copy, its `kid`.
    pub kid: &'a KeyId,
    /// Its lineage, `fleetring.chain`, root first; empty for a token the authority signed.
    pub chain: &'a [ChainLink],
    /// Its subject, `sub`.
    pub principal: &'a PrincipalName,
    /// When it was issued, `iat`, in milliseconds, if it says.
    pub issued_at: Option<Timestamp>,
    /// Who signed this copy.
    pub signer: SignerKind,
}

impl TokenRef<'_> {
    /// Whether a principal revocation of `principal`, issued at control time `before`, refuses
    /// this token (ADR-0047, point 3 and point 11.1).
    ///
    /// It does when the token's subject, and so every ancestor named in it, is at or below
    /// `principal` ([`PrincipalName::is_at_or_below`]) and the token was issued at or before
    /// `before`: exactly for an issuer's token, whose `iat` is control time, and with
    /// [`SignerKind::issued_at_leeway`], 30 seconds, for the authority's, whose `iat` comes from
    /// its signer's clock. A token that doesn't say when it was issued counts as issued before
    /// every revocation, so it is refused.
    ///
    /// ```
    /// use fleetring_control::delegation::SignerKind;
    /// use fleetring_control::{Timestamp, TokenRef};
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let root = TokenRef {
    ///     id: &"root-jti".parse()?,
    ///     kid: &"authority".parse()?,
    ///     chain: &[],
    ///     principal: &"agent-42".parse()?,
    ///     issued_at: Some(Timestamp::from_millis(130_000)),
    ///     signer: SignerKind::Authority,
    /// };
    /// let before = Timestamp::from_millis(100_000);
    /// assert!(root.refused_by_principal(&"agent-42".parse()?, before));
    /// assert!(!root.refused_by_principal(&"agent-42.c7".parse()?, before));
    /// let late = TokenRef { issued_at: Some(Timestamp::from_millis(131_000)), ..root };
    /// assert!(!late.refused_by_principal(&"agent-42".parse()?, before));
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn refused_by_principal(&self, principal: &PrincipalName, before: Timestamp) -> bool {
        principal_covers(
            self.principal,
            self.issued_at,
            self.signer,
            principal,
            before,
        )
    }
}

/// Point 3's comparison: whether a principal revocation of `revoked` with `before` refuses a
/// token naming `subject`, issued at `issued_at` and signed by `signer`.
fn principal_covers(
    subject: &PrincipalName,
    issued_at: Option<Timestamp>,
    signer: SignerKind,
    revoked: &PrincipalName,
    before: Timestamp,
) -> bool {
    subject.is_at_or_below(revoked)
        && issued_at
            .is_none_or(|issued_at| issued_at <= before.saturating_add(signer.issued_at_leeway()))
}

/// One entry as the list serializes it (JSON map keys must be strings): `[revoked, until]` for
/// an entry in 0.7's form, `[revoked, until, {added, before?, of?}]` for any other.
#[derive(Debug)]
struct Listed(Revoked, Entry);

/// What an entry holds beyond 0.7's form.
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Detail {
    added: Added,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    before: Option<Timestamp>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    of: Option<AgentName>,
}

impl Serialize for Listed {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let Self(revoked, entry) = self;
        let plain = entry.is_plain();
        let mut seq = serializer.serialize_seq(Some(if plain { 2 } else { 3 }))?;
        seq.serialize_element(revoked)?;
        seq.serialize_element(&entry.until)?;
        if !plain {
            seq.serialize_element(&Detail {
                added: entry.added.clone(),
                before: entry.before,
                of: entry.of.clone(),
            })?;
        }
        seq.end()
    }
}

impl<'de> Deserialize<'de> for Listed {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Visit;

        impl<'de> Visitor<'de> for Visit {
            type Value = Listed;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a denylist entry, [revoked, until] or [revoked, until, detail]")
            }

            fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Listed, A::Error> {
                let revoked: Revoked = seq
                    .next_element()?
                    .ok_or_else(|| de::Error::invalid_length(0, &self))?;
                let until: Timestamp = seq
                    .next_element()?
                    .ok_or_else(|| de::Error::invalid_length(1, &self))?;
                let entry = match seq.next_element::<Detail>()? {
                    None => Entry::fleet(until),
                    Some(Detail { added, before, of }) => Entry {
                        until,
                        added,
                        before,
                        of,
                    },
                };
                if seq.next_element::<de::IgnoredAny>()?.is_some() {
                    return Err(de::Error::invalid_length(4, &self));
                }
                Ok(Listed(revoked, entry))
            }
        }

        deserializer.deserialize_seq(Visit)
    }
}

impl TryFrom<Vec<Listed>> for Revocations {
    type Error = String;

    fn try_from(listed: Vec<Listed>) -> Result<Self, Self::Error> {
        let mut list = Self::new();
        for Listed(revoked, entry) in listed {
            if let Some(invalid) = entry.invalid_for(&revoked) {
                return Err(format!("{revoked}: {invalid}"));
            }
            list.insert(revoked, entry);
        }
        Ok(list)
    }
}

impl From<Revocations> for Vec<Listed> {
    fn from(list: Revocations) -> Self {
        list.entries
            .into_iter()
            .map(|(revoked, entry)| Listed(revoked, entry))
            .collect()
    }
}

/// Entries added with `fleet`, kept until the time each is paired with; a principal's refuses
/// every token it covers that was issued before the entry is forgotten (`before` is `until`).
impl FromIterator<(Revoked, Timestamp)> for Revocations {
    fn from_iter<I: IntoIterator<Item = (Revoked, Timestamp)>>(entries: I) -> Self {
        entries
            .into_iter()
            .map(|(revoked, until)| {
                let entry = match revoked {
                    Revoked::Principal { .. } => Entry::fleet_principal(until, until),
                    Revoked::Token(_) | Revoked::Key(_) => Entry::fleet(until),
                };
                (revoked, entry)
            })
            .collect()
    }
}

/// The entries as given, each raised by any later one of the same key. An entry that can't be
/// its key's (a principal's without `before`, say) is mended rather than refused: `before` is
/// added or dropped, and `of` dropped, closed rather than open.
impl FromIterator<(Revoked, Entry)> for Revocations {
    fn from_iter<I: IntoIterator<Item = (Revoked, Entry)>>(entries: I) -> Self {
        let mut list = Self::new();
        for (revoked, mut entry) in entries {
            if entry.invalid_for(&revoked).is_some() {
                entry = match revoked {
                    Revoked::Principal { .. } => {
                        Entry::fleet_principal(entry.until, entry.before.unwrap_or(Timestamp::MAX))
                    }
                    Revoked::Token(_) | Revoked::Key(_) => Entry::fleet(entry.until),
                };
            }
            list.insert(revoked, entry);
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

    fn name(text: &str) -> PrincipalName {
        text.parse().expect("a principal name")
    }

    fn root_token<'a>(
        id: &'a TokenId,
        kid: &'a KeyId,
        principal: &'a PrincipalName,
    ) -> TokenRef<'a> {
        TokenRef {
            id,
            kid,
            chain: &[],
            principal,
            issued_at: Some(Timestamp::from_millis(1_000)),
            signer: SignerKind::Authority,
        }
    }

    #[test]
    fn a_revoked_token_or_key_is_refused_until_forgotten() {
        let mut list = Revocations::new();
        list.insert(
            Revoked::Token(token("t1")),
            Entry::fleet(Timestamp::from_millis(100)),
        );
        list.insert(Revoked::Key(key("k1")), Entry::fleet(Timestamp::MAX));
        let principal = name("svc");
        let refuses = |list: &Revocations, id: &str, kid: &str| {
            list.refuses_token(&root_token(&token(id), &key(kid), &principal))
                .is_some()
        };
        assert!(refuses(&list, "t1", "k2"));
        assert!(refuses(&list, "t2", "k1"));
        assert!(!refuses(&list, "t2", "k2"));

        list.forget_through(Timestamp::from_millis(99));
        assert_eq!(list.len(), 2);
        list.forget_through(Timestamp::from_millis(100));
        assert!(!refuses(&list, "t1", "k2"));
        assert!(refuses(&list, "t1", "k1"));
    }

    #[test]
    fn revoking_twice_keeps_the_later_time() {
        let mut list = Revocations::new();
        let revoked = Revoked::Token(token("t"));
        let mut until = |millis| {
            list.insert(
                revoked.clone(),
                Entry::fleet(Timestamp::from_millis(millis)),
            )
            .until
        };
        assert_eq!(until(50), Timestamp::from_millis(50));
        assert_eq!(until(10), Timestamp::from_millis(50));
        assert_eq!(until(70), Timestamp::from_millis(70));
    }

    #[test]
    fn a_principal_entry_refuses_its_name_and_below_issued_until_before() {
        let list: Revocations = [(
            Revoked::principal(name("agent-42")),
            Entry::fleet_principal(Timestamp::MAX, Timestamp::from_millis(10_000)),
        )]
        .into_iter()
        .collect();
        let (id, kid) = (token("t"), key("k"));
        for (subject, refused) in [
            ("agent-42", true),
            ("agent-42.c7", true),
            ("agent-42.c7.r1", true),
            ("agent-420", false),
            ("agent-4", false),
        ] {
            let principal = name(subject);
            let mut token = root_token(&id, &kid, &principal);
            token.signer = SignerKind::for_principal(&principal);
            assert_eq!(
                list.refuses_token(&token),
                refused.then(|| Revoked::principal(name("agent-42"))),
                "{subject}"
            );
        }
    }

    #[test]
    fn at_or_above_lists_every_name_a_name_is_below() {
        let names: Vec<String> = at_or_above(&name("agent-42.c7.r1"))
            .map(|name| name.to_string())
            .collect();
        assert_eq!(names, ["agent-42", "agent-42.c7", "agent-42.c7.r1"]);
        for ancestor in &names {
            assert!(name("agent-42.c7.r1").is_at_or_below(&name(ancestor)));
        }
        let odd: Vec<String> = at_or_above(&name(".a..b"))
            .map(|name| name.to_string())
            .collect();
        assert_eq!(odd, [".a", ".a.", ".a..b"]);
    }

    #[test]
    fn ids_are_short_and_url_safe() {
        assert!("abc-_XYZ09".parse::<TokenId>().is_ok());
        assert!("".parse::<TokenId>().is_err());
        assert!("a.b".parse::<KeyId>().is_err());
        assert!("x".repeat(65).parse::<KeyId>().is_err());
        assert!("x".repeat(64).parse::<KeyId>().is_ok());
        assert!("prod.eu-1".parse::<FleetRef>().is_ok());
        assert!("Prod".parse::<FleetRef>().is_err());
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

    #[test]
    fn a_stored_entry_that_cant_be_its_keys_is_refused() {
        for text in [
            r#"[[{"principal":{"principal":"agent-42"}},5]]"#,
            r#"[[{"token":"t"},5,{"added":"fleet","before":1}]]"#,
            r#"[[{"token":"t"},5,{"added":"fleet","of":"agent-42.c7"}]]"#,
            r#"[[{"key":"k"},5,{"added":{"ancestor":{"root":"agent-42","by":"agent-42"}}}]]"#,
            r#"[[{"token":"t"},5,{"added":"fleet"},1]]"#,
            r#"[[{"token":"t"},5,{"added":"fleet","later":1}]]"#,
        ] {
            assert!(serde_json::from_str::<Revocations>(text).is_err(), "{text}");
        }
    }
}
