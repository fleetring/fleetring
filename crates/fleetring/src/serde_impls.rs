//! serde support for the types that ring specs and cells specs hold, behind the `serde` feature
//! (ADR-0008).
//!
//! Each type deserializes through its validating constructor, so serde can never build a value
//! the constructor would reject: labels must match the label grammar, weights and replication
//! factors must be nonzero, and a directory's rules must be well formed and distinct. Errors
//! carry the constructor's reason.

use std::fmt;
use std::marker::PhantomData;
use std::str::FromStr;

use serde::de::value::MapAccessDeserializer;
use serde::de::{self, Deserialize, Deserializer, MapAccess, Unexpected, Visitor};
use serde::ser::{Serialize, SerializeStruct, Serializer};

use crate::{
    CellId, Directory, Epoch, Fingerprint, Host, LabelKind, NodeId, ParseLabelError,
    ReplicationFactor, Weight, Zone,
};

/// Implements serde for label types: each is a string that must match the label grammar.
macro_rules! label_serde {
    ($($label:ident),* $(,)?) => {$(
        impl Serialize for $label {
            fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                serializer.serialize_str(self.as_str())
            }
        }

        impl<'de> Deserialize<'de> for $label {
            fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
                deserializer.deserialize_str(LabelVisitor::<$label>::new(LabelKind::$label))
            }
        }
    )*};
}

label_serde!(NodeId, Zone, Host, CellId);

/// Deserializes a label of type `T`, rejecting strings outside the label grammar.
struct LabelVisitor<T> {
    kind: LabelKind,
    label: PhantomData<fn() -> T>,
}

impl<T> LabelVisitor<T> {
    fn new(kind: LabelKind) -> Self {
        Self {
            kind,
            label: PhantomData,
        }
    }
}

impl<T> Visitor<'_> for LabelVisitor<T>
where
    T: FromStr<Err = ParseLabelError> + TryFrom<String, Error = ParseLabelError>,
{
    type Value = T;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "a {}", self.kind)
    }

    fn visit_str<E: de::Error>(self, label: &str) -> Result<T, E> {
        label.parse().map_err(E::custom)
    }

    fn visit_string<E: de::Error>(self, label: String) -> Result<T, E> {
        T::try_from(label).map_err(E::custom)
    }
}

impl Serialize for Weight {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_u32(self.get())
    }
}

impl<'de> Deserialize<'de> for Weight {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = u32::deserialize(deserializer)?;
        Self::new(value).ok_or_else(|| {
            de::Error::invalid_value(Unexpected::Unsigned(u64::from(value)), &"a nonzero weight")
        })
    }
}

impl Serialize for ReplicationFactor {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_u8(self.get())
    }
}

impl<'de> Deserialize<'de> for ReplicationFactor {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = u8::deserialize(deserializer)?;
        Self::new(value).ok_or_else(|| {
            de::Error::invalid_value(
                Unexpected::Unsigned(u64::from(value)),
                &"a nonzero replication factor",
            )
        })
    }
}

impl Serialize for Epoch {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_u64(self.get())
    }
}

impl<'de> Deserialize<'de> for Epoch {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        u64::deserialize(deserializer).map(Self::new)
    }
}

impl Serialize for Fingerprint {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for Fingerprint {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_str(FingerprintVisitor)
    }
}

/// Deserializes a fingerprint from its text form, 16 lowercase hex digits.
struct FingerprintVisitor;

impl Visitor<'_> for FingerprintVisitor {
    type Value = Fingerprint;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("a fingerprint of 16 lowercase hex digits")
    }

    fn visit_str<E: de::Error>(self, text: &str) -> Result<Fingerprint, E> {
        text.parse().map_err(E::custom)
    }
}

impl Serialize for Directory {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        DirectoryOut {
            exact: self
                .exact_rules()
                .map(|(key, cell)| ExactRuleOut {
                    key: RuleBytesOut(key),
                    cell,
                })
                .collect(),
            prefix: self
                .prefix_rules()
                .map(|(prefix, cell)| PrefixRuleOut {
                    prefix: RuleBytesOut(prefix),
                    cell,
                })
                .collect(),
        }
        .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for Directory {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let DirectoryIn { exact, prefix } = DirectoryIn::deserialize(deserializer)?;
        let mut directory = Directory::new();
        // Adding a rule for bytes that already have one replaces it, leaving the count unchanged.
        for ExactRuleIn { key, cell } in exact {
            let rules = directory.len();
            directory = directory.exact(&key.0, cell);
            if directory.len() == rules {
                return Err(de::Error::custom(format_args!(
                    "directory has two exact rules for key \"{}\"",
                    key.0.escape_ascii()
                )));
            }
        }
        for PrefixRuleIn { prefix, cell } in prefix {
            let rules = directory.len();
            directory = directory.prefix(&prefix.0, cell);
            if directory.len() == rules {
                return Err(de::Error::custom(format_args!(
                    "directory has two prefix rules for prefix \"{}\"",
                    prefix.0.escape_ascii()
                )));
            }
        }
        Ok(directory)
    }
}

/// A directory as it serializes: its exact rules, then its prefix rules, each sorted by bytes.
#[derive(serde::Serialize)]
struct DirectoryOut<'a> {
    exact: Vec<ExactRuleOut<'a>>,
    prefix: Vec<PrefixRuleOut<'a>>,
}

#[derive(serde::Serialize)]
struct ExactRuleOut<'a> {
    key: RuleBytesOut<'a>,
    cell: &'a CellId,
}

#[derive(serde::Serialize)]
struct PrefixRuleOut<'a> {
    prefix: RuleBytesOut<'a>,
    cell: &'a CellId,
}

/// Serializes a rule's key or prefix as a string when its bytes are valid UTF-8, and otherwise
/// as an object holding them as lowercase hex digits: `{"hex": "ff00"}`.
struct RuleBytesOut<'a>(&'a [u8]);

impl Serialize for RuleBytesOut<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match std::str::from_utf8(self.0) {
            Ok(text) => serializer.serialize_str(text),
            Err(_) => {
                let mut object = serializer.serialize_struct("Hex", 1)?;
                object.serialize_field("hex", &HexOut(self.0))?;
                object.end()
            }
        }
    }
}

/// Serializes bytes as lowercase hex digits, two per byte.
struct HexOut<'a>(&'a [u8]);

impl Serialize for HexOut<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

impl fmt::Display for HexOut<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.iter().try_for_each(|byte| write!(f, "{byte:02x}"))
    }
}

/// A directory as it deserializes, before its rules are checked for duplicates.
///
/// A directory carries no fingerprint of its own, so every object in it rejects unknown fields
/// rather than risk dropping a rule that a newer writer meant.
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct DirectoryIn {
    exact: Vec<ExactRuleIn>,
    prefix: Vec<PrefixRuleIn>,
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct ExactRuleIn {
    key: RuleBytesIn,
    cell: CellId,
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct PrefixRuleIn {
    prefix: RuleBytesIn,
    cell: CellId,
}

/// A rule's key or prefix: a string's bytes, or the bytes an object `{"hex": ...}` spells.
struct RuleBytesIn(Vec<u8>);

impl<'de> Deserialize<'de> for RuleBytesIn {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_any(RuleBytesVisitor)
    }
}

/// Deserializes a rule's key or prefix from either of its forms.
struct RuleBytesVisitor;

impl<'de> Visitor<'de> for RuleBytesVisitor {
    type Value = RuleBytesIn;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("a string, or an object whose `hex` field holds lowercase hex digits")
    }

    fn visit_str<E: de::Error>(self, text: &str) -> Result<RuleBytesIn, E> {
        Ok(RuleBytesIn(text.as_bytes().to_vec()))
    }

    fn visit_string<E: de::Error>(self, text: String) -> Result<RuleBytesIn, E> {
        Ok(RuleBytesIn(text.into_bytes()))
    }

    fn visit_map<A: MapAccess<'de>>(self, map: A) -> Result<RuleBytesIn, A::Error> {
        let HexIn { hex } = HexIn::deserialize(MapAccessDeserializer::new(map))?;
        Ok(RuleBytesIn(hex.0))
    }
}

/// The object form of a rule's key or prefix.
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct HexIn {
    hex: HexBytes,
}

/// Bytes written as lowercase hex digits, two per byte.
struct HexBytes(Vec<u8>);

impl<'de> Deserialize<'de> for HexBytes {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_str(HexVisitor)
    }
}

/// Deserializes bytes from lowercase hex digits, rejecting any other text.
struct HexVisitor;

impl Visitor<'_> for HexVisitor {
    type Value = HexBytes;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("lowercase hex digits, two per byte")
    }

    fn visit_str<E: de::Error>(self, digits: &str) -> Result<HexBytes, E> {
        decode_hex(digits.as_bytes())
            .map(HexBytes)
            .map_err(E::custom)
    }
}

/// Decodes lowercase hex digits, two per byte.
fn decode_hex(digits: &[u8]) -> Result<Vec<u8>, HexError> {
    if !digits.len().is_multiple_of(2) {
        return Err(HexError::OddLength { len: digits.len() });
    }
    let nibble = |index: usize| {
        let byte = digits[index];
        match byte {
            b'0'..=b'9' => Ok(byte - b'0'),
            b'a'..=b'f' => Ok(byte - b'a' + 10),
            _ => Err(HexError::InvalidByte { byte, index }),
        }
    };
    (0..digits.len())
        .step_by(2)
        .map(|index| Ok((nibble(index)? << 4) | nibble(index + 1)?))
        .collect()
}

/// Why text isn't lowercase hex.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HexError {
    OddLength { len: usize },
    InvalidByte { byte: u8, index: usize },
}

impl fmt::Display for HexError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            Self::OddLength { len } => {
                write!(f, "hex has odd length {len}; each byte takes two digits")
            }
            Self::InvalidByte { byte, index } => write!(
                f,
                "hex has invalid byte '{}' at index {index}; allowed are 0-9 and a-f",
                byte.escape_ascii()
            ),
        }
    }
}
