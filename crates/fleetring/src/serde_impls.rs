//! serde support for the types a ring spec holds, behind the `serde` feature (ADR-0008).
//!
//! Each type deserializes through its validating constructor, so serde can never build a value
//! the constructor would reject: labels must match the label grammar, and weights and
//! replication factors must be nonzero. Errors carry the constructor's reason.

use std::fmt;
use std::marker::PhantomData;
use std::str::FromStr;

use serde::de::{self, Deserialize, Deserializer, Unexpected, Visitor};
use serde::ser::{Serialize, Serializer};

use crate::{
    Epoch, Fingerprint, Host, LabelKind, NodeId, ParseLabelError, ReplicationFactor, Weight, Zone,
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

label_serde!(NodeId, Zone, Host);

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
