//! Delegation issuer certificates (ADR-0047, point 1): what a node checks of the certificate a
//! delegated token carries in `x5c`, and what a control node checks of its own at load.
//!
//! [`IssuerCertificate::check`] runs every check that depends on neither a token nor the time
//! (points 1.4.2 to 1.4.4, and the 90-day cap of point 1.4.5), so a load, a reload, and a cache
//! of parsed certificates can run them once. [`IssuerCertificate::valid_at`] checks the validity
//! window at a given time, and [`IssuerCertificate::matches_kid`] the token's `kid` (point 1.4.6);
//! a verifier runs both for every token.
//!
//! [`IssuerCache`] keeps the certificates that passed [`IssuerCertificate::check`] for one loaded
//! identity generation, so a token's `x5c` is parsed and its signature checked once, not per
//! request.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use fleetring::NodeId;
use fleetring_control::{KeyId, Timestamp};
use x509_parser::extensions::{
    BasicConstraints, GeneralName, KeyUsage, ParsedExtension, SubjectAlternativeName,
};
use x509_parser::oid_registry::{
    OID_SIG_ED25519, OID_X509_EXT_BASIC_CONSTRAINTS, OID_X509_EXT_EXTENDED_KEY_USAGE,
    OID_X509_EXT_KEY_USAGE, OID_X509_EXT_SUBJECT_ALT_NAME,
};
use x509_parser::prelude::{FromDer as _, X509Certificate, X509Version};
use x509_parser::x509::AlgorithmIdentifier;

use crate::auth::{NOT_BEFORE_LEEWAY_SECS, VerifyingKey};
use crate::tls::authority::{ISSUER_LIFETIME_SLACK, MAX_ISSUER_LIFETIME};
use crate::tls::names::{FleetName, SCHEME, SubjectUri};

/// The length of an Ed25519 signature, in bytes.
const ED25519_SIGNATURE_LEN: usize = 64;

/// The key usage bit for digital signature, as x509-parser numbers the bits: the only usage an
/// issuer certificate may name (ADR-0047, point 1.4.4).
const KEY_USAGE_DIGITAL_SIGNATURE: u16 = 1;

/// The DER tag of a `SEQUENCE`.
const DER_SEQUENCE: u8 = 0x30;

/// The DER tag of a `GeneralName` that is a URI, `[6] IMPLICIT IA5String` (RFC 5280, §4.2.1.6).
const DER_URI_NAME: u8 = 0x86;

/// The DER of basic constraints that say "not an authority": an empty `SEQUENCE`, since `cA`
/// defaults to false and a path length belongs only to an authority (RFC 5280, §4.2.1.9).
const NOT_AN_AUTHORITY_DER: [u8; 2] = [0x30, 0x00];

/// The DER of a key usage of digital signature alone: a `BIT STRING` of one byte, `0x80`, whose
/// last seven bits are unused (RFC 5280, §4.2.1.3).
const KEY_USAGE_DIGITAL_SIGNATURE_DER: [u8; 4] = [0x03, 0x02, 0x07, 0x80];

/// The DER of the Ed25519 `AlgorithmIdentifier`: a `SEQUENCE` holding only the OID 1.3.101.112
/// (RFC 8410, §3).
const ED25519_ALGORITHM_DER: [u8; 7] = [0x30, 0x05, 0x06, 0x03, 0x2b, 0x65, 0x70];

/// The header of the `BIT STRING` holding an Ed25519 signature: 65 content bytes, the first
/// saying no bits are unused.
const ED25519_SIGNATURE_HEADER: [u8; 3] = [0x03, 0x41, 0x00];

const MILLIS_PER_SEC: u64 = 1_000;

/// How many issuer certificates an [`IssuerCache`] holds. A fleet has one issuer per control
/// node, and a rotation overlaps two certificates for each, so 16 covers a fleet's live
/// certificates with room; more would only let a sender of many valid certificates hold memory.
const CACHE_ENTRIES: usize = 16;

/// The error returned for an issuer certificate a node refuses. The messages never quote the
/// certificate.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum IssuerCertError {
    /// The bytes aren't one X.509 version 3 certificate in canonical DER, without unique
    /// identifiers, with well-formed extensions, each present at most once, and with the
    /// subject alternative names, basic constraints, and key usage each the canonical DER of
    /// what it says.
    #[error("the issuer certificate can't be parsed")]
    Parse,
    /// The certificate isn't signed with Ed25519, or its key isn't an Ed25519 key.
    #[error("the issuer certificate's signature or key isn't Ed25519")]
    NotEd25519,
    /// No trusted authority's key verifies the certificate's signature.
    #[error("the issuer certificate wasn't issued by a trusted authority")]
    UnknownAuthority,
    /// The certificate's signature isn't the shape an Ed25519 signature has.
    #[error("the issuer certificate's signature is malformed")]
    BadSignature,
    /// The certificate's one subject alternative name isn't `fleetring://<this fleet>/issuer/<name>`.
    #[error("the issuer certificate doesn't name an issuer of this fleet")]
    Subject,
    /// The certificate names something besides its issuer URI.
    #[error("the issuer certificate names something besides its issuer")]
    ExtraName,
    /// The certificate's basic constraints mark it as an authority.
    #[error("the issuer certificate is marked as an authority")]
    Authority,
    /// The certificate certifies a trusted authority's own key, which already signs root tokens
    /// and certificates and must not also sign as an issuer.
    #[error("the issuer certificate certifies a trusted authority's key")]
    AuthorityKey,
    /// The certificate has an extended key usage extension, which would let a TLS stack take it
    /// for a client or a server.
    #[error("the issuer certificate has an extended key usage")]
    ExtendedKeyUsage,
    /// The certificate's key usage names something other than digital signature alone.
    #[error("the issuer certificate's key usage isn't digital signature alone")]
    KeyUsage,
    /// The certificate marks an extension critical other than basic constraints, key usage, and
    /// subject alternative name.
    #[error("the issuer certificate marks an unknown extension critical")]
    CriticalExtension,
    /// The certificate's validity ends before it starts, or its dates are out of range.
    #[error("the issuer certificate's validity dates are invalid")]
    Dates,
    /// The certificate lasts longer than 90 days plus one day of slack.
    #[error("the issuer certificate lasts longer than 90 days plus one day")]
    TooLong,
    /// The certificate starts more than 30 seconds after the time it was checked at.
    #[error("the issuer certificate isn't valid yet")]
    NotYetValid,
    /// The certificate ended before the time it was checked at.
    #[error("the issuer certificate has expired")]
    Expired,
    /// The token's `kid` isn't the ID of the certificate's key.
    #[error("the token's key id isn't the issuer certificate's key")]
    KeyId,
}

/// A delegation issuer certificate that passed every check that depends on neither a token nor
/// the time (ADR-0047, points 1.4.2 to 1.4.5): issued by a trusted authority with Ed25519 for a
/// key that isn't a trusted authority's, naming
/// only `fleetring://<fleet>/issuer/<name>`, not an authority, with no extended key usage, key
/// usage at most digital signature, no unknown critical extension, and lasting at most 90 days
/// plus one day of slack.
///
/// Whether it is valid at a given time, and whether a token's `kid` names its key, are separate
/// checks: [`valid_at`](Self::valid_at) and [`matches_kid`](Self::matches_kid).
///
/// `Debug` shows the name, the key IDs, and the dates, never the DER.
#[derive(Clone, PartialEq, Eq)]
pub struct IssuerCertificate {
    der: Box<[u8]>,
    name: NodeId,
    key: VerifyingKey,
    authority: KeyId,
    not_before: Timestamp,
    not_after: Timestamp,
}

impl fmt::Debug for IssuerCertificate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IssuerCertificate")
            .field("name", &self.name)
            .field("key_id", self.key.id())
            .field("authority", &self.authority)
            .field("not_before", &self.not_before)
            .field("not_after", &self.not_after)
            .finish_non_exhaustive()
    }
}

/// The extensions an issuer certificate's checks read, gathered in one pass.
#[derive(Default)]
struct Extensions<'c, 'a> {
    names: Option<Read<'c, 'a, SubjectAlternativeName<'a>>>,
    basic_constraints: Option<Read<'c, 'a, BasicConstraints>>,
    key_usage: Option<Read<'c, 'a, KeyUsage>>,
    extended_key_usage: Presence,
    unknown_critical: Presence,
}

/// An extension the checks read: what x509-parser made of it, and the DER it was made from,
/// which must be the canonical encoding of what it says.
struct Read<'c, 'a, T> {
    parsed: &'c T,
    value: &'a [u8],
}

/// Whether the certificate has something.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
enum Presence {
    #[default]
    Absent,
    Present,
}

impl<'c, 'a> Extensions<'c, 'a> {
    /// Reads `certificate`'s extensions, refusing a duplicate or one of the three the checks read
    /// that doesn't parse.
    fn of(certificate: &'c X509Certificate<'a>) -> Result<Self, IssuerCertError> {
        let mut seen = BTreeSet::new();
        let mut found = Self::default();
        for extension in certificate.extensions() {
            if !seen.insert(extension.oid.as_bytes()) {
                return Err(IssuerCertError::Parse);
            }
            let parsed = extension.parsed_extension();
            if extension.oid == OID_X509_EXT_SUBJECT_ALT_NAME {
                let ParsedExtension::SubjectAlternativeName(names) = parsed else {
                    return Err(IssuerCertError::Parse);
                };
                found.names = Some(Read {
                    parsed: names,
                    value: extension.value,
                });
            } else if extension.oid == OID_X509_EXT_BASIC_CONSTRAINTS {
                let ParsedExtension::BasicConstraints(constraints) = parsed else {
                    return Err(IssuerCertError::Parse);
                };
                found.basic_constraints = Some(Read {
                    parsed: constraints,
                    value: extension.value,
                });
            } else if extension.oid == OID_X509_EXT_KEY_USAGE {
                let ParsedExtension::KeyUsage(usage) = parsed else {
                    return Err(IssuerCertError::Parse);
                };
                found.key_usage = Some(Read {
                    parsed: usage,
                    value: extension.value,
                });
            } else if extension.oid == OID_X509_EXT_EXTENDED_KEY_USAGE {
                // Present at all is refused, however it parses.
                found.extended_key_usage = Presence::Present;
            } else if extension.critical {
                found.unknown_critical = Presence::Present;
            }
        }
        Ok(found)
    }
}

impl IssuerCertificate {
    /// Checks `der` as an issuer certificate of `fleet`, issued by one of `authorities` (the
    /// Ed25519 keys of the node's `[tls] ca` bundle): everything of ADR-0047 point 1.4 that
    /// depends on neither a token nor the time, which is points 1.4.2 to 1.4.4 and the 90-day
    /// cap, with one day of slack, of point 1.4.5.
    ///
    /// The checks run cheapest first; the signature is checked last.
    ///
    /// # Errors
    ///
    /// The [`IssuerCertError`] of the first check that fails: [`Parse`](IssuerCertError::Parse),
    /// [`NotEd25519`](IssuerCertError::NotEd25519),
    /// [`AuthorityKey`](IssuerCertError::AuthorityKey), [`Subject`](IssuerCertError::Subject),
    /// [`ExtraName`](IssuerCertError::ExtraName), [`Authority`](IssuerCertError::Authority),
    /// [`ExtendedKeyUsage`](IssuerCertError::ExtendedKeyUsage),
    /// [`KeyUsage`](IssuerCertError::KeyUsage),
    /// [`CriticalExtension`](IssuerCertError::CriticalExtension),
    /// [`Dates`](IssuerCertError::Dates), [`TooLong`](IssuerCertError::TooLong),
    /// [`BadSignature`](IssuerCertError::BadSignature), [`Parse`](IssuerCertError::Parse) again
    /// for bytes that aren't the certificate's canonical DER, then
    /// [`UnknownAuthority`](IssuerCertError::UnknownAuthority).
    pub fn check(
        der: &[u8],
        fleet: &FleetName,
        authorities: &[VerifyingKey],
    ) -> Result<Self, IssuerCertError> {
        let (rest, certificate) =
            X509Certificate::from_der(der).map_err(|_| IssuerCertError::Parse)?;
        let tbs = &certificate.tbs_certificate;
        if !rest.is_empty()
            || certificate.version() != X509Version::V3
            || tbs.issuer_uid.is_some()
            || tbs.subject_uid.is_some()
        {
            return Err(IssuerCertError::Parse);
        }

        // Point 1.4.2's algorithm: Ed25519 (1.3.101.112) for the signature, named the same inside
        // and outside the signed part, and for the certificate's own key.
        let spki = tbs.public_key();
        if !is_ed25519(&certificate.signature_algorithm)
            || !is_ed25519(&tbs.signature)
            || !is_ed25519(&spki.algorithm)
            || spki.subject_public_key.unused_bits != 0
        {
            return Err(IssuerCertError::NotEd25519);
        }
        // `from_raw` takes exactly 32 bytes.
        let key = VerifyingKey::from_raw(spki.subject_public_key.as_ref())
            .ok_or(IssuerCertError::NotEd25519)?;
        if authorities
            .iter()
            .any(|authority| authority.id() == key.id())
        {
            return Err(IssuerCertError::AuthorityKey);
        }

        let extensions = Extensions::of(&certificate)?;
        let name = issuer_name(extensions.names.as_ref(), fleet)?;
        if let Some(constraints) = &extensions.basic_constraints {
            if constraints.parsed.ca {
                return Err(IssuerCertError::Authority);
            }
            if constraints.value != NOT_AN_AUTHORITY_DER {
                return Err(IssuerCertError::Parse);
            }
        }
        if extensions.extended_key_usage == Presence::Present {
            return Err(IssuerCertError::ExtendedKeyUsage);
        }
        if let Some(usage) = &extensions.key_usage {
            if usage.parsed.flags != KEY_USAGE_DIGITAL_SIGNATURE {
                return Err(IssuerCertError::KeyUsage);
            }
            // x509-parser keeps only the first 16 bits, so a later bit shows here alone.
            if usage.value != KEY_USAGE_DIGITAL_SIGNATURE_DER {
                return Err(IssuerCertError::Parse);
            }
        }
        if extensions.unknown_critical == Presence::Present {
            return Err(IssuerCertError::CriticalExtension);
        }

        let (not_before, not_after) = validity(&certificate)?;

        // Point 1.4.2: issued directly by a trusted authority. The signature is checked last,
        // since it is the costliest check.
        let signature = &certificate.signature_value;
        if signature.unused_bits != 0 || signature.as_ref().len() != ED25519_SIGNATURE_LEN {
            return Err(IssuerCertError::BadSignature);
        }
        // One certificate, one spelling: the `x5c` cache is keyed by these bytes (P11).
        if !is_canonical(der, tbs.as_ref(), signature.as_ref()) {
            return Err(IssuerCertError::Parse);
        }
        let authority = authorities
            .iter()
            .find(|authority| authority.verifies(tbs.as_ref(), signature.as_ref()))
            .ok_or(IssuerCertError::UnknownAuthority)?;

        Ok(Self {
            der: der.into(),
            name,
            key,
            authority: authority.id().clone(),
            not_before,
            not_after,
        })
    }

    /// Checks that the certificate is valid at `now`, with the 30 seconds of leeway a token's
    /// `nbf` has on its start (ADR-0047 point 1.4.5, ADR-0026 point 5). Its end gets no leeway:
    /// it is valid through the instant its `notAfter` names, as X.509 has it, and not after.
    ///
    /// # Errors
    ///
    /// [`IssuerCertError::NotYetValid`] if it starts more than 30 seconds after `now`;
    /// [`IssuerCertError::Expired`] if it ended before `now`.
    pub fn valid_at(&self, now: Timestamp) -> Result<(), IssuerCertError> {
        let leeway = NOT_BEFORE_LEEWAY_SECS.saturating_mul(MILLIS_PER_SEC);
        if now.as_millis().saturating_add(leeway) < self.not_before.as_millis() {
            return Err(IssuerCertError::NotYetValid);
        }
        if now > self.not_after {
            return Err(IssuerCertError::Expired);
        }
        Ok(())
    }

    /// Checks that `kid`, a token's key ID, is the ID of the certificate's key, derived as every
    /// key ID is (ADR-0047, point 1.4.6).
    ///
    /// # Errors
    ///
    /// [`IssuerCertError::KeyId`] if it isn't.
    pub fn matches_kid(&self, kid: &KeyId) -> Result<(), IssuerCertError> {
        if kid == self.key.id() {
            Ok(())
        } else {
            Err(IssuerCertError::KeyId)
        }
    }

    /// Returns the certificate as it was checked, DER-encoded.
    #[must_use]
    pub fn der(&self) -> &[u8] {
        &self.der
    }

    /// Returns the issuer's name, from `fleetring://<fleet>/issuer/<name>`.
    #[must_use]
    pub fn name(&self) -> &NodeId {
        &self.name
    }

    /// Returns the issuer's key, which signs delegated tokens.
    #[must_use]
    pub fn verifying_key(&self) -> &VerifyingKey {
        &self.key
    }

    /// Returns the ID of the issuer's key: the `kid` its tokens carry.
    #[must_use]
    pub fn key_id(&self) -> &KeyId {
        self.key.id()
    }

    /// Returns the ID of the trusted authority key that signed the certificate.
    #[must_use]
    pub fn authority_key_id(&self) -> &KeyId {
        &self.authority
    }

    /// Returns when the certificate starts being valid.
    #[must_use]
    pub fn not_before(&self) -> Timestamp {
        self.not_before
    }

    /// Returns the last moment the certificate is valid.
    #[must_use]
    pub fn not_after(&self) -> Timestamp {
        self.not_after
    }
}

/// The issuer certificates that passed [`IssuerCertificate::check`], held by one loaded identity
/// generation (the plan's P11), so a node parses a token's `x5c` and checks its signature once
/// rather than for every request.
///
/// An entry is keyed by the certificate's signed part, its TBS bytes, so one certificate holds
/// one entry. A lookup hits only for exactly the DER that was checked, for the fleet it was
/// checked against, and while the authority key that signed it is among the authorities passed
/// in; anything else is checked in full. Whether a token verifies therefore never depends on
/// what the cache holds. A hit skips the parse and the certificate's signature check; the
/// validity window and the token's `kid` are the caller's to check for every token.
///
/// It holds [`CACHE_ENTRIES`] certificates. A new one first evicts every expired entry, then
/// the oldest. A reload of the identity builds a new generation with an empty cache, so a
/// changed authority bundle never meets a certificate checked against the old one.
#[derive(Default)]
pub(crate) struct IssuerCache {
    entries: Mutex<CacheEntries>,
}

#[derive(Default)]
struct CacheEntries {
    by_signed_part: BTreeMap<Box<[u8]>, Cached>,
    /// The insertion counter: the oldest entry has the lowest `inserted`.
    next: u64,
}

struct Cached {
    certificate: Arc<IssuerCertificate>,
    fleet: FleetName,
    inserted: u64,
}

impl fmt::Debug for IssuerCache {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IssuerCache")
            .field("entries", &self.len())
            .finish()
    }
}

impl IssuerCache {
    /// An empty cache.
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// [`IssuerCertificate::check`]s `der` for `fleet` against `authorities`, or returns what a
    /// check of the same DER returned before. `now` decides which entries have expired when one
    /// must be evicted; it doesn't decide whether `der` is valid now.
    ///
    /// # Errors
    ///
    /// What [`IssuerCertificate::check`] returns.
    ///
    /// # Panics
    ///
    /// If the cache's lock was poisoned by a panic in another thread.
    pub(crate) fn check(
        &self,
        der: &[u8],
        fleet: &FleetName,
        authorities: &[VerifyingKey],
        now: Timestamp,
    ) -> Result<Arc<IssuerCertificate>, IssuerCertError> {
        let key = signed_part(der);
        if let Some(key) = key
            && let Some(hit) = self.hit(key, der, fleet, authorities)
        {
            return Ok(hit);
        }
        // Checked outside the lock: the signature check is the costly part.
        let certificate = Arc::new(IssuerCertificate::check(der, fleet, authorities)?);
        if let Some(key) = key {
            self.insert(key, &certificate, fleet, now);
        }
        Ok(certificate)
    }

    /// How many certificates the cache holds.
    pub(crate) fn len(&self) -> usize {
        self.lock().by_signed_part.len()
    }

    fn lock(&self) -> MutexGuard<'_, CacheEntries> {
        self.entries.lock().expect("issuer cache lock poisoned")
    }

    fn hit(
        &self,
        key: &[u8],
        der: &[u8],
        fleet: &FleetName,
        authorities: &[VerifyingKey],
    ) -> Option<Arc<IssuerCertificate>> {
        let entries = self.lock();
        let cached = entries.by_signed_part.get(key)?;
        let trusted = authorities
            .iter()
            .any(|authority| authority.id() == cached.certificate.authority_key_id());
        (cached.certificate.der() == der && cached.fleet == *fleet && trusted)
            .then(|| Arc::clone(&cached.certificate))
    }

    fn insert(
        &self,
        key: &[u8],
        certificate: &Arc<IssuerCertificate>,
        fleet: &FleetName,
        now: Timestamp,
    ) {
        let mut entries = self.lock();
        let full = |entries: &CacheEntries| {
            !entries.by_signed_part.contains_key(key)
                && entries.by_signed_part.len() >= CACHE_ENTRIES
        };
        if full(&entries) {
            entries
                .by_signed_part
                .retain(|_, cached| cached.certificate.not_after() >= now);
        }
        if full(&entries) {
            let oldest = entries
                .by_signed_part
                .iter()
                .min_by_key(|(_, cached)| cached.inserted)
                .map(|(key, _)| key.clone());
            if let Some(oldest) = oldest {
                entries.by_signed_part.remove(&oldest);
            }
        }
        let inserted = entries.next;
        entries.next = entries.next.saturating_add(1);
        entries.by_signed_part.insert(
            key.into(),
            Cached {
                certificate: Arc::clone(certificate),
                fleet: fleet.clone(),
                inserted,
            },
        );
    }
}

/// The DER element at the start of `bytes`, as its whole encoding and its content, or `None` if
/// `bytes` doesn't start with a definite-length element that fits in it. Only the length is
/// read; the tag is the caller's to check.
fn der_element(bytes: &[u8]) -> Option<(&[u8], &[u8])> {
    let (_tag, rest) = bytes.split_first()?;
    let (&first, rest) = rest.split_first()?;
    let (len, rest) = if first < 0x80 {
        (usize::from(first), rest)
    } else {
        let count = usize::from(first & 0x7f);
        if count == 0 || count > size_of::<usize>() {
            return None;
        }
        let (digits, rest) = rest.split_at_checked(count)?;
        let len = digits
            .iter()
            .fold(0_usize, |len, &digit| (len << 8) | usize::from(digit));
        (len, rest)
    };
    let content = rest.get(..len)?;
    let header = bytes.len() - rest.len();
    Some((bytes.get(..header + len)?, content))
}

/// The signed part of a certificate, its `tbsCertificate` with its header: the first element
/// inside the outer `SEQUENCE`, found without parsing anything else. `None` for bytes that don't
/// start that way. [`IssuerCache`] keys its entries by it.
fn signed_part(der: &[u8]) -> Option<&[u8]> {
    if der.first() != Some(&DER_SEQUENCE) {
        return None;
    }
    let (_, certificate) = der_element(der)?;
    if certificate.first() != Some(&DER_SEQUENCE) {
        return None;
    }
    let (tbs, _) = der_element(certificate)?;
    Some(tbs)
}

/// Appends the DER length `len` to `out`, in its minimal form (X.690 §10.1).
fn push_der_length(out: &mut Vec<u8>, len: usize) {
    if let Ok(short @ 0..0x80) = u8::try_from(len) {
        out.push(short);
        return;
    }
    let bytes = len.to_be_bytes();
    let skip = bytes.iter().take_while(|&&byte| byte == 0).count();
    let significant = &bytes[skip..];
    // At most `size_of::<usize>()` bytes, so the count fits in the low seven bits.
    let count = u8::try_from(significant.len()).unwrap_or(0x7f) & 0x7f;
    out.push(0x80 | count);
    out.extend_from_slice(significant);
}

/// Whether `der` is the canonical DER of a certificate whose signed part is `tbs` and whose
/// Ed25519 signature is `signature`: a minimally encoded `SEQUENCE` holding exactly `tbs`, the
/// Ed25519 `AlgorithmIdentifier`, and the signature as a `BIT STRING` with no unused bits.
///
/// x509-parser ignores bytes after the signature inside the outer `SEQUENCE` and accepts
/// long-form lengths, so without this one public certificate would have unlimited spellings.
fn is_canonical(der: &[u8], tbs: &[u8], signature: &[u8]) -> bool {
    let len =
        tbs.len() + ED25519_ALGORITHM_DER.len() + ED25519_SIGNATURE_HEADER.len() + signature.len();
    let mut canonical = Vec::with_capacity(len + 1 + size_of::<usize>() + 1);
    canonical.push(DER_SEQUENCE);
    push_der_length(&mut canonical, len);
    canonical.extend_from_slice(tbs);
    canonical.extend_from_slice(&ED25519_ALGORITHM_DER);
    canonical.extend_from_slice(&ED25519_SIGNATURE_HEADER);
    canonical.extend_from_slice(signature);
    der == canonical
}

/// Whether `algorithm` is Ed25519 (RFC 8410: OID 1.3.101.112, parameters absent).
fn is_ed25519(algorithm: &AlgorithmIdentifier<'_>) -> bool {
    algorithm.algorithm == OID_SIG_ED25519 && algorithm.parameters.is_none()
}

/// The issuer's name, if `names` is exactly one name, the URI `fleetring://<fleet>/issuer/<name>`
/// (ADR-0047, point 1.4.3). A certificate with no `fleetring://` URI, several, or one that names
/// something else is refused as [`IssuerCertError::Subject`]; one whose issuer URI sits beside
/// any other name as [`IssuerCertError::ExtraName`]; and one whose extension holds anything but
/// the canonical DER of that one URI, such as a second list of names after it, as
/// [`IssuerCertError::Parse`].
fn issuer_name(
    names: Option<&Read<'_, '_, SubjectAlternativeName<'_>>>,
    fleet: &FleetName,
) -> Result<NodeId, IssuerCertError> {
    let value = names.map_or(&[][..], |names| names.value);
    let names = names.map_or(&[][..], |names| names.parsed.general_names.as_slice());
    let mut uris = names.iter().filter_map(|name| {
        if let GeneralName::URI(uri) = name
            && uri.starts_with(SCHEME)
        {
            Some(*uri)
        } else {
            None
        }
    });
    let (Some(uri), None) = (uris.next(), uris.next()) else {
        return Err(IssuerCertError::Subject);
    };
    let name = match uri.parse::<SubjectUri>() {
        Ok(SubjectUri::Issuer { fleet: named, name }) if named == *fleet => name,
        Ok(
            SubjectUri::Issuer { .. }
            | SubjectUri::Node { .. }
            | SubjectUri::Principal { .. }
            | SubjectUri::Authority { .. },
        )
        | Err(_) => return Err(IssuerCertError::Subject),
    };
    if names.len() != 1 {
        return Err(IssuerCertError::ExtraName);
    }
    if value != uri_names_der(uri) {
        return Err(IssuerCertError::Parse);
    }
    Ok(name)
}

/// The DER of subject alternative names holding only the URI `uri`: a `SEQUENCE` of one
/// `[6] IA5String`.
fn uri_names_der(uri: &str) -> Vec<u8> {
    let mut name = vec![DER_URI_NAME];
    push_der_length(&mut name, uri.len());
    name.extend_from_slice(uri.as_bytes());
    let mut names = vec![DER_SEQUENCE];
    push_der_length(&mut names, name.len());
    names.extend_from_slice(&name);
    names
}

/// The certificate's validity, checked to end at or after its start and to last at most
/// 90 days plus one day of slack (ADR-0047, point 1.4.5).
fn validity(certificate: &X509Certificate<'_>) -> Result<(Timestamp, Timestamp), IssuerCertError> {
    let window = certificate.validity();
    let start = u64::try_from(window.not_before.timestamp()).map_err(|_| IssuerCertError::Dates)?;
    let end = u64::try_from(window.not_after.timestamp()).map_err(|_| IssuerCertError::Dates)?;
    let lifetime = end.checked_sub(start).ok_or(IssuerCertError::Dates)?;
    if Duration::from_secs(lifetime) > MAX_ISSUER_LIFETIME.saturating_add(ISSUER_LIFETIME_SLACK) {
        return Err(IssuerCertError::TooLong);
    }
    let millis = |secs: u64| {
        secs.checked_mul(MILLIS_PER_SEC)
            .map(Timestamp::from_millis)
            .ok_or(IssuerCertError::Dates)
    };
    Ok((millis(start)?, millis(end)?))
}

#[cfg(test)]
mod tests {
    use rcgen::{
        BasicConstraints as CaConstraints, CertificateParams, CustomExtension, DistinguishedName,
        DnType, ExtendedKeyUsagePurpose, IsCa, KeyPair, KeyUsagePurpose, PKCS_ECDSA_P256_SHA256,
        PKCS_ED25519, SanType,
    };
    use rustls_pki_types::pem::PemObject as _;
    use rustls_pki_types::{CertificateDer, PrivatePkcs8KeyDer};
    use time::OffsetDateTime;

    use super::*;
    use crate::auth::TokenSigner;
    use crate::tls::{Authority, AuthorityError, DEFAULT_ISSUER_LIFETIME, Principal, Validity};

    /// 2027-01-15, in milliseconds since the epoch.
    const NOW: Timestamp = Timestamp::from_millis(1_800_000_000_000);

    const DAY: Duration = Duration::from_secs(24 * 60 * 60);

    const SECOND: Duration = Duration::from_secs(1);

    /// An extension no check knows: an OID under a private enterprise arc, holding NULL.
    const UNKNOWN_OID: &[u64] = &[1, 3, 6, 1, 4, 1, 99_999, 1];

    fn fleet() -> FleetName {
        "lab".parse().expect("fleet name")
    }

    fn name() -> NodeId {
        "control-1".parse().expect("node id")
    }

    fn authority_of(fleet: &str) -> Authority {
        Authority::create(
            fleet.parse().expect("fleet name"),
            Validity::new(Timestamp::ZERO, 100 * 365 * DAY),
        )
        .expect("authority")
    }

    fn keys(authority: &Authority) -> Vec<VerifyingKey> {
        vec![
            authority
                .token_signer()
                .expect("an Ed25519 authority signs")
                .verifying_key(),
        ]
    }

    fn der(pem: &str) -> Vec<u8> {
        CertificateDer::from_pem_slice(pem.as_bytes())
            .expect("a PEM certificate")
            .to_vec()
    }

    fn date(at: Timestamp) -> OffsetDateTime {
        let secs = i64::try_from(at.as_millis() / 1_000).expect("in range");
        OffsetDateTime::from_unix_timestamp(secs).expect("in range")
    }

    fn after(at: Timestamp, span: Duration) -> Timestamp {
        Timestamp::from_millis(at.as_millis() + u64::try_from(span.as_millis()).expect("fits"))
    }

    fn before(at: Timestamp, span: Duration) -> Timestamp {
        Timestamp::from_millis(at.as_millis() - u64::try_from(span.as_millis()).expect("fits"))
    }

    /// What `issue_issuer` builds, as another authority tool might: the parameters each refusal
    /// case changes one thing of.
    fn issuer_params(fleet: &str, start: Timestamp, lifetime: Duration) -> CertificateParams {
        let uri = format!("fleetring://{fleet}/issuer/control-1");
        let mut params = CertificateParams::default();
        params.not_before = date(start);
        params.not_after = date(after(start, lifetime));
        let mut subject = DistinguishedName::new();
        subject.push(DnType::CommonName, uri.as_str());
        params.distinguished_name = subject;
        params.is_ca = IsCa::ExplicitNoCa;
        params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
        params.subject_alt_names = vec![SanType::URI(uri.try_into().expect("an IA5 string"))];
        params
    }

    fn default_params() -> CertificateParams {
        issuer_params("lab", NOW, DEFAULT_ISSUER_LIFETIME)
    }

    fn ed25519_key() -> KeyPair {
        KeyPair::generate_for(&PKCS_ED25519).expect("key")
    }

    fn unknown_extension() -> CustomExtension {
        CustomExtension::from_oid_content(UNKNOWN_OID, vec![0x05, 0x00])
    }

    /// `params` with a new Ed25519 key, signed by `authority`.
    fn signed(authority: &Authority, params: &CertificateParams) -> Vec<u8> {
        authority
            .sign_for_test(params, &ed25519_key())
            .expect("sign")
            .der()
            .to_vec()
    }

    fn check(authority: &Authority, der: &[u8]) -> Result<IssuerCertificate, IssuerCertError> {
        IssuerCertificate::check(der, &fleet(), &keys(authority))
    }

    /// The DER encoding of `content` under the one-byte `tag`, with a minimal length.
    fn tlv(tag: u8, content: &[u8]) -> Vec<u8> {
        let mut out = vec![tag];
        push_der_length(&mut out, content.len());
        out.extend_from_slice(content);
        out
    }

    /// A certificate's signed part and its signature.
    fn parts(der: &[u8]) -> (Vec<u8>, Vec<u8>) {
        let (_, certificate) = X509Certificate::from_der(der).expect("a certificate");
        (
            certificate.tbs_certificate.as_ref().to_vec(),
            certificate.signature_value.as_ref().to_vec(),
        )
    }

    /// The signature `BIT STRING` of a certificate, with no unused bits.
    fn signature_bits(signature: &[u8]) -> Vec<u8> {
        tlv(0x03, &[&[0][..], signature].concat())
    }

    /// The certificate's outer `SEQUENCE`, minimally encoded, around `pieces` in turn.
    fn wrap(pieces: &[&[u8]]) -> Vec<u8> {
        tlv(0x30, &pieces.concat())
    }

    /// The lengths of the header and the content of the DER element at the start of `bytes`,
    /// which must have a one-byte tag.
    fn header_and_content_len(bytes: &[u8]) -> (usize, usize) {
        let first = bytes[1];
        if first < 0x80 {
            (2, usize::from(first))
        } else {
            let count = usize::from(first & 0x7f);
            let len = bytes[2..2 + count]
                .iter()
                .fold(0_usize, |len, byte| (len << 8) | usize::from(*byte));
            (2 + count, len)
        }
    }

    /// The content of the DER element `element`.
    fn content_of(element: &[u8]) -> &[u8] {
        let (header, len) = header_and_content_len(element);
        &element[header..header + len]
    }

    /// The whole DER elements, tag and length included, that `content` holds in turn.
    fn elements_of(mut content: &[u8]) -> Vec<Vec<u8>> {
        let mut elements = Vec::new();
        while !content.is_empty() {
            let (header, len) = header_and_content_len(content);
            let (element, rest) = content.split_at(header + len);
            elements.push(element.to_vec());
            content = rest;
        }
        elements
    }

    /// An authority whose key the tests hold, so that they can sign a hand-edited TBS: the
    /// checks take only the authorities' public keys.
    struct RawAuthority {
        key: KeyPair,
    }

    impl RawAuthority {
        fn new() -> Self {
            Self { key: ed25519_key() }
        }

        fn keys(&self) -> Vec<VerifyingKey> {
            vec![VerifyingKey::from_raw(self.key.public_key_raw()).expect("an Ed25519 key")]
        }

        /// The TBS of a certificate this authority signs from `params`.
        fn tbs(&self, params: &CertificateParams) -> Vec<u8> {
            let mut ca = CertificateParams::default();
            ca.is_ca = IsCa::Ca(CaConstraints::Unconstrained);
            let issuer = rcgen::Issuer::new(ca, &self.key);
            let certificate = params.signed_by(&ed25519_key(), &issuer).expect("sign");
            parts(certificate.der()).0
        }

        /// `tbs`, signed by this authority, with `algorithm` as the outer signature algorithm.
        fn sign_with(&self, tbs: &[u8], algorithm: &[u8]) -> Vec<u8> {
            let signature = rcgen::SigningKey::sign(&self.key, tbs).expect("sign");
            wrap(&[tbs, algorithm, &signature_bits(&signature)])
        }

        /// `tbs`, signed by this authority with Ed25519.
        fn sign(&self, tbs: &[u8]) -> Vec<u8> {
            self.sign_with(tbs, &ED25519_ALGORITHM_DER)
        }

        fn check(&self, der: &[u8]) -> Result<IssuerCertificate, IssuerCertError> {
            IssuerCertificate::check(der, &fleet(), &self.keys())
        }
    }

    /// `tbs` with its elements changed by `edit`, re-encoded. TBS elements, in order: 0 the
    /// version, 1 the serial, 2 the signature algorithm, 3 the issuer, 4 the validity, 5 the
    /// subject, 6 the subject public key, 7 the extensions.
    fn edit_tbs(tbs: &[u8], edit: impl FnOnce(&mut Vec<Vec<u8>>)) -> Vec<u8> {
        let mut elements = elements_of(content_of(tbs));
        edit(&mut elements);
        tlv(0x30, &elements.concat())
    }

    /// ECDSA with SHA-256 (1.2.840.10045.4.3.2), as an `AlgorithmIdentifier`.
    const ECDSA_SHA256_DER: [u8; 12] = [
        0x30, 0x0a, 0x06, 0x08, 0x2a, 0x86, 0x48, 0xce, 0x3d, 0x04, 0x03, 0x02,
    ];

    /// ADR-0047, point 1.4.1, and the cache key of P11: one public certificate has one byte
    /// string. x509-parser ignores bytes inside the outer `SEQUENCE` after the signature and
    /// accepts long-form lengths, so `check` compares the bytes with the canonical re-encoding.
    #[test]
    fn issuer_certificate_must_be_canonical_der() {
        let authority = authority_of("lab");
        let good = signed(&authority, &default_params());
        let (tbs, signature) = parts(&good);
        let bits = signature_bits(&signature);
        assert_eq!(
            wrap(&[&tbs, &ED25519_ALGORITHM_DER, &bits]),
            good,
            "the canonical re-encoding is the certificate"
        );
        check(&authority, &good).expect("the canonical certificate checks");

        let body = [&tbs[..], &ED25519_ALGORITHM_DER, &bits].concat();
        let mut variants: Vec<(String, Vec<u8>)> = Vec::new();
        for junk in 1..=7 {
            variants.push((
                format!("{junk} zero bytes inside the outer SEQUENCE"),
                wrap(&[&body, &vec![0; junk]]),
            ));
            variants.push((
                format!("{junk} 0xff bytes inside the outer SEQUENCE"),
                wrap(&[&body, &vec![0xff; junk]]),
            ));
        }
        variants.push((
            "an OCTET STRING after the signature".to_owned(),
            wrap(&[&body, &tlv(0x04, b"junk")]),
        ));
        let len = u16::try_from(body.len()).expect("a short certificate");
        let [high, low] = len.to_be_bytes();
        variants.push((
            "a non-minimal outer length".to_owned(),
            [&[0x30, 0x83, 0x00, high, low][..], &body].concat(),
        ));
        variants.push((
            "a long-form length on the signature BIT STRING".to_owned(),
            wrap(&[
                &tbs,
                &ED25519_ALGORITHM_DER,
                &[0x03, 0x81, 0x41, 0x00],
                &signature,
            ]),
        ));
        variants.push((
            "a long-form length on the signature algorithm".to_owned(),
            wrap(&[
                &tbs,
                &[0x30, 0x81, 0x05, 0x06, 0x03, 0x2b, 0x65, 0x70],
                &bits,
            ]),
        ));
        let wrong: Vec<_> = variants
            .iter()
            .filter_map(|(case, der)| {
                assert_ne!(der, &good, "{case}");
                let verdict = check(&authority, der).map(|_| ());
                (verdict != Err(IssuerCertError::Parse)).then(|| format!("{case}: {verdict:?}"))
            })
            .collect();
        assert!(wrong.is_empty(), "not refused as unparseable: {wrong:#?}");
    }

    #[test]
    fn issuer_certificate_from_this_authority_checks() {
        let authority = authority_of("lab");
        let issued = authority
            .issue_issuer(&name(), Validity::new(NOW, DEFAULT_ISSUER_LIFETIME))
            .expect("issue");
        assert_eq!(
            issued.subject().to_string(),
            "fleetring://lab/issuer/control-1"
        );
        let certificate = check(&authority, &der(issued.certificate_pem())).expect("checks");
        assert_eq!(certificate.name(), &name());
        assert_eq!(certificate.not_before(), NOW);
        assert_eq!(
            certificate.not_after(),
            after(NOW, DEFAULT_ISSUER_LIFETIME),
            "30 days by default"
        );
        assert_eq!(certificate.der(), der(issued.certificate_pem()).as_slice());
        let authority_key = &keys(&authority)[0];
        assert_eq!(certificate.authority_key_id(), authority_key.id());

        // The certificate's key is the issued key, and `kid` must name it.
        let pkcs8 =
            PrivatePkcs8KeyDer::from_pem_slice(issued.key_pem().as_bytes()).expect("a PKCS#8 key");
        let signer =
            TokenSigner::from_pkcs8(pkcs8.secret_pkcs8_der(), "lab").expect("an Ed25519 key");
        assert_eq!(certificate.key_id(), signer.key_id());
        assert_eq!(certificate.verifying_key(), &signer.verifying_key());
        assert_eq!(certificate.matches_kid(signer.key_id()), Ok(()));
        assert_eq!(
            certificate.matches_kid(authority_key.id()),
            Err(IssuerCertError::KeyId),
            "a kid that isn't the certificate key's ID"
        );
        assert_eq!(certificate.valid_at(NOW), Ok(()));

        // During an authority rotation, either trusted authority may have issued it.
        let mut rotating = keys(&authority_of("lab"));
        rotating.extend(keys(&authority));
        let rotated = IssuerCertificate::check(&der(issued.certificate_pem()), &fleet(), &rotating)
            .expect("checks against the second authority");
        assert_eq!(rotated.authority_key_id(), authority_key.id());

        // Key usage and basic constraints may be left out; neither is required.
        let mut bare = default_params();
        bare.is_ca = IsCa::NoCa;
        bare.key_usages = Vec::new();
        check(&authority, &signed(&authority, &bare)).expect("no key usage or basic constraints");

        // A non-critical extension the checks don't know is ignored.
        let mut extra = default_params();
        extra.custom_extensions.push(unknown_extension());
        check(&authority, &signed(&authority, &extra)).expect("a non-critical extension");

        // `Debug` never shows the certificate's bytes.
        let debug = format!("{certificate:?}");
        assert!(debug.contains("control-1"), "{debug}");
        assert!(!debug.contains("der"), "{debug}");
    }

    #[test]
    fn issuer_certificate_refusals() {
        let authority = authority_of("lab");
        let refused = |der: &[u8], want: IssuerCertError, case: &str| {
            assert_eq!(check(&authority, der).map(|_| ()), Err(want), "{case}");
        };

        // Another fleet's authority, and another authority that calls its fleet the same.
        let other_fleet = authority_of("other");
        let issued = other_fleet
            .issue_issuer(&name(), Validity::new(NOW, DAY))
            .expect("issue");
        refused(
            &der(issued.certificate_pem()),
            IssuerCertError::Subject,
            "another fleet's authority",
        );
        let impostor = authority_of("lab");
        let issued = impostor
            .issue_issuer(&name(), Validity::new(NOW, DAY))
            .expect("issue");
        refused(
            &der(issued.certificate_pem()),
            IssuerCertError::UnknownAuthority,
            "another authority for a fleet of the same name",
        );
        assert_eq!(
            IssuerCertificate::check(&der(issued.certificate_pem()), &fleet(), &[]).map(|_| ()),
            Err(IssuerCertError::UnknownAuthority),
            "no trusted authority"
        );

        // Self-signed.
        let key = ed25519_key();
        let self_signed = default_params().self_signed(&key).expect("self-sign");
        refused(
            self_signed.der(),
            IssuerCertError::UnknownAuthority,
            "self-signed",
        );

        // A node or client certificate offered as an issuer.
        let node = authority
            .issue_node(&name(), &["127.0.0.1".to_owned()], Validity::new(NOW, DAY))
            .expect("issue node");
        refused(
            &der(node.certificate_pem()),
            IssuerCertError::Subject,
            "a node certificate",
        );
        let principal: Principal = "control-1".parse().expect("principal");
        let client = authority
            .issue_client(&principal, Validity::new(NOW, DAY))
            .expect("issue client");
        refused(
            &der(client.certificate_pem()),
            IssuerCertError::Subject,
            "a client certificate",
        );

        // Names beside the issuer URI.
        let mut params = default_params();
        params.subject_alt_names.push(SanType::DnsName(
            "control-1".try_into().expect("a DNS name"),
        ));
        refused(
            &signed(&authority, &params),
            IssuerCertError::ExtraName,
            "an extra DNS name",
        );
        let mut params = default_params();
        params.subject_alt_names.push(SanType::URI(
            "https://example.com/".try_into().expect("an IA5 string"),
        ));
        refused(
            &signed(&authority, &params),
            IssuerCertError::ExtraName,
            "an extra URI",
        );
        let mut params = default_params();
        params.subject_alt_names.push(SanType::URI(
            "fleetring://lab/node/control-1"
                .try_into()
                .expect("an IA5 string"),
        ));
        refused(
            &signed(&authority, &params),
            IssuerCertError::Subject,
            "a second fleetring URI",
        );
        let mut params = default_params();
        params.subject_alt_names = Vec::new();
        refused(
            &signed(&authority, &params),
            IssuerCertError::Subject,
            "no subject alternative name",
        );

        // Usable as an authority, or for TLS.
        let mut params = default_params();
        params.is_ca = IsCa::Ca(CaConstraints::Unconstrained);
        refused(
            &signed(&authority, &params),
            IssuerCertError::Authority,
            "marked as an authority",
        );
        for purpose in [
            ExtendedKeyUsagePurpose::ClientAuth,
            ExtendedKeyUsagePurpose::ServerAuth,
            ExtendedKeyUsagePurpose::Any,
            ExtendedKeyUsagePurpose::CodeSigning,
        ] {
            let mut params = default_params();
            params.extended_key_usages = vec![purpose.clone()];
            refused(
                &signed(&authority, &params),
                IssuerCertError::ExtendedKeyUsage,
                &format!("with the extended key usage {purpose:?}"),
            );
        }
        for usages in [
            vec![
                KeyUsagePurpose::DigitalSignature,
                KeyUsagePurpose::KeyCertSign,
            ],
            vec![KeyUsagePurpose::KeyCertSign],
            vec![
                KeyUsagePurpose::DigitalSignature,
                KeyUsagePurpose::KeyEncipherment,
            ],
            vec![KeyUsagePurpose::CrlSign],
        ] {
            let mut params = default_params();
            params.key_usages = usages.clone();
            refused(
                &signed(&authority, &params),
                IssuerCertError::KeyUsage,
                &format!("with the key usages {usages:?}"),
            );
        }

        // An unknown critical extension.
        let mut params = default_params();
        let mut extension = unknown_extension();
        extension.set_criticality(true);
        params.custom_extensions.push(extension);
        refused(
            &signed(&authority, &params),
            IssuerCertError::CriticalExtension,
            "an unknown critical extension",
        );

        // Not Ed25519: signed by an ECDSA authority, or certifying an ECDSA key.
        let ecdsa_key = KeyPair::generate_for(&PKCS_ECDSA_P256_SHA256).expect("an ECDSA key");
        let mut ecdsa_authority = CertificateParams::default();
        ecdsa_authority.is_ca = IsCa::Ca(CaConstraints::Unconstrained);
        let ecdsa_issuer = rcgen::Issuer::new(ecdsa_authority, &ecdsa_key);
        let by_ecdsa = default_params()
            .signed_by(&ed25519_key(), &ecdsa_issuer)
            .expect("sign");
        refused(
            by_ecdsa.der(),
            IssuerCertError::NotEd25519,
            "an ECDSA signature",
        );
        let ecdsa_subject = KeyPair::generate_for(&PKCS_ECDSA_P256_SHA256).expect("an ECDSA key");
        let of_ecdsa = authority
            .sign_for_test(&default_params(), &ecdsa_subject)
            .expect("sign");
        refused(of_ecdsa.der(), IssuerCertError::NotEd25519, "an ECDSA key");

        // Damaged: not DER, trailing bytes, truncated, or a signature that doesn't verify.
        let good = signed(&authority, &default_params());
        refused(b"not a certificate", IssuerCertError::Parse, "not DER");
        let mut trailing = good.clone();
        trailing.push(0);
        refused(&trailing, IssuerCertError::Parse, "trailing bytes");
        refused(&good[..good.len() - 1], IssuerCertError::Parse, "truncated");
        let mut tampered = good.clone();
        let last = tampered.len() - 1;
        tampered[last] ^= 1;
        refused(
            &tampered,
            IssuerCertError::UnknownAuthority,
            "a signature that doesn't verify",
        );
        check(&authority, &good).expect("the undamaged certificate checks");
    }

    #[test]
    fn issuer_lifetime_is_ninety_days_plus_one_day_of_slack_on_nodes() {
        let authority = authority_of("lab");

        // Issuing: 90 days at most from the start, a start in the past included.
        for start in [NOW, before(NOW, 10 * DAY)] {
            let issued = authority
                .issue_issuer(&name(), Validity::new(start, MAX_ISSUER_LIFETIME))
                .expect("90 days");
            let certificate = check(&authority, &der(issued.certificate_pem())).expect("checks");
            assert_eq!(certificate.not_before(), start);
            let refused = authority
                .issue_issuer(&name(), Validity::new(start, MAX_ISSUER_LIFETIME + SECOND))
                .expect_err("90 days and 1 s");
            assert!(
                matches!(
                    refused,
                    AuthorityError::IssuerLifetime { seconds } if seconds == 90 * 86_400 + 1
                ),
                "{refused}"
            );
            assert!(refused.to_string().contains("at most 90 days"), "{refused}");
        }

        // Checking: one day of slack for a certificate another tool made.
        let longest = MAX_ISSUER_LIFETIME + ISSUER_LIFETIME_SLACK;
        assert_eq!(longest, 91 * DAY);
        let certificate = check(
            &authority,
            &signed(&authority, &issuer_params("lab", NOW, longest)),
        )
        .expect("exactly 91 days");
        assert_eq!(certificate.not_after(), after(NOW, longest));
        assert_eq!(
            check(
                &authority,
                &signed(&authority, &issuer_params("lab", NOW, longest + SECOND)),
            )
            .map(|_| ()),
            Err(IssuerCertError::TooLong),
            "91 days and 1 s"
        );

        // A validity that ends before it starts.
        let mut params = default_params();
        params.not_after = date(before(NOW, SECOND));
        assert_eq!(
            check(&authority, &signed(&authority, &params)).map(|_| ()),
            Err(IssuerCertError::Dates)
        );
    }

    #[test]
    fn issuer_certificate_validity_has_thirty_seconds_of_leeway_on_its_start_only() {
        let authority = authority_of("lab");
        let issued = authority
            .issue_issuer(&name(), Validity::new(NOW, DAY))
            .expect("issue");
        let certificate = check(&authority, &der(issued.certificate_pem())).expect("checks");
        let leeway = Duration::from_secs(NOT_BEFORE_LEEWAY_SECS);
        let millisecond = Duration::from_millis(1);

        assert_eq!(certificate.valid_at(before(NOW, leeway)), Ok(()));
        assert_eq!(
            certificate.valid_at(before(NOW, leeway + millisecond)),
            Err(IssuerCertError::NotYetValid),
            "not yet valid beyond 30 s"
        );
        let end = after(NOW, DAY);
        assert_eq!(certificate.valid_at(end), Ok(()));
        assert_eq!(
            certificate.valid_at(after(end, millisecond)),
            Err(IssuerCertError::Expired),
            "expired"
        );

        // Long expired, as `valid_at` sees it; `check` doesn't depend on the time.
        let old = authority
            .issue_issuer(&name(), Validity::new(Timestamp::ZERO, DAY))
            .expect("issue");
        let old = check(&authority, &der(old.certificate_pem())).expect("checks");
        assert_eq!(old.valid_at(NOW), Err(IssuerCertError::Expired));
    }

    /// An authority's key never signs as an issuer: certified by itself, or, during a rotation,
    /// by the other trusted authority.
    #[test]
    fn an_issuer_certificate_for_an_authority_key_is_refused() {
        let ca = || {
            let mut params = CertificateParams::default();
            params.is_ca = IsCa::Ca(CaConstraints::Unconstrained);
            params
        };
        let authority = RawAuthority::new();
        let own = default_params()
            .signed_by(&authority.key, &rcgen::Issuer::new(ca(), &authority.key))
            .expect("sign");
        assert_eq!(
            authority.check(own.der()).map(|_| ()),
            Err(IssuerCertError::AuthorityKey),
            "its own key"
        );

        let other = RawAuthority::new();
        let crossed = default_params()
            .signed_by(&authority.key, &rcgen::Issuer::new(ca(), &other.key))
            .expect("sign");
        let both: Vec<_> = other.keys().into_iter().chain(authority.keys()).collect();
        assert_eq!(
            IssuerCertificate::check(crossed.der(), &fleet(), &both).map(|_| ()),
            Err(IssuerCertError::AuthorityKey),
            "another trusted authority's key"
        );
        // Trusted by a node that trusts only the signer, the key is an ordinary issuer key.
        other
            .check(crossed.der())
            .expect("not an authority key to this node");
    }

    /// The OIDs of the extensions the checks read (RFC 5280, §4.2.1).
    const KEY_USAGE_OID: &[u64] = &[2, 5, 29, 15];
    const SUBJECT_ALT_NAME_OID: &[u64] = &[2, 5, 29, 17];
    const BASIC_CONSTRAINTS_OID: &[u64] = &[2, 5, 29, 19];
    const EXTENDED_KEY_USAGE_OID: &[u64] = &[2, 5, 29, 37];

    /// The subject alternative names holding only the URI `uri`, as DER.
    fn uri_names(uri: &str) -> Vec<u8> {
        tlv(0x30, &tlv(0x86, uri.as_bytes()))
    }

    /// The conjuncts of `check` no certificate rcgen makes can reach: each case edits the DER by
    /// hand and has it signed by an authority whose key the test holds.
    #[test]
    fn hand_edited_issuer_certificates_are_refused() {
        let authority = RawAuthority::new();
        let tbs = authority.tbs(&default_params());
        authority
            .check(&authority.sign(&tbs))
            .expect("the unedited certificate checks");
        let refused = |der: &[u8], want: IssuerCertError, case: &str| {
            assert_eq!(authority.check(der).map(|_| ()), Err(want), "{case}");
        };

        // The signature algorithm differs inside and outside the signed part, or has parameters.
        refused(
            &authority.sign_with(&tbs, &ECDSA_SHA256_DER),
            IssuerCertError::NotEd25519,
            "ECDSA outside the signed part, Ed25519 inside",
        );
        let inner_ecdsa = edit_tbs(&tbs, |tbs| tbs[2] = ECDSA_SHA256_DER.to_vec());
        refused(
            &authority.sign(&inner_ecdsa),
            IssuerCertError::NotEd25519,
            "Ed25519 outside the signed part, ECDSA inside",
        );
        let with_null = [0x30, 0x07, 0x06, 0x03, 0x2b, 0x65, 0x70, 0x05, 0x00];
        refused(
            &authority.sign_with(&tbs, &with_null),
            IssuerCertError::NotEd25519,
            "Ed25519 with NULL parameters outside the signed part",
        );
        let inner_null = edit_tbs(&tbs, |tbs| tbs[2] = with_null.to_vec());
        refused(
            &authority.sign(&inner_null),
            IssuerCertError::NotEd25519,
            "Ed25519 with NULL parameters inside the signed part",
        );

        // The subject key's BIT STRING says a bit is unused. Element 6 is the
        // SubjectPublicKeyInfo: 30 2a, the algorithm (7 bytes), then 03 21 and the unused bits.
        // The unused bit is cleared too, as DER requires, so that x509-parser reads it.
        let unused_bits = edit_tbs(&tbs, |tbs| {
            let key = &mut tbs[6];
            assert_eq!(key[9..12], [0x03, 0x21, 0x00], "an Ed25519 subject key");
            key[11] = 1;
            let last = key.len() - 1;
            key[last] &= 0xfe;
        });
        refused(
            &authority.sign(&unused_bits),
            IssuerCertError::NotEd25519,
            "a subject key with an unused bit",
        );

        // A version below 3: version 2, or the version left out, which is version 1.
        let version_2 = edit_tbs(&tbs, |tbs| {
            assert_eq!(tbs[0], [0xa0, 0x03, 0x02, 0x01, 0x02], "version 3");
            tbs[0] = vec![0xa0, 0x03, 0x02, 0x01, 0x01];
        });
        let version_1 = edit_tbs(&tbs, |tbs| {
            tbs.remove(0);
        });
        for (tbs, version) in [(version_2, X509Version::V2), (version_1, X509Version::V1)] {
            let der = authority.sign(&tbs);
            let (_, parsed) = X509Certificate::from_der(&der).expect("x509-parser reads it");
            assert_eq!(
                parsed.version(),
                version,
                "the version check is what refuses it"
            );
            refused(&der, IssuerCertError::Parse, &format!("{version:?}"));
        }

        // The signature's BIT STRING says a bit is unused, or the signature isn't 64 bytes.
        let signature = rcgen::SigningKey::sign(&authority.key, &tbs).expect("sign");
        let mut cleared = signature.clone();
        cleared[ED25519_SIGNATURE_LEN - 1] &= 0xfe;
        let with_unused = tlv(0x03, &[&[1][..], &cleared].concat());
        refused(
            &wrap(&[&tbs, &ED25519_ALGORITHM_DER, &with_unused]),
            IssuerCertError::BadSignature,
            "a signature with an unused bit",
        );
        for (len, bytes) in [
            (63, signature[..63].to_vec()),
            (65, [&signature[..], &[0]].concat()),
        ] {
            refused(
                &wrap(&[&tbs, &ED25519_ALGORITHM_DER, &signature_bits(&bytes)]),
                IssuerCertError::BadSignature,
                &format!("a signature of {len} bytes"),
            );
        }
    }

    /// Each extension appears once, the three the checks read must parse, and an extended key
    /// usage is refused even when it is marked critical or doesn't parse.
    #[test]
    fn issuer_certificate_extensions_parse_once_each_and_any_extended_key_usage_is_refused() {
        let authority = authority_of("lab");
        let refused = |params: &CertificateParams, want: IssuerCertError, case: &str| {
            assert_eq!(
                check(&authority, &signed(&authority, params)).map(|_| ()),
                Err(want),
                "{case}"
            );
        };
        let null = || vec![0x05, 0x00];

        // Duplicates, of an extension the checks read and of one they don't.
        let mut params = default_params();
        params
            .custom_extensions
            .push(CustomExtension::from_oid_content(
                KEY_USAGE_OID,
                vec![0x03, 0x02, 0x07, 0x80],
            ));
        refused(&params, IssuerCertError::Parse, "a second key usage");
        let mut params = default_params();
        params
            .custom_extensions
            .push(CustomExtension::from_oid_content(
                SUBJECT_ALT_NAME_OID,
                uri_names("fleetring://lab/issuer/control-1"),
            ));
        refused(
            &params,
            IssuerCertError::Parse,
            "a second subject alternative name",
        );
        let mut params = default_params();
        params.custom_extensions = vec![unknown_extension(), unknown_extension()];
        refused(
            &params,
            IssuerCertError::Parse,
            "an unknown extension twice",
        );

        // Each extension the checks read, holding something else.
        let mut params = default_params();
        params.subject_alt_names = Vec::new();
        params
            .custom_extensions
            .push(CustomExtension::from_oid_content(
                SUBJECT_ALT_NAME_OID,
                null(),
            ));
        refused(
            &params,
            IssuerCertError::Parse,
            "subject alternative names that don't parse",
        );
        let mut params = default_params();
        params.is_ca = IsCa::NoCa;
        params
            .custom_extensions
            .push(CustomExtension::from_oid_content(
                BASIC_CONSTRAINTS_OID,
                null(),
            ));
        refused(
            &params,
            IssuerCertError::Parse,
            "basic constraints that don't parse",
        );
        let mut params = default_params();
        params.key_usages = Vec::new();
        params
            .custom_extensions
            .push(CustomExtension::from_oid_content(KEY_USAGE_OID, null()));
        refused(
            &params,
            IssuerCertError::Parse,
            "a key usage that doesn't parse",
        );

        // An extended key usage marked critical, which the critical-extension rule doesn't
        // reach first, parsed or not.
        let client_auth = vec![
            0x30, 0x0a, 0x06, 0x08, 0x2b, 0x06, 0x01, 0x05, 0x05, 0x07, 0x03, 0x02,
        ];
        for (content, case) in [
            (client_auth, "a critical extended key usage"),
            (null(), "a critical extended key usage that doesn't parse"),
        ] {
            let mut params = default_params();
            let mut extension = CustomExtension::from_oid_content(EXTENDED_KEY_USAGE_OID, content);
            extension.set_criticality(true);
            params.custom_extensions.push(extension);
            refused(&params, IssuerCertError::ExtendedKeyUsage, case);
        }
    }

    /// The three extensions the checks read are each the canonical DER of what they say, with
    /// nothing after it, and the certificate carries no unique identifiers.
    #[test]
    fn issuer_certificate_extensions_and_identifiers_are_strict() {
        let authority = authority_of("lab");
        let uri = "fleetring://lab/issuer/control-1";
        let with = |oid: &[u64], value: Vec<u8>| {
            let mut params = default_params();
            if oid == SUBJECT_ALT_NAME_OID {
                params.subject_alt_names = Vec::new();
            } else if oid == BASIC_CONSTRAINTS_OID {
                params.is_ca = IsCa::NoCa;
            } else {
                params.key_usages = Vec::new();
            }
            params
                .custom_extensions
                .push(CustomExtension::from_oid_content(oid, value));
            signed(&authority, &params)
        };

        // The canonical values, as `issue_issuer` writes them, check.
        for (oid, value) in [
            (SUBJECT_ALT_NAME_OID, uri_names(uri)),
            (BASIC_CONSTRAINTS_OID, vec![0x30, 0x00]),
            (KEY_USAGE_OID, vec![0x03, 0x02, 0x07, 0x80]),
        ] {
            check(&authority, &with(oid, value.clone()))
                .unwrap_or_else(|error| panic!("{oid:?} holding {value:02x?}: {error}"));
        }

        let mut long_form = vec![0x30, 0x81];
        long_form.extend_from_slice(&uri_names(uri)[1..]);
        let refused = [
            (
                "a second GeneralNames",
                SUBJECT_ALT_NAME_OID,
                [uri_names(uri), uri_names(uri)].concat(),
            ),
            (
                "bytes after the names",
                SUBJECT_ALT_NAME_OID,
                [uri_names(uri), vec![0x05, 0x00]].concat(),
            ),
            (
                "a long-form length on the names",
                SUBJECT_ALT_NAME_OID,
                long_form,
            ),
            (
                "an explicit cA FALSE",
                BASIC_CONSTRAINTS_OID,
                vec![0x30, 0x03, 0x01, 0x01, 0x00],
            ),
            (
                "a path length without cA",
                BASIC_CONSTRAINTS_OID,
                vec![0x30, 0x03, 0x02, 0x01, 0x00],
            ),
            (
                "bytes after the basic constraints",
                BASIC_CONSTRAINTS_OID,
                vec![0x30, 0x00, 0x05, 0x00],
            ),
            (
                "a key usage that doesn't say its unused bits",
                KEY_USAGE_OID,
                vec![0x03, 0x02, 0x00, 0x80],
            ),
            (
                "a key usage bit beyond the ninth",
                KEY_USAGE_OID,
                vec![0x03, 0x04, 0x00, 0x80, 0x00, 0x01],
            ),
            (
                "bytes after the key usage",
                KEY_USAGE_OID,
                vec![0x03, 0x02, 0x07, 0x80, 0x05, 0x00],
            ),
        ];
        for (case, oid, value) in refused {
            let der = with(oid, value);
            assert_eq!(
                check(&authority, &der).map(|_| ()),
                Err(IssuerCertError::Parse),
                "{case}"
            );
        }

        // Unique identifiers, which RFC 5280 says a conforming authority doesn't write.
        let raw = RawAuthority::new();
        let tbs = raw.tbs(&default_params());
        for (tag, case) in [(0x81, "an issuer unique ID"), (0x82, "a subject unique ID")] {
            let edited = edit_tbs(&tbs, |tbs| tbs.insert(7, vec![tag, 0x02, 0x00, 0xaa]));
            let der = raw.sign(&edited);
            let (_, parsed) = X509Certificate::from_der(&der).expect("x509-parser reads it");
            assert!(
                parsed.tbs_certificate.issuer_uid.is_some()
                    || parsed.tbs_certificate.subject_uid.is_some(),
                "{case}"
            );
            assert_eq!(
                raw.check(&der).map(|_| ()),
                Err(IssuerCertError::Parse),
                "{case}"
            );
        }
    }

    /// An issuer certificate of `authority`, valid from `start` for `lifetime`.
    fn issued(authority: &Authority, start: Timestamp, lifetime: Duration) -> Vec<u8> {
        der(authority
            .issue_issuer(&name(), Validity::new(start, lifetime))
            .expect("an issuer certificate")
            .certificate_pem())
    }

    /// Whether `cache` holds `der` as checked for `authority` and the fleet `lab`.
    fn holds(cache: &IssuerCache, authority: &Authority, der: &[u8]) -> bool {
        let key = signed_part(der).expect("a certificate's signed part");
        cache.hit(key, der, &fleet(), &keys(authority)).is_some()
    }

    #[test]
    fn signed_part_is_the_tbs_and_never_panics() {
        let authority = authority_of("lab");
        let der = issued(&authority, NOW, DAY);
        let (_, parsed) = X509Certificate::from_der(&der).expect("a certificate");
        assert_eq!(signed_part(&der), Some(parsed.tbs_certificate.as_ref()));
        for len in 0..der.len() {
            // Every prefix is read without a panic; none holds the whole signed part.
            assert_ne!(signed_part(&der[..len]), signed_part(&der), "{len}");
        }
        for bytes in [
            &[][..],
            &[0x30],
            &[0x30, 0x80, 0x30, 0x00],
            &[
                0x30, 0x89, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
            ],
            &[0x30, 0x88, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff],
            &[0x30, 0x02, 0x31, 0x00],
            &[0x30, 0x02, 0x30, 0x00],
        ] {
            let got = signed_part(bytes);
            assert!(got.is_none() || got == Some(&[0x30, 0x00][..]), "{bytes:?}");
        }
    }

    #[test]
    fn the_cache_hits_only_the_der_it_checked_for_the_fleet_and_authorities_it_checked() {
        let authority = authority_of("lab");
        let other = authority_of("lab");
        let der = issued(&authority, NOW, DAY);
        let cache = IssuerCache::new();
        let first = cache
            .check(&der, &fleet(), &keys(&authority), NOW)
            .expect("checks");
        let again = cache
            .check(&der, &fleet(), &keys(&authority), NOW)
            .expect("checks");
        assert!(Arc::ptr_eq(&first, &again), "the second check is a hit");
        assert_eq!(cache.len(), 1);

        // The authorities passed in no longer hold the one that signed it, or another fleet.
        assert_eq!(
            cache.check(&der, &fleet(), &keys(&other), NOW).map(|_| ()),
            Err(IssuerCertError::UnknownAuthority)
        );
        assert_eq!(
            cache
                .check(
                    &der,
                    &"prod".parse().expect("a fleet"),
                    &keys(&authority),
                    NOW
                )
                .map(|_| ()),
            Err(IssuerCertError::Subject)
        );
        // The same signed part with another signature is checked in full, and refused.
        let mut forged = der.clone();
        let last = forged.len() - 1;
        forged[last] ^= 1;
        assert_eq!(signed_part(&forged), signed_part(&der));
        assert_eq!(
            cache
                .check(&forged, &fleet(), &keys(&authority), NOW)
                .map(|_| ()),
            Err(IssuerCertError::UnknownAuthority)
        );
        // Junk inside the outer `SEQUENCE` after the signature, which x509-parser ignores.
        let mut padded = der.clone();
        padded.push(0x00);
        let content = u16::try_from(padded.len() - 4).expect("short");
        padded[2..4].copy_from_slice(&content.to_be_bytes());
        assert_eq!(der[1], 0x82, "a two-byte length");
        assert_eq!(signed_part(&padded), signed_part(&der));
        assert_eq!(
            cache
                .check(&padded, &fleet(), &keys(&authority), NOW)
                .map(|_| ()),
            Err(IssuerCertError::Parse)
        );
        assert_eq!(cache.len(), 1, "a refused certificate is never cached");
        assert!(holds(&cache, &authority, &der));
    }

    #[test]
    fn the_cache_holds_16_and_evicts_expired_entries_first_then_the_oldest() {
        let authority = authority_of("lab");
        let cache = IssuerCache::new();
        let check = |der: &[u8], now| {
            cache
                .check(der, &fleet(), &keys(&authority), now)
                .expect("checks");
        };
        // The second certificate checked ends before the others.
        let mut certificates: Vec<Vec<u8>> =
            (0..16).map(|_| issued(&authority, NOW, 30 * DAY)).collect();
        certificates[1] = issued(&authority, NOW, DAY);
        for der in &certificates {
            check(der, NOW);
        }
        assert_eq!(cache.len(), 16);

        // Two days later the short one has expired: it goes, though the first is older.
        let later = after(NOW, 2 * DAY);
        let seventeenth = issued(&authority, NOW, 30 * DAY);
        check(&seventeenth, later);
        assert_eq!(cache.len(), 16);
        assert!(!holds(&cache, &authority, &certificates[1]));
        assert!(holds(&cache, &authority, &certificates[0]));

        // With none expired, the oldest goes.
        let eighteenth = issued(&authority, NOW, 30 * DAY);
        check(&eighteenth, later);
        assert_eq!(cache.len(), 16);
        assert!(!holds(&cache, &authority, &certificates[0]));
        for der in certificates[2..].iter().chain([&seventeenth, &eighteenth]) {
            assert!(holds(&cache, &authority, der));
        }

        // A hit adds nothing and evicts nothing.
        check(&eighteenth, later);
        assert_eq!(cache.len(), 16);
        assert!(holds(&cache, &authority, &certificates[2]));
    }
}
