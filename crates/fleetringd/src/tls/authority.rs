//! The fleet certificate authority: it creates itself and issues node, client, and delegation
//! issuer certificates.

use std::fmt;
use std::fs::{self, OpenOptions};
use std::io::{self, Write as _};
use std::net::IpAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use fleetring::NodeId;
use fleetring_control::Timestamp;
use rcgen::{
    BasicConstraints, CertificateParams, DistinguishedName, DnType, ExtendedKeyUsagePurpose, IsCa,
    Issuer, KeyPair, KeyUsagePurpose, PKCS_ED25519, SanType, SerialNumber,
};
use rustls_pki_types::CertificateDer;
use rustls_pki_types::pem::PemObject;
use time::OffsetDateTime;
use x509_parser::prelude::{FromDer as _, X509Certificate};

use crate::auth::{IssueError, NOT_BEFORE_LEEWAY_SECS, TokenSigner};
use crate::tls::names::{FleetName, Principal, SubjectUri};

/// The authority's certificate, in the directory `cert init` writes.
pub const AUTHORITY_CERT_FILE: &str = "ca.crt";

/// The authority's private key, in the directory `cert init` writes. It never goes to a node.
pub const AUTHORITY_KEY_FILE: &str = "ca.key";

/// How long a node certificate lasts unless `cert node --ttl` says otherwise.
pub const DEFAULT_NODE_LIFETIME: Duration = Duration::from_secs(30 * SECONDS_PER_DAY);

/// How long a client certificate lasts unless `cert client --ttl` says otherwise.
pub const DEFAULT_CLIENT_LIFETIME: Duration = Duration::from_secs(7 * SECONDS_PER_DAY);

/// How long a delegation issuer certificate lasts unless `cert issuer --ttl` says otherwise, as
/// long as a node certificate (ADR-0047, point 1.2).
pub const DEFAULT_ISSUER_LIFETIME: Duration = Duration::from_secs(30 * SECONDS_PER_DAY);

/// The longest a delegation issuer certificate may last, from its start to its end (ADR-0047,
/// point 1.2). A start set in the past counts: a certificate backdated by a day may run 89 days
/// ahead. It bounds how long a stolen issuer key reaches when nobody notices.
pub const MAX_ISSUER_LIFETIME: Duration = Duration::from_secs(90 * SECONDS_PER_DAY);

/// How far beyond [`MAX_ISSUER_LIFETIME`] nodes accept an issuer certificate's validity, for one
/// that another authority tool backdated (ADR-0047, point 1.4.5). FleetRing's own issuing never
/// uses it.
pub const ISSUER_LIFETIME_SLACK: Duration = Duration::from_secs(SECONDS_PER_DAY);

/// How long the authority's own certificate lasts unless `cert init --ttl` says otherwise.
pub const DEFAULT_AUTHORITY_LIFETIME: Duration = Duration::from_secs(3_650 * SECONDS_PER_DAY);

const SECONDS_PER_DAY: u64 = 24 * 60 * 60;

/// How far [`Validity::issued_at`] sets a certificate's start back: a token's `nbf` leeway.
const BACKDATE_MILLIS: u64 = NOT_BEFORE_LEEWAY_SECS * 1_000;

/// The error returned when the authority can't be created, loaded, or used to issue.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum AuthorityError {
    /// A certificate or key file can't be read or written. The message names the file, never
    /// its contents.
    #[error("can't {action} {path}")]
    File {
        /// What was attempted.
        action: &'static str,
        /// The file.
        path: PathBuf,
        /// The underlying error.
        source: io::Error,
    },
    /// A file exists where a new one would be written.
    #[error("{path} exists; refusing to overwrite it")]
    Exists {
        /// The file.
        path: PathBuf,
    },
    /// The authority's certificate or key isn't usable.
    #[error("invalid authority material in {path}: {reason}")]
    Invalid {
        /// The file.
        path: PathBuf,
        /// What is wrong, without any of the file's contents.
        reason: String,
    },
    /// A name doesn't fit in a certificate.
    #[error("invalid name: {0}")]
    Name(#[from] crate::tls::names::NameError),
    /// An address isn't an IP address or a DNS name a certificate can carry.
    #[error("invalid address '{address}' for a certificate: expected an IP address or a DNS name")]
    Address {
        /// The address as given.
        address: String,
    },
    /// The validity doesn't fit a certificate's dates.
    #[error("the validity's dates don't fit a certificate")]
    Dates,
    /// Certificate generation failed.
    #[error("can't generate the certificate: {0}")]
    Generate(#[from] rcgen::Error),
    /// A delegation issuer certificate would last longer than [`MAX_ISSUER_LIFETIME`] from its
    /// start, counting any backdating.
    #[error(
        "an issuer certificate lasts at most 90 days from its start, counting any backdating; \
         asked for {seconds} s"
    )]
    IssuerLifetime {
        /// The lifetime asked for, in seconds.
        seconds: u64,
    },
}

/// When a certificate starts being valid, and for how long.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Validity {
    not_before: Timestamp,
    lifetime: Duration,
}

impl Validity {
    /// A certificate valid from `not_before` for `lifetime`.
    #[must_use]
    pub fn new(not_before: Timestamp, lifetime: Duration) -> Self {
        Self {
            not_before,
            lifetime,
        }
    }

    /// A certificate issued at `now` that lasts `lifetime` from then, with its start set back by
    /// the 30 seconds of clock skew a token's `nbf` allows (ADR-0026, point 5). A node or a
    /// client whose clock is behind the issuer's then accepts it as soon as it is written,
    /// instead of failing the TLS handshake until its clock catches up. It still ends `lifetime`
    /// after `now`. A delegation issuer certificate takes [`Validity::new`]: its start already
    /// has that leeway where nodes check it, and its 90 days count any backdating (ADR-0047,
    /// point 1.2).
    #[must_use]
    pub fn issued_at(now: Timestamp, lifetime: Duration) -> Self {
        let backdate = now.as_millis().min(BACKDATE_MILLIS);
        Self {
            not_before: Timestamp::from_millis(now.as_millis() - backdate),
            lifetime: lifetime.saturating_add(Duration::from_millis(backdate)),
        }
    }

    /// Returns when the certificate starts being valid.
    #[must_use]
    pub fn not_before(&self) -> Timestamp {
        self.not_before
    }

    /// Returns how long the certificate lasts.
    #[must_use]
    pub fn lifetime(&self) -> Duration {
        self.lifetime
    }

    fn dates(&self) -> Result<(OffsetDateTime, OffsetDateTime), AuthorityError> {
        let start = i64::try_from(self.not_before.as_millis() / 1_000)
            .map_err(|_| AuthorityError::Dates)?;
        let end = start
            .checked_add(i64::try_from(self.lifetime.as_secs()).map_err(|_| AuthorityError::Dates)?)
            .ok_or(AuthorityError::Dates)?;
        let not_before =
            OffsetDateTime::from_unix_timestamp(start).map_err(|_| AuthorityError::Dates)?;
        let not_after =
            OffsetDateTime::from_unix_timestamp(end).map_err(|_| AuthorityError::Dates)?;
        Ok((not_before, not_after))
    }
}

/// A certificate the authority issued, with its private key, both PEM-encoded.
///
/// `Debug` shows the certificate and never the key.
#[derive(Clone)]
pub struct Issued {
    subject: SubjectUri,
    certificate_pem: String,
    key_pem: String,
}

impl fmt::Debug for Issued {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Issued")
            .field("subject", &self.subject)
            .field("certificate_pem", &self.certificate_pem)
            .finish_non_exhaustive()
    }
}

impl Issued {
    /// Returns what the certificate names: the node, the principal, or the issuer.
    #[must_use]
    pub fn subject(&self) -> &SubjectUri {
        &self.subject
    }

    /// Returns the certificate, PEM-encoded.
    #[must_use]
    pub fn certificate_pem(&self) -> &str {
        &self.certificate_pem
    }

    /// Returns the private key, PEM-encoded (PKCS#8). Keep it out of logs.
    #[must_use]
    pub fn key_pem(&self) -> &str {
        &self.key_pem
    }

    /// Writes `<stem>.crt` and `<stem>.key` (mode 0600 on Unix) into `dir`, refusing to
    /// overwrite either.
    ///
    /// # Errors
    ///
    /// [`AuthorityError::Exists`] if either file exists; [`AuthorityError::File`] if one can't
    /// be written.
    pub fn write(&self, dir: &Path, stem: &str) -> Result<(), AuthorityError> {
        write_new(
            &dir.join(format!("{stem}.crt")),
            &self.certificate_pem,
            Mode::Public,
        )?;
        write_new(
            &dir.join(format!("{stem}.key")),
            &self.key_pem,
            Mode::Secret,
        )
    }
}

/// The fleet's certificate authority: a self-signed Ed25519 certificate and its key.
///
/// `Debug` shows the fleet and the certificate and never the key.
pub struct Authority {
    fleet: FleetName,
    certificate_pem: String,
    /// The parameters the certificate was built from, which issuing signs under.
    params: CertificateParams,
    key: KeyPair,
}

impl fmt::Debug for Authority {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Authority")
            .field("fleet", &self.fleet)
            .field("certificate_pem", &self.certificate_pem)
            .finish_non_exhaustive()
    }
}

/// What a certificate is for, which sets its key usages.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Purpose {
    Node,
    Client,
    /// A delegation issuer: no extended key usage, so no TLS stack takes it for a client or a
    /// server (ADR-0047, point 1.2).
    Issuer,
}

impl Authority {
    /// Creates a new authority for `fleet`, valid as `validity` says.
    ///
    /// # Errors
    ///
    /// [`AuthorityError`] if the dates don't fit or generation fails.
    pub fn create(fleet: FleetName, validity: Validity) -> Result<Self, AuthorityError> {
        let key = KeyPair::generate_for(&PKCS_ED25519)?;
        let (not_before, not_after) = validity.dates()?;
        let mut params = authority_params(&fleet)?;
        params.not_before = not_before;
        params.not_after = not_after;
        params.serial_number = Some(serial(&[
            b"authority",
            fleet.as_str().as_bytes(),
            &validity.not_before.as_millis().to_le_bytes(),
        ]));
        let certificate = params.self_signed(&key)?;
        Ok(Self {
            fleet,
            certificate_pem: certificate.pem(),
            params,
            key,
        })
    }

    /// Loads the authority from `ca.crt` and `ca.key` in `dir`.
    ///
    /// # Errors
    ///
    /// [`AuthorityError::File`] if a file can't be read; [`AuthorityError::Invalid`] if the
    /// certificate isn't one `cert init` wrote or the key doesn't match it.
    pub fn load(dir: &Path) -> Result<Self, AuthorityError> {
        let cert_path = dir.join(AUTHORITY_CERT_FILE);
        let key_path = dir.join(AUTHORITY_KEY_FILE);
        let certificate_pem = read(&cert_path)?;
        let key_pem = read(&key_path)?;
        let invalid = |path: &Path, reason: String| AuthorityError::Invalid {
            path: path.to_owned(),
            reason,
        };
        let certificate = CertificateDer::from_pem_slice(certificate_pem.as_bytes())
            .map_err(|error| invalid(&cert_path, format!("not a PEM certificate: {error}")))?;
        let fleet = match SubjectUri::of_certificate(&certificate) {
            Ok(SubjectUri::Authority { fleet }) => fleet,
            Ok(other) => {
                return Err(invalid(
                    &cert_path,
                    format!("names {other}, not an authority"),
                ));
            }
            Err(error) => return Err(invalid(&cert_path, error.to_string())),
        };
        let key = KeyPair::from_pem(&key_pem)
            .map_err(|_| invalid(&key_path, "not a PEM private key".to_owned()))?;
        if !key.is_compatible(&PKCS_ED25519) {
            return Err(invalid(&key_path, "not an Ed25519 key".to_owned()));
        }
        let (_, parsed) = X509Certificate::from_der(certificate.as_ref())
            .map_err(|_| invalid(&cert_path, "not an X.509 certificate".to_owned()))?;
        if parsed.public_key().subject_public_key.as_ref() != key.public_key_raw() {
            return Err(invalid(
                &key_path,
                "the key doesn't match the certificate".to_owned(),
            ));
        }
        // The certificate was written by `cert init`, so its subject is what `create` builds.
        let params = authority_params(&fleet)?;
        Ok(Self {
            fleet,
            certificate_pem,
            params,
            key,
        })
    }

    /// Writes `ca.crt` and `ca.key` (mode 0600 on Unix) into `dir`, creating the directory
    /// and refusing to overwrite either file.
    ///
    /// # Errors
    ///
    /// [`AuthorityError::Exists`] if either file exists; [`AuthorityError::File`] if one can't
    /// be written.
    pub fn write(&self, dir: &Path) -> Result<(), AuthorityError> {
        fs::create_dir_all(dir).map_err(|source| AuthorityError::File {
            action: "create",
            path: dir.to_owned(),
            source,
        })?;
        write_new(
            &dir.join(AUTHORITY_CERT_FILE),
            &self.certificate_pem,
            Mode::Public,
        )?;
        write_new(
            &dir.join(AUTHORITY_KEY_FILE),
            &self.key.serialize_pem(),
            Mode::Secret,
        )
    }

    /// Returns the fleet the authority issues for.
    #[must_use]
    pub fn fleet(&self) -> &FleetName {
        &self.fleet
    }

    /// Returns the authority's certificate, PEM-encoded: what every node and client trusts.
    #[must_use]
    pub fn certificate_pem(&self) -> &str {
        &self.certificate_pem
    }

    /// Returns a signer that issues bearer tokens for this fleet with the authority's key
    /// (ADR-0025). Nodes verify them with the authority certificate they already trust.
    ///
    /// # Errors
    ///
    /// [`IssueError::Key`](crate::auth::IssueError::Key) if the key isn't one tokens can be
    /// signed with.
    pub fn token_signer(&self) -> Result<TokenSigner, IssueError> {
        TokenSigner::from_pkcs8(&self.key.serialize_der(), self.fleet.as_str())
    }

    /// Issues a node certificate for `id`, reachable at `addresses` (IP addresses or DNS names),
    /// with the URI `fleetring://<fleet>/node/<id>` as its subject alternative name.
    ///
    /// # Errors
    ///
    /// [`AuthorityError`] if an address isn't a valid IP address or DNS name, the dates don't
    /// fit, or generation fails.
    pub fn issue_node(
        &self,
        id: &NodeId,
        addresses: &[String],
        validity: Validity,
    ) -> Result<Issued, AuthorityError> {
        let subject = SubjectUri::node(&self.fleet, id);
        let mut names = Vec::with_capacity(addresses.len() + 1);
        names.push(SanType::URI(subject.to_string().try_into()?));
        for address in addresses {
            names.push(address_san(address)?);
        }
        self.issue(subject, names, Purpose::Node, validity)
    }

    /// Issues a client certificate for `principal`, with the URI
    /// `fleetring://<fleet>/principal/<name>` as its subject alternative name.
    ///
    /// # Errors
    ///
    /// [`AuthorityError`] if the dates don't fit or generation fails.
    pub fn issue_client(
        &self,
        principal: &Principal,
        validity: Validity,
    ) -> Result<Issued, AuthorityError> {
        let subject = SubjectUri::principal(&self.fleet, principal);
        let names = vec![SanType::URI(subject.to_string().try_into()?)];
        self.issue(subject, names, Purpose::Client, validity)
    }

    /// Issues a delegation issuer certificate for `name` (ADR-0047, point 1.2), with a new Ed25519
    /// key and the URI `fleetring://<fleet>/issuer/<name>` as its only subject alternative name.
    ///
    /// The certificate isn't an authority (basic constraints say so), its key usage is digital
    /// signature alone, and it has no extended key usage, so no TLS stack accepts it as a client
    /// or a server certificate. Each call makes a new key, so renewing rotates the key.
    ///
    /// `validity` runs from its start, so a start set in the past counts against
    /// [`MAX_ISSUER_LIFETIME`]: a caller that backdates must add the backdating to the lifetime.
    ///
    /// # Errors
    ///
    /// [`AuthorityError::IssuerLifetime`] if `validity` lasts longer than
    /// [`MAX_ISSUER_LIFETIME`]; otherwise [`AuthorityError`] if the dates don't fit or generation
    /// fails.
    pub fn issue_issuer(
        &self,
        name: &NodeId,
        validity: Validity,
    ) -> Result<Issued, AuthorityError> {
        if validity.lifetime > MAX_ISSUER_LIFETIME {
            return Err(AuthorityError::IssuerLifetime {
                seconds: validity.lifetime.as_secs(),
            });
        }
        let subject = SubjectUri::issuer(&self.fleet, name);
        let names = vec![SanType::URI(subject.to_string().try_into()?)];
        self.issue(subject, names, Purpose::Issuer, validity)
    }

    /// Signs `params` for `key` with the authority's key, whatever they hold: for tests that need
    /// a certificate the issuing methods refuse to make.
    #[cfg(test)]
    pub(crate) fn sign_for_test(
        &self,
        params: &CertificateParams,
        key: &KeyPair,
    ) -> Result<rcgen::Certificate, rcgen::Error> {
        let issuer = Issuer::from_params(&self.params, &self.key);
        params.signed_by(key, &issuer)
    }

    fn issue(
        &self,
        subject: SubjectUri,
        names: Vec<SanType>,
        purpose: Purpose,
        validity: Validity,
    ) -> Result<Issued, AuthorityError> {
        let key = KeyPair::generate_for(&PKCS_ED25519)?;
        let (not_before, not_after) = validity.dates()?;
        let mut params = CertificateParams::default();
        params.not_before = not_before;
        params.not_after = not_after;
        params.distinguished_name = distinguished_name(&subject.to_string());
        params.is_ca = IsCa::ExplicitNoCa;
        params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
        params.extended_key_usages = match purpose {
            Purpose::Node => vec![
                ExtendedKeyUsagePurpose::ServerAuth,
                ExtendedKeyUsagePurpose::ClientAuth,
            ],
            Purpose::Client => vec![ExtendedKeyUsagePurpose::ClientAuth],
            Purpose::Issuer => Vec::new(),
        };
        params.use_authority_key_identifier_extension = true;
        let mut serial_input: Vec<&[u8]> = vec![b"issued"];
        let subject_text = subject.to_string();
        let not_before_bytes = validity.not_before.as_millis().to_le_bytes();
        let lifetime_bytes = validity.lifetime.as_secs().to_le_bytes();
        serial_input.push(subject_text.as_bytes());
        serial_input.push(&not_before_bytes);
        serial_input.push(&lifetime_bytes);
        let public = key.public_key_raw();
        serial_input.push(public);
        params.serial_number = Some(serial(&serial_input));
        params.subject_alt_names = names;
        let issuer = Issuer::from_params(&self.params, &self.key);
        let certificate = params.signed_by(&key, &issuer)?;
        Ok(Issued {
            subject,
            certificate_pem: certificate.pem(),
            key_pem: key.serialize_pem(),
        })
    }
}

/// A serial number derived from `parts`: unique per subject and validity, with a fixed encoded
/// length so that certificates issued the same way have the same size.
fn serial(parts: &[&[u8]]) -> SerialNumber {
    let mut hasher = twox_hash::XxHash3_128::new();
    for part in parts {
        hasher.write(&u64::try_from(part.len()).unwrap_or(u64::MAX).to_le_bytes());
        hasher.write(part);
    }
    let mut bytes = hasher.finish_128().to_be_bytes();
    // A leading byte below 0x80 keeps the DER integer positive without a padding byte.
    bytes[0] &= 0x7f;
    bytes[0] |= 0x01;
    SerialNumber::from(bytes.to_vec())
}

/// The authority certificate's parameters, apart from its dates and serial: a CA that signs
/// certificates, named by its fleet.
fn authority_params(fleet: &FleetName) -> Result<CertificateParams, AuthorityError> {
    let mut params = CertificateParams::default();
    params.distinguished_name = distinguished_name(&format!("fleetring {fleet} authority"));
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
    params.subject_alt_names = vec![SanType::URI(
        SubjectUri::authority(fleet).to_string().try_into()?,
    )];
    Ok(params)
}

fn distinguished_name(common_name: &str) -> DistinguishedName {
    let mut name = DistinguishedName::new();
    name.push(DnType::CommonName, common_name);
    name
}

fn address_san(address: &str) -> Result<SanType, AuthorityError> {
    if let Ok(ip) = address.parse::<IpAddr>() {
        return Ok(SanType::IpAddress(ip));
    }
    let valid = !address.is_empty()
        && address
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'.');
    if !valid {
        return Err(AuthorityError::Address {
            address: address.to_owned(),
        });
    }
    address
        .try_into()
        .map(SanType::DnsName)
        .map_err(|_| AuthorityError::Address {
            address: address.to_owned(),
        })
}

/// Whether a file holds a secret, which sets its mode on Unix.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    Public,
    Secret,
}

fn write_new(path: &Path, contents: &str, mode: Mode) -> Result<(), AuthorityError> {
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.mode(match mode {
            Mode::Public => 0o644,
            Mode::Secret => 0o600,
        });
    }
    #[cfg(not(unix))]
    let _ = mode;
    let mut file = options.open(path).map_err(|source| {
        if source.kind() == io::ErrorKind::AlreadyExists {
            AuthorityError::Exists {
                path: path.to_owned(),
            }
        } else {
            AuthorityError::File {
                action: "create",
                path: path.to_owned(),
                source,
            }
        }
    })?;
    file.write_all(contents.as_bytes())
        .and_then(|()| file.sync_all())
        .map_err(|source| AuthorityError::File {
            action: "write",
            path: path.to_owned(),
            source,
        })
}

fn read(path: &Path) -> Result<String, AuthorityError> {
    fs::read_to_string(path).map_err(|source| AuthorityError::File {
        action: "read",
        path: path.to_owned(),
        source,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn validity() -> Validity {
        Validity::new(
            Timestamp::from_millis(0),
            Duration::from_secs(SECONDS_PER_DAY),
        )
    }

    #[test]
    fn serials_have_a_fixed_length_and_differ_by_input() {
        let a = serial(&[b"a"]);
        let b = serial(&[b"b"]);
        assert_ne!(a, b);
        assert_eq!(a.as_ref().len(), 16);
        assert!(a.as_ref()[0] < 0x80 && a.as_ref()[0] != 0);
    }

    #[test]
    fn a_written_authority_loads_back_and_refuses_to_overwrite() {
        let dir = tempfile::tempdir().expect("tempdir");
        let fleet: FleetName = "lab".parse().expect("fleet name");
        let authority = Authority::create(fleet.clone(), validity()).expect("create");
        authority.write(dir.path()).expect("write");
        let loaded = Authority::load(dir.path()).expect("load");
        assert_eq!(loaded.fleet(), &fleet);
        assert_eq!(loaded.certificate_pem(), authority.certificate_pem());
        let again = authority.write(dir.path()).expect_err("overwrite");
        assert!(matches!(again, AuthorityError::Exists { .. }), "{again}");
        let debug = format!("{authority:?}");
        assert!(!debug.contains("PRIVATE KEY"), "{debug}");
    }

    #[test]
    fn issued_at_starts_30_seconds_early_and_ends_lifetime_after_now() {
        let lifetime = Duration::from_secs(SECONDS_PER_DAY);
        let now = Timestamp::from_millis(1_800_000_000_250);
        let validity = Validity::issued_at(now, lifetime);
        assert_eq!(
            validity.not_before(),
            Timestamp::from_millis(1_799_999_970_250)
        );
        let (start, end) = validity.dates().expect("dates");
        assert_eq!(start.unix_timestamp(), 1_799_999_970);
        assert_eq!(end.unix_timestamp(), 1_800_000_000 + 86_400);
        // Near the epoch it starts at the epoch, still ending `lifetime` after `now`.
        let early = Validity::issued_at(Timestamp::from_millis(10_000), lifetime);
        assert_eq!(early.not_before(), Timestamp::ZERO);
        assert_eq!(
            early.dates().expect("dates").1.unix_timestamp(),
            10 + 86_400
        );
    }

    #[test]
    fn issued_certificates_name_their_subject() {
        let fleet: FleetName = "lab".parse().expect("fleet name");
        let authority = Authority::create(fleet, validity()).expect("create");
        let id: NodeId = "n1".parse().expect("node id");
        let node = authority
            .issue_node(&id, &["127.0.0.1".to_owned(), "n1".to_owned()], validity())
            .expect("issue node");
        assert_eq!(node.subject().to_string(), "fleetring://lab/node/n1");
        let principal: Principal = "alice".parse().expect("principal");
        let client = authority
            .issue_client(&principal, validity())
            .expect("issue client");
        assert_eq!(
            client.subject().to_string(),
            "fleetring://lab/principal/alice"
        );
        let debug = format!("{client:?}");
        assert!(!debug.contains("PRIVATE KEY"), "{debug}");
        let bad = authority.issue_node(&id, &["a b".to_owned()], validity());
        assert!(matches!(bad, Err(AuthorityError::Address { .. })));
    }
}
