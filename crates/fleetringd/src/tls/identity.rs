//! A node's or client's identity: its certificate chain, key, and the authorities it trusts,
//! reloaded from disk when the files change.

use std::fmt;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use arc_swap::ArcSwap;
use rustls::crypto::CryptoProvider;
use rustls::server::{VerifierBuilderError, WebPkiClientVerifier};
use rustls::{ClientConfig, RootCertStore, ServerConfig};
use rustls_pki_types::pem::PemObject as _;
use rustls_pki_types::{CertificateDer, PrivateKeyDer};
use x509_parser::oid_registry::OID_SIG_ED25519;
use x509_parser::prelude::{FromDer as _, X509Certificate};

use crate::auth::{TokenVerifier, VerifyingKey};
use crate::tls::names::{FleetName, NameError, SubjectUri};
use crate::tls::{Issued, Peer};

/// How long after one check of the files the next connection may check them again.
const RELOAD_CHECK_INTERVAL: Duration = Duration::from_secs(1);

/// The ALPN protocol every connection negotiates.
const ALPN_H2: &[u8] = b"h2";

/// The ALPN protocol the admin listener negotiates (ADR-0034).
const ALPN_HTTP1: &[u8] = b"http/1.1";

/// The error returned when an identity can't be loaded.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum IdentityError {
    /// A file can't be read. The message names the file, never its contents.
    #[error("can't read {path}")]
    Read {
        /// The file.
        path: PathBuf,
        /// The underlying error.
        source: io::Error,
    },
    /// A file isn't the PEM the daemon expects. The message says which, never what it holds.
    #[error("{path} isn't a PEM {expected}")]
    Pem {
        /// The file.
        path: PathBuf,
        /// What the file should hold.
        expected: &'static str,
    },
    /// The certificate's subject alternative name isn't one the authority issues.
    #[error("the certificate's subject is invalid")]
    Name(#[from] NameError),
    /// The certificate names the authority, not a node or a principal.
    #[error("the certificate names an authority, not a node or a principal")]
    Authority,
    /// The certificate and key don't form a usable identity.
    #[error("the certificate and key can't be used")]
    Rustls(#[from] rustls::Error),
    /// The trusted authorities can't verify clients.
    #[error("the trusted authorities can't be used to verify peers")]
    Verifier(#[from] VerifierBuilderError),
}

/// Where an identity's material comes from.
#[derive(Debug, Clone)]
enum Source {
    /// Files, re-read when they change.
    Files {
        ca: PathBuf,
        cert: PathBuf,
        key: PathBuf,
    },
    /// PEM text given at construction; it never changes.
    Memory,
}

/// The modification times and sizes of the files, to notice a change without reading them.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Stamp(Vec<Option<(SystemTime, u64)>>);

impl Stamp {
    fn of(paths: &[&Path]) -> Self {
        Self(
            paths
                .iter()
                .map(|path| {
                    let metadata = fs::metadata(path).ok()?;
                    Some((metadata.modified().ok()?, metadata.len()))
                })
                .collect(),
        )
    }

    fn none() -> Self {
        Self(Vec::new())
    }
}

/// The material in use: built once per load, shared by every connection opened meanwhile.
pub(crate) struct Loaded {
    subject: SubjectUri,
    peer: Peer,
    server: Arc<ServerConfig>,
    /// The admin listener's: the same certificate, asking for a client certificate without
    /// requiring one, over HTTP/1.1.
    admin_server: Arc<ServerConfig>,
    client: Arc<ClientConfig>,
    /// The trusted authorities' Ed25519 keys, which sign the bearer tokens nodes accept.
    token_keys: Vec<VerifyingKey>,
    stamp: Stamp,
}

impl Loaded {
    /// Builds the material from PEM text. Each input is paired with the path its errors name.
    fn from_pem(
        ca: (&Path, &[u8]),
        cert: (&Path, &[u8]),
        key: (&Path, &[u8]),
        stamp: Stamp,
    ) -> Result<Self, IdentityError> {
        let pem = |path: &Path, expected: &'static str| IdentityError::Pem {
            path: path.to_owned(),
            expected,
        };
        let authorities = CertificateDer::pem_slice_iter(ca.1)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| pem(ca.0, "certificate bundle"))?;
        let chain = CertificateDer::pem_slice_iter(cert.1)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| pem(cert.0, "certificate"))?;
        let private_key =
            PrivateKeyDer::from_pem_slice(key.1).map_err(|_| pem(key.0, "private key"))?;
        let end_entity = chain.first().ok_or_else(|| pem(cert.0, "certificate"))?;
        let subject = SubjectUri::of_certificate(end_entity)?;
        let peer = match &subject {
            SubjectUri::Node { id, .. } => Peer::Node(id.clone()),
            SubjectUri::Principal { name, .. } => Peer::Principal(name.clone()),
            _ => return Err(IdentityError::Authority),
        };
        let token_keys = authorities.iter().filter_map(token_key).collect();
        let mut roots = RootCertStore::empty();
        for authority in authorities {
            roots.add(authority)?;
        }
        let roots = Arc::new(roots);
        let verifier =
            WebPkiClientVerifier::builder_with_provider(Arc::clone(&roots), provider()).build()?;
        let mut server = ServerConfig::builder_with_provider(provider())
            .with_protocol_versions(&[&rustls::version::TLS13])?
            .with_client_cert_verifier(verifier)
            .with_single_cert(chain.clone(), private_key.clone_key())?;
        server.alpn_protocols = vec![ALPN_H2.to_vec()];
        let admin_verifier =
            WebPkiClientVerifier::builder_with_provider(Arc::clone(&roots), provider())
                .allow_unauthenticated()
                .build()?;
        let mut admin_server = ServerConfig::builder_with_provider(provider())
            .with_protocol_versions(&[&rustls::version::TLS13])?
            .with_client_cert_verifier(admin_verifier)
            .with_single_cert(chain.clone(), private_key.clone_key())?;
        admin_server.alpn_protocols = vec![ALPN_HTTP1.to_vec()];
        let mut client = ClientConfig::builder_with_provider(provider())
            .with_protocol_versions(&[&rustls::version::TLS13])?
            .with_root_certificates(Arc::clone(&roots))
            .with_client_auth_cert(chain, private_key)?;
        client.alpn_protocols = vec![ALPN_H2.to_vec()];
        Ok(Self {
            subject,
            peer,
            server: Arc::new(server),
            admin_server: Arc::new(admin_server),
            client: Arc::new(client),
            token_keys,
            stamp,
        })
    }

    fn from_files(ca: &Path, cert: &Path, key: &Path) -> Result<Self, IdentityError> {
        // The stamp is taken before the files are read, so a change during the read is noticed
        // at the next check rather than missed.
        let stamp = Stamp::of(&[ca, cert, key]);
        let read = |path: &Path| {
            fs::read(path).map_err(|source| IdentityError::Read {
                path: path.to_owned(),
                source,
            })
        };
        let (ca_pem, cert_pem, key_pem) = (read(ca)?, read(cert)?, read(key)?);
        Self::from_pem((ca, &ca_pem), (cert, &cert_pem), (key, &key_pem), stamp)
    }

    pub(crate) fn server_config(&self) -> Arc<ServerConfig> {
        Arc::clone(&self.server)
    }

    pub(crate) fn admin_server_config(&self) -> Arc<ServerConfig> {
        Arc::clone(&self.admin_server)
    }

    pub(crate) fn client_config(&self) -> Arc<ClientConfig> {
        Arc::clone(&self.client)
    }

    pub(crate) fn fleet(&self) -> &FleetName {
        self.subject.fleet()
    }
}

/// The Ed25519 key of an authority certificate, which can sign tokens; `None` for an authority
/// with another kind of key, such as one from an external certificate authority.
fn token_key(certificate: &CertificateDer<'_>) -> Option<VerifyingKey> {
    let (_, parsed) = X509Certificate::from_der(certificate.as_ref()).ok()?;
    let key = parsed.public_key();
    if key.algorithm.algorithm != OID_SIG_ED25519 {
        return None;
    }
    VerifyingKey::from_raw(key.subject_public_key.as_ref())
}

fn provider() -> Arc<CryptoProvider> {
    Arc::new(rustls::crypto::ring::default_provider())
}

/// A certificate chain, its private key, and the trusted authority certificates, as a node or
/// a client presents them.
///
/// An identity loaded from files re-reads them when their modification time or size changes.
/// The check runs when a connection is opened, at most once per second;
/// a reload that fails keeps the material in use and logs a warning. The authority file may
/// hold several certificates, so two authorities are trusted while a rotation overlaps.
///
/// Cloning is cheap; clones share the material and reload together. `Debug` shows the subject
/// and the sources, never key material.
#[derive(Clone)]
pub struct Identity {
    inner: Arc<Inner>,
}

struct Inner {
    source: Source,
    loaded: ArcSwap<Loaded>,
    checked: Mutex<Option<tokio::time::Instant>>,
}

impl fmt::Debug for Identity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Identity")
            .field("subject", &self.inner.loaded.load().subject)
            .field("source", &self.inner.source)
            .finish_non_exhaustive()
    }
}

impl Identity {
    /// Loads the identity from `ca` (the trusted authority certificates), `cert` (the
    /// certificate chain), and `key` (the private key), all PEM, and reloads them when they
    /// change.
    ///
    /// # Errors
    ///
    /// [`IdentityError`] if a file can't be read or isn't what it should be.
    pub fn load(ca: &Path, cert: &Path, key: &Path) -> Result<Self, IdentityError> {
        let loaded = Loaded::from_files(ca, cert, key)?;
        Ok(Self::new(
            Source::Files {
                ca: ca.to_owned(),
                cert: cert.to_owned(),
                key: key.to_owned(),
            },
            loaded,
        ))
    }

    /// An identity from PEM text: the trusted authority certificates, the certificate chain,
    /// and the private key. It never reloads.
    ///
    /// # Errors
    ///
    /// [`IdentityError`] if the text isn't what it should be.
    pub fn from_pem(ca_pem: &str, cert_pem: &str, key_pem: &str) -> Result<Self, IdentityError> {
        let loaded = Loaded::from_pem(
            (Path::new("ca"), ca_pem.as_bytes()),
            (Path::new("cert"), cert_pem.as_bytes()),
            (Path::new("key"), key_pem.as_bytes()),
            Stamp::none(),
        )?;
        Ok(Self::new(Source::Memory, loaded))
    }

    /// An identity from a certificate `issued` by the authority whose certificate is
    /// `authority_pem`. It never reloads.
    ///
    /// # Errors
    ///
    /// [`IdentityError`] if the material isn't what it should be.
    pub fn from_issued(authority_pem: &str, issued: &Issued) -> Result<Self, IdentityError> {
        Self::from_pem(authority_pem, issued.certificate_pem(), issued.key_pem())
    }

    fn new(source: Source, loaded: Loaded) -> Self {
        Self {
            inner: Arc::new(Inner {
                source,
                loaded: ArcSwap::from_pointee(loaded),
                checked: Mutex::new(None),
            }),
        }
    }

    /// Returns what the certificate names: this node, or this principal.
    #[must_use]
    pub fn peer(&self) -> Peer {
        self.inner.loaded.load().peer.clone()
    }

    /// Returns the fleet the certificate was issued for.
    #[must_use]
    pub fn fleet(&self) -> FleetName {
        self.inner.loaded.load().subject.fleet().clone()
    }

    /// Returns what bearer tokens are checked against: this fleet, and the Ed25519 keys of
    /// the trusted authorities, as of the last reload (ADR-0025). During an authority rotation
    /// both authorities' tokens verify.
    #[must_use]
    pub fn token_verifier(&self) -> TokenVerifier {
        let loaded = self.current();
        TokenVerifier::new(loaded.fleet().as_str(), loaded.token_keys.iter().cloned())
    }

    /// The material in use, after checking the files for a change if a check is due.
    ///
    /// # Panics
    ///
    /// If the identity's lock was poisoned by a panic in another thread.
    pub(crate) fn current(&self) -> Arc<Loaded> {
        if let Source::Files { ca, cert, key } = &self.inner.source
            && self.check_due()
        {
            let stamp = Stamp::of(&[ca, cert, key]);
            if stamp != self.inner.loaded.load().stamp {
                match Loaded::from_files(ca, cert, key) {
                    Ok(loaded) => {
                        tracing::info!(
                            subject = %loaded.subject,
                            "reloaded the certificate, key, and trusted authorities",
                        );
                        self.inner.loaded.store(Arc::new(loaded));
                    }
                    Err(error) => tracing::warn!(
                        %error,
                        "the certificate files changed but can't be loaded; keeping the ones in use",
                    ),
                }
            }
        }
        self.inner.loaded.load_full()
    }

    fn check_due(&self) -> bool {
        let now = tokio::time::Instant::now();
        let mut checked = self.inner.checked.lock().expect("identity lock poisoned");
        match *checked {
            Some(at) if now.duration_since(at) < RELOAD_CHECK_INTERVAL => false,
            _ => {
                *checked = Some(now);
                true
            }
        }
    }
}
