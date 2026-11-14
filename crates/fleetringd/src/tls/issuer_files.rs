//! The control node's delegation issuer pair (ADR-0047, points 1.3 and 1.7): the files
//! `[control] issuer_cert` and `issuer_key` name, checked at load, reloaded when they change, and,
//! on a dev node, replaced before they expire.
//!
//! [`IssuerPair::load`] refuses a pair whose certificate fails [`IssuerCertificate::check`] against
//! the node's trusted authorities, or whose key isn't the certificate's. An expired certificate
//! loads, with a warning: signing refuses it (point 1.6), and the time is checked there. A reload
//! that finds a pair it can't use keeps the one it had and logs why.
//!
//! On a dev node, [`IssuerPair::load_dev`] also replaces the pair, from the dev authority, when
//! fewer than [`DEV_RENEW_BEFORE`] remain, when it has expired, or when it can't be used. It
//! writes both files to temporary names beside them, syncs them, and renames the key, then the
//! certificate, into place, so a reader between the two renames finds a mismatch and keeps the
//! pair it had.

use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write as _};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arc_swap::ArcSwap;
use fleetring::NodeId;
use fleetring_control::{KeyId, Timestamp};
use ring::signature::{Ed25519KeyPair, KeyPair as _};
use rustls_pki_types::pem::PemObject as _;
use rustls_pki_types::{CertificateDer, PrivateKeyDer};
use tokio::task::JoinHandle;
use zeroize::Zeroizing;

use crate::auth::{DelegationSigner, IssuerError, VerifyingKey};
use crate::clock::Clock;
use crate::tls::names::FleetName;
use crate::tls::{
    Authority, AuthorityError, Identity, Issued, IssuerCertError, IssuerCertificate,
    MAX_ISSUER_LIFETIME, Validity,
};

/// How long after one check of the files the next call may check them again, as for the node's
/// own certificate.
const RELOAD_CHECK_INTERVAL: Duration = Duration::from_secs(1);

/// How often a running control node checks its pair even when nothing asks for it, so that the
/// expiry warning and a dev node's replacement don't wait for a delegation.
const WATCH_INTERVAL: Duration = Duration::from_secs(60 * 60);

const SECONDS_PER_DAY: u64 = 24 * 60 * 60;

/// How long before the certificate expires the node starts warning (ADR-0047, point 1.3).
pub const EXPIRY_WARNING_WINDOW: Duration = Duration::from_secs(7 * SECONDS_PER_DAY);

/// How often the expiry warning repeats.
const EXPIRY_WARNING_EVERY: Duration = Duration::from_secs(SECONDS_PER_DAY);

/// How long a dev node's issuer certificate lasts: the most an issuer certificate may
/// (ADR-0047, point 1.7).
pub const DEV_ISSUER_LIFETIME: Duration = MAX_ISSUER_LIFETIME;

/// A dev node replaces its issuer pair when less than this remains of the certificate
/// (ADR-0047, point 1.7).
pub const DEV_RENEW_BEFORE: Duration = Duration::from_secs(30 * SECONDS_PER_DAY);

/// The dev issuer's certificate, in dev mode's TLS directory.
pub const DEV_ISSUER_CERT_FILE: &str = "issuer.dev.crt";

/// The dev issuer's key, in dev mode's TLS directory.
pub const DEV_ISSUER_KEY_FILE: &str = "issuer.dev.key";

/// What a temporary name adds to the name of the file it replaces.
const TEMPORARY_SUFFIX: &str = ".tmp";

/// The dev issuer's two files and their temporary names: everything a dev node writes into its
/// TLS directory for the issuer.
pub(crate) const DEV_ISSUER_FILES: [&str; 4] = [
    DEV_ISSUER_CERT_FILE,
    DEV_ISSUER_KEY_FILE,
    "issuer.dev.crt.tmp",
    "issuer.dev.key.tmp",
];

/// The two files of a delegation issuer pair: `[control] issuer_cert` and `issuer_key`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IssuerFiles {
    /// The PEM file with the issuer certificate.
    pub cert: PathBuf,
    /// The PEM file with the issuer's Ed25519 private key (PKCS#8).
    pub key: PathBuf,
}

impl IssuerFiles {
    /// The dev issuer's files in dev mode's TLS directory `tls_dir`: [`DEV_ISSUER_CERT_FILE`] and
    /// [`DEV_ISSUER_KEY_FILE`].
    #[must_use]
    pub fn dev(tls_dir: &Path) -> Self {
        Self {
            cert: tls_dir.join(DEV_ISSUER_CERT_FILE),
            key: tls_dir.join(DEV_ISSUER_KEY_FILE),
        }
    }
}

/// The error returned when an issuer pair can't be loaded or replaced. It names both files, and
/// its source says what is wrong; neither ever quotes the key.
#[derive(Debug, thiserror::Error)]
#[error("{cert} and {key} aren't a usable delegation issuer pair")]
pub struct IssuerFilesError {
    cert: PathBuf,
    key: PathBuf,
    #[source]
    problem: IssuerFileProblem,
}

impl IssuerFilesError {
    pub(crate) fn new(files: &IssuerFiles, problem: IssuerFileProblem) -> Self {
        Self {
            cert: files.cert.clone(),
            key: files.key.clone(),
            problem,
        }
    }

    /// Returns the certificate file.
    #[must_use]
    pub fn cert(&self) -> &Path {
        &self.cert
    }

    /// Returns the key file.
    #[must_use]
    pub fn key(&self) -> &Path {
        &self.key
    }

    /// Returns what is wrong with the pair.
    #[must_use]
    pub fn problem(&self) -> &IssuerFileProblem {
        &self.problem
    }
}

/// What is wrong with an issuer pair. The messages name files, never what they hold.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum IssuerFileProblem {
    /// A file can't be read.
    #[error("can't read {path}")]
    Read {
        /// The file.
        path: PathBuf,
        /// The underlying error.
        source: io::Error,
    },
    /// A file isn't the PEM it should be.
    #[error("{path} isn't {expected}")]
    Pem {
        /// The file.
        path: PathBuf,
        /// What it should hold.
        expected: &'static str,
    },
    /// The key file doesn't hold an Ed25519 private key.
    #[error("{path} isn't an Ed25519 private key")]
    Key {
        /// The file.
        path: PathBuf,
    },
    /// The certificate fails a check of ADR-0047 point 1.4 that depends on neither a token nor
    /// the time.
    #[error(transparent)]
    Certificate(#[from] IssuerCertError),
    /// The key isn't the certificate's.
    #[error("the key doesn't match the certificate")]
    Mismatch,
    /// The node has no TLS identity, so no trusted authority to check the certificate against.
    #[error(
        "the node has no [tls] identity, so no trusted authority to check the issuer certificate \
         against"
    )]
    NoIdentity,
    /// A dev node can't issue a new pair from its authority.
    #[error("can't issue a new dev issuer pair")]
    Authority(#[source] AuthorityError),
    /// A dev node can't write a new pair.
    #[error("can't {action} {path}")]
    Write {
        /// What was attempted.
        action: &'static str,
        /// The file.
        path: PathBuf,
        /// The underlying error.
        source: io::Error,
    },
}

/// A digest of each file's contents, to notice a change.
///
/// Modification times and sizes aren't enough: a replacement key is the same size, and a write
/// within the file system's timestamp granularity leaves the time unchanged, so such a change
/// would go unnoticed. The files are a few hundred bytes and checked at most once a second, so
/// reading them is cheap. The digest of the key file reveals nothing about the key.
///
/// The stamp also holds the IDs of the trusted authority keys the pair was checked against, so
/// a change to the node's `[tls] ca` bundle checks the pair again, as a change to its files
/// does: a pair whose certifying authority left the bundle is no longer one nodes accept.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Stamp {
    files: [Option<[u8; 32]>; 2],
    authorities: Vec<KeyId>,
}

impl Stamp {
    fn of(files: &IssuerFiles, authorities: &[VerifyingKey]) -> Self {
        let one = |path: &Path| {
            fs::read(path)
                .ok()
                .map(|bytes| *blake3::hash(&bytes).as_bytes())
        };
        Self {
            files: [one(&files.cert), one(&files.key)],
            authorities: authorities.iter().map(|key| key.id().clone()).collect(),
        }
    }
}

/// A pair that passed the load checks: the certificate, and the key's PKCS#8 bytes, which are
/// wiped when the last reference goes.
///
/// `Debug` shows the certificate, never the key.
pub(crate) struct LoadedIssuer {
    certificate: IssuerCertificate,
    key: Zeroizing<Vec<u8>>,
    stamp: Stamp,
}

impl fmt::Debug for LoadedIssuer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LoadedIssuer")
            .field("certificate", &self.certificate)
            .finish_non_exhaustive()
    }
}

impl LoadedIssuer {
    /// Returns the issuer certificate, which delegated tokens carry in `x5c`.
    pub(crate) fn certificate(&self) -> &IssuerCertificate {
        &self.certificate
    }

    /// Returns the issuer's private key, PKCS#8 DER. Keep it out of logs and errors.
    pub(crate) fn pkcs8(&self) -> &[u8] {
        &self.key
    }

    /// How long the certificate has left at `now`; zero once it has expired.
    fn remaining(&self, now: Timestamp) -> Duration {
        Duration::from_millis(
            self.certificate
                .not_after()
                .as_millis()
                .saturating_sub(now.as_millis()),
        )
    }
}

/// Reads and checks the pair in `files`: the certificate passes [`IssuerCertificate::check`] for
/// `fleet` against `authorities`, and the key is the certificate's.
fn read_pair(
    files: &IssuerFiles,
    fleet: &FleetName,
    authorities: &[VerifyingKey],
) -> Result<LoadedIssuer, IssuerFileProblem> {
    // The stamp is taken before the files are read, so a change during the read is noticed at
    // the next check rather than missed.
    let stamp = Stamp::of(files, authorities);
    let read = |path: &Path| {
        fs::read(path).map_err(|source| IssuerFileProblem::Read {
            path: path.to_owned(),
            source,
        })
    };
    let cert_pem = read(&files.cert)?;
    let key_pem = Zeroizing::new(read(&files.key)?);
    let certificates = CertificateDer::pem_slice_iter(&cert_pem)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| IssuerFileProblem::Pem {
            path: files.cert.clone(),
            expected: "a PEM certificate",
        })?;
    let [der] = certificates.as_slice() else {
        return Err(IssuerFileProblem::Pem {
            path: files.cert.clone(),
            expected: "exactly one PEM certificate",
        });
    };
    let not_a_key = || IssuerFileProblem::Pem {
        path: files.key.clone(),
        expected: "a PEM private key",
    };
    let PrivateKeyDer::Pkcs8(pkcs8) =
        PrivateKeyDer::from_pem_slice(&key_pem).map_err(|_| not_a_key())?
    else {
        return Err(IssuerFileProblem::Key {
            path: files.key.clone(),
        });
    };
    let key = Zeroizing::new(pkcs8.secret_pkcs8_der().to_vec());
    let pair =
        Ed25519KeyPair::from_pkcs8_maybe_unchecked(&key).map_err(|_| IssuerFileProblem::Key {
            path: files.key.clone(),
        })?;
    let certificate = IssuerCertificate::check(der.as_ref(), fleet, authorities)?;
    let public = VerifyingKey::from_raw(pair.public_key().as_ref()).ok_or_else(|| {
        IssuerFileProblem::Key {
            path: files.key.clone(),
        }
    })?;
    if &public != certificate.verifying_key() {
        return Err(IssuerFileProblem::Mismatch);
    }
    Ok(LoadedIssuer {
        certificate,
        key,
        stamp,
    })
}

/// The fleet and trusted authority keys `identity` holds as of its last reload.
fn trust(identity: &Identity) -> (FleetName, Vec<VerifyingKey>) {
    (identity.fleet(), identity.token_verifier().keys().to_vec())
}

/// One step of writing a new dev pair, for a test to watch the order.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Step<'a> {
    /// A temporary file was written and synced.
    Wrote(&'a Path),
    /// A temporary file was renamed into place.
    Renamed {
        /// The temporary name.
        from: &'a Path,
        /// The name it replaced.
        to: &'a Path,
    },
}

/// The temporary name beside `path` a new file is written to before it is renamed over `path`.
fn temporary(path: &Path) -> PathBuf {
    let mut name = path.file_name().unwrap_or_default().to_owned();
    name.push(TEMPORARY_SUFFIX);
    path.with_file_name(name)
}

/// Whether a file holds a secret, which sets its mode on Unix.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    Public,
    Secret,
}

/// Writes `contents` to `path`'s temporary name, replacing whatever an interrupted write left
/// there, and syncs it. The key is created `0600` on Unix.
fn write_temporary(path: &Path, contents: &[u8], mode: Mode) -> Result<PathBuf, IssuerFileProblem> {
    let staged = temporary(path);
    let failed = |action, source| IssuerFileProblem::Write {
        action,
        path: staged.clone(),
        source,
    };
    // A file left by an interrupted write may have another mode; a new one gets this one.
    match fs::remove_file(&staged) {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(source) => return Err(failed("remove", source)),
    }
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
    let mut file = options
        .open(&staged)
        .map_err(|source| failed("create", source))?;
    file.write_all(contents)
        .and_then(|()| file.sync_all())
        .map_err(|source| failed("write", source))?;
    Ok(staged)
}

/// Issues a new dev pair named `name` from the authority in `authority_dir`, valid from `now` for
/// [`DEV_ISSUER_LIFETIME`], and writes it over `files`: both to temporary names, synced, then the
/// key renamed into place, then the certificate (ADR-0047, point 1.7). `step` sees each step.
fn replace_from(
    files: &IssuerFiles,
    authority: &Authority,
    name: &NodeId,
    now: Timestamp,
    step: &mut dyn FnMut(Step<'_>),
) -> Result<(), IssuerFileProblem> {
    let issued = authority
        .issue_issuer(name, Validity::new(now, DEV_ISSUER_LIFETIME))
        .map_err(IssuerFileProblem::Authority)?;
    write_pair(files, &issued, step)
}

/// Writes the issuer pair `issued` over `files`, whether or not they exist: both to temporary
/// names beside them, synced, then the key renamed into place, then the certificate, so a
/// control node reloading between the two renames finds a mismatch and keeps the pair it had
/// (ADR-0047, points 1.1 and 1.7). The key is `0600` on Unix. `fleetring cert issuer` renews a
/// pair this way, and a dev node replaces its own.
///
/// # Errors
///
/// [`IssuerFileProblem::Write`], naming the file, if a temporary file can't be written or
/// renamed. A failure before the first rename leaves the pair on disk as it was.
pub fn write_issuer_pair(files: &IssuerFiles, issued: &Issued) -> Result<(), IssuerFileProblem> {
    write_pair(files, issued, &mut |_| {})
}

fn write_pair(
    files: &IssuerFiles,
    issued: &Issued,
    step: &mut dyn FnMut(Step<'_>),
) -> Result<(), IssuerFileProblem> {
    let key_pem = Zeroizing::new(issued.key_pem().as_bytes().to_vec());
    let staged_key = write_temporary(&files.key, &key_pem, Mode::Secret)?;
    step(Step::Wrote(&staged_key));
    let staged_cert = write_temporary(
        &files.cert,
        issued.certificate_pem().as_bytes(),
        Mode::Public,
    )?;
    step(Step::Wrote(&staged_cert));
    for (from, to) in [(&staged_key, &files.key), (&staged_cert, &files.cert)] {
        fs::rename(from, to).map_err(|source| IssuerFileProblem::Write {
            action: "rename",
            path: from.clone(),
            source,
        })?;
        step(Step::Renamed { from, to });
    }
    sync_dir(&files.cert);
    Ok(())
}

/// Syncs the directory that holds `path`, so the renames survive a crash; best effort, since not
/// every platform can.
fn sync_dir(path: &Path) {
    #[cfg(unix)]
    if let Some(dir) = path.parent() {
        let dir = if dir.as_os_str().is_empty() {
            Path::new(".")
        } else {
            dir
        };
        if let Err(error) = File::open(dir).and_then(|dir| dir.sync_all()) {
            tracing::debug!(%error, dir = %dir.display(), "can't sync the issuer pair's directory");
        }
    }
    #[cfg(not(unix))]
    let _ = path;
}

/// Whether users other than its owner may read the file at `path`. Always `false` off Unix.
fn readable_by_others(path: &Path) -> bool {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        fs::metadata(path).is_ok_and(|metadata| metadata.permissions().mode() & 0o077 != 0)
    }
    #[cfg(not(unix))]
    {
        let _ = path;
        false
    }
}

/// Warns when the issuer key file is readable by other users than its owner: a copy or a
/// `chmod` may have loosened it, and the key signs delegated tokens.
fn warn_if_loose(key: &Path) {
    if readable_by_others(key) {
        tracing::warn!(
            key = %key.display(),
            "the delegation issuer key is readable by other users; run `chmod 600 {}`",
            key.display(),
        );
    }
}

/// What a dev node found of its pair.
#[derive(Debug)]
enum Inspection {
    /// The pair loads and has more than [`DEV_RENEW_BEFORE`] left.
    Keep(Box<LoadedIssuer>),
    /// The pair must be replaced, for this reason.
    Replace(&'static str),
}

fn inspect(
    files: &IssuerFiles,
    fleet: &FleetName,
    authorities: &[VerifyingKey],
    now: Timestamp,
) -> Inspection {
    match read_pair(files, fleet, authorities) {
        Ok(loaded) if loaded.remaining(now).is_zero() => Inspection::Replace("it has expired"),
        Ok(loaded) if loaded.remaining(now) < DEV_RENEW_BEFORE => {
            Inspection::Replace("fewer than 30 days remain")
        }
        Ok(loaded) => Inspection::Keep(Box::new(loaded)),
        Err(IssuerFileProblem::Mismatch) => Inspection::Replace("the key doesn't match"),
        Err(
            IssuerFileProblem::Read { .. }
            | IssuerFileProblem::Pem { .. }
            | IssuerFileProblem::Key { .. }
            | IssuerFileProblem::Certificate(_)
            | IssuerFileProblem::NoIdentity
            | IssuerFileProblem::Authority(_)
            | IssuerFileProblem::Write { .. },
        ) => Inspection::Replace("it is missing or can't be used"),
    }
}

/// Whether a dev pair was kept or written.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum DevIssuerOutcome {
    /// The pair was usable, with more than [`DEV_RENEW_BEFORE`] left.
    Kept,
    /// A new pair was written.
    Written,
}

/// Makes `files` a usable dev pair at `now`: one issued by `authority` for `name`, with the key
/// matching, and more than [`DEV_RENEW_BEFORE`] left. Anything else is replaced, through
/// temporary names (ADR-0047, point 1.7). `fleetring dev` runs it at every start.
///
/// # Errors
///
/// [`IssuerFilesError`] if a new pair can't be issued or written, or doesn't load once written.
pub fn ensure_dev_pair(
    files: &IssuerFiles,
    authority: &Authority,
    name: &NodeId,
    now: Timestamp,
) -> Result<DevIssuerOutcome, IssuerFilesError> {
    let error = |problem| IssuerFilesError::new(files, problem);
    let signer = authority.token_signer().map_err(|_| {
        error(IssuerFileProblem::Key {
            path: PathBuf::from(crate::tls::AUTHORITY_KEY_FILE),
        })
    })?;
    let authorities = [signer.verifying_key()];
    match inspect(files, authority.fleet(), &authorities, now) {
        Inspection::Keep(_) => Ok(DevIssuerOutcome::Kept),
        Inspection::Replace(_) => {
            replace_from(files, authority, name, now, &mut |_| {}).map_err(error)?;
            read_pair(files, authority.fleet(), &authorities).map_err(error)?;
            Ok(DevIssuerOutcome::Written)
        }
    }
}

/// How a dev node replaces its pair: from the dev authority in `authority_dir`, for `name`.
#[derive(Debug, Clone)]
struct DevIssuer {
    authority_dir: PathBuf,
    name: NodeId,
}

impl DevIssuer {
    fn replace(
        &self,
        files: &IssuerFiles,
        now: Timestamp,
        step: &mut dyn FnMut(Step<'_>),
    ) -> Result<(), IssuerFileProblem> {
        let authority =
            Authority::load(&self.authority_dir).map_err(IssuerFileProblem::Authority)?;
        replace_from(files, &authority, &self.name, now, step)
    }
}

/// What the expiry check tells the operator.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExpiryWarning {
    /// The certificate expires in this long.
    Soon(Duration),
    /// The certificate has expired.
    Expired,
}

/// Whether to warn at `now` about a certificate that ends at `not_after`, having last warned at
/// `last`: from [`EXPIRY_WARNING_WINDOW`] before the end, at most once per
/// [`EXPIRY_WARNING_EVERY`].
fn expiry_warning(
    not_after: Timestamp,
    now: Timestamp,
    last: Option<Timestamp>,
) -> Option<ExpiryWarning> {
    let window = millis(EXPIRY_WARNING_WINDOW);
    if now.as_millis().saturating_add(window) < not_after.as_millis() {
        return None;
    }
    if last.is_some_and(|last| {
        now.as_millis().saturating_sub(last.as_millis()) < millis(EXPIRY_WARNING_EVERY)
    }) {
        return None;
    }
    if now > not_after {
        Some(ExpiryWarning::Expired)
    } else {
        Some(ExpiryWarning::Soon(Duration::from_millis(
            not_after.as_millis() - now.as_millis(),
        )))
    }
}

fn millis(span: Duration) -> u64 {
    u64::try_from(span.as_millis()).unwrap_or(u64::MAX)
}

/// Whether the last reload found a pair it couldn't use.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
enum Refused {
    #[default]
    No,
    Once,
}

/// What checks remember between them.
#[derive(Debug, Default)]
struct CheckState {
    checked: Option<tokio::time::Instant>,
    refused: Refused,
    warned: Option<Timestamp>,
}

/// The control node's delegation issuer pair (ADR-0047, point 1.3), loaded from
/// `[control] issuer_cert` and `issuer_key`, and reloaded when they change.
///
/// The files are checked for a change at most once a second, when the pair is asked for and
/// hourly besides. A reload whose certificate fails [`IssuerCertificate::check`], or whose key
/// isn't the certificate's, keeps the pair in use and logs why. From [`EXPIRY_WARNING_WINDOW`]
/// before the certificate expires, the node warns once a day.
///
/// A pair loaded with [`load_dev`](Self::load_dev) is also replaced from the dev authority when
/// fewer than [`DEV_RENEW_BEFORE`] remain, when it has expired, or, if a reload finds it still
/// unusable a check later, when it can't be used (ADR-0047, point 1.7).
///
/// Cloning is cheap; clones share the pair. `Debug` shows the files and the certificate, never
/// the key.
#[derive(Clone)]
pub struct IssuerPair {
    inner: Arc<Inner>,
}

struct Inner {
    files: IssuerFiles,
    identity: Identity,
    clock: Arc<dyn Clock>,
    dev: Option<DevIssuer>,
    loaded: ArcSwap<LoadedIssuer>,
    state: Mutex<CheckState>,
}

impl fmt::Debug for IssuerPair {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IssuerPair")
            .field("files", &self.inner.files)
            .field("certificate", &self.inner.loaded.load().certificate)
            .field("dev", &self.inner.dev.is_some())
            .finish_non_exhaustive()
    }
}

impl IssuerPair {
    /// Loads the pair in `files`, checked against the trusted authorities and fleet of
    /// `identity`, the node's own (ADR-0047, point 1.3). `clock` says when the certificate
    /// expires relative to now, for the warning.
    ///
    /// # Errors
    ///
    /// [`IssuerFilesError`], naming both files, if a file can't be read or isn't PEM, the key
    /// isn't Ed25519, the certificate fails [`IssuerCertificate::check`], or the key doesn't
    /// match it. An expired certificate isn't an error.
    pub fn load(
        files: &IssuerFiles,
        identity: &Identity,
        clock: Arc<dyn Clock>,
    ) -> Result<Self, IssuerFilesError> {
        let (fleet, authorities) = trust(identity);
        let loaded = read_pair(files, &fleet, &authorities)
            .map_err(|problem| IssuerFilesError::new(files, problem))?;
        Ok(Self::new(files, identity, clock, None, loaded))
    }

    /// Loads the pair in `files` as [`load`](Self::load) does, on a dev node: first replacing it,
    /// from the dev authority in `authority_dir` and for `name`, when fewer than
    /// [`DEV_RENEW_BEFORE`] remain, when it has expired, or when it is missing or can't be used
    /// (ADR-0047, point 1.7). The running node replaces it the same way at its checks.
    ///
    /// # Errors
    ///
    /// [`IssuerFilesError`], naming both files, if the dev authority can't be loaded or used, or
    /// a new pair can't be written or doesn't load.
    pub fn load_dev(
        files: &IssuerFiles,
        identity: &Identity,
        clock: Arc<dyn Clock>,
        authority_dir: &Path,
        name: &NodeId,
    ) -> Result<Self, IssuerFilesError> {
        let error = |problem| IssuerFilesError::new(files, problem);
        let dev = DevIssuer {
            authority_dir: authority_dir.to_owned(),
            name: name.clone(),
        };
        let (fleet, authorities) = trust(identity);
        let now = clock.now();
        let loaded = match inspect(files, &fleet, &authorities, now) {
            Inspection::Keep(loaded) => *loaded,
            Inspection::Replace(why) => {
                tracing::info!(
                    cert = %files.cert.display(),
                    why,
                    "replacing the dev delegation issuer pair",
                );
                dev.replace(files, now, &mut |_| {}).map_err(error)?;
                read_pair(files, &fleet, &authorities).map_err(error)?
            }
        };
        Ok(Self::new(files, identity, clock, Some(dev), loaded))
    }

    fn new(
        files: &IssuerFiles,
        identity: &Identity,
        clock: Arc<dyn Clock>,
        dev: Option<DevIssuer>,
        loaded: LoadedIssuer,
    ) -> Self {
        let pair = Self {
            inner: Arc::new(Inner {
                files: files.clone(),
                identity: identity.clone(),
                clock,
                dev,
                loaded: ArcSwap::from_pointee(loaded),
                state: Mutex::new(CheckState::default()),
            }),
        };
        let mut state = pair.lock();
        pair.warn_if_expiring(&mut state, pair.inner.clock.now());
        drop(state);
        warn_if_loose(&files.key);
        pair
    }

    /// Returns the files the pair is loaded from.
    #[must_use]
    pub fn files(&self) -> &IssuerFiles {
        &self.inner.files
    }

    /// Returns the issuer certificate in use, after checking the files for a change if a check
    /// is due.
    ///
    /// # Panics
    ///
    /// If the pair's lock was poisoned by a panic in another thread.
    #[must_use]
    pub fn certificate(&self) -> IssuerCertificate {
        self.current().certificate().clone()
    }

    /// A signer for delegated tokens from the pair in use, after checking the files for a change
    /// if a check is due. It reads no clock: whether the certificate is valid when it signs is
    /// the caller's to check (ADR-0047, point 1.6).
    ///
    /// The certificate is checked again against the node's trusted authorities as they are now,
    /// so a pair whose certifying authority left the `[tls] ca` bundle signs nothing every node
    /// would refuse.
    ///
    /// # Errors
    ///
    /// [`IssuerError::Certificate`] if the node's trusted authorities no longer certify the pair
    /// in use; [`IssuerError`]'s other variants if the loaded key can't be used or isn't the
    /// certificate's, which the checks at load already refuse.
    ///
    /// # Panics
    ///
    /// If the pair's lock was poisoned by a panic in another thread.
    pub fn signer(&self) -> Result<DelegationSigner, IssuerError> {
        let loaded = self.current();
        let (fleet, authorities) = trust(&self.inner.identity);
        let certificate =
            IssuerCertificate::check(loaded.certificate().der(), &fleet, &authorities)?;
        DelegationSigner::from_checked(certificate, loaded.pkcs8(), fleet)
    }

    /// The pair in use, after checking the files for a change if a check is due.
    ///
    /// # Panics
    ///
    /// If the pair's lock was poisoned by a panic in another thread.
    pub(crate) fn current(&self) -> Arc<LoadedIssuer> {
        let mut state = self.lock();
        let now = tokio::time::Instant::now();
        let due = state
            .checked
            .is_none_or(|at| now.duration_since(at) >= RELOAD_CHECK_INTERVAL);
        if due {
            state.checked = Some(now);
            self.check(&mut state, &mut |_| {});
        }
        drop(state);
        self.inner.loaded.load_full()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, CheckState> {
        self.inner.state.lock().expect("issuer pair lock poisoned")
    }

    /// One check: reloads the files if they or the node's trusted authorities changed, keeping
    /// the pair in use if they can't be used; on a dev node, replaces the pair if it can't be used a second check running, or
    /// if fewer than [`DEV_RENEW_BEFORE`] remain; then warns if the certificate expires soon.
    /// `step` sees a replacement's steps.
    fn check(&self, state: &mut CheckState, step: &mut dyn FnMut(Step<'_>)) {
        let files = &self.inner.files;
        let now = self.inner.clock.now();
        let (_, authorities) = trust(&self.inner.identity);
        if Stamp::of(files, &authorities) != self.inner.loaded.load().stamp {
            match self.reload(state) {
                Ok(()) => state.refused = Refused::No,
                Err(problem) => {
                    tracing::warn!(
                        cert = %files.cert.display(),
                        key = %files.key.display(),
                        error = %problem,
                        "the delegation issuer pair, or the trusted authorities it is checked \
                         against, changed, and the pair can't be used; keeping the one in use",
                    );
                    // A writer between its two renames leaves a mismatch for a moment, so a dev
                    // node replaces the pair only when the next check still finds it unusable.
                    if let Some(dev) = &self.inner.dev
                        && state.refused == Refused::Once
                    {
                        state.refused = Refused::No;
                        self.replace(state, dev, now, "it can't be used", step);
                    } else {
                        state.refused = Refused::Once;
                    }
                }
            }
        }
        if let Some(dev) = &self.inner.dev
            && self.inner.loaded.load().remaining(now) < DEV_RENEW_BEFORE
        {
            self.replace(state, dev, now, "fewer than 30 days remain", step);
        }
        self.warn_if_expiring(state, now);
    }

    /// Reloads the files into the pair in use, if they can be used.
    fn reload(&self, state: &mut CheckState) -> Result<(), IssuerFileProblem> {
        let (fleet, authorities) = trust(&self.inner.identity);
        let loaded = read_pair(&self.inner.files, &fleet, &authorities)?;
        if loaded.stamp.files != self.inner.loaded.load().stamp.files {
            warn_if_loose(&self.inner.files.key);
        }
        tracing::info!(
            cert = %self.inner.files.cert.display(),
            key_id = %loaded.certificate.key_id(),
            "reloaded the delegation issuer pair",
        );
        if loaded.certificate != self.inner.loaded.load().certificate {
            // A new certificate gets its own warnings.
            state.warned = None;
        }
        self.inner.loaded.store(Arc::new(loaded));
        Ok(())
    }

    /// Replaces a dev pair and loads it; a failure keeps the pair in use and logs why.
    fn replace(
        &self,
        state: &mut CheckState,
        dev: &DevIssuer,
        now: Timestamp,
        why: &str,
        step: &mut dyn FnMut(Step<'_>),
    ) {
        let files = &self.inner.files;
        tracing::info!(cert = %files.cert.display(), why, "replacing the dev delegation issuer pair");
        let replaced = dev
            .replace(files, now, step)
            .and_then(|()| self.reload(state));
        if let Err(error) = replaced {
            tracing::warn!(
                cert = %files.cert.display(),
                %error,
                "can't replace the dev delegation issuer pair; keeping the one in use",
            );
        }
    }

    /// Logs the expiry warning if one is due.
    fn warn_if_expiring(&self, state: &mut CheckState, now: Timestamp) {
        let certificate = &self.inner.loaded.load().certificate;
        let Some(warning) = expiry_warning(certificate.not_after(), now, state.warned) else {
            return;
        };
        state.warned = Some(now);
        let cert = self.inner.files.cert.display();
        match warning {
            ExpiryWarning::Soon(left) => tracing::warn!(
                %cert,
                hours_left = left.as_secs() / 3_600,
                "the delegation issuer certificate expires within 7 days; issue a new pair with \
                 `fleetring cert issuer` and replace [control] issuer_cert and issuer_key",
            ),
            ExpiryWarning::Expired => tracing::warn!(
                %cert,
                "the delegation issuer certificate has expired, so this node delegates no token; \
                 issue a new pair with `fleetring cert issuer` and replace [control] issuer_cert \
                 and issuer_key",
            ),
        }
    }

    /// Checks the pair hourly while the returned watch lives, so the expiry warning and a dev
    /// node's replacement happen whether or not anything asks for the pair.
    pub(crate) fn watch(&self) -> IssuerWatch {
        let pair = self.clone();
        IssuerWatch(tokio::spawn(async move {
            loop {
                tokio::time::sleep(WATCH_INTERVAL).await;
                let _current = pair.current();
            }
        }))
    }
}

/// The task [`IssuerPair::watch`] starts; dropping it stops the task.
#[derive(Debug)]
pub(crate) struct IssuerWatch(JoinHandle<()>);

impl Drop for IssuerWatch {
    fn drop(&mut self) {
        self.0.abort();
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::clock::SimClock;

    const DAY: Duration = Duration::from_secs(SECONDS_PER_DAY);

    /// A time every certificate here is issued around.
    const BASE: Timestamp = Timestamp::from_millis(1_790_000_000_000);

    /// An authority written under a scratch directory, a node identity it issued, and the issuer
    /// files `issuer.c.crt` and `issuer.c.key` beside them.
    struct Fixture {
        _dir: tempfile::TempDir,
        tls: PathBuf,
        authority: Authority,
        identity: Identity,
        files: IssuerFiles,
    }

    impl Fixture {
        fn new() -> Self {
            let dir = tempfile::tempdir().expect("a directory");
            let tls = dir.path().join("tls");
            let century = Validity::new(Timestamp::ZERO, 36_500 * DAY);
            let authority =
                Authority::create("test".parse().expect("a fleet"), century).expect("authority");
            authority.write(&tls).expect("written");
            let node = authority
                .issue_node(
                    &"control".parse().expect("an id"),
                    &["127.0.0.1".to_owned()],
                    century,
                )
                .expect("a node certificate");
            let identity =
                Identity::from_issued(authority.certificate_pem(), &node).expect("an identity");
            let files = IssuerFiles {
                cert: tls.join("issuer.c.crt"),
                key: tls.join("issuer.c.key"),
            };
            Self {
                _dir: dir,
                tls,
                authority,
                identity,
                files,
            }
        }

        /// Writes a new pair from `authority` over the files, each renamed into place whole,
        /// lasting `lifetime` from [`BASE`], and returns its key ID.
        fn write_pair(&self, authority: &Authority, lifetime: Duration) -> KeyId {
            let issued = authority
                .issue_issuer(&name(), Validity::new(BASE, lifetime))
                .expect("an issuer pair");
            replace_whole(&self.files.key, issued.key_pem());
            replace_whole(&self.files.cert, issued.certificate_pem());
            let der =
                CertificateDer::from_pem_slice(issued.certificate_pem().as_bytes()).expect("PEM");
            let trusted = [authority.token_signer().expect("a signer").verifying_key()];
            IssuerCertificate::check(der.as_ref(), authority.fleet(), &trusted)
                .expect("a valid issuer certificate")
                .key_id()
                .clone()
        }

        /// A key that isn't the one on disk.
        fn stray_key(&self) -> String {
            self.authority
                .issue_issuer(&name(), Validity::new(BASE, DAY))
                .expect("another pair")
                .key_pem()
                .to_owned()
        }

        fn load(&self, clock: SimClock) -> IssuerPair {
            IssuerPair::load(&self.files, &self.identity, Arc::new(clock)).expect("loaded")
        }

        fn load_dev(&self, clock: SimClock) -> IssuerPair {
            IssuerPair::load_dev(
                &self.files,
                &self.identity,
                Arc::new(clock),
                &self.tls,
                &name(),
            )
            .expect("loaded")
        }

        fn on_disk(&self) -> Result<LoadedIssuer, IssuerFileProblem> {
            let (fleet, trusted) = trust(&self.identity);
            read_pair(&self.files, &fleet, &trusted)
        }
    }

    fn name() -> NodeId {
        "c".parse().expect("a name")
    }

    fn replace_whole(path: &Path, contents: &str) {
        let staged = path.with_extension("staged");
        fs::write(&staged, contents).expect("staged");
        fs::rename(&staged, path).expect("renamed");
    }

    fn after(base: Timestamp, span: Duration) -> Timestamp {
        Timestamp::from_millis(base.as_millis() + millis(span))
    }

    /// Lets the next call check the files.
    async fn next_check() {
        tokio::time::advance(RELOAD_CHECK_INTERVAL).await;
    }

    #[tokio::test(start_paused = true)]
    async fn issuer_pair_reloads_without_restart() {
        let fixture = Fixture::new();
        let first = fixture.write_pair(&fixture.authority, 30 * DAY);
        let pair = fixture.load(SimClock::new(BASE));
        assert_eq!(pair.certificate().key_id(), &first);

        let second = fixture.write_pair(&fixture.authority, 30 * DAY);
        assert_ne!(second, first);
        // At most one check a second.
        assert_eq!(pair.certificate().key_id(), &first);
        next_check().await;
        assert_eq!(pair.certificate().key_id(), &second);
        assert_eq!(
            pair.clone().certificate().key_id(),
            &second,
            "clones share it"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn the_pair_in_use_builds_the_delegation_signer() {
        let fixture = Fixture::new();
        let first = fixture.write_pair(&fixture.authority, 30 * DAY);
        let pair = fixture.load(SimClock::new(BASE));
        let signer = pair.signer().expect("a signer");
        assert_eq!(signer.key_id(), &first);
        assert_eq!(signer.expires_at(), after(BASE, 30 * DAY));

        let second = fixture.write_pair(&fixture.authority, 20 * DAY);
        next_check().await;
        let signer = pair.signer().expect("a signer");
        assert_eq!(signer.key_id(), &second);
        assert_eq!(signer.expires_at(), after(BASE, 20 * DAY));
    }

    #[tokio::test(start_paused = true)]
    async fn reload_of_a_mismatched_or_invalid_pair_keeps_the_old_pair() {
        let fixture = Fixture::new();
        let kept = fixture.write_pair(&fixture.authority, 30 * DAY);
        let pair = fixture.load(SimClock::new(BASE));
        assert_eq!(pair.certificate().key_id(), &kept);
        let good_key = fs::read_to_string(&fixture.files.key).expect("the key");

        replace_whole(&fixture.files.key, &fixture.stray_key());
        next_check().await;
        assert_eq!(pair.certificate().key_id(), &kept, "a mismatched key");

        replace_whole(&fixture.files.key, &good_key);
        replace_whole(&fixture.files.cert, "not a certificate");
        next_check().await;
        assert_eq!(
            pair.certificate().key_id(),
            &kept,
            "an unreadable certificate"
        );

        let stranger = Authority::create(
            "other".parse().expect("a fleet"),
            Validity::new(Timestamp::ZERO, 1_000 * DAY),
        )
        .expect("another authority");
        let foreign = fixture.write_pair(&stranger, 30 * DAY);
        assert_ne!(foreign, kept);
        next_check().await;
        assert_eq!(pair.certificate().key_id(), &kept, "another fleet's issuer");

        // A node that isn't in dev mode never writes the files.
        let on_disk = fs::read(&fixture.files.cert).expect("the certificate");
        next_check().await;
        assert_eq!(pair.certificate().key_id(), &kept);
        assert_eq!(fs::read(&fixture.files.cert).expect("unchanged"), on_disk);

        let fixed = fixture.write_pair(&fixture.authority, 30 * DAY);
        next_check().await;
        assert_eq!(
            pair.certificate().key_id(),
            &fixed,
            "a usable pair loads again"
        );
    }

    #[test]
    fn a_pair_that_cant_be_used_at_load_is_refused_naming_both_files() {
        let fixture = Fixture::new();
        fixture.write_pair(&fixture.authority, 30 * DAY);
        replace_whole(&fixture.files.key, &fixture.stray_key());
        let error = IssuerPair::load(
            &fixture.files,
            &fixture.identity,
            Arc::new(SimClock::new(BASE)),
        )
        .expect_err("a mismatched key");
        assert!(
            matches!(error.problem(), IssuerFileProblem::Mismatch),
            "{error}"
        );
        let message = format!("{error}: {}", error.problem());
        assert!(message.contains("issuer.c.crt"), "{message}");
        assert!(message.contains("issuer.c.key"), "{message}");
        assert!(!message.contains("PRIVATE KEY"), "{message}");

        // An expired certificate loads; signing refuses it.
        fixture.write_pair(&fixture.authority, DAY);
        let later = after(BASE, 10 * DAY);
        let pair = fixture.load(SimClock::new(later));
        assert!(pair.certificate().valid_at(later).is_err());
    }

    #[test]
    fn issuer_expiry_warns_from_seven_days() {
        let end = after(BASE, 30 * DAY);
        let before_end = |span: Duration| Timestamp::from_millis(end.as_millis() - millis(span));
        assert_eq!(expiry_warning(end, before_end(8 * DAY), None), None);
        assert_eq!(
            expiry_warning(end, before_end(7 * DAY), None),
            Some(ExpiryWarning::Soon(7 * DAY))
        );
        // Once a day.
        let warned = Some(before_end(6 * DAY));
        assert_eq!(
            expiry_warning(end, before_end(5 * DAY + DAY / 2), warned),
            None
        );
        assert_eq!(
            expiry_warning(end, before_end(5 * DAY), warned),
            Some(ExpiryWarning::Soon(5 * DAY))
        );
        assert_eq!(
            expiry_warning(end, after(end, Duration::from_millis(1)), None),
            Some(ExpiryWarning::Expired)
        );

        // A pair loaded with six days left warns at once; one with twenty doesn't.
        let fixture = Fixture::new();
        fixture.write_pair(&fixture.authority, 30 * DAY);
        let soon = fixture.load(SimClock::new(before_end(6 * DAY)));
        assert!(soon.lock().warned.is_some(), "warned at load");
        let fresh = fixture.load(SimClock::new(before_end(20 * DAY)));
        assert!(
            fresh.lock().warned.is_none(),
            "no warning with 20 days left"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn dev_replaces_key_then_certificate_through_temporary_names() {
        let fixture = Fixture::new();
        // Missing at start: written, for 90 days.
        let pair = fixture.load_dev(SimClock::new(BASE));
        let first = pair.certificate();
        assert_eq!(
            first.not_after().as_millis() - first.not_before().as_millis(),
            millis(DEV_ISSUER_LIFETIME)
        );
        // Another reader of the same files, as another process would be.
        let reader = fixture.load(SimClock::new(BASE));
        let staged_key = temporary(&fixture.files.key);
        let staged_cert = temporary(&fixture.files.cert);

        let mut steps = Vec::new();
        let dev = pair.inner.dev.as_ref().expect("a dev pair");
        dev.replace(&fixture.files, BASE, &mut |step| {
            steps.push(match step {
                Step::Wrote(path) => {
                    #[cfg(unix)]
                    if path == staged_key {
                        use std::os::unix::fs::PermissionsExt as _;
                        let mode = fs::metadata(path).expect("staged").permissions().mode();
                        assert_eq!(mode & 0o777, 0o600, "the staged key is private");
                    }
                    format!("wrote {}", path.display())
                }
                Step::Renamed { from, to } => {
                    if to == fixture.files.key {
                        // Between the renames: the key is new, the certificate old.
                        let between = fixture.on_disk();
                        assert!(
                            matches!(between, Err(IssuerFileProblem::Mismatch)),
                            "{between:?}"
                        );
                        reader.check(&mut reader.lock(), &mut |_| {});
                        assert_eq!(
                            reader.inner.loaded.load().certificate.key_id(),
                            first.key_id(),
                            "a reader between the renames keeps the pair it had"
                        );
                    }
                    format!("renamed {} to {}", from.display(), to.display())
                }
            });
        })
        .expect("replaced");
        let renamed =
            |from: &Path, to: &Path| format!("renamed {} to {}", from.display(), to.display());
        assert_eq!(
            steps,
            [
                format!("wrote {}", staged_key.display()),
                format!("wrote {}", staged_cert.display()),
                renamed(&staged_key, &fixture.files.key),
                renamed(&staged_cert, &fixture.files.cert),
            ]
        );
        assert!(!staged_key.exists() && !staged_cert.exists());
        reader.check(&mut reader.lock(), &mut |_| {});
        assert_ne!(
            reader.inner.loaded.load().certificate.key_id(),
            first.key_id(),
            "after both renames the reader takes the new pair"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_running_dev_pair_is_replaced_under_30_days_and_after_a_second_unusable_check() {
        let fixture = Fixture::new();
        let pair = fixture.load_dev(SimClock::new(BASE));
        let first = pair.certificate();

        // 61 days on, fewer than 30 remain: the next check replaces it.
        tokio::time::advance(61 * DAY).await;
        let renewed = pair.certificate();
        assert_ne!(renewed.key_id(), first.key_id());

        // A mismatched key is kept back once, for a writer between its renames, then replaced.
        replace_whole(&fixture.files.key, &fixture.stray_key());
        next_check().await;
        assert_eq!(pair.certificate().key_id(), renewed.key_id(), "kept once");
        next_check().await;
        let replaced = pair.certificate();
        assert_ne!(
            replaced.key_id(),
            renewed.key_id(),
            "replaced a check later"
        );
        let on_disk = fixture.on_disk().expect("a usable pair on disk");
        assert_eq!(on_disk.certificate.key_id(), replaced.key_id());
    }

    /// The pair is checked again when the node's `[tls] ca` bundle changes, not only its own
    /// files: a pair whose certifying authority left the bundle signs nothing, and signs again
    /// once the authority returns.
    #[tokio::test(start_paused = true)]
    async fn a_pair_whose_authority_leaves_the_bundle_signs_nothing() {
        let fixture = Fixture::new();
        let other = Authority::create(
            "test".parse().expect("a fleet"),
            Validity::new(Timestamp::ZERO, 36_500 * DAY),
        )
        .expect("a second authority");
        let node = fixture
            .authority
            .issue_node(
                &"control".parse().expect("an id"),
                &["127.0.0.1".to_owned()],
                Validity::new(Timestamp::ZERO, 36_500 * DAY),
            )
            .expect("a node certificate");
        let (bundle, cert, key) = (
            fixture.tls.join("bundle.crt"),
            fixture.tls.join("node.crt"),
            fixture.tls.join("node.key"),
        );
        let both = format!(
            "{}{}",
            fixture.authority.certificate_pem(),
            other.certificate_pem()
        );
        fs::write(&bundle, &both).expect("the bundle");
        fs::write(&cert, node.certificate_pem()).expect("the certificate");
        fs::write(&key, node.key_pem()).expect("the key");
        let identity = Identity::load(&bundle, &cert, &key).expect("an identity");
        let kept = fixture.write_pair(&other, 30 * DAY);
        let pair = IssuerPair::load(&fixture.files, &identity, Arc::new(SimClock::new(BASE)))
            .expect("loaded");
        assert_eq!(pair.signer().expect("a signer").key_id(), &kept);

        // The certifying authority leaves the bundle; the pair's files don't change.
        replace_whole(&bundle, fixture.authority.certificate_pem());
        next_check().await;
        assert_eq!(identity.token_verifier().keys().len(), 1, "reloaded");
        assert_eq!(
            pair.signer().map(|_| ()),
            Err(IssuerError::Certificate(IssuerCertError::UnknownAuthority))
        );
        assert_eq!(
            pair.certificate().key_id(),
            &kept,
            "kept, as a reload keeps it"
        );

        replace_whole(&bundle, &both);
        next_check().await;
        assert_eq!(pair.signer().expect("a signer again").key_id(), &kept);
    }

    #[test]
    fn the_stamp_changes_with_the_trusted_authorities() {
        let fixture = Fixture::new();
        fixture.write_pair(&fixture.authority, 30 * DAY);
        let (_, trusted) = trust(&fixture.identity);
        let other = Authority::create(
            "test".parse().expect("a fleet"),
            Validity::new(Timestamp::ZERO, 36_500 * DAY),
        )
        .expect("a second authority")
        .token_signer()
        .expect("a signer")
        .verifying_key();
        let more: Vec<VerifyingKey> = trusted.iter().cloned().chain([other]).collect();
        assert_eq!(
            Stamp::of(&fixture.files, &trusted),
            Stamp::of(&fixture.files, &trusted)
        );
        assert_ne!(
            Stamp::of(&fixture.files, &trusted),
            Stamp::of(&fixture.files, &more)
        );
    }

    #[cfg(unix)]
    #[test]
    fn a_key_readable_by_others_is_noticed() {
        use std::os::unix::fs::PermissionsExt as _;

        let fixture = Fixture::new();
        fixture.write_pair(&fixture.authority, 30 * DAY);
        let set = |mode| {
            fs::set_permissions(&fixture.files.key, fs::Permissions::from_mode(mode))
                .expect("chmod");
        };
        set(0o600);
        assert!(!readable_by_others(&fixture.files.key));
        for loose in [0o640, 0o604, 0o644] {
            set(loose);
            assert!(readable_by_others(&fixture.files.key), "{loose:o}");
        }
    }

    /// `fleetring cert issuer` renews a pair in place, through temporary names, key first.
    #[test]
    fn write_issuer_pair_replaces_an_existing_pair_key_first() {
        let fixture = Fixture::new();
        let first = fixture.write_pair(&fixture.authority, 30 * DAY);
        let issued = fixture
            .authority
            .issue_issuer(&name(), Validity::new(BASE, 30 * DAY))
            .expect("a new pair");
        let mut steps = Vec::new();
        write_pair(&fixture.files, &issued, &mut |step| {
            steps.push(match step {
                Step::Wrote(path) => format!("wrote {}", path.display()),
                Step::Renamed { to, .. } => format!("renamed {}", to.display()),
            });
        })
        .expect("written");
        let renamed: Vec<&String> = steps
            .iter()
            .filter(|step| step.starts_with("renamed"))
            .collect();
        assert_eq!(
            renamed,
            [
                &format!("renamed {}", fixture.files.key.display()),
                &format!("renamed {}", fixture.files.cert.display()),
            ]
        );
        let on_disk = fixture.on_disk().expect("a usable pair");
        assert_ne!(on_disk.certificate.key_id(), &first, "a new key");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            let mode = fs::metadata(&fixture.files.key)
                .expect("the key")
                .permissions()
                .mode();
            assert_eq!(mode & 0o777, 0o600);
        }
        for file in [&fixture.files.cert, &fixture.files.key] {
            assert!(!temporary(file).exists(), "{}", file.display());
        }
    }
}
