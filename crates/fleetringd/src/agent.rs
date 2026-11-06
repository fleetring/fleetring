//! Agent identities (ADR-0043, point 8): an agent's ID, the directory that holds its
//! credentials, and minting them from an authority.
//!
//! An identity is a directory, `<agents_dir>/<id>/`, created `0700`, holding `ca.crt`, the
//! certificate and key for the principal `agent-<id>`, a scoped token, and `identity.toml`, which
//! holds no secret. Every file is written `0600` under a temporary name and renamed into place,
//! with `identity.toml` last, so a directory with a record always holds the files it describes.
//! Every write holds `<dir>/.lock`.
//!
//! A mint from dev mode's authority needs a [`DevChecked`], which only
//! [`dev::check_dev_dir`] makes, so the dev-only rule can't be skipped (point 8.1).

use std::ffi::OsString;
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write as _};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::Duration;

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use fleetring_client::context::{Context, ContextName};
use fleetring_client::dirs::{self, DirsError};
use fleetring_control::{Millis, Timestamp, TokenId};
use fleetringd::auth::{Grant, IssueError, MAX_TOKEN_LIFETIME, ParseScopeError, Scope};
use fleetringd::dev::{self, DevChecked, DevError};
use fleetringd::mcp::Access;
use fleetringd::tls::{
    AGENT_PREFIX, AUTHORITY_CERT_FILE, AUTHORITY_KEY_FILE, Authority, AuthorityError, Principal,
    Validity,
};
use ring::rand::{SecureRandom as _, SystemRandom};
use serde::{Deserialize, Serialize};

/// The longest agent ID (ADR-0043, point 8.5).
const MAX_ID_LEN: usize = 32;

/// The record of an identity, in its directory.
pub(crate) const RECORD_FILE: &str = "identity.toml";

/// The token, in an identity's directory.
pub(crate) const TOKEN_FILE: &str = "token";

/// The lock every write to an identity's directory holds.
const LOCK_FILE: &str = ".lock";

/// The `identity.toml` format this release writes and reads (ADR-0044, point 1.7).
const RECORD_VERSION: u32 = 1;

/// How many random bytes a token's ID holds.
const TOKEN_ID_BYTES: usize = 16;

const SECONDS_PER_DAY: u64 = 24 * 60 * 60;

/// How long a dev agent's certificate lasts: a year, like dev mode's others (point 8.1).
pub(crate) const DEV_CERT_LIFETIME: Duration = Duration::from_secs(365 * SECONDS_PER_DAY);

/// How long an agent's certificate lasts with `--authority` unless `--ttl` says otherwise.
pub(crate) const AUTHORITY_CERT_LIFETIME: Duration = Duration::from_secs(30 * SECONDS_PER_DAY);

/// A start renews a dev agent's certificate once fewer than this remain (point 8.2).
const RENEW_CERT_WITHIN: Duration = Duration::from_secs(30 * SECONDS_PER_DAY);

/// A start renews a dev agent's token once fewer than this remain, so a session has at least a
/// week (point 8.2).
pub(crate) const RENEW_TOKEN_WITHIN: Duration = Duration::from_secs(7 * SECONDS_PER_DAY);

/// An agent's ID: 1 to 32 of `[a-z0-9_]`. Its principal is `agent-<id>` (ADR-0043, point 8.5).
///
/// Neither `-` nor `.` is allowed: the `-` after the ID in `agent-<id>-*` then always ends it,
/// and `.` is reserved for delegated agents' lineage (ADR-0044, point 1.4).
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct AgentId(String);

impl AgentId {
    /// The ID as text.
    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }

    /// The principal `agent-<id>`.
    pub(crate) fn principal(&self) -> Principal {
        format!("{AGENT_PREFIX}{}", self.0)
            .parse()
            .expect("an agent ID makes a valid principal name")
    }

    /// The scope an identity gets unless `--scope` says otherwise: `admin:agent-<id>-*`, or
    /// `read:agent-<id>-*` for a reading agent.
    pub(crate) fn default_scope(&self, access: Access) -> Scope {
        let action = match access {
            Access::ReadOnly => "read",
            _ => "admin",
        };
        format!("{action}:{AGENT_PREFIX}{}-*", self.0)
            .parse()
            .expect("an agent ID makes a valid scope pattern")
    }
}

impl FromStr for AgentId {
    type Err = AgentError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        let valid = !text.is_empty()
            && text.len() <= MAX_ID_LEN
            && text
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_');
        if valid {
            Ok(Self(text.to_owned()))
        } else {
            Err(AgentError::InvalidId {
                id: text.to_owned(),
            })
        }
    }
}

impl fmt::Display for AgentId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// `identity.toml`: what an identity is, with no secret (ADR-0043, point 8.3).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct AgentRecord {
    /// The format version, [`RECORD_VERSION`].
    pub(crate) version: u32,
    /// `agent-<id>`.
    pub(crate) principal: String,
    /// The principal that created the identity, or `authority` when only the authority's key
    /// was used.
    pub(crate) owner: String,
    /// The agent that created this one; always absent in 0.7.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) parent: Option<String>,
    /// The fleet the authority issues for.
    pub(crate) fleet: String,
    /// The token's scopes.
    pub(crate) scopes: Vec<String>,
    /// Whether `mcp --agent` serves only the reading tools.
    pub(crate) read_only: bool,
    /// The token's ID, by which it is revoked.
    pub(crate) token_id: String,
    /// When the token expires, in RFC 3339.
    pub(crate) token_expires_at: String,
    /// When the certificate expires, in RFC 3339.
    pub(crate) cert_expires_at: String,
    /// What issued the credentials.
    pub(crate) issued_by: IssuedBy,
}

impl AgentRecord {
    /// The access `mcp --agent` serves with, unless `--read-only` narrows it.
    pub(crate) fn access(&self) -> Access {
        if self.read_only {
            Access::ReadOnly
        } else {
            Access::ReadWrite
        }
    }

    /// When the token expires, or `None` if the record's time can't be read.
    pub(crate) fn token_expiry(&self) -> Option<Timestamp> {
        crate::parse_time(&self.token_expires_at)
            .ok()
            .map(Timestamp::from_millis)
    }

    fn cert_expiry(&self) -> Option<Timestamp> {
        crate::parse_time(&self.cert_expires_at)
            .ok()
            .map(Timestamp::from_millis)
    }
}

/// What issued an identity's credentials.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum IssuedBy {
    /// Dev mode's authority, after [`dev::check_dev_dir`].
    Dev,
    /// An authority directory `--authority` named.
    Authority,
}

impl fmt::Display for IssuedBy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Dev => "dev",
            Self::Authority => "authority",
        })
    }
}

/// The authority a mint signs with, and so what the record says issued it.
#[derive(Debug, Clone, Copy)]
pub(crate) enum Issuer<'a> {
    /// Dev mode's authority, in a directory that passed [`dev::check_dev_dir`].
    Dev(&'a DevChecked),
    /// An authority loaded from the directory `--authority` named.
    Authority(&'a Authority),
}

impl Issuer<'_> {
    fn authority(&self) -> &Authority {
        match self {
            Self::Dev(checked) => checked.authority(),
            Self::Authority(authority) => authority,
        }
    }

    fn issued_by(&self) -> IssuedBy {
        match self {
            Self::Dev(_) => IssuedBy::Dev,
            Self::Authority(_) => IssuedBy::Authority,
        }
    }
}

/// What a new identity gets.
#[derive(Debug, Clone)]
pub(crate) struct MintGrant {
    /// The token's scopes.
    pub(crate) scopes: Vec<Scope>,
    /// Whether `mcp --agent` serves every tool or only the reading ones.
    pub(crate) access: Access,
    /// How long the certificate lasts.
    pub(crate) cert_ttl: Duration,
    /// How long the token lasts, at most [`MAX_TOKEN_LIFETIME`].
    pub(crate) token_ttl: Duration,
    /// The principal creating the identity, or `authority`.
    pub(crate) owner: String,
}

/// What a start needs to renew (ADR-0043, point 8.2).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Renewal {
    None,
    Certificate,
    Token,
    Both,
}

impl Renewal {
    fn certificate(self) -> bool {
        matches!(self, Self::Certificate | Self::Both)
    }

    fn token(self) -> bool {
        matches!(self, Self::Token | Self::Both)
    }
}

/// What [`AgentDir::acquire`] found or did.
#[derive(Debug, Clone)]
pub(crate) enum Acquired {
    /// There was no identity; it minted one.
    Minted(AgentRecord),
    /// The identity was used as it was.
    Reused(AgentRecord),
    /// It renewed part of the identity; `old` is the record before.
    Renewed {
        record: AgentRecord,
        old: Box<AgentRecord>,
        renewal: Renewal,
    },
}

/// What [`AgentDir::write_credentials`] wrote, in the record's words: `None` for what it left.
#[derive(Debug, Clone, Default)]
struct Written {
    cert_expires_at: Option<String>,
    token_id: Option<String>,
    token_expires_at: Option<String>,
}

impl Acquired {
    /// The identity's record now.
    pub(crate) fn record(&self) -> &AgentRecord {
        match self {
            Self::Minted(record) | Self::Reused(record) | Self::Renewed { record, .. } => record,
        }
    }
}

/// One agent's directory (ADR-0043, point 8.3).
#[derive(Debug, Clone)]
pub(crate) struct AgentDir {
    dir: PathBuf,
    id: AgentId,
}

impl AgentDir {
    /// The directory for `id` under `agents_dir`.
    pub(crate) fn new(agents_dir: &Path, id: AgentId) -> Self {
        Self {
            dir: agents_dir.join(id.as_str()),
            id,
        }
    }

    /// The agent's ID.
    pub(crate) fn id(&self) -> &AgentId {
        &self.id
    }

    /// The directory itself, which is a complete TLS directory: `ca.crt` and the principal's
    /// certificate and key.
    pub(crate) fn tls_dir(&self) -> &Path {
        &self.dir
    }

    /// The token's file.
    pub(crate) fn token_file(&self) -> PathBuf {
        self.dir.join(TOKEN_FILE)
    }

    /// The record's file.
    pub(crate) fn record_file(&self) -> PathBuf {
        self.dir.join(RECORD_FILE)
    }

    fn stem(&self) -> String {
        format!("client.{}", self.id.principal())
    }

    fn cert_file(&self) -> PathBuf {
        self.dir.join(format!("{}.crt", self.stem()))
    }

    fn key_file(&self) -> PathBuf {
        self.dir.join(format!("{}.key", self.stem()))
    }

    fn ca_file(&self) -> PathBuf {
        self.dir.join(AUTHORITY_CERT_FILE)
    }

    /// The identity's record, or `None` if the directory holds none.
    ///
    /// The format version is read first: a record from a newer release is refused with a
    /// message that says to upgrade, never read in part (ADR-0044, point 1.7).
    ///
    /// # Errors
    ///
    /// [`AgentError::Newer`] for a record of a later format, [`AgentError::Record`] for one
    /// that isn't the format, and [`AgentError::File`] if it can't be read.
    pub(crate) fn record(&self) -> Result<Option<AgentRecord>, AgentError> {
        let path = self.record_file();
        let text = match fs::read_to_string(&path) {
            Ok(text) => text,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(source) => {
                return Err(AgentError::File {
                    action: "read",
                    path,
                    source,
                });
            }
        };
        #[derive(Deserialize)]
        struct Header {
            version: Option<u32>,
        }
        let parse = |source| AgentError::Record {
            path: path.clone(),
            source,
        };
        let header: Header = toml::from_str(&text).map_err(parse)?;
        if header
            .version
            .is_some_and(|version| version > RECORD_VERSION)
        {
            return Err(AgentError::Newer { path });
        }
        let record: AgentRecord = toml::from_str(&text).map_err(parse)?;
        if record.version != RECORD_VERSION {
            return Err(AgentError::Mismatch {
                path,
                reason: format!("its version is {}, not {RECORD_VERSION}", record.version),
            });
        }
        if record.principal != self.id.principal().as_str() {
            return Err(AgentError::Mismatch {
                path,
                reason: format!(
                    "it names the principal '{}', not '{}'",
                    record.principal,
                    self.id.principal()
                ),
            });
        }
        Ok(Some(record))
    }

    /// Mints a new identity from `issuer`: a certificate for `agent-<id>` and a token with
    /// `grant`'s scopes, both valid from `now`, with `ca.crt` and `identity.toml`. Holds the
    /// directory's lock while it works.
    ///
    /// # Errors
    ///
    /// [`AgentError::Exists`] if the directory holds an identity already (renew it instead), and
    /// the errors of reading the record, issuing, and writing.
    pub(crate) fn mint(
        &self,
        issuer: Issuer<'_>,
        grant: &MintGrant,
        now: Timestamp,
    ) -> Result<AgentRecord, AgentError> {
        let _lock = self.lock()?;
        if self.record()?.is_some() {
            return Err(AgentError::Exists {
                id: self.id.clone(),
                dir: self.dir.clone(),
            });
        }
        self.write_new(issuer, grant, now)
    }

    /// Uses the identity if there is one, renewing what [`renewal`](Self::renewal) says is due,
    /// and otherwise mints one as [`mint`](Self::mint) does. Under the directory's lock, so two
    /// starts at once mint one identity. A renewal keeps the record's scopes, access, and owner,
    /// and gives the certificate `cert_ttl` and the token `token_ttl`.
    ///
    /// # Errors
    ///
    /// [`AgentError::Mismatch`] if the identity is another fleet's, and the errors of reading
    /// the record, issuing, and writing.
    pub(crate) fn acquire(
        &self,
        issuer: Issuer<'_>,
        grant: &MintGrant,
        now: Timestamp,
    ) -> Result<Acquired, AgentError> {
        let _lock = self.lock()?;
        let Some(old) = self.record()? else {
            return self.write_new(issuer, grant, now).map(Acquired::Minted);
        };
        self.check_fleet(&old, issuer.authority())?;
        let renewal = self.renewal(&old, issuer.authority().certificate_pem(), now);
        if renewal == Renewal::None {
            return Ok(Acquired::Reused(old));
        }
        let record = self.reissue(issuer, &old, renewal, grant, now)?;
        Ok(Acquired::Renewed {
            record,
            old: Box::new(old),
            renewal,
        })
    }

    /// Reissues the certificate and the token now, keeping the record's scopes, access, and
    /// owner, and returns the new record and the old one.
    ///
    /// # Errors
    ///
    /// [`AgentError::NoIdentity`] if there is no identity to renew, [`AgentError::Mismatch`] if
    /// it is another fleet's, and the errors of issuing and writing.
    pub(crate) fn renew(
        &self,
        issuer: Issuer<'_>,
        grant: &MintGrant,
        now: Timestamp,
    ) -> Result<(AgentRecord, AgentRecord), AgentError> {
        let _lock = self.lock()?;
        let Some(old) = self.record()? else {
            return Err(AgentError::NoIdentity {
                id: self.id.clone(),
                dir: self.dir.clone(),
            });
        };
        self.check_fleet(&old, issuer.authority())?;
        let record = self.reissue(issuer, &old, Renewal::Both, grant, now)?;
        Ok((record, old))
    }

    /// What a start needs to renew (ADR-0043, point 8.2): both credentials if a file is
    /// missing, or if `ca.crt` isn't the authority's `ca_pem` (a dev directory that started over,
    /// whose new key signed neither); the certificate when fewer than 30 days remain; the token
    /// when fewer than 7 days remain.
    pub(crate) fn renewal(&self, record: &AgentRecord, ca_pem: &str, now: Timestamp) -> Renewal {
        let present = [
            self.cert_file(),
            self.key_file(),
            self.token_file(),
            self.ca_file(),
        ]
        .iter()
        .all(|path| path.is_file());
        let same_authority = fs::read_to_string(self.ca_file()).is_ok_and(|ca| ca == ca_pem);
        if !present || !same_authority {
            return Renewal::Both;
        }
        let due = |expiry: Option<Timestamp>, within: Duration| {
            let within = Millis::new(u64::try_from(within.as_millis()).unwrap_or(u64::MAX));
            expiry.is_none_or(|expiry| now.saturating_add(within) >= expiry)
        };
        match (
            due(record.cert_expiry(), RENEW_CERT_WITHIN),
            due(record.token_expiry(), RENEW_TOKEN_WITHIN),
        ) {
            (true, true) => Renewal::Both,
            (true, false) => Renewal::Certificate,
            (false, true) => Renewal::Token,
            (false, false) => Renewal::None,
        }
    }

    /// One warning for each of the directory, the key, and the token that others than its owner
    /// may read (`docs/cli.md`, section 9): a copy or a `chmod` may have loosened them.
    pub(crate) fn loose_files(&self) -> Vec<String> {
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            [
                (self.dir.clone(), "700"),
                (self.key_file(), "600"),
                (self.token_file(), "600"),
            ]
            .into_iter()
            .filter(|(path, _)| {
                fs::metadata(path).is_ok_and(|metadata| metadata.permissions().mode() & 0o077 != 0)
            })
            .map(|(path, mode)| {
                format!(
                    "warning: {} is readable by other users; run `chmod {mode} {}`",
                    path.display(),
                    path.display()
                )
            })
            .collect()
        }
        #[cfg(not(unix))]
        Vec::new()
    }

    /// Deletes the directory and everything in it.
    ///
    /// # Errors
    ///
    /// [`AgentError::File`] if it can't be removed.
    pub(crate) fn remove(&self) -> Result<(), AgentError> {
        refuse_link(&self.dir)?;
        fs::remove_dir_all(&self.dir).map_err(|source| AgentError::File {
            action: "remove",
            path: self.dir.clone(),
            source,
        })
    }

    /// Refuses an identity of another fleet than `authority`'s, which a mint from it would
    /// silently replace.
    fn check_fleet(&self, record: &AgentRecord, authority: &Authority) -> Result<(), AgentError> {
        if record.fleet == authority.fleet().as_str() {
            return Ok(());
        }
        Err(AgentError::Mismatch {
            path: self.record_file(),
            reason: format!(
                "it is for fleet '{}', but the authority is for fleet '{}'",
                record.fleet,
                authority.fleet()
            ),
        })
    }

    /// Creates the directory `0700` if it is missing, and locks its [`LOCK_FILE`], waiting for
    /// another holder. Dropping the file releases the lock.
    fn lock(&self) -> Result<File, AgentError> {
        create_private_dir(&self.dir)?;
        let path = self.dir.join(LOCK_FILE);
        let mut options = OpenOptions::new();
        options.write(true).create(true).truncate(false);
        #[cfg(unix)]
        std::os::unix::fs::OpenOptionsExt::mode(&mut options, 0o600);
        let file = options.open(&path).map_err(|source| AgentError::File {
            action: "open",
            path: path.clone(),
            source,
        })?;
        file.lock().map_err(|source| AgentError::File {
            action: "lock",
            path,
            source,
        })?;
        Ok(file)
    }

    /// Writes a whole new identity. The lock is held.
    fn write_new(
        &self,
        issuer: Issuer<'_>,
        grant: &MintGrant,
        now: Timestamp,
    ) -> Result<AgentRecord, AgentError> {
        let authority = issuer.authority();
        let written =
            self.write_credentials(authority, Renewal::Both, &grant.scopes, grant, now)?;
        let record = AgentRecord {
            version: RECORD_VERSION,
            principal: self.id.principal().to_string(),
            owner: grant.owner.clone(),
            parent: None,
            fleet: authority.fleet().to_string(),
            scopes: grant.scopes.iter().map(ToString::to_string).collect(),
            read_only: grant.access == Access::ReadOnly,
            token_id: written.token_id.unwrap_or_default(),
            token_expires_at: written.token_expires_at.unwrap_or_default(),
            cert_expires_at: written.cert_expires_at.unwrap_or_default(),
            issued_by: issuer.issued_by(),
        };
        self.write_record(&record)?;
        Ok(record)
    }

    /// Reissues what `renewal` names, keeping the rest of `old`. The lock is held.
    fn reissue(
        &self,
        issuer: Issuer<'_>,
        old: &AgentRecord,
        renewal: Renewal,
        grant: &MintGrant,
        now: Timestamp,
    ) -> Result<AgentRecord, AgentError> {
        let scopes = old
            .scopes
            .iter()
            .map(|scope| scope.parse())
            .collect::<Result<Vec<Scope>, _>>()
            .map_err(|source| AgentError::Scope {
                path: self.record_file(),
                source,
            })?;
        let written = self.write_credentials(issuer.authority(), renewal, &scopes, grant, now)?;
        let record = AgentRecord {
            token_id: written.token_id.unwrap_or_else(|| old.token_id.clone()),
            token_expires_at: written
                .token_expires_at
                .unwrap_or_else(|| old.token_expires_at.clone()),
            cert_expires_at: written
                .cert_expires_at
                .unwrap_or_else(|| old.cert_expires_at.clone()),
            issued_by: issuer.issued_by(),
            ..old.clone()
        };
        self.write_record(&record)?;
        Ok(record)
    }

    /// Issues and writes what `renewal` names: the certificate, its key and `ca.crt`, and the
    /// token with `scopes`. Returns the certificate's expiry, the token's ID and its expiry, for
    /// what it wrote.
    fn write_credentials(
        &self,
        authority: &Authority,
        renewal: Renewal,
        scopes: &[Scope],
        grant: &MintGrant,
        now: Timestamp,
    ) -> Result<Written, AgentError> {
        let principal = self.id.principal();
        let mut written = Written::default();
        let mut token = None;
        if renewal.token() {
            let ttl = grant.token_ttl.min(MAX_TOKEN_LIFETIME);
            let id = new_token_id()?;
            let expires_at = now.saturating_add(Millis::new(
                u64::try_from(ttl.as_millis()).unwrap_or(u64::MAX),
            ));
            let issued = authority.token_signer()?.issue(&Grant {
                principal: principal.clone(),
                scopes: scopes.iter().cloned().collect(),
                id: id.clone(),
                not_before: now,
                expires_at,
            })?;
            token = Some((issued, id, expires_at));
        }
        if renewal.certificate() {
            let issued = authority.issue_client(&principal, Validity::new(now, grant.cert_ttl))?;
            let lifetime =
                Millis::new(u64::try_from(grant.cert_ttl.as_millis()).unwrap_or(u64::MAX));
            write_private(&self.ca_file(), authority.certificate_pem().as_bytes())?;
            write_private(&self.cert_file(), issued.certificate_pem().as_bytes())?;
            write_private(&self.key_file(), issued.key_pem().as_bytes())?;
            written.cert_expires_at = Some(crate::format_time(
                seconds_floor(now.saturating_add(lifetime)).as_millis(),
            ));
        }
        if let Some((issued, id, expires_at)) = token {
            let mut text = issued.expose().to_owned();
            text.push('\n');
            write_private(&self.token_file(), text.as_bytes())?;
            written.token_id = Some(id.to_string());
            written.token_expires_at =
                Some(crate::format_time(seconds_floor(expires_at).as_millis()));
        }
        Ok(written)
    }

    fn write_record(&self, record: &AgentRecord) -> Result<(), AgentError> {
        let text = toml::to_string(record).map_err(|source| AgentError::File {
            action: "write",
            path: self.record_file(),
            source: io::Error::other(source),
        })?;
        let text = format!(
            "# Written by fleetring: agent {}'s identity. It holds no secret.\n{text}",
            self.id
        );
        write_private(&self.record_file(), text.as_bytes())?;
        sync_dir(&self.dir)
    }
}

/// `at`, with its milliseconds dropped, as certificates and tokens record times.
fn seconds_floor(at: Timestamp) -> Timestamp {
    Timestamp::from_millis(at.as_millis() / 1_000 * 1_000)
}

/// A new token ID: 16 random bytes in base64url.
///
/// # Errors
///
/// [`AgentError::Random`] if the system's random number generator fails.
pub(crate) fn new_token_id() -> Result<TokenId, AgentError> {
    let mut random = [0_u8; TOKEN_ID_BYTES];
    SystemRandom::new()
        .fill(&mut random)
        .map_err(|_| AgentError::Random)?;
    URL_SAFE_NO_PAD
        .encode(random)
        .parse()
        .map_err(|_| AgentError::Random)
}

/// Loads the authority in `dir`, and names the recovery when `ca.key` is there without
/// `ca.crt` (ADR-0043, point 8.10).
///
/// # Errors
///
/// [`AgentError::MissingCaCert`] for that case, and [`AgentError::Authority`] otherwise.
pub(crate) fn load_authority(dir: &Path) -> Result<Authority, AgentError> {
    if dir.join(AUTHORITY_KEY_FILE).is_file() && !dir.join(AUTHORITY_CERT_FILE).exists() {
        return Err(AgentError::MissingCaCert {
            dir: dir.to_owned(),
        });
    }
    Ok(Authority::load(dir)?)
}

/// Where `--agent` identities live for the selected context (ADR-0043, point 8.4): the
/// context's `agents_dir`; for a context dev mode wrote, `<dev_dir>/agents`; for any other,
/// `<data dir>/agents/<context>`; and with no context, dev mode's, `~/.fleetring/agents`.
///
/// # Errors
///
/// [`AgentError::NoHome`] if the directory needs a home directory and there is none.
pub(crate) fn agents_dir(
    context: Option<(&ContextName, &Context)>,
    env: &dyn Fn(&str) -> Option<OsString>,
) -> Result<PathBuf, AgentError> {
    let Some((name, context)) = context else {
        return Ok(dev::default_dir_in(env)?.join(dev::AGENTS_DIR));
    };
    if let Some(dir) = &context.agents_dir {
        return Ok(dir.clone());
    }
    if let Some(dev_dir) = &context.dev_dir {
        return Ok(dev_dir.join(dev::AGENTS_DIR));
    }
    Ok(dirs::data_dir(env)?
        .join(dev::AGENTS_DIR)
        .join(name.as_str()))
}

/// Refuses `path` if it is a symbolic link: an identity's directory is a real directory, so a
/// link can't send its keys elsewhere.
fn refuse_link(path: &Path) -> Result<(), AgentError> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() => Err(AgentError::Link {
            path: path.to_owned(),
        }),
        _ => Ok(()),
    }
}

/// Writes `contents` to `path`, which must not exist, `0600` on Unix.
///
/// # Errors
///
/// [`AgentError::File`] if the file exists or can't be written.
pub(crate) fn write_new_private(path: &Path, contents: &str) -> Result<(), AgentError> {
    let failed = |action, source| AgentError::File {
        action,
        path: path.to_owned(),
        source,
    };
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    std::os::unix::fs::OpenOptionsExt::mode(&mut options, 0o600);
    let mut file = options
        .open(path)
        .map_err(|source| failed("create", source))?;
    file.write_all(contents.as_bytes())
        .and_then(|()| file.sync_all())
        .map_err(|source| failed("write", source))
}

/// Creates `dir` and its missing parents `0700` on Unix, refuses a link, and tightens an
/// existing directory to `0700`.
///
/// # Errors
///
/// [`AgentError::Link`] for a link, and [`AgentError::File`] if it can't be created or
/// restricted.
pub(crate) fn create_private_dir(dir: &Path) -> Result<(), AgentError> {
    refuse_link(dir)?;
    let failed = |action, source| AgentError::File {
        action,
        path: dir.to_owned(),
        source,
    };
    let mut builder = fs::DirBuilder::new();
    builder.recursive(true);
    #[cfg(unix)]
    std::os::unix::fs::DirBuilderExt::mode(&mut builder, 0o700);
    builder
        .create(dir)
        .map_err(|source| failed("create", source))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        let mode = fs::metadata(dir)
            .map_err(|source| failed("check", source))?
            .permissions()
            .mode();
        if mode & 0o777 != 0o700 {
            fs::set_permissions(dir, fs::Permissions::from_mode(0o700))
                .map_err(|source| failed("restrict", source))?;
        }
    }
    Ok(())
}

/// Writes `contents` to `path`, `0600` on Unix: to a new temporary file beside it, synced, then
/// renamed over it, so a reader sees the old file or the new one. The temporary file is created
/// exclusively, so it never follows a link.
fn write_private(path: &Path, contents: &[u8]) -> Result<(), AgentError> {
    let mut name = path.as_os_str().to_owned();
    name.push(".tmp");
    let temporary = PathBuf::from(name);
    let failed = |action, source| AgentError::File {
        action,
        path: path.to_owned(),
        source,
    };
    match fs::remove_file(&temporary) {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(source) => return Err(failed("clear", source)),
    }
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    std::os::unix::fs::OpenOptionsExt::mode(&mut options, 0o600);
    let mut file = options
        .open(&temporary)
        .map_err(|source| failed("create", source))?;
    file.write_all(contents)
        .and_then(|()| file.sync_all())
        .map_err(|source| failed("write", source))?;
    drop(file);
    fs::rename(&temporary, path).map_err(|source| failed("replace", source))
}

/// Syncs `dir`, so the renames into it survive a crash; a no-op where directories can't be
/// opened.
fn sync_dir(dir: &Path) -> Result<(), AgentError> {
    #[cfg(unix)]
    File::open(dir)
        .and_then(|handle| handle.sync_all())
        .map_err(|source| AgentError::File {
            action: "sync",
            path: dir.to_owned(),
            source,
        })?;
    #[cfg(not(unix))]
    let _ = dir;
    Ok(())
}

/// Why an agent's identity couldn't be found, read, or minted.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub(crate) enum AgentError {
    #[error(
        "invalid agent ID '{id}': 1 to {MAX_ID_LEN} lowercase letters, digits, or '_' ('-' ends \
         the ID in agent-<id>-*, and '.' is reserved for delegated agents)"
    )]
    InvalidId { id: String },
    #[error(
        "agent '{id}' already has an identity in {}\n  hint: run `fleetring agent renew {id}` \
         to reissue its credentials",
        dir.display()
    )]
    Exists { id: AgentId, dir: PathBuf },
    #[error(
        "agent '{id}' has no identity in {}\n  hint: create it with `fleetring agent add {id}`",
        dir.display()
    )]
    NoIdentity { id: AgentId, dir: PathBuf },
    #[error("{}: this identity was written by a newer fleetring; upgrade", path.display())]
    Newer { path: PathBuf },
    #[error("{} isn't a valid identity file", path.display())]
    Record {
        path: PathBuf,
        #[source]
        source: toml::de::Error,
    },
    #[error("{} can't be used: {reason}", path.display())]
    Mismatch { path: PathBuf, reason: String },
    #[error("{} holds a scope this release refuses", path.display())]
    Scope {
        path: PathBuf,
        #[source]
        source: ParseScopeError,
    },
    #[error(
        "{} holds {AUTHORITY_KEY_FILE} but not {AUTHORITY_CERT_FILE}\n  hint: copy ca.crt back \
         from a node's [tls] ca file or a backup; in dev mode, remove {} to start a new authority \
         (every certificate and token the old one issued stops working)",
        dir.display(), dir.display()
    )]
    MissingCaCert { dir: PathBuf },
    #[error("{} is a symbolic link; an agent's identity must be a real directory", path.display())]
    Link { path: PathBuf },
    #[error("can't {action} {}", path.display())]
    File {
        action: &'static str,
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("the system's random number generator failed")]
    Random,
    #[error(transparent)]
    Authority(#[from] AuthorityError),
    #[error("can't issue the token")]
    Issue(#[from] IssueError),
    #[error(transparent)]
    Dev(#[from] DevError),
    #[error(transparent)]
    NoHome(#[from] DirsError),
}

#[cfg(test)]
mod tests {
    use fleetringd::tls::DEFAULT_AUTHORITY_LIFETIME;

    use super::*;

    fn id(text: &str) -> AgentId {
        text.parse().expect("an agent ID")
    }

    fn authority(fleet: &str) -> Authority {
        Authority::create(
            fleet.parse().expect("a fleet"),
            Validity::new(Timestamp::from_millis(0), DEFAULT_AUTHORITY_LIFETIME),
        )
        .expect("an authority")
    }

    fn grant(agent: &AgentId) -> MintGrant {
        MintGrant {
            scopes: vec![agent.default_scope(Access::ReadWrite)],
            access: Access::ReadWrite,
            cert_ttl: DEV_CERT_LIFETIME,
            token_ttl: MAX_TOKEN_LIFETIME,
            owner: "admin".to_owned(),
        }
    }

    const DAY: u64 = SECONDS_PER_DAY * 1_000;

    #[test]
    fn agent_ids_are_short_lowercase_words_without_dash_or_dot() {
        for valid in ["42", "a", "x_y", &"9".repeat(MAX_ID_LEN)] {
            assert!(valid.parse::<AgentId>().is_ok(), "{valid}");
        }
        for invalid in [
            "",
            "4-2",
            "42.c7",
            "Agent",
            "a b",
            "é",
            "../x",
            &"9".repeat(MAX_ID_LEN + 1),
        ] {
            let error = invalid.parse::<AgentId>().expect_err(invalid);
            assert!(error.to_string().contains("invalid agent ID"), "{error}");
        }
        let agent = id("42");
        assert_eq!(agent.principal().as_str(), "agent-42");
        assert!(!agent.principal().is_reserved_agent_name());
        assert_eq!(
            agent.default_scope(Access::ReadWrite).to_string(),
            "admin:agent-42-*"
        );
        assert_eq!(
            agent.default_scope(Access::ReadOnly).to_string(),
            "read:agent-42-*"
        );
        assert_eq!(
            agent.default_scope(Access::ReadWrite).separator_warning(),
            None
        );
    }

    #[cfg(unix)]
    #[test]
    fn a_mint_writes_private_files_and_a_record_without_a_secret() -> Result<(), AgentError> {
        use std::os::unix::fs::PermissionsExt as _;
        let root = tempfile::tempdir().map_err(|source| AgentError::File {
            action: "create",
            path: PathBuf::new(),
            source,
        })?;
        let agent = id("42");
        let dir = AgentDir::new(&root.path().join("agents"), agent.clone());
        let issuing = authority("dev");
        let now = Timestamp::from_millis(1_800_000_000_000);
        let record = dir.mint(Issuer::Authority(&issuing), &grant(&agent), now)?;
        assert_eq!(record.principal, "agent-42");
        assert_eq!(record.owner, "admin");
        assert_eq!(record.parent, None);
        assert_eq!(record.scopes, ["admin:agent-42-*"]);
        assert_eq!(record.issued_by, IssuedBy::Authority);
        let mode = |path: &Path| {
            fs::metadata(path)
                .map(|metadata| metadata.permissions().mode() & 0o777)
                .unwrap_or(0)
        };
        assert_eq!(mode(dir.tls_dir()), 0o700);
        for name in [
            "ca.crt",
            "client.agent-42.crt",
            "client.agent-42.key",
            "token",
            "identity.toml",
        ] {
            assert_eq!(mode(&dir.tls_dir().join(name)), 0o600, "{name}");
        }
        let token = fs::read_to_string(dir.token_file()).unwrap_or_default();
        let text = fs::read_to_string(dir.record_file()).unwrap_or_default();
        assert!(!text.contains(token.trim()), "the record holds no token");
        assert!(!text.contains("PRIVATE KEY"), "the record holds no key");
        assert!(!text.contains("parent"), "{text}");
        assert_eq!(dir.record()?, Some(record.clone()));
        // A second mint refuses, and names `renew`.
        let again = dir
            .mint(Issuer::Authority(&issuing), &grant(&agent), now)
            .expect_err("an identity exists");
        assert!(again.to_string().contains("agent renew 42"), "{again}");
        // Nothing is due a day later; the token is due once fewer than 7 days remain.
        let pem = issuing.certificate_pem();
        assert_eq!(
            dir.renewal(&record, pem, now.saturating_add(Millis::new(DAY))),
            Renewal::None
        );
        assert_eq!(
            dir.renewal(&record, pem, now.saturating_add(Millis::new(24 * DAY))),
            Renewal::Token
        );
        // Another authority signed none of it.
        assert_eq!(
            dir.renewal(&record, authority("dev").certificate_pem(), now),
            Renewal::Both
        );
        Ok(())
    }

    #[test]
    fn a_record_from_a_newer_release_says_to_upgrade() -> Result<(), Box<dyn std::error::Error>> {
        let root = tempfile::tempdir()?;
        let dir = AgentDir::new(root.path(), id("42"));
        fs::create_dir_all(dir.tls_dir())?;
        fs::write(
            dir.record_file(),
            "version = 2\nprincipal = \"agent-42\"\nholder_key = \"holder.key\"\n",
        )?;
        let error = dir.record().expect_err("a newer record");
        assert!(
            error
                .to_string()
                .ends_with("this identity was written by a newer fleetring; upgrade"),
            "{error}"
        );
        fs::write(dir.record_file(), "version = 1\nunknown = 1\n")?;
        assert!(matches!(dir.record(), Err(AgentError::Record { .. })));
        Ok(())
    }

    #[test]
    fn agents_live_beside_their_authority_or_under_the_data_directory() {
        let env = |name: &str| match name {
            "HOME" | "USERPROFILE" => Some(OsString::from("/home/me")),
            "XDG_DATA_HOME" => Some(OsString::from("/data")),
            "LOCALAPPDATA" => Some(OsString::from("/local")),
            _ => None,
        };
        let name: ContextName = "prod".parse().expect("a name");
        let dev = Context {
            dev_dir: Some(PathBuf::from("/home/me/.fleetring")),
            ..Context::default()
        };
        assert_eq!(
            agents_dir(Some((&name, &dev)), &env).ok(),
            Some(PathBuf::from("/home/me/.fleetring/agents"))
        );
        let pinned = Context {
            agents_dir: Some(PathBuf::from("/srv/agents")),
            ..dev
        };
        assert_eq!(
            agents_dir(Some((&name, &pinned)), &env).ok(),
            Some(PathBuf::from("/srv/agents"))
        );
        let prod = Context::default();
        let data = if cfg!(windows) { "/local" } else { "/data" };
        assert_eq!(
            agents_dir(Some((&name, &prod)), &env).ok(),
            Some(
                PathBuf::from(data)
                    .join("fleetring")
                    .join("agents")
                    .join("prod")
            )
        );
        assert_eq!(
            agents_dir(None, &env).ok(),
            Some(PathBuf::from("/home/me").join(".fleetring").join("agents"))
        );
    }

    #[test]
    fn a_missing_ca_crt_beside_its_key_names_the_recovery() -> Result<(), Box<dyn std::error::Error>>
    {
        let root = tempfile::tempdir()?;
        authority("prod").write(root.path())?;
        fs::remove_file(root.path().join(AUTHORITY_CERT_FILE))?;
        let error = load_authority(root.path()).expect_err("no ca.crt");
        let text = error.to_string();
        assert!(text.contains("holds ca.key but not ca.crt"), "{text}");
        assert!(text.contains("hint: copy ca.crt back"), "{text}");
        Ok(())
    }
}
