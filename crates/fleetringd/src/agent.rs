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

use std::collections::BTreeSet;
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
use fleetringd::auth::{
    BearerToken, Grant, IssueError, MAX_TOKEN_LIFETIME, NOT_BEFORE_LEEWAY_SECS, ParseScopeError,
    Scope,
};
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

/// The newest `identity.toml` format this release reads and writes (ADR-0044, point 1.7).
///
/// Version 2 adds `scopes_from`. A record is written at version 2 only when it holds that field,
/// and at version 1, field for field, when a 0.7 reader can use it as it is.
const RECORD_VERSION: u32 = 2;

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
///
/// The principal and the default scopes are built when the ID is parsed, so that using them
/// can't fail.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct AgentId {
    id: String,
    principal: Principal,
    admin: Scope,
    read: Scope,
    stream: Scope,
}

impl AgentId {
    /// The ID as text.
    pub(crate) fn as_str(&self) -> &str {
        &self.id
    }

    /// The principal `agent-<id>`.
    pub(crate) fn principal(&self) -> Principal {
        self.principal.clone()
    }

    /// The scopes an identity gets unless `--scope` says otherwise (FR-081): `admin:agent-<id>-*`
    /// and `stream:agent-<id>-*`, or `read:agent-<id>-*` and `stream:agent-<id>-*` for a reading
    /// agent. No other action implies `stream` (ADR-0025), and following an agent's own changes
    /// is reading them, so both get it.
    pub(crate) fn default_scopes(&self, access: Access) -> Vec<Scope> {
        vec![self.access_scope(access), self.stream.clone()]
    }

    /// The scope that gives `access`: `admin:agent-<id>-*` or `read:agent-<id>-*`. It was 0.7's
    /// only default.
    fn access_scope(&self, access: Access) -> Scope {
        match access {
            Access::ReadOnly => self.read.clone(),
            Access::ReadWrite => self.admin.clone(),
        }
    }

    /// The scopes `agent renew` gives `record` in place of its own: this release's defaults, if
    /// `record` holds exactly 0.7's default for its access and doesn't say `--scope` chose its
    /// scopes. `None` keeps the scopes as they are, so scopes of an identity's own are never
    /// widened.
    pub(crate) fn widened(&self, record: &AgentRecord) -> Option<Vec<Scope>> {
        if record.scopes_from != ScopesFrom::Defaults {
            return None;
        }
        let old = self.access_scope(record.access()).to_string();
        let held: BTreeSet<&str> = record.scopes.iter().map(String::as_str).collect();
        (held == BTreeSet::from([old.as_str()])).then(|| self.default_scopes(record.access()))
    }
}

impl FromStr for AgentId {
    type Err = AgentError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        let invalid = || AgentError::InvalidId {
            id: text.to_owned(),
        };
        let valid = !text.is_empty()
            && text.len() <= MAX_ID_LEN
            && text
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_');
        if !valid {
            return Err(invalid());
        }
        let scope = |action: &str| -> Result<Scope, AgentError> {
            format!("{action}:{AGENT_PREFIX}{text}-*")
                .parse()
                .map_err(|_| invalid())
        };
        Ok(Self {
            id: text.to_owned(),
            principal: format!("{AGENT_PREFIX}{text}")
                .parse()
                .map_err(|_| invalid())?,
            admin: scope("admin")?,
            read: scope("read")?,
            stream: scope("stream")?,
        })
    }
}

impl fmt::Display for AgentId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.id)
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
    /// Where the scopes came from. Written only when `--scope` chose them, in a version 2
    /// record, so that `agent renew` never widens them.
    #[serde(default, skip_serializing_if = "ScopesFrom::is_defaults")]
    pub(crate) scopes_from: ScopesFrom,
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

/// Where an identity's scopes came from (FR-081).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum ScopesFrom {
    /// The defaults of the release that minted the identity. A version 1 record can't say
    /// more, so for one that 0.7 wrote this also covers scopes `--scope` chose.
    #[default]
    Defaults,
    /// `--scope`, which replaced the defaults.
    Flags,
}

impl ScopesFrom {
    #[expect(
        clippy::trivially_copy_pass_by_ref,
        reason = "serde's skip_serializing_if passes a reference"
    )]
    fn is_defaults(&self) -> bool {
        *self == Self::Defaults
    }

    /// The `identity.toml` version a record is written at: 1 without `scopes_from`, so a 0.7
    /// reader uses it, and 2 with it, which a 0.7 reader refuses with "upgrade".
    fn record_version(self) -> u32 {
        match self {
            Self::Defaults => 1,
            Self::Flags => 2,
        }
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
    /// Whether they are the defaults or `--scope`'s.
    pub(crate) scopes_from: ScopesFrom,
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
    /// It renewed part of the identity; `old` is the token it replaced, if it replaced the
    /// token.
    Renewed {
        record: AgentRecord,
        old: Option<OldToken>,
        renewal: Renewal,
    },
}

/// What [`AgentDir::renew`] did.
#[derive(Debug, Clone)]
pub(crate) struct Renewed {
    /// The identity's record now.
    pub(crate) record: AgentRecord,
    /// The token it replaced, to revoke.
    pub(crate) old: OldToken,
    /// What happened to the scopes.
    pub(crate) scopes: ScopeRenewal,
}

/// What [`AgentDir::renew`] did to an identity's scopes (FR-081).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ScopeRenewal {
    /// They were 0.7's default and are now this release's defaults; `added` are the scopes
    /// the renewal added.
    Widened { added: Vec<String> },
    /// They were kept: this release's defaults already, or scopes of the identity's own.
    Kept,
}

/// A token to revoke: its ID, and when it stops being valid. Both are read from the token
/// itself, never from `identity.toml`, which anyone who can write the directory can edit: a
/// revocation that ended sooner than the token would let it back in.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct OldToken {
    /// The token's ID (`jti`).
    pub(crate) id: String,
    /// When the token expires (`exp`), or, if the token can't be read, the latest any token
    /// could.
    pub(crate) until: Timestamp,
}

/// Whether [`AgentDir::lock`] creates a missing directory.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Missing {
    /// Create it `0700`, for a mint.
    Create,
    /// Refuse, creating nothing, for a command on an identity that must exist.
    Refuse,
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
        let needs = record.scopes_from.record_version();
        if record.version < needs {
            return Err(AgentError::Mismatch {
                path,
                reason: format!(
                    "its version is {}, but scopes_from needs version {needs}",
                    record.version
                ),
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
        let _lock = self.lock(Missing::Create)?;
        if self.record()?.is_some() {
            return Err(AgentError::Exists {
                id: self.id.to_string(),
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
        let _lock = self.lock(Missing::Create)?;
        let Some(old) = self.record()? else {
            return self.write_new(issuer, grant, now).map(Acquired::Minted);
        };
        self.check_fleet(&old, issuer.authority())?;
        let renewal = self.renewal(&old, issuer.authority().certificate_pem(), now);
        if renewal == Renewal::None {
            return Ok(Acquired::Reused(old));
        }
        // Read before the new token replaces it.
        let old_token = renewal.token().then(|| self.old_token(&old, now));
        let record = self.reissue(issuer, &old, renewal, grant, now)?;
        Ok(Acquired::Renewed {
            record,
            old: old_token,
            renewal,
        })
    }

    /// Reissues the certificate and the token now, keeping the record's access and owner, and
    /// returns the new record, the token it replaced, and what happened to the scopes. Creates
    /// nothing if there is no identity.
    ///
    /// The scopes are kept, except that an identity holding exactly 0.7's default gets this
    /// release's defaults, as [`AgentId::widened`] decides: `admin:agent-<id>-*` alone becomes
    /// that and `stream:agent-<id>-*`. Scopes of an identity's own are never widened.
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
    ) -> Result<Renewed, AgentError> {
        let no_identity = || AgentError::NoIdentity {
            id: self.id.to_string(),
            dir: self.dir.clone(),
        };
        if self.record()?.is_none() {
            return Err(no_identity());
        }
        let _lock = self.lock(Missing::Refuse)?;
        let Some(old) = self.record()? else {
            return Err(no_identity());
        };
        self.check_fleet(&old, issuer.authority())?;
        let old_token = self.old_token(&old, now);
        let (old, scopes) = match self.id.widened(&old) {
            Some(widened) => {
                let widened: Vec<String> = widened.iter().map(ToString::to_string).collect();
                let added = widened
                    .iter()
                    .filter(|scope| !old.scopes.contains(scope))
                    .cloned()
                    .collect();
                // Still the defaults, so the record keeps the version a 0.7 reader can use.
                let record = AgentRecord {
                    scopes: widened,
                    ..old
                };
                (record, ScopeRenewal::Widened { added })
            }
            None => (old, ScopeRenewal::Kept),
        };
        let record = self.reissue(issuer, &old, Renewal::Both, grant, now)?;
        Ok(Renewed {
            record,
            old: old_token,
            scopes,
        })
    }

    /// The token in the directory, to revoke: its ID and expiry, read from the token itself.
    /// If the token can't be read, the record's ID, revoked until the latest any token issued
    /// by `now` could last, so the revocation never ends before the token does.
    pub(crate) fn old_token(&self, record: &AgentRecord, now: Timestamp) -> OldToken {
        let claimed = fs::read_to_string(self.token_file())
            .ok()
            .and_then(|text| BearerToken::new(text.trim()).claimed_id_and_expiry());
        match claimed {
            Some((id, until)) => OldToken {
                id: id.to_string(),
                until,
            },
            None => {
                let longest =
                    MAX_TOKEN_LIFETIME.as_millis() + u128::from(NOT_BEFORE_LEEWAY_SECS) * 1_000;
                OldToken {
                    id: record.token_id.clone(),
                    until: now
                        .saturating_add(Millis::new(u64::try_from(longest).unwrap_or(u64::MAX))),
                }
            }
        }
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

    /// Locks the directory's [`LOCK_FILE`], waiting for another holder; dropping the file
    /// releases the lock. A missing directory is created `0700`, or refused, as `missing` says.
    fn lock(&self, missing: Missing) -> Result<File, AgentError> {
        match missing {
            Missing::Create => create_private_dir(&self.dir)?,
            Missing::Refuse => {
                refuse_link(&self.dir)?;
                if !self.dir.is_dir() {
                    return Err(AgentError::NoIdentity {
                        id: self.id.to_string(),
                        dir: self.dir.clone(),
                    });
                }
            }
        }
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
            version: grant.scopes_from.record_version(),
            principal: self.id.principal().to_string(),
            owner: grant.owner.clone(),
            parent: None,
            fleet: authority.fleet().to_string(),
            scopes: grant.scopes.iter().map(ToString::to_string).collect(),
            scopes_from: grant.scopes_from,
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

/// Readies the directory `cert client --out` writes into. A missing one is created `0700`, with
/// its missing parents. An existing one must be an empty directory, not a link, and keeps its
/// mode, since it may be the working or home directory; the warning returned says so if others
/// can read it.
///
/// # Errors
///
/// [`AgentError::OutNotEmpty`] for a directory that holds anything, [`AgentError::Link`] for a
/// link, and [`AgentError::File`] if it can't be read or created.
pub(crate) fn prepare_out_dir(dir: &Path) -> Result<Option<String>, AgentError> {
    refuse_link(dir)?;
    let failed = |action, source| AgentError::File {
        action,
        path: dir.to_owned(),
        source,
    };
    let mut entries = match fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            let mut builder = fs::DirBuilder::new();
            builder.recursive(true);
            #[cfg(unix)]
            std::os::unix::fs::DirBuilderExt::mode(&mut builder, 0o700);
            builder
                .create(dir)
                .map_err(|source| failed("create", source))?;
            return Ok(None);
        }
        Err(source) => return Err(failed("read", source)),
    };
    if entries.next().is_some() {
        return Err(AgentError::OutNotEmpty {
            dir: dir.to_owned(),
        });
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        let mode = fs::metadata(dir)
            .map_err(|source| failed("check", source))?
            .permissions()
            .mode();
        if mode & 0o077 != 0 {
            return Ok(Some(format!(
                "warning: {} is readable by other users, and its mode is left as it is; the key \
                 is written 0600, but `chmod 700 {}` keeps the directory private",
                dir.display(),
                dir.display()
            )));
        }
    }
    Ok(None)
}

/// Creates an agent's directory and its missing parents `0700` on Unix, refuses a link, and
/// tightens an existing directory to `0700`.
fn create_private_dir(dir: &Path) -> Result<(), AgentError> {
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
    let written = file
        .write_all(contents)
        .and_then(|()| file.sync_all())
        .map_err(|source| failed("write", source));
    drop(file);
    let replaced = written
        .and_then(|()| fs::rename(&temporary, path).map_err(|source| failed("replace", source)));
    if replaced.is_err() {
        // A partial secret is no use to anyone; best effort, since the write already failed.
        let _ = fs::remove_file(&temporary);
    }
    replaced
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
    Exists { id: String, dir: PathBuf },
    #[error(
        "agent '{id}' has no identity in {}\n  hint: create it with `fleetring agent add {id}`",
        dir.display()
    )]
    NoIdentity { id: String, dir: PathBuf },
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
    #[error(
        "{} is a symbolic link; keys are written only into a real directory",
        path.display()
    )]
    Link { path: PathBuf },
    #[error(
        "{} isn't empty, so `--out` won't write keys into it\n  hint: pass a new or empty \
         directory",
        dir.display()
    )]
    OutNotEmpty { dir: PathBuf },
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
            scopes: agent.default_scopes(Access::ReadWrite),
            scopes_from: ScopesFrom::Defaults,
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
        let shown = |access| -> Vec<String> {
            agent
                .default_scopes(access)
                .iter()
                .map(ToString::to_string)
                .collect()
        };
        assert_eq!(
            shown(Access::ReadWrite),
            ["admin:agent-42-*", "stream:agent-42-*"]
        );
        assert_eq!(
            shown(Access::ReadOnly),
            ["read:agent-42-*", "stream:agent-42-*"]
        );
        for access in [Access::ReadWrite, Access::ReadOnly] {
            for scope in agent.default_scopes(access) {
                assert_eq!(scope.separator_warning(), None, "{scope}");
            }
        }
    }

    /// A record of agent 42 with `scopes`, as `scopes_from` says they came.
    fn record_with(scopes: &[&str], scopes_from: ScopesFrom, access: Access) -> AgentRecord {
        AgentRecord {
            version: scopes_from.record_version(),
            principal: "agent-42".to_owned(),
            owner: "admin".to_owned(),
            parent: None,
            fleet: "dev".to_owned(),
            scopes: scopes.iter().map(|&scope| scope.to_owned()).collect(),
            scopes_from,
            read_only: access == Access::ReadOnly,
            token_id: "t1".to_owned(),
            token_expires_at: "2026-12-01T00:00:00Z".to_owned(),
            cert_expires_at: "2027-11-01T00:00:00Z".to_owned(),
            issued_by: IssuedBy::Dev,
        }
    }

    #[test]
    fn only_exactly_the_old_default_is_widened() {
        let agent = id("42");
        let widened = |record: &AgentRecord| -> Option<Vec<String>> {
            agent
                .widened(record)
                .map(|scopes| scopes.iter().map(ToString::to_string).collect())
        };
        use Access::{ReadOnly, ReadWrite};
        use ScopesFrom::{Defaults, Flags};
        // 0.7's defaults become this release's.
        assert_eq!(
            widened(&record_with(&["admin:agent-42-*"], Defaults, ReadWrite)),
            Some(vec![
                "admin:agent-42-*".to_owned(),
                "stream:agent-42-*".to_owned()
            ])
        );
        assert_eq!(
            widened(&record_with(&["read:agent-42-*"], Defaults, ReadOnly)),
            Some(vec![
                "read:agent-42-*".to_owned(),
                "stream:agent-42-*".to_owned()
            ])
        );
        // Everything else stays as it is.
        for record in [
            // Today's defaults already.
            record_with(
                &["admin:agent-42-*", "stream:agent-42-*"],
                Defaults,
                ReadWrite,
            ),
            // The old default, chosen with --scope.
            record_with(&["admin:agent-42-*"], Flags, ReadWrite),
            // Another access's default.
            record_with(&["read:agent-42-*"], Defaults, ReadWrite),
            record_with(&["admin:agent-42-*"], Defaults, ReadOnly),
            // Scopes of its own, narrower, wider, or another agent's.
            record_with(&["write:agent-42-*"], Defaults, ReadWrite),
            record_with(&["admin:agent-42-*", "read:x-*"], Defaults, ReadWrite),
            record_with(&["admin:agent-4-*"], Defaults, ReadWrite),
            record_with(&[], Defaults, ReadWrite),
        ] {
            assert_eq!(widened(&record), None, "{record:?}");
        }
    }

    #[test]
    fn renew_widens_the_old_default_and_keeps_scopes_chosen_with_scope()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = tempfile::tempdir()?;
        let agent = id("42");
        let issuing = authority("dev");
        let now = Timestamp::from_millis(1_800_000_000_000);
        let dir = AgentDir::new(root.path(), agent.clone());
        // An identity as 0.7 minted it: version 1 and `admin:agent-42-*` alone.
        let old = MintGrant {
            scopes: vec!["admin:agent-42-*".parse()?],
            ..grant(&agent)
        };
        dir.mint(Issuer::Authority(&issuing), &old, now)?;
        let renewed = dir.renew(Issuer::Authority(&issuing), &grant(&agent), now)?;
        assert_eq!(
            renewed.scopes,
            ScopeRenewal::Widened {
                added: vec!["stream:agent-42-*".to_owned()]
            }
        );
        assert_eq!(
            renewed.record.scopes,
            ["admin:agent-42-*", "stream:agent-42-*"]
        );
        assert_eq!(renewed.record.version, 1, "a 0.7 reader still reads it");
        // The token carries the new scope: its claims, read without verifying.
        let token = fs::read_to_string(dir.token_file())?;
        let payload = token.trim().split('.').nth(1).ok_or("a token")?;
        let claims = String::from_utf8(URL_SAFE_NO_PAD.decode(payload)?)?;
        assert!(claims.contains("stream:agent-42-*"), "{claims}");
        // A second renewal has nothing to add.
        let again = dir.renew(Issuer::Authority(&issuing), &grant(&agent), now)?;
        assert_eq!(again.scopes, ScopeRenewal::Kept);
        assert_eq!(again.record.scopes, renewed.record.scopes);

        // The same scope chosen with --scope is recorded so, at version 2, and kept.
        let chosen = AgentDir::new(root.path(), id("43"));
        let flags = MintGrant {
            scopes: vec!["admin:agent-43-*".parse()?],
            scopes_from: ScopesFrom::Flags,
            ..grant(&id("43"))
        };
        let record = chosen.mint(Issuer::Authority(&issuing), &flags, now)?;
        assert_eq!(record.version, 2);
        let text = fs::read_to_string(chosen.record_file())?;
        assert!(text.contains("scopes_from = \"flags\""), "{text}");
        let kept = chosen.renew(Issuer::Authority(&issuing), &grant(&id("43")), now)?;
        assert_eq!(kept.scopes, ScopeRenewal::Kept);
        assert_eq!(kept.record.scopes, ["admin:agent-43-*"]);
        assert_eq!(kept.record.version, 2);
        assert_eq!(chosen.record()?, Some(kept.record));
        Ok(())
    }

    #[test]
    fn a_version_1_record_that_says_scopes_from_is_refused()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = tempfile::tempdir()?;
        let dir = AgentDir::new(root.path(), id("42"));
        fs::create_dir_all(dir.tls_dir())?;
        let mut record = record_with(&["admin:agent-42-*"], ScopesFrom::Flags, Access::ReadWrite);
        record.version = 1;
        fs::write(dir.record_file(), toml::to_string(&record)?)?;
        assert!(
            matches!(dir.record(), Err(AgentError::Mismatch { .. })),
            "{:?}",
            dir.record()
        );
        Ok(())
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
        assert_eq!(record.scopes, ["admin:agent-42-*", "stream:agent-42-*"]);
        assert_eq!(
            record.version, 1,
            "the defaults need nothing a 0.7 reader lacks"
        );
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
    fn the_token_to_revoke_is_read_from_the_token_never_the_record()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = tempfile::tempdir()?;
        let agent = id("42");
        let dir = AgentDir::new(root.path(), agent.clone());
        let issuing = authority("dev");
        let now = Timestamp::from_millis(1_800_000_000_000);
        let record = dir.mint(Issuer::Authority(&issuing), &grant(&agent), now)?;
        let token = BearerToken::new(fs::read_to_string(dir.token_file())?.trim());
        let (id, exp) = token.claimed_id_and_expiry().ok_or("a token")?;
        // A record that says the token ends sooner, under another ID, changes nothing.
        let edited = AgentRecord {
            token_id: "forged".to_owned(),
            token_expires_at: crate::format_time(now.as_millis()),
            ..record.clone()
        };
        let old = dir.old_token(&edited, now);
        assert_eq!(old.id, id.to_string());
        assert_eq!(old.until, exp);
        // Without a readable token, the revocation outlasts any token issued by now.
        fs::write(dir.token_file(), "not a token")?;
        let old = dir.old_token(&record, now);
        assert_eq!(old.id, record.token_id);
        let longest =
            u64::try_from(MAX_TOKEN_LIFETIME.as_millis())? + NOT_BEFORE_LEEWAY_SECS * 1_000;
        assert_eq!(old.until, now.saturating_add(Millis::new(longest)));
        Ok(())
    }

    #[test]
    fn renewing_a_missing_identity_creates_nothing() -> Result<(), Box<dyn std::error::Error>> {
        let root = tempfile::tempdir()?;
        let dir = AgentDir::new(&root.path().join("agents"), id("9"));
        let issuing = authority("dev");
        let renewed = dir.renew(
            Issuer::Authority(&issuing),
            &grant(&id("9")),
            Timestamp::from_millis(1_800_000_000_000),
        );
        assert!(matches!(renewed, Err(AgentError::NoIdentity { .. })));
        assert!(!root.path().join("agents").exists());
        Ok(())
    }

    #[test]
    fn a_record_from_a_newer_release_says_to_upgrade() -> Result<(), Box<dyn std::error::Error>> {
        let root = tempfile::tempdir()?;
        let dir = AgentDir::new(root.path(), id("42"));
        fs::create_dir_all(dir.tls_dir())?;
        fs::write(
            dir.record_file(),
            format!(
                "version = {}\nprincipal = \"agent-42\"\nholder_key = \"holder.key\"\n",
                RECORD_VERSION + 1
            ),
        )?;
        let error = dir.record().expect_err("a newer record");
        assert!(
            error
                .to_string()
                .ends_with("this identity was written by a newer fleetring; upgrade"),
            "{error}"
        );
        // A version this release knows is read strictly: an unknown field is refused.
        for version in 1..=RECORD_VERSION {
            fs::write(
                dir.record_file(),
                format!("version = {version}\nunknown = 1\n"),
            )?;
            assert!(
                matches!(dir.record(), Err(AgentError::Record { .. })),
                "{version}"
            );
        }
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
